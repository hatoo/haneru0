use fs2::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::File,
    ops::{Deref, DerefMut},
};

pub const PAGE_SIZE: usize = 4096;

#[repr(align(4096))]
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Aligned([u8; PAGE_SIZE]);

impl Deref for Aligned {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Aligned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Aligned {
    fn default() -> Self {
        Aligned([0; PAGE_SIZE])
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct PageId(u64);

impl PageId {
    pub fn first() -> Self {
        PageId(0)
    }
}

pub struct DiskManager {
    heap_file: File,
    ring: rio::Rio,
    next_page_id: u64,
}

impl DiskManager {
    pub fn new(heap_file: File) -> Result<Self, std::io::Error> {
        heap_file.lock_exclusive()?;
        let heap_file_size = heap_file.metadata()?.len();
        let next_page_id = heap_file_size / PAGE_SIZE as u64;
        let ring = rio::new()?;

        Ok(Self {
            heap_file,
            ring,
            next_page_id,
        })
    }

    pub fn open(heap_file_path: impl AsRef<std::path::Path>) -> Result<Self, std::io::Error> {
        let heap_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT)
            .open(heap_file_path)?;

        Self::new(heap_file)
    }

    pub async fn read_page_data(
        &self,
        page_id: PageId,
        data: &mut Aligned,
    ) -> Result<(), std::io::Error> {
        assert!(page_id.0 < self.next_page_id);

        let at = page_id.0 * PAGE_SIZE as u64;
        let mut read_len = self
            .ring
            .read_at(&self.heap_file, data.deref_mut(), at)
            .await?;

        while read_len < PAGE_SIZE {
            let mut buf = Aligned::default();
            let len = self
                .ring
                .read_at(
                    &self.heap_file,
                    &mut buf[..PAGE_SIZE - read_len].as_mut(),
                    at + read_len as u64,
                )
                .await?;

            data[read_len..read_len + len].copy_from_slice(&buf[..read_len]);

            read_len += len;
        }
        Ok(())
    }

    pub async fn write_page_data(
        &self,
        page_id: PageId,
        data: &Aligned,
    ) -> Result<(), std::io::Error> {
        assert!(page_id.0 <= self.next_page_id);

        let at = page_id.0 * PAGE_SIZE as u64;
        let mut written_len = self.ring.write_at(&self.heap_file, &data.0, at).await?;
        while written_len < PAGE_SIZE {
            let mut buf = Aligned::default();
            buf[..PAGE_SIZE - written_len].copy_from_slice(&data[written_len..]);
            written_len += self
                .ring
                .write_at(
                    &self.heap_file,
                    &buf[..PAGE_SIZE - written_len].as_ref(),
                    at + written_len as u64,
                )
                .await?;
        }

        Ok(())
    }

    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }

    pub async fn sync(&self) -> Result<(), std::io::Error> {
        self.ring.fsync(&self.heap_file).await
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        self.ring.fdatasync(&self.heap_file).await
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_disk_manager_read_write_1() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let mut disk_manager = DiskManager::open(&path).unwrap();
        let page_id = disk_manager.allocate_page();
        let mut write_buf = Aligned::default();

        rand::thread_rng().fill_bytes(&mut write_buf[..]);

        disk_manager
            .write_page_data(page_id, &write_buf)
            .await
            .unwrap();

        let mut read_buf = Aligned::default();

        disk_manager
            .read_page_data(page_id, &mut read_buf)
            .await
            .unwrap();

        assert_eq!(write_buf, read_buf);
        assert_eq!(
            disk_manager.heap_file.metadata().unwrap().len(),
            PAGE_SIZE as u64
        );
    }

    #[tokio::test]
    async fn test_disk_manager_writes() {
        use rand::seq::SliceRandom;

        const N_PAGES: usize = 16;

        let path = NamedTempFile::new().unwrap().into_temp_path();
        let mut disk_manager = DiskManager::open(&path).unwrap();

        let pages: Vec<PageId> = (0..N_PAGES).map(|_| disk_manager.allocate_page()).collect();
        let mut memory: std::collections::HashMap<PageId, Aligned> = Default::default();

        let mut rng = rand::thread_rng();

        for _ in 0..4 * N_PAGES {
            let page_id = *pages.choose(&mut rng).unwrap();

            let mut buf = Aligned::default();
            rng.fill_bytes(&mut buf[..]);
            disk_manager.write_page_data(page_id, &buf).await.unwrap();
            memory.insert(page_id, buf);
        }

        for (page_id, buf) in memory.into_iter() {
            let mut read_buf = Aligned::default();
            disk_manager
                .read_page_data(page_id, &mut read_buf)
                .await
                .unwrap();
            assert_eq!(read_buf, buf);
        }
    }
}
