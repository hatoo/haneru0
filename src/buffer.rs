use crate::disk::Aligned;
use crate::disk::DiskManager;
use crate::disk::PageId;
use async_rwlock::{RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
struct BufferId(usize);

#[derive(Debug, Default)]
pub struct Page {
    page: Box<Aligned>,
    is_dirty: bool,
}

impl Deref for Page {
    type Target = Aligned;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.is_dirty = true;
        &mut self.page
    }
}

#[derive(Debug)]
pub struct Buffer {
    pub page_id: PageId,
    pub page: RwLock<Page>,
}

#[derive(Debug)]
struct Frame<T> {
    usage_count: AtomicU64,
    buffer: Arc<T>,
}

#[derive(Debug)]
struct BufferPool<T> {
    buffers: Vec<Option<Frame<T>>>,
    next_victim_id: BufferId,
}

impl<T: std::fmt::Debug> BufferPool<T> {
    fn new(pool_size: usize) -> Self {
        assert!(pool_size > 0);
        let mut buffers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            buffers.push(None);
        }
        Self {
            buffers,
            next_victim_id: BufferId(0),
        }
    }

    fn evict(&mut self) -> Option<(BufferId, Option<T>)> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;

        loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self.buffers[next_victim_id.0];

            match frame {
                None => {
                    return Some((next_victim_id, None));
                }
                Some(f) => {
                    if f.usage_count.load(Ordering::Acquire) == 0
                    // Always Arc::strong_count(&f.buffer) == 1
                    {
                        debug_assert_eq!(Arc::strong_count(&f.buffer), 1);
                        let last = frame.take().map(|g| Arc::try_unwrap(g.buffer).unwrap());
                        return Some((next_victim_id, last));
                    }
                    if Arc::get_mut(&mut f.buffer).is_some() {
                        f.usage_count.fetch_sub(1, Ordering::Release);
                        consecutive_pinned = 0;
                    } else {
                        consecutive_pinned += 1;
                        if consecutive_pinned >= pool_size {
                            return None;
                        }
                    }
                }
            }
            self.next_victim_id = BufferId((self.next_victim_id.0 + 1) % pool_size)
        }
    }

    fn insert(&mut self, buffer_id: BufferId, value: T) {
        self.buffers[buffer_id.0] = Some(Frame {
            usage_count: AtomicU64::new(0),
            buffer: Arc::new(value),
        });
    }

    fn remove(&mut self, buffer_id: BufferId) {
        self.buffers[buffer_id.0] = None;
    }

    fn size(&self) -> usize {
        self.buffers.len()
    }

    fn get(&self, buffer_id: BufferId) -> Arc<T> {
        let frame = self.buffers[buffer_id.0].as_ref().unwrap();
        frame.usage_count.fetch_add(1, Ordering::Release);
        frame.buffer.clone()
    }
}

#[derive(Debug)]
enum PageTableItem {
    Read(BufferId),
    // We can merge Reading/Writing variant but keep separated for debug purpose.
    Reading(flume::Receiver<()>),
    Writing(flume::Receiver<()>),
}

#[derive(Debug)]
struct PagePool {
    pool: BufferPool<Buffer>,
    page_table: std::collections::HashMap<PageId, PageTableItem>,
}

pub struct BufferPoolManager {
    disk: DiskManager,
    page_pool: RwLock<PagePool>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool_size: usize) -> Self {
        Self {
            disk,
            page_pool: RwLock::new(PagePool {
                pool: BufferPool::new(pool_size),
                page_table: Default::default(),
            }),
        }
    }

    async fn alloc_page(&self) -> Result<(BufferId, RwLockWriteGuard<'_, PagePool>), Error> {
        let mut lock = self.page_pool.write().await;
        let (buffer_id, prev_buffer) = lock.pool.evict().ok_or(Error::NoFreeBuffer)?;

        let lock = if let Some(prev_buffer) = prev_buffer {
            let buffer_lock = prev_buffer.page.read().await;
            if buffer_lock.is_dirty {
                std::mem::drop(buffer_lock);
                let (_tx, rx) = flume::bounded(0);
                lock.page_table
                    .insert(prev_buffer.page_id, PageTableItem::Writing(rx));
                lock.pool.insert(buffer_id, prev_buffer);
                let prev_buffer = lock.pool.get(buffer_id);
                std::mem::drop(lock);
                let buffer_lock = prev_buffer.page.read().await;
                if let Err(err) = self
                    .disk
                    .write_page_data(prev_buffer.page_id, &buffer_lock)
                    .await
                {
                    std::mem::drop(buffer_lock);
                    let mut lock = self.page_pool.write().await;
                    lock.page_table
                        .insert(prev_buffer.page_id, PageTableItem::Read(buffer_id));
                    return Err(err.into());
                }
                let mut lock = self.page_pool.write().await;
                lock.page_table.remove(&prev_buffer.page_id);
                lock.pool.remove(buffer_id);
                lock
            } else {
                lock.page_table.remove(&prev_buffer.page_id);
                lock
            }
        } else {
            lock
        };

        Ok((buffer_id, lock))
    }

    pub async fn fetch_page(&self, page_id: PageId) -> Result<Arc<Buffer>, Error> {
        {
            loop {
                let lock = self.page_pool.read().await;

                if let Some(page_table_item) = lock.page_table.get(&page_id) {
                    match page_table_item {
                        PageTableItem::Read(buffer_id) => {
                            let buffer = lock.pool.get(*buffer_id);
                            debug_assert_eq!(buffer.page_id, page_id);
                            return Ok(buffer);
                        }
                        PageTableItem::Reading(watch) => {
                            let watch = watch.clone();
                            std::mem::drop(lock);
                            let _ = watch.recv_async().await;
                        }
                        PageTableItem::Writing(watch) => {
                            let watch = watch.clone();
                            std::mem::drop(lock);
                            let _ = watch.recv_async().await;
                        }
                    }
                } else {
                    break;
                }
            }
        }

        loop {
            let mut lock = self.page_pool.write().await;

            if let Some(page_table_item) = lock.page_table.get(&page_id) {
                match page_table_item {
                    PageTableItem::Read(buffer_id) => {
                        let buffer = lock.pool.get(*buffer_id);
                        debug_assert_eq!(buffer.page_id, page_id);
                        return Ok(buffer);
                    }
                    PageTableItem::Reading(watch) => {
                        let watch = watch.clone();
                        std::mem::drop(lock);
                        let _ = watch.recv_async().await;
                    }
                    PageTableItem::Writing(watch) => {
                        let watch = watch.clone();
                        std::mem::drop(lock);
                        let _ = watch.recv_async().await;
                    }
                }
            } else {
                let (_tx, rx) = flume::bounded(0);
                lock.page_table.insert(page_id, PageTableItem::Reading(rx));
                std::mem::drop(lock);

                let mut page = Aligned::default();
                if let Err(err) = self.disk.read_page_data(page_id, &mut page).await {
                    let mut lock = self.page_pool.write().await;
                    lock.page_table.remove(&page_id);
                    return Err(err.into());
                }

                let (buffer_id, mut lock) = match self.alloc_page().await {
                    Ok(ret) => ret,
                    Err(err) => {
                        let mut lock = self.page_pool.write().await;
                        lock.page_table.remove(&page_id);
                        return Err(err);
                    }
                };

                lock.page_table
                    .insert(page_id, PageTableItem::Read(buffer_id));
                let buffer = Buffer {
                    page_id,
                    page: RwLock::new(Page {
                        is_dirty: false,
                        page: Box::new(page),
                    }),
                };
                lock.pool.insert(buffer_id, buffer);
                return Ok(lock.pool.get(buffer_id));
            }
        }
    }

    pub async fn create_page(&self) -> Result<Arc<Buffer>, Error> {
        let page_id = self.disk.allocate_page().await?;
        let (buffer_id, mut lock) = self.alloc_page().await?;

        lock.page_table
            .insert(page_id, PageTableItem::Read(buffer_id));
        let buffer = Buffer {
            page_id,
            page: RwLock::new(Page {
                is_dirty: false,
                page: Default::default(),
            }),
        };
        lock.pool.insert(buffer_id, buffer);
        Ok(lock.pool.get(buffer_id))
    }
}

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        futures::executor::block_on(async {
            let lock = self.page_pool.read().await;
            for (&page_id, page_table_item) in lock.page_table.iter() {
                if let PageTableItem::Read(buffer_id) = page_table_item {
                    let buffer = lock.pool.get(*buffer_id);
                    let lock = buffer.page.read().await;
                    if lock.is_dirty {
                        self.disk.write_page_data(page_id, &lock).await.unwrap();
                    }
                } else {
                    panic!("Found PageTableItem::Reading/Writing variant while dropping. This is a bug!");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_buffer_pool_manager_simple() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let mut memory = Aligned::default();
        let mut rng = rand::thread_rng();
        rng.fill(&mut memory[..]);
        {
            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
            let buffer = buffer_pool_manager.create_page().await.unwrap();
            buffer.page.write().await.copy_from_slice(&memory[..]);
        }

        {
            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
            let buffer = buffer_pool_manager
                .fetch_page(PageId::first())
                .await
                .unwrap();

            assert_eq!(buffer.page.read().await.deref().deref(), &memory);
        }
    }

    #[tokio::test]
    async fn test_buffer_pool_manager_single() {
        const N_PAGES: usize = 64;
        const POOL_SIZE: usize = 1;
        const N_ITER: u8 = 16;

        let path = NamedTempFile::new().unwrap().into_temp_path();
        let mut pages = Vec::new();
        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, POOL_SIZE);

        for _ in 0..N_PAGES {
            pages.push(buffer_pool_manager.create_page().await.unwrap().page_id);
        }

        {
            let buffer_pool_manager = buffer_pool_manager;

            let mut rng = StdRng::from_entropy();
            for v in 1..=N_ITER {
                pages.shuffle(&mut rng);
                for &page_id in &pages {
                    let buffer = buffer_pool_manager.fetch_page(page_id).await.unwrap();
                    assert_eq!(page_id, buffer.page_id);
                    let mut lock = buffer.page.write().await;
                    lock.fill(v);
                }
            }
        }
        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, POOL_SIZE);
        for page_id in pages {
            let buffer = buffer_pool_manager.fetch_page(page_id).await.unwrap();
            assert!(buffer.page.read().await.iter().all(|&v| v == N_ITER));
        }
    }

    #[tokio::test]
    async fn test_buffer_pool_manager_concurrent() {
        const N_PAGES: usize = 64;
        const POOL_SIZE: usize = 4;
        const N_ITER: u8 = 16;

        let path = NamedTempFile::new().unwrap().into_temp_path();
        let mut pages = Vec::new();
        {
            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, POOL_SIZE);

            for _ in 0..N_PAGES {
                pages.push(buffer_pool_manager.create_page().await.unwrap().page_id);
            }

            let mut rng = thread_rng();
            pages.shuffle(&mut rng);

            let buffer_pool_manager = Arc::new(buffer_pool_manager);

            let v = pages
                .chunks(N_PAGES / POOL_SIZE)
                .map(|page_ids| {
                    let mut page_ids = page_ids.to_vec();
                    let buffer_pool_manager = buffer_pool_manager.clone();
                    tokio::spawn(async move {
                        let mut rng = StdRng::from_entropy();
                        for v in 1..=N_ITER {
                            page_ids.shuffle(&mut rng);
                            for &page_id in &page_ids {
                                let buffer = buffer_pool_manager.fetch_page(page_id).await.unwrap();
                                assert_eq!(page_id, buffer.page_id);
                                let mut lock = buffer.page.write().await;
                                lock.fill(v);
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            for f in v {
                f.await.unwrap();
            }
        }
        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, POOL_SIZE);
        for page_id in pages {
            let buffer = buffer_pool_manager.fetch_page(page_id).await.unwrap();
            assert!(buffer.page.read().await.iter().all(|&v| v == N_ITER));
        }
    }
}
