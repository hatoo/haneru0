use crate::disk::Aligned;
use crate::disk::DiskManager;
use crate::disk::PageId;
use async_rwlock::RwLock;
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
    page_id: PageId,
    page: RwLock<Page>,
}

struct Frame<T> {
    usage_count: AtomicU64,
    buffer: Arc<T>,
}

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

    fn size(&self) -> usize {
        self.buffers.len()
    }

    fn get(&self, buffer_id: BufferId) -> Arc<T> {
        let frame = self.buffers[buffer_id.0].as_ref().unwrap();
        frame.usage_count.fetch_add(1, Ordering::Release);
        frame.buffer.clone()
    }
}

struct PagePool {
    pool: BufferPool<Buffer>,
    page_table: std::collections::HashMap<PageId, BufferId>,
}

impl PagePool {
    async fn alloc_page(&mut self, disk: &DiskManager) -> Result<BufferId, Error> {
        let (buffer_id, prev_buffer) = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        if let Some(prev_buffer) = prev_buffer {
            let lock = prev_buffer.page.read().await;
            if lock.is_dirty {
                if let Err(err) = disk.write_page_data(prev_buffer.page_id, &lock).await {
                    std::mem::drop(lock);
                    self.pool.insert(buffer_id, prev_buffer);
                    return Err(err.into());
                }
            }
            self.page_table.remove(&prev_buffer.page_id);
        }
        Ok(buffer_id)
    }
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

    pub async fn fetch_page(&self, page_id: PageId) -> Result<Arc<Buffer>, Error> {
        {
            let lock = self.page_pool.read().await;

            if let Some(&buffer_id) = lock.page_table.get(&page_id) {
                let buffer = lock.pool.get(buffer_id);
                debug_assert_eq!(buffer.page_id, page_id);
                return Ok(buffer);
            }
        }

        let mut lock = self.page_pool.write().await;

        if let Some(&buffer_id) = lock.page_table.get(&page_id) {
            let buffer = lock.pool.get(buffer_id);
            debug_assert_eq!(buffer.page_id, page_id);
            return Ok(buffer);
        }

        let buffer_id = lock.alloc_page(&self.disk).await?;
        let mut page = Aligned::default();
        self.disk.read_page_data(page_id, &mut page).await?;

        lock.page_table.insert(page_id, buffer_id);
        let buffer = Buffer {
            page_id,
            page: RwLock::new(Page {
                is_dirty: false,
                page: Box::new(page),
            }),
        };
        lock.pool.insert(buffer_id, buffer);
        Ok(lock.pool.get(buffer_id))
    }

    pub async fn create_page(&self) -> Result<Arc<Buffer>, Error> {
        let mut lock = self.page_pool.write().await;
        let buffer_id = lock.alloc_page(&self.disk).await?;
        let page_id = self.disk.allocate_page();

        lock.page_table.insert(page_id, buffer_id);
        let buffer = Buffer {
            page_id,
            page: Default::default(),
        };
        lock.pool.insert(buffer_id, buffer);
        Ok(lock.pool.get(buffer_id))
    }
}

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        futures::executor::block_on(async {
            let lock = self.page_pool.read().await;
            for (&page_id, &buffer_id) in lock.page_table.iter() {
                let buffer = lock.pool.get(buffer_id);
                let lock = buffer.page.read().await;
                if lock.is_dirty {
                    self.disk.write_page_data(page_id, &lock).await.unwrap();
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
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
}
