use crate::disk::Aligned;
use crate::disk::DiskManager;
use crate::disk::PageId;
use crate::sync::{Arc, AtomicU64, Ordering};
use async_rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
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

pub struct WithReadLockGuard {
    buffer: Arc<Buffer>,
    lock: RwLockReadGuard<'static, Page>,
}

impl WithReadLockGuard {
    pub async fn new(buffer: Arc<Buffer>) -> Self {
        let lock = buffer.page.read().await;
        Self {
            lock: unsafe { std::mem::transmute(lock) },
            buffer,
        }
    }

    pub fn into_inner(self) -> Arc<Buffer> {
        self.buffer
    }
}

impl Deref for WithReadLockGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.lock.deref()
    }
}

pub struct WithWriteLockGuard {
    buffer: Arc<Buffer>,
    lock: RwLockWriteGuard<'static, Page>,
}

impl WithWriteLockGuard {
    pub async fn new(buffer: Arc<Buffer>) -> Self {
        let lock = buffer.page.write().await;
        Self {
            lock: unsafe { std::mem::transmute(lock) },
            buffer,
        }
    }

    pub fn try_new(buffer: Arc<Buffer>) -> Option<Self> {
        let lock = buffer
            .page
            .try_write()
            .map(|lock| unsafe { std::mem::transmute(lock) });

        lock.map(|lock| Self { lock, buffer })
    }

    pub fn into_inner(self) -> Arc<Buffer> {
        self.buffer
    }

    pub fn page_id(&self) -> PageId {
        self.buffer.page_id
    }
}

impl Deref for WithWriteLockGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.lock.deref()
    }
}

impl DerefMut for WithWriteLockGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.lock.deref_mut()
    }
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
        let buffers = (0..pool_size).map(|_| None).collect();
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

                        #[cfg(not(loom))]
                        let last = frame.take().map(|g| Arc::try_unwrap(g.buffer).unwrap());

                        #[cfg(loom)]
                        let last = None;

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

#[derive(Debug)]
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

    async fn alloc_page<'a>(
        &'a self,
        lock: RwLockWriteGuard<'a, PagePool>,
    ) -> Result<(BufferId, RwLockWriteGuard<'a, PagePool>), Error> {
        let mut lock = lock;

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
                        PageTableItem::Reading(watch) | PageTableItem::Writing(watch) => {
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
            let lock = self.page_pool.write().await;

            if let Some(page_table_item) = lock.page_table.get(&page_id) {
                match page_table_item {
                    PageTableItem::Read(buffer_id) => {
                        let buffer = lock.pool.get(*buffer_id);
                        debug_assert_eq!(buffer.page_id, page_id);
                        return Ok(buffer);
                    }
                    PageTableItem::Reading(watch) | PageTableItem::Writing(watch) => {
                        let watch = watch.clone();
                        std::mem::drop(lock);
                        let _ = watch.recv_async().await;
                    }
                }
            } else {
                let (buffer_id, mut lock) = match self.alloc_page(lock).await {
                    Ok(ret) => ret,
                    Err(err) => {
                        return Err(err);
                    }
                };
                let (_tx, rx) = flume::bounded(0);
                lock.page_table.insert(page_id, PageTableItem::Reading(rx));
                lock.pool.insert(
                    buffer_id,
                    Buffer {
                        page_id,
                        page: Default::default(),
                    },
                );
                let buffer = lock.pool.get(buffer_id);
                std::mem::drop(lock);

                let mut page = buffer.page.write().await;
                if let Err(err) = self.disk.read_page_data(page_id, &mut page).await {
                    let mut lock = self.page_pool.write().await;
                    lock.page_table.remove(&page_id);
                    lock.pool.remove(buffer_id);
                    return Err(err.into());
                }
                std::mem::drop(page);

                let mut lock = self.page_pool.write().await;

                lock.page_table
                    .insert(page_id, PageTableItem::Read(buffer_id));
                return Ok(buffer);
            }
        }
    }

    pub async fn create_page(&self) -> Result<Arc<Buffer>, Error> {
        let (buffer_id, mut lock) = self.alloc_page(self.page_pool.write().await).await?;
        let page_id = self.disk.allocate_page();
        lock.page_table
            .insert(page_id, PageTableItem::Read(buffer_id));
        lock.pool.insert(
            buffer_id,
            Buffer {
                page_id,
                page: RwLock::new(Page {
                    is_dirty: true,
                    page: Default::default(),
                }),
            },
        );
        Ok(lock.pool.get(buffer_id))
    }
}

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        let lock = self.page_pool.try_read().unwrap();
        for (&page_id, page_table_item) in lock.page_table.iter() {
            if let PageTableItem::Read(buffer_id) = page_table_item {
                let buffer = lock.pool.get(*buffer_id);
                let lock = buffer.page.try_read().unwrap();
                if lock.is_dirty {
                    self.disk.write_page_data_sync(page_id, &lock).unwrap();
                }
            } else {
                panic!(
                    "Found PageTableItem::Reading/Writing variant while dropping. This is a bug!"
                );
            }
        }
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

    #[cfg(loom)]
    #[test]
    fn test_buffer_pool_manager_loom() {
        const N_PAGES: usize = 4;
        const POOL_SIZE: usize = 4;

        loom::model(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(async {
                    let disk_manager = DiskManager::default();
                    let buffer_pool_manager = BufferPoolManager::new(disk_manager, POOL_SIZE);

                    let buffer_pool_manager = Arc::new(buffer_pool_manager);

                    let v = (0..N_PAGES)
                        .map(|i| {
                            let buffer_pool_manager = buffer_pool_manager.clone();
                            tokio::spawn(async move {
                                let buffer = buffer_pool_manager.create_page().await.unwrap();
                                let page_id = buffer.page_id;
                                {
                                    let mut lock = buffer.page.write().await;
                                    lock.fill(i as u8);
                                }
                                drop(buffer);
                                let buffer = buffer_pool_manager.fetch_page(page_id).await.unwrap();
                                assert!(buffer.page.read().await.iter().all(|b| *b == i as u8));
                            })
                        })
                        .collect::<Vec<_>>();

                    for f in v {
                        f.await.unwrap();
                    }
                })
        })
    }
}
