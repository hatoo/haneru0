use crate::sync::Arc;

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use crate::{buffer::Buffer, stack::Stack};
use crate::{
    buffer::{self, BufferPoolManager},
    disk::PageId,
};

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
struct Meta {
    first_free_list: PageId,
}

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
struct Header {
    next_free_list: PageId,
}

#[derive(FromBytes)]
#[repr(C)]
struct FreePage<B> {
    header: LayoutVerified<B, Header>,
    stack: Stack<B, PageId>,
}

#[derive(Debug)]
pub struct FreeList {
    meta_page_id: PageId,
    buffer_pool_manager: BufferPoolManager,
}

impl<B: ByteSliceMut> FreePage<B> {
    fn initialize(&mut self) {
        self.header.next_free_list = PageId::INVALID_PAGE_ID;
        self.stack.initialize();
    }
}

impl<B: ByteSlice> FreePage<B> {
    fn new(bytes: B) -> Self {
        let (header, body) = LayoutVerified::new_from_prefix(bytes).unwrap();
        Self {
            header,
            stack: Stack::new(body),
        }
    }
}

impl FreeList {
    pub async fn create(buffer_pool_manager: BufferPoolManager) -> Result<Self, buffer::Error> {
        let meta_page = buffer_pool_manager.create_page().await?;
        let mut meta_page_lock = meta_page.page.write().await;
        let mut meta = LayoutVerified::<_, Meta>::new_from_prefix(meta_page_lock.as_bytes_mut())
            .unwrap()
            .0;
        meta.first_free_list = PageId::INVALID_PAGE_ID;

        Ok(Self {
            meta_page_id: meta_page.page_id,
            buffer_pool_manager,
        })
    }

    pub fn new(meta_page_id: PageId, buffer_pool_manager: BufferPoolManager) -> Self {
        Self {
            meta_page_id,
            buffer_pool_manager,
        }
    }

    async fn fetch_meta_page(&self) -> Result<Arc<Buffer>, buffer::Error> {
        self.buffer_pool_manager.fetch_page(self.meta_page_id).await
    }

    pub async fn new_page(&self) -> Result<Arc<Buffer>, buffer::Error> {
        let meta_buffer = self.fetch_meta_page().await?;
        let mut meta_buffer_lock = meta_buffer.page.write().await;

        let (mut meta, _) =
            LayoutVerified::<_, Meta>::new_from_prefix(meta_buffer_lock.as_bytes_mut()).unwrap();

        if let Some(first_free_list) = meta.first_free_list.valid() {
            let free_list_buffer = self.buffer_pool_manager.fetch_page(first_free_list).await?;
            let mut free_list_buffer_lock = free_list_buffer.page.write().await;
            let mut free_list = FreePage::new(free_list_buffer_lock.as_bytes_mut());

            if let Some(free_page_id) = free_list.stack.pop() {
                self.buffer_pool_manager
                    .fetch_page(free_page_id.clone())
                    .await
            } else {
                let next_free_list_id = free_list.header.next_free_list;
                meta.first_free_list = next_free_list_id;
                drop(free_list);
                drop(free_list_buffer_lock);
                Ok(free_list_buffer)
            }
        } else {
            self.buffer_pool_manager.create_page().await
        }
    }

    pub async fn remove_page(&self, page_id: PageId) -> Result<(), buffer::Error> {
        let meta_buffer = self
            .buffer_pool_manager
            .fetch_page(self.meta_page_id)
            .await?;
        let mut meta_buffer_lock = meta_buffer.page.write().await;

        let (mut meta, _) =
            LayoutVerified::<_, Meta>::new_from_prefix(meta_buffer_lock.as_bytes_mut()).unwrap();

        if let Some(first_free_list) = meta.first_free_list.valid() {
            let free_list_buffer = self.buffer_pool_manager.fetch_page(first_free_list).await?;
            let mut free_list_buffer_lock = free_list_buffer.page.write().await;
            let mut free_list = FreePage::new(free_list_buffer_lock.as_bytes_mut());
            if free_list.stack.push(&page_id).is_none() {
                let page_buffer = self.buffer_pool_manager.fetch_page(page_id).await?;
                let mut page_buffer_lock = page_buffer.page.write().await;
                let mut page = FreePage::new(page_buffer_lock.as_bytes_mut());
                page.initialize();
                page.header.next_free_list = first_free_list;
                meta.first_free_list = page_id;
            }
        } else {
            let page_buffer = self.buffer_pool_manager.fetch_page(page_id).await?;
            let mut page_buffer_lock = page_buffer.page.write().await;
            let mut page = FreePage::new(page_buffer_lock.as_bytes_mut());
            page.initialize();
            meta.first_free_list = page_id;
        }

        Ok(())
    }

    pub async fn fetch_page(&self, page_id: PageId) -> Result<Arc<Buffer>, buffer::Error> {
        self.buffer_pool_manager.fetch_page(page_id).await
    }
}
