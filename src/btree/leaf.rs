use std::borrow::Cow;

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use super::Pair;
use crate::slotted_overflow::SlottedOverflow;
use crate::{bsearch::binary_search_by_async, disk::PageId};
use crate::{buffer, freelist::FreeList};

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    prev_page_id: PageId,
    next_page_id: PageId,
}

pub struct Leaf<B> {
    header: LayoutVerified<B, Header>,
    body: SlottedOverflow<B>,
}

impl<B: ByteSlice> Leaf<B> {
    pub fn new(bytes: B) -> Self {
        let (header, body) =
            LayoutVerified::new_from_prefix(bytes).expect("leaf header must be aligned");
        let body = SlottedOverflow::new(body);
        Self { header, body }
    }

    pub fn prev_page_id(&self) -> Option<PageId> {
        self.header.prev_page_id.valid()
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        self.header.next_page_id.valid()
    }

    pub fn num_pairs(&self) -> usize {
        self.body.num_slots()
    }

    pub async fn search_slot_id(
        &self,
        key: &[u8],
        free_list: &FreeList,
    ) -> Result<Result<usize, usize>, buffer::Error> {
        binary_search_by_async(self.num_pairs(), |slot_id| async move {
            let data = self.data_at(slot_id, free_list).await?;
            Ok(Pair::from_bytes(&data).key.cmp(key))
        })
        .await
    }

    #[cfg(test)]
    pub async fn search_data(
        &self,
        key: &[u8],
        free_list: &FreeList,
    ) -> Result<Option<Cow<'_, [u8]>>, buffer::Error> {
        if let Some(slot_id) = self.search_slot_id(key, free_list).await?.ok() {
            Ok(Some(self.data_at(slot_id, free_list).await?))
        } else {
            Ok(None)
        }
    }

    pub async fn data_at(
        &self,
        slot_id: usize,
        free_list: &FreeList,
    ) -> Result<Cow<'_, [u8]>, buffer::Error> {
        Ok(self.body.fetch(slot_id, free_list).await?)
    }
}

impl<B: ByteSliceMut> Leaf<B> {
    pub fn initialize(&mut self) {
        self.header.prev_page_id = PageId::INVALID_PAGE_ID;
        self.header.next_page_id = PageId::INVALID_PAGE_ID;
        self.body.initialize();
    }

    pub fn set_prev_page_id(&mut self, prev_page_id: Option<PageId>) {
        self.header.prev_page_id = prev_page_id.into()
    }

    pub fn set_next_page_id(&mut self, next_page_id: Option<PageId>) {
        self.header.next_page_id = next_page_id.into()
    }

    #[must_use = "insertion may fail"]
    pub async fn insert(
        &mut self,
        slot_id: usize,
        key: &[u8],
        value: &[u8],
        free_list: &FreeList,
    ) -> Result<Option<()>, buffer::Error> {
        let pair = Pair { key, value };
        let pair_bytes = pair.to_bytes();
        Ok(self.body.insert(slot_id, &pair_bytes, free_list).await?)
    }

    fn is_half_full(&self) -> bool {
        2 * self.body.free_capacity() < self.body.capacity()
    }

    pub async fn split_insert(
        &mut self,
        new_leaf: &mut Leaf<impl ByteSliceMut>,
        new_key: &[u8],
        new_value: &[u8],
        free_list: &FreeList,
    ) -> Result<Vec<u8>, buffer::Error> {
        new_leaf.initialize();
        loop {
            if new_leaf.is_half_full() {
                let index = self
                    .search_slot_id(new_key, free_list)
                    .await?
                    .expect_err("key must be unique");
                self.insert(index, new_key, new_value, free_list)
                    .await?
                    .expect("old leaf must have space");
                break;
            }
            let data = self.data_at(0, free_list).await?;
            let pair = Pair::from_bytes(&data);
            if pair.key < new_key {
                self.transfer(new_leaf);
            } else {
                new_leaf
                    .insert(new_leaf.num_pairs(), new_key, new_value, free_list)
                    .await?
                    .expect("new leaf must have space");
                while !new_leaf.is_half_full() {
                    self.transfer(new_leaf);
                }
                break;
            }
        }
        let data = self.data_at(0, free_list).await?;
        Ok(Pair::from_bytes(&data).key.to_vec())
    }

    pub async fn remove(
        &mut self,
        slot_id: usize,
        free_list: &FreeList,
    ) -> Result<(), buffer::Error> {
        self.body.remove(slot_id, free_list).await
    }

    pub fn transfer(&mut self, dest: &mut Leaf<impl ByteSliceMut>) {
        self.body.transfer(&mut dest.body).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    use crate::buffer::BufferPoolManager;
    use crate::disk::DiskManager;

    #[tokio::test]
    async fn test_leaf_insert() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();

        let page = free_list.new_page().await.unwrap();
        let mut page_lock = page.page.write().await;
        let mut leaf_page = Leaf::new(page_lock.as_bytes_mut());
        leaf_page.initialize();

        let id = leaf_page
            .search_slot_id(b"deadbeef", &free_list)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(0, id);
        leaf_page
            .insert(id, b"deadbeef", b"world", &free_list)
            .await
            .unwrap()
            .unwrap();

        let data = leaf_page.data_at(0, &free_list).await.unwrap();
        assert_eq!(b"deadbeef", Pair::from_bytes(&data).key);

        let id = leaf_page
            .search_slot_id(b"facebook", &free_list)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(1, id);
        leaf_page
            .insert(id, b"facebook", b"!", &free_list)
            .await
            .unwrap()
            .unwrap();

        let data = leaf_page.data_at(0, &free_list).await.unwrap();
        assert_eq!(b"deadbeef", Pair::from_bytes(&data).key);
        let data = leaf_page.data_at(1, &free_list).await.unwrap();
        assert_eq!(b"facebook", Pair::from_bytes(&data).key);

        let id = leaf_page
            .search_slot_id(b"beefdead", &free_list)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(0, id);
        leaf_page
            .insert(id, b"beefdead", b"hello", &free_list)
            .await
            .unwrap()
            .unwrap();

        let data = leaf_page.data_at(0, &free_list).await.unwrap();
        assert_eq!(b"beefdead", Pair::from_bytes(&data).key);
        let data = leaf_page.data_at(1, &free_list).await.unwrap();
        assert_eq!(b"deadbeef", Pair::from_bytes(&data).key);
        let data = leaf_page.data_at(2, &free_list).await.unwrap();
        assert_eq!(b"facebook", Pair::from_bytes(&data).key);
        let data = leaf_page
            .search_data(b"beefdead", &free_list)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"hello", Pair::from_bytes(&data).value);
    }
}
