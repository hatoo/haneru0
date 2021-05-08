use std::borrow::Cow;

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use super::Pair;
use crate::{bsearch::binary_search_by_async, disk::PageId};
use crate::{buffer, freelist::FreeList};
// use crate::slotted::{self, Slotted};
use crate::slotted_overflow::SlottedOverflow;

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    right_child: PageId,
}

pub struct Branch<B> {
    header: LayoutVerified<B, Header>,
    body: SlottedOverflow<B>,
}

impl<B: ByteSlice> Branch<B> {
    pub fn new(bytes: B) -> Self {
        let (header, body) =
            LayoutVerified::new_from_prefix(bytes).expect("branch header must be aligned");
        let body = SlottedOverflow::new(body);
        Self { header, body }
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

    pub async fn search_child(
        &self,
        key: &[u8],
        free_list: &FreeList,
    ) -> Result<PageId, buffer::Error> {
        let child_idx = self.search_child_idx(key, free_list).await?;
        self.child_at(child_idx, free_list).await
    }

    pub async fn search_child_idx(
        &self,
        key: &[u8],
        free_list: &FreeList,
    ) -> Result<usize, buffer::Error> {
        Ok(match self.search_slot_id(key, free_list).await? {
            Ok(slot_id) => slot_id + 1,
            Err(slot_id) => slot_id,
        })
    }

    pub async fn child_at(
        &self,
        child_idx: usize,
        free_list: &FreeList,
    ) -> Result<PageId, buffer::Error> {
        if child_idx == self.num_pairs() {
            Ok(self.header.right_child)
        } else {
            let data = self.data_at(child_idx, free_list).await?;
            Ok(Pair::from_bytes(&data).value.into())
        }
    }

    pub fn right_child(&self) -> Option<PageId> {
        self.header.right_child.valid()
    }

    pub async fn data_at(
        &self,
        slot_id: usize,
        free_list: &FreeList,
    ) -> Result<Cow<'_, [u8]>, buffer::Error> {
        Ok(self.body.fetch(slot_id, free_list).await?)
    }
}

impl<B: ByteSliceMut> Branch<B> {
    pub async fn initialize(
        &mut self,
        key: &[u8],
        left_child: PageId,
        right_child: PageId,
        free_list: &FreeList,
    ) -> Result<(), buffer::Error> {
        self.body.initialize();
        self.insert(0, key, left_child, free_list)
            .await?
            .expect("new leaf must have space");
        self.header.right_child = right_child;
        Ok(())
    }

    pub async fn fill_right_child(
        &mut self,
        free_list: &FreeList,
    ) -> Result<Vec<u8>, buffer::Error> {
        let last_id = self.num_pairs() - 1;
        let data = self.data_at(last_id, free_list).await?;
        let Pair { key, value } = Pair::from_bytes(&data);
        let right_child: PageId = value.into();
        let key_vec = key.to_vec();
        self.body.remove(last_id, free_list).await?;
        self.header.right_child = right_child;
        Ok(key_vec)
    }

    #[must_use = "insertion may fail"]
    pub async fn insert(
        &mut self,
        slot_id: usize,
        key: &[u8],
        page_id: PageId,
        free_list: &FreeList,
    ) -> Result<Option<()>, buffer::Error> {
        let pair = Pair {
            key,
            value: page_id.as_bytes(),
        };
        let pair_bytes = pair.to_bytes();
        Ok(self.body.insert(slot_id, &pair_bytes, free_list).await?)
    }

    fn is_half_full(&self) -> bool {
        2 * self.body.free_capacity() < self.body.capacity()
    }

    pub async fn split_insert(
        &mut self,
        new_branch: &mut Branch<impl ByteSliceMut>,
        new_key: &[u8],
        new_page_id: PageId,
        free_list: &FreeList,
    ) -> Result<Vec<u8>, buffer::Error> {
        new_branch.body.initialize();
        loop {
            if new_branch.is_half_full() {
                let index = self
                    .search_slot_id(new_key, free_list)
                    .await?
                    .expect_err("key must be unique");
                self.insert(index, new_key, new_page_id, free_list)
                    .await?
                    .expect("old branch must have space");
                break;
            }
            let data = self.data_at(0, free_list).await?;
            let pair = Pair::from_bytes(&data);
            if pair.key < new_key {
                self.transfer(new_branch);
            } else {
                new_branch
                    .insert(new_branch.num_pairs(), new_key, new_page_id, free_list)
                    .await?
                    .expect("new branch must have space");
                while !new_branch.is_half_full() {
                    self.transfer(new_branch);
                }
                break;
            }
        }
        new_branch.fill_right_child(free_list).await
    }

    pub async fn remove(
        &mut self,
        child_idx: usize,
        free_list: &FreeList,
    ) -> Result<(), buffer::Error> {
        if child_idx == self.num_pairs() {
            if self.num_pairs() > 0 {
                self.header.right_child = self.child_at(self.num_pairs() - 1, free_list).await?;
                self.body.remove(self.num_pairs() - 1, free_list).await?;
            } else {
                self.header.right_child = PageId::INVALID_PAGE_ID;
            }
        } else {
            self.body.remove(child_idx, free_list).await?;
        }
        Ok(())
    }

    pub fn transfer(&mut self, dest: &mut Branch<impl ByteSliceMut>) {
        self.body.transfer(&mut dest.body).unwrap()
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_search() {
        let mut data = vec![0u8; 100];
        let mut branch = Branch::new(data.as_mut_slice());
        branch.initialize(&5u64.to_be_bytes(), PageId(1), PageId(2));
        branch.insert(1, &8u64.to_be_bytes(), PageId(3)).unwrap();
        branch.insert(2, &11u64.to_be_bytes(), PageId(4)).unwrap();
        assert_eq!(PageId(1), branch.search_child(&1u64.to_be_bytes()));
        assert_eq!(PageId(3), branch.search_child(&5u64.to_be_bytes()));
        assert_eq!(PageId(3), branch.search_child(&6u64.to_be_bytes()));
        assert_eq!(PageId(4), branch.search_child(&8u64.to_be_bytes()));
        assert_eq!(PageId(4), branch.search_child(&10u64.to_be_bytes()));
        assert_eq!(PageId(2), branch.search_child(&11u64.to_be_bytes()));
        assert_eq!(PageId(2), branch.search_child(&12u64.to_be_bytes()));
    }

    #[test]
    fn test_split() {
        let mut data = vec![0u8; 100];
        let mut branch = Branch::new(data.as_mut_slice());
        branch.initialize(&5u64.to_be_bytes(), PageId(1), PageId(2));
        branch.insert(1, &8u64.to_be_bytes(), PageId(3)).unwrap();
        branch.insert(2, &11u64.to_be_bytes(), PageId(4)).unwrap();

        let mut data2 = vec![0u8; 100];
        let mut branch2 = Branch::new(data2.as_mut_slice());
        let mid_key = branch.split_insert(&mut branch2, &10u64.to_be_bytes(), PageId(5));
        assert_eq!(&8u64.to_be_bytes(), mid_key.as_slice());

        assert_eq!(2, branch.num_pairs());
        assert_eq!(1, branch2.num_pairs());

        assert_eq!(PageId(1), branch2.search_child(&1u64.to_be_bytes()));
        assert_eq!(PageId(3), branch2.search_child(&5u64.to_be_bytes()));
        assert_eq!(PageId(3), branch2.search_child(&6u64.to_be_bytes()));

        assert_eq!(PageId(5), branch.search_child(&9u64.to_be_bytes()));
        assert_eq!(PageId(4), branch.search_child(&10u64.to_be_bytes()));
        assert_eq!(PageId(2), branch.search_child(&11u64.to_be_bytes()));
        assert_eq!(PageId(2), branch.search_child(&12u64.to_be_bytes()));
    }
}

*/
