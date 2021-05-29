use std::{convert::identity, mem::size_of, sync::Arc};

use byteorder::{ByteOrder, NativeEndian};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{AsBytes, ByteSlice};

use crate::buffer::{self, Buffer, WithReadLockGuard, WithWriteLockGuard};
use crate::disk::PageId;
use crate::freelist::FreeList;

use self::branch::Branch;

pub mod branch;
pub mod leaf;
pub mod meta;
pub mod node;

#[derive(Serialize, Deserialize)]
pub struct Pair<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Pair<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(size_of::<u64>() + self.key.len() + self.value.len());
        buf.extend_from_slice(&[0; size_of::<u64>()]);
        NativeEndian::write_u64(&mut buf, self.key.len() as u64);
        buf.extend_from_slice(self.key);
        buf.extend_from_slice(self.value);
        buf
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        let len = NativeEndian::read_u64(&bytes[..size_of::<u64>()]) as usize;
        let body = &bytes[size_of::<u64>()..];

        Self {
            key: &body[..len],
            value: &body[len..],
        }
    }
}

#[derive(Debug, Error)]
pub enum InsertError {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] buffer::Error),
}

#[derive(Debug, Error)]
pub enum RemoveError {
    #[error("key not found")]
    KeyNotFound,
    #[error(transparent)]
    Buffer(#[from] buffer::Error),
}

#[derive(Debug, Clone)]
pub enum SearchMode {
    Last,
    Key(Vec<u8>),
}

impl SearchMode {
    async fn child_page_id(
        &self,
        branch: &branch::Branch<impl ByteSlice>,
        free_list: &FreeList,
    ) -> Result<PageId, buffer::Error> {
        match self {
            SearchMode::Last => Ok(branch.right_child().unwrap()),
            SearchMode::Key(key) => branch.search_child(key, free_list).await,
        }
    }

    async fn tuple_slot_id(
        &self,
        leaf: &leaf::Leaf<impl ByteSlice>,
        free_list: &FreeList,
    ) -> Result<Result<usize, Option<usize>>, buffer::Error> {
        match self {
            SearchMode::Last => Ok(Err(if leaf.num_pairs() == 0 {
                None
            } else {
                Some(leaf.num_pairs() - 1)
            })),
            SearchMode::Key(key) => leaf
                .search_slot_id(key, free_list)
                .await
                .map(|r| r.map_err(|i| if i == 0 { None } else { Some(i - 1) })),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BTree<'a> {
    pub meta_page_id: PageId,
    free_list: &'a FreeList,
}

impl<'a> BTree<'a> {
    pub async fn create(free_list: &'a FreeList) -> Result<BTree<'a>, buffer::Error> {
        let root_buffer = free_list.new_page().await?;
        let mut root_buffer_lock = root_buffer.page.write().await;
        let mut root = node::Node::new(root_buffer_lock.as_bytes_mut());
        root.initialize_as_leaf();
        let mut leaf = leaf::Leaf::new(root.body);
        leaf.initialize();
        let meta_buffer = free_list.new_page().await?;
        let mut meta_buffer_lock = meta_buffer.page.write().await;
        let mut meta = meta::Meta::new(meta_buffer_lock.as_bytes_mut());
        meta.header.root_page_id = root_buffer.page_id;
        Ok(Self {
            meta_page_id: meta_buffer.page_id,
            free_list,
        })
    }

    pub fn new(meta_page_id: PageId, free_list: &'a FreeList) -> Self {
        Self {
            meta_page_id,
            free_list,
        }
    }

    async fn fetch_meta_page(&self) -> Result<Arc<Buffer>, buffer::Error> {
        Ok(self.free_list.fetch_page(self.meta_page_id).await?)
    }

    async fn fetch_root_page(&self) -> Result<Arc<Buffer>, buffer::Error> {
        let root_page_id = {
            let meta_buffer = self.free_list.fetch_page(self.meta_page_id).await?;
            let meta_buffer_lock = meta_buffer.page.read().await;
            let meta = meta::Meta::new(meta_buffer_lock.as_bytes());
            meta.header.root_page_id
        };
        Ok(self.free_list.fetch_page(root_page_id).await?)
    }

    pub async fn search(&self, search_mode: SearchMode) -> Result<Iter<'_>, buffer::Error> {
        let mut node_buffer_lock = WithReadLockGuard::new(self.fetch_root_page().await?).await;

        loop {
            let node = node::Node::new(node_buffer_lock.as_bytes());

            match node::Body::new(node.header.node_type, node.body.as_bytes()) {
                node::Body::Leaf(leaf) => {
                    let slot_id = search_mode
                        .tuple_slot_id(&leaf, self.free_list)
                        .await?
                        .map(Some)
                        .unwrap_or_else(identity);

                    return Ok(Iter::new(self.free_list, node_buffer_lock, slot_id).await?);
                }
                node::Body::Branch(branch) => {
                    let child_page_id = search_mode.child_page_id(&branch, self.free_list).await?;
                    let lock =
                        WithReadLockGuard::new(self.free_list.fetch_page(child_page_id).await?)
                            .await;

                    node_buffer_lock = lock;
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn search_height(
        &self,
        search_mode: SearchMode,
    ) -> Result<(Iter<'_>, usize), buffer::Error> {
        let mut node_buffer_lock = WithReadLockGuard::new(self.fetch_root_page().await?).await;
        let mut height = 0;

        loop {
            let node = node::Node::new(node_buffer_lock.as_bytes());

            match node::Body::new(node.header.node_type, node.body.as_bytes()) {
                node::Body::Leaf(leaf) => {
                    let slot_id = search_mode
                        .tuple_slot_id(&leaf, self.free_list)
                        .await?
                        .map(Some)
                        .unwrap_or_else(identity);
                    return Ok((
                        Iter::new(self.free_list, node_buffer_lock, slot_id).await?,
                        height,
                    ));
                }
                node::Body::Branch(branch) => {
                    let child_page_id = search_mode.child_page_id(&branch, self.free_list).await?;
                    let lock =
                        WithReadLockGuard::new(self.free_list.fetch_page(child_page_id).await?)
                            .await;

                    node_buffer_lock = lock;

                    height += 1;
                }
            }
        }
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), InsertError> {
        let buffer = self.fetch_root_page().await?;
        let root_page_id = buffer.page_id;
        let mut buffer_lock = WithWriteLockGuard::new(buffer).await;
        let mut overflow_stack: Vec<(WithWriteLockGuard, usize)> = Vec::new();

        let new_root = loop {
            let node = node::Node::new(buffer_lock.as_bytes_mut());
            match node::Body::new(node.header.node_type, node.body) {
                node::Body::Leaf(mut leaf) => {
                    let slot_id = match leaf.search_slot_id(key, self.free_list).await? {
                        Ok(_) => return Err(InsertError::DuplicateKey),
                        Err(slot_id) => slot_id,
                    };
                    if leaf
                        .insert(slot_id, key, value, self.free_list)
                        .await?
                        .is_some()
                    {
                        overflow_stack.clear();
                        break None;
                    } else {
                        let prev_leaf_page_id = leaf.prev_page_id();
                        let prev_leaf_buffer = if let Some(next_leaf_page_id) = prev_leaf_page_id {
                            Some(self.free_list.fetch_page(next_leaf_page_id).await)
                        } else {
                            None
                        }
                        .transpose()?;

                        let new_leaf_buffer = self.free_list.new_page().await?;

                        if let Some(prev_leaf_buffer) = prev_leaf_buffer {
                            let mut prev_leaf_buffer_lock = prev_leaf_buffer.page.write().await;
                            let node = node::Node::new(prev_leaf_buffer_lock.as_bytes_mut());
                            let mut prev_leaf = leaf::Leaf::new(node.body);
                            prev_leaf.set_next_page_id(Some(new_leaf_buffer.page_id));
                        }
                        leaf.set_prev_page_id(Some(new_leaf_buffer.page_id));

                        let mut new_leaf_buffer_lock = new_leaf_buffer.page.write().await;
                        let mut new_leaf_node =
                            node::Node::new(new_leaf_buffer_lock.as_bytes_mut());
                        new_leaf_node.initialize_as_leaf();
                        let mut new_leaf = leaf::Leaf::new(new_leaf_node.body);
                        new_leaf.initialize();
                        let overflow_key = leaf
                            .split_insert(&mut new_leaf, key, value, self.free_list)
                            .await?;
                        new_leaf.set_next_page_id(Some(buffer_lock.page_id()));
                        new_leaf.set_prev_page_id(prev_leaf_page_id);

                        let mut overflow_key = overflow_key;
                        let mut overflow_page_id = new_leaf_buffer.page_id;

                        if buffer_lock.page_id() == root_page_id {
                            break Some((buffer_lock, overflow_key, overflow_page_id));
                        }

                        break loop {
                            let (mut lock, child_idx) = overflow_stack.pop().unwrap();
                            let node = node::Node::new(lock.as_bytes_mut());
                            let mut branch = Branch::new(node.body);

                            if branch
                                .insert(child_idx, &overflow_key, overflow_page_id, self.free_list)
                                .await?
                                .is_some()
                            {
                                break None;
                            } else {
                                let new_branch_buffer = self.free_list.new_page().await?;
                                let mut new_branch_buffer_lock =
                                    new_branch_buffer.page.write().await;
                                let mut new_branch_node =
                                    node::Node::new(new_branch_buffer_lock.as_bytes_mut());
                                new_branch_node.initialize_as_branch();
                                let mut new_branch = branch::Branch::new(new_branch_node.body);
                                overflow_key = branch
                                    .split_insert(
                                        &mut new_branch,
                                        &overflow_key,
                                        overflow_page_id,
                                        self.free_list,
                                    )
                                    .await?;
                                overflow_page_id = new_branch_buffer.page_id;

                                if lock.page_id() == root_page_id {
                                    break Some((lock, overflow_key, overflow_page_id));
                                }
                            }
                        };
                    }
                }
                node::Body::Branch(branch) => {
                    let child_idx = branch.search_child_idx(key, self.free_list).await?;
                    let child_page_id = branch.child_at(child_idx, self.free_list).await?;
                    let child_node_buffer =
                        WithWriteLockGuard::new(self.free_list.fetch_page(child_page_id).await?)
                            .await;

                    if !branch.may_overflow() {
                        overflow_stack.clear();
                    }

                    overflow_stack.push((buffer_lock, child_idx));
                    buffer_lock = child_node_buffer;
                }
            }
        };

        if let Some((_root_lock, key, child_page_id)) = new_root {
            let new_root_buffer = self.free_list.new_page().await?;
            let mut new_root_buffer_lock = new_root_buffer.page.write().await;
            let mut node = node::Node::new(new_root_buffer_lock.as_bytes_mut());
            node.initialize_as_branch();
            let mut branch = branch::Branch::new(node.body);
            branch
                .initialize(&key, child_page_id, root_page_id, self.free_list)
                .await?;
            let meta_buffer = self.fetch_meta_page().await?;
            let mut meta_buffer_lock = meta_buffer.page.write().await;
            let mut meta = meta::Meta::new(meta_buffer_lock.as_bytes_mut());
            meta.header.root_page_id = new_root_buffer.page_id;
        }
        Ok(())
    }

    pub async fn remove(&self, key: &[u8]) -> Result<(), RemoveError> {
        let mut buffer_lock = WithWriteLockGuard::new(self.fetch_root_page().await?).await;
        let root_page_id = buffer_lock.page_id();
        let mut shrink_stack: Vec<(WithWriteLockGuard, usize)> = Vec::new();

        loop {
            let node = node::Node::new(buffer_lock.as_bytes_mut());
            match node::Body::new(node.header.node_type, node.body) {
                node::Body::Leaf(mut leaf) => {
                    let slot_id = match leaf.search_slot_id(key, self.free_list).await? {
                        Ok(slot_id) => slot_id,
                        Err(_) => return Err(RemoveError::KeyNotFound),
                    };

                    leaf.remove(slot_id, self.free_list).await?;
                    if leaf.num_pairs() == 0 {
                        let modified_siblings = match (leaf.prev_page_id(), leaf.next_page_id()) {
                            (Some(prev_page_id), Some(next_page_id)) => {
                                let prev_page_buffer =
                                    self.free_list.fetch_page(prev_page_id).await?;
                                let next_page_buffer =
                                    self.free_list.fetch_page(next_page_id).await?;

                                if let (Some(mut prev_page_lock), Some(mut next_page_lock)) = (
                                    WithWriteLockGuard::try_new(prev_page_buffer),
                                    WithWriteLockGuard::try_new(next_page_buffer),
                                ) {
                                    let prev_page = node::Node::new(prev_page_lock.as_bytes_mut());
                                    let mut prev_leaf = leaf::Leaf::new(prev_page.body);
                                    prev_leaf.set_next_page_id(leaf.next_page_id());

                                    let next_page = node::Node::new(next_page_lock.as_bytes_mut());
                                    let mut next_leaf = leaf::Leaf::new(next_page.body);
                                    next_leaf.set_prev_page_id(leaf.prev_page_id());
                                    true
                                } else {
                                    false
                                }
                            }
                            (Some(prev_page_id), None) => {
                                let prev_page_buffer =
                                    self.free_list.fetch_page(prev_page_id).await?;

                                let mut prev_page_lock =
                                    WithWriteLockGuard::new(prev_page_buffer).await;
                                let prev_page = node::Node::new(prev_page_lock.as_bytes_mut());
                                let mut prev_leaf = leaf::Leaf::new(prev_page.body);
                                prev_leaf.set_next_page_id(leaf.next_page_id());
                                true
                            }
                            (None, Some(next_page_id)) => {
                                let next_page_buffer =
                                    self.free_list.fetch_page(next_page_id).await?;

                                if let Some(mut next_page_lock) =
                                    WithWriteLockGuard::try_new(next_page_buffer)
                                {
                                    let next_page = node::Node::new(next_page_lock.as_bytes_mut());
                                    let mut next_leaf = leaf::Leaf::new(next_page.body);
                                    next_leaf.set_prev_page_id(leaf.prev_page_id());
                                    true
                                } else {
                                    false
                                }
                            }
                            (None, None) => true,
                        };

                        if modified_siblings {
                            if buffer_lock.page_id() == root_page_id {
                                let mut node = node::Node::new(buffer_lock.as_bytes_mut());
                                node.initialize_as_leaf();
                                let mut leaf = leaf::Leaf::new(node.body);
                                leaf.initialize();
                            } else {
                                let leaf_page_id = buffer_lock.page_id();
                                drop(buffer_lock);
                                self.free_list.remove_page(leaf_page_id).await?;

                                loop {
                                    let (mut lock, child_idx) = shrink_stack.pop().unwrap();
                                    let lock_page_id = lock.page_id();
                                    let node = node::Node::new(lock.as_bytes_mut());
                                    let mut branch = Branch::new(node.body);

                                    branch.remove(child_idx, &self.free_list).await?;

                                    if branch.num_pairs() == 0 && branch.right_child().is_none() {
                                        if lock_page_id == root_page_id {
                                            drop(branch);
                                            let mut node = node::Node::new(lock.as_bytes_mut());
                                            node.initialize_as_leaf();
                                            let mut leaf = leaf::Leaf::new(node.body);
                                            leaf.initialize();
                                            break;
                                        } else {
                                            drop(branch);
                                            drop(lock);
                                            self.free_list.remove_page(lock_page_id).await?;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                    break;
                }
                node::Body::Branch(branch) => {
                    let child_idx = branch.search_child_idx(key, self.free_list).await?;
                    let child_page_id = branch.child_at(child_idx, self.free_list).await?;
                    let child_node_buffer =
                        WithWriteLockGuard::new(self.free_list.fetch_page(child_page_id).await?)
                            .await;

                    if branch.num_pairs() + branch.right_child().map(|_| 1).unwrap_or(0) > 1 {
                        shrink_stack.clear();
                    }

                    shrink_stack.push((buffer_lock, child_idx));
                    buffer_lock = child_node_buffer;
                }
            }
        }

        Ok(())
    }
}

pub struct Iter<'a> {
    free_list: &'a FreeList,
    read_lock: Option<WithReadLockGuard>,
    slot_id: usize,
}

impl<'a> Iter<'a> {
    async fn new(
        free_list: &'a FreeList,
        read_lock: WithReadLockGuard,
        mut slot_id: Option<usize>,
    ) -> Result<Iter<'a>, buffer::Error> {
        let mut read_lock = Some(read_lock);
        loop {
            let lock = read_lock.as_ref().unwrap();
            let leaf_node = node::Node::new(lock.as_bytes());
            let leaf = leaf::Leaf::new(leaf_node.body);

            if leaf.num_pairs() == 0 || slot_id.is_none() {
                if let Some(prev_page_id) = leaf.prev_page_id() {
                    let prev_read_lock =
                        WithReadLockGuard::new(free_list.fetch_page(prev_page_id).await?).await;
                    let prev_leaf_node = node::Node::new(prev_read_lock.as_bytes());
                    let prev_leaf = leaf::Leaf::new(prev_leaf_node.body);

                    if prev_leaf.num_pairs() > 0 {
                        slot_id = Some(prev_leaf.num_pairs() - 1);
                        read_lock = Some(prev_read_lock);
                        break;
                    }
                    read_lock = Some(prev_read_lock);
                    continue;
                } else {
                    read_lock = None;
                }
            }
            break;
        }

        Ok(Iter {
            free_list,
            read_lock,
            slot_id: slot_id.unwrap_or(0),
        })
    }

    async fn get(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, buffer::Error> {
        if let Some(read_lock) = self.read_lock.as_ref() {
            let leaf_node = node::Node::new(read_lock.as_bytes());
            let leaf = leaf::Leaf::new(leaf_node.body);

            let data = leaf.data_at(self.slot_id, &self.free_list).await?;
            let pair = Pair::from_bytes(&data);
            Ok(Some((pair.key.to_vec(), pair.value.to_vec())))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::type_complexity)]
    pub async fn prev(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, buffer::Error> {
        let value = self.get().await?;

        if value.is_some() {
            if self.slot_id == 0 {
                loop {
                    let read_lock = self.read_lock.as_ref().unwrap();
                    let leaf_node = node::Node::new(read_lock.as_bytes());
                    let leaf = leaf::Leaf::new(leaf_node.body);

                    if let Some(prev_page_id) = leaf.prev_page_id() {
                        let prev_read_lock =
                            WithReadLockGuard::new(self.free_list.fetch_page(prev_page_id).await?)
                                .await;

                        let prev_leaf_node = node::Node::new(prev_read_lock.as_bytes());
                        let prev_leaf = leaf::Leaf::new(prev_leaf_node.body);

                        if prev_leaf.num_pairs() > 0 {
                            self.slot_id = prev_leaf.num_pairs() - 1;
                            self.read_lock = Some(prev_read_lock);
                            break;
                        } else {
                            self.read_lock = Some(prev_read_lock);
                        }
                    } else {
                        self.read_lock = None;
                        break;
                    }
                }
            } else {
                self.slot_id -= 1;
            }
        }

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use tempfile::NamedTempFile;

    use crate::buffer::BufferPoolManager;
    use crate::disk::DiskManager;

    use super::*;
    #[tokio::test]
    async fn test_simple() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();
        btree.insert(&6u64.to_be_bytes(), b"world").await.unwrap();
        btree.insert(&3u64.to_be_bytes(), b"hello").await.unwrap();
        btree.insert(&8u64.to_be_bytes(), b"!").await.unwrap();
        btree.insert(&5u64.to_be_bytes(), b",").await.unwrap();

        let (_, value) = btree
            .search(SearchMode::Key(3u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"hello", &value[..]);
        let (_, value) = btree
            .search(SearchMode::Key(8u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"!", &value[..]);

        let (_, value) = btree
            .search(SearchMode::Key(4u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"hello", &value[..]);
    }

    #[tokio::test]
    async fn test_split() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();
        let long_padding = vec![0xDEu8; 1500];
        btree
            .insert(&6u64.to_be_bytes(), &long_padding)
            .await
            .unwrap();
        btree
            .insert(&3u64.to_be_bytes(), &long_padding)
            .await
            .unwrap();
        btree
            .insert(&8u64.to_be_bytes(), &long_padding)
            .await
            .unwrap();
        btree
            .insert(&4u64.to_be_bytes(), &long_padding)
            .await
            .unwrap();
        btree.insert(&5u64.to_be_bytes(), b"hello").await.unwrap();

        let (_, value) = btree
            .search(SearchMode::Key(5u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"hello", &value[..]);
    }

    #[tokio::test]
    async fn test_remove() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();
        btree.insert(&6u64.to_be_bytes(), b"world").await.unwrap();
        btree.insert(&3u64.to_be_bytes(), b"hello").await.unwrap();
        btree.insert(&8u64.to_be_bytes(), b"!").await.unwrap();
        btree.insert(&4u64.to_be_bytes(), b",").await.unwrap();

        btree.remove(&6u64.to_be_bytes()).await.unwrap();
        btree.remove(&4u64.to_be_bytes()).await.unwrap();

        let (_, value) = btree
            .search(SearchMode::Key(3u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"hello", &value[..]);
        let (_, value) = btree
            .search(SearchMode::Key(8u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"!", &value[..]);
    }

    #[tokio::test]
    async fn test_random() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 256);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();
        let mut memory: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();

        let mut rng = StdRng::from_seed([0xDE; 32]);

        for _ in 0..512 {
            let p: f32 = rng.gen();

            match p {
                p if p < 0.6 => {
                    let key = loop {
                        let mut key = vec![0; rng.gen_range(0..4096)];
                        rng.fill(key.as_mut_slice());
                        if !memory.contains_key(&key) {
                            break key;
                        }
                    };
                    let value = {
                        let mut value = vec![0; rng.gen_range(0..4096)];
                        rng.fill(value.as_mut_slice());
                        value
                    };
                    btree.insert(&key, &value).await.unwrap();
                    assert!(memory.insert(key, value).is_none());
                }
                _ => {
                    if let Some(key) = memory.keys().choose(&mut rng).cloned() {
                        memory.remove(&key).unwrap();
                        btree.remove(key.as_slice()).await.unwrap();
                    }
                }
            }

            let mut iter = btree.search(SearchMode::Last).await.unwrap();
            let mut snapshot: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();

            while let Some((key, value)) = iter.prev().await.unwrap() {
                snapshot.insert(key, value);
            }
            assert_eq!(snapshot.len(), memory.len());
            assert_eq!(snapshot, memory);
            for (k, v) in memory.iter() {
                let (bk, bv) = btree
                    .search(SearchMode::Key(k.clone()))
                    .await
                    .unwrap()
                    .get()
                    .await
                    .unwrap()
                    .unwrap();

                assert_eq!(&bk, k);
                assert_eq!(&bv, v);
            }
        }
    }

    #[tokio::test]
    async fn test_insert_height() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 64);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();
        let mut keys: std::collections::BTreeSet<Vec<u8>> = Default::default();

        let mut rng = StdRng::from_seed([0xDE; 32]);

        for _ in 0..256 {
            let key = loop {
                let mut key = vec![0; 512];
                rng.fill(key.as_mut_slice());
                if !keys.contains(&key) {
                    break key;
                }
            };
            let value = {
                let mut value = vec![0; 512];
                rng.fill(value.as_mut_slice());
                value
            };
            btree.insert(&key, &value).await.unwrap();
            assert!(keys.insert(key));

            let mut heights = Vec::new();

            for k in keys.iter() {
                let (_, height) = btree
                    .search_height(SearchMode::Key(k.clone()))
                    .await
                    .unwrap();
                heights.push(height);
            }

            let min_height = heights.iter().min().cloned();
            let max_height = heights.iter().max().cloned();

            assert_eq!(min_height, max_height);
        }
    }

    #[tokio::test]
    async fn test_insert_remove_height() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 64);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();
        let mut keys: std::collections::BTreeSet<Vec<u8>> = Default::default();

        let mut rng = StdRng::from_seed([0xAF; 32]);

        for _ in 0..256 {
            let p: f32 = rng.gen();
            match p {
                p if p < 0.66 => {
                    let key = loop {
                        let mut key = vec![0; 512];
                        rng.fill(key.as_mut_slice());
                        if !keys.contains(&key) {
                            break key;
                        }
                    };
                    let value = {
                        let mut value = vec![0; 512];
                        rng.fill(value.as_mut_slice());
                        value
                    };
                    btree.insert(&key, &value).await.unwrap();
                    assert!(keys.insert(key));
                }
                _ => {
                    if let Some(k) = keys.iter().choose(&mut rng).cloned() {
                        btree.remove(&k).await.unwrap();
                        assert!(keys.remove(&k));
                    }
                }
            }

            let mut heights = Vec::new();

            for k in keys.iter() {
                let (_, height) = btree
                    .search_height(SearchMode::Key(k.clone()))
                    .await
                    .unwrap();
                heights.push(height);
            }

            let min_height = heights.iter().min().cloned();
            let max_height = heights.iter().max().cloned();

            assert_eq!(min_height, max_height);
        }
    }
}
