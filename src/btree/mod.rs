use std::{convert::identity, sync::Arc};

use async_recursion::async_recursion;
use bincode::Options;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{AsBytes, ByteSlice};

use crate::buffer::{self, Buffer, BufferPoolManager};
use crate::disk::PageId;
use crate::freelist::FreeList;

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
        bincode::options().serialize(self).unwrap()
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        bincode::options().deserialize(bytes).unwrap()
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
    Start,
    Key(Vec<u8>),
}

impl SearchMode {
    fn child_page_id(&self, branch: &branch::Branch<impl ByteSlice>) -> PageId {
        match self {
            SearchMode::Start => branch.child_at(0),
            SearchMode::Key(key) => branch.search_child(key),
        }
    }

    async fn tuple_slot_id(
        &self,
        leaf: &leaf::Leaf<impl ByteSlice>,
        free_list: &FreeList,
    ) -> Result<Result<usize, usize>, buffer::Error> {
        match self {
            SearchMode::Start => Ok(Err(0)),
            SearchMode::Key(key) => leaf.search_slot_id(key, free_list).await,
        }
    }
}

pub struct BTree {
    free_list: FreeList,
}

impl BTree {
    pub async fn create(buffer_pool_manager: BufferPoolManager) -> Result<Self, buffer::Error> {
        let free_list = FreeList::create(buffer_pool_manager).await?;
        let root_buffer = free_list.new_page().await?;
        let mut root_buffer_lock = root_buffer.page.write().await;
        let mut root = node::Node::new(root_buffer_lock.as_bytes_mut());
        root.initialize_as_leaf();
        let mut leaf = leaf::Leaf::new(root.body);
        leaf.initialize();
        let meta_buffer = free_list.fetch_meta_page().await?;
        let mut meta_buffer_lock = meta_buffer.page.write().await;
        let mut meta = meta::Meta::new(FreeList::other_meta(meta_buffer_lock.as_bytes_mut()));
        meta.header.root_page_id = root_buffer.page_id;
        Ok(Self { free_list })
    }

    pub fn new(meta_page_id: PageId, buffer_pool_manager: BufferPoolManager) -> Self {
        Self {
            free_list: FreeList::new(meta_page_id, buffer_pool_manager),
        }
    }

    async fn fetch_root_page(&self) -> Result<Arc<Buffer>, buffer::Error> {
        let root_page_id = {
            let meta_buffer = self.free_list.fetch_meta_page().await?;
            let meta_buffer_lock = meta_buffer.page.read().await;
            let meta = meta::Meta::new(FreeList::other_meta(meta_buffer_lock.as_bytes()));
            meta.header.root_page_id
        };
        Ok(self.free_list.fetch_page(root_page_id).await?)
    }

    #[async_recursion]
    async fn search_internal(
        &self,
        node_buffer: Arc<Buffer>,
        search_mode: SearchMode,
    ) -> Result<Iter<'_>, buffer::Error> {
        let node_buffer_lock = node_buffer.page.read().await;
        let node = node::Node::new(node_buffer_lock.as_bytes());
        match node::Body::new(node.header.node_type, node.body.as_bytes()) {
            node::Body::Leaf(leaf) => {
                let slot_id = search_mode
                    .tuple_slot_id(&leaf, &self.free_list)
                    .await?
                    .unwrap_or_else(identity);
                drop(node);
                drop(node_buffer_lock);
                Ok(Iter {
                    free_list: &self.free_list,
                    buffer: node_buffer,
                    slot_id,
                })
            }
            node::Body::Branch(branch) => {
                let child_page_id = search_mode.child_page_id(&branch);
                drop(node);
                drop(node_buffer_lock);
                drop(node_buffer);
                let child_node_page = self.free_list.fetch_page(child_page_id).await?;
                self.search_internal(child_node_page, search_mode).await
            }
        }
    }

    pub async fn search(&self, search_mode: SearchMode) -> Result<Iter<'_>, buffer::Error> {
        let root_page = self.fetch_root_page().await?;
        self.search_internal(root_page, search_mode).await
    }

    #[cfg(test)]
    #[async_recursion]
    async fn search_internal_height(
        &self,
        node_buffer: Arc<Buffer>,
        search_mode: SearchMode,
    ) -> Result<(Iter<'_>, usize), buffer::Error> {
        let node_buffer_lock = node_buffer.page.read().await;
        let node = node::Node::new(node_buffer_lock.as_bytes());
        match node::Body::new(node.header.node_type, node.body.as_bytes()) {
            node::Body::Leaf(leaf) => {
                let slot_id = search_mode
                    .tuple_slot_id(&leaf, &self.free_list)
                    .await?
                    .unwrap_or_else(identity);
                drop(node);
                drop(node_buffer_lock);
                Ok((
                    Iter {
                        free_list: &self.free_list,
                        buffer: node_buffer,
                        slot_id,
                    },
                    0,
                ))
            }
            node::Body::Branch(branch) => {
                let child_page_id = search_mode.child_page_id(&branch);
                drop(node);
                drop(node_buffer_lock);
                drop(node_buffer);
                let child_node_page = self.free_list.fetch_page(child_page_id).await?;
                let (iter, height) = self
                    .search_internal_height(child_node_page, search_mode)
                    .await?;
                Ok((iter, height + 1))
            }
        }
    }

    #[cfg(test)]
    pub async fn search_height(
        &self,
        search_mode: SearchMode,
    ) -> Result<(Iter<'_>, usize), buffer::Error> {
        let root_page = self.fetch_root_page().await?;
        self.search_internal_height(root_page, search_mode).await
    }

    #[async_recursion]
    async fn insert_internal(
        &self,
        buffer: Arc<Buffer>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, PageId)>, InsertError> {
        let mut buffer_lock = buffer.page.write().await;
        let node = node::Node::new(buffer_lock.as_bytes_mut());
        match node::Body::new(node.header.node_type, node.body) {
            node::Body::Leaf(mut leaf) => {
                let slot_id = match leaf.search_slot_id(key, &self.free_list).await? {
                    Ok(_) => return Err(InsertError::DuplicateKey),
                    Err(slot_id) => slot_id,
                };
                if leaf
                    .insert(slot_id, key, value, &self.free_list)
                    .await?
                    .is_some()
                {
                    Ok(None)
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
                    let mut new_leaf_node = node::Node::new(new_leaf_buffer_lock.as_bytes_mut());
                    new_leaf_node.initialize_as_leaf();
                    let mut new_leaf = leaf::Leaf::new(new_leaf_node.body);
                    new_leaf.initialize();
                    let overflow_key = leaf
                        .split_insert(&mut new_leaf, key, value, &self.free_list)
                        .await?;
                    new_leaf.set_next_page_id(Some(buffer.page_id));
                    new_leaf.set_prev_page_id(prev_leaf_page_id);
                    Ok(Some((overflow_key, new_leaf_buffer.page_id)))
                }
            }
            node::Body::Branch(mut branch) => {
                let child_idx = branch.search_child_idx(key);
                let child_page_id = branch.child_at(child_idx);
                let child_node_buffer = self.free_list.fetch_page(child_page_id).await?;
                if let Some((overflow_key_from_child, overflow_child_page_id)) =
                    self.insert_internal(child_node_buffer, key, value).await?
                {
                    if branch
                        .insert(child_idx, &overflow_key_from_child, overflow_child_page_id)
                        .is_some()
                    {
                        Ok(None)
                    } else {
                        let new_branch_buffer = self.free_list.new_page().await?;
                        let mut new_branch_buffer_lock = new_branch_buffer.page.write().await;
                        let mut new_branch_node =
                            node::Node::new(new_branch_buffer_lock.as_bytes_mut());
                        new_branch_node.initialize_as_branch();
                        let mut new_branch = branch::Branch::new(new_branch_node.body);
                        let overflow_key = branch.split_insert(
                            &mut new_branch,
                            &overflow_key_from_child,
                            overflow_child_page_id,
                        );
                        Ok(Some((overflow_key, new_branch_buffer.page_id)))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), InsertError> {
        let root_buffer = self.fetch_root_page().await?;
        let root_page_id = root_buffer.page_id;
        if let Some((key, child_page_id)) = self.insert_internal(root_buffer, key, value).await? {
            let new_root_buffer = self.free_list.new_page().await?;
            let mut new_root_buffer_lock = new_root_buffer.page.write().await;
            let mut node = node::Node::new(new_root_buffer_lock.as_bytes_mut());
            node.initialize_as_branch();
            let mut branch = branch::Branch::new(node.body);
            branch.initialize(&key, child_page_id, root_page_id);
            let meta_buffer = self.free_list.fetch_meta_page().await?;
            let mut meta_buffer_lock = meta_buffer.page.write().await;
            let mut meta = meta::Meta::new(FreeList::other_meta(meta_buffer_lock.as_bytes_mut()));
            meta.header.root_page_id = new_root_buffer.page_id;
        }
        Ok(())
    }

    #[async_recursion]
    async fn remove_internal(&self, buffer: Arc<Buffer>, key: &[u8]) -> Result<bool, RemoveError> {
        let mut buffer_lock = buffer.page.write().await;
        let node = node::Node::new(buffer_lock.as_bytes_mut());
        match node::Body::new(node.header.node_type, node.body) {
            node::Body::Leaf(mut leaf) => {
                let slot_id = match leaf.search_slot_id(key, &self.free_list).await? {
                    Ok(slot_id) => slot_id,
                    Err(_) => return Err(RemoveError::KeyNotFound),
                };
                leaf.remove(slot_id, &self.free_list).await?;
                if leaf.num_pairs() == 0 {
                    if let Some(prev_page_id) = leaf.prev_page_id() {
                        let prev_page_buffer = self.free_list.fetch_page(prev_page_id).await?;
                        let mut prev_page_lock = prev_page_buffer.page.write().await;
                        let prev_page = node::Node::new(prev_page_lock.as_bytes_mut());
                        let mut prev_leaf = leaf::Leaf::new(prev_page.body);
                        prev_leaf.set_next_page_id(leaf.next_page_id());
                    }
                    if let Some(next_page_id) = leaf.next_page_id() {
                        let next_page_buffer = self.free_list.fetch_page(next_page_id).await?;
                        let mut next_page_lock = next_page_buffer.page.write().await;
                        let next_page = node::Node::new(next_page_lock.as_bytes_mut());
                        let mut next_leaf = leaf::Leaf::new(next_page.body);
                        next_leaf.set_prev_page_id(leaf.prev_page_id());
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            node::Body::Branch(mut branch) => {
                let child_idx = branch.search_child_idx(key);
                let child_page_id = branch.child_at(child_idx);
                let child_node_buffer = self.free_list.fetch_page(child_page_id).await?;
                if self.remove_internal(child_node_buffer, key).await? {
                    self.free_list.remove_page(child_page_id).await?;
                    branch.remove(child_idx);
                    Ok(branch.num_pairs() == 0 && branch.right_child().is_none())
                } else {
                    Ok(false)
                }
            }
        }
    }

    pub async fn remove(&self, key: &[u8]) -> Result<(), RemoveError> {
        let root_page = self.fetch_root_page().await?;
        if self.remove_internal(root_page.clone(), key).await? {
            let mut root_buffer_lock = root_page.page.write().await;
            let mut root = node::Node::new(root_buffer_lock.as_bytes_mut());
            root.initialize_as_leaf();
            let mut leaf = leaf::Leaf::new(root.body);
            leaf.initialize();
        }

        Ok(())
    }
}

pub struct Iter<'a> {
    free_list: &'a FreeList,
    buffer: Arc<Buffer>,
    slot_id: usize,
}

impl<'a> Iter<'a> {
    #[async_recursion]
    async fn get(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, buffer::Error> {
        let buffer = self.buffer.clone();
        let buffer_lock = buffer.page.read().await;
        let leaf_node = node::Node::new(buffer_lock.as_bytes());
        let leaf = leaf::Leaf::new(leaf_node.body);
        if self.slot_id < leaf.num_pairs() {
            let data = leaf.data_at(self.slot_id, &self.free_list).await?;
            let pair = Pair::from_bytes(&data);
            Ok(Some((pair.key.to_vec(), pair.value.to_vec())))
        } else {
            if let Some(page_id) = leaf.next_page_id() {
                let buffer = self.free_list.fetch_page(page_id).await?;
                self.buffer = buffer;
                self.slot_id = 0;
                self.get().await
            } else {
                Ok(None)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, buffer::Error> {
        let value = self.get().await?;
        self.slot_id += 1;
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
        let btree = BTree::create(buffer_pool_manager).await.unwrap();
        btree.insert(&6u64.to_be_bytes(), b"world").await.unwrap();
        btree.insert(&3u64.to_be_bytes(), b"hello").await.unwrap();
        btree.insert(&8u64.to_be_bytes(), b"!").await.unwrap();
        btree.insert(&4u64.to_be_bytes(), b",").await.unwrap();

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
    async fn test_split() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let btree = BTree::create(buffer_pool_manager).await.unwrap();
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
        let btree = BTree::create(buffer_pool_manager).await.unwrap();
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
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let btree = BTree::create(buffer_pool_manager).await.unwrap();
        let mut memory: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();

        let mut rng = StdRng::from_seed([0xDE; 32]);

        for _ in 0..512 {
            let p: f32 = rng.gen();

            match p {
                p if p < 0.6 => {
                    let key = loop {
                        let mut key = vec![0; rng.gen_range(0..512)];
                        rng.fill(key.as_mut_slice());
                        if !memory.contains_key(&key) {
                            break key;
                        }
                    };
                    let value = {
                        let mut value = vec![0; rng.gen_range(0..512)];
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

            let mut iter = btree.search(SearchMode::Start).await.unwrap();
            let mut snapshot: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();

            while let Some((key, value)) = iter.next().await.unwrap() {
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
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let btree = BTree::create(buffer_pool_manager).await.unwrap();
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
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let btree = BTree::create(buffer_pool_manager).await.unwrap();
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
