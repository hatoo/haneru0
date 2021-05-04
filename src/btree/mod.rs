use std::{convert::identity, sync::Arc};

use async_recursion::async_recursion;
use bincode::Options;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{AsBytes, ByteSlice};

use crate::buffer::{self, Buffer, BufferPoolManager};
use crate::disk::PageId;

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
pub enum Error {
    #[error("duplicate key")]
    DuplicateKey,
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

    fn tuple_slot_id(&self, leaf: &leaf::Leaf<impl ByteSlice>) -> Result<usize, usize> {
        match self {
            SearchMode::Start => Err(0),
            SearchMode::Key(key) => leaf.search_slot_id(key),
        }
    }
}

pub struct BTree {
    pub meta_page_id: PageId,
    buffer_pool_manager: BufferPoolManager,
}

impl BTree {
    pub async fn create(buffer_pool_manager: BufferPoolManager) -> Result<Self, Error> {
        let meta_buffer = buffer_pool_manager.create_page().await?;
        let mut meta_buffer_lock = meta_buffer.page.write().await;
        let mut meta = meta::Meta::new(meta_buffer_lock.as_bytes_mut());
        let root_buffer = buffer_pool_manager.create_page().await?;
        let mut root_buffer_lock = root_buffer.page.write().await;
        let mut root = node::Node::new(root_buffer_lock.as_bytes_mut());
        root.initialize_as_leaf();
        let mut leaf = leaf::Leaf::new(root.body);
        leaf.initialize();
        meta.header.root_page_id = root_buffer.page_id;
        Ok(Self::new(meta_buffer.page_id, buffer_pool_manager))
    }

    pub fn new(meta_page_id: PageId, buffer_pool_manager: BufferPoolManager) -> Self {
        Self {
            meta_page_id,
            buffer_pool_manager,
        }
    }

    async fn fetch_root_page(&self) -> Result<Arc<Buffer>, Error> {
        let root_page_id = {
            let meta_buffer = self
                .buffer_pool_manager
                .fetch_page(self.meta_page_id)
                .await?;
            let meta_buffer_lock = meta_buffer.page.read().await;
            let meta = meta::Meta::new(meta_buffer_lock.as_bytes());
            meta.header.root_page_id
        };
        Ok(self.buffer_pool_manager.fetch_page(root_page_id).await?)
    }

    #[async_recursion]
    async fn search_internal(
        &self,
        node_buffer: Arc<Buffer>,
        search_mode: SearchMode,
    ) -> Result<Iter<'_>, Error> {
        let node_buffer_lock = node_buffer.page.read().await;
        let node = node::Node::new(node_buffer_lock.as_bytes());
        match node::Body::new(node.header.node_type, node.body.as_bytes()) {
            node::Body::Leaf(leaf) => {
                let slot_id = search_mode.tuple_slot_id(&leaf).unwrap_or_else(identity);
                drop(node);
                drop(node_buffer_lock);
                Ok(Iter {
                    buffer_pool_manager: &self.buffer_pool_manager,
                    buffer: node_buffer,
                    slot_id,
                })
            }
            node::Body::Branch(branch) => {
                let child_page_id = search_mode.child_page_id(&branch);
                drop(node);
                drop(node_buffer_lock);
                drop(node_buffer);
                let child_node_page = self.buffer_pool_manager.fetch_page(child_page_id).await?;
                self.search_internal(child_node_page, search_mode).await
            }
        }
    }

    pub async fn search(&self, search_mode: SearchMode) -> Result<Iter<'_>, Error> {
        let root_page = self.fetch_root_page().await?;
        self.search_internal(root_page, search_mode).await
    }

    #[async_recursion]
    async fn insert_internal(
        &self,
        buffer: Arc<Buffer>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, PageId)>, Error> {
        let mut buffer_lock = buffer.page.write().await;
        let node = node::Node::new(buffer_lock.as_bytes_mut());
        match node::Body::new(node.header.node_type, node.body) {
            node::Body::Leaf(mut leaf) => {
                let slot_id = match leaf.search_slot_id(key) {
                    Ok(_) => return Err(Error::DuplicateKey),
                    Err(slot_id) => slot_id,
                };
                if leaf.insert(slot_id, key, value).is_some() {
                    Ok(None)
                } else {
                    let prev_leaf_page_id = leaf.prev_page_id();
                    let prev_leaf_buffer = if let Some(next_leaf_page_id) = prev_leaf_page_id {
                        Some(self.buffer_pool_manager.fetch_page(next_leaf_page_id).await)
                    } else {
                        None
                    }
                    .transpose()?;

                    let new_leaf_buffer = self.buffer_pool_manager.create_page().await?;

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
                    let overflow_key = leaf.split_insert(&mut new_leaf, key, value);
                    new_leaf.set_next_page_id(Some(buffer.page_id));
                    new_leaf.set_prev_page_id(prev_leaf_page_id);
                    Ok(Some((overflow_key, new_leaf_buffer.page_id)))
                }
            }
            node::Body::Branch(mut branch) => {
                let child_idx = branch.search_child_idx(key);
                let child_page_id = branch.child_at(child_idx);
                let child_node_buffer = self.buffer_pool_manager.fetch_page(child_page_id).await?;
                if let Some((overflow_key_from_child, overflow_child_page_id)) =
                    self.insert_internal(child_node_buffer, key, value).await?
                {
                    if branch
                        .insert(child_idx, &overflow_key_from_child, overflow_child_page_id)
                        .is_some()
                    {
                        Ok(None)
                    } else {
                        let new_branch_buffer = self.buffer_pool_manager.create_page().await?;
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

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let meta_buffer = self
            .buffer_pool_manager
            .fetch_page(self.meta_page_id)
            .await?;
        let mut meta_buffer_lock = meta_buffer.page.write().await;
        let mut meta = meta::Meta::new(meta_buffer_lock.as_bytes_mut());
        let root_page_id = meta.header.root_page_id;
        let root_buffer = self.buffer_pool_manager.fetch_page(root_page_id).await?;
        if let Some((key, child_page_id)) = self.insert_internal(root_buffer, key, value).await? {
            let new_root_buffer = self.buffer_pool_manager.create_page().await?;
            let mut new_root_buffer_lock = new_root_buffer.page.write().await;
            let mut node = node::Node::new(new_root_buffer_lock.as_bytes_mut());
            node.initialize_as_branch();
            let mut branch = branch::Branch::new(node.body);
            branch.initialize(&key, child_page_id, root_page_id);
            meta.header.root_page_id = new_root_buffer.page_id;
        }
        Ok(())
    }
}

pub struct Iter<'a> {
    buffer_pool_manager: &'a BufferPoolManager,
    buffer: Arc<Buffer>,
    slot_id: usize,
}

impl<'a> Iter<'a> {
    async fn get(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let buffer_lock = self.buffer.page.read().await;
        let leaf_node = node::Node::new(buffer_lock.as_bytes());
        let leaf = leaf::Leaf::new(leaf_node.body);
        if self.slot_id < leaf.num_pairs() {
            let pair = leaf.pair_at(self.slot_id);
            Some((pair.key.to_vec(), pair.value.to_vec()))
        } else {
            None
        }
    }

    #[allow(clippy::type_complexity)]
    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let value = self.get().await;
        self.slot_id += 1;
        let next_page_id = {
            let buffer_lock = self.buffer.page.read().await;
            let leaf_node = node::Node::new(buffer_lock.as_bytes());
            let leaf = leaf::Leaf::new(leaf_node.body);
            if self.slot_id < leaf.num_pairs() {
                return Ok(value);
            }
            leaf.next_page_id()
        };
        if let Some(next_page_id) = next_page_id {
            self.buffer = self.buffer_pool_manager.fetch_page(next_page_id).await?;
            self.slot_id = 0;
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::buffer::BufferPoolManager;
    use crate::disk::DiskManager;

    use super::*;
    #[tokio::test]
    async fn test() {
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
            .unwrap();
        assert_eq!(b"hello", &value[..]);
        let (_, value) = btree
            .search(SearchMode::Key(8u64.to_be_bytes().to_vec()))
            .await
            .unwrap()
            .get()
            .await
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
            .unwrap();
        assert_eq!(b"hello", &value[..]);
    }
}
