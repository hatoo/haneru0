use crate::tuple;
use crate::{
    btree::{self, BTree, SearchMode},
    buffer,
};
use async_trait::async_trait;

pub type Tuple = Vec<Vec<u8>>;
pub type TupleSlice<'a> = &'a [Vec<u8>];

pub enum TupleSearchMode<'a> {
    Start,
    Key(&'a [&'a [u8]]),
}

impl<'a> TupleSearchMode<'a> {
    fn encode(&self) -> SearchMode {
        match self {
            TupleSearchMode::Start => SearchMode::Start,
            TupleSearchMode::Key(tuple) => {
                let mut key = vec![];
                tuple::encode(tuple.iter(), &mut key);
                SearchMode::Key(key)
            }
        }
    }
}

#[async_trait]
pub trait Executor {
    async fn next(&mut self) -> Result<Option<Tuple>, buffer::Error>;
}

pub type BoxExecutor<'a> = Box<dyn Executor + Sync + Send + 'a>;

#[async_trait]
pub trait PlanNode: Sync {
    async fn start(&self) -> Result<BoxExecutor, buffer::Error>;
}

pub struct SeqScan<'a> {
    pub btree: BTree<'a>,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> PlanNode for SeqScan<'a> {
    async fn start(&self) -> Result<BoxExecutor, buffer::Error> {
        let table_iter = self.btree.search(self.search_mode.encode()).await?;
        Ok(Box::new(ExecSeqScan {
            table_iter,
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecSeqScan<'a> {
    table_iter: btree::Iter<'a>,
    while_cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> Executor for ExecSeqScan<'a> {
    async fn next(&mut self) -> Result<Option<Tuple>, buffer::Error> {
        let (pkey_bytes, tuple_bytes) = match self.table_iter.next().await? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut pkey = vec![];
        tuple::decode(&pkey_bytes, &mut pkey);
        if !(self.while_cond)(&pkey) {
            return Ok(None);
        }
        let mut tuple = pkey;
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct Filter<'a> {
    pub inner_plan: &'a dyn PlanNode,
    pub cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> PlanNode for Filter<'a> {
    async fn start(&self) -> Result<BoxExecutor, buffer::Error> {
        let inner_iter = self.inner_plan.start().await?;
        Ok(Box::new(ExecFilter {
            inner_iter,
            cond: self.cond,
        }))
    }
}

pub struct ExecFilter<'a> {
    inner_iter: BoxExecutor<'a>,
    cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> Executor for ExecFilter<'a> {
    async fn next(&mut self) -> Result<Option<Tuple>, buffer::Error> {
        loop {
            match self.inner_iter.next().await? {
                Some(tuple) => {
                    if (self.cond)(&tuple) {
                        return Ok(Some(tuple));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

pub struct IndexScan<'a> {
    pub table_btree: BTree<'a>,
    pub index_btree: BTree<'a>,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> PlanNode for IndexScan<'a> {
    async fn start(&self) -> Result<BoxExecutor, buffer::Error> {
        let index_iter = self.index_btree.search(self.search_mode.encode()).await?;
        Ok(Box::new(ExecIndexScan {
            table_btree: self.table_btree,
            index_iter,
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecIndexScan<'a> {
    table_btree: BTree<'a>,
    index_iter: btree::Iter<'a>,
    while_cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> Executor for ExecIndexScan<'a> {
    async fn next(&mut self) -> Result<Option<Tuple>, buffer::Error> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next().await? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut skey = vec![];
        tuple::decode(&skey_bytes, &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let mut table_iter = self.table_btree.search(SearchMode::Key(pkey_bytes)).await?;
        let (pkey_bytes, tuple_bytes) = table_iter.next().await?.unwrap();
        let mut tuple = vec![];
        tuple::decode(&pkey_bytes, &mut tuple);
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct IndexOnlyScan<'a> {
    pub btree: BTree<'a>,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> PlanNode for IndexOnlyScan<'a> {
    async fn start(&self) -> Result<BoxExecutor, buffer::Error> {
        let index_iter = self.btree.search(self.search_mode.encode()).await?;
        Ok(Box::new(ExecIndexOnlyScan {
            index_iter,
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecIndexOnlyScan<'a> {
    index_iter: btree::Iter<'a>,
    while_cond: &'a (dyn Fn(TupleSlice) -> bool + Sync + Send),
}

#[async_trait]
impl<'a> Executor for ExecIndexOnlyScan<'a> {
    async fn next(&mut self) -> Result<Option<Tuple>, buffer::Error> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next().await? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut skey = vec![];
        tuple::decode(&skey_bytes, &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let mut tuple = skey;
        tuple::decode(&pkey_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

#[cfg(test)]
mod test {
    use tempfile::NamedTempFile;

    use crate::buffer::BufferPoolManager;
    use crate::disk::DiskManager;
    use crate::freelist::FreeList;
    use crate::table::SimpleTable;

    use super::*;
    #[tokio::test]
    async fn test_seq_scan() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();
        let btree = BTree::create(&free_list).await.unwrap();

        let table = SimpleTable {
            num_key_elems: 1,
            btree,
        };

        table.insert(&[b"hello0", b"world0"]).await.unwrap();
        table.insert(&[b"hello1", b"world1"]).await.unwrap();

        let seq_scan = SeqScan {
            btree,
            search_mode: TupleSearchMode::Start,
            while_cond: &|_| true,
        };

        let mut executor = seq_scan.start().await.unwrap();

        assert_eq!(
            executor.next().await.unwrap(),
            Some(vec![b"hello0".to_vec(), b"world0".to_vec()])
        );

        assert_eq!(
            executor.next().await.unwrap(),
            Some(vec![b"hello1".to_vec(), b"world1".to_vec()])
        );

        assert_eq!(executor.next().await.unwrap(), None);
    }
}
