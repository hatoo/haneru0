use crate::btree;
use crate::btree::BTree;
use crate::tuple;

#[derive(Debug)]
pub struct SimpleTable<'a> {
    pub num_key_elems: usize,
    pub btree: BTree<'a>,
}

impl<'a> SimpleTable<'a> {
    pub async fn insert(&self, record: &[&[u8]]) -> Result<(), btree::InsertError> {
        let mut key = vec![];
        tuple::encode(record[..self.num_key_elems].iter(), &mut key);
        let mut value = vec![];
        tuple::encode(record[self.num_key_elems..].iter(), &mut value);
        self.btree.insert(&key, &value).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Table<'a> {
    pub btree: BTree<'a>,
    pub num_key_elems: usize,
    pub unique_indices: Vec<UniqueIndex<'a>>,
}

impl<'a> Table<'a> {
    pub async fn insert(&self, record: &[&[u8]]) -> Result<(), btree::InsertError> {
        let mut key = vec![];
        tuple::encode(record[..self.num_key_elems].iter(), &mut key);
        let mut value = vec![];
        tuple::encode(record[self.num_key_elems..].iter(), &mut value);
        self.btree.insert(&key, &value).await?;
        for unique_index in &self.unique_indices {
            unique_index.insert(&key, record).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct UniqueIndex<'a> {
    pub btree: BTree<'a>,
    pub skey: Vec<usize>,
}

impl<'a> UniqueIndex<'a> {
    pub async fn insert(
        &self,
        pkey: &[u8],
        record: &[impl AsRef<[u8]>],
    ) -> Result<(), btree::InsertError> {
        let mut skey = vec![];
        tuple::encode(
            self.skey.iter().map(|&index| record[index].as_ref()),
            &mut skey,
        );
        self.btree.insert(&skey, pkey).await?;
        Ok(())
    }
}
