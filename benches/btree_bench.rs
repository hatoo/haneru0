use criterion::{criterion_group, criterion_main, Criterion};
use haneru::btree::BTree;
use haneru::buffer::BufferPoolManager;
use haneru::disk::DiskManager;

use rand::prelude::*;
use tempfile::NamedTempFile;

use std::collections::BTreeSet;

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("btree_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let path = NamedTempFile::new().unwrap().into_temp_path();

            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, 256);
            let btree = BTree::create(buffer_pool_manager).await.unwrap();
            let mut rng = StdRng::from_seed([0xDE; 32]);

            for _ in 0..512 {
                let key = {
                    let mut key = vec![0; rng.gen_range(0..512)];
                    rng.fill_bytes(key.as_mut_slice());
                    key
                };
                let value = {
                    let mut value = vec![0; rng.gen_range(0..512)];
                    rng.fill_bytes(value.as_mut_slice());
                    value
                };

                let _ = btree.insert(&key, &value).await;
            }
        })
    });

    c.bench_function("btree_random", |b| {
        b.to_async(&rt).iter(|| async {
            let path = NamedTempFile::new().unwrap().into_temp_path();

            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, 256);
            let btree = BTree::create(buffer_pool_manager).await.unwrap();
            let mut rng = StdRng::from_seed([0xDE; 32]);
            let mut keys: BTreeSet<Vec<u8>> = Default::default();

            for _ in 0..512 {
                let p: f32 = rng.gen();
                match p {
                    p if p < 0.60 => {
                        // insert
                        let key = {
                            let mut key = vec![0; rng.gen_range(0..512)];
                            rng.fill_bytes(key.as_mut_slice());
                            key
                        };
                        let value = {
                            let mut value = vec![0; rng.gen_range(0..512)];
                            rng.fill_bytes(value.as_mut_slice());
                            value
                        };

                        let _ = btree.insert(&key, &value).await;
                        keys.insert(key);
                    }
                    _ => {
                        if let Some(key) = keys.iter().choose(&mut rng).cloned() {
                            btree.remove(&key).await.unwrap();
                            keys.remove(&key);
                        }
                    }
                }
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
