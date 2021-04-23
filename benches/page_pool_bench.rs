use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("pool_manager", |b| {
        b.to_async(&rt).iter(|| async {
            use haneru::buffer::BufferPoolManager;
            use haneru::disk::DiskManager;
            use rand::prelude::*;
            use std::ops::DerefMut;
            use std::sync::Arc;
            use tempfile::NamedTempFile;

            let path = NamedTempFile::new().unwrap().into_temp_path();

            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, 4);

            let mut pages = Vec::new();
            for _ in 0usize..16 {
                pages.push(buffer_pool_manager.create_page().await.unwrap().page_id);
            }
            let buffer_pool_manager = Arc::new(buffer_pool_manager);
            let v = (0..4)
                .map(|_| {
                    let buffer_pool_manager = buffer_pool_manager.clone();
                    let mut pages = pages.clone();
                    tokio::spawn(async move {
                        let mut rng = rand::rngs::StdRng::from_entropy();
                        for _ in 0usize..4 {
                            pages.shuffle(&mut rng);
                            for &page_id in &pages {
                                for _ in 0..2 {
                                    let buffer =
                                        buffer_pool_manager.fetch_page(page_id).await.unwrap();
                                    rng.fill_bytes(
                                        buffer
                                            .page
                                            .write()
                                            .await
                                            .deref_mut()
                                            .deref_mut()
                                            .deref_mut(),
                                    );
                                }
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            for f in v {
                f.await.unwrap();
            }
        })
    });
    c.bench_function("pool_manager_write_create", |b| {
        b.to_async(&rt).iter(|| async {
            use haneru::buffer::BufferPoolManager;
            use haneru::disk::DiskManager;
            use rand::prelude::*;
            use std::ops::DerefMut;
            use std::sync::Arc;
            use tempfile::NamedTempFile;

            let path = NamedTempFile::new().unwrap().into_temp_path();

            let disk_manager = DiskManager::open(&path).unwrap();
            let buffer_pool_manager = BufferPoolManager::new(disk_manager, 4);

            let buffer_pool_manager = Arc::new(buffer_pool_manager);
            let v = (0..4)
                .map(|_| {
                    let buffer_pool_manager = buffer_pool_manager.clone();
                    tokio::spawn(async move {
                        let mut rng = rand::rngs::StdRng::from_entropy();
                        let mut pages = Vec::new();
                        for _ in 0usize..8 {
                            let page_id = buffer_pool_manager.create_page().await.unwrap().page_id;
                            pages.push(page_id);
                            pages.shuffle(&mut rng);
                            for &page_id in &pages {
                                let buffer = buffer_pool_manager.fetch_page(page_id).await.unwrap();
                                rng.fill_bytes(
                                    buffer
                                        .page
                                        .write()
                                        .await
                                        .deref_mut()
                                        .deref_mut()
                                        .deref_mut(),
                                );
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            for f in v {
                f.await.unwrap();
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
