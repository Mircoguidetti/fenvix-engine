//! Does the local object store (MinIO) honour conditional writes under concurrency? Raw `object_store` calls, no
//! journal3z code: N clients race to (a) create the same new object (`If-None-Match: *`), (b) update the same object
//! from the same version (`If-Match`). Exactly one must win each round.
use j3z_bench::*;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::sync::Arc;

async fn race(store: Arc<dyn ObjectStore>, rounds: u64, n: usize, update: bool, read_first: bool) -> Vec<(u64, usize)> {
    let mut bad = vec![];
    for r in 0..rounds {
        let p = Path::from(format!("race-{update}-{n}-{read_first}/{r}"));
        let etag = if update { store.put(&p, PutPayload::from("v0")).await.unwrap().e_tag } else { None };
        let hs: Vec<_> = (0..n)
            .map(|i| {
                let (store, p, etag) = (store.clone(), p.clone(), etag.clone());
                tokio::spawn(async move {
                    if read_first {
                        let _ = store.get(&p).await.map(|_| ());
                    }
                    let mode = match etag {
                        None => PutMode::Create,
                        Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e), version: None }),
                    };
                    store.put_opts(&p, PutPayload::from(format!("w{i}")), PutOptions { mode, ..Default::default() }).await.is_ok()
                })
            })
            .collect();
        let mut w = 0;
        for h in hs {
            w += h.await.unwrap() as usize;
        }
        if w != 1 {
            bad.push((r, w));
        }
    }
    bad
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minio_conditional_writes_under_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    let minio = Minio::start(&dir.path().join("minio"));
    minio.ready().await;
    let store = minio.meta().store.clone();
    let mut total_bad = 0;
    for read_first in [false, true] {
        for (update, n) in [(false, 3), (true, 3), (false, 8), (true, 8)] {
            let bad = race(store.clone(), 300, n, update, read_first).await;
            total_bad += bad.len();
            eprintln!("[minio_cas] {} with {n} clients{}: {} of 300 rounds without exactly one winner {:?}", if update { "update (If-Match)" } else { "create (If-None-Match)" }, if read_first { ", each reading first" } else { "" }, bad.len(), &bad[..bad.len().min(8)]);
        }
    }
    assert_eq!(total_bad, 0, "the local object store does not honour conditional writes under concurrency");
}
