//! Tests of the generated-data publish (the step 9 ballast generator, `fx/build.rs` `publish_generated`): rows written
//! straight into sealed runs (the customer space, streamed) and sorted rows handed over at the end (the shared
//! keyspace), merged with SlateDB's own rows and published as one version, read back exactly by the merged reader, in
//! this process and after a cold open, with SlateDB's writes above X_d on top.

use super::build::{GroupWriter, Generated, SortedRows};
use super::tests::SERIAL;
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

const T_PIN: u64 = 200;

struct VecRows(std::vec::IntoIter<(Vec<u8>, Vec<u8>)>);
impl SortedRows for VecRows {
    fn next_row(&mut self) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self.0.next().map(|(k, v)| (k, crate::runfmt::live(&v))))
    }
}

fn opts() -> build::BuildOpts {
    build::BuildOpts { group_bytes: 8 << 10, sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2), ..Default::default() }
}

fn ckey(c: usize, i: usize) -> Vec<u8> {
    let name = format!("c{c:03}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(format!("msgs/d/{i:05}").as_bytes());
    k
}

async fn begin(t: &mut Tenant) -> u64 {
    for _ in 0..50 {
        if let Some(x) = t.fx_ballast_begin().await.unwrap() {
            return x;
        }
        tokio::time::sleep(std::time::Duration::from_millis(T_PIN / 4)).await;
    }
    panic!("no envelope marker");
}

fn as_map(v: Vec<(bytes::Bytes, bytes::Bytes)>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    v.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_rows_read_back_exactly_with_slatedb_rows_and_later_writes() {
    let _s = SERIAL.lock().await;
    ENABLED.store(true, Relaxed);
    PUBLISH_EVERY.store(0, Relaxed);
    T_PIN_MS.store(T_PIN, Relaxed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    let mut t = lc.create("fx-gen").await.unwrap();
    // the catalog, written through SlateDB as the server's commands write it
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let cat = vec![(b"meta/collections".to_vec(), b"msgs".to_vec()), (b"meta/nat/msgs".to_vec(), b"old".to_vec()), (b"zz/catalog".to_vec(), b"z".to_vec())];
    t.apply(cat.iter().map(|(k, v)| Op::Put(k.clone(), v.clone())).collect()).await.unwrap();
    model.extend(cat);
    let x0 = begin(&mut t).await;
    // the customer space, streamed into runs (several groups: 8 KiB each)
    let (st, id, epoch) = t.fx_ballast_target().unwrap();
    let mut direct = GroupWriter::new(&st, &id, x0, epoch, &opts(), 1 << 20, false);
    for c in 0..60 {
        for i in 0..40 {
            let (k, v) = (ckey(c, i), format!("doc {c} {i} {}", "x".repeat(40)).into_bytes());
            direct.add(&k, x0, &crate::runfmt::live(&v)).await.unwrap();
            model.insert(k, v);
        }
    }
    // out of order is refused
    assert!(direct.add(&ckey(0, 0), x0, &crate::runfmt::live(b"v")).await.is_err());
    let direct_last = direct.last_key().map(|k| k.to_vec());
    let groups = direct.finish().await.unwrap();
    assert!(groups.len() > 3, "the test must cut several groups ({})", groups.len());
    // the shared keyspace, sorted; meta/nat/msgs is written again (the generated value is newer and wins)
    let mut shared: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..500 {
        shared.push((format!("doc/plain/{i:05}").into_bytes(), format!("p{i}").into_bytes()));
        shared.push((format!("loc/msgs/{i:05}").into_bytes(), format!("l{i}").into_bytes()));
    }
    shared.push((b"meta/nat/msgs".to_vec(), b"new".to_vec()));
    shared.push((b"zz/generated".to_vec(), b"g".to_vec()));
    shared.sort();
    for (k, v) in &shared {
        model.insert(k.clone(), v.clone());
    }
    let gen = Generated { x0, direct: groups, direct_last, sorted: Box::new(VecRows(shared.into_iter())) };
    let rep = t.fx_publish_generated(&opts(), gen).await.unwrap();
    assert!(rep.groups > 5 && rep.x_d >= x0, "{rep:?}");
    let marker = crate::envelope::MARKER.to_vec();
    let check = |got: BTreeMap<Vec<u8>, Vec<u8>>, model: &BTreeMap<Vec<u8>, Vec<u8>>| {
        let mut got = got;
        got.remove(&marker);
        assert_eq!(&got, model);
    };
    // every key, and the whole keyspace, in this process
    for (k, v) in &model {
        assert_eq!(t.get(k).await.unwrap().as_deref(), Some(&v[..]), "{}", String::from_utf8_lossy(k));
    }
    check(as_map(t.scan_range(b"", None, false, None, 1 << 20).await.unwrap()), &model);
    // the generated rows come from our runs: a cold open (another process) reads the same
    view::clear_cache();
    let lc2 = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&store))), None);
    t.close().await.unwrap();
    let mut t2 = lc2.wake("fx-gen").await.unwrap();
    check(as_map(t2.scan_range(b"", None, false, None, 1 << 20).await.unwrap()), &model);
    // writes after the publish are SlateDB's, above X_d, and win over generated rows
    t2.apply(vec![Op::Put(ckey(5, 5), b"later".to_vec()), Op::Put(b"doc/plain/00007".to_vec(), b"later".to_vec())]).await.unwrap();
    assert_eq!(t2.get(&ckey(5, 5)).await.unwrap().as_deref(), Some(&b"later"[..]));
    assert_eq!(t2.get(b"doc/plain/00007").await.unwrap().as_deref(), Some(&b"later"[..]));
    // a database with runs is never generated into again
    let empty = Generated { x0: rep.x_d, direct: vec![], direct_last: None, sorted: Box::new(VecRows(Vec::new().into_iter())) };
    assert!(t2.fx_publish_generated(&opts(), empty).await.is_err(), "a second generation must be refused");
    t2.abandon().await;
    ENABLED.store(false, Relaxed);
}
