//! Review of compaction 2 step 4 (reviewer, 2026-11-09): what SlateDB's own reads cost when the young data sits in
//! several sorted runs and L0 SSTs (aged runs, the owner's default) instead of one sorted run (rule 12).
//!
//! The same rows (chat-like documents, 4,000 customers, Zipf 1.1, layout v2 keys) are laid out in SlateDB as:
//! - "rule 12": 1 sorted run + 2 L0 SSTs (the run folds every absorbed L0 SST; L0 holds what is not absorbed yet);
//! - "aged, busy": 4 sorted runs + 6 L0 SSTs (the owner's estimate at busy: up to 4 runs; L0 waits to 6 before folding);
//! - "aged, cap": 6 sorted runs + 6 L0 SSTs (the cap);
//! - "1 run + 6 L0": to separate the L0 effect.
//! Then a fresh reader (cold data blocks; index and filter blocks warmed first) does: point reads of keys that are not
//! in SlateDB (the merged read asks SlateDB first for every key), point reads of keys that are, and the scan of one
//! customer (limit 20) for customers with and without rows in SlateDB. Counted: GETs per read.
//!
//! Run: `cargo run --release --bin review_sr_bench -- [unit_kib=1024]`.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::CompactorOptions;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct Count {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicU64,
}

impl std::fmt::Display for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Count")
    }
}

#[async_trait]
impl ObjectStore for Count {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        if !o.head {
            self.gets.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(l, o).await
    }
    fn delete_stream(&self, s: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(s)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(&self, f: &Path, t: &Path, o: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

/// What the scheduler should do next: fold every L0 SST into a new run, or into the newest run.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Want {
    Nothing,
    NewRun,
    Fold,
}

#[derive(Clone)]
struct Sched(Arc<Mutex<Want>>);

impl CompactionSchedulerSupplier for Sched {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.clone())
    }
}

impl CompactionScheduler for Sched {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if let Some(c) = state.compactions() {
            if c.recent_compactions().any(|c| c.active()) {
                return vec![];
            }
        }
        let m = state.manifest();
        if m.l0().is_empty() {
            return vec![];
        }
        let mut w = self.0.lock().unwrap();
        let l0: Vec<SourceId> = m.l0().iter().map(|v| SourceId::SstView(v.id)).collect();
        let srs: Vec<u32> = m.compacted().iter().map(|r| r.id).collect();
        let out = match *w {
            Want::Nothing => vec![],
            Want::NewRun => vec![CompactionSpec::new(l0, srs.iter().max().map_or(0, |x| x + 1))],
            Want::Fold => match srs.first() {
                Some(&n) => {
                    let mut s = l0;
                    s.push(SourceId::SortedRun(n));
                    vec![CompactionSpec::new(s, n)]
                }
                None => vec![CompactionSpec::new(l0, 0)],
            },
        };
        if !out.is_empty() {
            *w = Want::Nothing;
        }
        out
    }
}

fn ckey(c: u32, i: u64) -> Vec<u8> {
    let name = format!("c{c:05}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(format!("msgs/d/{i:010}").as_bytes());
    k
}

fn cprefix(c: u32) -> (Vec<u8>, Vec<u8>) {
    let name = format!("c{c:05}");
    let mut lo = b"u/".to_vec();
    lo.push(name.len() as u8);
    lo.extend(name.as_bytes());
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    (lo, hi)
}

struct Layout {
    name: &'static str,
    runs: usize,
    l0: usize,
}

async fn wait_l0(admin: &slatedb::admin::Admin, want_l0: usize, want_runs: usize) {
    for _ in 0..3000 {
        let m = admin.read_manifest(None).await.unwrap().unwrap();
        if m.l0().len() == want_l0 && m.compacted().len() == want_runs {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let m = admin.read_manifest(None).await.unwrap().unwrap();
    panic!("layout not reached: L0 {} runs {} (want {want_l0}, {want_runs})", m.l0().len(), m.compacted().len());
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let unit_kib: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let units = 12usize; // the same rows in every layout: 12 L0 SSTs' worth
    let layouts = [
        Layout { name: "rule 12: 1 run + 2 L0", runs: 1, l0: 2 },
        Layout { name: "1 run + 6 L0", runs: 1, l0: 6 },
        Layout { name: "aged busy: 4 runs + 6 L0", runs: 4, l0: 6 },
        Layout { name: "aged cap: 6 runs + 6 L0", runs: 6, l0: 6 },
    ];
    let customers = 4000u32;
    let zipf: Vec<f64> = {
        let w: Vec<f64> = (1..=customers).map(|r| 1.0 / (r as f64).powf(1.1)).collect();
        let s: f64 = w.iter().sum();
        let mut acc = 0.0;
        w.iter().map(|x| {
            acc += x / s;
            acc
        }).collect()
    };
    let pick = |rng: &mut StdRng| -> u32 {
        let x: f64 = rng.gen();
        zipf.partition_point(|c| *c < x).min(customers as usize - 1) as u32
    };
    println!("review_sr_bench: {units} units of {unit_kib} KiB of ~400-byte rows, {customers} customers (Zipf 1.1), SlateDB 0.17 defaults (4 KiB blocks, bloom filters), in-memory store, GETs counted");
    for lay in &layouts {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "review-sr";
        let mut s = slatedb::config::Settings::default();
        s.l0_sst_size_bytes = (unit_kib << 10) * 4; // one L0 SST per flush
        s.compactor_options = None;
        s.garbage_collector_options = None;
        s.l0_max_ssts = 64;
        s.l0_max_ssts_per_key = 64;
        let db = slatedb::Db::builder(path, Arc::clone(&inner)).with_settings(s).build().await.unwrap();
        let want = Arc::new(Mutex::new(Want::Nothing));
        let sched = Sched(Arc::clone(&want));
        let mut co = CompactorOptions::default();
        co.poll_interval = Duration::from_millis(10);
        let compactor = Arc::new(slatedb::CompactorBuilder::new(path, Arc::clone(&inner)).with_options(co).with_scheduler_supplier(Arc::new(sched)).build());
        let cc = Arc::clone(&compactor);
        let h = tokio::spawn(async move { cc.run().await });
        let admin = slatedb::admin::Admin::builder(path, Arc::clone(&inner)).build();
        let mut rng = StdRng::seed_from_u64(42);
        let mut in_slate: Vec<Vec<u8>> = Vec::new();
        let mut recent: std::collections::HashSet<u32> = Default::default();
        let mut next_id = 0u64;
        let to_runs = units - lay.l0;
        for u in 0..units {
            let mut bytes = 0usize;
            while bytes < unit_kib << 10 {
                let c = pick(&mut rng) * 2; // written customers have even numbers; odd ones never write
                let k = ckey(c, next_id);
                next_id += 1;
                let v: Vec<u8> = (0..rng.gen_range(300..500)).map(|_| rng.gen_range(b'a'..=b'z')).collect();
                bytes += k.len() + v.len();
                db.put(&k, &v).await.unwrap();
                if rng.gen_bool(0.01) {
                    in_slate.push(k);
                }
                recent.insert(c);
            }
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            if u < to_runs {
                // into the runs: rule 12 folds every unit into the one run; aged runs spread the units evenly over `runs`
                let run_of = |u: usize| u * lay.runs / to_runs;
                if lay.runs == 1 {
                    *want.lock().unwrap() = Want::Fold;
                    wait_l0(&admin, 0, 1).await;
                } else if u + 1 == to_runs || run_of(u + 1) != run_of(u) {
                    let runs_before = admin.read_manifest(None).await.unwrap().unwrap().compacted().len();
                    *want.lock().unwrap() = Want::NewRun;
                    wait_l0(&admin, 0, runs_before + 1).await;
                }
            }
        }
        compactor.stop().await.ok();
        let _ = h.await;
        db.close().await.unwrap();
        let m = admin.read_manifest(None).await.unwrap().unwrap();
        let (n_l0, n_runs) = (m.l0().len(), m.compacted().len());
        // a fresh reader: cold cache
        let count = Arc::new(Count { inner: Arc::clone(&inner), gets: AtomicU64::new(0) });
        let reader = slatedb::DbReader::builder(path, count.clone() as Arc<dyn ObjectStore>).build().await.unwrap();
        // warm the index and filter blocks of every SST: one point read of a key in no SST
        let g0 = count.gets.load(Relaxed);
        reader.get(b"zz-warm").await.unwrap();
        let warm = count.gets.load(Relaxed) - g0;
        let mut rng = StdRng::seed_from_u64(7);
        let n = 400u64;
        // point reads of keys not in SlateDB (ids beyond any written)
        let g0 = count.gets.load(Relaxed);
        for i in 0..n {
            let c = pick(&mut rng) * 2;
            assert!(reader.get(&ckey(c, 10_000_000_000 - i)).await.unwrap().is_none());
        }
        let absent = (count.gets.load(Relaxed) - g0) as f64 / n as f64;
        // point reads of keys in SlateDB
        let g0 = count.gets.load(Relaxed);
        let mut found = 0;
        for i in 0..n.min(in_slate.len() as u64) {
            let k = &in_slate[(i as usize * 7919) % in_slate.len()];
            found += reader.get(k).await.unwrap().is_some() as u64;
        }
        let present = (count.gets.load(Relaxed) - g0) as f64 / found.max(1) as f64;
        // the scan of one customer, limit 20: customers with rows in SlateDB, and customers without
        let mut with_rows: Vec<u32> = recent.iter().copied().collect();
        with_rows.sort();
        let without: Vec<u32> = (0..400).map(|i| i * 2 + 1).collect(); // between written customers, no row in SlateDB
        let mut scan_cost = |list: Vec<u32>| {
            let reader = &reader;
            let count = Arc::clone(&count);
            async move {
                let g0 = count.gets.load(Relaxed);
                let mut rows = 0usize;
                let k = list.len().min(n as usize);
                for c in list.iter().take(k) {
                    let (lo, hi) = cprefix(*c);
                    let mut it = reader.scan(lo..hi).await.unwrap();
                    let mut got = 0;
                    while got < 20 {
                        match it.next().await.unwrap() {
                            Some(_) => got += 1,
                            None => break,
                        }
                    }
                    rows += got;
                }
                ((count.gets.load(Relaxed) - g0) as f64 / k as f64, rows as f64 / k as f64)
            }
        };
        let step = (with_rows.len() / n as usize).max(1);
        let sample: Vec<u32> = with_rows.iter().step_by(step).copied().collect();
        let (scan_with, rows_with) = scan_cost(sample).await;
        let (scan_without, _) = scan_cost(without).await;
        println!(
            "{}: L0 {n_l0}, sorted runs {n_runs}; warm-up {warm} GETs; per read: absent key {absent:.2} GETs, present key {present:.2}, customer scan with rows {scan_with:.2} ({rows_with:.1} rows), customer scan without rows {scan_without:.2}",
            lay.name
        );
        reader.close().await.unwrap();
    }
}
