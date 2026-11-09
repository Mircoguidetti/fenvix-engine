//! Compaction 2, step 4: the time-compressed replay that measures write amplification (fix 7 of the plan, round 2).
//!
//! Usage: `cargo run --release --bin fx_replay -- <ours|today> <1tb|10tb> [D MiB, default 512] [measured fraction of D,
//! default 0.5] [warm-up fraction of D, default 0.25] [seed]`
//!
//! The scaling (plan round 2, fix 7, section C): the database holds D bytes; the delta tier averages theta * D / 2; L0
//! SSTs are scaled down with the memtable override so that the delta tier has as many tiers as at full size:
//! - 1 TB ratio: L0 = theta * D / 2 / 16384 (at 1 TB: 128 GB of delta over 8 MiB L0 SSTs = 4^7, seven tiers);
//! - 10 TB ratio: L0 = theta * D / 2 / 163840 (1,280 GB over 8 MiB: about 8.7 tiers).
//! Time is compressed so that D over the inserts of one day matches the profile (26.7 days at 1 TB medium, 267 at 10 TB
//! medium): with no cold reads, no erasure and R2 off, nothing here depends on time except the drop's lease, so the
//! compressed day is only a unit for the report.
//!
//! What runs: a writer `Tenant` (in-process, in-memory store) writes chat-like documents of about 400 bytes for 4,000
//! customers (Zipf 1.1) in key layout v2: 70% new documents, 25% rewrites of existing ones, 5% deletes, in batches of 32.
//! - `ours`: the step 4 compactor with the drop on (absorption every 4 L0 SSTs, delta tiers T = 4 with a tier-0 unit of
//!   4 L0 SSTs, reseals at theta = 0.25, 64 groups that split at 2 * D / 64 and merge under D / 256);
//! - `today`: compaction-1's tiered scheduler (T = 4, `tiered.rs`) on the same writes, as the server runs it.
//! First D is loaded with 16 MiB L0 SSTs (and, for `ours`, sealed by the offline builder of step 3; for `today`,
//! compacted by the tiered scheduler), then the writer reopens with the small L0 SSTs, then a warm-up, then the measured
//! window.
//!
//! Amplification = bytes written to the store by L0 flushes and compaction (SlateDB SSTs, our runs) in the window, over the
//! bytes of the L0 flushes in the window (one write of the inserted data in the stored format). WAL and manifest bytes
//! are reported apart. SlateDB writes lz4 SSTs here, like our runs, so the stored bytes compare.

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart};
use object_store::{CopyOptions, RenameOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::counting::CountingStore;
use tenant_bench::fx;
use tenant_bench::lifecycle::{self, Lifecycle, Op};

/// Bytes written per (tag, class): tag "w" the writer, "c" the compaction side.
#[derive(Default)]
struct Bytes {
    m: Mutex<HashMap<(&'static str, &'static str), u64>>,
}

static BYTES: std::sync::LazyLock<Bytes> = std::sync::LazyLock::new(Bytes::default);

fn class(p: &str) -> &'static str {
    if p.contains("/fx/run/") {
        "fxrun"
    } else if p.contains("/fx/") {
        "fxmeta"
    } else if p.contains("/compacted/") {
        "sst"
    } else if p.contains("/wal/") {
        "wal"
    } else {
        "other"
    }
}

fn add(tag: &'static str, p: &str, n: u64) {
    *BYTES.m.lock().unwrap().entry((tag, class(p))).or_default() += n;
}

fn snap() -> HashMap<(&'static str, &'static str), u64> {
    BYTES.m.lock().unwrap().clone()
}

#[derive(Debug)]
struct Tag {
    inner: Arc<dyn ObjectStore>,
    tag: &'static str,
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tag({})", self.tag)
    }
}

#[async_trait]
impl ObjectStore for Tag {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        add(self.tag, location.as_ref(), payload.content_length() as u64);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        let u = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(TagUpload { inner: u, tag: self.tag, path: location.to_string() }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct TagUpload {
    inner: Box<dyn MultipartUpload>,
    tag: &'static str,
    path: String,
}

#[async_trait]
impl MultipartUpload for TagUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        add(self.tag, &self.path, data.content_length() as u64);
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

const WORDS: &[&str] = &["the", "agent", "replied", "with", "a", "summary", "of", "order", "status", "please", "check", "invoice", "shipping", "tomorrow", "thanks", "customer", "support", "ticket", "update", "refund", "account", "password", "reset", "delivery", "address", "confirmed", "pending", "error"];

fn doc(rng: &mut StdRng, n: usize) -> Vec<u8> {
    let mut d = Vec::with_capacity(n + 16);
    d.extend(b"{\"role\":\"user\",\"ts\":");
    d.extend(format!("{},\"text\":\"", rng.gen::<u32>()).as_bytes());
    while d.len() < n {
        if rng.gen_bool(0.5) {
            d.extend(WORDS[rng.gen_range(0..WORDS.len())].as_bytes());
        } else {
            for _ in 0..rng.gen_range(3..9) {
                d.push(b'a' + rng.gen_range(0..26));
            }
        }
        d.push(b' ');
    }
    d.extend(b"\"}");
    d
}

fn ckey(c: u32, i: u64) -> Vec<u8> {
    let name = format!("c{c:05}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(b"msgs/d/");
    k.extend(i.to_be_bytes());
    k
}

/// Zipf over n customers with exponent s, by inverse CDF on a table.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Zipf {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for i in 1..=n {
            acc += 1.0 / (i as f64).powf(s);
            cdf.push(acc);
        }
        for x in cdf.iter_mut() {
            *x /= acc;
        }
        Zipf { cdf }
    }
    fn sample(&self, rng: &mut StdRng) -> u32 {
        let u: f64 = rng.gen();
        self.cdf.partition_point(|&x| x < u) as u32
    }
}

struct Writer {
    rng: StdRng,
    z: Zipf,
    next: Vec<u64>,
    written: u64,
}

impl Writer {
    fn batch(&mut self, n: usize) -> (Vec<Op>, u64) {
        let mut ops = Vec::with_capacity(n);
        let mut bytes = 0;
        for _ in 0..n {
            let c = self.z.sample(&mut self.rng);
            let r: f64 = self.rng.gen();
            let have = self.next[c as usize];
            if r < 0.70 || have == 0 {
                let k = ckey(c, have);
                self.next[c as usize] += 1;
                let len = self.rng.gen_range(250..550);
                let v = doc(&mut self.rng, len);
                bytes += (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
            } else if r < 0.95 {
                let k = ckey(c, self.rng.gen_range(0..have));
                let len = self.rng.gen_range(250..550);
                let v = doc(&mut self.rng, len);
                bytes += (k.len() + v.len()) as u64;
                ops.push(Op::Put(k, v));
            } else {
                let k = ckey(c, self.rng.gen_range(0..have));
                bytes += k.len() as u64;
                ops.push(Op::Delete(k));
            }
        }
        self.written += bytes;
        (ops, bytes)
    }
}

fn mib(x: u64) -> f64 {
    x as f64 / (1 << 20) as f64
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let mode = a.get(1).map(|s| s.as_str()).unwrap_or("ours").to_string();
    let ratio = a.get(2).map(|s| s.as_str()).unwrap_or("1tb").to_string();
    let d_mib: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(512);
    let frac: f64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(0.5);
    let warm: f64 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(0.25);
    let seed: u64 = a.get(6).and_then(|s| s.parse().ok()).unwrap_or(42);
    let theta = 0.25;
    let d = d_mib << 20;
    let tiers_div: f64 = if ratio == "10tb" { 163_840.0 } else { 16_384.0 };
    let days_per_d: f64 = if ratio == "10tb" { 267.0 } else { 26.7 };
    let l0 = ((theta * d as f64 / 2.0) / tiers_div).max(512.0) as u64;
    println!("replay: mode {mode}, ratio {ratio}, D {d_mib} MiB, L0 {l0} bytes, measured {frac} D after a warm-up of {warm} D, seed {seed}");
    tenant_bench::tiered::set_compression("lz4")?;
    fx::ENABLED.store(true, Relaxed);
    fx::E1_WAIT_MS.store(0, Relaxed);
    fx::hold::HOLD_LIFE_MS.store(2000, Relaxed);
    fx::T_PIN_MS.store(1000, Relaxed); // under the compressed lease (1.6 s)
    lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let w_store: Arc<dyn ObjectStore> = Arc::new(Tag { inner: Arc::clone(&mem), tag: "w" });
    let c_store: Arc<dyn ObjectStore> = Arc::new(Tag { inner: Arc::clone(&mem), tag: "c" });
    let lc = Lifecycle::new(Arc::new(CountingStore::new(Arc::clone(&w_store))), None);
    let id = "replay";
    let mut w = Writer { rng: StdRng::seed_from_u64(seed), z: Zipf::new(4000, 1.1), next: vec![0; 4000], written: 0 };

    // 1. load D with large L0 SSTs (no stall), then seal it (ours) or compact it (today)
    lifecycle::L0_SST_BYTES_OVERRIDE.store(16 << 20, Relaxed);
    lifecycle::L0_MAX_SSTS_OVERRIDE.store(10_000, Relaxed);
    let t0 = Instant::now();
    let mut t = lc.create(id).await?;
    while w.written < d {
        let (ops, _) = w.batch(256);
        t.apply(ops).await?;
    }
    println!("loaded {:.0} MiB in {:.1} s", mib(w.written), t0.elapsed().as_secs_f64());
    let groups = 64u64;
    if mode == "ours" {
        let opts = fx::build::BuildOpts { reseal: fx::build::Reseal::All, group_bytes: d / groups, ..Default::default() };
        loop {
            if t.fx_publish(&opts, None).await?.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        println!("first version built: {} groups", t.fx_view().unwrap().groups.len());
    }
    t.close().await?;

    // 2. reopen with small L0 SSTs
    lifecycle::L0_SST_BYTES_OVERRIDE.store(l0, Relaxed);
    // L0 may pass 8 here (it measures bytes; the pool test measures L0): no stall waiting for the writer's manifest poll
    lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    let mut t = lc.wake(id).await?;
    t.upgrade().await?;
    let slots = 3;
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(tokio::runtime::Builder::new_multi_thread().worker_threads(slots).thread_name("fx-pool").enable_all().build()?)); // lives for the whole process
    let pool = fx::pool::Pool::new(slots);
    let mut comp = None;
    let mut today: Option<(Arc<slatedb::compactor::Compactor>, Arc<slatedb::CompactionWorker>)> = None;
    if mode == "ours" {
        let k = fx::compactor::Knobs {
            theta,
            r3: false,
            tier_unit: 4 * l0,
            absorb_min_l0: 4,
            absorb_max_l0: 8,
            local_max_bytes: 0,
            split_bytes: 2 * d / groups,
            merge_bytes: d / groups / 4,
            min_seal_bytes: d / groups / 2,
            drop: true,
            lease: Duration::from_millis(1600),
            remote_lease: Duration::from_millis(3200),
            serving_lease: Duration::from_millis(200),
            put_timeout: Duration::from_millis(1000),
            gc_grace: Duration::from_millis(3200),
            version_retention: Duration::ZERO,
            gc_every: Duration::from_secs(2),
            tick: Duration::from_millis(5),
            ..Default::default()
        };
        let c = fx::compactor::Compactor::start(&mut t, Arc::clone(&c_store), k, pool, rt.handle().clone()).await?;
        c.run();
        comp = Some(c);
    } else {
        let mut opts = slatedb::config::CompactorOptions::default();
        opts.poll_interval = Duration::from_millis(20);
        opts.worker = None;
        let path = lifecycle::tenant_path(id);
        let cb = slatedb::CompactorBuilder::new(path.clone(), Arc::clone(&c_store)).with_options(opts).with_scheduler_supplier(tenant_bench::tiered::scheduler().unwrap());
        let compactor = Arc::new(cb.build());
        let worker = Arc::new(tenant_bench::tiered::worker_builder(path.into(), Arc::clone(&c_store), None).build().await?);
        let (cc, ww) = (Arc::clone(&compactor), Arc::clone(&worker));
        rt.spawn(async move {
            let _ = tokio::join!(cc.run(), ww.run());
        });
        today = Some((compactor, worker));
    }
    let gc_lc = Arc::clone(&mem);
    let gc_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gs = Arc::clone(&gc_stop);
    let gc_task = tokio::spawn(async move {
        // SlateDB's own GC: folded L0 SSTs, rewritten sorted runs and old WAL objects leave the store
        while !gs.load(Relaxed) {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let _ = lifecycle::gc_tenant(&gc_lc, "replay", Duration::from_secs(5)).await;
        }
    });

    // 3. warm-up, then the measured window
    let max_l0 = AtomicU64::new(0);
    let mut phase_start = snap();
    let mut inserted_window = 0u64;
    let mut stall_max = Duration::ZERO;
    let warm_end = w.written + (warm * d as f64) as u64;
    let end = warm_end + (frac * d as f64) as u64;
    let t_start = Instant::now();
    let mut t_window = Instant::now();
    let mut cpu_window = 0u64;
    let mut last_print = Instant::now();
    let mut started = false;
    let base_stats = |c: &Option<Arc<fx::compactor::Compactor>>| c.as_ref().map(|c| (c.sh.stats.absorbed_raw.load(Relaxed), c.sh.stats.merge_raw_out.load(Relaxed), c.sh.stats.reseal_raw_out.load(Relaxed), c.sh.stats.job_cpu_us.load(Relaxed)));
    let mut base = base_stats(&comp);
    while w.written < end {
        if !started && w.written >= warm_end {
            started = true;
            phase_start = snap();
            inserted_window = 0;
            t_window = Instant::now();
            cpu_window = fx::compactor::process_cpu_us();
            base = base_stats(&comp);
            println!("measured window starts at {:.0} MiB written", mib(w.written));
        }
        t.fx_sync();
        let (ops, b) = w.batch(32);
        let s = Instant::now();
        t.apply(ops).await?;
        stall_max = stall_max.max(s.elapsed());
        if started {
            inserted_window += b;
        }
        if let Some(n) = t.writer_l0() {
            max_l0.fetch_max(n as u64, Relaxed);
        }
        if last_print.elapsed() > Duration::from_secs(20) {
            last_print = Instant::now();
            let extra = comp.as_ref().map(|c| format!(", groups {}, delta runs {}, reseals {}, merges {}, absorbs {}, drop jobs {}", c.sh.pending.lock().unwrap().as_ref().map_or(0, |v| v.groups.len()), c.sh.pending.lock().unwrap().as_ref().map_or(0, |v| v.delta.len()), c.sh.stats.reseals.load(Relaxed), c.sh.stats.merges.load(Relaxed), c.sh.stats.absorbs.load(Relaxed), c.drop.jobs_done.load(Relaxed))).unwrap_or_default();
            println!("  {:.0} s: {:.0} MiB written, L0 max {}{extra}", t_start.elapsed().as_secs_f64(), mib(w.written), max_l0.load(Relaxed));
        }
    }
    // let the compaction catch up on what was written in the window (both modes the same way)
    let settle = Instant::now();
    tokio::time::sleep(Duration::from_secs(3)).await;
    if let Some(c) = &comp {
        while !(c.idle() && c.sh.stats.reseals_due.load(Relaxed) == 0) && settle.elapsed() < Duration::from_secs(120) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let end_snap = snap();
    let cpu = fx::compactor::process_cpu_us() - cpu_window;
    let wall = t_window.elapsed().as_secs_f64();
    let delta = |tag: &'static str, cl: &'static str| end_snap.get(&(tag, cl)).copied().unwrap_or(0) - phase_start.get(&(tag, cl)).copied().unwrap_or(0);
    let l0_bytes = delta("w", "sst");
    let comp_sst = delta("c", "sst");
    let runs = delta("c", "fxrun");
    let amp = (l0_bytes + comp_sst + runs) as f64 / l0_bytes.max(1) as f64;
    println!("RESULT mode {mode} ratio {ratio} D {d_mib} MiB L0 {l0} B: inserted {:.1} MiB raw in the window ({:.2} compressed days), L0 flushes {:.1} MiB, SlateDB compaction {:.1} MiB, our runs {:.1} MiB, WAL {:.1} MiB, our manifests {:.1} MiB", mib(inserted_window), inserted_window as f64 / (d as f64 / days_per_d), mib(l0_bytes), mib(comp_sst), mib(runs), mib(delta("w", "wal")), mib(delta("c", "fxmeta")));
    println!("RESULT amplification {amp:.2} (stored bytes written per stored byte flushed to L0); L0 max {}; longest write {:.0} ms; window {:.0} s, process CPU {:.0} s", max_l0.load(Relaxed), stall_max.as_secs_f64() * 1000.0, wall, cpu as f64 / 1e6);
    if let (Some(c), Some(b)) = (&comp, base) {
        let s = &c.sh.stats;
        let ab = s.absorbed_raw.load(Relaxed) - b.0;
        let me = s.merge_raw_out.load(Relaxed) - b.1;
        let re = s.reseal_raw_out.load(Relaxed) - b.2;
        let raw_amp = 1.0 + (ab + me + re) as f64 / ab.max(1) as f64;
        let delta_writes = (ab + me) as f64 / ab.max(1) as f64;
        let v = c.sh.pending.lock().unwrap().clone().unwrap();
        let delta_raw: u64 = v.delta.iter().map(|d| d.raw).sum();
        let unit = 4 * l0;
        let formula_tiers = ((theta * d as f64 / 2.0) / unit as f64).ln() / 4f64.ln() + 1.0;
        println!("RESULT ours, raw rows: amplification {raw_amp:.2} = 1 (L0) + delta {delta_writes:.2} (absorb + merges) + reseals {:.2}; reseals {}, splits {}, group merges {}, merges {}, absorbs {}, groups {}, delta runs {} holding {:.1} MiB raw; job CPU {:.1} s, {:.1} s per GiB written by our jobs", re as f64 / ab.max(1) as f64, s.reseals.load(Relaxed), s.splits.load(Relaxed), s.group_merges.load(Relaxed), s.merges.load(Relaxed), s.absorbs.load(Relaxed), v.groups.len(), v.delta.len(), mib(delta_raw), (s.job_cpu_us.load(Relaxed) - b.3) as f64 / 1e6, (s.job_cpu_us.load(Relaxed) - b.3) as f64 / 1e6 / ((ab + me + re) as f64 / (1u64 << 30) as f64));
        println!("RESULT formula for these parameters: 1 + {formula_tiers:.2} delta writes + {:.2} reseal = {:.2} (the plan's formula at full size: 13 at 1 TB, 15 at 10 TB)", (1.0 + theta) / theta, 1.0 + formula_tiers + (1.0 + theta) / theta);
        println!("RESULT drop: jobs {}, rows dropped {}, rows kept {}, GC runs deleted {}, versions deleted {}", c.drop.jobs_done.load(Relaxed), c.drop.rows_dropped.load(Relaxed), c.drop.rows_kept.load(Relaxed), c.gc.runs_deleted.load(Relaxed), c.gc.versions_deleted.load(Relaxed));
    }
    // a final check: a sample of keys reads the same through the merged reader and a model is not kept here; the model
    // tests cover correctness. Stop.
    gc_stop.store(true, Relaxed);
    let _ = gc_task.await;
    if let Some(c) = comp {
        c.stop().await;
    }
    if let Some((c, wk)) = today {
        let _ = c.stop().await;
        let _ = wk.stop().await;
    }
    t.abandon().await;
    Ok(())
}
