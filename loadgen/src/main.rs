//! Load generator for the MongoDB wire server, built on the official Rust `mongodb` driver.
//!
//! `loadgen load` creates tenants (one database per tenant, one collection `c`, N documents of about 1 KB).
//! `loadgen run` drives a scenario: `rotate` (a fixed number of tenants active at a time, each for a fixed
//! lifetime, then replaced by a new one) or `burst` (many cold tenants all sending their first request within a
//! ramp window), optionally with one hot tenant. It records per-operation latency, the latency of each tenant's
//! first request (the wake), errors by code, samples the servers' CPU, memory, open tenants and object-store
//! requests every few seconds, and can verify afterwards that every acknowledged write is present.

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};
use hdrhistogram::Histogram;
use mongodb::bson::{doc, Bson, Document};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::ClientOptions;
use mongodb::Client;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(about = "Load generator for mongo-server")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Load(LoadArgs),
    Run(Box<RunArgs>),
}

#[derive(Args, Clone)]
struct Common {
    /// Server address(es), comma separated (several for the two-process scenario).
    #[arg(long, default_value = "mongodb://127.0.0.1:27018")]
    uris: String,
    /// Driver connection pool size per server.
    #[arg(long, default_value_t = 256)]
    pool_size: u32,
    /// Tenant database name prefix; the name is `{prefix}{index:06}`.
    #[arg(long, default_value = "t")]
    prefix: String,
    #[arg(long, default_value_t = 2)]
    worker_threads: usize,
}

#[derive(Args)]
struct LoadArgs {
    #[command(flatten)]
    common: Common,
    /// Number of tenants to create.
    #[arg(long, default_value_t = 50_000)]
    tenants: u64,
    /// Index of the first tenant.
    #[arg(long, default_value_t = 0)]
    first: u64,
    /// Documents per tenant.
    #[arg(long, default_value_t = 50)]
    docs: usize,
    /// Approximate document size in bytes.
    #[arg(long, default_value_t = 1000)]
    doc_bytes: usize,
    /// Tenants loaded in parallel.
    #[arg(long, default_value_t = 64)]
    parallel: usize,
    /// Close each tenant right after loading it (keeps the server's memory flat).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    close_after: bool,
    #[arg(long, default_value = "load-out.json")]
    out: PathBuf,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    common: Common,
    /// Scenario name (recorded in the output).
    #[arg(long, default_value = "run")]
    name: String,
    /// `rotate` or `burst`.
    #[arg(long, default_value = "rotate")]
    mode: String,
    /// Index of the first tenant of the pool the scenario draws from.
    #[arg(long, default_value_t = 0)]
    start: u64,
    /// Size of the tenant pool (at least `active` in rotate mode; the cursor wraps around it).
    #[arg(long, default_value_t = 50_000)]
    pool: u64,
    /// Tenants active at a time (rotate) or arriving in total (burst).
    #[arg(long, default_value_t = 1000)]
    active: u64,
    /// Rotate: seconds each tenant stays active before being replaced.
    #[arg(long, default_value_t = 60)]
    lifetime_secs: u64,
    /// Burst: all tenants send their first request within this many seconds.
    #[arg(long, default_value_t = 60)]
    ramp_secs: u64,
    /// Burst: seconds each tenant keeps sending after its first request.
    #[arg(long, default_value_t = 120)]
    hold_secs: u64,
    /// Operations per second per active tenant.
    #[arg(long, default_value_t = 1.0)]
    rate: f64,
    /// Percentage of operations that are reads (the rest are inserts and updates, half each).
    #[arg(long, default_value_t = 80)]
    read_pct: u32,
    /// Total scenario duration in seconds.
    #[arg(long, default_value_t = 600)]
    duration_secs: u64,
    /// Documents per tenant as loaded (read ids are drawn from 0..docs).
    #[arg(long, default_value_t = 50)]
    docs: u64,
    /// Index of one hot tenant (outside the pool), or -1 for none.
    #[arg(long, default_value_t = -1)]
    hot_tenant: i64,
    /// Operations per second sent to the hot tenant.
    #[arg(long, default_value_t = 500.0)]
    hot_rate: f64,
    /// Concurrent workers sharing the hot tenant's rate.
    #[arg(long, default_value_t = 16)]
    hot_workers: u32,
    /// Seconds between samples of the servers' CPU, memory, open tenants and requests.
    #[arg(long, default_value_t = 5)]
    sample_secs: u64,
    /// Extra processes to sample, `name=pid` (for example `minio=1234`).
    #[arg(long, value_delimiter = ',')]
    proc: Vec<String>,
    /// Attempts per operation before it counts as a failure.
    #[arg(long, default_value_t = 30)]
    max_attempts: u32,
    #[arg(long, default_value_t = 100)]
    retry_backoff_ms: u64,
    /// Per-attempt timeout in seconds.
    #[arg(long, default_value_t = 10)]
    op_timeout_secs: u64,
    /// After the run, read back every tenant that received acknowledged writes and check them.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    verify: bool,
    #[arg(long, default_value = "run-out")]
    out: PathBuf,
}

fn tname(prefix: &str, idx: u64) -> String {
    format!("{prefix}{idx:06}")
}

fn unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

async fn make_client(uri: &str, pool: u32) -> Result<Client> {
    let mut o = ClientOptions::parse(uri).await?;
    o.max_pool_size = Some(pool);
    o.min_pool_size = Some(0);
    o.direct_connection = Some(true);
    o.retry_reads = Some(false);
    o.retry_writes = Some(false);
    o.server_selection_timeout = Some(Duration::from_secs(3));
    o.connect_timeout = Some(Duration::from_secs(3));
    o.app_name = Some("loadgen".into());
    Ok(Client::with_options(o)?)
}

fn pad(rng: &mut StdRng, n: usize) -> String {
    const A: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..n).map(|_| A[rng.gen_range(0..A.len())] as char).collect()
}

/// A short, stable label for an error, used to count errors by code.
fn classify(e: &mongodb::error::Error) -> String {
    match e.kind.as_ref() {
        ErrorKind::Command(c) => format!("cmd:{}", c.code),
        ErrorKind::Write(WriteFailure::WriteError(w)) => format!("write:{}", w.code),
        ErrorKind::Write(_) => "write:concern".into(),
        ErrorKind::Io(_) => "io".into(),
        ErrorKind::ServerSelection { .. } => "server_selection".into(),
        ErrorKind::ConnectionPoolCleared { .. } => "pool_cleared".into(),
        other => {
            let s = format!("{other:?}");
            format!("other:{}", s.split(['{', '(', ' ']).next().unwrap_or("?"))
        }
    }
}

fn retryable(e: &mongodb::error::Error) -> bool {
    match e.kind.as_ref() {
        ErrorKind::Command(c) => c.code == 134 || e.contains_label("RetryableWriteError"),
        ErrorKind::Write(WriteFailure::WriteError(w)) => w.code == 134,
        ErrorKind::Io(_) | ErrorKind::ServerSelection { .. } | ErrorKind::ConnectionPoolCleared { .. } => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// load
// ---------------------------------------------------------------------------

async fn load(a: LoadArgs) -> Result<()> {
    let uri = a.common.uris.split(',').next().unwrap().to_string();
    let client = make_client(&uri, a.common.pool_size.max(a.parallel as u32 + 8)).await?;
    let started = Instant::now();
    let done = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).unwrap()));
    let prog = {
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                eprintln!("loaded {} tenants after {:.0} s", done.load(Relaxed), started.elapsed().as_secs_f64());
            }
        })
    };
    let (docs, doc_bytes, prefix, close_after) = (a.docs, a.doc_bytes, a.common.prefix.clone(), a.close_after);
    futures::stream::iter(a.first..a.first + a.tenants)
        .map(|idx| {
            let client = client.clone();
            let (done, failed, lat, prefix) = (Arc::clone(&done), Arc::clone(&failed), Arc::clone(&lat), prefix.clone());
            async move {
                let t0 = Instant::now();
                let name = tname(&prefix, idx);
                let db = client.database(&name);
                let mut rng = StdRng::seed_from_u64(idx);
                let batch: Vec<Document> = (0..docs).map(|i| doc! { "_id": i as i32, "n": i as i32, "pad": pad(&mut rng, doc_bytes.saturating_sub(100)) }).collect();
                let mut ok = false;
                for attempt in 0..20 {
                    let r = db.collection::<Document>("c").insert_many(batch.clone()).await;
                    match r {
                        Ok(_) => ok = true,
                        Err(e) if matches!(e.kind.as_ref(), ErrorKind::InsertMany(_)) => {
                            // A retry after an unknown outcome finds the documents already there.
                            ok = e.to_string().contains("11000");
                        }
                        Err(e) => {
                            if attempt == 19 {
                                eprintln!("tenant {name}: {e}");
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                    if ok {
                        break;
                    }
                }
                if ok && close_after {
                    let _ = db.run_command(doc! { "tenantAdmin": 1, "close": true }).await;
                }
                if ok {
                    done.fetch_add(1, Relaxed);
                    lat.lock().unwrap().record(t0.elapsed().as_micros().max(1) as u64).ok();
                } else {
                    failed.fetch_add(1, Relaxed);
                }
            }
        })
        .buffer_unordered(a.parallel)
        .collect::<Vec<()>>()
        .await;
    prog.abort();
    let secs = started.elapsed().as_secs_f64();
    let h = lat.lock().unwrap();
    let out = json!({
        "tenants": a.tenants, "first": a.first, "docs_per_tenant": a.docs, "doc_bytes": a.doc_bytes, "parallel": a.parallel,
        "loaded": done.load(Relaxed), "failed": failed.load(Relaxed), "seconds": secs, "tenants_per_sec": done.load(Relaxed) as f64 / secs,
        "per_tenant_latency_ms": { "p50": h.value_at_quantile(0.5) as f64 / 1000.0, "p99": h.value_at_quantile(0.99) as f64 / 1000.0, "max": h.max() as f64 / 1000.0 },
        "unix_ms_end": unix_ms(),
    });
    std::fs::write(&a.out, serde_json::to_string_pretty(&out)?)?;
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

type Hist = Histogram<u64>;

fn new_hist() -> Hist {
    Histogram::new_with_bounds(1, 600_000_000, 3).unwrap()
}

#[derive(Default)]
struct Group {
    ops: BTreeMap<&'static str, Hist>,
    steady: BTreeMap<&'static str, Hist>,
    wake: Option<Hist>,
    wake_by_kind: BTreeMap<&'static str, Hist>,
    attempts_hist: BTreeMap<u32, u64>,
    activations: u64,
    ops_done: u64,
    late_ticks: u64,
}

#[derive(Default)]
struct Stats {
    groups: [Group; 2], // 0 = normal tenants, 1 = hot tenant
    errors: BTreeMap<String, u64>,
    failures: BTreeMap<String, u64>,
    misses: u64,
    retries: u64,
    acked_writes: u64,
}

#[derive(Default)]
struct Acks {
    inserted: Vec<String>,
    updates: HashMap<String, u64>,
}

struct Shared {
    clients: Vec<Client>,
    uris: Vec<String>,
    a: RunArgs,
    start: Instant,
    end: Instant,
    stats: Mutex<Stats>,
    acks: Mutex<HashMap<String, Acks>>,
    ver: AtomicU64,
    act: AtomicU64,
    cursor: AtomicU64,
    ops_total: AtomicU64,
    stop: AtomicBool,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    FindId,
    FindFilter,
    Insert,
    Update,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::FindId => "find_by_id",
            Kind::FindFilter => "find_filter",
            Kind::Insert => "insert",
            Kind::Update => "update",
        }
    }
}

struct OpOutcome {
    micros: u64,
    ok: bool,
}

impl Shared {
    fn is_over(&self) -> bool {
        self.stop.load(Relaxed) || Instant::now() >= self.end
    }

    fn bump_error(&self, class: &str, final_failure: bool) {
        let mut s = self.stats.lock().unwrap();
        *s.errors.entry(class.to_string()).or_default() += 1;
        if final_failure {
            *s.failures.entry(class.to_string()).or_default() += 1;
        }
    }

    /// One logical operation with retries across the servers. Returns the client-perceived latency.
    async fn op(&self, tenant: &str, kind: Kind, rng: &mut StdRng, act: u64, seq: u64, hot_worker: Option<u32>) -> OpOutcome {
        let t0 = Instant::now();
        let ver = self.ver.fetch_add(1, Relaxed) + 1;
        let id_read = rng.gen_range(0..self.a.docs.max(1)) as i32;
        let insert_id = match hot_worker {
            Some(w) => format!("w-{act}-{w}-{seq}"),
            None => format!("w-{act}-{seq}"),
        };
        let update_id: Bson = match hot_worker {
            Some(w) => Bson::String(format!("u{w}")),
            None => Bson::Int32(0),
        };
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            let client = &self.clients[rng.gen_range(0..self.clients.len())];
            let coll = client.database(tenant).collection::<Document>("c");
            let fut = async {
                match kind {
                    Kind::FindId => {
                        let d = coll.find_one(doc! { "_id": id_read }).await?;
                        Ok::<bool, mongodb::error::Error>(d.is_some())
                    }
                    Kind::FindFilter => {
                        let cur = coll.find(doc! { "n": id_read }).await?;
                        let v: Vec<Document> = cur.try_collect().await?;
                        Ok(!v.is_empty())
                    }
                    Kind::Insert => {
                        coll.insert_one(doc! { "_id": &insert_id, "n": 1000 + seq as i64, "pad": "x".repeat(100) }).await?;
                        Ok(true)
                    }
                    Kind::Update => {
                        let r = coll.update_one(doc! { "_id": update_id.clone() }, doc! { "$set": { "last": ver as i64 } }).await?;
                        Ok(r.matched_count > 0)
                    }
                }
            };
            let res = tokio::time::timeout(Duration::from_secs(self.a.op_timeout_secs), fut).await;
            let (ok, class, can_retry) = match res {
                Ok(Ok(found)) => {
                    if !found {
                        // For an update or read of a loaded document this means the data is gone (hot-tenant
                        // `u{w}` documents are created on first use below).
                        if kind == Kind::Update && hot_worker.is_some() {
                            let _ = coll.insert_one(doc! { "_id": update_id.clone(), "last": ver as i64 }).await;
                        } else if kind != Kind::Insert {
                            self.stats.lock().unwrap().misses += 1;
                        }
                    }
                    (true, String::new(), false)
                }
                Ok(Err(e)) => {
                    let class = classify(&e);
                    // An insert retried after an unknown outcome may find its own earlier write.
                    if kind == Kind::Insert && attempts > 1 && class == "write:11000" {
                        (true, String::new(), false)
                    } else {
                        let r = retryable(&e);
                        (false, class, r)
                    }
                }
                Err(_) => (false, "timeout".to_string(), true),
            };
            if ok {
                let micros = t0.elapsed().as_micros().max(1) as u64;
                if kind == Kind::Insert || kind == Kind::Update {
                    let mut s = self.stats.lock().unwrap();
                    s.acked_writes += 1;
                    if attempts > 1 {
                        s.retries += (attempts - 1) as u64;
                    }
                    drop(s);
                    let mut acks = self.acks.lock().unwrap();
                    let e = acks.entry(tenant.to_string()).or_default();
                    if kind == Kind::Insert {
                        e.inserted.push(insert_id.clone());
                    } else {
                        let key = match &update_id {
                            Bson::String(s) => s.clone(),
                            _ => "0".to_string(),
                        };
                        let v = e.updates.entry(key).or_default();
                        *v = (*v).max(ver);
                    }
                } else if attempts > 1 {
                    self.stats.lock().unwrap().retries += (attempts - 1) as u64;
                }
                self.stats.lock().unwrap().groups[hot_worker.is_some() as usize].attempts_hist.entry(attempts.min(10)).and_modify(|c| *c += 1).or_insert(1);
                return OpOutcome { micros, ok: true };
            }
            let last = !can_retry || attempts >= self.a.max_attempts || self.is_over_hard();
            self.bump_error(&class, last);
            if last {
                return OpOutcome { micros: t0.elapsed().as_micros().max(1) as u64, ok: false };
            }
            tokio::time::sleep(Duration::from_millis(self.a.retry_backoff_ms + rng.gen_range(0..self.a.retry_backoff_ms))).await;
        }
    }

    /// Retries stop a little after the scenario ends so a dead server cannot hold the run open.
    fn is_over_hard(&self) -> bool {
        Instant::now() >= self.end + Duration::from_secs(20)
    }

    fn record(&self, hot: bool, kind: Kind, first: bool, o: &OpOutcome) {
        if !o.ok {
            return;
        }
        let mut s = self.stats.lock().unwrap();
        let g = &mut s.groups[hot as usize];
        g.ops.entry(kind.name()).or_insert_with(new_hist).record(o.micros).ok();
        g.ops_done += 1;
        if first {
            g.wake.get_or_insert_with(new_hist).record(o.micros).ok();
            g.wake_by_kind.entry(kind.name()).or_insert_with(new_hist).record(o.micros).ok();
        } else {
            g.steady.entry(kind.name()).or_insert_with(new_hist).record(o.micros).ok();
        }
        drop(s);
        self.ops_total.fetch_add(1, Relaxed);
    }

    fn pick(&self, rng: &mut StdRng) -> Kind {
        let r = rng.gen_range(0..100);
        if r < self.a.read_pct {
            if rng.gen_bool(0.5) { Kind::FindId } else { Kind::FindFilter }
        } else if rng.gen_bool(0.5) {
            Kind::Insert
        } else {
            Kind::Update
        }
    }
}

/// One tenant's activation: operations at `rate` per second for `life`, the first one being the wake.
async fn activation(sh: Arc<Shared>, idx: i64, life: Duration, rate: f64, hot_worker: Option<u32>, immediate: bool) {
    let hot = hot_worker.is_some();
    let tenant = tname(&sh.a.common.prefix, idx as u64);
    let act = sh.act.fetch_add(1, Relaxed);
    let mut rng = StdRng::seed_from_u64(act ^ 0x9e3779b97f4a7c15);
    let interval = Duration::from_secs_f64(1.0 / rate);
    let end = (Instant::now() + life).min(sh.end);
    let mut next = Instant::now() + if immediate { Duration::ZERO } else { interval.mul_f64(rng.gen::<f64>()) };
    let mut seq = 0u64;
    sh.stats.lock().unwrap().groups[hot as usize].activations += 1;
    while !sh.is_over() && next < end {
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        } else if now - next > interval {
            sh.stats.lock().unwrap().groups[hot as usize].late_ticks += 1;
        }
        // The very first request of an activation is a read (a tenant is woken by whatever arrives first).
        let kind = if seq == 0 && !hot { if rng.gen_bool(0.5) { Kind::FindId } else { Kind::FindFilter } } else { sh.pick(&mut rng) };
        let o = sh.op(&tenant, kind, &mut rng, act, seq, hot_worker).await;
        sh.record(hot, kind, seq == 0, &o);
        seq += 1;
        next += interval;
        if next < Instant::now() {
            next = Instant::now();
        }
    }
}

fn read_proc(pid: u32) -> Option<(f64, f64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = after.split_whitespace().collect();
    let ticks: f64 = f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?;
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let rss_pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some((ticks / 100.0, rss_pages * 4096.0 / 1048576.0))
}

async fn sampler(sh: Arc<Shared>, out: PathBuf) {
    let mut file = std::fs::File::create(out).unwrap();
    use std::io::Write;
    let extra: Vec<(String, u32)> = sh.a.proc.iter().filter_map(|p| p.split_once('=').and_then(|(n, v)| v.parse().ok().map(|v| (n.to_string(), v)))).collect();
    let mut tick = tokio::time::interval(Duration::from_secs(sh.a.sample_secs));
    loop {
        tick.tick().await;
        let mut servers = Vec::new();
        for (i, c) in sh.clients.iter().enumerate() {
            let r = tokio::time::timeout(Duration::from_secs(3), c.database("admin").run_command(doc! { "tenantStats": 1 })).await;
            match r {
                Ok(Ok(d)) => {
                    let pid = d.get_i64("pid").unwrap_or(0) as u32;
                    let (cpu_s, rss_mb) = read_proc(pid).unwrap_or((0.0, 0.0));
                    let req = d.get_document("requests").cloned().unwrap_or_default();
                    let g = |k: &str| req.get_i64(k).unwrap_or(0);
                    let leases = d.get_document("leases").cloned().unwrap_or_default();
                    let lat = d.get_document("lat").cloned().unwrap_or_default();
                    let arr = |k: &str| -> Vec<i64> { lat.get_array(k).map(|a| a.iter().filter_map(|x| x.as_i64()).collect()).unwrap_or_default() };
                    servers.push(json!({
                        "uri": sh.uris[i], "up": true, "pid": pid, "cpu_s": cpu_s, "rss_mb": rss_mb,
                        "open_readers": d.get_i64("openReaders").unwrap_or(0), "open_writers": d.get_i64("openWriters").unwrap_or(0),
                        "entries": d.get_i64("entries").unwrap_or(0), "leases_held": d.get_i64("leasesHeld").unwrap_or(0),
                        "lease_stats": { "acquired": leases.get_i64("acquired").unwrap_or(0), "takeovers": leases.get_i64("takeovers").unwrap_or(0), "refused": leases.get_i64("refused").unwrap_or(0), "renewals": leases.get_i64("renewals").unwrap_or(0), "lost": leases.get_i64("lost").unwrap_or(0) },
                        "lat": { "put": arr("put"), "get": arr("get") },
                        "req": { "put": g("put"), "copy": g("copy"), "list": g("list"), "get": g("get"), "head": g("head"), "delete": g("delete"), "billed_put_tier": g("billedPutTier"), "billed_get_tier": g("billedGetTier") },
                    }));
                }
                _ => servers.push(json!({ "uri": sh.uris[i], "up": false })),
            }
        }
        let (me_cpu, me_rss) = read_proc(std::process::id()).unwrap_or((0.0, 0.0));
        let procs: serde_json::Map<String, serde_json::Value> = extra.iter().filter_map(|(n, p)| read_proc(*p).map(|(c, r)| (n.clone(), json!({ "cpu_s": c, "rss_mb": r })))).collect();
        let line = json!({
            "t": sh.start.elapsed().as_secs_f64(), "unix_ms": unix_ms(), "servers": servers,
            "loadgen": { "cpu_s": me_cpu, "rss_mb": me_rss }, "procs": procs,
            "ops_total": sh.ops_total.load(Relaxed),
        });
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}

fn hist_json(h: &Hist) -> serde_json::Value {
    let ms = |q: f64| h.value_at_quantile(q) as f64 / 1000.0;
    json!({ "count": h.len(), "mean_ms": h.mean() / 1000.0, "p50_ms": ms(0.5), "p90_ms": ms(0.9), "p99_ms": ms(0.99), "p999_ms": ms(0.999), "max_ms": h.max() as f64 / 1000.0 })
}

fn group_json(g: &Group) -> serde_json::Value {
    let mut all = new_hist();
    for h in g.ops.values() {
        all.add(h).ok();
    }
    let mut steady_all = new_hist();
    for h in g.steady.values() {
        steady_all.add(h).ok();
    }
    json!({
        "activations": g.activations, "ops": g.ops_done, "late_ticks": g.late_ticks,
        "all_ops": hist_json(&all),
        "steady_ops": hist_json(&steady_all),
        "by_type": g.ops.iter().map(|(k, h)| (k.to_string(), hist_json(h))).collect::<serde_json::Map<_, _>>(),
        "steady_by_type": g.steady.iter().map(|(k, h)| (k.to_string(), hist_json(h))).collect::<serde_json::Map<_, _>>(),
        "wake": g.wake.as_ref().map(hist_json),
        "wake_by_type": g.wake_by_kind.iter().map(|(k, h)| (k.to_string(), hist_json(h))).collect::<serde_json::Map<_, _>>(),
        "attempts": g.attempts_hist.iter().map(|(k, v)| (k.to_string(), *v)).collect::<BTreeMap<_, _>>(),
    })
}

async fn verify(sh: &Arc<Shared>) -> serde_json::Value {
    let acks: Vec<(String, Acks)> = std::mem::take(&mut *sh.acks.lock().unwrap()).into_iter().collect();
    let checked = acks.len();
    let acked_inserts: usize = acks.iter().map(|(_, a)| a.inserted.len()).sum();
    let acked_updates: usize = acks.iter().map(|(_, a)| a.updates.len()).sum();
    let missing = Arc::new(AtomicU64::new(0));
    let stale = Arc::new(AtomicU64::new(0));
    let unreadable = Arc::new(AtomicU64::new(0));
    let examples = Arc::new(Mutex::new(Vec::<String>::new()));
    let sh2 = Arc::clone(sh);
    futures::stream::iter(acks)
        .map(|(tenant, a)| {
            let (missing, stale, unreadable, examples, sh) = (Arc::clone(&missing), Arc::clone(&stale), Arc::clone(&unreadable), Arc::clone(&examples), Arc::clone(&sh2));
            async move {
                let mut docs: Option<Vec<Document>> = None;
                let t0 = Instant::now();
                let mut rng = StdRng::from_entropy();
                while t0.elapsed() < Duration::from_secs(90) {
                    let c = &sh.clients[rng.gen_range(0..sh.clients.len())];
                    let r = tokio::time::timeout(Duration::from_secs(10), async {
                        let cur = c.database(&tenant).collection::<Document>("c").find(doc! {}).await?;
                        cur.try_collect::<Vec<Document>>().await
                    })
                    .await;
                    if let Ok(Ok(d)) = r {
                        docs = Some(d);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                let Some(docs) = docs else {
                    unreadable.fetch_add(1, Relaxed);
                    return;
                };
                let ids: std::collections::HashSet<String> = docs.iter().filter_map(|d| d.get("_id").map(|b| match b { Bson::String(s) => s.clone(), other => other.to_string() })).collect();
                for id in &a.inserted {
                    if !ids.contains(id) {
                        missing.fetch_add(1, Relaxed);
                        let mut e = examples.lock().unwrap();
                        if e.len() < 20 {
                            e.push(format!("{tenant}/{id}"));
                        }
                    }
                }
                for (uid, v) in &a.updates {
                    let want: Bson = if uid == "0" { Bson::Int32(0) } else { Bson::String(uid.clone()) };
                    let got = docs.iter().find(|d| d.get("_id") == Some(&want)).and_then(|d| d.get_i64("last").ok());
                    if got.map_or(true, |g| (g as u64) < *v) {
                        stale.fetch_add(1, Relaxed);
                        let mut e = examples.lock().unwrap();
                        if e.len() < 20 {
                            e.push(format!("{tenant}/update {uid}: acked {v}, found {got:?}"));
                        }
                    }
                }
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<()>>()
        .await;
    json!({
        "tenants_checked": checked, "acknowledged_inserts": acked_inserts, "acknowledged_update_docs": acked_updates,
        "missing_inserts": missing.load(Relaxed), "stale_updates": stale.load(Relaxed), "unreadable_tenants": unreadable.load(Relaxed),
        "examples": *examples.lock().unwrap(),
    })
}

async fn run(a: RunArgs) -> Result<()> {
    if a.mode == "rotate" && a.pool < a.active {
        return Err(anyhow!("--pool must be at least --active in rotate mode"));
    }
    let uris: Vec<String> = a.common.uris.split(',').map(str::to_string).collect();
    let mut clients = Vec::new();
    for u in &uris {
        clients.push(make_client(u, a.common.pool_size).await?);
    }
    std::fs::create_dir_all(&a.out)?;
    let start = Instant::now();
    let start_unix = unix_ms();
    let sh = Arc::new(Shared {
        clients,
        uris,
        end: start + Duration::from_secs(a.duration_secs),
        start,
        stats: Mutex::new(Stats::default()),
        acks: Mutex::new(HashMap::new()),
        ver: AtomicU64::new(unix_ms() * 1000),
        act: AtomicU64::new(unix_ms()),
        cursor: AtomicU64::new(0),
        ops_total: AtomicU64::new(0),
        stop: AtomicBool::new(false),
        a,
    });
    let samp = tokio::spawn(sampler(Arc::clone(&sh), sh.a.out.join("samples.jsonl")));
    let mut tasks = Vec::new();
    let a = &sh.a;
    match a.mode.as_str() {
        "rotate" => {
            for k in 0..a.active {
                let sh = Arc::clone(&sh);
                tasks.push(tokio::spawn(async move {
                    // Staggered first lifetimes spread the rotation evenly over the lifetime.
                    let mut life = Duration::from_secs_f64(sh.a.lifetime_secs as f64 * (k + 1) as f64 / sh.a.active as f64);
                    while !sh.is_over() {
                        let idx = sh.a.start + sh.cursor.fetch_add(1, Relaxed) % sh.a.pool;
                        activation(Arc::clone(&sh), idx as i64, life, sh.a.rate, None, false).await;
                        life = Duration::from_secs(sh.a.lifetime_secs);
                    }
                }));
            }
        }
        "burst" => {
            for k in 0..a.active {
                let sh = Arc::clone(&sh);
                tasks.push(tokio::spawn(async move {
                    let offset = Duration::from_secs_f64(sh.a.ramp_secs as f64 * k as f64 / sh.a.active as f64);
                    tokio::time::sleep_until((sh.start + offset).into()).await;
                    activation(Arc::clone(&sh), (sh.a.start + k % sh.a.pool) as i64, Duration::from_secs(sh.a.hold_secs), sh.a.rate, None, true).await;
                }));
            }
        }
        m => return Err(anyhow!("unknown mode {m}")),
    }
    if a.hot_tenant >= 0 {
        for w in 0..a.hot_workers {
            let sh = Arc::clone(&sh);
            tasks.push(tokio::spawn(async move {
                activation(Arc::clone(&sh), sh.a.hot_tenant, Duration::from_secs(sh.a.duration_secs), sh.a.hot_rate / sh.a.hot_workers as f64, Some(w), true).await;
            }));
        }
    }
    for t in tasks {
        let _ = t.await;
    }
    let run_secs = start.elapsed().as_secs_f64();
    let end_unix = unix_ms();
    sh.stop.store(true, Relaxed);
    // Let the sampler take one last sample, then stop it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    samp.abort();
    let verification = if sh.a.verify { Some(verify(&sh).await) } else { None };
    let stats = sh.stats.lock().unwrap();
    let a = &sh.a;
    let out = json!({
        "scenario": a.name,
        "config": { "mode": a.mode, "uris": a.common.uris, "start": a.start, "pool": a.pool, "active": a.active, "lifetime_secs": a.lifetime_secs, "ramp_secs": a.ramp_secs, "hold_secs": a.hold_secs, "rate": a.rate, "read_pct": a.read_pct, "duration_secs": a.duration_secs, "hot_tenant": a.hot_tenant, "hot_rate": a.hot_rate, "hot_workers": a.hot_workers, "pool_size": a.common.pool_size, "max_attempts": a.max_attempts },
        "start_unix_ms": start_unix, "end_unix_ms": end_unix, "seconds": run_secs,
        "normal": group_json(&stats.groups[0]),
        "hot": group_json(&stats.groups[1]),
        "errors_by_code": stats.errors, "failures_by_code": stats.failures,
        "retries": stats.retries, "acknowledged_writes": stats.acked_writes, "read_misses": stats.misses,
        "ops_per_sec": sh.ops_total.load(Relaxed) as f64 / run_secs,
        "verification": verification,
    });
    std::fs::write(a.out.join("summary.json"), serde_json::to_string_pretty(&out)?)?;
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let threads = match &cli.cmd {
        Cmd::Load(a) => a.common.worker_threads,
        Cmd::Run(a) => a.common.worker_threads,
    };
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build()?;
    rt.block_on(async {
        match cli.cmd {
            Cmd::Load(a) => load(a).await,
            Cmd::Run(a) => run(*a).await,
        }
    })
}
