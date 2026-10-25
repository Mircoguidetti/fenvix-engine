mod counting;
mod experiment;
mod lifecycle;
mod lifecycle_exp;

use anyhow::{Context, Result};
use clap::Parser;
use counting::{CountingStore, Snapshot};
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::seq::index::sample;
use rand::{distributions::Alphanumeric, Rng};
use serde::Serialize;
use slatedb::Db;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DOCS_PER_TENANT: usize = 200;
const DOC_BYTES: usize = 1024;
const COLD_READS: usize = 100;
const WARM_READS: usize = 1000;

// AWS S3 Standard, us-east-1 list prices.
const STORAGE_USD_PER_GB_MONTH: f64 = 0.023;
const PUT_TIER_USD_PER_1K: f64 = 0.005;
const GET_TIER_USD_PER_1K: f64 = 0.0004;
const ATLAS_FLEX_USD_PER_TENANT_MONTH: f64 = 8.0;
const MODEL_TENANTS: f64 = 1_000_000.0;
// AWS bills storage in binary GB (2^30 bytes).
const GB: f64 = 1024.0 * 1024.0 * 1024.0;

#[derive(Parser, Debug)]
struct Args {
    /// Number of tenants (one slatedb Db each).
    #[arg(long, default_value_t = 1000)]
    tenants: usize,
    /// Tenants loaded concurrently in the load phase.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    /// Length of each idle-cost observation window, in seconds.
    #[arg(long, default_value_t = 60)]
    idle_secs: u64,
    /// Run an experiment instead of the default benchmark. Supported: `wake-variants`, `lifecycle`.
    /// (`crash-child` is used internally by `lifecycle`.)
    #[arg(long)]
    experiment: Option<String>,
    /// `crash-child` only: tenant to write to.
    #[arg(long)]
    tenant: Option<usize>,
    /// `crash-child` only: how many of the 10 writes to await as durable before reporting DONE.
    #[arg(long)]
    acks: Option<usize>,
    /// With `--experiment wake-variants`: comma-separated variant names to run (default: all).
    #[arg(long)]
    only: Option<String>,
    /// Where to write this run's report (markdown) and raw data (json).
    #[arg(long, default_value = "runs")]
    out_dir: String,
}

#[derive(Serialize)]
struct Latency {
    p50_ms: f64,
    p99_ms: f64,
    min_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

#[derive(Serialize)]
struct Report {
    tenants: usize,
    concurrency: usize,
    load_secs: f64,
    load_requests: Snapshot,
    storage_objects: u64,
    storage_bytes: u64,
    avg_bytes_per_tenant: f64,
    avg_objects_per_tenant: f64,
    /// Total bytes per slatedb directory (wal, compacted, manifest, compactions).
    bytes_by_dir: BTreeMap<String, u64>,
    cold_open_get: Latency,
    cold_open_get_requests: Snapshot,
    cold_close_requests: Snapshot,
    warm_get: Latency,
    warm_requests: Snapshot,
    idle_secs: u64,
    idle_closed_requests: Snapshot,
    idle_open_requests: Snapshot,
}

fn tenant_path(id: usize) -> String {
    format!("tenants/{id}")
}

fn make_doc(tenant: usize, i: usize, rng: &mut impl Rng) -> Vec<u8> {
    let head = format!(
        r#"{{"id":"doc:{i}","tenant":{tenant},"type":"note","version":1,"tags":["bench","json"],"body":""#
    );
    let pad = DOC_BYTES.saturating_sub(head.len() + 2);
    let body: String = rng.sample_iter(&Alphanumeric).take(pad).map(char::from).collect();
    let doc = format!("{head}{body}\"}}");
    debug_assert!(serde_json::from_str::<serde_json::Value>(&doc).is_ok());
    doc.into_bytes()
}

fn latency(mut samples: Vec<Duration>) -> Latency {
    samples.sort();
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    // Nearest-rank percentile.
    let pct = |p: f64| ms(samples[((p * samples.len() as f64).ceil() as usize).max(1) - 1]);
    Latency {
        p50_ms: pct(0.50),
        p99_ms: pct(0.99),
        min_ms: ms(samples[0]),
        max_ms: ms(*samples.last().unwrap()),
        mean_ms: samples.iter().map(|d| ms(*d)).sum::<f64>() / samples.len() as f64,
    }
}

async fn load_tenant(store: Arc<dyn ObjectStore>, id: usize) -> Result<()> {
    let mut rng = rand::thread_rng();
    let db = Db::open(tenant_path(id), store).await?;
    for i in 0..DOCS_PER_TENANT {
        db.put(format!("doc:{i}"), make_doc(id, i, &mut rng)).await?;
    }
    db.flush().await?;
    db.close().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let n = args.tenants;
    anyhow::ensure!(n >= 2, "need at least 2 tenants");

    let s3: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name("bench")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .build()?,
    );
    let counting = Arc::new(CountingStore::new(Arc::clone(&s3)));
    if let Some(name) = args.experiment.as_deref() {
        return match name {
            "wake-variants" => experiment::run(&args, s3, counting).await,
            "lifecycle" => lifecycle_exp::run(&args, s3).await,
            "crash-child" => lifecycle_exp::crash_child(&args, s3).await,
            "probe-wal" => lifecycle_exp::probe_wal(&args, s3).await,
            other => Err(anyhow::anyhow!("unknown experiment {other:?}")),
        };
    }
    let counters = Arc::clone(&counting.counters);
    let store: Arc<dyn ObjectStore> = counting;

    // Start from an empty prefix so storage numbers describe this run only.
    let prefix = Path::from("tenants");
    let stale = s3.list(Some(&prefix)).map_ok(|m| m.location).boxed();
    let removed = s3.delete_stream(stale).try_collect::<Vec<_>>().await?.len();
    eprintln!("cleaned {removed} objects from a previous run");

    // ---- Load phase -------------------------------------------------------
    eprintln!("load: {n} tenants x {DOCS_PER_TENANT} docs, concurrency {}", args.concurrency);
    let before = counters.snapshot();
    let started = Instant::now();
    futures::stream::iter(0..n)
        .map(|id| {
            let store = Arc::clone(&store);
            async move { load_tenant(store, id).await.with_context(|| format!("tenant {id}")) }
        })
        .buffer_unordered(args.concurrency)
        .try_collect::<()>()
        .await?;
    let load_secs = started.elapsed().as_secs_f64();
    let load_requests = counters.snapshot().since(&before);
    eprintln!("load: {load_secs:.1}s, {} requests", load_requests.total());

    // ---- Storage (listed on the raw store so it is not counted) -----------
    let mut per_tenant: HashMap<String, (u64, u64)> = HashMap::new();
    let mut bytes_by_dir: BTreeMap<String, u64> = BTreeMap::new();
    let mut objects = s3.list(Some(&prefix));
    while let Some(meta) = objects.try_next().await? {
        let tenant = meta.location.parts().nth(1).context("bad path")?.as_ref().to_string();
        let dir = meta.location.parts().nth(2).context("bad path")?.as_ref().to_string();
        *bytes_by_dir.entry(dir).or_default() += meta.size;
        let entry = per_tenant.entry(tenant).or_default();
        entry.0 += 1;
        entry.1 += meta.size;
    }
    anyhow::ensure!(per_tenant.len() == n, "expected {n} tenants, found {}", per_tenant.len());
    let storage_objects: u64 = per_tenant.values().map(|v| v.0).sum();
    let storage_bytes: u64 = per_tenant.values().map(|v| v.1).sum();
    let avg_bytes_per_tenant = storage_bytes as f64 / n as f64;
    let avg_objects_per_tenant = storage_objects as f64 / n as f64;
    eprintln!("storage: {storage_objects} objects, {storage_bytes} bytes");

    // ---- Cold reads: open a never-reopened tenant + one get ---------------
    let mut rng = rand::thread_rng();
    // Reserve one tenant for the warm phase; cold tenants are distinct when possible.
    let warm_id = rng.gen_range(0..n);
    let candidates: Vec<usize> = (0..n).filter(|&id| id != warm_id).collect();
    let cold_ids: Vec<usize> = if candidates.len() >= COLD_READS {
        sample(&mut rng, candidates.len(), COLD_READS).into_iter().map(|i| candidates[i]).collect()
    } else {
        (0..COLD_READS).map(|_| candidates[rng.gen_range(0..candidates.len())]).collect()
    };
    let mut cold_samples = Vec::with_capacity(COLD_READS);
    let mut cold_open_get_requests = Snapshot::default();
    let mut cold_close_requests = Snapshot::default();
    for &id in &cold_ids {
        let key = format!("doc:{}", rng.gen_range(0..DOCS_PER_TENANT));
        let before = counters.snapshot();
        let t = Instant::now();
        let db = Db::open(tenant_path(id), Arc::clone(&store)).await?;
        let value = db.get(&key).await?;
        let elapsed = t.elapsed();
        let mid = counters.snapshot();
        anyhow::ensure!(value.is_some(), "tenant {id} missing {key}");
        db.close().await?;
        let after = counters.snapshot();
        cold_samples.push(elapsed);
        cold_open_get_requests = add(&cold_open_get_requests, &mid.since(&before));
        cold_close_requests = add(&cold_close_requests, &after.since(&mid));
    }
    let cold = latency(cold_samples);
    eprintln!("cold: p50 {:.2}ms p99 {:.2}ms", cold.p50_ms, cold.p99_ms);

    // ---- Idle cost of closed tenants --------------------------------------
    // Nothing is open now: every tenant is a set of objects at rest.
    eprintln!("idle (all tenants closed): observing {}s", args.idle_secs);
    let before = counters.snapshot();
    tokio::time::sleep(Duration::from_secs(args.idle_secs)).await;
    let idle_closed_requests = counters.snapshot().since(&before);

    // ---- Warm reads --------------------------------------------------------
    let db = Db::open(tenant_path(warm_id), Arc::clone(&store)).await?;
    let mut warm_samples = Vec::with_capacity(WARM_READS);
    let before = counters.snapshot();
    for _ in 0..WARM_READS {
        let key = format!("doc:{}", rng.gen_range(0..DOCS_PER_TENANT));
        let t = Instant::now();
        let value = db.get(&key).await?;
        warm_samples.push(t.elapsed());
        anyhow::ensure!(value.is_some(), "warm tenant missing {key}");
    }
    let warm_requests = counters.snapshot().since(&before);
    let warm = latency(warm_samples);
    eprintln!("warm: p50 {:.3}ms p99 {:.3}ms", warm.p50_ms, warm.p99_ms);

    // ---- Idle cost of an open (but unused) tenant, for contrast -----------
    eprintln!("idle (one tenant open, no traffic): observing {}s", args.idle_secs);
    let before = counters.snapshot();
    tokio::time::sleep(Duration::from_secs(args.idle_secs)).await;
    let idle_open_requests = counters.snapshot().since(&before);
    db.close().await?;

    let report = Report {
        tenants: n,
        concurrency: args.concurrency,
        load_secs,
        load_requests,
        storage_objects,
        storage_bytes,
        avg_bytes_per_tenant,
        avg_objects_per_tenant,
        bytes_by_dir,
        cold_open_get: cold,
        cold_open_get_requests,
        cold_close_requests,
        warm_get: warm,
        warm_requests,
        idle_secs: args.idle_secs,
        idle_closed_requests,
        idle_open_requests,
    };

    std::fs::create_dir_all(&args.out_dir)?;
    let base = format!("{}/n{n}", args.out_dir);
    std::fs::write(format!("{base}.json"), serde_json::to_string_pretty(&report)?)?;
    let md = render(&report);
    std::fs::write(format!("{base}.md"), &md)?;
    println!("{md}");
    Ok(())
}

fn add(a: &Snapshot, b: &Snapshot) -> Snapshot {
    Snapshot {
        put: a.put + b.put,
        multipart_create: a.multipart_create + b.multipart_create,
        multipart_part: a.multipart_part + b.multipart_part,
        multipart_complete: a.multipart_complete + b.multipart_complete,
        copy: a.copy + b.copy,
        get: a.get + b.get,
        get_range: a.get_range + b.get_range,
        head: a.head + b.head,
        list: a.list + b.list,
        delete: a.delete + b.delete,
    }
}

fn request_cost(s: &Snapshot) -> f64 {
    s.billed_put_tier() as f64 / 1000.0 * PUT_TIER_USD_PER_1K
        + s.billed_get_tier() as f64 / 1000.0 * GET_TIER_USD_PER_1K
}

fn per(s: &Snapshot, n: usize) -> String {
    let n = n as f64;
    format!(
        "PUT-type {:.2} (put {:.2}, multipart {:.2}, copy {:.2}) · GET-type {:.2} (get {:.2}, get_range {:.2}, head {:.2}, list {:.2}) · delete {:.2}",
        s.put_type() as f64 / n,
        s.put as f64 / n,
        (s.multipart_create + s.multipart_part + s.multipart_complete) as f64 / n,
        s.copy as f64 / n,
        s.get_type() as f64 / n,
        s.get as f64 / n,
        s.get_range as f64 / n,
        s.head as f64 / n,
        s.list as f64 / n,
        s.delete as f64 / n,
    )
}

fn usd(x: f64) -> String {
    if x >= 1.0 {
        let whole = format!("{:.0}", x.trunc());
        let mut out = String::new();
        for (i, c) in whole.chars().enumerate() {
            if i > 0 && (whole.len() - i) % 3 == 0 {
                out.push(',');
            }
            out.push(c);
        }
        format!("${out}.{:02}", ((x.fract() * 100.0).round() as u64).min(99))
    } else {
        format!("${x:.8}")
    }
}

fn render(r: &Report) -> String {
    let n = r.tenants;
    let cold_n = COLD_READS;
    let wake = r.cold_open_get_requests;
    let wake_cost = request_cost(&wake) / cold_n as f64;
    let close_cost = request_cost(&r.cold_close_requests) / cold_n as f64;
    let storage_month = r.avg_bytes_per_tenant * MODEL_TENANTS / GB * STORAGE_USD_PER_GB_MONTH;
    let wal_bytes = r.bytes_by_dir.get("wal").copied().unwrap_or(0) as f64;
    let no_wal_per_tenant = (r.storage_bytes as f64 - wal_bytes) / n as f64;
    let storage_no_wal_month = no_wal_per_tenant * MODEL_TENANTS / GB * STORAGE_USD_PER_GB_MONTH;
    let load_cost_per_tenant = request_cost(&r.load_requests) / n as f64;
    let atlas = ATLAS_FLEX_USD_PER_TENANT_MONTH * MODEL_TENANTS;

    let mut s = String::new();
    let _ = writeln!(s, "### Run: {n} tenants\n");
    let _ = writeln!(s, "| Measurement | Value |\n|---|---|");
    let _ = writeln!(s, "| Load time ({n} tenants × {DOCS_PER_TENANT} docs, concurrency {}) | {:.1} s ({:.1} ms/tenant wall) |", r.concurrency, r.load_secs, r.load_secs * 1000.0 / n as f64);
    let _ = writeln!(s, "| Load requests total | PUT-type {} · GET-type {} · delete {} |", r.load_requests.put_type(), r.load_requests.get_type(), r.load_requests.delete);
    let _ = writeln!(s, "| Load requests per tenant | {} |", per(&r.load_requests, n));
    let _ = writeln!(s, "| Storage total | {} objects, {} bytes ({:.2} MiB) |", r.storage_objects, r.storage_bytes, r.storage_bytes as f64 / 1048576.0);
    let _ = writeln!(s, "| Storage per tenant (avg) | {:.0} bytes, {:.2} objects |", r.avg_bytes_per_tenant, r.avg_objects_per_tenant);
    let by_dir: Vec<String> = r.bytes_by_dir.iter().map(|(d, b)| format!("{d}/ {:.0} B", *b as f64 / n as f64)).collect();
    let _ = writeln!(s, "| Storage per tenant by directory (avg) | {} |", by_dir.join(" · "));
    let _ = writeln!(s, "| Cold open+get latency ({cold_n} samples) | p50 {:.2} ms · p99 {:.2} ms (min {:.2}, mean {:.2}, max {:.2}) |", r.cold_open_get.p50_ms, r.cold_open_get.p99_ms, r.cold_open_get.min_ms, r.cold_open_get.mean_ms, r.cold_open_get.max_ms);
    let _ = writeln!(s, "| Cold open+get requests per wake | {} |", per(&r.cold_open_get_requests, cold_n));
    let _ = writeln!(s, "| Close-after-wake requests per wake | {} |", per(&r.cold_close_requests, cold_n));
    let _ = writeln!(s, "| Warm get latency ({WARM_READS} samples, one open tenant) | p50 {:.3} ms · p99 {:.3} ms (min {:.3}, mean {:.3}, max {:.3}) |", r.warm_get.p50_ms, r.warm_get.p99_ms, r.warm_get.min_ms, r.warm_get.mean_ms, r.warm_get.max_ms);
    let _ = writeln!(s, "| Requests during {WARM_READS} warm gets | PUT-type {} · GET-type {} (get {}, get_range {}, head {}, list {}) |", r.warm_requests.put_type(), r.warm_requests.get_type(), r.warm_requests.get, r.warm_requests.get_range, r.warm_requests.head, r.warm_requests.list);
    let _ = writeln!(s, "| Idle **closed** tenants, {} s | **{} requests** (PUT-type {}, GET-type {}, delete {}) |", r.idle_secs, r.idle_closed_requests.total(), r.idle_closed_requests.put_type(), r.idle_closed_requests.get_type(), r.idle_closed_requests.delete);
    let _ = writeln!(s, "| Idle **open** tenant (no traffic), {} s | {} requests (PUT-type {}, GET-type {}: get {}, get_range {}, head {}, list {}; delete {}) |", r.idle_secs, r.idle_open_requests.total(), r.idle_open_requests.put_type(), r.idle_open_requests.get_type(), r.idle_open_requests.get, r.idle_open_requests.get_range, r.idle_open_requests.head, r.idle_open_requests.list, r.idle_open_requests.delete);

    let open_idle_month = request_cost(&r.idle_open_requests) * (30.0 * 86400.0 / r.idle_secs as f64);
    let _ = writeln!(s, "\n#### Cost model (S3 Standard us-east-1 list prices)\n");
    let _ = writeln!(s, "| Item | Basis | Cost |\n|---|---|---|");
    let _ = writeln!(s, "| Storage, 1,000,000 idle tenants / month | {:.0} B/tenant × 1e6 = {:.2} GB × $0.023 | **{}/month** |", r.avg_bytes_per_tenant, r.avg_bytes_per_tenant * MODEL_TENANTS / GB, usd(storage_month));
    let _ = writeln!(s, "| Storage, 1,000,000 tenants, obsolete WAL removed by GC | {:.0} B/tenant × 1e6 = {:.2} GB × $0.023 | {}/month |", no_wal_per_tenant, no_wal_per_tenant * MODEL_TENANTS / GB, usd(storage_no_wal_month));
    let _ = writeln!(s, "| Idle request cost, closed tenant | {} requests in {} s | {}/month |", r.idle_closed_requests.total(), r.idle_secs, usd(request_cost(&r.idle_closed_requests) * (30.0 * 86400.0 / r.idle_secs as f64)));
    let _ = writeln!(s, "| Wake one tenant (cold open + 1 get) | {:.2} PUT/LIST-tier + {:.2} GET-tier requests | **{} per wake** |", wake.billed_put_tier() as f64 / cold_n as f64, wake.billed_get_tier() as f64 / cold_n as f64, usd(wake_cost));
    let _ = writeln!(s, "| Close after wake (flush on close) | {:.2} PUT/LIST-tier + {:.2} GET-tier requests | {} per close |", r.cold_close_requests.billed_put_tier() as f64 / cold_n as f64, r.cold_close_requests.billed_get_tier() as f64 / cold_n as f64, usd(close_cost));
    let _ = writeln!(s, "| Initial load of one tenant (200 docs) | {:.2} PUT/LIST-tier + {:.2} GET-tier requests | {} per tenant |", r.load_requests.billed_put_tier() as f64 / n as f64, r.load_requests.billed_get_tier() as f64 / n as f64, usd(load_cost_per_tenant));
    let _ = writeln!(s, "| One tenant kept **open** but idle | extrapolated from {} requests / {} s | {}/month per tenant ({}/month for 1e6) |", r.idle_open_requests.total(), r.idle_secs, usd(open_idle_month), usd(open_idle_month * MODEL_TENANTS));
    let _ = writeln!(s, "| MongoDB Atlas Flex, 1,000,000 tenants | $8 × 1e6 | **{}/month** |", usd(atlas));
    let _ = writeln!(s, "| Ratio Atlas / object-storage idle | | {:.0}× |", atlas / storage_month);
    let _ = writeln!(s, "| Wakes per month that equal Atlas cost | ($8e6 − storage) / (wake + close) | {:.3e} wakes/month ({:.0} per tenant) |", (atlas - storage_month) / (wake_cost + close_cost), (atlas - storage_month) / (wake_cost + close_cost) / MODEL_TENANTS);
    s
}
