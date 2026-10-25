//! `--experiment wake-variants`: where do wake requests go, and which slatedb
//! configurations reduce them? Everything here is opt-in; the default benchmark
//! in `main.rs` is untouched.

use super::{latency, load_tenant, make_doc, request_cost, tenant_path, usd, Args, Latency};
use crate::counting::{CountingStore, Snapshot, TraceEvent};
use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::seq::index::sample;
use rand::Rng;
use serde::Serialize;
use slatedb::admin::Admin;
use slatedb::config::{
    FlushOptions, FlushType, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
    DbReaderOptions, Settings,
};
use slatedb::{Db, DbReader, DbReaderMode};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

const COLD_WAKES: usize = 100;
const TRACED_WAKES: usize = 10;
const DOCS: usize = super::DOCS_PER_TENANT;
/// Longest polling interval used in the "tuned" variants. slatedb's own tests use one hour.
const LONG_POLL: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, PartialEq)]
enum Load {
    /// Existing load phase: put x200, `flush()` (WAL), `close()` (memtable flush).
    Existing,
    /// put x200, `flush_with_options(MemTable)`, `close()`, then an offline GC pass.
    MemtableFlushThenGc,
}

#[derive(Clone, Copy)]
enum Wake {
    Writer { gc_in_db: bool, long_poll: bool, compactor: bool },
    Reader { mode: DbReaderMode, skip_wal: bool, long_poll: bool },
}

struct Variant {
    name: &'static str,
    desc: &'static str,
    load: Load,
    wake: Wake,
}

fn variants() -> Vec<Variant> {
    vec![
        Variant { name: "V0", desc: "writer `Db::open` + get, default settings (current behaviour)", load: Load::Existing,
            wake: Wake::Writer { gc_in_db: true, long_poll: false, compactor: true } },
        Variant { name: "V1a", desc: "`DbReader` ManagedCheckpoint (API default) + get", load: Load::Existing,
            wake: Wake::Reader { mode: DbReaderMode::ManagedCheckpoint, skip_wal: false, long_poll: false } },
        Variant { name: "V1b", desc: "`DbReader` FollowLatest (no object-store writes) + get", load: Load::Existing,
            wake: Wake::Reader { mode: DbReaderMode::FollowLatest, skip_wal: false, long_poll: false } },
        Variant { name: "V1c", desc: "`DbReader` FollowLatest + skip_wal_replay + 1 h poll + get", load: Load::Existing,
            wake: Wake::Reader { mode: DbReaderMode::FollowLatest, skip_wal: true, long_poll: true } },
        Variant { name: "V2", desc: "memtable-flush load + offline GC (min_age 0); writer open with in-Db GC off", load: Load::MemtableFlushThenGc,
            wake: Wake::Writer { gc_in_db: false, long_poll: false, compactor: true } },
        Variant { name: "V3", desc: "V2 + 1 h manifest/compactor poll intervals", load: Load::MemtableFlushThenGc,
            wake: Wake::Writer { gc_in_db: false, long_poll: true, compactor: true } },
        Variant { name: "V4", desc: "V3 + in-Db compactor off (compaction would have to run offline)", load: Load::MemtableFlushThenGc,
            wake: Wake::Writer { gc_in_db: false, long_poll: true, compactor: false } },
    ]
}

fn writer_settings(gc_in_db: bool, long_poll: bool, compactor: bool) -> Settings {
    let mut s = Settings::default();
    if !compactor {
        s.compactor_options = None;
    }
    if !gc_in_db {
        s.garbage_collector_options = None;
    }
    if long_poll {
        s.manifest_poll_interval = LONG_POLL;
        if let Some(c) = s.compactor_options.as_mut() {
            c.poll_interval = LONG_POLL;
        }
    }
    s
}

fn reader_options(skip_wal: bool, long_poll: bool) -> DbReaderOptions {
    let mut o = DbReaderOptions::default();
    o.skip_wal_replay = skip_wal;
    if long_poll {
        o.manifest_poll_interval = LONG_POLL;
    }
    o
}

/// An open tenant, either as writer or reader.
enum Handle {
    Writer(Db),
    Reader(DbReader),
}

impl Handle {
    async fn open(wake: Wake, store: Arc<dyn ObjectStore>, id: usize) -> Result<Self> {
        Ok(match wake {
            Wake::Writer { gc_in_db, long_poll, compactor } => Handle::Writer(
                Db::builder(tenant_path(id), store)
                    .with_settings(writer_settings(gc_in_db, long_poll, compactor))
                    .build()
                    .await?,
            ),
            Wake::Reader { mode, skip_wal, long_poll } => Handle::Reader(
                DbReader::builder(tenant_path(id), store)
                    .with_reader_mode(mode)
                    .with_options(reader_options(skip_wal, long_poll))
                    .build()
                    .await?,
            ),
        })
    }

    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>> {
        Ok(match self {
            Handle::Writer(db) => db.get(key).await?,
            Handle::Reader(r) => r.get(key).await?,
        })
    }

    async fn close(&self) -> Result<()> {
        match self {
            Handle::Writer(db) => db.close().await?,
            Handle::Reader(r) => r.close().await?,
        }
        Ok(())
    }
}

async fn load_variant_tenant(store: Arc<dyn ObjectStore>, v: &Variant, id: usize) -> Result<()> {
    match v.load {
        Load::Existing => load_tenant(store, id).await,
        Load::MemtableFlushThenGc => {
            let Wake::Writer { gc_in_db, long_poll, compactor } = v.wake else { unreachable!() };
            let mut rng = rand::thread_rng();
            let db = Db::builder(tenant_path(id), store)
                .with_settings(writer_settings(gc_in_db, long_poll, compactor))
                .build()
                .await?;
            for i in 0..DOCS {
                db.put(format!("doc:{i}"), make_doc(id, i, &mut rng)).await?;
            }
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
            db.close().await?;
            Ok(())
        }
    }
}

/// One offline GC pass over a closed tenant. min_age 0 is only safe because no
/// writer is open: min_age exists to protect uploads that are still in flight.
async fn offline_gc(store: Arc<dyn ObjectStore>, id: usize) -> Result<()> {
    let dir = GarbageCollectorDirectoryOptions { interval: None, min_age: Duration::ZERO, dry_run: false };
    let opts = GarbageCollectorOptions {
        manifest_options: Some(dir),
        wal_options: Some(dir),
        compacted_options: Some(dir),
        compactions_options: Some(dir),
        ..GarbageCollectorOptions::default()
    };
    let admin = Admin::builder(tenant_path(id), store).build();
    // Checkpoints pin old manifests (and through them old WAL files). Measured: a cleanly
    // closed tenant has none, so this is only a guard and costs one manifest read.
    let checkpoints = admin.list_checkpoints(None).await?;
    if id == 0 {
        eprintln!("   tenant 0 has {} checkpoint(s) before GC", checkpoints.len());
    }
    for cp in checkpoints {
        admin.delete_checkpoint(cp.id).await?;
    }
    admin.run_gc_once(opts).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Trace analysis
// ---------------------------------------------------------------------------

/// Group a call by what it touches: manifest / WAL / SST / other.
fn purpose(ev: &TraceEvent) -> &'static str {
    // Paths look like tenants/{id}/{dir}/{file} (LIST prefixes may stop at {dir}).
    match ev.path.split('/').nth(2).unwrap_or("") {
        "manifest" => "manifest",
        "wal" => "WAL",
        "compacted" => "SST",
        "compactions" => "other (compactions/)",
        _ => "other",
    }
}

fn rel(path: &str) -> String {
    path.splitn(3, '/').nth(2).map(str::to_string).unwrap_or_else(|| path.to_string())
}

#[derive(Serialize, Clone, Copy, Default)]
pub(crate) struct Shape {
    pub(crate) total: usize,
    /// Calls that no other traced call overlapped with at any point.
    pub(crate) strictly_sequential: usize,
    /// Calls that overlapped at least one other call.
    pub(crate) parallel: usize,
    /// Round trips on the critical path: calls whose time intervals overlap are merged
    /// into one step, so this counts the sequential steps (what latency scales with).
    pub(crate) steps: usize,
}

pub(crate) fn shape(events: &[TraceEvent]) -> Shape {
    let mut iv: Vec<(u64, u64)> = events.iter().map(|e| (e.start_us, e.start_us + e.dur_us)).collect();
    iv.sort();
    let mut steps = 0;
    let mut end = 0u64;
    for (s, e) in iv {
        if steps == 0 || s >= end {
            steps += 1;
            end = e;
        } else {
            end = end.max(e);
        }
    }
    let parallel = events.iter().filter(|e| e.overlapped).count();
    Shape { total: events.len(), strictly_sequential: events.len() - parallel, parallel, steps }
}

fn mean_shape(shapes: &[Shape]) -> [f64; 4] {
    let n = shapes.len() as f64;
    let m = |f: fn(&Shape) -> usize| shapes.iter().map(|s| f(s) as f64).sum::<f64>() / n;
    [m(|s| s.total), m(|s| s.strictly_sequential), m(|s| s.parallel), m(|s| s.steps)]
}

fn trace_table(open: &[TraceEvent], get: &[TraceEvent], offset_us: u64) -> String {
    trace_table_labeled(open, get, offset_us, "open")
}

fn trace_table_labeled(open: &[TraceEvent], get: &[TraceEvent], offset_us: u64, first: &str) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "| # | phase | purpose | op | object (relative to tenants/{{id}}/) | bytes | start ms | dur ms | overlapped |");
    let _ = writeln!(s, "|---|---|---|---|---|---|---|---|---|");
    let mut n = 0;
    for (phase, evs, off) in [(first, open, 0u64), ("get", get, offset_us)] {
        for e in evs {
            n += 1;
            let _ = writeln!(
                s, "| {n} | {phase} | {} | {} | `{}` | {} | {:.2} | {:.2} | {} |",
                purpose(e), e.op, rel(&e.path), e.bytes,
                (e.start_us + off) as f64 / 1000.0, e.dur_us as f64 / 1000.0,
                if e.overlapped { "yes" } else { "no" },
            );
        }
    }
    s
}

fn group_table(events: &[&TraceEvent]) -> String {
    // purpose -> op -> (count, total ms)
    let mut g: BTreeMap<&str, BTreeMap<String, (usize, f64)>> = BTreeMap::new();
    for e in events {
        let c = g.entry(purpose(e)).or_default().entry(e.op.clone()).or_default();
        c.0 += 1;
        c.1 += e.dur_us as f64 / 1000.0;
    }
    let mut s = String::from("| purpose | operations (count, summed duration) | total calls |\n|---|---|---|\n");
    for (p, ops) in &g {
        let list: Vec<String> = ops.iter().map(|(o, (c, ms))| format!("{o} ×{c} ({ms:.1} ms)")).collect();
        let _ = writeln!(s, "| {p} | {} | {} |", list.join(", "), ops.values().map(|v| v.0).sum::<usize>());
    }
    s
}

// ---------------------------------------------------------------------------
// Per-variant run
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct VariantResult {
    name: &'static str,
    desc: &'static str,
    load_secs: f64,
    gc_requests_total: Snapshot,
    storage_bytes_per_tenant_raw: f64,
    storage_bytes_per_tenant_after_cleanup: Option<f64>,
    storage_objects_per_tenant_final: f64,
    bytes_by_dir_final: BTreeMap<String, f64>,
    wake_latency: Latency,
    wake_requests_per_wake: Snapshot,
    close_requests_per_wake: Snapshot,
    wake_shape_mean: [f64; 4],
    close_shape_mean: [f64; 4],
    idle_secs: u64,
    idle_open_requests: Snapshot,
    #[serde(skip)]
    first_trace: (Vec<TraceEvent>, Vec<TraceEvent>, Vec<TraceEvent>, u64),
    #[serde(skip)]
    idle_events: Vec<TraceEvent>,
}

pub(crate) async fn storage(s3: &Arc<dyn ObjectStore>, n: usize) -> Result<(f64, f64, BTreeMap<String, f64>)> {
    let prefix = Path::from("tenants");
    let mut bytes = 0u64;
    let mut objects = 0u64;
    let mut by_dir: BTreeMap<String, u64> = BTreeMap::new();
    let mut it = s3.list(Some(&prefix));
    while let Some(m) = it.try_next().await? {
        bytes += m.size;
        objects += 1;
        let dir = m.location.parts().nth(2).context("bad path")?.as_ref().to_string();
        *by_dir.entry(dir).or_default() += m.size;
    }
    let nf = n as f64;
    Ok((bytes as f64 / nf, objects as f64 / nf, by_dir.into_iter().map(|(k, v)| (k, v as f64 / nf)).collect()))
}

fn per_f(total: &Snapshot, n: usize) -> (f64, f64) {
    (total.billed_put_tier() as f64 / n as f64, total.billed_get_tier() as f64 / n as f64)
}

fn sum(a: &Snapshot, b: &Snapshot) -> Snapshot {
    super::add(a, b)
}

async fn run_variant(
    v: &Variant,
    args: &Args,
    s3: &Arc<dyn ObjectStore>,
    counting: &Arc<CountingStore>,
) -> Result<VariantResult> {
    let n = args.tenants;
    let counters = Arc::clone(&counting.counters);
    let trace = Arc::clone(&counting.trace);
    let store: Arc<dyn ObjectStore> = counting.clone();
    eprintln!("== {} — {}", v.name, v.desc);

    // Fresh tenants for every variant.
    let prefix = Path::from("tenants");
    let stale = s3.list(Some(&prefix)).map_ok(|m| m.location).boxed();
    s3.delete_stream(stale).try_collect::<Vec<_>>().await?;

    let started = Instant::now();
    futures::stream::iter(0..n)
        .map(|id| {
            let store = Arc::clone(&store);
            async move { load_variant_tenant(store, v, id).await.with_context(|| format!("load tenant {id}")) }
        })
        .buffer_unordered(args.concurrency)
        .try_collect::<()>()
        .await?;
    let load_secs = started.elapsed().as_secs_f64();
    let (raw_bytes, _, _) = storage(s3, n).await?;
    eprintln!("   loaded in {load_secs:.1}s, {raw_bytes:.0} B/tenant");

    let mut gc_total = Snapshot::default();
    let mut after_cleanup = None;
    if v.load == Load::MemtableFlushThenGc {
        let before = counters.snapshot();
        futures::stream::iter(0..n)
            .map(|id| {
                let store = Arc::clone(&store);
                async move { offline_gc(store, id).await.with_context(|| format!("gc tenant {id}")) }
            })
            .buffer_unordered(args.concurrency)
            .try_collect::<()>()
            .await?;
        gc_total = counters.snapshot().since(&before);
        after_cleanup = Some(storage(s3, n).await?.0);
        eprintln!("   offline GC: {} requests, {:.0} B/tenant after", gc_total.total(), after_cleanup.unwrap());
    }

    // Distinct tenants: 100 untraced wakes, 10 traced wakes, 1 for idle-open.
    let mut rng = rand::thread_rng();
    let ids: Vec<usize> = sample(&mut rng, n, COLD_WAKES + TRACED_WAKES + 1).into_vec();
    let (untraced, rest) = ids.split_at(COLD_WAKES);
    let (traced, idle_id) = rest.split_at(TRACED_WAKES);
    let idle_id = idle_id[0];

    // Untraced wakes: latency and request counts.
    let mut lat = Vec::new();
    let mut wake_req = Snapshot::default();
    let mut close_req = Snapshot::default();
    for &id in untraced {
        let key = format!("doc:{}", rng.gen_range(0..DOCS));
        let before = counters.snapshot();
        let t = Instant::now();
        let h = Handle::open(v.wake, Arc::clone(&store), id).await?;
        let val = h.get(&key).await?;
        lat.push(t.elapsed());
        let mid = counters.snapshot();
        anyhow::ensure!(val.is_some(), "{} tenant {id} missing {key}", v.name);
        h.close().await?;
        let after = counters.snapshot();
        wake_req = sum(&wake_req, &mid.since(&before));
        close_req = sum(&close_req, &after.since(&mid));
    }
    let wake_latency = latency(lat);
    eprintln!("   wake p50 {:.1} ms p99 {:.1} ms", wake_latency.p50_ms, wake_latency.p99_ms);

    // Traced wakes: request order and sequential depth.
    let mut wake_shapes = Vec::new();
    let mut close_shapes = Vec::new();
    let mut first_trace = (vec![], vec![], vec![], 0u64);
    for (i, &id) in traced.iter().enumerate() {
        let key = format!("doc:{}", rng.gen_range(0..DOCS));
        trace.start();
        let h = Handle::open(v.wake, Arc::clone(&store), id).await?;
        let open_ev = trace.take();
        let open_dur = open_ev.iter().map(|e| e.start_us + e.dur_us).max().unwrap_or(0);
        trace.start();
        let val = h.get(&key).await?;
        let get_ev = trace.take();
        anyhow::ensure!(val.is_some());
        trace.start();
        h.close().await?;
        let close_ev = trace.take();
        // Wake shape covers open then get, so shift get events after the open window.
        let mut wake: Vec<TraceEvent> = open_ev.clone();
        wake.extend(get_ev.iter().cloned().map(|mut e| {
            e.start_us += open_dur;
            e
        }));
        wake_shapes.push(shape(&wake));
        close_shapes.push(shape(&close_ev));
        if i == 0 {
            first_trace = (open_ev, get_ev, close_ev, open_dur);
        }
    }

    // Idle-open: one tenant open with no traffic.
    let h = Handle::open(v.wake, Arc::clone(&store), idle_id).await?;
    let _ = h.get("doc:0").await?;
    tokio::time::sleep(Duration::from_secs(1)).await; // let start-up work settle
    let before = counters.snapshot();
    trace.start();
    tokio::time::sleep(Duration::from_secs(args.idle_secs)).await;
    let idle_events = trace.take();
    let idle_open_requests = counters.snapshot().since(&before);
    h.close().await?;
    eprintln!("   idle-open: {} requests in {} s", idle_open_requests.total(), args.idle_secs);

    // Final state of storage after everything above.
    let (_, final_objects, by_dir) = storage(s3, n).await?;
    Ok(VariantResult {
        name: v.name,
        desc: v.desc,
        load_secs,
        gc_requests_total: gc_total,
        storage_bytes_per_tenant_raw: raw_bytes,
        storage_bytes_per_tenant_after_cleanup: after_cleanup,
        storage_objects_per_tenant_final: final_objects,
        bytes_by_dir_final: by_dir,
        wake_latency,
        wake_requests_per_wake: wake_req,
        close_requests_per_wake: close_req,
        wake_shape_mean: mean_shape(&wake_shapes),
        close_shape_mean: mean_shape(&close_shapes),
        idle_secs: args.idle_secs,
        idle_open_requests,
        first_trace,
        idle_events,
    })
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

fn fmt_inf(x: f64) -> String {
    if x.is_finite() {
        if x >= 3600.0 { format!("{:.1} h", x / 3600.0) } else if x >= 60.0 { format!("{:.1} min", x / 60.0) } else { format!("{x:.1} s") }
    } else {
        "never (no idle requests)".to_string()
    }
}

fn render(results: &[VariantResult], n: usize) -> String {
    let mut s = String::new();

    // --- Step 1: trace of the V0 wake -----------------------------------------
    let v0 = &results[0];
    let (open_ev, get_ev, close_ev, off) = &v0.first_trace;
    let _ = writeln!(s, "## Step 1 — Request trace of one cold wake (V0: writer open + get)\n");
    let _ = writeln!(s, "One tenant created by the existing load phase, woken once with tracing on. `start ms` is relative to the start of that phase (the get phase is shifted to follow the open phase). Bodies are fully read inside the timed window. *overlapped* = another call was in flight at some point during this call.\n");
    s.push_str(&trace_table(open_ev, get_ev, *off));
    let all: Vec<&TraceEvent> = open_ev.iter().chain(get_ev.iter()).collect();
    let _ = writeln!(s, "\n**Grouped by purpose (this wake):**\n");
    s.push_str(&group_table(&all));
    let mut wake_all: Vec<TraceEvent> = open_ev.clone();
    wake_all.extend(get_ev.iter().cloned().map(|mut e| { e.start_us += off; e }));
    let sh = shape(&wake_all);
    let _ = writeln!(s, "\n**This wake:** {} requests; **{} strictly sequential** (never overlapped with another call), **{} parallel** (overlapped with at least one other call); {} sequential steps after merging overlapping calls.\n", sh.total, sh.strictly_sequential, sh.parallel, sh.steps);
    let m = v0.wake_shape_mean;
    let _ = writeln!(s, "**Mean over {TRACED_WAKES} traced wakes of different tenants:** {:.1} requests, {:.1} strictly sequential, {:.1} parallel, {:.1} sequential steps.\n", m[0], m[1], m[2], m[3]);
    let _ = writeln!(s, "**The matching close (same tenant)** — {} requests:\n", close_ev.len());
    s.push_str(&trace_table_labeled(close_ev, &[], 0, "close"));
    let close_refs: Vec<&TraceEvent> = close_ev.iter().collect();
    let _ = writeln!(s, "\n{}", group_table(&close_refs));
    let c = v0.close_shape_mean;
    let _ = writeln!(s, "\nClose, mean over {TRACED_WAKES}: {:.1} requests, {:.1} strictly sequential, {:.1} parallel, {:.1} sequential steps.\n", c[0], c[1], c[2], c[3]);

    let _ = writeln!(s, "### Where an open-but-idle Db spends its requests ({} s window, V0)\n", v0.idle_secs);
    let idle_refs: Vec<&TraceEvent> = v0.idle_events.iter().collect();
    s.push_str(&group_table(&idle_refs));

    let _ = writeln!(s, "\n<details><summary>Traces of one wake and its close for every other variant</summary>\n");
    for r in &results[1..] {
        let (o, g, c, off) = &r.first_trace;
        let _ = writeln!(s, "#### {} — {}\n", r.name, r.desc);
        let _ = writeln!(s, "Wake:\n");
        s.push_str(&trace_table(o, g, *off));
        let _ = writeln!(s, "\nClose:\n");
        s.push_str(&trace_table_labeled(c, &[], 0, "close"));
        let _ = writeln!(s, "\nIdle-open window by purpose:\n");
        let refs: Vec<&TraceEvent> = r.idle_events.iter().collect();
        s.push_str(&group_table(&refs));
        s.push('\n');
    }
    let _ = writeln!(s, "</details>\n");

    // --- Step 2: comparison -----------------------------------------------------
    let _ = writeln!(s, "## Step 2 — Variant comparison ({n} fresh tenants per variant, {COLD_WAKES} cold wakes each, MinIO on localhost)\n");
    let _ = writeln!(s, "| | Variant | Sequential steps / wake | Strictly seq. / parallel / total req. per wake | Wake: PUT/LIST-tier · GET-tier | Close: PUT/LIST-tier · GET-tier | Wake p50 / p99 (ms) | Idle-open req. / {}s (PUT/LIST · GET) | Storage B/tenant (raw → after cleanup) |", results[0].idle_secs);
    let _ = writeln!(s, "|---|---|---|---|---|---|---|---|---|");
    for r in results {
        let (wp, wg) = per_f(&r.wake_requests_per_wake, COLD_WAKES);
        let (cp, cg) = per_f(&r.close_requests_per_wake, COLD_WAKES);
        let w = r.wake_shape_mean;
        let cleanup = match r.storage_bytes_per_tenant_after_cleanup {
            Some(a) => format!("{:.0} → **{:.0}**", r.storage_bytes_per_tenant_raw, a),
            None => format!("{:.0} → no cleanup run", r.storage_bytes_per_tenant_raw),
        };
        let _ = writeln!(
            s, "| **{}** | {} | **{:.1}** | {:.1} / {:.1} / {:.1} | {:.2} · {:.2} | {:.2} · {:.2} | {:.1} / {:.1} | {} ({} · {}) | {} |",
            r.name, r.desc, w[3], w[1], w[2], w[0], wp, wg, cp, cg,
            r.wake_latency.p50_ms, r.wake_latency.p99_ms,
            r.idle_open_requests.total(), r.idle_open_requests.billed_put_tier(), r.idle_open_requests.billed_get_tier(),
            cleanup
        );
    }
    let _ = writeln!(s, "\nPUT/LIST-tier = PUT + COPY + LIST (multipart included); GET-tier = GET + range GET + HEAD. Per-wake figures are exact means over {COLD_WAKES} untraced wakes of distinct tenants; sequential-step figures are means over {TRACED_WAKES} separate traced wakes (tracing buffers response bodies, so latency is taken only from the untraced wakes).\n");
    let _ = writeln!(s, "Offline maintenance (not part of any wake):\n");
    let _ = writeln!(s, "| Variant | Load time | Offline GC requests (all {n} tenants) | per tenant PUT/LIST · GET · DELETE | Final objects/tenant |\n|---|---|---|---|---|");
    for r in results {
        let g = &r.gc_requests_total;
        let _ = writeln!(s, "| {} | {:.1} s | {} | {:.2} · {:.2} · {:.2} | {:.2} |", r.name, r.load_secs, g.total(),
            g.billed_put_tier() as f64 / n as f64, g.billed_get_tier() as f64 / n as f64, g.delete as f64 / n as f64,
            r.storage_objects_per_tenant_final);
    }

    // --- Step 3: projections ------------------------------------------------------
    let _ = writeln!(s, "\n## Step 3 — Projections (NOT measurements)\n");
    let _ = writeln!(s, "Projected wake latency = sequential steps × per-request latency. Assumptions: **5 ms** per request (S3 Express One Zone) and **30 ms** (S3 Standard). This ignores transfer time, request-size effects and tail latency, so it is a rough lower bound; parallel requests are assumed to cost one step.\n");
    let _ = writeln!(s, "Break-even idle time = (cost of wake + cost of close) ÷ (cost per second of an open-but-idle Db), at S3 Standard prices (PUT/COPY/POST/LIST $0.005 per 1,000; GET $0.0004 per 1,000). If a tenant will stay idle longer than this, closing it and waking it later is cheaper than leaving it open.\n");
    let _ = writeln!(s, "| Variant | Sequential steps | Projected wake @ 5 ms | Projected wake @ 30 ms | Wake + close cost | Idle-open cost | Break-even idle time |\n|---|---|---|---|---|---|---|");
    for r in results {
        let steps = r.wake_shape_mean[3];
        let wake_cost = request_cost(&r.wake_requests_per_wake) / COLD_WAKES as f64;
        let close_cost = request_cost(&r.close_requests_per_wake) / COLD_WAKES as f64;
        let idle_per_sec = request_cost(&r.idle_open_requests) / r.idle_secs as f64;
        let breakeven = if idle_per_sec > 0.0 { (wake_cost + close_cost) / idle_per_sec } else { f64::INFINITY };
        let _ = writeln!(
            s, "| {} | {:.1} | {:.0} ms | {:.0} ms | {} | {}/month | {} |",
            r.name, steps, steps * 5.0, steps * 30.0, usd(wake_cost + close_cost),
            usd(idle_per_sec * 30.0 * 86400.0), fmt_inf(breakeven)
        );
    }
    s
}

pub async fn run(args: &Args, s3: Arc<dyn ObjectStore>, counting: Arc<CountingStore>) -> Result<()> {
    let wanted: Option<Vec<String>> = args
        .only
        .as_ref()
        .map(|o| o.split(',').map(|x| x.trim().to_string()).collect());
    let mut results = Vec::new();
    for v in variants() {
        if let Some(w) = &wanted {
            if !w.iter().any(|x| x == v.name) {
                continue;
            }
        }
        results.push(run_variant(&v, args, &s3, &counting).await?);
    }
    anyhow::ensure!(!results.is_empty(), "no variant selected");
    std::fs::create_dir_all(&args.out_dir)?;
    let md = render(&results, args.tenants);
    let base = format!("{}/variants_n{}", args.out_dir, args.tenants);
    std::fs::write(format!("{base}.md"), &md)?;
    std::fs::write(format!("{base}.json"), serde_json::to_string_pretty(&results)?)?;
    println!("{md}");
    Ok(())
}
