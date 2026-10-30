//! Latency cells (closed loop: `conc` writers, each waits for its ack before the next write) and fleet cells (open
//! loop: `dbs` databases, each writes `rate`/s with Poisson arrivals; latency from the scheduled time, so a slow
//! backend cannot hide queueing).

use crate::codec;
use crate::committer::{Committer, Mode};
use crate::counters::{self, snap};
use crate::store::{Backend, BackendArgs, Kind};
use anyhow::Result;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub fn summarize(v: &mut [u64]) -> Value {
    if v.is_empty() {
        return json!({"n": 0});
    }
    v.sort_unstable();
    let q = |p: f64| v[(((v.len() as f64) * p).ceil() as usize).clamp(1, v.len()) - 1] as f64 / 1000.0;
    let mean = v.iter().sum::<u64>() as f64 / v.len() as f64 / 1000.0;
    // log2 histogram of microseconds: bucket k holds [2^k, 2^(k+1)) us
    let mut hist = vec![0u64; 32];
    for &x in v.iter() {
        hist[(64 - x.max(1).leading_zeros() - 1) as usize] += 1;
    }
    while hist.last() == Some(&0) {
        hist.pop();
    }
    json!({"n": v.len(), "unit": "ms", "p50": q(0.50), "p90": q(0.90), "p95": q(0.95), "p99": q(0.99), "p999": q(0.999),
           "min": v[0] as f64 / 1000.0, "max": *v.last().unwrap() as f64 / 1000.0, "mean": mean, "hist_log2_us": hist})
}

pub fn diff(after: &Value, before: &Value) -> Value {
    match (after, before) {
        (Value::Object(a), Value::Object(b)) => Value::Object(a.iter().map(|(k, v)| (k.clone(), diff(v, b.get(k).unwrap_or(&Value::Null)))).collect()),
        (Value::Array(a), Value::Array(b)) => Value::Array(a.iter().zip(b.iter()).map(|(x, y)| diff(x, y)).collect()),
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
                json!(x.saturating_sub(y))
            } else {
                json!(((a.as_f64().unwrap_or(0.0) - b.as_f64().unwrap_or(0.0)) * 1000.0).round() / 1000.0)
            }
        }
        (x, _) => x.clone(),
    }
}

pub fn serial(kind: Kind) -> bool {
    matches!(kind, Kind::File | Kind::Quorum)
}

#[derive(clap::Args, Debug, Clone)]
pub struct CellArgs {
    #[command(flatten)]
    pub b: BackendArgs,
    /// perwrite | w0 | w1 | w5 | w20 ...
    #[arg(long)]
    pub variant: String,
    #[arg(long, default_value_t = 1024)]
    pub size: usize,
    #[arg(long, default_value_t = 1)]
    pub conc: usize,
    /// measured writes
    #[arg(long, default_value_t = 1500)]
    pub writes: u64,
    #[arg(long, default_value_t = 100)]
    pub warmup: u64,
    /// stop measuring after this many seconds even if `writes` is not reached
    #[arg(long, default_value_t = 90)]
    pub max_secs: u64,
    #[arg(long)]
    pub log: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub out: String,
}

pub async fn cell(a: CellArgs) -> Result<()> {
    counters::start_live(a.name.clone());
    let backend = Backend::new(a.b.clone()).await?;
    let store = backend.log(&a.log).await?;
    let mode = Mode::parse(&a.variant, a.b.backend.max_batch_bytes(), serial(a.b.backend))?;
    let com = Committer::new(store, mode);
    let size = a.size;
    // warm-up (connections, sessions, page cache): not measured, but its requests are counted in "warmup_counters"
    let c0 = serde_json::to_value(snap())?;
    run_closed(&com, a.conc, a.warmup, size, 60, None).await;
    let c1 = serde_json::to_value(snap())?;
    let b0 = (com.stats.batches.load(Relaxed), com.stats.entries.load(Relaxed), com.stats.bytes.load(Relaxed));
    let lat = Arc::new(Mutex::new(Vec::with_capacity(a.writes as usize)));
    let t = Instant::now();
    let (done, errors) = run_closed(&com, a.conc, a.writes, size, a.max_secs, Some(lat.clone())).await;
    let secs = t.elapsed().as_secs_f64();
    let c2 = serde_json::to_value(snap())?;
    let batches = com.stats.batches.load(Relaxed) - b0.0;
    let entries = com.stats.entries.load(Relaxed) - b0.1;
    let mut v = std::mem::take(&mut *lat.lock().unwrap());
    let out = json!({
        "kind": "cell", "name": a.name, "backend": a.b.backend, "variant": a.variant, "mode": mode, "size": size, "conc": a.conc,
        "writes_target": a.writes, "writes_acked": done, "write_errors": errors, "first_error": FIRST_ERROR.lock().unwrap().clone(), "secs": secs, "writes_per_s": done as f64 / secs,
        "batches": batches, "avg_batch": if batches > 0 { entries as f64 / batches as f64 } else { 0.0 },
        "batch_bytes": com.stats.bytes.load(Relaxed) - b0.2,
        "latency": summarize(&mut v), "counters": diff(&c2, &c1), "warmup_counters": diff(&c1, &c0),
        "started_unix_ms": counters::now_ms() - (secs * 1000.0) as u64,
    });
    std::fs::write(&a.out, serde_json::to_string_pretty(&out)?)?;
    counters::flush_live(&a.name);
    println!("{}", json!({"name": a.name, "p50": out["latency"]["p50"], "p99": out["latency"]["p99"], "acked": done, "errors": errors}));
    Ok(())
}

/// `conc` writers share `total` writes. Returns (acked, errors).
pub static FIRST_ERROR: Mutex<Option<String>> = Mutex::new(None);

async fn run_closed(com: &Committer, conc: usize, total: u64, size: usize, max_secs: u64, lat: Option<Arc<Mutex<Vec<u64>>>>) -> (u64, u64) {
    let issued = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop_at = Instant::now() + Duration::from_secs(max_secs);
    let mut hs = Vec::new();
    for w in 0..conc {
        let (com, issued, acked, errors, lat) = (com.clone(), issued.clone(), acked.clone(), errors.clone(), lat.clone());
        hs.push(tokio::spawn(async move {
            let mut mine = Vec::new();
            while issued.fetch_add(1, Relaxed) < total && Instant::now() < stop_at {
                let id = NEXT_ID.fetch_add(1, Relaxed);
                let data = codec::payload(id, size);
                let t = Instant::now();
                match com.write(w as u32, id, data).await {
                    Ok(()) => {
                        mine.push(t.elapsed().as_micros() as u64);
                        acked.fetch_add(1, Relaxed);
                    }
                    Err(e) => {
                        errors.fetch_add(1, Relaxed);
                        FIRST_ERROR.lock().unwrap().get_or_insert_with(|| format!("{e:#}").chars().take(600).collect());
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            if let Some(l) = lat {
                l.lock().unwrap().extend(mine);
            }
        }));
    }
    for h in hs {
        let _ = h.await;
    }
    (acked.load(Relaxed), errors.load(Relaxed))
}

#[derive(clap::Args, Debug, Clone)]
pub struct FleetArgs {
    #[command(flatten)]
    pub b: BackendArgs,
    /// shared = one log and one committer for all databases; perdb = one log and committer per database
    #[arg(long)]
    pub layout: String,
    #[arg(long)]
    pub variant: String,
    #[arg(long, default_value_t = 500)]
    pub dbs: u32,
    /// writes per second per database (Poisson)
    #[arg(long, default_value_t = 1.0)]
    pub rate: f64,
    #[arg(long, default_value_t = 60)]
    pub secs: u64,
    #[arg(long, default_value_t = 5)]
    pub warm_secs: u64,
    #[arg(long, default_value_t = 1024)]
    pub size: usize,
    #[arg(long)]
    pub log: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub out: String,
}

pub async fn fleet(a: FleetArgs) -> Result<()> {
    counters::start_live(a.name.clone());
    let backend = Backend::new(a.b.clone()).await?;
    let mode = Mode::parse(&a.variant, a.b.backend.max_batch_bytes(), serial(a.b.backend))?;
    let mut coms = Vec::new();
    if a.layout == "shared" {
        coms.push(Committer::new(backend.log(&a.log).await?, mode));
    } else if a.layout == "perdb" {
        for d in 0..a.dbs {
            coms.push(Committer::new(backend.log(&format!("{}-d{d}", a.log)).await?, mode));
        }
    } else {
        anyhow::bail!("--layout shared|perdb");
    }
    let start = tokio::time::Instant::now() + Duration::from_millis(200);
    let measure_from = start + Duration::from_secs(a.warm_secs);
    let end = measure_from + Duration::from_secs(a.secs);
    let lat = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(AtomicU64::new(0));
    let c_start = Arc::new(Mutex::new(None));
    let mut hs = Vec::new();
    for d in 0..a.dbs {
        let com = coms[if coms.len() == 1 { 0 } else { d as usize }].clone();
        let (lat, errors, rate, size, c_start) = (lat.clone(), errors.clone(), a.rate, a.size, c_start.clone());
        hs.push(tokio::spawn(async move {
            let mut t = start + Duration::from_secs_f64(rand::random::<f64>() / rate);
            let mut ws = Vec::new();
            while t < end {
                tokio::time::sleep_until(t).await;
                if t >= measure_from {
                    let mut g = c_start.lock().unwrap();
                    if g.is_none() {
                        *g = Some((serde_json::to_value(snap()).unwrap(), Instant::now()));
                    }
                }
                let (com, lat, errors, sched) = (com.clone(), lat.clone(), errors.clone(), t);
                let measured = t >= measure_from;
                ws.push(tokio::spawn(async move {
                    let id = NEXT_ID.fetch_add(1, Relaxed);
                    match com.write(d, id, codec::payload(id, size)).await {
                        Ok(()) => {
                            if measured {
                                lat.lock().unwrap().push(sched.elapsed().as_micros() as u64);
                            }
                        }
                        Err(e) => {
                            errors.fetch_add(1, Relaxed);
                            FIRST_ERROR.lock().unwrap().get_or_insert_with(|| format!("{e:#}").chars().take(600).collect());
                        }
                    }
                }));
                let u: f64 = rand::random::<f64>().max(1e-12);
                t += Duration::from_secs_f64(-u.ln() / rate);
            }
            for w in ws {
                let _ = w.await;
            }
        }));
    }
    for h in hs {
        let _ = h.await;
    }
    let (c1, t1) = c_start.lock().unwrap().clone().unwrap_or((serde_json::to_value(snap())?, Instant::now()));
    let secs = t1.elapsed().as_secs_f64();
    let c2 = serde_json::to_value(snap())?;
    let batches: u64 = coms.iter().map(|c| c.stats.batches.load(Relaxed)).sum();
    let entries: u64 = coms.iter().map(|c| c.stats.entries.load(Relaxed)).sum();
    let mut v = std::mem::take(&mut *lat.lock().unwrap());
    let n = v.len();
    let out = json!({
        "kind": "fleet", "name": a.name, "backend": a.b.backend, "layout": a.layout, "variant": a.variant, "mode": mode,
        "dbs": a.dbs, "rate_per_db": a.rate, "size": a.size, "secs_measured": secs, "writes_measured": n,
        "write_errors": errors.load(Relaxed), "first_error": FIRST_ERROR.lock().unwrap().clone(), "batches_total": batches, "avg_batch_total": if batches > 0 { entries as f64 / batches as f64 } else { 0.0 },
        "latency": summarize(&mut v),
        // counters from the first measured write to the end (includes the drain of writes scheduled in the window)
        "counters": diff(&c2, &c1),
    });
    std::fs::write(&a.out, serde_json::to_string_pretty(&out)?)?;
    counters::flush_live(&a.name);
    println!("{}", json!({"name": a.name, "p50": out["latency"]["p50"], "p99": out["latency"]["p99"], "n": n}));
    Ok(())
}
