//! log-on-pair: the read load of the waiter for `proto/quaderno/scripts/pair_bench.py`.
//!
//! * `fill`: one database `--db`, collection `c`, documents `_id` 0..`--docs` of about `--doc-bytes` bytes (random,
//!   incompressible padding), inserted in batches of `--batch` by `--parallel` tasks.
//! * `read`: open loop, `--rate` `find_one({_id})` per second over `--conns` connections, each read scheduled at a fixed
//!   instant (a stalled server makes the reads queue up, they are not skipped). `--hot-pct` percent of the reads pick an
//!   `_id` in the first `--hot-frac` of the documents (the working set, served from RAM), the rest an `_id` uniform over
//!   all documents (mostly not in RAM: the local disk). Prints one JSON line: reads/s done, errors, latency from the
//!   scheduled instant (p50, p99, p99.9, max, microseconds).
use clap::Parser;
use hdrhistogram::Histogram;
use mongodb::bson::{doc, Document};
use mongodb::options::ClientOptions;
use mongodb::Client;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
struct Args {
    mode: String,
    #[arg(long, default_value = "mongodb://127.0.0.1:27018")]
    uri: String,
    #[arg(long, default_value = "pair")]
    db: String,
    #[arg(long, default_value_t = 1_000_000)]
    docs: u64,
    #[arg(long, default_value_t = 1000)]
    doc_bytes: usize,
    #[arg(long, default_value_t = 1000)]
    batch: u64,
    #[arg(long, default_value_t = 8)]
    parallel: u64,
    #[arg(long, default_value_t = 2000.0)]
    rate: f64,
    #[arg(long, default_value_t = 32)]
    conns: u64,
    #[arg(long, default_value_t = 60)]
    secs: u64,
    #[arg(long, default_value_t = 5)]
    warm: u64,
    #[arg(long, default_value_t = 90)]
    hot_pct: u32,
    #[arg(long, default_value_t = 0.05)]
    hot_frac: f64,
    #[arg(long, default_value_t = 2)]
    threads: usize,
}

async fn client(uri: &str, pool: u32) -> Client {
    let mut o = ClientOptions::parse(uri).await.unwrap();
    o.max_pool_size = Some(pool);
    o.min_pool_size = Some(pool);
    o.direct_connection = Some(true);
    o.retry_reads = Some(false);
    o.retry_writes = Some(false);
    o.app_name = Some("pairload".into());
    Client::with_options(o).unwrap()
}

fn pad(rng: &mut StdRng, n: usize) -> String {
    const A: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    (0..n).map(|_| A[rng.gen_range(0..A.len())] as char).collect()
}

async fn fill(a: Args) {
    let c = client(&a.uri, a.parallel as u32 + 2).await;
    let next = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut hs = vec![];
    for w in 0..a.parallel {
        let (c, next, a) = (c.clone(), next.clone(), a.clone());
        hs.push(tokio::spawn(async move {
            let coll = c.database(&a.db).collection::<Document>("c");
            let mut rng = StdRng::seed_from_u64(w + 7);
            loop {
                let from = next.fetch_add(a.batch, Relaxed);
                if from >= a.docs {
                    break;
                }
                let to = (from + a.batch).min(a.docs);
                let v: Vec<Document> = (from..to).map(|i| doc! { "_id": i as i64, "n": i as i64, "pad": pad(&mut rng, a.doc_bytes.saturating_sub(40)) }).collect();
                for attempt in 0..50 {
                    match coll.insert_many(v.clone()).ordered(false).await {
                        Ok(_) => break,
                        Err(e) if e.to_string().contains("11000") => break,
                        Err(e) => {
                            if attempt == 49 {
                                panic!("fill {from}: {e}");
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                if from % (a.batch * 200) == 0 {
                    eprintln!("fill {from}/{} after {:.0} s", a.docs, t0.elapsed().as_secs_f64());
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    println!("{}", serde_json::json!({"fill": a.docs, "doc_bytes": a.doc_bytes, "secs": t0.elapsed().as_secs_f64()}));
}

async fn read(a: Args) {
    let c = client(&a.uri, a.conns as u32 + 2).await;
    // warm the pool
    c.database(&a.db).collection::<Document>("c").find_one(doc! {"_id": 0i64}).await.unwrap();
    let h = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()));
    let (ops, errs) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let start = Instant::now() + Duration::from_millis(100);
    let meas = start + Duration::from_secs(a.warm);
    let end = meas + Duration::from_secs(a.secs);
    let period = Duration::from_secs_f64(a.conns as f64 / a.rate);
    let hot_n = ((a.docs as f64 * a.hot_frac) as u64).max(1);
    let mut hs = vec![];
    for w in 0..a.conns {
        let (c, h, ops, errs, a) = (c.clone(), h.clone(), ops.clone(), errs.clone(), a.clone());
        hs.push(tokio::spawn(async move {
            let coll = c.database(&a.db).collection::<Document>("c");
            let mut rng = StdRng::seed_from_u64(w * 31 + 1);
            let mut next = start + period.mul_f64(w as f64 / a.conns as f64);
            while next < end {
                let now = Instant::now();
                if next > now {
                    tokio::time::sleep(next - now).await;
                }
                let sched = next;
                next += period;
                let id = if rng.gen_range(0..100) < a.hot_pct { rng.gen_range(0..hot_n) } else { rng.gen_range(0..a.docs) };
                let r = coll.find_one(doc! {"_id": id as i64}).await;
                if sched >= meas {
                    match r {
                        Ok(Some(_)) => {
                            ops.fetch_add(1, Relaxed);
                            h.lock().unwrap().record(sched.elapsed().as_micros().max(1) as u64).ok();
                        }
                        _ => {
                            errs.fetch_add(1, Relaxed);
                        }
                    }
                }
            }
        }));
    }
    for x in hs {
        x.await.unwrap();
    }
    let h = h.lock().unwrap();
    println!(
        "{}",
        serde_json::json!({"read": true, "rate": a.rate, "secs": a.secs, "ops": ops.load(Relaxed), "ops_per_s": ops.load(Relaxed) as f64 / a.secs as f64,
            "errors": errs.load(Relaxed), "hot_pct": a.hot_pct, "hot_frac": a.hot_frac, "docs": a.docs,
            "p50_us": h.value_at_quantile(0.5), "p99_us": h.value_at_quantile(0.99), "p999_us": h.value_at_quantile(0.999), "max_us": h.max()})
    );
}

fn main() {
    let a = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(a.threads).enable_all().build().unwrap();
    match a.mode.as_str() {
        "fill" => rt.block_on(fill(a)),
        "read" => rt.block_on(read(a)),
        m => panic!("mode {m}: fill | read"),
    }
}
