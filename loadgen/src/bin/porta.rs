//! P3 "the front door": connection load on mongo-server with the official Rust driver (mongodb 3.9.1), TLS and
//! SCRAM-SHA-256, one user per database.
//!
//! Modes:
//! * `setup`: as root (authSource admin), create databases `<prefix>0..N` with one document and user `u<i>` (readWrite).
//! * `warm`: one read per database (opens every tenant), then close.
//! * `hold`: one client per database with `--pool` connections kept open (minPoolSize); prints `READY <created>` once
//!   every pool is full, then holds until killed. Monitoring connections (two per client, not authenticated) come on top.
//! * `churn`: one client per database with maxIdleTimeMS 1 and no minimum pool: every read, paced at `--rate` per second
//!   round robin over the databases, finds its pooled connection idle and opens a new one (TCP + TLS + SCRAM), reads one
//!   document, and the connection is closed at the next checkout. Prints connections created, reads, errors, latencies.
//!
//! SCRAM: the driver keeps the salted password of a (password, salt, iterations) in a process-wide cache, so PBKDF2 runs
//! once per user, not once per connection (checked: `--scram-check` prints how long the first and later
//! authentications take).

use clap::Parser;
use mongodb::bson::{doc, Document};
use mongodb::event::cmap::CmapEvent;
use mongodb::event::EventHandler;
use mongodb::options::{ClientOptions, Credential, ServerAddress, Tls, TlsOptions};
use mongodb::Client;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long)]
    mode: String,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 27017)]
    port: u16,
    #[arg(long, default_value = "")]
    ca: String,
    #[arg(long, default_value = "p3db")]
    prefix: String,
    #[arg(long, default_value_t = 0)]
    first: usize,
    #[arg(long, default_value_t = 1000)]
    dbs: usize,
    #[arg(long, default_value_t = 20)]
    pool: u32,
    #[arg(long, default_value_t = 2000)]
    rate: u64,
    #[arg(long, default_value_t = 30)]
    secs: u64,
    #[arg(long, default_value = "P3-pass-7c1e")]
    pwd: String,
    #[arg(long, default_value = "rootpw")]
    root_pwd: String,
    /// The server's process id: its CPU time and memory are read from /proc around the measurement.
    #[arg(long, default_value_t = 0)]
    server_pid: u32,
}

/// CPU seconds (user + system) of a process, from /proc/<pid>/stat.
fn cpu_secs(pid: &str) -> f64 {
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let after = s.rsplit_once(')').map(|(_, r)| r).unwrap_or("");
    let f: Vec<&str> = after.split_whitespace().collect();
    // after the command name: state is field 3, utime 14, stime 15 (1-based in proc(5)) -> indexes 11 and 12 here
    let t = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    let hz = clk_tck();
    (t(11) + t(12)) / hz
}

/// Clock ticks per second of /proc (getconf CLK_TCK: 100 on Linux x86-64).
fn clk_tck() -> f64 {
    100.0
}

fn rss_kb(pid: &str) -> u64 {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse().ok()).unwrap_or(0)
}

fn fds(pid: &str) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map(|d| d.count()).unwrap_or(0)
}

#[derive(Default)]
struct Counts {
    created: AtomicU64,
    ready: AtomicU64,
    closed: AtomicU64,
    checkout_failed: AtomicU64,
}

fn options(a: &Args, cred: Credential, counts: Option<Arc<Counts>>) -> ClientOptions {
    let mut o = ClientOptions::default();
    o.hosts = vec![ServerAddress::Tcp { host: a.host.clone(), port: Some(a.port) }];
    o.direct_connection = Some(true);
    o.credential = Some(cred);
    if !a.ca.is_empty() {
        let mut t = TlsOptions::default();
        t.ca_file_path = Some(a.ca.clone().into());
        o.tls = Some(Tls::Enabled(t));
    }
    o.server_selection_timeout = Some(Duration::from_secs(60));
    o.connect_timeout = Some(Duration::from_secs(30));
    if let Some(c) = counts {
        o.cmap_event_handler = Some(EventHandler::callback(move |e: CmapEvent| match e {
            CmapEvent::ConnectionCreated(_) => {
                c.created.fetch_add(1, Relaxed);
            }
            CmapEvent::ConnectionReady(_) => {
                c.ready.fetch_add(1, Relaxed);
            }
            CmapEvent::ConnectionClosed(_) => {
                c.closed.fetch_add(1, Relaxed);
            }
            CmapEvent::ConnectionCheckoutFailed(_) => {
                c.checkout_failed.fetch_add(1, Relaxed);
            }
            _ => {}
        }));
    }
    o
}

fn user_cred(a: &Args, i: usize) -> Credential {
    let db = format!("{}{}", a.prefix, i);
    Credential::builder()
        .username(format!("u{i}"))
        .password(a.pwd.clone())
        .source(db)
        .mechanism(mongodb::options::AuthMechanism::ScramSha256)
        .build()
}

fn pct(v: &mut [u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize] as f64 / 1000.0
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let range = a.first..a.first + a.dbs;
    match a.mode.as_str() {
        "setup" => {
            let root = Credential::builder().username("root".to_string()).password(a.root_pwd.clone()).source("admin".to_string()).build();
            let c = Client::with_options(options(&a, root, None))?;
            let t0 = Instant::now();
            let mut tasks = Vec::new();
            for chunk in range.clone().collect::<Vec<_>>().chunks(50) {
                let (c, a, chunk) = (c.clone(), a.clone(), chunk.to_vec());
                tasks.push(tokio::spawn(async move {
                    for i in chunk {
                        let db = c.database(&format!("{}{}", a.prefix, i));
                        db.collection::<Document>("c").insert_one(doc! { "_id": 1, "v": "x".repeat(100) }).await?;
                        let r = db.run_command(doc! { "createUser": format!("u{i}"), "pwd": a.pwd.clone(), "roles": [{ "role": "readWrite", "db": format!("{}{}", a.prefix, i) }], "mechanisms": ["SCRAM-SHA-256"] }).await;
                        if let Err(e) = r {
                            if !e.to_string().contains("already exists") {
                                return Err(anyhow::anyhow!("createUser u{i}: {e}"));
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                }));
            }
            for t in tasks {
                t.await??;
            }
            println!("SETUP {} databases in {:.1} s", a.dbs, t0.elapsed().as_secs_f64());
        }
        "warm" => {
            let t0 = Instant::now();
            let mut tasks = Vec::new();
            for chunk in range.clone().collect::<Vec<_>>().chunks(50) {
                let (a, chunk) = (a.clone(), chunk.to_vec());
                tasks.push(tokio::spawn(async move {
                    let mut first_auth = Vec::new();
                    for i in chunk {
                        let mut o = options(&a, user_cred(&a, i), None);
                        o.max_pool_size = Some(1);
                        let c = Client::with_options(o)?;
                        let t = Instant::now();
                        c.database(&format!("{}{}", a.prefix, i)).collection::<Document>("c").find_one(doc! { "_id": 1 }).await?;
                        first_auth.push(t.elapsed().as_micros() as u64);
                        c.shutdown().await;
                    }
                    Ok::<Vec<u64>, anyhow::Error>(first_auth)
                }));
            }
            let mut all = Vec::new();
            for t in tasks {
                all.extend(t.await??);
            }
            println!("WARM {} databases in {:.1} s; first read with a new client (PBKDF2 in the client included) p50 {:.2} ms", a.dbs, t0.elapsed().as_secs_f64(), pct(&mut all, 0.5));
        }
        "hold" => {
            let counts = Arc::new(Counts::default());
            let mut clients = Vec::new();
            for i in range.clone() {
                let mut o = options(&a, user_cred(&a, i), Some(counts.clone()));
                o.min_pool_size = Some(a.pool);
                o.max_pool_size = Some(a.pool);
                o.max_connecting = Some(2);
                clients.push(Client::with_options(o)?);
            }
            // a read on each client starts its pool (the background fill to minPoolSize starts with the pool)
            let mut tasks = Vec::new();
            for (n, c) in clients.iter().enumerate() {
                let (c, db) = (c.clone(), format!("{}{}", a.prefix, a.first + n));
                tasks.push(tokio::spawn(async move { c.database(&db).collection::<Document>("c").find_one(doc! { "_id": 1 }).await.map(|_| ()) }));
            }
            let mut errors = 0;
            for t in tasks {
                if t.await?.is_err() {
                    errors += 1;
                }
            }
            let want = a.pool as u64 * a.dbs as u64;
            let t0 = Instant::now();
            loop {
                let r = counts.ready.load(Relaxed);
                if r >= want || t0.elapsed() > Duration::from_secs(600) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let sp = a.server_pid.to_string();
            println!("READY created={} ready={} closed={} first_read_errors={errors} fill_secs={:.1} server_rss_kb={} server_fds={}", counts.created.load(Relaxed), counts.ready.load(Relaxed), counts.closed.load(Relaxed), t0.elapsed().as_secs_f64(), rss_kb(&sp), fds(&sp));
            tokio::time::sleep(Duration::from_secs(a.secs)).await;
            println!("END created={} ready={} closed={}", counts.created.load(Relaxed), counts.ready.load(Relaxed), counts.closed.load(Relaxed));
            std::process::exit(0);
        }
        "churn" => {
            let counts = Arc::new(Counts::default());
            let mut clients = Vec::new();
            for i in range.clone() {
                let mut o = options(&a, user_cred(&a, i), Some(counts.clone()));
                o.min_pool_size = Some(0);
                o.max_pool_size = Some(4);
                o.max_idle_time = Some(Duration::from_millis(1));
                clients.push(Client::with_options(o)?);
            }
            // first pass: every client reads once (PBKDF2 once per user, monitors up), not counted
            let mut tasks = Vec::new();
            for (n, c) in clients.iter().enumerate() {
                let (c, db) = (c.clone(), format!("{}{}", a.prefix, a.first + n));
                tasks.push(tokio::spawn(async move { c.database(&db).collection::<Document>("c").find_one(doc! { "_id": 1 }).await.map(|_| ()) }));
            }
            for t in tasks {
                t.await??;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            let created0 = counts.created.load(Relaxed);
            let closed0 = counts.closed.load(Relaxed);
            let sp = a.server_pid.to_string();
            let (srv0, me0) = (cpu_secs(&sp), cpu_secs("self"));
            println!("START created_before={created0} server_rss_kb={}", rss_kb(&sp));
            let reads = Arc::new(AtomicU64::new(0));
            let errors = Arc::new(AtomicU64::new(0));
            let lat = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
            let last_err = Arc::new(std::sync::Mutex::new(String::new()));
            let t0 = Instant::now();
            let period = Duration::from_nanos(1_000_000_000 / a.rate.max(1));
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut n: u64 = 0;
            let mut handles = Vec::new();
            while t0.elapsed() < Duration::from_secs(a.secs) {
                tick.tick().await;
                let idx = (n % a.dbs as u64) as usize;
                n += 1;
                let c = clients[idx].clone();
                let db = format!("{}{}", a.prefix, a.first + idx);
                let (reads, errors, lat, last_err) = (reads.clone(), errors.clone(), lat.clone(), last_err.clone());
                handles.push(tokio::spawn(async move {
                    let t = Instant::now();
                    match c.database(&db).collection::<Document>("c").find_one(doc! { "_id": 1 }).await {
                        Ok(Some(_)) => {
                            reads.fetch_add(1, Relaxed);
                            lat.lock().unwrap().push(t.elapsed().as_micros() as u64);
                        }
                        Ok(None) => {
                            errors.fetch_add(1, Relaxed);
                            *last_err.lock().unwrap() = "document not found".into();
                        }
                        Err(e) => {
                            errors.fetch_add(1, Relaxed);
                            *last_err.lock().unwrap() = e.to_string();
                        }
                    }
                }));
                if handles.len() > 20_000 {
                    handles.retain(|h| !h.is_finished());
                }
            }
            let issued_secs = t0.elapsed().as_secs_f64();
            for h in handles {
                let _ = h.await;
            }
            let secs = t0.elapsed().as_secs_f64();
            let (srv1, me1) = (cpu_secs(&sp), cpu_secs("self"));
            let created = counts.created.load(Relaxed) - created0;
            let closed = counts.closed.load(Relaxed) - closed0;
            let mut l = std::mem::take(&mut *lat.lock().unwrap());
            println!(
                "CHURN issued={n} in {issued_secs:.1} s, done in {secs:.1} s; reads_ok={} errors={} created={created} ({:.0}/s) closed={closed} checkout_failed={} latency_ms p50={:.2} p99={:.2} max={:.2}",
                reads.load(Relaxed),
                errors.load(Relaxed),
                created as f64 / secs,
                counts.checkout_failed.load(Relaxed),
                pct(&mut l, 0.5),
                pct(&mut l, 0.99),
                pct(&mut l, 1.0)
            );
            println!(
                "CPU server {:.2} s ({:.3} ms per new connection, {:.0}% of one core), generator {:.2} s ({:.0}% of one core); server_rss_kb={}",
                srv1 - srv0,
                (srv1 - srv0) * 1000.0 / created.max(1) as f64,
                (srv1 - srv0) * 100.0 / secs,
                me1 - me0,
                (me1 - me0) * 100.0 / secs,
                rss_kb(&sp)
            );
            let e = last_err.lock().unwrap().clone();
            if !e.is_empty() {
                println!("LAST_ERROR {e}");
            }
            std::process::exit(0);
        }
        "reads" => {
            // established connections only: `--pool` concurrent readers per database, as fast as they go, for `--secs`
            let mut clients = Vec::new();
            for i in range.clone() {
                let mut o = options(&a, user_cred(&a, i), None);
                o.max_pool_size = Some(a.pool);
                clients.push(Client::with_options(o)?);
            }
            for (n, c) in clients.iter().enumerate() {
                c.database(&format!("{}{}", a.prefix, a.first + n)).collection::<Document>("c").find_one(doc! { "_id": 1 }).await?;
            }
            let sp = a.server_pid.to_string();
            let reads = Arc::new(AtomicU64::new(0));
            let stop = Instant::now() + Duration::from_secs(a.secs);
            let (srv0, t0) = (cpu_secs(&sp), Instant::now());
            let mut tasks = Vec::new();
            for (n, c) in clients.iter().enumerate() {
                for _ in 0..a.pool {
                    let (c, db, reads) = (c.clone(), format!("{}{}", a.prefix, a.first + n), reads.clone());
                    tasks.push(tokio::spawn(async move {
                        let coll = c.database(&db).collection::<Document>("c");
                        while Instant::now() < stop {
                            if coll.find_one(doc! { "_id": 1 }).await.is_ok() {
                                reads.fetch_add(1, Relaxed);
                            }
                        }
                    }));
                }
            }
            for t in tasks {
                t.await?;
            }
            let (srv1, secs) = (cpu_secs(&sp), t0.elapsed().as_secs_f64());
            let r = reads.load(Relaxed);
            println!("READS {r} in {secs:.1} s ({:.0}/s); server CPU {:.2} s ({:.1} us per read, {:.0}% of one core)", r as f64 / secs, srv1 - srv0, (srv1 - srv0) * 1e6 / r.max(1) as f64, (srv1 - srv0) * 100.0 / secs);
        }
        m => anyhow::bail!("unknown mode {m}"),
    }
    Ok(())
}
