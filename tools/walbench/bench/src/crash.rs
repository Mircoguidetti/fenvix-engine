//! Crash test. The harness (`crashtest`) runs, for each cycle, a writer child process that prints `A <id>` on stdout
//! for every write once it is acknowledged (durable); after a random delay the harness SIGKILLs it (kill -9), then runs
//! a fresh `recover` process on the same log (the restart) and checks every acknowledged id is recovered, with the
//! right bytes. An id is counted as acknowledged only once the harness has read its line, i.e. strictly after the ack:
//! the check never counts a write that was not acknowledged.

use crate::bench::{serial, NEXT_ID};
use crate::codec;
use crate::committer::{Committer, Mode};
use crate::counters;
use crate::store::{Backend, BackendArgs, Kind};
use anyhow::{anyhow, bail, Context, Result};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io::Write;
use std::process::Stdio;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(clap::Args, Debug, Clone)]
pub struct WriterArgs {
    #[command(flatten)]
    pub b: BackendArgs,
    #[arg(long)]
    pub variant: String,
    #[arg(long, default_value_t = 1024)]
    pub size: usize,
    #[arg(long, default_value_t = 8)]
    pub conc: usize,
    #[arg(long)]
    pub log: String,
    #[arg(long, default_value_t = 1)]
    pub first_id: u64,
    /// issue at most this many writes, then wait for their acks and exit (bounds the requests of one crash cycle)
    #[arg(long, default_value_t = 1000)]
    pub max_writes: u64,
    /// exit by itself after this many seconds (no orphan writer if the harness is gone)
    #[arg(long, default_value_t = 30)]
    pub max_secs: u64,
}

pub async fn writer(a: WriterArgs) -> Result<()> {
    counters::start_live_every(format!("writer-{}", a.log), 100);
    let backend = Backend::new(a.b.clone()).await?;
    let store = backend.log(&a.log).await?;
    let mode = Mode::parse(&a.variant, a.b.backend.max_batch_bytes(), serial(a.b.backend))?;
    let com = Committer::new(store, mode);
    NEXT_ID.store(a.first_id, Relaxed);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let printer = tokio::spawn(async move {
        let out = std::io::stdout();
        while let Some(l) = rx.recv().await {
            let mut o = out.lock();
            let _ = writeln!(o, "{l}");
            while let Ok(l) = rx.try_recv() {
                let _ = writeln!(o, "{l}");
            }
            let _ = o.flush();
        }
    });
    let max_secs = a.max_secs;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(max_secs)).await;
        std::process::exit(3);
    });
    let issued = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    tx.send("READY".into())?;
    let mut hs = Vec::new();
    for w in 0..a.conc {
        let (com, tx, size, issued, max_writes) = (com.clone(), tx.clone(), a.size, issued.clone(), a.max_writes);
        hs.push(tokio::spawn(async move {
            while issued.fetch_add(1, Relaxed) < max_writes {
                let id = NEXT_ID.fetch_add(1, Relaxed);
                match com.write(w as u32, id, codec::payload(id, size)).await {
                    Ok(()) => {
                        let _ = tx.send(format!("A {id}"));
                    }
                    Err(e) => {
                        let _ = tx.send(format!("E {e:#}"));
                        return;
                    }
                }
            }
        }));
    }
    for h in hs {
        let _ = h.await;
    }
    drop(tx);
    let _ = printer.await;
    Ok(())
}

#[derive(clap::Args, Debug, Clone)]
pub struct RecoverArgs {
    #[command(flatten)]
    pub b: BackendArgs,
    #[arg(long)]
    pub log: String,
}

pub fn ranges(ids: &BTreeSet<u64>) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &i in ids {
        match out.last_mut() {
            Some(r) if r.1 + 1 == i => r.1 = i,
            _ => out.push((i, i)),
        }
    }
    out
}

/// Reads the whole log and prints {ids (as ranges), bad batches, bad payloads, duplicates, ms}.
pub async fn recover(a: RecoverArgs) -> Result<()> {
    let t = Instant::now();
    let backend = Backend::new(a.b.clone()).await?;
    let store = backend.log(&a.log).await?;
    let r = store.read_all().await?;
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let mut ids = BTreeSet::new();
    let (mut dup, mut bad_payload) = (0u64, 0u64);
    for b in &r.batches {
        for e in &b.entries {
            if !codec::check_payload(e) {
                bad_payload += 1;
            }
            if !ids.insert(e.id) {
                dup += 1;
            }
        }
    }
    let out = json!({"ids": ranges(&ids), "n": ids.len(), "batches": r.batches.len(), "objects": r.objects, "bad": r.bad,
                     "bad_payload": bad_payload, "duplicates": dup, "ms": ms, "counters": counters::snap()});
    println!("{out}");
    Ok(())
}

pub fn ids_from_ranges(v: &Value) -> BTreeSet<u64> {
    let mut s = BTreeSet::new();
    for r in v.as_array().into_iter().flatten() {
        let (a, b) = (r[0].as_u64().unwrap_or(1), r[1].as_u64().unwrap_or(0));
        for i in a..=b {
            s.insert(i);
        }
    }
    s
}

#[derive(clap::Args, Debug, Clone)]
pub struct CrashArgs {
    #[command(flatten)]
    pub b: BackendArgs,
    #[arg(long, default_value_t = 200)]
    pub cycles: u32,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// comma separated variants to draw from
    #[arg(long)]
    pub variants: String,
    #[arg(long, default_value = "1,8,64")]
    pub concs: String,
    #[arg(long, default_value = "200,1024")]
    pub sizes: String,
    #[arg(long, default_value_t = 150)]
    pub kill_min_ms: u64,
    #[arg(long, default_value_t = 2500)]
    pub kill_max_ms: u64,
    /// each writer issues at most this many writes; the kill also fires when a random number of acks in
    /// [kill_min_acks, max_writes - 1] has been read, so it lands mid-run even for fast backends
    #[arg(long, default_value_t = 1000)]
    pub max_writes: u64,
    #[arg(long, default_value_t = 20)]
    pub kill_min_acks: u64,
    /// log ids are <log>-c<cycle>
    #[arg(long)]
    pub log: String,
    #[arg(long)]
    pub out: String,
    /// stop after this many seconds (remaining cycles are reported as not run)
    #[arg(long, default_value_t = 3600)]
    pub max_secs: u64,
}

fn backend_flags(b: &BackendArgs) -> Vec<String> {
    let k = match b.backend {
        Kind::S3 => "s3",
        Kind::Express => "express",
        Kind::Ddb => "ddb",
        Kind::File => "file",
        Kind::Quorum => "quorum",
    };
    let mut v = vec!["--backend".into(), k.into(), "--prefix".into(), b.prefix.clone(), "--dir".into(), b.dir.clone(), "--prealloc-mb".into(), b.prealloc_mb.to_string()];
    for (f, x) in [("--bucket", &b.bucket), ("--table", &b.table), ("--nodes", &b.nodes)] {
        if !x.is_empty() {
            v.push(f.into());
            v.push(x.clone());
        }
    }
    v
}

pub async fn crashtest(a: CrashArgs) -> Result<()> {
    let exe = std::env::current_exe()?;
    let mut rng = rand::rngs::StdRng::seed_from_u64(a.seed);
    let variants: Vec<String> = a.variants.split(',').map(|s| s.to_string()).collect();
    let concs: Vec<usize> = a.concs.split(',').map(|s| s.parse().unwrap()).collect();
    let sizes: Vec<usize> = a.sizes.split(',').map(|s| s.parse().unwrap()).collect();
    let started = Instant::now();
    let mut cycles = Vec::new();
    let (mut total_acked, mut total_lost, mut failures) = (0u64, 0u64, 0u64);
    let mut req_totals = json!({});
    let mut next_id = 1u64;
    for c in 0..a.cycles {
        if started.elapsed().as_secs() > a.max_secs {
            break;
        }
        let log = format!("{}-c{c}", a.log);
        let variant = variants.choose(&mut rng).unwrap().clone();
        let conc = *concs.choose(&mut rng).unwrap();
        let size = *sizes.choose(&mut rng).unwrap();
        let kill_ms = rng.gen_range(a.kill_min_ms..=a.kill_max_ms);
        let kill_acks = rng.gen_range(a.kill_min_acks..a.max_writes.max(a.kill_min_acks + 1));
        let read_nodes = if a.b.backend == Kind::Quorum {
            let pairs = ["0,1", "0,2", "1,2"];
            pairs.choose(&mut rng).unwrap().to_string()
        } else {
            String::new()
        };
        let r = cycle(&exe, &a.b, &log, &variant, conc, size, kill_ms, kill_acks, a.max_writes, &read_nodes, next_id).await;
        let v = match r {
            Ok(v) => v,
            Err(e) => {
                failures += 1;
                json!({"cycle": c, "log": log, "variant": variant, "conc": conc, "size": size, "kill_ms": kill_ms, "harness_error": format!("{e:#}")})
            }
        };
        total_acked += v["acked"].as_u64().unwrap_or(0);
        total_lost += v["lost"].as_u64().unwrap_or(0);
        next_id = v["next_id"].as_u64().unwrap_or(next_id + 1_000_000);
        for k in ["recover_counters", "writer_counters"] {
            if let Some(rc) = v.get(k).filter(|x| x.is_object()) {
                req_totals = add_json(&req_totals, rc);
            }
        }
        let mut v = v;
        v["cycle"] = json!(c);
        eprintln!("cycle {c}: {}", json!({"variant": v["variant"], "conc": conc, "acked": v["acked"], "lost": v["lost"], "err": v.get("harness_error")}));
        cycles.push(v);
        // write the file after every cycle so a stopped run keeps what it did
        let out = json!({"kind": "crashtest", "backend": a.b.backend, "cycles_planned": a.cycles, "cycles_run": cycles.len(),
                         "acked_total": total_acked, "lost_total": total_lost, "harness_failures": failures,
                         "requests_total_writer_and_recover": req_totals, "secs": started.elapsed().as_secs_f64(), "cycles": cycles});
        std::fs::write(&a.out, serde_json::to_string_pretty(&out)?)?;
    }
    println!("{}", json!({"backend": a.b.backend, "cycles": cycles.len(), "acked": total_acked, "lost": total_lost, "harness_failures": failures}));
    Ok(())
}

fn add_json(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut m = x.clone();
            for (k, v) in y {
                let cur = m.get(k).cloned().unwrap_or(Value::Null);
                m.insert(k.clone(), add_json(&cur, v));
            }
            Value::Object(m)
        }
        (Value::Array(x), Value::Array(y)) => Value::Array(y.iter().enumerate().map(|(i, v)| add_json(x.get(i).unwrap_or(&Value::Null), v)).collect()),
        (Value::Null, v) => v.clone(),
        (Value::Number(x), Value::Number(y)) => match (x.as_u64(), y.as_u64()) {
            (Some(p), Some(q)) => json!(p + q),
            _ => json!(x.as_f64().unwrap_or(0.0) + y.as_f64().unwrap_or(0.0)),
        },
        (_, v) => v.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn cycle(exe: &std::path::Path, b: &BackendArgs, log: &str, variant: &str, conc: usize, size: usize, kill_ms: u64, kill_acks: u64, max_writes: u64,
               read_nodes: &str, first_id: u64) -> Result<Value> {
    let mut args = vec!["writer".to_string()];
    args.extend(backend_flags(b));
    args.extend(["--log", log, "--variant", variant, "--size", &size.to_string(), "--conc", &conc.to_string(), "--first-id", &first_id.to_string(),
                 "--max-writes", &max_writes.to_string()].map(String::from));
    let mut child = tokio::process::Command::new(exe).args(&args).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true).spawn()?;
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    // wait for READY (store opened)
    let ready = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(l) = lines.next_line().await? {
            if l == "READY" {
                return Ok::<bool, anyhow::Error>(true);
            }
        }
        Ok(false)
    })
    .await;
    if !matches!(ready, Ok(Ok(true))) {
        let _ = child.start_kill();
        bail!("writer did not become ready");
    }
    let mut acked = BTreeSet::new();
    let mut errors = Vec::new();
    let kill_at = tokio::time::Instant::now() + Duration::from_millis(kill_ms);
    let mut trigger = "time";
    loop {
        tokio::select! {
            l = lines.next_line() => match l? {
                Some(l) => {
                    parse_line(&l, &mut acked, &mut errors);
                    if acked.len() as u64 >= kill_acks {
                        trigger = "acks";
                        break;
                    }
                }
                None => {
                    trigger = "writer exited before the kill";
                    break;
                }
            },
            _ = tokio::time::sleep_until(kill_at) => break,
        }
    }
    let acked_at_kill = acked.len();
    let t_kill = Instant::now();
    let pid = child.id();
    child.start_kill().context("SIGKILL")?; // kill -9
    let status = child.wait().await?;
    // lines already written to the pipe before the kill
    while let Ok(Some(l)) = lines.next_line().await {
        parse_line(&l, &mut acked, &mut errors);
    }
    let max_id = acked.iter().next_back().copied().unwrap_or(first_id);
    // the restart: a new process reads the log
    let mut rargs = vec!["recover".to_string()];
    rargs.extend(backend_flags(b));
    rargs.extend(["--log".to_string(), log.to_string()]);
    if !read_nodes.is_empty() {
        rargs.extend(["--read-nodes".to_string(), read_nodes.to_string()]);
    }
    let t_rec = Instant::now();
    let out = tokio::process::Command::new(exe).args(&rargs).stderr(Stdio::piped()).output().await?;
    let rec_wall_ms = t_rec.elapsed().as_secs_f64() * 1000.0;
    if !out.status.success() {
        return Err(anyhow!("recover failed: {}", String::from_utf8_lossy(&out.stderr).chars().take(500).collect::<String>()));
    }
    let r: Value = serde_json::from_slice(&out.stdout)?;
    let recovered = ids_from_ranges(&r["ids"]);
    let lost: Vec<u64> = acked.difference(&recovered).copied().collect();
    let extra = recovered.difference(&acked).count();
    let _ = t_kill;
    // requests of the killed writer: its last live snapshot (written every 100 ms; the last <100 ms are not in it)
    let writer_counters = std::env::var("WALBENCH_LIVE_DIR").ok().zip(pid).and_then(|(d, p)| {
        let path = format!("{d}/{p}.json");
        let v = std::fs::read(&path).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok());
        v.map(|v| v["counters"].clone())
    });
    Ok(json!({
        "log": log, "variant": variant, "conc": conc, "size": size, "kill_ms": kill_ms, "kill_acks": kill_acks, "kill_trigger": trigger,
        "acked_at_kill_decision": acked_at_kill, "read_nodes": read_nodes,
        "exit": format!("{status}"), "acked": acked.len(), "recovered": recovered.len(), "lost": lost.len(),
        "lost_sample": lost.iter().take(20).collect::<Vec<_>>(), "recovered_not_acked": extra,
        "bad_batches": r["bad"], "bad_payload": r["bad_payload"], "duplicates": r["duplicates"],
        "recover_ms": r["ms"], "recover_process_ms": rec_wall_ms, "objects": r["objects"],
        "writer_errors": errors.iter().take(3).collect::<Vec<_>>(), "recover_counters": r["counters"], "writer_counters": writer_counters,
        "next_id": max_id.max(first_id) + 1_000_000,
    }))
}

fn parse_line(l: &str, acked: &mut BTreeSet<u64>, errors: &mut Vec<String>) {
    if let Some(id) = l.strip_prefix("A ") {
        if let Ok(id) = id.parse() {
            acked.insert(id);
        }
    } else if let Some(e) = l.strip_prefix("E ") {
        errors.push(e.to_string());
    }
}
