//! Quorum failure scenario (one run, about 70 s of writes, 8 writers, 1 KB, group commit w0):
//!   0 s   all three nodes up
//!   15 s  node 1 is SIGKILLed (its supervisor restarts it 15 s later, with the log it had: a stale node)
//!   30 s  node 1 is back but its replicator is held, so it stays stale; negative control: reading node 1 alone
//!         misses acknowledged writes (shows the check can see a loss); reading nodes 0+2 has them all
//!   36 s  hold released: node 1 is caught up from the writer; time to catch up measured
//!   45 s  node 0 is SIGKILLed (restarts 15 s later); writes continue on nodes 1+2 (1 was stale 15 s earlier)
//!   58 s  writes stop; a fresh process recovers from nodes 1+2 only; every acknowledged write must be there
//! Per-second ack latency timeline and the event times are written to the output.

use crate::bench::{summarize, NEXT_ID};
use crate::codec;
use crate::committer::{Committer, Mode};
use crate::counters;
use crate::crash::ids_from_ranges;
use crate::quorum::{crash_node, QuorumStore};
use crate::store::Store;
use anyhow::Result;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering::{Relaxed, SeqCst}};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(clap::Args, Debug, Clone)]
pub struct QtArgs {
    #[arg(long)]
    pub nodes: String,
    #[arg(long)]
    pub log: String,
    #[arg(long)]
    pub out: String,
    #[arg(long, default_value_t = 8)]
    pub conc: usize,
    #[arg(long, default_value_t = 1024)]
    pub size: usize,
    /// seconds scale (1.0 = the times above; local tests use less)
    #[arg(long, default_value_t = 1.0)]
    pub scale: f64,
}

pub async fn quorumtest(a: QtArgs) -> Result<()> {
    counters::start_live("quorumtest".into());
    let nodes: Vec<String> = a.nodes.split(',').map(|s| s.to_string()).collect();
    let q = QuorumStore::new(nodes.clone(), vec![0, 1, 2], a.log.clone(), 64);
    q.start();
    let com = Committer::new(q.clone() as Arc<dyn Store>, Mode::parse("w0", 1_000_000, true)?);
    let t0 = Instant::now();
    let at = |s: f64| t0 + Duration::from_secs_f64(s * a.scale);
    let samples = Arc::new(Mutex::new(Vec::<(f64, u64)>::new())); // (ack time s, latency us)
    let acked = Arc::new(Mutex::new(BTreeSet::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for w in 0..a.conc {
        let (com, samples, acked, stop, size) = (com.clone(), samples.clone(), acked.clone(), stop.clone(), a.size);
        hs.push(tokio::spawn(async move {
            while !stop.load(Relaxed) {
                let id = NEXT_ID.fetch_add(1, Relaxed);
                let t = Instant::now();
                if com.write(w as u32, id, codec::payload(id, size)).await.is_ok() {
                    let lat = t.elapsed().as_micros() as u64;
                    samples.lock().unwrap().push((t0.elapsed().as_secs_f64(), lat));
                    acked.lock().unwrap().insert(id);
                } else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }));
    }
    let mut events = Vec::new();
    let ev = |events: &mut Vec<Value>, what: &str, extra: Value| {
        let mut v = json!({"t_s": t0.elapsed().as_secs_f64(), "event": what});
        if let Value::Object(m) = extra {
            for (k, x) in m {
                v[k] = x;
            }
        }
        eprintln!("{v}");
        events.push(v);
    };
    let sleep_until = |i: Instant| tokio::time::sleep_until(tokio::time::Instant::from_std(i));

    sleep_until(at(15.0)).await;
    crash_node(&nodes[1], (15000.0 * a.scale) as u32).await?;
    ev(&mut events, "node1 SIGKILL", json!({"restart_after_s": 15.0 * a.scale}));
    // hold node 1's replicator so that, once restarted, it stays stale until 36 s
    q.sh.hold[1].store(true, SeqCst);
    sleep_until(at(34.0)).await;
    let acked_now: BTreeSet<u64> = acked.lock().unwrap().clone();
    let only1 = read_ids(&nodes, &a.log, vec![1]).await;
    let pair02 = read_ids(&nodes, &a.log, vec![0, 2]).await;
    let neg = match &only1 {
        Ok(s) => json!({"acked_so_far": acked_now.len(), "on_node1_alone": s.len(), "acked_missing_on_node1_alone": acked_now.difference(s).count()}),
        Err(e) => json!({"error": format!("{e:#}")}),
    };
    let pos = match &pair02 {
        Ok(s) => json!({"acked_missing_on_nodes_0_2": acked_now.difference(s).count()}),
        Err(e) => json!({"error": format!("{e:#}")}),
    };
    ev(&mut events, "stale check (node1 back, held)", json!({"negative_control": neg, "positive": pos}));
    sleep_until(at(36.0)).await;
    let head = NEXT_ID.load(Relaxed);
    let _ = head;
    let target_seq = q.sh.durable[0].load(SeqCst).max(q.sh.durable[2].load(SeqCst));
    let stale_seq = q.sh.durable[1].load(SeqCst);
    q.sh.hold[1].store(false, SeqCst);
    let t_rel = Instant::now();
    ev(&mut events, "node1 hold released (catch-up starts)", json!({"node1_durable_seq": stale_seq, "head_seq": target_seq}));
    let mut caught = None;
    while t_rel.elapsed() < Duration::from_secs(20) {
        if q.sh.durable[1].load(SeqCst) >= target_seq && q.sh.connected[1].load(SeqCst) {
            caught = Some(t_rel.elapsed().as_secs_f64() * 1000.0);
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    ev(&mut events, "node1 caught up", json!({"catch_up_ms": caught, "batches_behind": target_seq.saturating_sub(stale_seq)}));
    sleep_until(at(45.0)).await;
    crash_node(&nodes[0], (15000.0 * a.scale) as u32).await?;
    ev(&mut events, "node0 SIGKILL", json!({"restart_after_s": 15.0 * a.scale}));
    sleep_until(at(58.0)).await;
    stop.store(true, Relaxed);
    for h in hs {
        let _ = h.await;
    }
    let acked_final: BTreeSet<u64> = acked.lock().unwrap().clone();
    ev(&mut events, "writes stopped", json!({"acked": acked_final.len()}));
    // the restart: a fresh process reads nodes 1+2 only
    let exe = std::env::current_exe()?;
    let out = tokio::process::Command::new(exe)
        .args(["recover", "--backend", "quorum", "--nodes", &a.nodes, "--read-nodes", "1,2", "--log", &a.log])
        .output()
        .await?;
    let r: Value = serde_json::from_slice(&out.stdout).unwrap_or(json!({"error": String::from_utf8_lossy(&out.stderr)}));
    let rec = ids_from_ranges(&r["ids"]);
    let lost = acked_final.difference(&rec).count();
    // timeline: per second, acks and latency percentiles
    let s = samples.lock().unwrap().clone();
    let end = s.iter().map(|x| x.0).fold(0.0, f64::max).ceil() as usize;
    let mut timeline = Vec::new();
    for sec in 0..end {
        let mut v: Vec<u64> = s.iter().filter(|x| x.0 >= sec as f64 && x.0 < (sec + 1) as f64).map(|x| x.1).collect();
        let n = v.len();
        let sm = summarize(&mut v);
        timeline.push(json!({"s": sec, "acks": n, "p50": sm["p50"], "p99": sm["p99"], "max": sm["max"]}));
    }
    let mut all: Vec<u64> = s.iter().map(|x| x.1).collect();
    let phase = |from: f64, to: f64| {
        let mut v: Vec<u64> = s.iter().filter(|x| x.0 >= from * a.scale && x.0 < to * a.scale).map(|x| x.1).collect();
        summarize(&mut v)
    };
    let res = json!({
        "kind": "quorumtest", "scale": a.scale, "conc": a.conc, "size": a.size, "events": events,
        "acked": acked_final.len(), "recovered_from_nodes_1_2": rec.len(), "lost": lost,
        "recover": {"ms": r["ms"], "bad": r["bad"], "bad_payload": r["bad_payload"], "duplicates": r["duplicates"]},
        "latency_all": summarize(&mut all),
        "latency_phases": {"all_up_0_15": phase(0.0, 15.0), "node1_down_15_30": phase(15.5, 30.0), "node1_stale_held_30_36": phase(30.0, 36.0),
                           "node1_catching_up_36_45": phase(36.0, 45.0), "node0_down_45_58": phase(45.5, 58.0)},
        "timeline": timeline, "counters": counters::snap(),
    });
    std::fs::write(&a.out, serde_json::to_string_pretty(&res)?)?;
    println!("{}", json!({"acked": acked_final.len(), "lost": lost, "catch_up_ms": caught}));
    Ok(())
}

async fn read_ids(nodes: &[String], log: &str, read: Vec<usize>) -> Result<BTreeSet<u64>> {
    let q = QuorumStore::new(nodes.to_vec(), read, log.to_string(), 0);
    let r = q.read_all().await?;
    Ok(r.batches.iter().flat_map(|b| b.entries.iter().map(|e| e.id)).collect())
}
