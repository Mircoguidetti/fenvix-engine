//! `openraft` spike (openraft 0.9.25) on the common test bed. Criteria: `docs/journal3z-libraries.md`.
//! Modes: `latency` (1 group, client on node 0 = leader), `idle --groups N` (N groups x 3 replicas, one shared
//! LogFile per node), `fence` (C5 scenarios), `trim` (C6: purge to an external index).
mod net;
mod store;

use bytes::Bytes;
use j3z_harness::{closed_loop, idle, Args, DelayLine, LogFile, WriteFn};
use net::{NetFactory, Router};
use openraft::raft::VoteRequest;
use openraft::{BasicNode, Config, Raft, ServerState, SnapshotPolicy, Vote};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::{LogStore, Sm};

openraft::declare_raft_types!(pub TC: D = Bytes, R = ());

struct Cluster {
    r: Arc<Router>,
    rafts: Vec<Raft<TC>>,
    logs: Vec<LogStore>,
    sms: Vec<Sm>,
    files: Vec<Arc<LogFile>>,
    groups: usize,
}

fn members() -> BTreeMap<u64, BasicNode> {
    (0..3u64).map(|i| (i, BasicNode { addr: format!("n{i}") })).collect()
}

async fn build(a: &Args, dl: &Arc<DelayLine>, groups: usize, cfg: Config) -> Cluster {
    let dir = a.fresh_dir();
    // "shared journal" variant: one LogFile (one group-commit thread, one fdatasync stream) per node for all groups
    let files: Vec<Arc<LogFile>> = (0..3).map(|i| LogFile::create(&dir.join(format!("n{i}.log"))).unwrap()).collect();
    let cfg = Arc::new(cfg.validate().unwrap());
    let r = Router::new(dl.clone(), groups);
    let (mut rafts, mut logs, mut sms) = (Vec::new(), Vec::new(), Vec::new());
    for g in 0..groups {
        for i in 0..3 {
            let ls = LogStore::new(g as u32, files[i].clone());
            let sm = Sm::default();
            let raft = Raft::new(i as u64, cfg.clone(), NetFactory { r: r.clone(), g, from: i }, ls.clone(), sm.clone()).await.unwrap();
            let _ = r.rafts[g * 3 + i].set(raft.clone());
            rafts.push(raft);
            logs.push(ls);
            sms.push(sm);
        }
    }
    Cluster { r, rafts, logs, sms, files, groups }
}

/// `initialize` on node 0 of every group (it elects itself at once; the others have no membership yet, so they cannot
/// campaign), then wait until node 0 is leader and both followers know it.
async fn start(c: &Cluster, timeout: Duration) -> anyhow::Result<f64> {
    let t0 = Instant::now();
    for g in 0..c.groups {
        c.rafts[g * 3].initialize(members()).await?;
    }
    let waits = (0..c.groups).map(|g| {
        let rs: Vec<Raft<TC>> = c.rafts[g * 3..g * 3 + 3].to_vec();
        async move {
            rs[0].wait(Some(timeout)).state(ServerState::Leader, "leader").await?;
            for f in &rs[1..] {
                f.wait(Some(timeout)).current_leader(0, "follower knows leader").await?;
            }
            anyhow::Ok(())
        }
    });
    for r in futures::future::join_all(waits).await {
        r?;
    }
    Ok(t0.elapsed().as_secs_f64())
}

fn cfg_json(c: &Config) -> Value {
    json!({"heartbeat_interval_ms": c.heartbeat_interval, "election_timeout_ms": [c.election_timeout_min, c.election_timeout_max],
           "leader_lease_ms (= election_timeout_max)": c.election_timeout_max, "snapshot_policy": format!("{:?}", c.snapshot_policy),
           "max_in_snapshot_log_to_keep": c.max_in_snapshot_log_to_keep, "max_payload_entries": c.max_payload_entries,
           "enable_elect": c.enable_elect, "enable_heartbeat": c.enable_heartbeat})
}

fn storage_json(c: &Cluster) -> Value {
    json!({"layout": "one harness LogFile per node shared by all groups on that node",
           "fsyncs_per_node": c.files.iter().map(|f| f.fsyncs.load(Relaxed)).collect::<Vec<_>>(),
           "bytes_written_per_node": c.files.iter().map(|f| f.written.load(Relaxed)).collect::<Vec<_>>()})
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let dl = DelayLine::new(a.delay_cfg());
    match a.mode.as_str() {
        "latency" | "latency_batched" => {
            let cfg = Config { cluster_name: "j3z".into(), ..Default::default() };
            let c = build(&a, &dl, 1, cfg.clone()).await;
            let startup = start(&c, Duration::from_secs(10)).await.expect("start");
            let raft0 = c.rafts[0].clone();
            let batched = a.mode == "latency_batched";
            let inflight: usize = std::env::var("J3Z_INFLIGHT").ok().and_then(|x| x.parse().ok()).unwrap_or(1);
            let stats = Arc::new(BatchStats::default());
            let w: WriteFn = if batched {
                batcher(raft0, inflight, stats.clone())
            } else {
                Arc::new(move |p: Bytes| {
                    let raft = raft0.clone();
                    Box::pin(async move { raft.client_write(p).await.map(|_| ()).map_err(|e| anyhow::anyhow!("{e}")) })
                })
            };
            let r = closed_loop(w, a.conc, a.size, a.warmup, a.n, Some(dl.clone())).await;
            let m = c.rafts[0].metrics().borrow().clone();
            let (nb, nw) = (stats.batches.load(Relaxed), stats.writes.load(Relaxed));
            let extra = json!({"openraft": "0.9.25", "config": cfg_json(&cfg), "startup_s": startup, "storage": storage_json(&c),
                "leader": m.current_leader, "term": m.current_term, "last_log_index": m.last_log_index,
                "snapshots_built_per_node": c.sms.iter().map(|s| s.inner.lock().unwrap().built).collect::<Vec<_>>(),
                "purged_upto_node0": m.purged.map(|x| x.index),
                "batching": if batched { json!({"max_writes": BATCH_MAX_WRITES, "max_bytes": BATCH_MAX_BYTES, "max_inflight_client_write": inflight,
                    "entries": nb, "writes": nw, "writes_per_entry": nw as f64 / nb.max(1) as f64}) } else { json!(null) }});
            a.emit("openraft", extra, &r);
        }
        "idle" => {
            let cfg = Config { cluster_name: "j3z".into(), ..Default::default() };
            let t0 = Instant::now();
            let c = build(&a, &dl, a.groups, cfg.clone()).await;
            let built = t0.elapsed().as_secs_f64();
            let started = start(&c, Duration::from_secs(120)).await.expect("start");
            let startup = t0.elapsed().as_secs_f64();
            tokio::time::sleep(Duration::from_secs(1)).await; // settle
            let r = idle(&dl, a.groups, a.secs, startup).await;
            let leaders = c.rafts.iter().step_by(3).filter(|r| r.metrics().borrow().state == ServerState::Leader).count();
            let extra = json!({"openraft": "0.9.25", "config": cfg_json(&cfg), "raft_instances": c.rafts.len(), "build_s": built,
                "elect_s": started, "groups_with_node0_leader_at_end": leaders, "storage": storage_json(&c),
                "note": "idle traffic = leader heartbeats (AppendEntries without entries) + responses; heartbeats do not touch the disk"});
            a.emit("openraft", extra, &r);
        }
        "fence" => fence(&a, &dl).await,
        "trim" => trim(&a, &dl).await,
        m => panic!("unknown mode {m}"),
    }
    std::process::exit(0);
}

// ----------------------------------------------------------------------------------------------------------- batching

const BATCH_MAX_WRITES: usize = 64;
const BATCH_MAX_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct BatchStats {
    batches: std::sync::atomic::AtomicU64,
    writes: std::sync::atomic::AtomicU64,
}

type Pending = (Bytes, tokio::sync::oneshot::Sender<Result<(), String>>);

/// Application-level batching in front of `client_write` on node 0: one task takes every write queued while the
/// previous `client_write` calls are in flight (at most `inflight` at once; up to 64 writes / 256 KB per entry), submits
/// them as ONE entry (payload = concatenated `frame(write)`s) and resolves every writer when that entry is committed.
fn batcher(raft: Raft<TC>, inflight: usize, stats: Arc<BatchStats>) -> WriteFn {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Pending>();
    let sem = Arc::new(tokio::sync::Semaphore::new(inflight));
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let mut batch = vec![first];
            let mut bytes = batch[0].0.len();
            while batch.len() < BATCH_MAX_WRITES && bytes < BATCH_MAX_BYTES {
                match rx.try_recv() {
                    Ok(p) => {
                        bytes += p.0.len();
                        batch.push(p);
                    }
                    Err(_) => break,
                }
            }
            let mut body = Vec::with_capacity(bytes + 4 * batch.len());
            let mut waiters = Vec::with_capacity(batch.len());
            for (p, w) in batch {
                body.extend_from_slice(&j3z_harness::frame(&p));
                waiters.push(w);
            }
            stats.batches.fetch_add(1, Relaxed);
            stats.writes.fetch_add(waiters.len() as u64, Relaxed);
            let raft = raft.clone();
            tokio::spawn(async move {
                let res = raft.client_write(Bytes::from(body)).await.map(|_| ()).map_err(|e| e.to_string());
                drop(permit);
                for w in waiters {
                    let _ = w.send(res.clone());
                }
            });
        }
    });
    Arc::new(move |p: Bytes| {
        let tx = tx.clone();
        Box::pin(async move {
            let (otx, orx) = tokio::sync::oneshot::channel();
            tx.send((p, otx)).map_err(|_| anyhow::anyhow!("batcher gone"))?;
            orx.await?.map_err(|e| anyhow::anyhow!(e))
        })
    })
}

// ------------------------------------------------------------------------------------------------------------ helpers

fn state(r: &Raft<TC>) -> Value {
    let m = r.metrics().borrow().clone();
    json!({"state": format!("{:?}", m.state), "term": m.current_term, "leader": m.current_leader, "last_log_index": m.last_log_index,
           "vote": m.vote.to_string()})
}

/// What the lease holder would do: `trigger().elect()` (optionally after injecting a vote for term `inject-1` into
/// its own node), wait a little, repeat until leader or `max`.
async fn become_leader(c: &Cluster, node: usize, inject: Option<u64>, max: Duration) -> Value {
    let raft = &c.rafts[node];
    let t0 = Instant::now();
    let mut attempts = 0;
    let mut inject_res = Vec::new();
    while t0.elapsed() < max {
        if raft.metrics().borrow().state == ServerState::Leader {
            break;
        }
        attempts += 1;
        if let Some(e) = inject.filter(|e| raft.metrics().borrow().current_term + 1 < *e) {
            // the only public entry point that sets a term from outside: a VoteRequest delivered to the node itself
            let last = raft.metrics().borrow().last_applied; // conservative; the log may be longer
            let (_, li) = c.logs[node].bounds();
            let last = li.and_then(|i| c.logs[node].inner.lock().unwrap().log.get(&i).map(|e| e.log_id)).or(last);
            let res = raft.vote(VoteRequest::new(Vote::new(e - 1, node as u64), last)).await;
            inject_res.push(match res {
                Ok(v) => format!("granted={} vote={}", v.vote_granted, v.vote),
                Err(e) => format!("error {e}"),
            });
        }
        let _ = raft.trigger().elect().await;
        let _ = raft.wait(Some(Duration::from_millis(150))).state(ServerState::Leader, "elect").await;
    }
    let ok = raft.metrics().borrow().state == ServerState::Leader;
    inject_res.dedup();
    json!({"node": node, "became_leader": ok, "attempts": attempts, "ms": t0.elapsed().as_millis() as u64,
           "inject_vote_results": inject_res, "after": state(raft)})
}

async fn write(r: &Raft<TC>, tag: &str, timeout: Duration) -> Value {
    match tokio::time::timeout(timeout, r.client_write(Bytes::from(tag.as_bytes().to_vec()))).await {
        Ok(Ok(resp)) => json!({"write": tag, "committed": true, "log_id": resp.log_id.to_string()}),
        Ok(Err(e)) => json!({"write": tag, "committed": false, "error": e.to_string()}),
        Err(_) => json!({"write": tag, "committed": false, "error": format!("not committed within {timeout:?}")}),
    }
}

fn set_lease(c: &Cluster, node: usize, epoch: u64) {
    c.r.lease_epoch[node].store(epoch, SeqCst);
}

fn cut(c: &Cluster, node: usize, on: bool) {
    c.r.cut[node].store(on, SeqCst);
}

// ---------------------------------------------------------------------------------------------------------- C5 fence

async fn fence(a: &Args, dl: &Arc<DelayLine>) {
    // No independent election: automatic elections off; leadership only via trigger().elect() by the lease holder.
    let cfg = Config { cluster_name: "j3z".into(), enable_elect: false, ..Default::default() };
    let c = build(a, dl, 1, cfg.clone()).await;
    start(&c, Duration::from_secs(10)).await.expect("start");
    let wt = Duration::from_secs(2);
    let mut out = Vec::new();
    set_lease(&c, 0, 10);
    out.push(json!({"step": "0: node 0 leader (lease epoch 10)", "w": write(&c.rafts[0], "w1@n0", wt).await, "n0": state(&c.rafts[0])}));

    // S1: lease moves to node 1 while node 0 is alive and connected (cooperative case, no epoch guard).
    set_lease(&c, 1, 20);
    let el = become_leader(&c, 1, None, Duration::from_secs(3)).await;
    let w_old = write(&c.rafts[0], "w2@n0 (stale)", wt).await;
    let w_new = write(&c.rafts[1], "w2@n1", wt).await;
    out.push(json!({"step": "S1: lease -> node 1 (epoch 20), old leader node 0 alive, no guard", "elect": el,
        "stale_write_n0": w_old, "new_write_n1": w_new}));

    // S2: current leader node 1 frozen/partitioned with a write in flight; lease -> node 2 (epoch 30), term injection tried.
    cut(&c, 1, true);
    let r1 = c.rafts[1].clone();
    let inflight = tokio::spawn(async move { write(&r1, "w3@n1 (in flight while cut)", Duration::from_secs(6)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (_, l1) = c.logs[1].bounds();
    let inflight_entry = l1.map(|i| (i, c.logs[1].term_at(i)));
    set_lease(&c, 2, 30);
    let el = become_leader(&c, 2, Some(30), Duration::from_secs(3)).await;
    let w_new = write(&c.rafts[2], "w4@n2", wt).await;
    cut(&c, 1, false);
    let w_inflight = inflight.await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (idx, t_before) = inflight_entry.unwrap_or((0, None));
    out.push(json!({"step": "S2: leader node 1 cut with a write in flight; lease -> node 2 (epoch 30, try term:=epoch via self-vote)",
        "elect": el, "term_equals_epoch": c.rafts[2].metrics().borrow().current_term == 30,
        "new_write_n2": w_new, "inflight_write_n1": w_inflight,
        "inflight_entry_on_n1": {"index": idx, "term_before": t_before, "term_after_heal_n1": c.logs[1].term_at(idx), "term_on_n2": c.logs[2].term_at(idx)},
        "n1_after_heal": state(&c.rafts[1])}));

    // S3: stale writer node 0 (epoch 10) calls elect while the legit holder node 2 (epoch 30) is briefly unreachable.
    cut(&c, 2, true);
    let el = become_leader(&c, 0, None, Duration::from_secs(3)).await;
    let w_stale = write(&c.rafts[0], "w5@n0 (stale epoch 10)", wt).await;
    cut(&c, 2, false);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let w_legit = write(&c.rafts[2], "w5@n2 (legit epoch 30)", wt).await;
    out.push(json!({"step": "S3: no guard; legit holder node 2 cut ~briefly, stale node 0 (epoch 10) triggers elect",
        "elect_stale": el, "stale_write_n0": w_stale, "legit_write_n2_after_heal": w_legit, "n2": state(&c.rafts[2])}));

    // let node 2 catch up with what the stale leader committed (otherwise it cannot win any election: S5)
    let l0 = c.rafts[0].metrics().borrow().last_log_index;
    let caught = c.rafts[2].wait(Some(Duration::from_secs(3))).log_index_at_least(l0, "catch up").await.is_ok();
    out.push(json!({"step": "S3b: node 2 catches up from node 0 before S4", "caught_up": caught, "n2": state(&c.rafts[2])}));

    // S4: same with an application-level epoch guard in the transport (every RPC carries the sender's lease epoch;
    // a node refuses anything below the highest epoch it has seen). Node 2 re-acquires the lease (epoch 40).
    c.r.guard.store(true, SeqCst);
    set_lease(&c, 2, 40);
    let el_legit = become_leader(&c, 2, None, Duration::from_secs(3)).await;
    let w_legit1 = write(&c.rafts[2], "w6@n2 (epoch 40)", wt).await;
    cut(&c, 2, true);
    let fenced0 = c.r.fenced.load(SeqCst);
    let el_stale = become_leader(&c, 0, None, Duration::from_secs(2)).await;
    let w_stale = write(&c.rafts[0], "w7@n0 (stale epoch 10)", wt).await;
    cut(&c, 2, false);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let w_legit2 = write(&c.rafts[2], "w8@n2 (epoch 40) after heal", wt).await;
    let el_again = if w_legit2["committed"] == true { json!(null) } else { become_leader(&c, 2, None, Duration::from_secs(3)).await };
    let w_legit3 = write(&c.rafts[2], "w9@n2 (epoch 40)", wt).await;
    out.push(json!({"step": "S4: epoch guard on; node 2 leader at epoch 40; node 2 cut; stale node 0 (epoch 10) triggers elect",
        "elect_legit": el_legit, "legit_write": w_legit1, "elect_stale": el_stale, "stale_write_n0": w_stale,
        "rpcs_refused_by_guard": c.r.fenced.load(SeqCst) - fenced0, "legit_write_after_heal": w_legit2,
        "legit_reelect_if_needed": el_again, "legit_write_final": w_legit3}));

    // S5: lease moves to a node whose log is behind (guard on). Node 0 misses writes, then gets the lease (epoch 50)
    // while the old holder node 2 is gone.
    cut(&c, 0, true);
    let mut ws = Vec::new();
    for i in 0..5 {
        ws.push(write(&c.rafts[2], &format!("w10.{i}@n2"), wt).await["committed"].clone());
    }
    cut(&c, 2, true);
    cut(&c, 0, false);
    set_lease(&c, 0, 50);
    let el = become_leader(&c, 0, None, Duration::from_secs(3)).await;
    out.push(json!({"step": "S5: guard on; node 0 lagging (missed 5 committed writes), lease -> node 0 (epoch 50), old holder node 2 gone",
        "writes_while_n0_cut_committed": ws, "elect_lagging_holder": el, "n1": state(&c.rafts[1]),
        "logs_last_index": c.logs.iter().map(|l| l.bounds().1).collect::<Vec<_>>()}));

    a.emit("openraft", json!({"openraft": "0.9.25", "config": cfg_json(&cfg)}), &json!({"scenarios": out}));
}

// ----------------------------------------------------------------------------------------------------------- C6 trim

async fn wait_applied(c: &Cluster, nodes: &[usize], idx: u64) -> bool {
    for &n in nodes {
        if c.rafts[n].wait(Some(Duration::from_secs(5))).applied_index_at_least(Some(idx), "applied").await.is_err() {
            return false;
        }
    }
    true
}

/// Trim nodes `nodes` to the external marker `k`: snapshot built at `k` (a pointer, no state), then purge_log(k).
async fn trim_to(c: &Cluster, nodes: &[usize], k: u64) -> Value {
    let mut res = Vec::new();
    for &n in nodes {
        c.sms[n].inner.lock().unwrap().marker = Some(k);
        let _ = c.rafts[n].trigger().snapshot().await;
        let snap = c.rafts[n].wait(Some(Duration::from_secs(3))).metrics(|m| m.snapshot.map(|s| s.index) >= Some(k), "snapshot").await.is_ok();
        let _ = c.rafts[n].trigger().purge_log(k).await;
        let t0 = Instant::now();
        let purged = c.rafts[n].wait(Some(Duration::from_secs(5))).metrics(|m| m.purged.map(|s| s.index) >= Some(k), "purged").await.is_ok();
        res.push(json!({"node": n, "snapshot_at_k": snap, "purged_to_k": purged, "purge_wait_ms": t0.elapsed().as_millis() as u64,
            "log_first_last": c.logs[n].bounds(), "metrics_purged": c.rafts[n].metrics().borrow().purged.map(|x| x.index)}));
    }
    json!(res)
}

async fn trim(a: &Args, dl: &Arc<DelayLine>) {
    let cfg = Config { cluster_name: "j3z".into(), snapshot_policy: SnapshotPolicy::Never, ..Default::default() };
    let c = build(a, dl, 1, cfg.clone()).await;
    start(&c, Duration::from_secs(10)).await.expect("start");
    let w = Duration::from_secs(2);
    for i in 0..200 {
        write(&c.rafts[0], &format!("t{i}"), w).await;
    }
    let last = c.rafts[0].metrics().borrow().last_log_index.unwrap();
    let all_applied = wait_applied(&c, &[0, 1, 2], last).await;
    let k1 = last - 50;
    let t1 = trim_to(&c, &[0, 1, 2], k1).await;
    // Lagging follower: node 2 cut, 100 more writes, marker moves past what node 2 has, trim nodes 0 and 1, heal.
    cut(&c, 2, true);
    for i in 0..100 {
        write(&c.rafts[0], &format!("u{i}"), w).await;
    }
    let last2 = c.rafts[0].metrics().borrow().last_log_index.unwrap();
    let k2 = last2 - 10;
    let t2 = trim_to(&c, &[0, 1], k2).await;
    let before_heal = state(&c.rafts[0]);
    let (s0, b0) = (c.r.snap_msgs.load(SeqCst), c.r.snap_bytes.load(SeqCst));
    cut(&c, 2, false);
    let caught_up = wait_applied(&c, &[2], last2).await;
    let i2 = c.sms[2].inner.lock().unwrap().installed;
    let res = json!({"entries": last, "all_applied": all_applied, "trim1": {"k": k1, "nodes": t1},
        "lagging": {"k": k2, "last": last2, "trim_nodes_0_1": t2, "node2_caught_up": caught_up,
            "snapshot_rpcs_to_node2": c.r.snap_msgs.load(SeqCst) - s0, "snapshot_bytes": c.r.snap_bytes.load(SeqCst) - b0,
            "node2_snapshots_installed": i2, "node2_log_first_last": c.logs[2].bounds(),
            "node2_applied": c.rafts[2].metrics().borrow().last_applied.map(|x| x.index),
            "n0_before_heal": before_heal, "n0_after": state(&c.rafts[0])}});
    a.emit("openraft", json!({"openraft": "0.9.25", "config": cfg_json(&cfg),
        "how": "SnapshotPolicy::Never; per node: state machine snapshot built AT the external marker k (data = 's3-marker:k', no state), then trigger().purge_log(k)"}), &res);
}
