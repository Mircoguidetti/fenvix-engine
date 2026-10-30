//! Spike: raft-rs (crates.io `raft` 0.7.0, rust-protobuf codec) on the common journal3z test bed.
//! Modes: `latency` (1 group, leader on node 0, closed loop), `idle --groups N` (N groups x 3 replicas, one shared
//! LogFile per node), `fence` (C5: leadership driven by an external lease), `trim` (C6: trim to an external index).
//! Env: `J3Z_RAFTRS_TICK_MS` (default 10), `J3Z_RAFTRS_ELECTION_TICK` (10), `J3Z_RAFTRS_HEARTBEAT_TICK` (3),
//! `J3Z_RAFTRS_ACK` = `commit` (default) | `apply`, `J3Z_RAFTRS_SKIP_BCAST_COMMIT` = 0 (default) | 1.
mod node;

use bytes::Bytes;
use j3z_harness::{closed_loop, idle, Args, DelayLine, LogFile, WriteFn};
use node::{Ack, Ev, Net, Node, RaftCfg};
use raft::eraftpb::{Message, MessageType};
use raft::{StateRole, INVALID_ID};
use serde_json::json;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Peers = Arc<Vec<Sender<Ev>>>;

fn env_or(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// Run `f` on node `i`'s thread and return its result.
async fn on<R: Send + 'static>(p: &Peers, i: usize, f: impl FnOnce(&mut Node) -> R + Send + 'static) -> R {
    let (tx, rx) = tokio::sync::oneshot::channel();
    p[i].send(Ev::Ctl(Box::new(move |n: &mut Node| {
        let _ = tx.send(f(n));
    })))
    .unwrap();
    rx.await.unwrap()
}

/// A write on group `g` submitted on node `i` (the writer is co-located with that node: no delay).
async fn write(p: &Peers, i: usize, g: u32, data: Bytes) -> anyhow::Result<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    p[i].send(Ev::Propose(g, data, tx)).map_err(|_| anyhow::anyhow!("node gone"))?;
    rx.await?
}

/// (is_leader, term) of every group on node `i`.
async fn roles(p: &Peers, i: usize) -> Vec<(bool, u64)> {
    on(p, i, |n| n.groups.iter().map(|g| (g.rn.raft.state == StateRole::Leader, g.rn.raft.term)).collect()).await
}

/// Number of groups with a leader whose term is the highest term among the 3 replicas; leaders per node.
async fn count_leaders(p: &Peers) -> (usize, [usize; 3]) {
    let r = [roles(p, 0).await, roles(p, 1).await, roles(p, 2).await];
    let mut n = 0;
    let mut per = [0; 3];
    for g in 0..r[0].len() {
        let max = (0..3).map(|i| r[i][g].1).max().unwrap();
        if let Some(i) = (0..3).find(|&i| r[i][g].0 && r[i][g].1 == max) {
            n += 1;
            per[i] += 1;
        }
    }
    (n, per)
}

async fn wait_until<F: std::future::Future<Output = bool>>(timeout: Duration, mut f: impl FnMut() -> F) -> bool {
    let t = Instant::now();
    while t.elapsed() < timeout {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[derive(serde::Serialize, Debug, Clone)]
struct St {
    role: String,
    term: u64,
    last: u64,
    committed: u64,
    first: u64,
}
/// Flatten nested results (timeouts, joins, writes) into one short string.
fn fr<T: std::fmt::Debug, E: std::fmt::Display>(r: &Result<T, E>) -> String {
    match r {
        Ok(v) => match format!("{v:?}") {
            u if u == "()" => "ok".into(),
            o => format!("ok: {o}"),
        },
        Err(e) => format!("err: {e}"),
    }
}
fn fw(r: &anyhow::Result<()>) -> String {
    match r {
        Ok(()) => "ok".into(),
        Err(e) => format!("err: {e}"),
    }
}
fn ft(r: &Result<anyhow::Result<()>, tokio::time::error::Elapsed>) -> String {
    r.as_ref().map(fw).unwrap_or_else(|_| "no answer within the timeout (not acknowledged)".into())
}
fn fj(r: &Result<Result<anyhow::Result<()>, tokio::task::JoinError>, tokio::time::error::Elapsed>) -> String {
    match r {
        Err(_) => "no answer within the timeout (not acknowledged)".into(),
        Ok(Err(e)) => format!("join error {e}"),
        Ok(Ok(x)) => fw(x),
    }
}

async fn state(p: &Peers, i: usize) -> St {
    on(p, i, |n| {
        let r = &n.groups[0].rn.raft;
        St { role: format!("{:?}", r.state), term: r.term, last: r.raft_log.last_index(), committed: r.raft_log.committed, first: r.raft_log.first_index() }
    })
    .await
}

/// "Acquire the lease" on node `i` with lease sequence `seq`: raise the term to `seq - 1` (pub `Raft::become_follower`)
/// and step a `MsgTimeoutNow` (the message `transfer_leader` sends), so the node campaigns at term `seq` with the
/// transfer flag (voters skip the check-quorum "leader lease" check). The voters still apply Raft's vote rules.
async fn take_lease(p: &Peers, i: usize, seq: u64) -> Result<(), String> {
    on(p, i, move |n| {
        let id = n.id as u64 + 1;
        let rn = &mut n.groups[0].rn;
        if seq - 1 < rn.raft.term {
            return Err(format!("lease seq {seq} <= term {}+1", rn.raft.term));
        }
        if seq - 1 > rn.raft.term {
            rn.raft.become_follower(seq - 1, INVALID_ID);
        }
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgTimeoutNow);
        m.to = id;
        m.from = id;
        m.term = seq - 1;
        rn.step(m).map_err(|e| e.to_string())
    })
    .await
}

fn committed_has(n: &Node, tag: &[u8]) -> Option<(u64, u64)> {
    n.groups[0].committed_log.iter().find(|(_, _, d)| d.as_ref() == tag).map(|(i, t, _)| (*i, *t))
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let dir = a.fresh_dir();
    let dl = DelayLine::new(a.delay_cfg());
    let net = Net::new();
    let fence_like = a.mode == "fence" || a.mode == "trim";
    let cfg = RaftCfg {
        tick: Duration::from_millis(env_or("J3Z_RAFTRS_TICK_MS", 10)),
        // fence/trim: leadership only by explicit campaigns (no spontaneous election in the run)
        election_tick: if fence_like { 1_000_000 } else { env_or("J3Z_RAFTRS_ELECTION_TICK", 10) as usize },
        heartbeat_tick: env_or("J3Z_RAFTRS_HEARTBEAT_TICK", 3) as usize,
        ack: if std::env::var("J3Z_RAFTRS_ACK").as_deref() == Ok("apply") { Ack::Apply } else { Ack::Commit },
        record: fence_like,
        skip_bcast_commit: env_or("J3Z_RAFTRS_SKIP_BCAST_COMMIT", 0) == 1,
    };
    let groups = if a.mode == "idle" { a.groups } else { 1 };
    let files: Vec<Arc<LogFile>> = (0..3).map(|i| LogFile::create(&dir.join(format!("n{i}.log"))).unwrap()).collect();
    let t_start = Instant::now();
    let p: Peers = node::start(groups, cfg.clone(), dl.clone(), net.clone(), files.clone());
    let base = json!({
        "raft": "raft 0.7.0 (crates.io, 2023-03-07)", "codec": "protobuf-codec (rust-protobuf 2.28; prost-codec needs protoc, not installed)",
        "tick_ms": cfg.tick.as_millis() as u64, "election_tick": cfg.election_tick, "heartbeat_tick": cfg.heartbeat_tick,
        "check_quorum": true, "pre_vote": true, "skip_bcast_commit": cfg.skip_bcast_commit, "max_size_per_msg": 1u64 << 20, "max_inflight_msgs": 256,
        "ack": format!("{:?}", cfg.ack), "storage": "MemStorage (+snapshot override) + LogFile per node (fdatasync), async ready",
    });
    match a.mode.as_str() {
        "latency" => {
            on(&p, 0, |n| n.groups[0].rn.campaign().unwrap()).await;
            assert!(wait_until(Duration::from_secs(10), || async { roles(&p, 0).await[0].0 }).await, "node 0 not leader");
            let p2 = p.clone();
            let w: WriteFn = Arc::new(move |d: Bytes| {
                let p = p2.clone();
                Box::pin(async move { write(&p, 0, 0, d).await })
            });
            let r = closed_loop(w, a.conc, a.size, a.warmup, a.n, Some(dl.clone())).await;
            let s = state(&p, 0).await;
            let mut e = base.clone();
            e["fsyncs"] = json!(files.iter().map(|f| f.fsyncs.load(Relaxed)).collect::<Vec<_>>());
            e["leader_end"] = json!(s);
            a.emit("raftrs", e, &r);
        }
        "idle" => {
            // spread leaders like a balanced deployment: group g campaigns on node g % 3
            for i in 0..3 {
                on(&p, i, move |n| {
                    for g in (i..n.groups.len()).step_by(3) {
                        n.groups[g].rn.campaign().unwrap();
                    }
                })
                .await;
            }
            let ok = wait_until(Duration::from_secs(120), || async { count_leaders(&p).await.0 == groups }).await;
            let startup = t_start.elapsed().as_secs_f64();
            assert!(ok, "not every group has a leader after 120 s: {:?}", count_leaders(&p).await);
            tokio::time::sleep(Duration::from_secs(1)).await;
            let terms0: Vec<Vec<(bool, u64)>> = vec![roles(&p, 0).await, roles(&p, 1).await, roles(&p, 2).await];
            let r = idle(&dl, groups, a.secs, startup).await;
            let terms1: Vec<Vec<(bool, u64)>> = vec![roles(&p, 0).await, roles(&p, 1).await, roles(&p, 2).await];
            let changed = (0..groups).filter(|&g| (0..3).any(|i| terms0[i][g].1 != terms1[i][g].1)).count();
            let (nl, per) = count_leaders(&p).await;
            let mut e = base.clone();
            e["layout"] = json!("one Raft group per database; 3N RawNodes; one thread per node drives all its groups; one shared LogFile per node (group commit across groups)");
            e["groups_with_leader_end"] = json!(nl);
            e["leaders_per_node"] = json!(per);
            e["groups_with_term_change_during_idle"] = json!(changed);
            e["fsyncs"] = json!(files.iter().map(|f| f.fsyncs.load(Relaxed)).collect::<Vec<_>>());
            e["hibernate"] = json!("raft-rs has no hibernate/quiesce API; plain raft-rs measured (every group ticks and heartbeats)");
            a.emit("raftrs", e, &r);
        }
        "fence" => {
            let r = fence(&p, &net).await;
            a.emit("raftrs", base, &r);
        }
        "trim" => {
            let r = trim(&p, &net, &files).await;
            a.emit("raftrs", base, &r);
        }
        m => panic!("unknown mode {m}"),
    }
    std::process::exit(0);
}

async fn fence(p: &Peers, net: &Arc<Net>) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    // 1. lease seq 5 -> node 0 (term 5)
    let r = take_lease(p, 0, 5).await;
    let lead0 = wait_until(Duration::from_secs(5), || async { state(p, 0).await.role == "Leader" }).await;
    let mut ok = 0;
    for i in 0..3 {
        ok += write(p, 0, 0, Bytes::from(format!("A{i}"))).await.is_ok() as u32;
    }
    out.insert("1_lease5_on_node0".into(), json!({"take_lease": fr(&r), "leader": lead0, "state": state(p, 0).await, "writes_ok": ok}));

    // 2. node 0 freezes/partitions (lease expires); an in-flight write on node 0
    net.isolate(0, true);
    let pc = p.clone();
    let inflight = tokio::spawn(async move { write(&pc, 0, 0, Bytes::from_static(b"INFLIGHT")).await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 3. lease seq 9 -> node 1
    let r = take_lease(p, 1, 9).await;
    let lead1 = wait_until(Duration::from_secs(5), || async { state(p, 1).await.role == "Leader" }).await;
    let w_new = write(p, 1, 0, Bytes::from_static(b"NEW1")).await;
    out.insert("2_lease9_on_node1".into(), json!({"take_lease": fr(&r), "leader": lead1, "state": state(p, 1).await,
        "term_equals_lease_seq": state(p, 1).await.term == 9, "write_on_node1": fw(&w_new)}));

    // 4. the stale writer (node 0, still leader in its own view) writes again
    let s0 = state(p, 0).await;
    let pc = p.clone();
    let stale = tokio::spawn(async move { write(&pc, 0, 0, Bytes::from_static(b"STALE")).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let s0b = state(p, 0).await;
    // 5. heal: node 0 hears term 9
    net.isolate(0, false);
    let healed = wait_until(Duration::from_secs(5), || async { state(p, 0).await.term == 9 && state(p, 0).await.committed == state(p, 1).await.committed }).await;
    let inflight = tokio::time::timeout(Duration::from_secs(2), inflight).await;
    let stale = tokio::time::timeout(Duration::from_secs(2), stale).await;
    let fwd_committed_inflight = on(p, 1, |n| (committed_has(n, b"INFLIGHT"), committed_has(n, b"STALE"))).await;
    let old_writer_committed = matches!(inflight, Ok(Ok(Ok(())))) || matches!(stale, Ok(Ok(Ok(())))) || fwd_committed_inflight != (None, None);
    out.insert("old_writer_write_committed_after_new_lease".into(), json!(old_writer_committed));
    out.insert("3_stale_writer_node0".into(), json!({
        "state_before_stale_write": s0, "state_after_local_append": s0b,
        "healed": healed, "state_after_heal": state(p, 0).await,
        "inflight_write_result": fj(&inflight), "stale_write_result": fj(&stale),
        "inflight_or_stale_in_new_leader_committed_log": format!("{fwd_committed_inflight:?}"),
    }));

    // 6. raw RawNode::propose on the stale node (bypassing the application's leader check): forwarded?
    let r = on(p, 0, |n| n.groups[0].rn.propose(vec![], b"STALE-FWD".to_vec()).map_err(|e| e.to_string())).await;
    let fwd = wait_until(Duration::from_secs(2), || async { on(p, 1, |n| committed_has(n, b"STALE-FWD")).await.is_some() }).await;
    out.insert("4_raw_propose_on_old_writer_after_stepdown".into(), json!({"propose": fr(&r), "committed_by_new_leader": fwd,
        "at": format!("{:?}", on(p, 1, |n| committed_has(n, b"STALE-FWD")).await)}));

    // 7. lease holder whose log is behind: node 0 isolated while node 1 commits with node 2; then node 1 dies and
    //    the lease (seq 12) goes to node 0.
    net.isolate(0, true);
    let w = write(p, 1, 0, Bytes::from_static(b"WHILE-0-AWAY")).await;
    net.isolate(1, true);
    net.isolate(0, false);
    let r = take_lease(p, 0, 12).await;
    let lead = wait_until(Duration::from_secs(1), || async { state(p, 0).await.role == "Leader" }).await;
    let w0 = tokio::time::timeout(Duration::from_millis(500), write(p, 0, 0, Bytes::from_static(b"BEHIND"))).await;
    let s_after = [state(p, 0).await, state(p, 2).await];
    // remedy available in raft-rs: the up-to-date node 2 campaigns (an election the lease did not choose), then
    // transfers leadership to the lease holder once it has caught up
    let r2 = take_lease(p, 2, 13).await;
    let lead2 = wait_until(Duration::from_secs(2), || async { state(p, 2).await.role == "Leader" }).await;
    on(p, 2, |n| n.groups[0].rn.transfer_leader(1)).await;
    let lead0b = wait_until(Duration::from_secs(2), || async { state(p, 0).await.role == "Leader" }).await;
    let w0b = write(p, 0, 0, Bytes::from_static(b"AFTER-TRANSFER")).await;
    out.insert("5_lease_holder_behind".into(), json!({
        "write_on_node1_while_node0_away": fw(&w), "take_lease_node0_seq12": fr(&r), "node0_became_leader": lead,
        "write_on_node0": ft(&w0), "states_node0_node2": s_after,
        "remedy": {"node2_campaign_seq13": fr(&r2), "node2_leader": lead2, "transfer_leader_to_node0": lead0b,
                   "node0_state": state(p, 0).await, "write_on_node0": fw(&w0b)},
    }));
    serde_json::Value::Object(out)
}

async fn trim(p: &Peers, net: &Arc<Net>, files: &[Arc<LogFile>]) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    take_lease(p, 0, 2).await.unwrap();
    assert!(wait_until(Duration::from_secs(5), || async { state(p, 0).await.role == "Leader" }).await);
    for i in 0..20 {
        write(p, 0, 0, Bytes::from(format!("a{i}"))).await.unwrap();
    }
    net.isolate(2, true);
    for i in 0..20 {
        write(p, 0, 0, Bytes::from(format!("b{i}"))).await.unwrap();
    }
    let s2 = state(p, 2).await;
    let s0 = state(p, 0).await;
    // "S3 confirmed the upload up to index k": k above node 2's last index, below the commit index
    let k = s0.committed - 5;
    for i in 0..2 {
        on(p, i, move |n| {
            let g = &mut n.groups[0];
            assert!(k <= g.rn.raft.raft_log.applied, "compact above applied");
            g.rn.mut_store().marker.store(k, Relaxed);
            g.rn.mut_store().mem.wl().compact(k).unwrap();
            n.file.append(node::rec(node::K_TRIM, 0, &[k], &[]), Box::new(|| {}));
        })
        .await;
    }
    let s0c = state(p, 0).await;
    net.isolate(2, false);
    for i in 0..5 {
        write(p, 0, 0, Bytes::from(format!("c{i}"))).await.unwrap();
    }
    let caught = wait_until(Duration::from_secs(5), || async { state(p, 2).await.last == state(p, 0).await.last }).await;
    let (snaps, log2) = on(p, 2, |n| (n.groups[0].snapshots_applied, n.groups[0].committed_log.iter().map(|x| x.0).collect::<Vec<_>>())).await;
    out.insert("node2_before".into(), json!(s2));
    out.insert("leader_before_trim".into(), json!(s0));
    out.insert("trim_index_k".into(), json!(k));
    out.insert("leader_after_compact".into(), json!(s0c));
    out.insert("node2_caught_up".into(), json!(caught));
    out.insert("node2_after".into(), json!(state(p, 2).await));
    out.insert("node2_snapshots_applied".into(), json!(snaps));
    out.insert("node2_committed_indices_handed_out".into(), json!(log2));
    out.insert("snapshot_data_bytes".into(), json!(0));
    out.insert("fsyncs".into(), json!(files.iter().map(|f| f.fsyncs.load(Relaxed)).collect::<Vec<_>>()));
    serde_json::Value::Object(out)
}
