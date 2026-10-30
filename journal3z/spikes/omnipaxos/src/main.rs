//! `omnipaxos` spike (OmniPaxos 0.2.4) on the common test bed. Modes: latency, idle, fence (C5), trim (C6).
//! Durability rule and message ordering: see `server.rs`. Storage adapter: `storage.rs`.
mod server;
mod storage;

use bytes::Bytes;
use j3z_harness::{closed_loop, idle, Args, DelayLine, WriteFn};
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::messages::sequence_paxos::{PaxosMessage, PaxosMsg, Prepare};
use omnipaxos::messages::Message;
use omnipaxos::util::LogEntry;
use serde_json::{json, Value};
use server::{Cluster, Opts, Server};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::E;

const CANDIDATE: &str = "omnipaxos";

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let dir = a.fresh_dir();
    let dl = DelayLine::new(a.delay_cfg());
    match a.mode.as_str() {
        "latency" => latency(&a, dl, &dir).await,
        "idle" => idle_mode(&a, dl, &dir).await,
        "fence" => fence(&a, dl, &dir).await,
        "trim" => trim(&a, dl, &dir).await,
        m => panic!("unknown mode {m} (latency|idle|fence|trim)"),
    }
    std::process::exit(0);
}

async fn latency(a: &Args, dl: Arc<DelayLine>, dir: &std::path::Path) {
    let opts = Opts::from_env(false);
    let t0 = Instant::now();
    let cl = Arc::new(Cluster::start(1, dl.clone(), dir, opts.clone()));
    assert!(cl.wait_ready(1, Duration::from_secs(10)).await, "no leader within 10 s");
    let startup = t0.elapsed().as_secs_f64();
    let leader = cl.ctl(0, 0, |s| s.op.get_current_leader()).await;
    assert_eq!(leader, Some((1, true)), "harness node 0 (pid 1) must be the leader");
    let c = cl.clone();
    let w: WriteFn = Arc::new(move |p: Bytes| {
        let rx = c.append(0, 0, p);
        Box::pin(async move { rx.await?.map(|_| ()) })
    });
    let f0 = cl.fsyncs();
    let r = closed_loop(w, a.conc, a.size, a.warmup, a.n, Some(dl)).await;
    let f1 = cl.fsyncs();
    let decided = cl.ctl(0, 0, |s| s.op.get_decided_idx()).await;
    let extra = json!({
        "omnipaxos": "0.2.4", "config": opts.json(), "leader_pid": 1, "startup_s": startup, "decided_idx": decided,
        "fsyncs_per_node_total_run": f1.iter().zip(&f0).map(|(x, y)| x - y).collect::<Vec<_>>(),
        "ack_rule": "decided_idx >= index of the append on node 0; the leader's own vote is only counted after its fdatasync (Accepted gated), followers reply Accepted only after fdatasync",
    });
    a.emit(CANDIDATE, extra, &r);
}

async fn idle_mode(a: &Args, dl: Arc<DelayLine>, dir: &std::path::Path) {
    let opts = Opts::from_env(false);
    let t0 = Instant::now();
    let cl = Cluster::start(a.groups, dl.clone(), dir, opts.clone());
    let timeout = Duration::from_secs(60 + a.groups as u64 / 10);
    let ok = cl.wait_ready(a.groups, timeout).await;
    let startup = t0.elapsed().as_secs_f64();
    let ready = cl.ready.count.load(SeqCst);
    if !ok {
        eprintln!("only {ready} of {} groups have a leader after {:.1} s", a.groups, startup);
    }
    // Let the start-up tail finish (the third server's Promise/AcceptSync/Accepted, first BLE round) before measuring.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let r = idle(&dl, a.groups, a.secs, startup).await;
    let extra = json!({
        "omnipaxos": "0.2.4", "config": opts.json(), "groups_with_leader": ready, "instances": 3 * a.groups, "settle_after_ready_s": 1,
        "files": "one LogFile per node shared by all groups", "fsyncs_per_node_total_run": cl.fsyncs(),
    });
    a.emit(CANDIDATE, extra, &r);
}

// ------------------------------------------------------------------------------------------------------- scenarios

fn tag(e: &E) -> String {
    String::from_utf8_lossy(&e.0).into_owned()
}

/// The server's log as "D:tag" (decided) / "U:tag" (accepted, undecided).
fn log_view(s: &Server) -> Vec<String> {
    let n = s.op.get_accepted_idx();
    let c = s.op.get_compacted_idx();
    s.op.read_entries(c..n)
        .unwrap_or_default()
        .into_iter()
        .map(|e| match e {
            LogEntry::Decided(e) => format!("D:{}", tag(&e)),
            LogEntry::Undecided(e) => format!("U:{}", tag(&e)),
            LogEntry::Trimmed(i) => format!("T:{i}"),
            o => format!("{o:?}"),
        })
        .collect()
}

fn state(s: &Server) -> Value {
    let p = s.op.get_promise();
    json!({"node": s.node, "promise": {"n": p.n, "pid": p.pid}, "leader": s.op.get_current_leader(), "decided": s.op.get_decided_idx(), "accepted": s.op.get_accepted_idx(), "log": log_view(s)})
}

async fn states(cl: &Cluster) -> Value {
    let mut v = Vec::new();
    for n in 0..3 {
        v.push(cl.ctl(0, n, |s| state(s)).await);
    }
    Value::Array(v)
}

/// Where `t` is decided in node `n`'s log (index, 1-based like OmniPaxos's decided_idx), if anywhere.
async fn decided_at(cl: &Cluster, n: usize, t: &'static str) -> Option<usize> {
    cl.ctl(0, n, move |s| {
        let v = s.op.read_decided_suffix(0).unwrap_or_default();
        v.iter().position(|e| matches!(e, LogEntry::Decided(e) if tag(e) == t)).map(|i| i + 1)
    })
    .await
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// Workaround (not an API): make this server's ballot number equal to the external epoch. OmniPaxos only offers
/// `try_become_leader`, which picks `promise.n + 1`; so first raise our own promise to `epoch - 1` with a forged
/// Prepare addressed to ourselves, then call it. Refuse when we already promised a ballot >= epoch (we are stale).
fn open_with_epoch(s: &mut Server, epoch: u32) -> Result<u32, String> {
    let p = s.op.get_promise();
    if p.n >= epoch {
        return Err(format!("refused locally: promised ballot n={} >= my epoch {epoch}", p.n));
    }
    let me = s.pid();
    let na = s.pending.lock().unwrap().accepted_round;
    let forged = Prepare { n: Ballot::with(1, epoch - 1, 0, me), decided_idx: s.op.get_decided_idx(), n_accepted: na, accepted_idx: s.op.get_accepted_idx() };
    s.op.handle_incoming(Message::SequencePaxos(PaxosMessage { from: me, to: me, msg: PaxosMsg::Prepare(forged) }));
    s.op.try_become_leader();
    Ok(s.op.get_promise().n)
}

/// Append only while leader in the accept phase with ballot n == epoch (never forwarded).
fn guarded_append(s: &mut Server, epoch: u32, t: &str) -> Result<usize, String> {
    if !s.is_leader() || s.op.get_promise().n != epoch {
        return Err(format!("refused locally: leader={:?} promise.n={} epoch={epoch}", s.op.get_current_leader(), s.op.get_promise().n));
    }
    s.op.append(E(Bytes::from(t.to_string()))).map_err(|e| format!("{e:?}"))?;
    Ok(s.op.get_accepted_idx())
}

async fn wait_leader(cl: &Cluster, node: usize) -> bool {
    for _ in 0..400 {
        if cl.ctl(0, node, |s| s.is_leader()).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

async fn fence(a: &Args, dl: Arc<DelayLine>, dir: &std::path::Path) {
    let opts = Opts::from_env(true); // BLE off: leadership only from outside (try_become_leader)
    let cl = Cluster::start(1, dl, dir, opts.clone());
    assert!(cl.wait_ready(1, Duration::from_secs(10)).await);
    for i in 0..3 {
        cl.append(0, 0, Bytes::from(format!("w1-{i}"))).await.unwrap().unwrap();
    }

    // S1: library-native. The lease moves to node 1, which calls try_become_leader while node 0 appends.
    let inflight = cl.append(0, 0, Bytes::from_static(b"s1-stale-inflight"));
    cl.ctl_nowait(0, 1, |s| s.op.try_become_leader());
    let s1_new_leader = wait_leader(&cl, 1).await;
    settle().await;
    let inflight_ack = match tokio::time::timeout(Duration::from_millis(300), inflight).await {
        Ok(Ok(Ok(i))) => json!({"acked_at_index": i}),
        Ok(Ok(Err(e))) => json!({"error": e.to_string()}),
        _ => json!("no ack"),
    };
    let inflight_at = decided_at(&cl, 1, "s1-stale-inflight").await;
    // The old writer appends after it lost leadership: append() returns Ok and forwards it to the new leader.
    let late_ret = cl.ctl(0, 0, |s| format!("{:?}", s.op.append(E(Bytes::from_static(b"s1-stale-late"))))).await;
    settle().await;
    let late_at = decided_at(&cl, 1, "s1-stale-late").await;
    let s1 = json!({
        "new_leader_node1": s1_new_leader,
        "inflight_append_on_node0": {"ack": inflight_ack, "decided_at_on_new_leader": inflight_at},
        "late_append_on_node0_after_fence": {"append_returned": late_ret, "decided_at_on_new_leader": late_at,
            "how": "node 0 is now a follower: OmniPaxos forwards the proposal (ProposalForward) to the leader, which commits it"},
        "states": states(&cl).await,
    });

    // S2: library-native. The stale writer (node 0) runs its "open" late: try_become_leader picks promise.n + 1, so it
    // outranks the newer lease holder and takes leadership back.
    cl.ctl_nowait(0, 0, |s| s.op.try_become_leader());
    let s2_old_leader_again = wait_leader(&cl, 0).await;
    let r_new = cl.ctl(0, 1, |s| format!("{:?}", s.op.append(E(Bytes::from_static(b"s2-new-writer"))))).await;
    let ack_old = cl.append(0, 0, Bytes::from_static(b"s2-stale-after-reopen"));
    let ack_old = tokio::time::timeout(Duration::from_millis(500), ack_old).await;
    settle().await;
    let s2 = json!({
        "stale_node0_leader_again": s2_old_leader_again,
        "stale_append_acked": matches!(ack_old, Ok(Ok(Ok(_)))),
        "stale_append_decided_at": decided_at(&cl, 0, "s2-stale-after-reopen").await,
        "new_writer_append_returned": r_new, "new_writer_append_decided_at": decided_at(&cl, 0, "s2-new-writer").await,
        "states": states(&cl).await,
    });

    // S3: workaround: ballot number := lease epoch (forged self-Prepare), BLE off, appends only as leader with that
    // ballot (never forwarded), stale open refused locally. Node 0 opens with epoch 9; the lease then moves to node 1
    // with epoch 10, while node 0 keeps appending.
    let o9 = cl.ctl(0, 0, |s| open_with_epoch(s, 9)).await;
    let ok9 = wait_leader(&cl, 0).await;
    let w9 = cl.ctl(0, 0, |s| guarded_append(s, 9, "s3-e9-ok")).await;
    settle().await;
    // Cut node 1 -> node 0 so the stale writer never hears about epoch 10 (worst case: it keeps believing it leads).
    cl.net.cut[1][0].store(true, SeqCst);
    let o10 = cl.ctl(0, 1, |s| open_with_epoch(s, 10)).await;
    let inflight9 = cl.ctl(0, 0, |s| guarded_append(s, 9, "s3-e9-inflight")).await;
    let ok10 = wait_leader(&cl, 1).await;
    let w10 = cl.ctl(0, 1, |s| guarded_append(s, 10, "s3-e10-barrier")).await;
    let late9 = cl.ctl(0, 0, |s| guarded_append(s, 9, "s3-e9-late-partitioned")).await;
    settle().await;
    let node0_view = cl.ctl(0, 0, |s| state(s)).await;
    cl.net.cut[1][0].store(false, SeqCst);
    // Reconnect: OmniPaxos needs `reconnected` to resync a follower that missed messages.
    cl.ctl(0, 1, |s| s.op.reconnected(1)).await;
    cl.ctl(0, 0, |s| s.op.reconnected(2)).await;
    settle().await;
    let reopen9 = cl.ctl(0, 0, |s| open_with_epoch(s, 9)).await;
    let after9 = cl.ctl(0, 0, |s| guarded_append(s, 9, "s3-e9-after-reconnect")).await;
    settle().await;
    let s3 = json!({
        "open_epoch9_node0": o9, "node0_leader": ok9, "append_e9": w9, "decided_e9_ok": decided_at(&cl, 1, "s3-e9-ok").await,
        "open_epoch10_node1": o10, "node1_leader": ok10, "append_e10": w10,
        "e9_inflight": {"append": inflight9, "decided_at": decided_at(&cl, 1, "s3-e9-inflight").await},
        "e9_late_while_partitioned_from_node1": {"append": late9, "decided_at": decided_at(&cl, 1, "s3-e9-late-partitioned").await},
        "node0_view_while_partitioned": node0_view,
        "stale_reopen_epoch9_after_reconnect": reopen9, "e9_append_after_reconnect": after9,
        "e9_after_reconnect_decided_at": decided_at(&cl, 1, "s3-e9-after-reconnect").await,
        "states": states(&cl).await,
    });

    let verdict = json!({
        "library_native": "FAIL: no way to give a server a ballot from outside (try_become_leader uses promise.n + 1; BLE elects on its own unless its timeout is disabled); a stale writer's append is forwarded to and committed by the new leader (S1), and a stale writer that re-opens outranks the newer lease holder (S2)",
        "with_workaround": "fencing by epoch works only through a forged internal Prepare message plus local guards (S3); not a supported API",
    });
    a.emit(CANDIDATE, json!({"omnipaxos": "0.2.4", "config": opts.json()}), &json!({"S1_native_takeover": s1, "S2_native_stale_reopen": s2, "S3_epoch_ballot_workaround": s3, "verdict": verdict}));
}

/// C6: trim with all servers up, then with node 2 down (trim beyond what node 2 accepted is refused).
async fn trim(a: &Args, dl: Arc<DelayLine>, dir: &std::path::Path) {
    let opts = Opts::from_env(true);
    let cl = Cluster::start(1, dl, dir, opts.clone());
    assert!(cl.wait_ready(1, Duration::from_secs(10)).await);
    let p = || Bytes::from_static(b"x");
    for _ in 0..100 {
        cl.append(0, 0, p()).await.unwrap().unwrap();
    }
    settle().await;
    let t50 = cl.ctl(0, 0, |s| format!("{:?}", s.op.trim(Some(50)))).await;
    settle().await;
    let c1: Vec<usize> = futures::future::join_all((0..3).map(|n| cl.ctl(0, n, |s| s.op.get_compacted_idx()))).await;
    for n in 0..3 {
        cl.net.cut[n][2].store(true, SeqCst);
        cl.net.cut[2][n].store(true, SeqCst);
    }
    for _ in 0..100 {
        cl.append(0, 0, p()).await.unwrap().unwrap();
    }
    settle().await;
    let decided = cl.ctl(0, 0, |s| s.op.get_decided_idx()).await;
    let t150 = cl.ctl(0, 0, |s| format!("{:?}", s.op.trim(Some(150)))).await;
    let t_follower = cl.ctl(0, 1, |s| format!("{:?}", s.op.trim(Some(80)))).await;
    let t_none = cl.ctl(0, 0, |s| format!("{:?}", s.op.trim(None))).await;
    settle().await;
    let c2: Vec<usize> = futures::future::join_all((0..3).map(|n| cl.ctl(0, n, |s| s.op.get_compacted_idx()))).await;
    let r = json!({
        "trim_50_all_up": t50, "compacted_after": c1,
        "node2_down_then_decided": decided,
        "trim_150_with_node2_down (S3 marker = 150)": t150,
        "trim_on_follower": t_follower,
        "trim_none (= min accepted by ALL servers)": t_none, "compacted_after_down": c2,
        "snapshot_api": "snapshot(Some(i), local_only) compacts a node locally up to its decided index without asking the others, but needs Entry::Snapshot with use_snapshots() = true, and then log sync to a lagging node sends a snapshot (state transfer) instead of entries: not run (NoSnapshot panics in create)",
    });
    a.emit(CANDIDATE, json!({"omnipaxos": "0.2.4", "config": opts.json()}), &r);
}
