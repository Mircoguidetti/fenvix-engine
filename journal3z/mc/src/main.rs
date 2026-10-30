//! Exhaustive model checking of the journal3z protocol core — the same `NodeCore` / `WriterDb` code the simulator and
//! the custom spike run — for one database, 3 nodes, writers with epochs 1..=E, at most I indices.
//!
//! State = the 3 nodes' state (durable: a node replies after fdatasync, so handling + persisting is one step), every
//! writer's state, the object store (writer fence, marker, content), the set of messages ever sent (never removed:
//! any message can be delivered late, several times, in any order, or never), and the acknowledged writes.
//! Actions: a writer opens (store fence + marker read + FENCE), a message is delivered, a client write, a retransmit
//! tick, an upload of a prefix (only by the store's current writer) followed by TRIM messages.
//! Every reachable state is checked against the invariants of docs/journal3z-design.md §4:
//! (a) durability (recovery by rule R3 from EVERY 2 of 3 nodes, or S3), (b2) one ack per index, (b3) acks of an
//! older epoch are below a newer committed barrier, (c1) log matching, (d) trim never passes the S3 marker and S3
//! agrees with the acks.
//!
//!   journal3z-mc [--epochs 2] [--idx 2] [--mutation none|no_fencing|...] [--max-states N]
use bytes::Bytes;
use journal3z_core::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashSet};
use std::hash::{Hash, Hasher};

pub(crate) const DB: DbId = 1;
pub(crate) const W: Addr = 1000;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Store {
    pub(crate) epoch: Epoch,
    pub(crate) applied: Idx,
    pub(crate) content: Vec<(Epoch, bool)>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct State {
    pub(crate) nodes: Vec<NodeCore>,
    pub(crate) writers: Vec<Option<WriterDb>>,
    pub(crate) store: Store,
    /// (to: node id or 0 = writer of msg epoch, msg)
    pub(crate) msgs: BTreeSet<(u32, Msg)>,
    /// FIFO mode: one queue per (from, to) link, as one TCP connection per writer-node pair (writer address 100 + epoch)
    pub(crate) links: std::collections::BTreeMap<(u32, u32), Vec<Msg>>,
    pub(crate) acked: BTreeSet<(Idx, Epoch)>,
}

pub(crate) fn key(s: &State) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

pub(crate) fn data(e: Epoch, i: Idx) -> Bytes {
    Bytes::from(vec![e as u8, i as u8])
}

pub(crate) fn msg_epoch(m: &Msg) -> Epoch {
    match m {
        Msg::Promise { epoch, .. } | Msg::Reject { epoch, .. } | Msg::Entries { epoch, .. } | Msg::Ack { epoch, .. } | Msg::Mismatch { epoch, .. } => *epoch,
        _ => 0,
    }
}

pub(crate) struct Cfg {
    pub(crate) fifo: bool,
    pub(crate) instant: bool,
    pub(crate) ticks: bool,
    pub(crate) dup: bool,
    pub(crate) epochs: u64,
    pub(crate) max_idx: Idx,
    pub(crate) m: Mutations,
}

/// The commit hint in APPEND is ignored by nodes: normalize it so equal messages are one message.
fn norm(m: Msg) -> Msg {
    match m {
        Msg::Append { db, epoch, applied, prev_idx, prev_epoch, entries, .. } => Msg::Append { db, epoch, applied, prev_idx, prev_epoch, entries, commit: 0 },
        x => x,
    }
}

fn apply_out(s: &mut State, e: Epoch, out: WOut, cfg: &Cfg) {
    for (n, m) in out.msgs {
        if cfg.fifo {
            s.links.entry((100 + e as u32, n)).or_default().push(norm(m));
        } else {
            s.msgs.insert((n, norm(m)));
        }
    }
    for ev in out.events {
        if let WEvent::Committed(c) = ev {
            let w = s.writers[(e - 1) as usize].as_ref().unwrap();
            for (i, en) in w.log.range(..=c) {
                if en.epoch == e && !en.barrier {
                    s.acked.insert((*i, e));
                }
            }
        }
    }
    let _ = cfg;
}

pub(crate) fn successors(s: &State, cfg: &Cfg) -> Vec<State> {
    let mut out = vec![];
    // a writer opens
    for e in 1..=cfg.epochs {
        if s.writers[(e - 1) as usize].is_none() && s.store.epoch < e {
            let mut t = s.clone();
            t.store.epoch = e;
            let (w, o) = WriterDb::open(DB, e, vec![1, 2, 3], t.store.applied, cfg.m);
            t.writers[(e - 1) as usize] = Some(w);
            apply_out(&mut t, e, o, cfg);
            out.push(t);
        }
    }
    // FIFO links: deliver the head of any link, or lose it
    for (&(from, to), q) in &s.links {
        if q.is_empty() {
            continue;
        }
        let m = q[0].clone();
        let mut lost = s.clone();
        lost.links.get_mut(&(from, to)).unwrap().remove(0);
        if lost.links[&(from, to)].is_empty() {
            lost.links.remove(&(from, to));
        }
        let mut t = lost.clone();
        out.push(lost);
        if to >= 1 && to <= 3 {
            let o = t.nodes[(to - 1) as usize].handle(W, m);
            if o.broken.is_some() {
                continue;
            }
            for (_, r) in o.replies {
                t.links.entry((to, from)).or_default().push(r);
            }
        } else {
            let e = (to - 100) as Epoch;
            if let Some(w) = t.writers.get_mut((e - 1) as usize).and_then(|w| w.as_mut()) {
                let o = w.on_msg(from, m);
                apply_out(&mut t, e, o, cfg);
            }
        }
        out.push(t);
    }
    // deliver any pending message (consumed, unless --dup: then it stays and can be delivered again)
    for (to, m) in &s.msgs {
        let mut t = s.clone();
        if !cfg.dup {
            t.msgs.remove(&(*to, m.clone()));
        }
        if *to >= 1 && *to <= 3 {
            let o = t.nodes[(*to - 1) as usize].handle(W, m.clone());
            if o.broken.is_some() {
                continue;
            }
            if cfg.instant {
                // replies reach the writer in the same step, or are lost (a second successor)
                if !o.replies.is_empty() {
                    out.push(t.clone());
                }
                for (_, r) in o.replies {
                    let e = msg_epoch(&r);
                    if let Some(w) = t.writers.get_mut((e.max(1) - 1) as usize).and_then(|w| w.as_mut()) {
                        let wo = w.on_msg(*to, r);
                        apply_out(&mut t, e, wo, cfg);
                    }
                }
            } else {
                for (_, r) in o.replies {
                    t.msgs.insert((0, tag_from(*to, r)));
                }
            }
        } else {
            let (from, inner) = untag(m);
            let e = msg_epoch(&inner);
            let Some(w) = t.writers.get_mut((e.max(1) - 1) as usize).and_then(|w| w.as_mut()) else { continue };
            let o = w.on_msg(from, inner);
            apply_out(&mut t, e, o, cfg);
        }
        if t != *s {
            out.push(t);
        }
    }
    for e in 1..=cfg.epochs {
        let Some(w) = s.writers[(e - 1) as usize].as_ref() else { continue };
        if !w.is_active() {
            // retransmission while opening: only the Reading phase does anything new (re-fence after 5 ticks);
            // FENCE retransmissions are already in the message set, which never loses a message
            if !matches!(w.phase, Phase::Reading { .. }) {
                continue;
            }
            let mut t = s.clone();
            let o = t.writers[(e - 1) as usize].as_mut().unwrap().tick();
            apply_out(&mut t, e, o, cfg);
            if t != *s {
                out.push(t);
            }
            continue;
        }
        // client write
        if w.last < cfg.max_idx {
            let mut t = s.clone();
            let w = t.writers[(e - 1) as usize].as_mut().unwrap();
            let i = w.last + 1;
            w.append(data(e, i));
            let mut o = WOut::default();
            w.flush(&mut o);
            apply_out(&mut t, e, o, cfg);
            out.push(t);
        }
        // retransmission tick (Active: resend from matched + 1; adds APPEND variants after a mismatch)
        if cfg.ticks {
            let mut t = s.clone();
            let o = t.writers[(e - 1) as usize].as_mut().unwrap().tick();
            apply_out(&mut t, e, o, cfg);
            if t != *s {
                out.push(t);
            }
        }
        // upload a prefix (only the store's current writer), then TRIM
        if s.store.epoch == e {
            for k in (s.store.applied + 1)..=w.last {
                let mut t = s.clone();
                let w = t.writers[(e - 1) as usize].as_mut().unwrap();
                for i in (t.store.applied + 1)..=k {
                    let en = &w.log[&i];
                    t.store.content.push((en.epoch, en.barrier));
                }
                t.store.applied = k;
                let o = if cfg.m.trim_before_upload { w.trim_unconfirmed(w.last) } else { w.uploaded(k) };
                apply_out(&mut t, e, o, cfg);
                out.push(t);
            }
        }
    }
    out
}

/// Replies carry the sending node in a wrapper: encode it in the Mismatch/Ack/... by pairing (from, msg).
pub(crate) fn tag_from(from: u32, m: Msg) -> Msg {
    // store the sender in an Entries-free envelope: reuse Msg::Trim's db field is not possible; use a side encoding
    // via the db id (DB * 10 + from), decoded by `untag`.
    match m {
        Msg::Promise { epoch, last, last_epoch, .. } => Msg::Promise { db: DB * 10 + from as u64, epoch, last, last_epoch },
        Msg::Reject { epoch, promised, .. } => Msg::Reject { db: DB * 10 + from as u64, epoch, promised },
        Msg::Entries { epoch, ok, entries, .. } => Msg::Entries { db: DB * 10 + from as u64, epoch, ok, entries },
        Msg::Ack { epoch, matched, .. } => Msg::Ack { db: DB * 10 + from as u64, epoch, matched },
        Msg::Mismatch { epoch, hint, .. } => Msg::Mismatch { db: DB * 10 + from as u64, epoch, hint },
        x => x,
    }
}

pub(crate) fn untag(m: &Msg) -> (NodeId, Msg) {
    let from = (m.db() % 10) as NodeId;
    let m = match m.clone() {
        Msg::Promise { epoch, last, last_epoch, .. } => Msg::Promise { db: DB, epoch, last, last_epoch },
        Msg::Reject { epoch, promised, .. } => Msg::Reject { db: DB, epoch, promised },
        Msg::Entries { epoch, ok, entries, .. } => Msg::Entries { db: DB, epoch, ok, entries },
        Msg::Ack { epoch, matched, .. } => Msg::Ack { db: DB, epoch, matched },
        Msg::Mismatch { epoch, hint, .. } => Msg::Mismatch { db: DB, epoch, hint },
        x => x,
    };
    (from, m)
}

pub(crate) fn check(s: &State, cfg: &Cfg) -> Result<(), String> {
    let applied = s.store.applied;
    let logs: Vec<DbLog> = s.nodes.iter().map(|n| n.dbs.get(&DB).cloned().unwrap_or_default()).collect();
    // (d) trim never passes the marker
    for (i, l) in logs.iter().enumerate() {
        if l.trim > applied {
            return Err(format!("d: node {} trimmed to {} with S3 marker {applied}", i + 1, l.trim));
        }
    }
    for &(i, e) in &s.acked {
        // (b2)
        if s.acked.iter().any(|&(j, f)| j == i && f != e) {
            return Err(format!("b2: two acknowledged writes at index {i}"));
        }
        // (b3)
        for w in s.writers.iter().flatten() {
            if w.epoch > e && w.barrier > 0 && w.commit >= w.barrier && i >= w.barrier {
                return Err(format!("b3: write ({i}, epoch {e}) acknowledged at or above the committed barrier {} of epoch {}", w.barrier, w.epoch));
            }
        }
        // (a) / (d) durability
        if i <= applied {
            if s.store.content[(i - 1) as usize] != (e, false) {
                return Err(format!("a: acknowledged ({i}, epoch {e}) differs from S3 {:?}", s.store.content[(i - 1) as usize]));
            }
            continue;
        }
        for (a, b) in [(0usize, 1usize), (0, 2), (1, 2)] {
            let cands = [(a as NodeId, logs[a].last(), logs[a].last_epoch()), (b as NodeId, logs[b].last(), logs[b].last_epoch())];
            let src = choose_source(cands, applied, &Mutations::default());
            let got = src.and_then(|(n, _)| logs[n as usize].get(i)).map(|en| (en.epoch, en.barrier, en.data.clone()));
            if got != Some((e, false, data(e, i))) {
                return Err(format!("a: acknowledged ({i}, epoch {e}) not recoverable from nodes {}+{}: got {got:?}", a + 1, b + 1));
            }
        }
    }
    // (c1) log matching
    for a in 0..3 {
        for b in (a + 1)..3 {
            for x in &logs[a].entries {
                if let Some(y) = logs[b].get(x.idx) {
                    if y.epoch == x.epoch && y != x {
                        return Err(format!("c1: nodes {} and {} hold different entries at ({}, epoch {})", a + 1, b + 1, x.idx, x.epoch));
                    }
                }
            }
        }
    }
    let _ = cfg;
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned().unwrap_or(d.to_string());
    let cfg = Cfg { fifo: args.iter().any(|a| a == "--fifo"), instant: args.iter().any(|a| a == "--instant-replies"), ticks: args.iter().any(|a| a == "--ticks"), dup: args.iter().any(|a| a == "--dup"), epochs: get("--epochs", "2").parse().unwrap(), max_idx: get("--idx", "2").parse().unwrap(), m: Mutations::parse(&get("--mutation", "none")).expect("mutation") };
    let shuffle: u64 = get("--shuffle-seed", "0").parse().unwrap();
    let mut rng: u64 = shuffle.wrapping_mul(0x9E3779B97F4A7C15) | 1;
    let max_states: usize = get("--max-states", "60000000").parse().unwrap();
    let init = State {
        nodes: (1..=3).map(|i| NodeCore::new(i, cfg.m)).collect(),
        writers: vec![None; cfg.epochs as usize],
        store: Store { epoch: 0, applied: 0, content: vec![] },
        msgs: BTreeSet::new(),
        links: Default::default(),
        acked: BTreeSet::new(),
    };
    let t0 = std::time::Instant::now();
    // depth-first with only state hashes kept (64-bit; a collision could hide a state: probability ~ n^2 / 2^65)
    let mut seen: HashSet<u64> = HashSet::new();
    let mut q: Vec<(State, u32)> = Vec::new();
    seen.insert(key(&init));
    q.push((init, 0));
    let (mut explored, mut max_depth, mut max_acked) = (0usize, 0u32, 0usize);
    let mut verdict = "complete: no violation".to_string();
    while let Some((s, d)) = q.pop() {
        explored += 1;
        max_depth = max_depth.max(d);
        max_acked = max_acked.max(s.acked.len());
        if let Err(e) = check(&s, &cfg) {
            verdict = format!("VIOLATION at depth {d}: {e}");
            break;
        }
        if explored % 2_000_000 == 0 {
            eprintln!("explored {explored} distinct {} depth {d} stack {} acked {max_acked} {:.0}s", seen.len(), q.len(), t0.elapsed().as_secs_f64());
        }
        if seen.len() >= max_states {
            verdict = format!("bounded: stopped at {max_states} states (not exhaustive)");
            break;
        }
        let mut succ = successors(&s, &cfg);
        if shuffle != 0 {
            // randomized order (swarm search): a different corner of the space per seed; not exhaustive
            for i in (1..succ.len()).rev() {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                succ.swap(i, (rng % (i as u64 + 1)) as usize);
            }
        }
        for t in succ {
            if seen.insert(key(&t)) {
                q.push((t, d + 1));
            }
        }
    }
    println!(
        "{{\"epochs\": {}, \"max_idx\": {}, \"dup\": {}, \"fifo\": {}, \"instant_replies\": {}, \"ticks\": {}, \"shuffle_seed\": {}, \"mutation\": \"{}\", \"distinct_states\": {}, \"explored\": {}, \"max_depth\": {}, \"max_acked\": {}, \"verdict\": \"{}\", \"secs\": {:.1}}}",
        cfg.epochs, cfg.max_idx, cfg.dup, cfg.fifo, cfg.instant, cfg.ticks, shuffle, get("--mutation", "none"), seen.len(), explored, max_depth, max_acked, verdict.replace('"', "'"), t0.elapsed().as_secs_f64()
    );
}
