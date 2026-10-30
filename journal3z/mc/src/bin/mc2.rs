//! Phase-2 plan §2f, route 1: the two-writer exhaustive check made to finish.
//!
//! The model, the transitions and the invariants are the phase-1 ones (`src/main.rs`, imported as module `p1`, not
//! copied): the real `NodeCore` / `WriterDb` code, 3 nodes, writers with epochs 1..=E, at most I indices, messages
//! delivered in any order, each at most once, or never. What changes is only how the states are searched:
//!
//! 1. **Dead messages are removed** from a state (`prune`). A message is dead when delivering it can never change
//!    anything, now or later; removing it merges states that differ only by such a message. The rules, each with
//!    the reason it holds forever:
//!    - W1: any message to a writer that is stale (a stale writer ignores everything, and stays stale);
//!    - W2: `Promise` or `Entries` to an active writer (only the fencing / reading phases use them; active is final
//!      until stale);
//!    - W3: an `Ack` to an active writer that changes nothing when delivered now, with `matched` ≤ the writer's
//!      `last` (the writer keeps max(matched) per node; `last` only grows);
//!    - N1: `Trim` with `upto` ≤ the node's trim point (the trim point only grows);
//!    - N2: `Read` of a writer that is active or stale (the node does not change on `Read`; its answer is dead by
//!      W1/W2);
//!    - N3: `Fence` / `Append` of a stale writer to a node that has promised a higher epoch (the node rejects, the
//!      answer goes to a stale writer). Not applied with the `no_fencing` or `forget_fence` mutations, where a node
//!      can accept it or its promise can go back.
//! 2. **Node symmetry** (`--symmetry`): the three nodes are interchangeable; a state is stored under the smallest key
//!    of its 6 renamings. Needs `WriterDb::rename_nodes` (core feature `mc`).
//! 3. **Parallel search**: N threads, one shared set of visited keys (64-bit hashes, as in phase 1).
//! 4. **Subsumption by pending messages** (`--subsume`): a state is skipped when an already visited state has the same
//!    nodes, writers, store and acknowledgements and a **superset** of its pending messages. Why it is sound: the
//!    invariants (`p1::check`) never look at pending messages; every transition's condition depends only on the
//!    non-message part (plus "message m is pending" for a delivery, which holds in the superset too); and every
//!    transition's effect on the messages is "remove the delivered one, add new ones" (then the same dead-message
//!    rule). So from the visited state the same transitions reach states with the same non-message part and a
//!    superset of messages: anything reachable from the skipped state is matched, with the same invariant values. A
//!    message never delivered is the same as a lost one, which the model allows anyway.
//!
//!   mc2 [--epochs 2] [--idx 2] [--mutation none|...] [--threads 4] [--symmetry] [--no-prune] [--max-states N]
#[path = "../main.rs"]
#[allow(dead_code)]
mod p1;

use journal3z_core::*;
use p1::{check, key, msg_epoch, successors, untag, Cfg, State, DB};
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// Keys are already hashes.
#[derive(Default)]
struct Pass(u64);
impl Hasher for Pass {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}
type Set = HashSet<u64, BuildHasherDefault<Pass>>;
type Antichains = std::collections::HashMap<u64, Vec<Vec<u64>>, BuildHasherDefault<Pass>>;

/// Key of everything but the pending messages, and the sorted keys of the pending messages.
fn split_key(s: &State) -> (u64, Vec<u64>) {
    use std::hash::Hash;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (&s.nodes, &s.writers, &s.store, &s.acked, &s.links).hash(&mut h);
    let mut m: Vec<u64> = s
        .msgs
        .iter()
        .map(|x| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            x.hash(&mut h);
            h.finish()
        })
        .collect();
    m.sort_unstable();
    (h.finish(), m)
}

fn superset(a: &[u64], b: &[u64]) -> bool {
    // a ⊇ b, both sorted
    let mut i = 0;
    for x in b {
        while i < a.len() && a[i] < *x {
            i += 1;
        }
        if i == a.len() || a[i] != *x {
            return false;
        }
        i += 1;
    }
    true
}

const SHARDS: usize = 256;
static NO_REJECT: AtomicBool = AtomicBool::new(false);

fn writer<'a>(s: &'a State, e: Epoch) -> Option<&'a WriterDb> {
    s.writers.get((e.max(1) - 1) as usize).and_then(|w| w.as_ref())
}

fn is_stale(w: &WriterDb) -> bool {
    matches!(w.phase, Phase::Stale(_))
}

/// Remove dead messages (rules in the module comment).
fn prune(s: &mut State, cfg: &Cfg) {
    let m = cfg.m;
    let mut dead = vec![];
    for (to, msg) in &s.msgs {
        let is_dead = if *to == 0 {
            let (from, inner) = untag(msg);
            if NO_REJECT.load(SeqCst) && matches!(inner, Msg::Reject { .. }) {
                // W0 (--no-reject): delivering a Reject only makes a writer stale, i.e. only takes behaviours away;
                // the invariants never look at the phase. Every behaviour after it is a behaviour without it.
                dead.push((*to, msg.clone()));
                continue;
            }
            let e = msg_epoch(&inner);
            match writer(s, e) {
                None => false,
                Some(w) if is_stale(w) => true, // W1
                Some(w) if w.is_active() => match &inner {
                    Msg::Promise { .. } | Msg::Entries { .. } => true, // W2
                    Msg::Ack { matched, .. } if *matched <= w.last => {
                        // W3
                        let mut c = w.clone();
                        let out = c.on_msg(from, inner.clone());
                        c == *w && out.msgs.is_empty() && out.events.is_empty()
                    }
                    _ => false,
                },
                _ => false,
            }
        } else {
            let n = &s.nodes[(*to - 1) as usize];
            let d = n.dbs.get(&DB).cloned().unwrap_or_default();
            match msg {
                Msg::Trim { upto, .. } => *upto <= d.trim || d.erased, // N1
                Msg::Read { epoch, .. } => writer(s, *epoch).map_or(false, |w| w.is_active() || is_stale(w)), // N2
                Msg::Fence { epoch, .. } | Msg::Append { epoch, .. } => {
                    // N3
                    !m.no_fencing && !m.forget_fence && writer(s, *epoch).map_or(false, is_stale) && (d.erased || *epoch < d.promised)
                }
                _ => false,
            }
        };
        if is_dead {
            dead.push((*to, msg.clone()));
        }
    }
    for x in dead {
        s.msgs.remove(&x);
    }
}

/// The 6 renamings of nodes 1..=3.
const PERMS: [[u32; 4]; 6] = [[0, 1, 2, 3], [0, 1, 3, 2], [0, 2, 1, 3], [0, 2, 3, 1], [0, 3, 1, 2], [0, 3, 2, 1]];

#[cfg(feature = "symmetry")]
fn rename(s: &State, p: &[u32; 4]) -> State {
    let mut t = s.clone();
    // nodes: position i holds node i+1; after renaming, node p[i+1] holds what node i+1 held
    let mut nodes = s.nodes.clone();
    for i in 1..=3u32 {
        let mut n = s.nodes[(i - 1) as usize].clone();
        n.id = p[i as usize];
        nodes[(p[i as usize] - 1) as usize] = n;
    }
    t.nodes = nodes;
    t.writers = s.writers.iter().map(|w| w.as_ref().map(|w| w.rename_nodes(&|x| p[x as usize]))).collect();
    t.msgs = s
        .msgs
        .iter()
        .map(|(to, m)| {
            if *to == 0 {
                let (from, inner) = untag(m);
                (0, p1::tag_from(p[from as usize], inner))
            } else {
                (p[*to as usize], m.clone())
            }
        })
        .collect();
    t
}

fn canon_key(s: &State, symmetry: bool) -> u64 {
    if !symmetry {
        return key(s);
    }
    #[cfg(feature = "symmetry")]
    {
        PERMS.iter().map(|p| key(&rename(s, p))).min().unwrap()
    }
    #[cfg(not(feature = "symmetry"))]
    {
        let _ = PERMS;
        panic!("--symmetry needs the `symmetry` feature (core feature `mc`)")
    }
}

struct Shared {
    seen: Vec<Mutex<Set>>,
    sub: Vec<Mutex<Antichains>>,
    subsume: bool,
    count: AtomicUsize,
    queue: Mutex<Vec<(State, u32)>>,
    cv: Condvar,
    idle: AtomicUsize,
    stop: AtomicBool,
    verdict: Mutex<Option<String>>,
    explored: AtomicUsize,
    max_depth: AtomicUsize,
    max_acked: AtomicUsize,
}

impl Shared {
    /// Subsumption: false if a visited state with the same non-message part has a superset of the messages.
    fn insert_state(&self, s: &State, symmetry: bool) -> bool {
        if !self.subsume {
            return self.insert(canon_key(s, symmetry));
        }
        let (core, msgs) = split_key(s);
        let mut g = self.sub[(core as usize) % SHARDS].lock().unwrap();
        let list = g.entry(core).or_default();
        if list.iter().any(|m| superset(m, &msgs)) {
            return false;
        }
        list.retain(|m| !superset(&msgs, m));
        list.push(msgs);
        self.count.fetch_add(1, SeqCst);
        true
    }

    fn insert(&self, k: u64) -> bool {
        let new = self.seen[(k as usize) % SHARDS].lock().unwrap().insert(k);
        if new {
            self.count.fetch_add(1, SeqCst);
        }
        new
    }
}

fn worker(sh: &Shared, cfg: &Cfg, threads: usize, prune_on: bool, symmetry: bool, max_states: usize) {
    let mut local: Vec<(State, u32)> = vec![];
    loop {
        if sh.stop.load(SeqCst) {
            return;
        }
        let Some((s, d)) = local.pop().or_else(|| {
            let mut q = sh.queue.lock().unwrap();
            loop {
                if sh.stop.load(SeqCst) {
                    return None;
                }
                if let Some(x) = q.pop() {
                    return Some(x);
                }
                let idle = sh.idle.fetch_add(1, SeqCst) + 1;
                if idle == threads {
                    sh.stop.store(true, SeqCst);
                    sh.cv.notify_all();
                    return None;
                }
                q = sh.cv.wait(q).unwrap();
                sh.idle.fetch_sub(1, SeqCst);
            }
        }) else {
            return;
        };
        let n = sh.explored.fetch_add(1, SeqCst);
        if std::env::var("MC2_SAMPLE").is_ok() && n % 1_000_000 == 0 {
            let mut c: std::collections::BTreeMap<String, usize> = Default::default();
            for (to, m) in &s.msgs {
                let k = format!("{}{:?}", if *to == 0 { "->W " } else { "->N " }, std::mem::discriminant(m));
                let name = format!("{m:?}");
                let name = name.split(|ch: char| ch == ' ' || ch == '{').next().unwrap_or("").to_string();
                let _ = k;
                *c.entry(format!("{}{}", if *to == 0 { "toW:" } else { "toN:" }, name)).or_default() += 1;
            }
            let ph: Vec<String> = s.writers.iter().map(|w| w.as_ref().map_or("-".into(), |w| format!("{:?}/last{}/commit{}", w.phase, w.last, w.commit))).collect();
            let logs: Vec<String> = s.nodes.iter().map(|x| x.dbs.get(&DB).map_or("empty".into(), |d| format!("p{} t{} {:?}", d.promised, d.trim, d.entries.iter().map(|e| (e.idx, e.epoch)).collect::<Vec<_>>()))).collect();
            eprintln!("SAMPLE depth {d} msgs {} {c:?} writers {ph:?} nodes {logs:?} store {:?}", s.msgs.len(), (s.store.epoch, s.store.applied));
        }
        sh.max_depth.fetch_max(d as usize, SeqCst);
        sh.max_acked.fetch_max(s.acked.len(), SeqCst);
        if let Err(e) = check(&s, cfg) {
            *sh.verdict.lock().unwrap() = Some(format!("VIOLATION at depth {d}: {e}"));
            sh.stop.store(true, SeqCst);
            sh.cv.notify_all();
            return;
        }
        if sh.count.load(SeqCst) >= max_states {
            sh.verdict.lock().unwrap().get_or_insert(format!("bounded: stopped at {max_states} states (not exhaustive)"));
            sh.stop.store(true, SeqCst);
            sh.cv.notify_all();
            return;
        }
        for mut t in successors(&s, cfg) {
            if prune_on {
                prune(&mut t, cfg);
            }
            if sh.insert_state(&t, symmetry) {
                local.push((t, d + 1));
            }
        }
        // share work when others are idle
        if local.len() > 8 && sh.idle.load(SeqCst) > 0 {
            let half = local.split_off(local.len() / 2);
            sh.queue.lock().unwrap().extend(half);
            sh.cv.notify_all();
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned().unwrap_or(d.to_string());
    let has = |k: &str| args.iter().any(|a| a == k);
    let cfg = Cfg { fifo: false, instant: false, ticks: false, dup: false, epochs: get("--epochs", "2").parse().unwrap(), max_idx: get("--idx", "2").parse().unwrap(), m: Mutations::parse(&get("--mutation", "none")).expect("mutation") };
    let threads: usize = get("--threads", "4").parse().unwrap();
    let max_states: usize = get("--max-states", "2000000000").parse().unwrap();
    let prune_on = !has("--no-prune");
    let symmetry = has("--symmetry");
    let subsume = has("--subsume");
    NO_REJECT.store(has("--no-reject"), SeqCst);
    assert!(!(subsume && symmetry), "--subsume and --symmetry together are not supported");
    let init = State {
        nodes: (1..=3).map(|i| NodeCore::new(i, cfg.m)).collect(),
        writers: vec![None; cfg.epochs as usize],
        store: p1::Store { epoch: 0, applied: 0, content: vec![] },
        msgs: Default::default(),
        links: Default::default(),
        acked: Default::default(),
    };
    let sh = Shared {
        seen: (0..SHARDS).map(|_| Mutex::new(Set::default())).collect(),
        sub: (0..SHARDS).map(|_| Mutex::new(Antichains::default())).collect(),
        subsume,
        count: AtomicUsize::new(0),
        queue: Mutex::new(vec![]),
        cv: Condvar::new(),
        idle: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
        verdict: Mutex::new(None),
        explored: AtomicUsize::new(0),
        max_depth: AtomicUsize::new(0),
        max_acked: AtomicUsize::new(0),
    };
    let t0 = Instant::now();
    sh.insert_state(&init, symmetry);
    sh.queue.lock().unwrap().push((init, 0));
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| worker(&sh, &cfg, threads, prune_on, symmetry, max_states));
        }
        sc.spawn(|| {
            let mut last = Instant::now();
            while !sh.stop.load(SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(200));
                if last.elapsed().as_secs() >= 30 {
                    last = Instant::now();
                    eprintln!("distinct {} explored {} depth {} acked {} {:.0}s", sh.count.load(SeqCst), sh.explored.load(SeqCst), sh.max_depth.load(SeqCst), sh.max_acked.load(SeqCst), t0.elapsed().as_secs_f64());
                }
            }
        });
    });
    let verdict = sh.verdict.lock().unwrap().clone().unwrap_or_else(|| "complete: no violation".into());
    println!(
        "{{\"checker\": \"mc2\", \"epochs\": {}, \"max_idx\": {}, \"mutation\": \"{}\", \"prune\": {prune_on}, \"symmetry\": {symmetry}, \"subsume\": {subsume}, \"threads\": {threads}, \"distinct_states\": {}, \"explored\": {}, \"max_depth\": {}, \"max_acked\": {}, \"verdict\": \"{}\", \"secs\": {:.1}}}",
        cfg.epochs,
        cfg.max_idx,
        get("--mutation", "none"),
        sh.count.load(SeqCst),
        sh.explored.load(SeqCst),
        sh.max_depth.load(SeqCst),
        sh.max_acked.load(SeqCst),
        verdict.replace('"', "'"),
        t0.elapsed().as_secs_f64()
    );
}
