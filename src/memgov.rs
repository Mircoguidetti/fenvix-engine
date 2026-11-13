//! One memtable budget for the whole process (review of big-speed, 2026-11-13, error 2).
//!
//! A database's writes wait in its memtable (and its WAL buffer) until SlateDB flushes them to an L0 SST: at
//! `--memtable-max-mb` (8 MiB), after 4096 WAL flushes, or at our age rule (`MEMTABLE_MAX_AGE_SECS`, 600 s). SlateDB
//! bounds this per database (`max_unflushed_bytes`, 4 memtables), and nothing bounded the sum across databases: 200
//! databases written 6 MiB each held 1.2 GB of memtables, and the server was OOM-killed under 1 GiB with
//! `--memory-limit-mb 1024` (MEASURED by the reviewer; `manydb.py`).
//!
//! The governor counts, per open writer, the bytes written and not yet in an L0 SST (in chunks of `CHUNK` bytes, each
//! with the highest sequence number in it; a chunk is released once the writer's manifest records an L0 flush at or
//! past that number, `last_l0_seq`). When the process-wide sum passes the budget (`membudget::Budget::memtables`), it
//! flushes the memtables of the largest writers first (SlateDB's `FlushType::MemTable`, the same call as the age rule)
//! until the sum is projected under 3/4 of the budget. When the sum passes the hard limit (1.5 x the budget), a new
//! write waits until flushes bring it back under (`admit`), as SlateDB's own backpressure does for one database.
//!
//! Exactness and durability do not change: a memtable flush only writes an L0 SST of rows that are already in the WAL
//! (acknowledged writes are durable before their ack, as before). The wait is bounded (`MAX_WAIT`): past it the write
//! goes on and the wait is counted (`ADMIT_TIMEOUTS`), so a slow store never deadlocks writes behind a flush that
//! needs the same database's compaction; SlateDB's per-database backpressure still applies then.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Duration;

/// Bytes of the budget (0: no governor). Set from the memory budget at start (`--memtables-total-mb` wins).
pub static BUDGET_BYTES: AtomicU64 = AtomicU64::new(0);
/// Bytes counted per write entry beyond its key and value (SlateDB's skip map node and sequence number).
pub const ENTRY_OVERHEAD: u64 = 64;
/// A writer's count is kept in chunks of this many bytes.
pub const CHUNK: u64 = 1 << 20;
/// Longest wait of one write at the hard limit.
pub const MAX_WAIT: Duration = Duration::from_secs(10);

/// Bytes counted now across every writer (serverStatus).
pub static TOTAL: AtomicU64 = AtomicU64::new(0);
pub static PEAK: AtomicU64 = AtomicU64::new(0);
/// Memtable flushes the governor started.
pub static FLUSHES: AtomicU64 = AtomicU64::new(0);
pub static FLUSH_ERRORS: AtomicU64 = AtomicU64::new(0);
/// Writes that waited at the hard limit, and those whose wait reached `MAX_WAIT`.
pub static ADMIT_WAITS: AtomicU64 = AtomicU64::new(0);
pub static ADMIT_TIMEOUTS: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct Acc {
    /// (highest seq in the chunk, bytes), oldest first.
    chunks: VecDeque<(u64, u64)>,
    bytes: u64,
}

#[derive(Default)]
struct State {
    dbs: HashMap<String, Acc>,
    flushing: HashSet<String>,
}

static STATE: std::sync::LazyLock<Mutex<State>> = std::sync::LazyLock::new(Default::default);
static WAKE: std::sync::LazyLock<tokio::sync::Notify> = std::sync::LazyLock::new(tokio::sync::Notify::new);
static DRAINED: std::sync::LazyLock<tokio::sync::Notify> = std::sync::LazyLock::new(tokio::sync::Notify::new);
static STARTED: AtomicBool = AtomicBool::new(false);

pub fn budget() -> u64 {
    BUDGET_BYTES.load(Relaxed)
}

pub fn hard() -> u64 {
    let b = budget();
    b + b / 2
}

/// Bytes a batch of (key length, value length) adds to its memtable.
pub fn batch_bytes<'a>(sizes: impl Iterator<Item = (usize, usize)>) -> u64 {
    sizes.map(|(k, v)| (k + v) as u64 + ENTRY_OVERHEAD).sum()
}

/// A write of `bytes` with sequence number `seq` reached writer `id`'s memtable.
pub fn note(id: &str, seq: u64, bytes: u64) {
    if budget() == 0 || bytes == 0 {
        return;
    }
    start();
    let total = {
        let mut st = STATE.lock().unwrap();
        let a = st.dbs.entry(id.to_string()).or_default();
        match a.chunks.back_mut() {
            Some((s, b)) if *b < CHUNK => {
                *s = (*s).max(seq);
                *b += bytes;
            }
            _ => a.chunks.push_back((seq, bytes)),
        }
        a.bytes += bytes;
        TOTAL.fetch_add(bytes, Relaxed) + bytes
    };
    PEAK.fetch_max(total, Relaxed);
    if total > budget() {
        WAKE.notify_one();
    }
}

/// Writer `id` closed (its memtable was flushed by the close or dropped with the process's ownership).
pub fn forget(id: &str) {
    let mut st = STATE.lock().unwrap();
    if let Some(a) = st.dbs.remove(id) {
        TOTAL.fetch_sub(a.bytes, Relaxed);
    }
    drop(st);
    DRAINED.notify_waiters();
}

/// Release the chunks of `id` whose rows are all in L0 SSTs (`last_l0_seq` at or past their highest seq).
fn reclaim(st: &mut State, id: &str, last_l0_seq: u64) {
    let Some(a) = st.dbs.get_mut(id) else { return };
    while let Some(&(s, b)) = a.chunks.front() {
        if s > last_l0_seq {
            break;
        }
        a.chunks.pop_front();
        a.bytes -= b;
        TOTAL.fetch_sub(b, Relaxed);
    }
    if a.chunks.is_empty() {
        st.dbs.remove(id);
    }
}

/// Wait, before a write, while the process's memtables are over the hard limit (at most `MAX_WAIT`).
pub async fn admit() {
    let h = hard();
    if h == 0 || TOTAL.load(Relaxed) <= h {
        return;
    }
    ADMIT_WAITS.fetch_add(1, Relaxed);
    let deadline = tokio::time::Instant::now() + MAX_WAIT;
    while TOTAL.load(Relaxed) > h {
        WAKE.notify_one();
        let now = tokio::time::Instant::now();
        if now >= deadline {
            ADMIT_TIMEOUTS.fetch_add(1, Relaxed);
            eprintln!("memtable budget: a write waited {} s at {} MiB of memtables (hard limit {} MiB); it goes on", MAX_WAIT.as_secs(), TOTAL.load(Relaxed) >> 20, h >> 20);
            return;
        }
        // a flush ends with `DRAINED`; the timeout re-checks in case a release came from a writer's own flush
        let _ = tokio::time::timeout((deadline - now).min(Duration::from_millis(50)), DRAINED.notified()).await;
    }
}

fn start() {
    if STARTED.swap(true, Relaxed) {
        return;
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        STARTED.store(false, Relaxed);
        return;
    };
    rt.spawn(async {
        loop {
            let _ = tokio::time::timeout(Duration::from_millis(100), WAKE.notified()).await;
            govern_once().await;
        }
    });
}

/// One pass: release what SlateDB flushed by itself, then flush the largest memtables while over the budget.
pub async fn govern_once() {
    let b = budget();
    if b == 0 {
        return;
    }
    // (the writers' handles stay in lifecycle.rs: only their L0 sequence numbers and a memtable flush are asked for)
    let dbs: HashMap<String, u64> = crate::lifecycle::writer_l0_seqs();
    let mut st = STATE.lock().unwrap();
    let ids: Vec<String> = st.dbs.keys().cloned().collect();
    for id in ids {
        match dbs.get(&id) {
            Some(&l0) => reclaim(&mut st, &id, l0),
            // the writer closed without telling us (a lost lease): its memtable is gone with its `Db`
            None => {
                if let Some(a) = st.dbs.remove(&id) {
                    TOTAL.fetch_sub(a.bytes, Relaxed);
                }
            }
        }
    }
    let total = TOTAL.load(Relaxed);
    if total <= b {
        drop(st);
        DRAINED.notify_waiters();
        return;
    }
    // largest first, until the rest is projected under 3/4 of the budget
    let mut v: Vec<(u64, String)> = st.dbs.iter().filter(|(id, _)| !st.flushing.contains(*id)).map(|(id, a)| (a.bytes, id.clone())).collect();
    v.sort_unstable_by(|x, y| y.cmp(x));
    let in_flight: u64 = st.flushing.iter().filter_map(|id| st.dbs.get(id)).map(|a| a.bytes).sum();
    let mut projected = total.saturating_sub(in_flight);
    let mut picked = Vec::new();
    for (bytes, id) in v {
        if projected <= b / 4 * 3 {
            break;
        }
        if !dbs.contains_key(&id) {
            continue;
        }
        projected = projected.saturating_sub(bytes);
        st.flushing.insert(id.clone());
        picked.push(id);
    }
    drop(st);
    for id in picked {
        FLUSHES.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let r = crate::lifecycle::flush_writer_memtable_l0(&id).await;
            if let Err(e) = &r {
                FLUSH_ERRORS.fetch_add(1, Relaxed);
                eprintln!("memtable flush (process budget) of tenant {id}: {e:#}");
            }
            let mut st = STATE.lock().unwrap();
            st.flushing.remove(&id);
            match r {
                Ok(Some(l0)) => reclaim(&mut st, &id, l0),
                // the writer closed meanwhile: its count goes with it
                Ok(None) => {
                    if let Some(a) = st.dbs.remove(&id) {
                        TOTAL.fetch_sub(a.bytes, Relaxed);
                    }
                }
                Err(_) => {}
            }
            drop(st);
            DRAINED.notify_waiters();
            WAKE.notify_one();
        });
    }
}

/// (bytes counted now, writers counted, flushes in flight).
pub fn usage() -> (u64, usize, usize) {
    let st = STATE.lock().unwrap();
    (TOTAL.load(Relaxed), st.dbs.len(), st.flushing.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_release_by_l0_seq() {
        let mut st = State::default();
        let id = "memgov-unit";
        // three chunks: seqs up to 10, 20, 30
        for (seq, bytes) in [(10u64, CHUNK), (20, CHUNK), (30, CHUNK / 2)] {
            let a = st.dbs.entry(id.into()).or_default();
            a.chunks.push_back((seq, bytes));
            a.bytes += bytes;
            TOTAL.fetch_add(bytes, Relaxed);
        }
        let t0 = TOTAL.load(Relaxed);
        reclaim(&mut st, id, 15);
        assert_eq!(st.dbs[id].bytes, CHUNK + CHUNK / 2);
        assert_eq!(t0 - TOTAL.load(Relaxed), CHUNK);
        reclaim(&mut st, id, 30);
        assert!(!st.dbs.contains_key(id));
        assert_eq!(t0 - TOTAL.load(Relaxed), 2 * CHUNK + CHUNK / 2);
        assert_eq!(batch_bytes([(10usize, 100usize), (5, 0)].into_iter()), 110 + 5 + 2 * ENTRY_OVERHEAD);
    }
}
