//! The compaction pool of a cell (plan round 2, fix 7): one per process, shared by every database, at most `slots` jobs
//! at once (about 70% of the cores), jobs taken by priority.
//!
//! Priorities, highest first:
//! 0. absorption (and SlateDB's fold, which SlateDB's own compactor runs; it is never deferred): L0 must stay at or below
//!    8, or the writer stops. Absorption never waits for a slot: it runs at once, and counts as busy.
//! 1. erasures (24-hour deadline);
//! 2. delta tier merges;
//! 3. reseals (theta, R3, R2), which can wait;
//! 4. GC.
//!
//! Within one priority, first come first served. The pool bounds how many jobs run, not how much CPU each takes: the
//! bench runs the jobs on a runtime of `slots` threads, so the cap is also a CPU cap there (`bench`).

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Prio {
    Absorb = 0,
    Erase = 1,
    Merge = 2,
    Reseal = 3,
    Gc = 4,
}

pub const PRIOS: [Prio; 5] = [Prio::Absorb, Prio::Erase, Prio::Merge, Prio::Reseal, Prio::Gc];

struct Waiter {
    prio: Prio,
    seq: u64,
    tx: tokio::sync::oneshot::Sender<()>,
}

impl PartialEq for Waiter {
    fn eq(&self, o: &Self) -> bool {
        self.prio == o.prio && self.seq == o.seq
    }
}
impl Eq for Waiter {}
impl PartialOrd for Waiter {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Waiter {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap: the lowest priority number and then the lowest seq first
        o.prio.cmp(&self.prio).then(o.seq.cmp(&self.seq))
    }
}

#[derive(Default)]
struct State {
    busy: usize,
    seq: u64,
    waiting: BinaryHeap<Waiter>,
}

/// Counters per priority: jobs run, time waited for a slot, time held.
#[derive(Default)]
pub struct PrioStats {
    pub jobs: AtomicU64,
    pub wait_us: AtomicU64,
    pub max_wait_us: AtomicU64,
    pub held_us: AtomicU64,
}

pub struct Pool {
    slots: usize,
    st: Mutex<State>,
    pub stats: [PrioStats; 5],
    /// Highest number of jobs running at once.
    pub max_busy: AtomicU64,
}

impl Pool {
    pub fn new(slots: usize) -> Arc<Pool> {
        Arc::new(Pool { slots: slots.max(1), st: Mutex::new(State::default()), stats: Default::default(), max_busy: AtomicU64::new(0) })
    }

    /// The default size: 70% of the cores, at least 1.
    pub fn default_slots() -> usize {
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        ((n as f64 * 0.7).floor() as usize).max(1)
    }

    pub fn slots(&self) -> usize {
        self.slots
    }

    pub fn busy(&self) -> usize {
        self.st.lock().unwrap().busy
    }

    pub fn waiting(&self) -> usize {
        self.st.lock().unwrap().waiting.len()
    }

    /// Wait for a slot by priority (absorption never waits).
    pub async fn acquire(self: &Arc<Self>, prio: Prio) -> Permit {
        let t0 = Instant::now();
        let rx = {
            let mut st = self.st.lock().unwrap();
            if prio == Prio::Absorb || (st.busy < self.slots && st.waiting.is_empty()) {
                st.busy += 1;
                self.max_busy.fetch_max(st.busy as u64, Relaxed);
                None
            } else {
                let (tx, rx) = tokio::sync::oneshot::channel();
                st.seq += 1;
                let seq = st.seq;
                st.waiting.push(Waiter { prio, seq, tx });
                Some(rx)
            }
        };
        if let Some(rx) = rx {
            // the slot is handed over by `release` (busy already counted for us)
            let _ = rx.await;
        }
        let waited = t0.elapsed().as_micros() as u64;
        let s = &self.stats[prio as usize];
        s.jobs.fetch_add(1, Relaxed);
        s.wait_us.fetch_add(waited, Relaxed);
        s.max_wait_us.fetch_max(waited, Relaxed);
        Permit { pool: Arc::clone(self), prio, since: Instant::now() }
    }

    fn release(&self) {
        let mut st = self.st.lock().unwrap();
        st.busy -= 1;
        while st.busy < self.slots {
            let Some(w) = st.waiting.pop() else { break };
            st.busy += 1;
            self.max_busy.fetch_max(st.busy as u64, Relaxed);
            if w.tx.send(()).is_err() {
                st.busy -= 1; // the waiter was dropped
            }
        }
    }
}

pub struct Permit {
    pool: Arc<Pool>,
    prio: Prio,
    since: Instant,
}

impl Permit {
    pub fn held(&self) -> Duration {
        self.since.elapsed()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.pool.stats[self.prio as usize].held_us.fetch_add(self.since.elapsed().as_micros() as u64, Relaxed);
        self.pool.release();
    }
}

static GLOBAL: std::sync::OnceLock<Arc<Pool>> = std::sync::OnceLock::new();

/// The process's pool (`--sealed-pool-slots`, default 70% of the cores).
pub fn global() -> Arc<Pool> {
    Arc::clone(GLOBAL.get_or_init(|| Pool::new(Pool::default_slots())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn priorities_and_absorption_never_waits() {
        let p = Pool::new(1);
        let a = p.acquire(Prio::Reseal).await;
        // queue a reseal, a merge and an erasure; then release: the erasure goes first, then the merge
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut hs = Vec::new();
        for (i, pr) in [Prio::Reseal, Prio::Merge, Prio::Erase].into_iter().enumerate() {
            let (p, o) = (Arc::clone(&p), Arc::clone(&order));
            hs.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10 * i as u64)).await;
                let _g = p.acquire(pr).await;
                o.lock().unwrap().push(pr);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(p.waiting(), 3);
        // absorption runs although the only slot is taken
        let ab = tokio::time::timeout(Duration::from_millis(50), p.acquire(Prio::Absorb)).await.expect("absorption waited");
        assert_eq!(p.busy(), 2);
        drop(ab);
        drop(a);
        for h in hs {
            h.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![Prio::Erase, Prio::Merge, Prio::Reseal]);
        assert_eq!(p.busy(), 0);
    }
}
