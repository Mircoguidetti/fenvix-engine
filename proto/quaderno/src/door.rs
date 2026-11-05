//! Q8c · The per-database cap at the door (token bucket in front of the lanes).
//!
//! Kitchen: every table has its own ticket counter at the door. A table that orders faster than its cap waits
//! outside, in its own line, and enters when its turn comes; only if it would wait longer than `max_wait_ms` it is
//! told "come back in N ms" (one retryable `Busy` with the exact time, not a storm of tries). The other tables never
//! see that line: their orders go straight in.
//!
//! Real terms: a GCRA (generic cell rate algorithm, the virtual-time form of a token bucket) per database, in the
//! network threads, before the commit reaches the lane of its database and long before the leader's main thread.
//! Cost of a commit = max(1 / recs_per_s, bytes / bytes_per_s) seconds of the database's budget; `burst_s` seconds
//! of budget can be spent at once. A commit whose send time is in the future waits in the database's outside queue
//! (one timer task per database with waiting commits, not one per commit); a commit that would wait more than
//! `max_wait_ms` is rejected at once with `Busy { retry_ms }` = the time after which it would fit.

use crate::codec::{CommitReq, Db};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct DoorCfg {
    pub on: bool,
    pub recs_per_s: f64,
    pub bytes_per_s: f64,
    pub burst_s: f64,
    pub max_wait_ms: u64,
    /// over `max_wait_ms`: block the connection (TCP backpressure, no reply, no CPU per refused try) instead of
    /// rejecting, for at most `block_max_ms` in all; then reject
    pub block: bool,
    pub block_max_ms: u64,
}

impl DoorCfg {
    pub fn from_env() -> DoorCfg {
        let g = |k: &str, d: f64| std::env::var(k).ok().and_then(|x| x.parse().ok()).unwrap_or(d);
        DoorCfg {
            on: std::env::var("PQ_DOOR").map(|x| x == "1").unwrap_or(false),
            recs_per_s: g("PQ_DOOR_RECS", 1000.0),
            bytes_per_s: g("PQ_DOOR_MBS", 2.0) * 1048576.0,
            burst_s: g("PQ_DOOR_BURST_S", 0.1),
            max_wait_ms: g("PQ_DOOR_WAIT_MS", 500.0) as u64,
            block: std::env::var("PQ_DOOR_BLOCK").map(|x| x != "0").unwrap_or(true),
            block_max_ms: g("PQ_DOOR_BLOCK_MAX_MS", 1000.0) as u64,
        }
    }
}

/// What the door says about one commit.
pub enum Verdict {
    /// enter now
    Now,
    /// wait outside until this instant
    At(Instant),
    /// rejected: retry after this many ms
    Reject(u32),
}

struct Bucket<T> {
    /// theoretical arrival time (GCRA): the instant at which the database's budget is fully spent
    tat: Instant,
    q: VecDeque<(Instant, T)>,
    drainer: bool,
}

#[derive(Default, Clone, Debug, serde::Serialize)]
pub struct DoorStats {
    pub now: u64,
    /// connection pauses (blocking mode) and their total time
    pub blocked: u64,
    pub blocked_ms: u64,
    pub waited: u64,
    pub rejected: u64,
    pub waiting: u64,
    pub max_wait_us: u64,
}

pub struct Door<T> {
    pub cfg: DoorCfg,
    map: Mutex<HashMap<Db, Bucket<T>>>,
    pub st: Mutex<DoorStats>,
}

impl<T: Send + 'static> Door<T> {
    pub fn new(cfg: DoorCfg) -> Door<T> {
        Door { cfg, map: Mutex::new(HashMap::new()), st: Mutex::new(DoorStats::default()) }
    }

    fn cost(&self, recs: u64, bytes: u64) -> Duration {
        let a = recs as f64 / self.cfg.recs_per_s.max(1e-9);
        let b = bytes as f64 / self.cfg.bytes_per_s.max(1e-9);
        Duration::from_secs_f64(a.max(b))
    }

    /// Pure decision (also used by the tests): charge `recs`/`bytes` to `db` at `now`.
    pub fn judge(&self, db: &Db, recs: u64, bytes: u64, now: Instant) -> Verdict {
        let cost = self.cost(recs, bytes);
        let burst = Duration::from_secs_f64(self.cfg.burst_s);
        let mut m = self.map.lock();
        let b = m.entry(*db).or_insert_with(|| Bucket { tat: now, q: VecDeque::new(), drainer: false });
        let tat = b.tat.max(now);
        // send time: when the budget spent before this commit is back under the burst
        let send = (tat.checked_sub(burst)).map(|x| x.max(now)).unwrap_or(now);
        let wait = send - now;
        // a database with commits already waiting outside keeps its order: nobody overtakes the line
        let queued = !b.q.is_empty();
        if wait > Duration::from_millis(self.cfg.max_wait_ms) {
            // retry hint: the time after which this commit would fit, but at least 100 ms (many rejected commits of the
            // same database must not all come back 1 ms later: the client doubles this at each new rejection)
            return Verdict::Reject(((wait - Duration::from_millis(self.cfg.max_wait_ms)).as_millis() as u32).max(100));
        }
        b.tat = tat + cost;
        if wait.is_zero() && !queued {
            Verdict::Now
        } else {
            Verdict::At(send)
        }
    }

    /// From a network thread: `enter` runs now (in this thread) for the commits that may enter, or later from the
    /// database's drainer task; rejected commits get `reject(item, retry_ms)` at once.
    /// `Err((item, ms))` = it would wait more than `max_wait_ms`: the caller either blocks its connection for `ms`
    /// and tries again (`block`, the commit waits outside without any reply) or rejects it with `Busy`.
    pub fn admit(self: &Arc<Self>, db: Db, recs: u64, bytes: u64, item: T, enter: Arc<dyn Fn(Vec<T>) + Send + Sync>) -> Result<(), (T, u32)> {
        let now = Instant::now();
        match self.judge(&db, recs, bytes, now) {
            Verdict::Now => {
                self.st.lock().now += 1;
                enter(vec![item]);
            }
            Verdict::Reject(ms) => {
                return Err((item, ms));
            }
            Verdict::At(t) => {
                let spawn = {
                    let mut m = self.map.lock();
                    let b = m.get_mut(&db).unwrap();
                    b.q.push_back((t, item));
                    let s = !b.drainer;
                    b.drainer = true;
                    s
                };
                {
                    let mut st = self.st.lock();
                    st.waited += 1;
                    st.waiting += 1;
                    st.max_wait_us = st.max_wait_us.max((t - now).as_micros() as u64);
                }
                if spawn {
                    let door = self.clone();
                    tokio::spawn(async move { door.drain(db, enter).await });
                }
            }
        }
        Ok(())
    }

    async fn drain(self: Arc<Self>, db: Db, enter: Arc<dyn Fn(Vec<T>) + Send + Sync>) {
        loop {
            let (ready, next) = {
                let mut m = self.map.lock();
                let Some(b) = m.get_mut(&db) else { return };
                let now = Instant::now();
                let mut ready = vec![];
                while b.q.front().map(|x| x.0 <= now).unwrap_or(false) {
                    ready.push(b.q.pop_front().unwrap().1);
                }
                let next = b.q.front().map(|x| x.0);
                if next.is_none() {
                    b.drainer = false;
                }
                (ready, next)
            };
            if !ready.is_empty() {
                self.st.lock().waiting -= ready.len() as u64;
                enter(ready);
            }
            match next {
                Some(t) => tokio::time::sleep_until(t.into()).await,
                None => return,
            }
        }
    }

    /// Forget idle databases (budget fully recovered, nobody waiting): keeps the map small.
    pub fn gc(&self) {
        let now = Instant::now();
        self.map.lock().retain(|_, b| !b.q.is_empty() || b.drainer || b.tat > now);
    }

    pub fn stats(&self) -> DoorStats {
        self.st.lock().clone()
    }
}

/// Bytes of a commit for the door (same measure as the lanes).
pub fn bytes_of(c: &CommitReq) -> u64 {
    crate::lanes::commit_bytes(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(rps: f64, burst_s: f64, wait_ms: u64) -> DoorCfg {
        DoorCfg { on: true, recs_per_s: rps, bytes_per_s: 1e12, burst_s, max_wait_ms: wait_ms, block: false, block_max_ms: 0 }
    }

    #[test]
    fn under_the_cap_enters_at_once() {
        let d: Door<()> = Door::new(cfg(1000.0, 0.1, 1000));
        let db = [1u8; 16];
        let t0 = Instant::now();
        // 100/s for 2 s: always Now
        for k in 0..200u64 {
            let t = t0 + Duration::from_millis(10 * k);
            assert!(matches!(d.judge(&db, 1, 2000, t), Verdict::Now), "commit {k}");
        }
    }

    #[test]
    fn over_the_cap_waits_then_rejects_with_exact_retry() {
        let d: Door<()> = Door::new(cfg(1000.0, 0.1, 1000));
        let db = [2u8; 16];
        let t0 = Instant::now();
        let (mut now, mut at, mut rej) = (0, 0, vec![]);
        // 5,000 commits at the same instant: 100 (burst) enter, the next 1,000 wait up to 1 s, the rest are rejected
        for _ in 0..5000 {
            match d.judge(&db, 1, 2000, t0) {
                Verdict::Now => now += 1,
                Verdict::At(t) => {
                    assert!(t - t0 <= Duration::from_millis(1000));
                    at += 1
                }
                Verdict::Reject(ms) => rej.push(ms),
            }
        }
        assert_eq!(now, 101);
        assert!((999..=1001).contains(&at), "waiting {at}");
        assert_eq!(rej.len(), 5000 - now - at);
        // the retry is the time after which it fits (a few ms here), raised to the 100 ms floor
        assert!(rej.iter().all(|ms| *ms == 100), "{:?}", &rej[..5]);
    }

    #[test]
    fn one_database_over_the_cap_does_not_touch_another() {
        let d: Door<()> = Door::new(cfg(1000.0, 0.1, 1000));
        let (noisy, quiet) = ([3u8; 16], [4u8; 16]);
        let t0 = Instant::now();
        for _ in 0..50_000 {
            let _ = d.judge(&noisy, 1, 2000, t0);
        }
        assert!(matches!(d.judge(&quiet, 1, 2000, t0), Verdict::Now));
    }

    #[test]
    fn big_records_pay_in_bytes() {
        let mut c = cfg(1000.0, 0.1, 1000);
        c.bytes_per_s = 2.0 * 1048576.0;
        let d: Door<()> = Door::new(c);
        let db = [5u8; 16];
        let t0 = Instant::now();
        // 1 MB records: cost 0.5 s each; the first enters (burst 0.1 s is below one record, but the bucket is full)
        assert!(matches!(d.judge(&db, 1, 1 << 20, t0), Verdict::Now));
        match d.judge(&db, 1, 1 << 20, t0) {
            Verdict::At(t) => assert!((t - t0).as_millis() >= 390 && (t - t0).as_millis() <= 410),
            _ => panic!("the second 1 MB record must wait about 0.4 s"),
        }
    }

    #[tokio::test]
    async fn drainer_keeps_order_and_rate() {
        let d: Arc<Door<u64>> = Arc::new(Door::new(cfg(1000.0, 0.01, 2000)));
        let db = [6u8; 16];
        let got = Arc::new(Mutex::new(vec![]));
        let g = got.clone();
        let enter: Arc<dyn Fn(Vec<u64>) + Send + Sync> = Arc::new(move |v: Vec<u64>| {
            let t = Instant::now();
            g.lock().extend(v.into_iter().map(|k| (k, t)));
        });
        let t0 = Instant::now();
        for k in 0..500u64 {
            assert!(d.admit(db, 1, 100, k, enter.clone()).is_ok(), "no reject expected");
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
        let v = got.lock().clone();
        assert_eq!(v.len(), 500);
        assert!(v.windows(2).all(|w| w[0].0 < w[1].0), "order kept");
        let last = v.last().unwrap().1 - t0;
        // 500 commits at 1,000/s with 10 commits of burst: about 0.49 s
        assert!(last >= Duration::from_millis(470) && last <= Duration::from_millis(600), "{last:?}");
    }
}
