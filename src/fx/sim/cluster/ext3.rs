//! sim-test-3 (P-sim-test-3): the replicated log (`pq`, 3 copies) and the whole-database lifecycle inside the simulator,
//! as new event kinds on the same executor and oracle (a script of `generate` or `generate2` runs exactly as before).
//!
//! The replicated log. A write of a process is staged in its tenant (memtable and WAL buffer) with the log's marker
//! (`sys/local-log` = the record's sequence number) in the same batch, its record goes to the log, and it is acknowledged
//! when the log confirmed the record on 2 copies of 3 and the process still holds the database (the server's
//! `Commit::with_local` and `owner_valid`). The store gets the data at the uploader's tick (`Upload`), after which the log
//! is told "stored up to position P" and drops those records. A takeover reads the database's records back from the log
//! and applies the ones after the stored marker (`replay_on_wake`); a restart reads every record back
//! (`recover_remote`), and a replay refused because another process holds the database keeps them (`Kept`) and retries
//! in the background (`LogRetry`). Copies go down and come back (`LogCopy`), the leader changes (`LogLeader`).
//!
//! What is real and what is a port. The tenant, SlateDB, our manifest, the compactor, the store's faults and the
//! `Commit` that waits for the log are the engine's own code. The log's client rules (`mongo-server/src/locallog.rs`
//! and `qlog.rs`: kept records, copies, the retry's confirmation, drop records, `drop_stale`, the marker check) live in
//! the server crate behind a process-wide singleton and the server's lease table, which three simulated processes in one
//! OS process cannot share; they are PORTED here, line for line, with the function they come from named at each step.
//! The log itself (`proto/quaderno`) is a model: a record is confirmed once the leader and one more copy have it, the
//! confirmed records are never lost, a record the leader holds alone is confirmed when a copy comes back or dropped
//! when the leader changes, a client that gets no confirmation within its deadline sees "outcome unknown".
//! `Ext::old_kept` switches the ported rules to the base's (fb38790e), as the engine's own control switch
//! (`MONGO_TEST_KEPT_RETRY_OLD=1`, ctl test_91) does.
//!
//! The lease. The script decides who owns the database (as in the core simulator): the last process that opened it as a
//! writer, until it closes, abandons or dies. Here the lease also refuses a replay or a drop of a process while another
//! live process owns the database, and refuses the acknowledgement of a write whose process lost it (a zombie's log
//! record is not acknowledged; its plain-WAL writes still are, as in the core: SlateDB's fencing is what stops those).
//!
//! The whole-database lifecycle. `DropDb` (dropDatabase) and `EraseDb` (the admin erase) take the lease, write the log's
//! drop record, close, move the name to a new incarnation and delete every object; a later writer `Open` creates the
//! database again under the same name. `AdminCheck` is what listDatabases, the admin API's existence check and the erase
//! handler's first step do in a process, before any lock or lease. The model gets one "every key deleted" write,
//! acknowledged when the drop returns (writes in flight across a drop may be ordered before it).

use super::*;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use tokio::sync::oneshot;

/// The process-wide settings of a world with the replicated log: the writer's own WAL flush only every second (the
/// server's `--local-log-upload-secs`, scaled), the log stamps each record with the writer epoch (`opened_epoch`), and a
/// batch is visible once its record is confirmed (`TRACK_VISIBLE`, set by the server with the log on). The guard puts the
/// previous values back.
#[must_use]
pub fn setup_log_world() -> LogWorldGuard {
    use std::sync::atomic::Ordering::Relaxed;
    let g = LogWorldGuard { flush: crate::lifecycle::LOCAL_LOG_FLUSH_SECS.load(Relaxed), track: crate::lifecycle::TRACK_VISIBLE.load(Relaxed) };
    crate::lifecycle::LOCAL_LOG_FLUSH_SECS.store(1, Relaxed);
    crate::lifecycle::TRACK_VISIBLE.store(true, Relaxed);
    let _ = crate::lifecycle::VIS_IGNORE_KEY.set(MARKER_KEY.to_vec());
    g
}

pub struct LogWorldGuard {
    flush: u64,
    track: bool,
}

impl Drop for LogWorldGuard {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        crate::lifecycle::LOCAL_LOG_FLUSH_SECS.store(self.flush, Relaxed);
        crate::lifecycle::TRACK_VISIBLE.store(self.track, Relaxed);
    }
}

/// The log's name in the markers (`--quaderno-id`, the same for every engine on this log).
pub const LOG_ID: &str = "quaderno:cell1";
/// `locallog::MARKER_KEY`.
pub const MARKER_KEY: &[u8] = b"sys/local-log";
/// How long a client waits for a record's confirmation (leader changes included) before "outcome unknown" (the server's
/// `qlog::COMMIT_DEADLINE`, 10 s, scaled as the simulator's other timeouts).
const COMMIT_DEADLINE: Duration = Duration::from_millis(2000);
/// A leader that stepped down is replaced after this long (an election).
const ELECTION_MS: u64 = 300;

/// One record of the log (`locallog::Record`).
#[derive(Clone, Debug)]
pub struct Rec {
    pub seq: u64,
    pub orig: u64,
    pub ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub drop: bool,
    pub epoch: u64,
}

fn to_ops(ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<Op> {
    ops.iter().map(|(k, v)| match v {
        Some(v) => Op::Put(k.clone(), v.clone()),
        None => Op::Delete(k.clone()),
    }).collect()
}

fn marker_value(seq: u64) -> Vec<u8> {
    format!("{LOG_ID}|{seq}").into_bytes()
}

fn parse_marker(b: &[u8]) -> Option<(String, u64)> {
    let s = std::str::from_utf8(b).ok()?;
    let (a, b) = s.rsplit_once('|')?;
    Some((a.to_string(), b.parse().ok()?))
}

// ---------------------------------------------------------------------------------------------------------------------
// The log (a model of `proto/quaderno` as the engine sees it through `qlog::Remote`)

struct Queued {
    rec: Rec,
    leader: usize,
    tx: oneshot::Sender<u64>,
}

#[derive(Default)]
pub struct LogInner {
    up: [bool; 3],
    leader: Option<usize>,
    /// When the leader was lost (an election follows after `ELECTION_MS`).
    lost_at: Option<std::time::Instant>,
    /// The confirmed records still in the log (not confirmed as stored), in position order.
    recs: Vec<(u64, Rec)>,
    /// The highest position confirmed as stored ("flush done") for the database.
    flushed: u64,
    next_pos: u64,
    /// Records the leader holds alone (no second copy up): confirmed when one comes back, dropped if the leader changes.
    queued: Vec<Queued>,
    pub confirmed: u64,
    pub dropped_unconfirmed: u64,
    pub flush_dones: u64,
    pub refused: u64,
}

pub struct SimLog {
    pub inner: Mutex<LogInner>,
}

impl SimLog {
    pub fn new() -> SimLog {
        SimLog { inner: Mutex::new(LogInner { up: [true; 3], leader: Some(0), next_pos: 1, ..Default::default() }) }
    }

    fn elect(i: &mut LogInner) {
        if i.leader.is_some() {
            return;
        }
        let n = i.up.iter().filter(|u| **u).count();
        if n < 2 || i.lost_at.is_some_and(|t| t.elapsed() < Duration::from_millis(ELECTION_MS)) {
            return;
        }
        i.leader = (0..3).find(|r| i.up[*r]);
        i.lost_at = None;
    }

    fn available(i: &LogInner) -> bool {
        i.leader.is_some() && i.up.iter().filter(|u| **u).count() >= 2
    }

    fn commit(i: &mut LogInner, rec: Rec) -> u64 {
        let pos = i.next_pos;
        i.next_pos += 1;
        i.recs.push((pos, rec));
        i.confirmed += 1;
        pos
    }

    fn drain_queued(i: &mut LogInner) {
        if !Self::available(i) {
            return;
        }
        let l = i.leader.unwrap();
        for q in std::mem::take(&mut i.queued) {
            if q.leader == l {
                let pos = Self::commit(i, q.rec);
                let _ = q.tx.send(pos);
            } else {
                i.dropped_unconfirmed += 1;
            }
        }
    }

    /// `qlog::Remote::append` + its batcher: the position once 2 copies of 3 have the record, or an error ("unknown").
    pub async fn append(&self, rec: Rec) -> Result<u64, String> {
        let deadline = tokio::time::Instant::now() + COMMIT_DEADLINE;
        let mut rx = None;
        loop {
            {
                let mut i = self.inner.lock().unwrap();
                Self::elect(&mut i);
                Self::drain_queued(&mut i);
                if rx.is_none() {
                    if Self::available(&i) {
                        return Ok(Self::commit(&mut i, rec));
                    }
                    if let Some(l) = i.leader {
                        let (tx, r) = oneshot::channel();
                        i.queued.push(Queued { rec: rec.clone(), leader: l, tx });
                        rx = Some(r);
                    }
                }
            }
            if let Some(r) = rx.as_mut() {
                match tokio::time::timeout_at(deadline, r).await {
                    Ok(Ok(pos)) => return Ok(pos),
                    Ok(Err(_)) => return Err("the log did not confirm the record (its leader changed); its outcome is unknown".into()),
                    Err(_) => return Err("the log did not confirm the record in time; its outcome is unknown".into()),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("the log has no leader that answers; the record's outcome is unknown".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// `qlog::Remote::flush_done` (the log side): records of the database up to `pos` leave the log.
    pub fn flush_done(&self, pos: u64) -> Result<(), String> {
        let mut i = self.inner.lock().unwrap();
        Self::elect(&mut i);
        if !Self::available(&i) {
            i.refused += 1;
            return Err("the log did not take the confirmation: no leader with a majority".into());
        }
        i.flushed = i.flushed.max(pos);
        let f = i.flushed;
        i.recs.retain(|(p, _)| *p > f);
        i.flush_dones += 1;
        Ok(())
    }

    /// `qlog::Remote::read_db` / `read_pending`: every record still in the log (`caught_up`: the leader's confirmed position,
    /// read from a copy that applied up to it).
    pub fn read(&self) -> Result<Vec<(u64, Rec)>, String> {
        let mut i = self.inner.lock().unwrap();
        Self::elect(&mut i);
        if i.leader.is_none() {
            i.refused += 1;
            return Err("the log did not give its confirmed position (no leader)".into());
        }
        Ok(i.recs.clone())
    }

    pub fn copy(&self, r: usize, up: bool) -> String {
        let mut i = self.inner.lock().unwrap();
        i.up[r] = up;
        if !up && i.leader == Some(r) {
            i.leader = None;
            i.lost_at = Some(std::time::Instant::now());
            i.dropped_unconfirmed += i.queued.len() as u64;
            i.queued.clear();
        }
        Self::elect(&mut i);
        Self::drain_queued(&mut i);
        format!("log copy {r} {} (up {:?}, leader {:?})", if up { "up" } else { "down" }, i.up, i.leader)
    }

    pub fn leader_change(&self) -> String {
        let mut i = self.inner.lock().unwrap();
        let old = i.leader;
        let n = i.up.iter().filter(|u| **u).count();
        if n >= 2 {
            i.leader = (0..3).map(|k| (old.unwrap_or(0) + 1 + k) % 3).find(|r| i.up[*r] && Some(*r) != old).or(old);
            i.lost_at = None;
        } else {
            i.leader = None;
            i.lost_at = Some(std::time::Instant::now());
        }
        if i.leader != old {
            i.dropped_unconfirmed += i.queued.len() as u64;
            i.queued.clear();
        }
        format!("log leader {old:?} -> {:?} (up {:?})", i.leader, i.up)
    }

    pub fn calm(&self) {
        let mut i = self.inner.lock().unwrap();
        i.up = [true; 3];
        i.lost_at = None;
        Self::elect(&mut i);
        if i.leader.is_none() {
            i.leader = Some(0);
        }
        Self::drain_queued(&mut i);
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// One process's side of the log (`locallog::LocalLog` and `qlog::Remote`'s per-database positions)

#[derive(Default)]
pub struct ProcLog {
    next_seq: u64,
    /// `TenantLog`: the highest sequence number appended for the database, and the highest known to be stored.
    staged: u64,
    uploaded: u64,
    pinned: Option<Vec<Rec>>,
    frozen: bool,
    /// fix-hole-t-freeze (`LocalLog::waking`): a takeover replay read the records back and has not applied them (it
    /// failed): nothing confirms them, the abandon does not freeze, the next opening replays again.
    waking: bool,
    /// fix-takeover-retry: the highest position read back at the start.
    read_pos: Option<u64>,
    /// `Remote::committed` and `Remote::flushed` for the database.
    committed: u64,
    flushed: u64,
    /// `Leases::takes`: how many times this process took the database's lease.
    takes: u64,
    /// A restart's replay that failed, stopped at `hold_kept_records`.
    parked: Option<Parked>,
    /// fix-hole-k: `LocalLog::kept_inc`, the incarnation the kept records were acknowledged for.
    kept_inc: Option<u64>,
}

pub struct Parked {
    keep: Vec<Rec>,
    opened: bool,
    seen: Option<u64>,
    inc: Option<u64>,
}

/// fix-hole-k: the mutation control (`MONGO_TEST_KEPT_INC_OFF=1` in the engine): the incarnation check of kept records off.
pub static KEPT_INC_OFF: AtomicBool = AtomicBool::new(false);

fn kept_inc_on(x3: &X3) -> bool {
    !x3.old_kept && !KEPT_INC_OFF.load(SeqCst)
}

impl ProcLog {
    /// `LocalLog::next_seq` with the replicated log: a hybrid clock, above every number given before by this process.
    fn next_seq(&mut self) -> u64 {
        let now = (ext_now_ms() as u64) << 16;
        let s = self.next_seq.max(now);
        self.next_seq = s + 1;
        s
    }

    fn is_pinned(&self) -> bool {
        self.pinned.is_some() || self.frozen
    }
}

fn ext_now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

// ---------------------------------------------------------------------------------------------------------------------
// The world's sim-test-3 state

#[derive(Clone)]
pub struct X3 {
    /// The lifecycle events and the lease bookkeeping are on (scripts of `generate3` and the stories).
    pub on: bool,
    pub log: Option<Arc<SimLog>>,
    pub old_kept: bool,
    pub strict_versions: bool,
    net: Arc<Net>,
    /// Per process life.
    procs: Arc<Mutex<HashMap<u64, ProcLog>>>,
    /// The lease as the script gives it: (slot, life) of the owner.
    owner: Arc<Mutex<Option<(usize, u64)>>>,
    /// The database was dropped or erased and not created again.
    absent: Arc<AtomicBool>,
    /// A drop or erase failed after the name moved to a new incarnation: the database may or may not exist.
    maybe_absent: Arc<AtomicBool>,
    pub found: Arc<Mutex<Vec<String>>>,
    counts: Arc<Mutex<HashMap<&'static str, u64>>>,
}

impl X3 {
    pub fn new(ext: &Ext, net: Arc<Net>) -> X3 {
        X3 {
            on: ext.flavor == 3 || ext.log,
            log: if ext.log { Some(Arc::new(SimLog::new())) } else { None },
            old_kept: ext.old_kept,
            strict_versions: ext.strict_versions,
            net,
            procs: Default::default(),
            owner: Default::default(),
            // the database does not exist until a writer's create succeeds (the first opening can fail: a 503 on the
            // lease's incarnation read left the core's `created` true with nothing in the store, batch 43496082968)
            absent: Arc::new(AtomicBool::new(ext.flavor == 3 || ext.log)),
            maybe_absent: Default::default(),
            found: Default::default(),
            counts: Default::default(),
        }
    }

    /// Core events that run here once sim-test-3 is on (with the lease and the log's rules).
    pub fn routes(&self, e: &Ev) -> bool {
        self.on && matches!(e, Ev::Open { .. } | Ev::Upgrade { .. } | Ev::Close { .. } | Ev::Abandon { .. } | Ev::Write { .. } | Ev::Erase { .. })
    }

    pub fn is_absent(&self) -> bool {
        self.absent.load(SeqCst)
    }

    pub fn on_kill(&self, life: u64) {
        let mut o = self.owner.lock().unwrap();
        if o.is_some_and(|(_, l)| l == life) {
            *o = None;
        }
    }

    fn count(&self, k: &'static str) {
        *self.counts.lock().unwrap().entry(k).or_default() += 1;
    }

    fn with<R>(&self, life: u64, f: impl FnOnce(&mut ProcLog) -> R) -> R {
        let mut m = self.procs.lock().unwrap();
        f(m.entry(life).or_default())
    }

    /// Another live process holds the database's lease.
    fn other_holds(&self, life: u64) -> bool {
        self.owner.lock().unwrap().is_some_and(|(_, l)| l != life && !self.net.is_dead(l))
    }

    fn holds(&self, life: u64) -> bool {
        self.owner.lock().unwrap().is_some_and(|(_, l)| l == life)
    }

    fn take(&self, p: usize, life: u64) {
        *self.owner.lock().unwrap() = Some((p, life));
        self.with(life, |x| x.takes += 1);
    }

    fn release(&self, life: u64) {
        let mut o = self.owner.lock().unwrap();
        if o.is_some_and(|(_, l)| l == life) {
            *o = None;
        }
    }

    /// `Remote::flush_done` (the client side): skipped when already confirmed this far.
    fn flush_done(&self, life: u64, pos: u64) -> Result<(), String> {
        let Some(log) = &self.log else { return Ok(()) };
        if pos == 0 || self.with(life, |x| x.flushed >= pos) {
            return Ok(());
        }
        log.flush_done(pos)?;
        self.with(life, |x| x.flushed = x.flushed.max(pos));
        self.count("flush-done");
        Ok(())
    }

    /// `LocalLog::append_rec` with the replicated log (the record's position noted as `Remote::committed` on confirmation).
    async fn append(&self, life: u64, rec: Rec) -> Result<u64, String> {
        let log = self.log.clone().ok_or("no log")?;
        self.with(life, |x| x.staged = x.staged.max(rec.seq));
        let r = log.append(rec).await;
        if let Ok(pos) = r {
            self.with(life, |x| x.committed = x.committed.max(pos));
        }
        r
    }

    /// `LocalLog::append_drop`: a drop record, then the log told its position is done.
    async fn append_drop(&self, life: u64) -> Result<(), String> {
        let seq = self.with(life, |x| x.next_seq());
        let pos = self.append(life, Rec { seq, orig: seq, ops: Vec::new(), drop: true, epoch: 0 }).await?;
        self.with(life, |x| {
            x.staged = seq;
            x.uploaded = seq;
        });
        if let Err(e) = self.flush_done(life, pos) {
            return Err(format!("drop record confirmed, its flush-done refused: {e}"));
        }
        Ok(())
    }
}

pub fn is_ext3(e: &Ev) -> bool {
    matches!(e, Ev::Upload { .. } | Ev::Restart { .. } | Ev::Kept { .. } | Ev::LogRetry { .. } | Ev::DropDb { .. } | Ev::EraseDb { .. } | Ev::AdminCheck { .. } | Ev::PlantLate { .. })
}

/// `locallog::drop_stale`.
fn drop_stale(recs: &mut Vec<Rec>, stored: u64) -> usize {
    let before = recs.len();
    recs.retain(|r| r.epoch == 0 || r.epoch >= stored);
    before - recs.len()
}

/// `locallog::stored_marker`: the marker as the store holds it, read without opening the database (through our manifest
/// as well as SlateDB).
async fn stored_marker(store: &Arc<dyn ObjectStore>, id: &str) -> Option<(String, u64)> {
    let mut o = slatedb::config::DbReaderOptions::default();
    o.manifest_poll_interval = Duration::from_secs(3600);
    let r = crate::fx::pair::PairedReader::open_follow(store, id, crate::lifecycle::tenant_path(id), crate::fx::pair::Follow::Newest, o).await.ok()?;
    let v = r.get(MARKER_KEY).await.ok().flatten();
    let _ = r.close().await;
    v.as_deref().and_then(parse_marker)
}

/// `locallog::drop_stored`: true when nothing is left to apply.
async fn drop_stored(store: &Arc<dyn ObjectStore>, id: &str, recs: &mut Vec<Rec>) -> bool {
    if let Some((lid, done)) = stored_marker(store, id).await {
        if lid == LOG_ID {
            recs.retain(|r| r.orig > done);
        }
    }
    recs.is_empty()
}

enum Outcome {
    Nothing,
    Applied(usize),
    Conflict,
    /// fix-hole-k: the records' incarnation was dropped or erased since: never applied.
    Moved,
}

/// Apply records after the stored marker durably, 256 per batch, each batch ending with its last record's marker.
async fn apply_after_marker(t: &mut Tenant, mut recs: Vec<Rec>) -> anyhow::Result<(usize, u64)> {
    let done = match t.get(MARKER_KEY).await?.as_deref().and_then(parse_marker) {
        Some((lid, s)) if lid == LOG_ID => s,
        Some((other, _)) => anyhow::bail!("written by another log ({other}) since"),
        None => 0,
    };
    recs.sort_by_key(|r| (r.orig, r.seq));
    let todo: Vec<Rec> = recs.into_iter().filter(|r| r.orig > done).collect();
    for chunk in todo.chunks(256) {
        let mut ops: Vec<Op> = Vec::new();
        for r in chunk {
            ops.extend(to_ops(&r.ops));
        }
        ops.push(Op::Put(MARKER_KEY.to_vec(), marker_value(chunk.last().unwrap().orig)));
        t.apply(ops).await?;
    }
    Ok((todo.len(), done))
}

/// `locallog::replay_on_wake`: a takeover reads the database's records back from the log and applies the ones after the
/// stored marker, then confirms them to the log.
async fn replay_on_wake(x3: &X3, life: u64, store: &Arc<dyn ObjectStore>, id: &str, t: &mut Tenant) -> anyhow::Result<String> {
    if x3.log.is_none() {
        return Ok(String::new());
    }
    // fix-hole-t-freeze (`locallog::replay_on_wake`): marked until the replay succeeds
    x3.with(life, |x| x.waking = true);
    let r = wake_apply(x3, life, store, id, t).await;
    if r.is_ok() {
        x3.with(life, |x| x.waking = false);
    }
    r
}

async fn wake_apply(x3: &X3, life: u64, store: &Arc<dyn ObjectStore>, id: &str, t: &mut Tenant) -> anyhow::Result<String> {
    let log = x3.log.clone().unwrap();
    let raw = log.read().map_err(|e| anyhow::anyhow!(e))?;
    if raw.is_empty() {
        return Ok(String::new());
    }
    let mut recs: Vec<Rec> = Vec::new();
    let (mut max_pos, mut max_seq) = (0, 0);
    for (pos, r) in raw {
        max_pos = max_pos.max(pos);
        max_seq = max_seq.max(r.seq);
        if r.drop {
            recs.clear();
            continue;
        }
        recs.push(r);
    }
    x3.with(life, |x| {
        x.committed = x.committed.max(max_pos);
        x.next_seq = x.next_seq.max(max_seq + 1);
    });
    x3.count("takeover-replay");
    let stored = crate::lifecycle::stored_writer_epoch(store, id).await?;
    let mut stale = 0;
    if let Some(s) = stored {
        stale = drop_stale(&mut recs, s);
    }
    // fix-hole-t-freeze: a record and its copy (same `orig`): only the last written is applied
    if !x3.old_kept {
        recs.sort_by_key(|r| (r.orig, std::cmp::Reverse(r.seq)));
        recs.dedup_by_key(|r| r.orig);
    }
    let rec_epoch = recs.iter().map(|r| r.epoch).max().unwrap_or(0);
    if rec_epoch > 0 && stored.is_some_and(|s| s > rec_epoch) {
        return Ok(format!("; takeover: {} records of epoch {rec_epoch} < stored {stored:?}: NOT applied", recs.len()));
    }
    // fix-log-takeover-stale, hole T (`locallog::replay_on_wake`, as on fix-log-takeover-stale): before the apply opens
    // this process's writer, copies of the records after the marker go to the log stamped with the epoch that writer
    // will open with (the stored one plus one)
    let mut copies_pos = 0;
    let mut copies = 0;
    if let Some(s) = stored.filter(|_| !x3.old_kept) {
        let done = match t.get(MARKER_KEY).await?.as_deref().and_then(parse_marker) {
            Some((lid, d)) if lid == LOG_ID => d,
            _ => 0,
        };
        for r in recs.iter().filter(|r| r.orig > done && r.epoch > 0 && r.epoch <= s) {
            let seq = x3.with(life, |x| x.next_seq());
            copies_pos = x3.append(life, Rec { seq, orig: r.orig, ops: r.ops.clone(), drop: false, epoch: s + 1 }).await.map_err(|e| anyhow::anyhow!(e))?;
            copies += 1;
        }
    }
    let (n, done) = match apply_after_marker(t, recs).await {
        Ok(x) => x,
        Err(e) if format!("{e:#}").contains("another log") => return Ok("; takeover: written by another log: NOT applied".into()),
        Err(e) => return Err(e),
    };
    x3.flush_done(life, max_pos.max(copies_pos)).map_err(|e| anyhow::anyhow!(e))?;
    if copies > 0 {
        x3.count("takeover-copies");
    }
    if n > 0 {
        x3.count("takeover-applied");
    }
    Ok(format!("; takeover replay: {n} records applied after {done} ({stale} stale dropped), log told {max_pos}"))
}

/// Open the database in `g` as a writer (the lease taken), with the takeover's log replay (`Entry::ensure_open`).
async fn open_writer(x3: &X3, p: usize, life: u64, lc: &Arc<Lifecycle>, store: &Arc<dyn ObjectStore>, id: &str, create: bool, g: &mut Option<Tenant>) -> anyhow::Result<String> {
    // the lease acquisition reads the name's incarnation and uses it from now on in this process
    let k = crate::lifecycle::load_incarnation(store, id).await?;
    // a name a failed drop left in a new, empty incarnation is created again (the server finds it absent)
    let create = create || (x3.maybe_absent.load(SeqCst) && !crate::lifecycle::db_exists(store, id).await?);
    // the server's opening is a reader until its first write upgrades it: the takeover's replay reads the stored writer
    // epoch before this process's writer moves it (`drop_stale` keeps the last writer's records)
    let mut t = if create { lc.create(id).await? } else { lc.wake(id).await? };
    x3.take(p, life);
    x3.absent.store(false, SeqCst);
    x3.maybe_absent.store(false, SeqCst);
    let rep = match replay_on_wake(x3, life, store, id, &mut t).await {
        Ok(s) => s,
        Err(e) => {
            // not served without them: the next command retries the open
            let _ = t.abandon().await;
            x3.release(life);
            anyhow::bail!("taken over but its records in the log could not be read back: {e:#}");
        }
    };
    if !t.is_writer() {
        if let Err(e) = t.upgrade().await {
            let _ = t.abandon().await;
            x3.release(life);
            return Err(e);
        }
    }
    let ep = t.writer_epoch();
    *g = Some(t);
    Ok(format!("opened as writer (life {life}, incarnation {k}, writer epoch {ep:?}{}){rep}", if create { ", created" } else { "" }))
}

impl World {
    pub(super) fn x3_copy(&mut self, r: usize, up: bool) {
        if let Some(l) = &self.x3.log {
            let s = l.copy(r, up);
            self.note(s);
        }
    }

    pub(super) fn x3_leader(&mut self) {
        if let Some(l) = &self.x3.log {
            let s = l.leader_change();
            self.note(s);
        }
    }

    /// Settle: the log calm; a restart parked at `hold_kept_records` goes on; kept records retried (a few rounds).
    pub(super) async fn x3_settle(&mut self) {
        let Some(l) = self.x3.log.clone() else { return };
        l.calm();
        for p in 0..SLOTS {
            let life = self.slots[p].life;
            if self.x3.with(life, |x| x.parked.is_some()) {
                let f = self.ext3_op(u32::MAX - 1, Ev::Kept { p }, true);
                let out = f.await;
                self.note(format!("settle: slot {p} kept: {out}"));
            }
        }
        for _ in 0..3 {
            for p in 0..SLOTS {
                let life = self.slots[p].life;
                if self.x3.with(life, |x| x.pinned.is_some()) {
                    let f = self.ext3_op(u32::MAX - 1, Ev::LogRetry { p }, true);
                    let out = f.await;
                    self.note(format!("settle: slot {p} retry: {out}"));
                }
            }
        }
    }

    pub(super) fn ext3_op(&mut self, eid: u32, e: Ev, created: bool) -> futures::future::BoxFuture<'static, String> {
        let p = e.slot().unwrap();
        let id = self.id.clone();
        let s = &self.slots[p];
        let (store, lc, tl, compl, life) = (Arc::clone(&s.store), Arc::clone(&s.lc), Arc::clone(&s.t), Arc::clone(&s.comp), s.life);
        let model = Arc::clone(&self.model);
        let x3 = self.x3.clone();
        let seed = self.net.seed;
        let kp = self.knobs;
        Box::pin(async move {
            match e {
                Ev::Open { writer, .. } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    if g.is_some() {
                        return "already open (skipped)".into();
                    }
                    if x3.with(life, |x| x.is_pinned()) {
                        return "refused: in recovery (RecoveryPending: kept or frozen records)".into();
                    }
                    let absent = x3.is_absent();
                    if writer {
                        match open_writer(&x3, p, life, &lc, &store, &id, !created || absent, &mut g).await {
                            Ok(s) => s,
                            Err(e) => format!("open failed: {}", short(&e)),
                        }
                    } else {
                        if absent {
                            return "reader: the database does not exist (skipped)".into();
                        }
                        let k = match crate::lifecycle::load_incarnation(&store, &id).await {
                            Ok(k) => k,
                            Err(e) => return format!("open failed: {}", short(&e)),
                        };
                        match lc.wake(&id).await {
                            Ok(t) => {
                                *g = Some(t);
                                format!("opened as reader (life {life}, incarnation {k})")
                            }
                            Err(e) => format!("open failed: {}", short(&e)),
                        }
                    }
                }
                Ev::Upgrade { .. } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if t.is_writer() {
                        return "already writer".into();
                    }
                    if x3.with(life, |x| x.is_pinned()) {
                        return "refused: in recovery (RecoveryPending)".into();
                    }
                    // the lease, the takeover's replay (before this writer moves the stored epoch), then the writer
                    x3.take(p, life);
                    let rep = match replay_on_wake(&x3, life, &store, &id, t).await {
                        Ok(s) => s,
                        Err(e) => {
                            let t = g.take().unwrap();
                            let _ = t.abandon().await;
                            x3.release(life);
                            return format!("the log could not be read back: {} (abandoned)", short(&e));
                        }
                    };
                    let t = g.as_mut().unwrap();
                    if t.is_writer() {
                        return format!("upgraded by the replay (epoch {:?}){rep}", t.writer_epoch());
                    }
                    match t.upgrade().await {
                        Ok(()) => format!("upgraded (epoch {:?}){rep}", t.writer_epoch()),
                        Err(e) => {
                            x3.release(life);
                            format!("upgrade failed: {}", short(&e))
                        }
                    }
                }
                Ev::Write { ops, .. } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = ops.iter().enumerate().map(|(j, (c, i, del))| (ckey(*c, *i), if *del { None } else { Some(ext::value(kp, format!("s{seed}-e{eid}-{j}"))) })).collect();
                    let mut last: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
                    for (k, v) in &kv {
                        last.insert(k.clone(), v.clone());
                    }
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = last.into_iter().collect();
                    log_write(&x3, p, life, eid, t, &model, kv).await
                }
                Ev::Erase { cust, .. } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if !t.is_writer() {
                        return "not a writer (skipped)".into();
                    }
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..PER_CUSTOMER).map(|i| (ckey(cust, i), None)).collect();
                    let out = log_write(&x3, p, life, eid, t, &model, kv).await;
                    if !out.starts_with("acked") {
                        return format!("deletes: {out}");
                    }
                    match t.fx_erasure_register("msgs", format!("c{cust:02}").as_bytes(), &cust_targets(cust), None).await {
                        Ok(Some(e)) => format!("deletes {out}; erasure {} registered, S_e {}", e.id, e.s_e),
                        Ok(None) => format!("deletes {out}; no erasure (sealed runs off)"),
                        Err(e) => format!("deletes {out}; register failed: {}", short(&e)),
                    }
                }
                Ev::Close { .. } => {
                    let c = compl.lock().unwrap().take();
                    if let Some(c) = c {
                        c.stop().await;
                    }
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.take() else { return "not open (skipped)".into() };
                    match t.close().await {
                        Ok(()) => {
                            let mut out = "closed".to_string();
                            // `LocalLog::closed`: every record confirmed so far was staged before the close
                            if x3.log.is_some() && !x3.with(life, |x| x.is_pinned() || x.waking) {
                                let pos = x3.with(life, |x| {
                                    x.uploaded = x.staged;
                                    x.committed
                                });
                                match x3.flush_done(life, pos) {
                                    Ok(()) => out.push_str(&format!("; log told {pos}")),
                                    Err(e) => out.push_str(&format!("; {e}")),
                                }
                            }
                            x3.release(life);
                            out
                        }
                        Err(e) => {
                            x3.release(life);
                            format!("close failed: {}", short(&e))
                        }
                    }
                }
                Ev::Abandon { .. } => {
                    let c = compl.lock().unwrap().take();
                    if let Some(c) = c {
                        c.stop().await;
                    }
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.take() else { return "not open (skipped)".into() };
                    // `Entry::abandon_with`: ownership lost with records only in the log: kept for a restart (frozen)
                    let frozen = x3.log.is_some() && x3.with(life, |x| {
                        if x.uploaded < x.staged && !x.is_pinned() && !x.waking {
                            x.frozen = true;
                            true
                        } else {
                            false
                        }
                    });
                    x3.release(life);
                    format!("abandoned (close done {}){}", t.abandon().await, if frozen { "; frozen (records only in the log)" } else { "" })
                }
                Ev::Upload { .. } => upload(&x3, life, &tl).await,
                Ev::Restart { .. } => restart(&x3, p, life, &lc, &store, &id, &tl).await,
                Ev::Kept { .. } => kept(&x3, life).await,
                Ev::LogRetry { .. } => retry(&x3, p, life, &lc, &store, &id, &tl).await,
                Ev::DropDb { .. } | Ev::EraseDb { .. } => {
                    let erase = matches!(e, Ev::EraseDb { .. });
                    whole_db(&x3, p, life, eid, &store, &id, &tl, &compl, &model, erase).await
                }
                Ev::AdminCheck { .. } => admin_check(&store, &id).await,
                Ev::PlantLate { .. } => plant_late(&store, &id).await,
                _ => unreachable!(),
            }
        })
    }
}

/// `Ev::PlantLate`: a lost writer's late version at the next slot (see the event).
async fn plant_late(store: &Arc<dyn ObjectStore>, id: &str) -> String {
    let r: anyhow::Result<String> = async {
        let Some(cur) = manifest::load_newest(store, id).await? else { return Ok("no version (skipped)".into()) };
        let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(store)).build();
        let last_l0 = admin.read_manifest(None).await?.map(|m| m.last_l0_seq()).unwrap_or(0);
        let ep = cur.writer_epoch.unwrap_or(1);
        if last_l0 <= cur.x_d || ep < 1 {
            return Ok(format!("late version not planted (L0 last seq {last_l0}, newest X_d {}, epoch {ep})", cur.x_d));
        }
        let mut stale = cur.clone();
        stale.version = cur.version + 1;
        stale.kind = "publish".into();
        stale.writer_epoch = Some(ep - 1);
        stale.x_d = last_l0;
        let made = manifest::create(store, id, &stale).await?;
        Ok(format!("late version {} planted (epoch {}, X_d {last_l0} over {}): created {made}", stale.version, ep - 1, cur.x_d))
    }
    .await;
    r.unwrap_or_else(|e| format!("plant failed: {}", short(&e)))
}

/// The stories' structural check (`Ext::strict_versions`): no version is listed above a version of a higher writer
/// epoch (a writer that lost the database published late on top).
pub(super) async fn strict_versions(w: &World) -> Vec<String> {
    let mut v = Vec::new();
    let Ok(vs) = manifest::list_versions(&w.raw, &w.id).await else { return v };
    let mut best: Option<(u64, u64)> = None;
    for n in vs {
        let Ok(tv) = manifest::load(&w.raw, &w.id, n).await else { continue };
        let e = tv.writer_epoch.unwrap_or(0);
        if let Some((bn, be)) = best {
            if e < be {
                v.push(format!("a lost writer's version is on top: version {n} (writer epoch {e}) above version {bn} (writer epoch {be})"));
            }
        }
        if best.map_or(true, |(_, be)| e >= be) {
            best = Some((n, e));
        }
    }
    v
}

/// What listDatabases (`dbs.rs`), the admin API's existence check (`ctl/meta.rs`) and the admin erase handler's first
/// step (`ctl/erase.rs` `erase`) do in a process before any lock or lease: on this head (fix-incarnation) they read the
/// name's incarnation without changing the process's table. (On 0ed53558 the erase handler called `load_incarnation`,
/// which sets it: the simulator's copy for that commit calls that instead.)
async fn admin_check(store: &Arc<dyn ObjectStore>, id: &str) -> String {
    let k = crate::lifecycle::read_incarnation(store, id).await;
    let e = crate::lifecycle::db_exists(store, id).await;
    format!("admin check: incarnation {:?}, exists {:?}", k.map_err(|e| short(&e)), e.map_err(|e| short(&e)))
}

/// A write batch: with the log, as `Entry::apply_mode` (the marker in the batch, the record to the log, acknowledged on
/// the log's confirmation if this process still holds the database); without it, as the core (`Tenant::apply`).
async fn log_write(x3: &X3, p: usize, life: u64, eid: u32, t: &mut Tenant, model: &Arc<Mutex<Model>>, kv: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> String {
    let writer = t.is_writer();
    let st = model.lock().unwrap().issue(eid, p, &kv);
    t.fx_sync();
    if x3.log.is_none() {
        return match t.apply(to_ops(&kv)).await {
            Ok(()) => {
                st.store(ACKED, SeqCst);
                format!("acked {} keys", kv.len())
            }
            Err(e) => {
                st.store(if writer || t.is_writer() { FAILED } else { FAILED_DEFINITE }, SeqCst);
                format!("failed{}: {}", if writer { "" } else { " (definite: not a writer)" }, short(&e))
            }
        };
    }
    if x3.with(life, |x| x.is_pinned()) {
        st.store(FAILED_DEFINITE, SeqCst);
        return "refused: in recovery (RecoveryPending)".into();
    }
    let seq = x3.with(life, |x| x.next_seq());
    let mut ops = to_ops(&kv);
    ops.push(Op::Put(MARKER_KEY.to_vec(), marker_value(seq)));
    let commit = match t.stage(ops).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            st.store(ACKED, SeqCst);
            return "nothing to write".into();
        }
        Err(e) => {
            st.store(if writer || t.is_writer() { FAILED } else { FAILED_DEFINITE }, SeqCst);
            return format!("stage failed{}: {}", if writer { "" } else { " (definite: not a writer)" }, short(&e));
        }
    };
    let epoch = t.writer_epoch().unwrap_or(0);
    let rec = Rec { seq, orig: seq, ops: kv.clone(), drop: false, epoch };
    let (tx, rx) = oneshot::channel();
    let x = x3.clone();
    let pos = Arc::new(AtomicU64::new(0));
    let pos2 = Arc::clone(&pos);
    tokio::spawn(async move {
        let r = x.append(life, rec).await;
        if let Ok(p) = &r {
            pos2.store(*p, SeqCst);
        }
        let _ = tx.send(r.map(|_| ()));
    });
    match commit.with_local(rx).wait().await {
        Ok(()) => {
            // `owner_valid`: the record is confirmed, but this process may have lost the database meanwhile
            if !x3.holds(life) {
                st.store(FAILED, SeqCst);
                x3.count("ack-refused");
                return format!("not acknowledged: ownership lost before the acknowledgement (record seq {seq}, position {})", pos.load(SeqCst));
            }
            st.store(ACKED, SeqCst);
            x3.count("log-acked");
            let at = pos.load(SeqCst);
            format!("acked {} keys (seq {seq}, epoch {epoch}, {})", kv.len(), if at > 0 { format!("log position {at}") } else { "the log failed: WAL fallback".into() })
        }
        Err(e) => {
            st.store(FAILED, SeqCst);
            format!("failed: {}", short(&e))
        }
    }
}

/// One tick of `LocalLog::spawn_uploader` for the database: the writer's WAL flushed to the store, then the log told the
/// position confirmed before the flush. The flush runs without the database's lock, as `lifecycle::flush_writer` does.
async fn upload(x3: &X3, life: u64, tl: &Arc<tokio::sync::Mutex<Option<Tenant>>>) -> String {
    if x3.log.is_none() {
        return "no log (skipped)".into();
    }
    let (due, target, pos) = x3.with(life, |x| (!x.is_pinned() && !x.waking && x.uploaded < x.staged, x.staged, x.committed));
    if !due {
        return "nothing to upload".into();
    }
    let db = {
        let Ok(g) = tl.try_lock() else { return "busy (skipped)".into() };
        g.as_ref().and_then(|t| t.fx_writer_db().cloned())
    };
    let Some(db) = db else { return "no open writer (skipped)".into() };
    match db.flush().await {
        Ok(()) => {
            x3.with(life, |x| x.uploaded = x.uploaded.max(target));
            x3.count("upload");
            match x3.flush_done(life, pos) {
                Ok(()) => format!("uploaded; log told {pos}"),
                Err(e) => format!("uploaded; {e}"),
            }
        }
        Err(e) => format!("upload failed: {e}"),
    }
}

/// `replay_db`: open the database through the lease (refused while another process holds it) and apply the records
/// after the stored marker. `retry`: a background retry of kept records.
#[allow(clippy::too_many_arguments)]
async fn replay_db(x3: &X3, p: usize, life: u64, lc: &Arc<Lifecycle>, store: &Arc<dyn ObjectStore>, id: &str, tl: &Arc<tokio::sync::Mutex<Option<Tenant>>>, mut recs: Vec<Rec>, retry: bool, inc: Option<u64>, seen: &mut Option<u64>) -> anyhow::Result<Outcome> {
    if !retry {
        if let Some(stored) = crate::lifecycle::stored_writer_epoch(store, id).await? {
            drop_stale(&mut recs, stored);
        }
        if recs.is_empty() {
            return Ok(Outcome::Nothing);
        }
    }
    let rec_epoch = recs.iter().map(|r| r.epoch).max().unwrap_or(0);
    if rec_epoch > 0 {
        if let Some(stored) = crate::lifecycle::stored_writer_epoch(store, id).await? {
            *seen = Some(stored);
            if stored > rec_epoch {
                return Ok(Outcome::Conflict);
            }
        }
    }
    let mut g = tokio::time::timeout(Duration::from_secs(10), tl.lock()).await.map_err(|_| anyhow::anyhow!("no progress (the database's lock)"))?;
    if retry && !x3.with(life, |x| x.pinned.is_some()) {
        return Ok(Outcome::Nothing);
    }
    // fix-hole-k: into the incarnation the records were acknowledged for only (checked before the lease, so a moved name
    // is never taken or created, and again under it)
    let inc = inc.filter(|_| kept_inc_on(x3));
    if let Some(k) = inc {
        if crate::lifecycle::read_incarnation(store, id).await? != k {
            return Ok(Outcome::Moved);
        }
    }
    if g.is_none() || !g.as_ref().unwrap().is_writer() || !x3.holds(life) {
        // `Entry::ensure_open`: the lease first
        if x3.other_holds(life) {
            anyhow::bail!("tenant {id} is owned by another server process");
        }
        if let Some(t) = g.take() {
            let _ = t.abandon().await;
        }
        open_writer(x3, p, life, lc, store, id, x3.is_absent(), &mut g).await?;
    }
    if let Some(k) = inc {
        if crate::lifecycle::read_incarnation(store, id).await? != k {
            return Ok(Outcome::Moved);
        }
    }
    let t = g.as_mut().unwrap();
    let (n, _) = apply_after_marker(t, recs).await?;
    Ok(if n == 0 { Outcome::Nothing } else { Outcome::Applied(n) })
}

/// `locallog::recover_remote` and `replay_all` for the database, up to `hold_kept_records` when the replay fails.
async fn restart(x3: &X3, p: usize, life: u64, lc: &Arc<Lifecycle>, store: &Arc<dyn ObjectStore>, id: &str, tl: &Arc<tokio::sync::Mutex<Option<Tenant>>>) -> String {
    let Some(log) = x3.log.clone() else { return "restarted (no log)".into() };
    x3.count("restart");
    let raw = match log.read() {
        Ok(r) => r,
        Err(e) => return format!("restart failed (the process does not start): {e}"),
    };
    let mut recs: Vec<Rec> = Vec::new();
    let (mut max_pos, mut max_seq) = (0, 0);
    for (pos, r) in raw {
        max_pos = max_pos.max(pos);
        max_seq = max_seq.max(r.seq);
        if r.drop {
            recs.clear();
            continue;
        }
        recs.push(r);
    }
    x3.with(life, |x| {
        x.committed = x.committed.max(max_pos);
        x.next_seq = x.next_seq.max(max_seq + 1);
        x.read_pos = Some(max_pos);
    });
    let n_read = recs.len();
    // fix-hole-k (`incarnations_of`, `recheck_after_incarnations`): the incarnation, then the log again; a drop record
    // after the last record read, or that record gone, means nothing of them is to be applied
    let mut inc = None;
    if !recs.is_empty() && kept_inc_on(x3) {
        let k = match crate::lifecycle::read_incarnation(store, id).await {
            Ok(k) => k,
            Err(e) => return format!("restart failed (the process does not start): incarnation: {}", short(&e)),
        };
        let again = match log.read() {
            Ok(r) => r,
            Err(e) => return format!("restart failed (the process does not start): {e}"),
        };
        let dropped = again.iter().any(|(pos, r)| r.drop && *pos > max_pos);
        let gone = !again.iter().any(|(pos, _)| *pos == max_pos);
        if dropped || gone {
            recs.clear();
            x3.count("restart-recheck-discarded");
        }
        inc = Some(k);
    }
    if recs.is_empty() {
        let _ = x3.flush_done(life, max_pos);
        return format!("restart: {n_read} records read{}", if n_read > 0 { " (dropped or confirmed since: discarded)" } else { "" });
    }
    if drop_stored(store, id, &mut recs).await {
        return match x3.flush_done(life, max_pos) {
            Ok(()) => format!("restart: {n_read} records read, all already stored; log told {max_pos}"),
            Err(e) => format!("restart: {n_read} records read, all already stored; {e}"),
        };
    }
    let keep = recs.clone();
    let mut seen = None;
    let takes = x3.with(life, |x| x.takes);
    let r = tokio::time::timeout(Duration::from_secs(20), replay_db(x3, p, life, lc, store, id, tl, recs, false, inc, &mut seen)).await.unwrap_or_else(|_| Err(anyhow::anyhow!("no progress")));
    let opened = x3.with(life, |x| x.takes) != takes;
    match r {
        Ok(Outcome::Nothing) | Ok(Outcome::Applied(_)) | Ok(Outcome::Moved) => {
            let n = if let Ok(Outcome::Applied(n)) = r { n } else { 0 };
            match x3.flush_done(life, max_pos) {
                Ok(()) => format!("restart: {n_read} records read, {n} applied; log told {max_pos}"),
                Err(e) => format!("restart: {n_read} records read, {n} applied; {e}"),
            }
        }
        Ok(Outcome::Conflict) => format!("restart: {n_read} records read: conflict with a later writer, kept in the log, NOT applied"),
        Err(e) => {
            x3.with(life, |x| x.parked = Some(Parked { keep, opened, seen, inc }));
            x3.count("replay-refused");
            format!("restart: {n_read} records read; replay failed ({}); stopped at hold_kept_records (took the lease: {opened})", short(&e))
        }
    }
}

/// `replay_all`'s failure branch, after `hold_kept_records`: the records are kept (in memory only on 759f9dca when the
/// replay never took the lease; copies written to the log with the base's rules or when it did).
async fn kept(x3: &X3, life: u64) -> String {
    let Some(pk) = x3.with(life, |x| x.parked.take()) else { return "nothing parked".into() };
    x3.with(life, |x| x.kept_inc = pk.inc);
    if !pk.opened && !x3.old_kept {
        let n = pk.keep.len();
        x3.with(life, |x| x.pinned = Some(pk.keep));
        return format!("{n} records kept in memory only (the replay never took the lease: no copies)");
    }
    let epoch = pk.keep.iter().map(|r| r.epoch).max().unwrap_or(0).max(pk.seen.map_or(0, |s| s + 1));
    let mut kept = Vec::with_capacity(pk.keep.len());
    let mut last = 0;
    for r in pk.keep {
        let seq = x3.with(life, |x| x.next_seq());
        let ep = if r.epoch > 0 { epoch } else { 0 };
        match x3.append(life, Rec { seq, orig: r.orig, ops: r.ops.clone(), drop: false, epoch: ep }).await {
            Ok(pos) => last = pos,
            Err(e) => return format!("restart failed while writing kept copies (the process stops): {e}"),
        }
        kept.push(Rec { seq, orig: r.orig, ops: r.ops, drop: false, epoch: ep });
    }
    let n = kept.len();
    x3.with(life, |x| x.pinned = Some(kept));
    x3.count("kept-copies");
    format!("{n} kept records written again to the log as copies (epoch {epoch}, last at position {last})")
}

/// One round of `locallog::spawn_retry` for the database.
async fn retry(x3: &X3, p: usize, life: u64, lc: &Arc<Lifecycle>, store: &Arc<dyn ObjectStore>, id: &str, tl: &Arc<tokio::sync::Mutex<Option<Tenant>>>) -> String {
    let Some(mut recs) = x3.with(life, |x| x.pinned.clone()) else { return "nothing kept".into() };
    x3.count("retry");
    let rec_epoch = recs.iter().map(|r| r.epoch).max().unwrap_or(0);
    let takes = x3.with(life, |x| x.takes);
    let mut seen = None;
    let r = if drop_stored(store, id, &mut recs).await {
        Ok(Outcome::Nothing)
    } else {
        let inc = x3.with(life, |x| x.kept_inc);
        tokio::time::timeout(Duration::from_secs(20), replay_db(x3, p, life, lc, store, id, tl, recs, true, inc, &mut seen)).await.unwrap_or_else(|_| Err(anyhow::anyhow!("no progress")))
    };
    let opened = x3.with(life, |x| x.takes) != takes;
    let holder = x3.holds(life);
    let old = x3.old_kept;
    let moved = matches!(r, Ok(Outcome::Moved));
    if moved {
        x3.count("retry-moved");
    }
    match r {
        Ok(Outcome::Applied(_)) | Ok(Outcome::Nothing) | Ok(Outcome::Moved) => {
            // fix-takeover-retry: only the holder confirms what it knows; another process at most what it read at its start
            let pos = x3.with(life, |x| {
                x.uploaded = x.staged;
                if holder || old {
                    x.committed
                } else {
                    x.read_pos.unwrap_or(0)
                }
            });
            let told = x3.flush_done(life, pos);
            x3.with(life, |x| x.pinned = None);
            let which = if holder || old { "its committed position" } else { "the highest it read at its start" };
            let what = if moved { "discarded (their incarnation was dropped or erased)" } else { "stored" };
            format!("retry: kept records {what} (holder {holder}); log told {pos} ({which}) {:?}", told)
        }
        Ok(Outcome::Conflict) => {
            x3.with(life, |x| x.pinned = None);
            if holder || old {
                let d = x3.append_drop(life).await;
                format!("retry: conflict with another writer; drop record written {d:?}")
            } else {
                "retry: conflict with another writer; not the holder: no drop record".into()
            }
        }
        Err(e) => {
            let ep = seen.map_or(0, |s| s + 1);
            let mut extra = String::new();
            if rec_epoch > 0 && ep > rec_epoch && (opened || old) {
                let seq = x3.with(life, |x| x.next_seq());
                if let Ok(pos) = x3.append(life, Rec { seq, orig: seq, ops: Vec::new(), drop: false, epoch: ep }).await {
                    x3.with(life, |x| {
                        if let Some(v) = x.pinned.as_mut() {
                            v.push(Rec { seq, orig: seq, ops: Vec::new(), drop: false, epoch: ep });
                        }
                    });
                    extra = format!("; epoch record {ep} at position {pos}");
                }
            }
            format!("retry failed: {}{extra}", short(&e))
        }
    }
}

/// dropDatabase (`Entry::drop_database`) or the admin erase (`ctl/erase.rs` `run`) of the whole database by slot `p`.
#[allow(clippy::too_many_arguments)]
async fn whole_db(x3: &X3, p: usize, life: u64, eid: u32, store: &Arc<dyn ObjectStore>, id: &str, tl: &Arc<tokio::sync::Mutex<Option<Tenant>>>, compl: &Arc<Mutex<Option<Arc<Compactor>>>>, model: &Arc<Mutex<Model>>, erase: bool) -> String {
    let what = if erase { "erase" } else { "drop" };
    if x3.is_absent() {
        return format!("{what}: the database does not exist (skipped)");
    }
    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
    if erase {
        // `erase`: the incarnation read (without changing the table), then `run`: the process's own opening given up first
        let _ = crate::lifecycle::read_incarnation(store, id).await;
        let c = compl.lock().unwrap().take();
        if let Some(c) = c {
            c.stop().await;
        }
        if let Some(t) = g.take() {
            if !t.abandon().await {
                return format!("{what}: refused, the close is still pending (nothing deleted)");
            }
        }
    }
    // the lease (refused while another live process holds the database)
    if x3.other_holds(life) {
        return format!("{what}: refused, the database is owned by another process");
    }
    *x3.owner.lock().unwrap() = Some((p, life));
    if !erase {
        // dropDatabase: the log's drop record first, then the close
        if x3.log.is_some() {
            if let Err(e) = x3.append_drop(life).await {
                x3.release(life);
                return format!("{what} failed: the log's drop record: {e}");
            }
            x3.with(life, |x| x.pinned = None);
        }
        let c = compl.lock().unwrap().take();
        if let Some(c) = c {
            c.stop().await;
        }
        if let Some(t) = g.take() {
            if let Err(e) = t.close().await {
                x3.release(life);
                return format!("{what} failed at the close: {}", short(&e));
            }
        }
        if !x3.holds(life) {
            return format!("{what} failed: ownership lost before the objects went");
        }
    }
    if erase && x3.log.is_some() {
        // fix-hole-k: as a drop, the log's drop record before the incarnation moves (`ctl/erase.rs`)
        if let Err(e) = x3.append_drop(life).await {
            x3.release(life);
            return format!("{what} failed: the log's drop record: {e}");
        }
        x3.with(life, |x| x.pinned = None);
    }
    let from = crate::lifecycle::read_incarnation(store, id).await.unwrap_or(0);
    // from here on a failure leaves the outcome unknown to the client (the name may already be in a new, empty
    // incarnation): the model gets the drop as a write of unknown outcome, and the database may be absent
    let unknown = |why: String| {
        x3.release(life);
        x3.maybe_absent.store(true, SeqCst);
        let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..CUSTOMERS).flat_map(|c| (0..PER_CUSTOMER).map(move |i| (ckey(c, i), None))).collect();
        model.lock().unwrap().issue(eid, p, &kv).store(FAILED, SeqCst);
        why
    };
    let k = match crate::lifecycle::bump_incarnation(store, id).await {
        Ok(k) => k,
        Err(e) => return unknown(format!("{what} failed at the incarnation move (outcome unknown): {}", short(&e))),
    };
    // every object of the name
    let prefix = object_store::path::Path::from(format!("tenants/{id}"));
    let mut deleted = 0;
    for _ in 0..5 {
        use futures::TryStreamExt;
        let objs: Vec<object_store::path::Path> = match store.list(Some(&prefix)).map_ok(|m| m.location).try_collect().await {
            Ok(v) => v,
            Err(e) => return unknown(format!("{what} failed while listing (outcome unknown): {e} ({deleted} objects deleted; the client retries)")),
        };
        if objs.is_empty() {
            break;
        }
        for o in objs {
            if store.delete(&o).await.is_ok() {
                deleted += 1;
            }
        }
    }
    x3.release(life);
    x3.absent.store(true, SeqCst);
    x3.maybe_absent.store(false, SeqCst);
    x3.count(if erase { "erase-db" } else { "drop-db" });
    // the model: every key deleted, acknowledged now (a write in flight across the drop may be ordered before it)
    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..CUSTOMERS).flat_map(|c| (0..PER_CUSTOMER).map(move |i| (ckey(c, i), None))).collect();
    model.lock().unwrap().issue(eid, p, &kv).store(ACKED, SeqCst);
    format!("{what} done: incarnation {from} -> {k}, {deleted} objects deleted")
}

/// Before the oracle: a database a failed drop may have removed counts as absent when no fresh process sees it.
pub(super) async fn settle_absent(w: &World) {
    if w.x3.maybe_absent.load(SeqCst) && !w.x3.is_absent() {
        let fresh = w.net.proc_store(5_000_000 + w.next_life);
        if let Ok(false) = crate::lifecycle::db_exists(&fresh, &w.id).await {
            w.x3.absent.store(true, SeqCst);
            w.note("oracle: a failed drop left the name empty: the database counts as dropped".into());
        }
    }
}

/// With the log: a fresh process takes the database over before the oracle (its takeover reads the log back, as the
/// server's next owner would), then closes it cleanly. The core oracle then checks a fresh opening.
pub(super) async fn before_oracle(w: &World) -> Vec<String> {
    let mut v: Vec<String> = w.x3.found.lock().unwrap().clone();
    if !w.created || w.x3.is_absent() {
        return v;
    }
    let raw = w.net.proc_store(3_000_000 + w.next_life);
    let _ = crate::lifecycle::load_incarnation(&raw, &w.id).await;
    // the trace: the current incarnation's versions and where their runs are
    if let Ok(vs) = manifest::list_versions(&w.raw, &w.id).await {
        let mut d = Vec::new();
        for n in vs.iter().rev().take(4) {
            if let Ok(tv) = manifest::load(&w.raw, &w.id, *n).await {
                let runs: Vec<String> = tv.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).chain(tv.delta.iter().map(|r| r.path.clone())).collect();
                let other = runs.iter().filter(|r| !r.starts_with(&format!("{}/", crate::lifecycle::tenant_path(&*w.id)))).count();
                d.push(format!("v{n} (x_d {}, epoch {:?}, {} runs, {other} outside {})", tv.x_d, tv.writer_epoch, runs.len(), crate::lifecycle::tenant_path(&*w.id)));
            }
        }
        w.note(format!("oracle: incarnation {} versions {vs:?}: {}", crate::lifecycle::incarnation(&w.id), d.join("; ")));
    }
    if w.x3.log.is_none() {
        return v;
    }
    view::clear_cache();
    let life = 3_000_000 + w.next_life;
    let lc = Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&raw))), None);
    let mut g: Option<Tenant> = None;
    // the processes are stopped: the lease is free
    *w.x3.owner.lock().unwrap() = None;
    match tokio::time::timeout(Duration::from_secs(60), open_writer(&w.x3, 0, life, &lc, &raw, &w.id, false, &mut g)).await {
        Ok(Ok(s)) => w.note(format!("before the oracle: {s}")),
        Ok(Err(e)) => v.push(format!("the final takeover (with the log's replay) failed: {}", short(&e))),
        Err(_) => v.push("the final takeover did not return in 60 s".into()),
    }
    if let Some(t) = g.take() {
        if let Err(e) = tokio::time::timeout(Duration::from_secs(20), t.close()).await.unwrap_or_else(|_| Err(anyhow::anyhow!("timeout"))) {
            v.push(format!("the final takeover's close failed: {}", short(&e)));
        }
    }
    w.x3.release(life);
    v
}

/// The database was dropped or erased last: no process sees its data, and a database created again with the same name
/// starts empty (no row of an older incarnation, no late write of a process that lost it).
pub(super) async fn oracle_absent(w: &World) -> Vec<String> {
    let mut v = Vec::new();
    view::clear_cache();
    let fresh = w.net.proc_store(4_000_000 + w.next_life);
    match crate::lifecycle::db_exists(&fresh, &w.id).await {
        Ok(false) => {}
        Ok(true) => v.push("dropped database still exists for a fresh process (db_exists)".into()),
        Err(e) => v.push(format!("db_exists failed on a calm store: {}", short(&e))),
    }
    let _ = crate::lifecycle::load_incarnation(&fresh, &w.id).await;
    let lc = Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&fresh))), None);
    let model = w.model.lock().unwrap().snapshot();
    match tokio::time::timeout(Duration::from_secs(60), lc.create(&w.id)).await {
        Ok(Ok(t)) => {
            let reads = read_all(&t).await;
            v.extend(check_reads("re-created", &reads, &model));
            let _ = tokio::time::timeout(Duration::from_secs(20), t.close()).await;
        }
        Ok(Err(e)) => v.push(format!("creating the dropped name again failed: {}", short(&e))),
        Err(_) => v.push("creating the dropped name again did not return in 60 s".into()),
    }
    v
}

/// What the run reached of sim-test-3's scenarios (for the seed's line).
pub(super) fn coverage(w: &World) -> String {
    if !w.x3.on {
        return String::new();
    }
    let c = w.x3.counts.lock().unwrap();
    let g = |k: &str| c.get(k).copied().unwrap_or(0);
    let lg = w.x3.log.as_ref().map(|l| {
        let i = l.inner.lock().unwrap();
        format!(" log confirmed {} unconfirmed-dropped {} refused {}", i.confirmed, i.dropped_unconfirmed, i.refused)
    });
    format!(
        "; x3: log-acked {} ack-refused {} uploads {} flush-done {} restarts {} replay-refused {} kept-copies {} retries {} takeover-replays {} (applied {}) drops {} erases {}{}",
        g("log-acked"),
        g("ack-refused"),
        g("upload"),
        g("flush-done"),
        g("restart"),
        g("replay-refused"),
        g("kept-copies"),
        g("retry"),
        g("takeover-replay"),
        g("takeover-applied"),
        g("drop-db"),
        g("erase-db"),
        lg.unwrap_or_default()
    )
}

/// The scripts of sim-test-3: the random walk of `generate` (unchanged) with the log's events and the whole-database
/// lifecycle woven in by a third random stream of the same seed. The log is on in 85% of the seeds (the others run the
/// lifecycle with the plain S3 WAL). A takeover-retry pattern is woven in on purpose: the owner's WAL uploads held, its
/// writes acknowledged by the log only, a restart of another process whose replay is refused, the owner's upload, more
/// writes, the restart's kept records, its retry, the owner's death, a takeover.
pub fn generate3(seed: u64, steps: usize) -> Script {
    let base = generate(seed, steps);
    let mut r = StdRng::seed_from_u64(seed ^ 0x51_6d_2e_74_33_33);
    let log = r.gen_bool(0.85);
    let mut owner: Option<usize> = Some(0);
    let mut down: Vec<usize> = vec![1, 2];
    let mut out: Vec<Ev> = Vec::new();
    let mut parked: Vec<usize> = Vec::new();
    let mut copies_down: Vec<usize> = Vec::new();
    let mut rid = 900_000u64;
    let mut pattern_at = if log && r.gen_bool(0.5) { r.gen_range(4..steps.max(5)) } else { usize::MAX };
    for (i, (_, e)) in base.events.into_iter().enumerate() {
        match &e {
            Ev::Open { p, writer: true } | Ev::Upgrade { p } => {
                owner = Some(*p);
                down.retain(|x| x != p);
            }
            Ev::Open { p, writer: false } => down.retain(|x| x != p),
            Ev::Close { p } | Ev::Abandon { p } | Ev::Kill { p } => {
                if owner == Some(*p) {
                    owner = None;
                }
                if !down.contains(p) {
                    down.push(*p);
                }
            }
            _ => {}
        }
        out.push(e);
        if i == 0 {
            continue;
        }
        if i >= pattern_at && log {
            if let (Some(o), Some(&q)) = (owner, down.first()) {
                // the takeover-retry pattern (ctl test_91's order, drawn at random around it)
                rid += 1;
                let w = |p: usize, r: &mut StdRng| Ev::Write { p, ops: (0..r.gen_range(3..12)).map(|_| (r.gen_range(0..CUSTOMERS), r.gen_range(0..PER_CUSTOMER), false)).collect() };
                out.push(w(o, &mut r));
                out.push(Ev::Upload { p: o });
                out.push(Ev::Hold { p: o, class: Class::Wal, op: Some(NOp::Put), skip: 0, land: true, all: true, rid });
                out.push(w(o, &mut r));
                out.push(Ev::Restart { p: q });
                if r.gen_bool(0.7) {
                    out.push(Ev::Release { rid });
                    out.push(Ev::Upload { p: o });
                    rid += 1;
                    out.push(Ev::Hold { p: o, class: Class::Wal, op: Some(NOp::Put), skip: 0, land: true, all: true, rid });
                }
                for _ in 0..r.gen_range(1..3) {
                    out.push(w(o, &mut r));
                }
                out.push(Ev::Kept { p: q });
                out.push(Ev::LogRetry { p: q });
                if r.gen_bool(0.7) {
                    out.push(Ev::Kill { p: o });
                    out.push(Ev::Release { rid });
                    out.push(Ev::Open { p: q, writer: true });
                    owner = Some(q);
                    down.retain(|x| *x != q);
                    down.push(o);
                } else {
                    out.push(Ev::Release { rid });
                }
                // once per script
                parked.retain(|x| *x != q);
                pattern_at = usize::MAX;
                continue;
            }
        }
        let o = owner.unwrap_or_else(|| r.gen_range(0..SLOTS));
        let x: u32 = r.gen_range(0..1000);
        match x {
            0..=139 if log => out.push(Ev::Upload { p: o }),
            140..=169 if log => {
                let q = r.gen_range(0..SLOTS);
                out.push(Ev::Restart { p: q });
                if owner == Some(q) {
                    owner = None;
                }
                if !down.contains(&q) {
                    down.push(q);
                }
                parked.push(q);
            }
            170..=209 if !parked.is_empty() => {
                let q = parked.remove(r.gen_range(0..parked.len()));
                out.push(Ev::Kept { p: q });
                out.push(Ev::LogRetry { p: q });
            }
            210..=239 if log => out.push(Ev::LogRetry { p: r.gen_range(0..SLOTS) }),
            240..=264 if log && copies_down.len() < 2 => {
                let c = r.gen_range(0..3);
                if !copies_down.contains(&c) {
                    copies_down.push(c);
                    out.push(Ev::LogCopy { r: c, up: false });
                }
            }
            265..=304 if !copies_down.is_empty() => {
                let c = copies_down.remove(0);
                out.push(Ev::LogCopy { r: c, up: true });
            }
            305..=319 if log => out.push(Ev::LogLeader),
            320..=339 => out.push(Ev::AdminCheck { p: r.gen_range(0..SLOTS) }),
            340..=354 if owner.is_some() => {
                out.push(if r.gen_bool(0.5) { Ev::DropDb { p: o } } else { Ev::EraseDb { p: o } });
                owner = None;
                down = (0..SLOTS).collect();
                // the name created again soon, often while a process that lost it is still working (its zombies go on)
                if r.gen_bool(0.7) {
                    let q = r.gen_range(0..SLOTS);
                    if r.gen_bool(0.5) {
                        out.push(Ev::AdminCheck { p: (q + 1) % SLOTS });
                    }
                    out.push(Ev::Open { p: q, writer: true });
                    owner = Some(q);
                    down.retain(|x| *x != q);
                }
            }
            355..=364 => out.push(Ev::AdminCheck { p: o }),
            _ => {}
        }
    }
    if !copies_down.is_empty() {
        for c in copies_down {
            out.push(Ev::LogCopy { r: c, up: true });
        }
    }
    Script { seed, profile: base.profile, knobs: base.knobs, events: out.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Ext { flavor: 3, log, old_kept: false, strict_versions: false } }
}
