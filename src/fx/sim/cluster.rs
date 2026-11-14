//! The simulated cluster (P-sim-test step 2) and the oracle (step 3).
//!
//! A seed gives a script: a list of events over 3 process slots on one database. Each slot runs a real engine process
//! (its own `Lifecycle`, its own view of the store through [`Net`]): openings as reader or writer (an opening as writer
//! while another slot owns the database is a takeover: the old owner goes on as a zombie until an `Abandon`), writes,
//! deletes and customer erasures, memtable flushes, publishes, the first build off the lock, compactors (start, drain,
//! stop, GC), the erasure pass (scrub, commit, record) and its retire step, the closed database's GC and the close-time
//! compaction pass, closes, kills (-9: the process sends nothing more; what it already sent lands or not), restarts (a
//! new life of the slot), held requests and their release, and pauses.
//!
//! The settle phase releases every hold, stops the faults, lets in-flight work end, and stops every process. The oracle
//! then opens the database in a fresh process and checks:
//! 1. every acknowledged write reads back (the last acknowledged value of a key, or a later write whose outcome is
//!    unknown);
//! 2. no erased or deleted row reads back (the same rule: a delete is a write of "absent");
//! 3. no run that the version readers trust lists is missing (no live run deleted), and no read fails;
//! 4. no write that failed with a definite error is read back;
//! 5. a FIRST read returns what the NEXT read returns, and a reader and a writer read the same.
//!
//! What runs for real: the engine's code (lifecycle, fx manifest, builds, compactor, GC, erasure, SlateDB 0.17) on an
//! in-memory object store. What is simulated: the store's timing and faults, the processes (slots in one OS process),
//! the lease (the script decides who owns the database; nothing stops a zombie but the store's own fences).

use super::net::{Class, Net, Op as NOp, Profile, Rule};
use crate::fx::build::{BuildOpts, Reseal};
use crate::fx::compactor::{Compactor, Knobs};
use crate::fx::erase::{self, Batch, Targets};
use crate::fx::{gc, manifest, pool, view};
use crate::lifecycle::{Lifecycle, Op, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::SeqCst};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

pub mod ext;
pub mod ext3;
pub use ext::Ext;

pub const SLOTS: usize = 3;
pub const CUSTOMERS: usize = 6;
pub const PER_CUSTOMER: usize = 24;

// ---------------------------------------------------------------------------------------------------------------------
// Timing of the simulated world (the server's rules, scaled down; `fx::check_config` accepts them)

pub const T_PIN_MS: u64 = 200;
pub const HOLD_LIFE_MS: u64 = 1000;
pub const PUT_TIMEOUT_MS: u64 = 1000;
pub const GC_GRACE_MS: u64 = 2500;

/// The compactor's settings of a seed: 0 eager (the write-stalls tests' small groups: reseals and merges often), 1 lazy
/// (no reseal, no split, no merge: runs stay listed for long, as on a large database between reseals).
pub fn knobs_for(profile: u8) -> Knobs {
    let mut k = knobs();
    if profile == 2 {
        // sim-test-2: many small groups (the shape of a 1 TB database: an erasure and a scrub touch a few groups of many)
        k.split_bytes = 3 << 10;
        k.merge_bytes = 1 << 10;
        k.min_seal_bytes = 1 << 10;
        k.tier_unit = 1 << 10;
    }
    if profile == 1 {
        k.theta = 1e9;
        k.tier_t = 64;
        k.absorb_max_l0 = 64;
        k.split_bytes = 1 << 30;
        k.merge_bytes = 0;
        k.min_seal_bytes = 0;
    }
    k
}

pub fn knobs() -> Knobs {
    Knobs {
        theta: 0.25,
        r3: false,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        recognise_in_background: true,
        local_max_bytes: 0,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2),
        drop: true,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(3000),
        remote_lease: Duration::from_millis(HOLD_LIFE_MS + PUT_TIMEOUT_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(PUT_TIMEOUT_MS),
        gc_grace: Duration::from_millis(GC_GRACE_MS),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_secs(3600),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
    }
}

/// The builder's settings for a seed's compactor settings (`knobs_for`): profile 2 cuts many small groups.
pub fn build_opts_for(profile: u8) -> BuildOpts {
    let mut o = build_opts();
    if profile == 2 {
        o.group_bytes = 2 << 10;
    }
    o
}

pub fn build_opts() -> BuildOpts {
    BuildOpts { reseal: Reseal::All, group_bytes: 16 << 10, sealed: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2), delta: RunOptions::unchecked(1024, 1 << 20, CustomerKind::V2) }
}

/// Process-wide settings of the simulated world (every seed uses the same). The returned guard puts the previous
/// values back when it is dropped, so the other tests of the same process see their defaults.
#[must_use]
pub fn setup_world() -> WorldGuard {
    use std::sync::atomic::Ordering::Relaxed;
    let g = WorldGuard {
        enabled: crate::fx::ENABLED.load(Relaxed),
        publish_every: crate::fx::PUBLISH_EVERY.load(Relaxed),
        t_pin: crate::fx::T_PIN_MS.load(Relaxed),
        hold: crate::fx::hold::HOLD_LIFE_MS.load(Relaxed),
        put: crate::fx::PUT_TIMEOUT_MS.load(Relaxed),
        lease: crate::fx::LEASE_MS.load(Relaxed),
        margin: crate::fx::LEASE_MARGIN_MS.load(Relaxed),
        remote: crate::fx::REMOTE_LEASE_MS.load(Relaxed),
        grace: crate::fx::GC_GRACE_MS.load(Relaxed),
        retention: crate::fx::VERSION_RETENTION_MS.load(Relaxed),
        l0_bytes: crate::lifecycle::L0_SST_BYTES_OVERRIDE.load(Relaxed),
        l0_max: crate::lifecycle::L0_MAX_SSTS_OVERRIDE.load(Relaxed),
        poll: crate::lifecycle::WRITER_POLL_SECS.load(Relaxed),
        server_on: crate::fx::compactor::SERVER_ON.load(Relaxed),
    };
    crate::fx::ENABLED.store(true, Relaxed);
    crate::fx::PUBLISH_EVERY.store(0, Relaxed);
    crate::fx::T_PIN_MS.store(T_PIN_MS, Relaxed);
    crate::fx::hold::HOLD_LIFE_MS.store(HOLD_LIFE_MS, Relaxed);
    crate::fx::PUT_TIMEOUT_MS.store(PUT_TIMEOUT_MS, Relaxed);
    crate::fx::LEASE_MS.store(4000, Relaxed);
    crate::fx::LEASE_MARGIN_MS.store(1500, Relaxed);
    crate::fx::REMOTE_LEASE_MS.store(HOLD_LIFE_MS + PUT_TIMEOUT_MS + 600, Relaxed);
    crate::fx::GC_GRACE_MS.store(GC_GRACE_MS, Relaxed);
    crate::fx::VERSION_RETENTION_MS.store(0, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(64, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    // the server with compactors on (the close-time pass gates on our versions, `lifecycle::compact_tenant_pass_with`)
    crate::fx::compactor::SERVER_ON.store(true, Relaxed);
    crate::fx::check_config().expect("the simulated world's timing rules");
    knobs().check().expect("the simulated compactor's knobs");
    g
}

pub struct WorldGuard {
    enabled: bool,
    publish_every: u64,
    t_pin: u64,
    hold: u64,
    put: u64,
    lease: u64,
    margin: u64,
    remote: u64,
    grace: u64,
    retention: u64,
    l0_bytes: u64,
    l0_max: usize,
    poll: u64,
    server_on: bool,
}

impl Drop for WorldGuard {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        crate::fx::ENABLED.store(self.enabled, Relaxed);
        crate::fx::PUBLISH_EVERY.store(self.publish_every, Relaxed);
        crate::fx::T_PIN_MS.store(self.t_pin, Relaxed);
        crate::fx::hold::HOLD_LIFE_MS.store(self.hold, Relaxed);
        crate::fx::PUT_TIMEOUT_MS.store(self.put, Relaxed);
        crate::fx::LEASE_MS.store(self.lease, Relaxed);
        crate::fx::LEASE_MARGIN_MS.store(self.margin, Relaxed);
        crate::fx::REMOTE_LEASE_MS.store(self.remote, Relaxed);
        crate::fx::GC_GRACE_MS.store(self.grace, Relaxed);
        crate::fx::VERSION_RETENTION_MS.store(self.retention, Relaxed);
        crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(self.l0_bytes, Relaxed);
        crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(self.l0_max, Relaxed);
        crate::lifecycle::WRITER_POLL_SECS.store(self.poll, Relaxed);
        crate::fx::compactor::SERVER_ON.store(self.server_on, Relaxed);
    }
}

pub fn ckey(c: usize, i: usize) -> Vec<u8> {
    let name = format!("c{c:02}");
    let mut k = b"u/".to_vec();
    k.push(name.len() as u8);
    k.extend(name.as_bytes());
    k.extend(format!("msgs/d/{i:05}").as_bytes());
    k
}

fn cust_targets(c: usize) -> Targets {
    let k = ckey(c, 0);
    let lo = k[..k.len() - "msgs/d/00000".len()].to_vec();
    let mut hi = lo.clone();
    *hi.last_mut().unwrap() += 1;
    let mut t = Targets::default();
    t.ranges.push((lo, hi));
    t
}

// ---------------------------------------------------------------------------------------------------------------------
// The script

#[derive(Clone, Debug)]
pub enum Ev {
    /// Open the database in slot `p` (a create if it does not exist yet); as writer it takes the database over.
    Open { p: usize, writer: bool },
    /// A reader of slot `p` becomes the writer (a takeover if another slot owns the database).
    Upgrade { p: usize },
    /// One batch of puts and deletes (customer, index, delete).
    Write { p: usize, ops: Vec<(usize, usize, bool)> },
    /// The customer's rows deleted in one batch, then its erasure registered (as the server's erase command).
    Erase { p: usize, cust: usize },
    Flush { p: usize },
    Publish { p: usize },
    FirstStart { p: usize },
    FirstPoll { p: usize },
    CompStart { p: usize },
    CompDrain { p: usize },
    CompStop { p: usize },
    /// SlateDB's compaction with our drop, through the compactor's X_d now (what the fold does by itself once L0 and the
    /// holds allow; forced here so a script does not depend on its timing).
    DropThrough { p: usize },
    Gc { p: usize },
    /// One erasure pass with the slot's compactor: scrub, commit, record (as `ctl/erase.rs` without the agent).
    ErasePass { p: usize },
    /// The retire step of every scrubbed erasure.
    Retire { p: usize },
    GcClosed { p: usize },
    /// The server's pass over a closed database (`lifecycle` maintenance loop): SlateDB's closed GC
    /// (`lifecycle::gc_tenant_closed`, WAL, manifests, compacted SSTs, compactions, with this minimum age), then our
    /// runs' (`fx::gc::gc_closed`).
    GcClosedServer { p: usize, min_age_ms: u64 },
    ClosePass { p: usize },
    Close { p: usize },
    Abandon { p: usize },
    Kill { p: usize },
    Hold { p: usize, class: Class, op: Option<NOp>, skip: u64, land: bool, all: bool, rid: u64 },
    Release { rid: u64 },
    Sleep { ms: u64 },
    /// Read one customer's rows in process `p` (point reads, or one prefix scan), checked at once: every row this
    /// opening of `p` had acknowledged before the read started comes back (or a later write of it), or the read fails;
    /// never fewer rows (the coordinator's lead of 2026-11-13: reads that came back empty under a slow store).
    ReadCheck { p: usize, cust: usize, scan: bool },
    // ---- sim-test-2 (`ext.rs`) ----
    /// A named restore point now (`ctl/pitr.rs` `make_point`): a SlateDB checkpoint paired with a version of ours.
    Point { p: usize, tag: u64 },
    /// The restore point `tag` deleted (its window passed: `pitr::prune`).
    PointPrune { p: usize, tag: u64 },
    /// A wall-clock time to restore to later (the model's state now is what a restore to it must give).
    Mark { tag: u64 },
    /// A restore to the time of mark `mark` (`pitr::restore`): stage the newest point at or before it plus the WAL
    /// written up to it, check the staged state against the model at the mark, and (`apply`) write it into the live
    /// database through slot `p`'s writer.
    Restore { p: usize, mark: u64, apply: bool },
    /// A fork of the database (`ctl/fork.rs` `fork_paired`) by slot `p` (it quiesces its own tenant first), `clone`
    /// (runs shared by reference) or `copy`.
    Fork { p: usize, copy: bool, tag: u64 },
    /// The ballast generator's parallel publish (`publish_generated_parts`) into a database with no version yet.
    Generate { p: usize, writers: u8 },
    /// SlateDB's own GC of the database (`gc_tenant`, the WAL kept as a restore window keeps it).
    SlateGc { p: usize },
    /// S3 in trouble from now on (`on`: many 503s and timeouts after the write landed) or back to the seed's profile.
    Storm { on: bool },
    /// The server's default erasure pass (`ctl/erase.rs` `run_pass_ride`, every erasure due): forced reseals of the groups
    /// still owing a ready erasure, cleanses of the delta runs still holding its rows, the confirmation, the record.
    EraseRide { p: usize },
    // ---- sim-test-3 (`ext3.rs`): the replicated log and the whole-database lifecycle ----
    /// One tick of slot `p`'s uploader (`LocalLog::spawn_uploader`): its writer's WAL to the store, then the log told
    /// "stored up to" the position it had confirmed before the flush.
    Upload { p: usize },
    /// Slot `p`'s process is killed (-9) and starts again: it reads back every record the log still holds and replays
    /// them (`locallog::recover_remote`). A replay that fails stops where the server keeps the records
    /// (`hold_kept_records`); `Kept` goes on from there.
    Restart { p: usize },
    /// The restart of slot `p` goes on past `hold_kept_records`: its records are kept (`replay_all`'s failure branch).
    Kept { p: usize },
    /// One round of slot `p`'s background retry of kept records (`locallog::spawn_retry`).
    LogRetry { p: usize },
    /// Copy `r` of the log goes down or comes back.
    LogCopy { r: usize, up: bool },
    /// The log's leader steps down; another copy is elected.
    LogLeader,
    /// dropDatabase by slot `p` (`Entry::drop_database`): the lease, the log's drop record, the close, the name moved to a
    /// new incarnation, every object deleted.
    DropDb { p: usize },
    /// The admin erase of the whole database by slot `p` (`ctl/erase.rs` `erase` and `run`).
    EraseDb { p: usize },
    /// What listDatabases, the admin API's existence check and the erase handler's first step do in slot `p`'s process,
    /// before any lock or lease (`db_exists`, the incarnation lookup).
    AdminCheck { p: usize },
    /// A late version of a writer that lost the database lands at the next slot: the newest version again, one writer
    /// epoch lower, with X_d at SlateDB's last L0 seq (as `write_stalls_tests::older_version_scenario` and the review of
    /// hole C plant it). An injected fault standing for any late publisher of a lost writer (on 65dd663: the generator's
    /// parts publish, hole G). Skipped when it would not pass the newest version's X_d.
    PlantLate { p: usize },
}

impl Ev {
    fn slot(&self) -> Option<usize> {
        use Ev::*;
        match self {
            Open { p, .. } | Upgrade { p } | Write { p, .. } | Erase { p, .. } | Flush { p } | Publish { p } | FirstStart { p } | FirstPoll { p } | CompStart { p } | CompDrain { p } | CompStop { p } | DropThrough { p } | Gc { p } | ErasePass { p } | Retire { p } | GcClosed { p } | GcClosedServer { p, .. } | ReadCheck { p, .. } | ClosePass { p } | Close { p } | Abandon { p } | Kill { p } | Hold { p, .. } => Some(*p),
            Point { p, .. } | PointPrune { p, .. } | Restore { p, .. } | Fork { p, .. } | Generate { p, .. } | SlateGc { p } | EraseRide { p } => Some(*p),
            Upload { p } | Restart { p } | Kept { p } | LogRetry { p } | DropDb { p } | EraseDb { p } | AdminCheck { p } | PlantLate { p } => Some(*p),
            Release { .. } | Sleep { .. } | Mark { .. } | Storm { .. } | LogCopy { .. } | LogLeader => None,
        }
    }
}

/// The script of a seed: the events, the fault profile, and the steps.
#[derive(Clone, Debug)]
pub struct Script {
    pub seed: u64,
    pub profile: Profile,
    /// The compactor's settings (`knobs_for`).
    pub knobs: u8,
    pub events: Vec<(u32, Ev)>,
    /// sim-test-2's additions (default: none, the scripts of `generate`).
    pub ext: Ext,
}

/// Generate the script of `seed`: a weighted random walk over the events that apply in an abstract state (who owns the
/// database, who is a zombie, which processes have a compactor, whether a version exists). The executor still takes
/// any event in any state (an event that does not apply is a no-op, noted in the trace).
/// The odds of generator 1's cold-restart round and slow-read pattern at each step: `SIM_PATTERN` unset (or `mix`) keeps
/// the defaults (0.05, 0.08), so a seed's script is the same as before; `slow-reads` makes the slow-read pattern frequent
/// (0.4) and `cold-restart` the cold-restart round (0.3). Generators 2 and 3 build on generator 1, so they follow it.
/// The seed's report names the pattern: replay a seed with the same `SIM_PATTERN`.
fn pattern_odds() -> (f64, f64) {
    match std::env::var("SIM_PATTERN").as_deref() {
        Ok("slow-reads") => (0.05, 0.4),
        Ok("cold-restart") => (0.3, 0.08),
        Ok("") | Ok("mix") | Err(_) => (0.05, 0.08),
        Ok(x) => panic!("SIM_PATTERN={x}: expected mix, slow-reads or cold-restart"),
    }
}

pub fn generate(seed: u64, steps: usize) -> Script {
    let (p_cold, p_slow) = pattern_odds();
    let mut r = StdRng::seed_from_u64(seed);
    let profile = match r.gen_range(0..4) {
        0 => Profile::none(),
        1 => Profile { delay_ppm: 300_000, delay_max_ms: 30, fail_ppm: 0, timeout_before_ppm: 0, timeout_after_ppm: 0, timeout_ms: 0 },
        2 => Profile { delay_ppm: 200_000, delay_max_ms: 60, fail_ppm: 5_000, timeout_before_ppm: 2_000, timeout_after_ppm: 2_000, timeout_ms: 300 },
        _ => Profile { delay_ppm: 100_000, delay_max_ms: 120, fail_ppm: 15_000, timeout_before_ppm: 5_000, timeout_after_ppm: 8_000, timeout_ms: 400 },
    };
    #[derive(Clone, Copy, Default, PartialEq)]
    enum Role {
        #[default]
        Down,
        Reader,
        Owner,
        Zombie,
    }
    #[derive(Clone, Copy, Default)]
    struct S {
        role: Role,
        comp: bool,
    }
    let knobs = r.gen_range(0..2u8);
    let mut st = [S::default(); SLOTS];
    let mut versioned = false;
    let mut rid = 0u64;
    let mut open_rules: Vec<u64> = Vec::new();
    let mut ev: Vec<Ev> = vec![Ev::Open { p: 0, writer: true }];
    st[0].role = Role::Owner;
    let hold_class = |r: &mut StdRng| match r.gen_range(0..12) {
        0..=4 => Class::FxVersion,
        5 => Class::Wal,
        6 => Class::SlateManifest,
        7 => Class::FxErasure,
        8 => Class::FxRun,
        9 => Class::FxOther,
        _ => Class::Other,
    };
    while ev.len() < steps {
        let owner = (0..SLOTS).find(|p| st[*p].role == Role::Owner);
        let zombies: Vec<usize> = (0..SLOTS).filter(|p| st[*p].role == Role::Zombie).collect();
        let down: Vec<usize> = (0..SLOTS).filter(|p| st[*p].role == Role::Down).collect();
        let readers: Vec<usize> = (0..SLOTS).filter(|p| st[*p].role == Role::Reader).collect();
        let mut cands: Vec<(u32, Ev)> = Vec::new();
        let ops = |r: &mut StdRng| -> Vec<(usize, usize, bool)> {
            let n = r.gen_range(1..12);
            (0..n).map(|_| (r.gen_range(0..CUSTOMERS), r.gen_range(0..PER_CUSTOMER), r.gen_bool(0.12))).collect()
        };
        if let Some(o) = owner {
            cands.push((20, Ev::Write { p: o, ops: ops(&mut r) }));
            cands.push((6, Ev::ReadCheck { p: o, cust: r.gen_range(0..CUSTOMERS), scan: r.gen_bool(0.5) }));
            cands.push((6, Ev::Flush { p: o }));
            if !st[o].comp {
                cands.push((if versioned { 3 } else { 10 }, Ev::Publish { p: o }));
                cands.push((3, Ev::FirstStart { p: o }));
                cands.push((3, Ev::FirstPoll { p: o }));
                if versioned {
                    cands.push((10, Ev::CompStart { p: o }));
                }
            } else {
                cands.push((7, Ev::CompDrain { p: o }));
                cands.push((4, Ev::Gc { p: o }));
                cands.push((4, Ev::ErasePass { p: o }));
                cands.push((2, Ev::DropThrough { p: o }));
                cands.push((1, Ev::CompStop { p: o }));
            }
            cands.push((5, Ev::Erase { p: o, cust: r.gen_range(0..CUSTOMERS) }));
            cands.push((2, Ev::Close { p: o }));
            cands.push((1, Ev::Abandon { p: o }));
            cands.push((2, Ev::Kill { p: o }));
            // a takeover: another process opens as writer, or a reader upgrades
            if let Some(&q) = down.first() {
                cands.push((5, Ev::Open { p: q, writer: true }));
                cands.push((1, Ev::Open { p: q, writer: false }));
            }
            if let Some(&q) = readers.first() {
                cands.push((3, Ev::Upgrade { p: q }));
            }
        } else {
            for &q in &down {
                cands.push((6, Ev::Open { p: q, writer: true }));
                cands.push((1, Ev::Open { p: q, writer: false }));
            }
            for &q in &readers {
                cands.push((4, Ev::Upgrade { p: q }));
            }
            let q = r.gen_range(0..SLOTS);
            cands.push((3, Ev::GcClosed { p: q }));
            cands.push((2, Ev::GcClosedServer { p: q, min_age_ms: if r.gen_bool(0.5) { 0 } else { GC_GRACE_MS } }));
            cands.push((3, Ev::ClosePass { p: q }));
        }
        for &z in &zombies {
            cands.push((4, Ev::Write { p: z, ops: ops(&mut r) }));
            cands.push((2, Ev::Flush { p: z }));
            if st[z].comp {
                cands.push((6, Ev::CompDrain { p: z }));
                cands.push((3, Ev::Gc { p: z }));
                cands.push((8, Ev::ErasePass { p: z }));
            } else {
                cands.push((2, Ev::Publish { p: z }));
                cands.push((2, Ev::FirstPoll { p: z }));
            }
            cands.push((2, Ev::Erase { p: z, cust: r.gen_range(0..CUSTOMERS) }));
            cands.push((3, Ev::Abandon { p: z }));
            // a zombie that has not noticed its loss closes the database as idle (the server's reaper) or at shutdown
            cands.push((2, Ev::Close { p: z }));
            cands.push((2, Ev::Kill { p: z }));
        }
        for &q in &readers {
            cands.push((1, Ev::Write { p: q, ops: ops(&mut r) }));
            cands.push((1, Ev::Abandon { p: q }));
        }
        cands.push((4, Ev::Retire { p: r.gen_range(0..SLOTS) }));
        // holds: mostly on the owner (the next to lose the database) and on zombies
        let holders: Vec<usize> = owner.into_iter().chain(zombies.iter().copied()).collect();
        if !holders.is_empty() {
            let p = holders[r.gen_range(0..holders.len())];
            let class = hold_class(&mut r);
            let op = match r.gen_range(0..8) {
                0 => None,
                1 => Some(NOp::Delete),
                2 if class == Class::FxRun || class == Class::Other => Some(NOp::Multipart),
                // a slow answer to a LIST of our versions (a publish takes its slot from one, a GC decides from one)
                3 | 4 if class == Class::FxVersion => Some(NOp::List),
                5 => Some(NOp::Get),
                _ => Some(NOp::Put),
            };
            cands.push((8, Ev::Hold { p, class, op, skip: r.gen_range(0..3), land: r.gen_bool(0.75), all: r.gen_bool(0.2), rid: rid + 1 }));
        }
        if !open_rules.is_empty() {
            let i = r.gen_range(0..open_rules.len());
            cands.push((6, Ev::Release { rid: open_rules[i] }));
        }
        cands.push((4, Ev::Sleep { ms: if r.gen_bool(0.2) { r.gen_range(400..1500) } else { r.gen_range(10..400) } }));
        // v3, the late-write pattern (the family this simulator hunts): the owner's next request of a class is held, the
        // owner starts the work that sends it, another process takes the database over and works on it, then the held
        // request goes on. Every part is drawn at random.
        // the cold-restart round (B3, 2026-11-13: hole W on the bench): after many absorbed rows, every process stops
        // cold, then one reopens as writer and its write path publishes through the builder before its compactor starts
        if owner.is_some() && versioned && ev.len() + 12 < steps && r.gen_bool(p_cold) {
            let o = owner.unwrap();
            if st[o].comp {
                ev.push(Ev::CompDrain { p: o });
                if r.gen_bool(0.7) {
                    ev.push(Ev::DropThrough { p: o });
                }
            }
            for q in 0..SLOTS {
                if st[q].role != Role::Down {
                    ev.push(if r.gen_bool(0.5) { Ev::Kill { p: q } } else { Ev::Close { p: q } });
                    st[q] = S::default();
                }
            }
            let q = r.gen_range(0..SLOTS);
            ev.push(Ev::Open { p: q, writer: true });
            st[q].role = Role::Owner;
            for _ in 0..r.gen_range(1..4) {
                ev.push(Ev::Write { p: q, ops: ops(&mut r) });
                if r.gen_bool(0.6) {
                    ev.push(Ev::Publish { p: q });
                }
            }
            ev.push(Ev::ReadCheck { p: q, cust: r.gen_range(0..CUSTOMERS), scan: r.gen_bool(0.5) });
            continue;
        }
        // the slow-read pattern (coordinator, 2026-11-13: reads that came back empty under a slow store and a starved
        // host): every GET of the owner's SSTs and runs (and sometimes its versions) answers late, up to 4 s (past the
        // hold life and T_pin), while it reads one customer and other work goes on; then the store answers again
        if let Some(o) = owner {
            if ev.len() + 10 < steps && r.gen_bool(p_slow) {
                let mut rids = Vec::new();
                let mut classes = vec![Class::Other, Class::FxRun];
                if r.gen_bool(0.5) {
                    classes.push(Class::FxVersion);
                }
                for class in classes {
                    rid += 1;
                    rids.push(rid);
                    ev.push(Ev::Hold { p: o, class, op: Some(NOp::Get), skip: r.gen_range(0..2), land: true, all: true, rid });
                }
                for _ in 0..r.gen_range(1..4) {
                    ev.push(match r.gen_range(0..5) {
                        0 => Ev::Write { p: o, ops: ops(&mut r) },
                        1 if st[o].comp => Ev::CompDrain { p: o },
                        2 => Ev::Sleep { ms: r.gen_range(200..1500) },
                        _ => Ev::ReadCheck { p: o, cust: r.gen_range(0..CUSTOMERS), scan: r.gen_bool(0.5) },
                    });
                }
                ev.push(Ev::ReadCheck { p: o, cust: r.gen_range(0..CUSTOMERS), scan: r.gen_bool(0.5) });
                for id in rids {
                    ev.push(Ev::Release { rid: id });
                }
                ev.push(Ev::Sleep { ms: 300 });
                ev.push(Ev::ReadCheck { p: o, cust: r.gen_range(0..CUSTOMERS), scan: r.gen_bool(0.5) });
                continue;
            }
        }
        if let (Some(o), Some(&q)) = (owner, down.first()) {
            if ev.len() + 16 < steps && r.gen_bool(if versioned { 0.10 } else { 0.25 }) {
                rid += 1;
                let class = if r.gen_bool(0.7) { Class::FxVersion } else { hold_class(&mut r) };
                let op = match r.gen_range(0..4) {
                    0 => Some(NOp::List),
                    1 => None,
                    _ => Some(NOp::Put),
                };
                ev.push(Ev::Hold { p: o, class, op, skip: r.gen_range(0..2), land: r.gen_bool(0.85), all: r.gen_bool(0.3), rid });
                ev.push(match r.gen_range(0..5) {
                    // before the first version: the first build (its slot comes from a LIST) or the first publish
                    _ if !versioned => {
                        if r.gen_bool(0.5) {
                            Ev::FirstStart { p: o }
                        } else {
                            Ev::Publish { p: o }
                        }
                    }
                    0 => Ev::FirstStart { p: o },
                    1 => Ev::Publish { p: o },
                    2 if st[o].comp => Ev::ErasePass { p: o },
                    3 if st[o].comp => Ev::Gc { p: o },
                    _ if st[o].comp => Ev::CompDrain { p: o },
                    _ => Ev::Publish { p: o },
                });
                ev.push(Ev::Open { p: q, writer: true });
                st[o].role = Role::Zombie;
                st[q] = S { role: Role::Owner, comp: false };
                if !versioned {
                    // as the server's write path: a writer of a database with no version builds its first version at
                    // its first writes, and starts its compactor at the next write once it has one
                    ev.push(Ev::Write { p: q, ops: ops(&mut r) });
                    ev.push(Ev::Publish { p: q });
                    ev.push(Ev::Write { p: q, ops: ops(&mut r) });
                    ev.push(Ev::CompStart { p: q });
                    st[q].comp = true;
                }
                for _ in 0..r.gen_range(2..7) {
                    ev.push(match r.gen_range(0..8) {
                        0 | 1 => Ev::Write { p: q, ops: ops(&mut r) },
                        2 => Ev::Flush { p: q },
                        3 => Ev::Publish { p: q },
                        4 => {
                            st[q].comp = true;
                            Ev::CompStart { p: q }
                        }
                        5 => Ev::CompDrain { p: q },
                        6 => Ev::Gc { p: q },
                        _ => Ev::Erase { p: q, cust: r.gen_range(0..CUSTOMERS) },
                    });
                }
                ev.push(Ev::Release { rid });
                if r.gen_bool(0.5) {
                    ev.push(match r.gen_range(0..3) {
                        0 => Ev::FirstPoll { p: o },
                        1 => Ev::Abandon { p: o },
                        _ => Ev::CompDrain { p: o },
                    });
                }
                versioned = true;
                continue;
            }
        }
        let total: u32 = cands.iter().map(|c| c.0).sum();
        let mut x = r.gen_range(0..total);
        let mut chosen = cands[0].1.clone();
        for (w, e) in cands {
            if x < w {
                chosen = e;
                break;
            }
            x -= w;
        }
        // the abstract state after the event
        match &chosen {
            Ev::Open { p, writer } => {
                if *writer {
                    if let Some(o) = owner {
                        st[o].role = Role::Zombie;
                    }
                    st[*p].role = Role::Owner;
                } else {
                    st[*p].role = Role::Reader;
                }
                st[*p].comp = false;
            }
            Ev::Upgrade { p } => {
                if let Some(o) = owner {
                    if o != *p {
                        st[o].role = Role::Zombie;
                    }
                }
                st[*p].role = Role::Owner;
            }
            Ev::Publish { p } | Ev::FirstPoll { p } if owner == Some(*p) => versioned = true,
            Ev::CompStart { p } => st[*p].comp = true,
            Ev::CompStop { p } => st[*p].comp = false,
            Ev::Close { p } | Ev::Abandon { p } | Ev::Kill { p } => st[*p] = S::default(),
            Ev::Hold { rid: id, .. } => {
                rid = *id;
                open_rules.push(*id);
            }
            Ev::Release { rid: id } => open_rules.retain(|x| x != id),
            _ => {}
        }
        ev.push(chosen);
    }
    Script { seed, profile, knobs, events: ev.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ext: Ext::default() }
}

// ---------------------------------------------------------------------------------------------------------------------
// The model of what clients were told

const PENDING: u8 = 0;
const ACKED: u8 = 1;
const FAILED: u8 = 2;
const FAILED_DEFINITE: u8 = 3;

/// The outcome of one write and when it came: `done` is the number of writes issued when the outcome was stored
/// (`u64::MAX` while pending). A write `w` ended before a write `a` started (real time) iff `a.issue > w.done`; only then
/// may `w` be ordered before `a`. Writes that overlap may land in either order (seed 43032: a reader's write upgrades
/// it to the writer and lands after a concurrent write the old writer acknowledged).
struct Status {
    s: AtomicU8,
    done: AtomicU64,
    clock: Arc<AtomicU64>,
}

impl Status {
    fn store(&self, v: u8, o: std::sync::atomic::Ordering) {
        self.done.store(self.clock.load(SeqCst), SeqCst);
        self.s.store(v, o);
    }
    fn load(&self, o: std::sync::atomic::Ordering) -> u8 {
        self.s.load(o)
    }
    fn done(&self) -> u64 {
        if self.s.load(SeqCst) == PENDING { u64::MAX } else { self.done.load(SeqCst) }
    }
}

struct WriteRec {
    issue: u64,
    eid: u32,
    slot: usize,
    value: Option<Vec<u8>>,
    status: Arc<Status>,
}

impl WriteRec {
    /// Whether this write may be ordered after the write issued as `issue`: it had not ended when that one started.
    fn overlaps(&self, issue: u64) -> bool {
        self.status.done() >= issue
    }
}

#[derive(Default)]
struct Model {
    writes: BTreeMap<Vec<u8>, Vec<WriteRec>>,
    issued: u64,
    clock: Arc<AtomicU64>,
}

impl Model {
    /// A copy with the statuses as they are now (the oracle checks against it without holding the lock).
    fn snapshot(&self) -> Model {
        let mut m = Model { writes: BTreeMap::new(), issued: self.issued, clock: Arc::clone(&self.clock) };
        for (k, ws) in &self.writes {
            m.writes.insert(k.clone(), ws.iter().map(|w| WriteRec { issue: w.issue, eid: w.eid, slot: w.slot, value: w.value.clone(), status: Arc::new(Status { s: AtomicU8::new(w.status.load(SeqCst)), done: AtomicU64::new(w.status.done.load(SeqCst)), clock: Arc::clone(&self.clock) }) }).collect());
        }
        m
    }

    fn issue(&mut self, eid: u32, slot: usize, ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Arc<Status> {
        let st = Arc::new(Status { s: AtomicU8::new(PENDING), done: AtomicU64::new(u64::MAX), clock: Arc::clone(&self.clock) });
        self.issued += 1;
        self.clock.store(self.issued, SeqCst);
        for (k, v) in ops {
            self.writes.entry(k.clone()).or_default().push(WriteRec { issue: self.issued, eid, slot, value: v.clone(), status: Arc::clone(&st) });
        }
        st
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// The executor

struct Slot {
    life: u64,
    store: Arc<dyn ObjectStore>,
    lc: Arc<Lifecycle>,
    t: Arc<tokio::sync::Mutex<Option<Tenant>>>,
    comp: Arc<Mutex<Option<Arc<Compactor>>>>,
}

pub struct Run {
    pub seed: u64,
    pub violations: Vec<String>,
    pub trace: Vec<String>,
    pub stats: String,
}

struct World {
    id: String,
    net: Arc<Net>,
    raw: Arc<dyn ObjectStore>,
    slots: Vec<Slot>,
    next_life: u64,
    created: bool,
    model: Arc<Mutex<Model>>,
    inflight: Vec<(u32, tokio::task::JoinHandle<()>)>,
    log: Arc<Mutex<Vec<String>>>,
    knobs: u8,
    /// sim-test-2: restore points, marks, forks, and the violations found during the run (`ext.rs`).
    x: ext::XState,
    /// sim-test-3: the replicated log, the lease as the script gives it, the whole-database lifecycle (`ext3.rs`).
    x3: ext3::X3,
    /// The event of each slot's current opening (`ReadCheck` checks the writes acknowledged since).
    opened_at: Arc<Mutex<Vec<u32>>>,
}

const STEP_WAIT: Duration = Duration::from_millis(1500);

fn new_slot(net: &Arc<Net>, life: u64) -> Slot {
    let store = net.proc_store(life);
    let lc = Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&store))), None);
    Slot { life, store, lc, t: Arc::new(tokio::sync::Mutex::new(None)), comp: Arc::new(Mutex::new(None)) }
}

fn short(e: &anyhow::Error) -> String {
    let s = format!("{e:#}");
    s.chars().take(160).collect()
}

impl World {
    fn note(&self, s: String) {
        self.net.note(s.clone());
        self.log.lock().unwrap().push(format!("[{}] {s}", self.net.step.load(SeqCst)));
    }

    /// Run one event: in its own task, waited for up to `STEP_WAIT`; past that it stays in flight (a stuck operation of
    /// that process) and the script goes on.
    async fn step(&mut self, eid: u32, e: &Ev) {
        self.net.step.store(eid as u64, SeqCst);
        let fut: Option<futures::future::BoxFuture<'static, String>> = match e.clone() {
            Ev::Sleep { ms } => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                None
            }
            Ev::Release { rid } => {
                self.net.release(rid);
                None
            }
            Ev::Mark { tag } => {
                self.mark(tag).await;
                None
            }
            Ev::Storm { on } => {
                self.storm(on);
                None
            }
            Ev::Hold { p, class, op, skip, land, all, rid } => {
                self.net.hold(Rule { id: rid, life: self.slots[p].life, op, class, skip, land, all });
                None
            }
            Ev::Kill { p } => {
                self.kill_slot(p);
                None
            }
            Ev::LogCopy { r, up } => {
                self.x3_copy(r, up);
                None
            }
            Ev::LogLeader => {
                self.x3_leader();
                None
            }
            Ev::Restart { p } => {
                // sim-test-3: kill -9, then the new process reads the log back (in its own task, as any operation)
                self.kill_slot(p);
                Some(self.op(eid, e.clone()))
            }
            other => Some(self.op(eid, other)),
        };
        if let Some(f) = fut {
            let log = Arc::clone(&self.log);
            let net = Arc::clone(&self.net);
            let desc = format!("{e:?}");
            let desc: String = desc.chars().take(120).collect();
            let mut h = tokio::spawn(async move {
                let out = f.await;
                let line = format!("[{eid}] {desc} -> {out}");
                net.note(line.clone());
                log.lock().unwrap().push(line);
            });
            let wait = if matches!(e, Ev::DropThrough { .. }) { Duration::from_secs(45) } else if matches!(e, Ev::Fork { .. } | Ev::Restore { .. } | Ev::Generate { .. }) { ext::LONG_STEP_WAIT } else { STEP_WAIT };
            if tokio::time::timeout(wait, &mut h).await.is_err() {
                self.note(format!("event {eid} still in flight after {STEP_WAIT:?}"));
                self.inflight.push((eid, h));
            }
        }
    }

    /// Kill -9 of slot `p`: the process sends nothing more; a new life of the slot starts with nothing open.
    fn kill_slot(&mut self, p: usize) {
        let s = &self.slots[p];
        self.net.kill(s.life);
        self.note(format!("kill -9 slot {p} (life {})", s.life));
        self.x3.on_kill(s.life);
        // its tasks go on in memory but reach nothing; they are stopped in the background
        let comp = s.comp.lock().unwrap().take();
        let t = Arc::clone(&s.t);
        tokio::spawn(async move {
            if let Some(c) = comp {
                c.stop().await;
            }
            if let Ok(mut g) = tokio::time::timeout(Duration::from_secs(30), t.lock()).await {
                if let Some(t) = g.take() {
                    let _ = tokio::time::timeout(Duration::from_secs(15), t.abandon()).await;
                }
            }
        });
        self.next_life += 1;
        self.slots[p] = new_slot(&self.net, self.next_life);
    }

    /// The future of one operation on a slot. It takes the slot's tenant lock with `try_lock`: a process busy with a
    /// stuck operation does not start another (the event is skipped).
    fn op(&mut self, eid: u32, e: Ev) -> futures::future::BoxFuture<'static, String> {
        let p = e.slot().unwrap();
        let id = self.id.clone();
        let s = &self.slots[p];
        let (store, lc, tl, compl, life) = (Arc::clone(&s.store), Arc::clone(&s.lc), Arc::clone(&s.t), Arc::clone(&s.comp), s.life);
        let model = Arc::clone(&self.model);
        let created = self.created;
        if let Ev::Open { writer: true, .. } = e {
            self.created = true;
        }
        if matches!(e, Ev::Open { .. } | Ev::Upgrade { .. }) {
            let mut o = self.opened_at.lock().unwrap();
            if o[p] == u32::MAX || matches!(e, Ev::Upgrade { .. }) {
                o[p] = eid;
            }
        }
        let opened = self.opened_at.lock().unwrap()[p];
        let found = Arc::clone(&self.x.found);
        let seed = self.net.seed;
        let kp = self.knobs;
        if ext3::is_ext3(&e) || self.x3.routes(&e) {
            return self.ext3_op(eid, e, created);
        }
        if ext::is_ext(&e) {
            return self.ext_op(eid, e);
        }
        Box::pin(async move {
            let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
            let comp = compl.lock().unwrap().clone();
            match e {
                Ev::Open { writer, .. } => {
                    if g.is_some() {
                        return "already open (skipped)".into();
                    }
                    let r = if !created && writer {
                        lc.create(&id).await
                    } else {
                        match lc.wake(&id).await {
                            Ok(mut t) => {
                                if writer {
                                    match t.upgrade().await {
                                        Ok(()) => Ok(t),
                                        Err(e) => {
                                            let _ = t.abandon().await;
                                            Err(e)
                                        }
                                    }
                                } else {
                                    Ok(t)
                                }
                            }
                            Err(e) => Err(e),
                        }
                    };
                    match r {
                        Ok(t) => {
                            let ep = t.writer_epoch();
                            *g = Some(t);
                            format!("opened (life {life}, writer epoch {ep:?})")
                        }
                        Err(e) => format!("open failed: {}", short(&e)),
                    }
                }
                Ev::Upgrade { .. } => {
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if t.is_writer() {
                        return "already writer".into();
                    }
                    match t.upgrade().await {
                        Ok(()) => format!("upgraded (epoch {:?})", t.writer_epoch()),
                        Err(e) => format!("upgrade failed: {}", short(&e)),
                    }
                }
                Ev::Write { ops, .. } => {
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    let writer = t.is_writer();
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = ops.iter().enumerate().map(|(j, (c, i, del))| (ckey(*c, *i), if *del { None } else { Some(ext::value(kp, format!("s{seed}-e{eid}-{j}"))) })).collect();
                    // the last op of a key in one batch is what the batch leaves
                    let mut last: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
                    for (k, v) in &kv {
                        last.insert(k.clone(), v.clone());
                    }
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = last.into_iter().collect();
                    let st = model.lock().unwrap().issue(eid, p, &kv);
                    t.fx_sync();
                    let batch: Vec<Op> = kv.iter().map(|(k, v)| match v {
                        Some(v) => Op::Put(k.clone(), v.clone()),
                        None => Op::Delete(k.clone()),
                    }).collect();
                    match t.apply(batch).await {
                        Ok(()) => {
                            st.store(ACKED, SeqCst);
                            format!("acked {} keys", kv.len())
                        }
                        Err(e) => {
                            // `apply` upgrades a reader first: once it is the writer, its write may have landed
                            let definite = !writer && !t.is_writer();
                            st.store(if definite { FAILED_DEFINITE } else { FAILED }, SeqCst);
                            format!("failed{}: {}", if definite { " (definite: not a writer)" } else { "" }, short(&e))
                        }
                    }
                }
                Ev::Erase { cust, .. } => {
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if !t.is_writer() {
                        return "not a writer (skipped)".into();
                    }
                    let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..PER_CUSTOMER).map(|i| (ckey(cust, i), None)).collect();
                    let st = model.lock().unwrap().issue(eid, p, &kv);
                    t.fx_sync();
                    if let Err(e) = t.apply(kv.iter().map(|(k, _)| Op::Delete(k.clone())).collect()).await {
                        st.store(FAILED, SeqCst);
                        return format!("deletes failed: {}", short(&e));
                    }
                    st.store(ACKED, SeqCst);
                    match t.fx_erasure_register("msgs", format!("c{cust:02}").as_bytes(), &cust_targets(cust), None).await {
                        Ok(Some(e)) => format!("deletes acked; erasure {} registered, S_e {}", e.id, e.s_e),
                        Ok(None) => "deletes acked; no erasure (sealed runs off)".into(),
                        Err(e) => format!("deletes acked; register failed: {}", short(&e)),
                    }
                }
                Ev::ReadCheck { cust, scan, .. } => {
                    let Some(t) = g.as_ref() else { return "not open (skipped)".into() };
                    // what must come back: per key, the last write this opening acknowledged before the read starts
                    let snap = model.lock().unwrap().snapshot();
                    let mut must: BTreeMap<Vec<u8>, (u32, Vec<Option<Vec<u8>>>)> = BTreeMap::new();
                    for i in 0..PER_CUSTOMER {
                        let k = ckey(cust, i);
                        let Some(ws) = snap.writes.get(&k) else { continue };
                        let Some(w) = ws.iter().rposition(|w| w.slot == p && opened != u32::MAX && w.eid > opened && w.status.load(SeqCst) == ACKED) else { continue };
                        let mut legal: Vec<Option<Vec<u8>>> = ws[w..].iter().map(|x| x.value.clone()).collect();
                        // a write of another process that overlapped this one may be ordered after it
                        legal.extend(ws[..w].iter().filter(|x| x.status.load(SeqCst) != FAILED_DEFINITE && x.overlaps(ws[w].issue)).map(|x| x.value.clone()));
                        must.insert(k, (ws[w].eid, legal));
                    }
                    if must.is_empty() {
                        return "nothing of this opening to check".into();
                    }
                    let got: Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>, String> = if scan {
                        let prefix = cust_targets(cust).ranges[0].0.clone();
                        match t.scan_prefix(&prefix).await {
                            Ok(rows) => {
                                let m: BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                                Ok(must.keys().map(|k| (k.clone(), m.get(k).cloned())).collect())
                            }
                            Err(e) => Err(short(&e)),
                        }
                    } else {
                        let mut m = BTreeMap::new();
                        let mut err = None;
                        for k in must.keys() {
                            match t.get(k).await {
                                Ok(v) => {
                                    m.insert(k.clone(), v.map(|b| b.to_vec()));
                                }
                                Err(e) => {
                                    err = Some(short(&e));
                                    break;
                                }
                            }
                        }
                        match err {
                            Some(e) => Err(e),
                            None => Ok(m),
                        }
                    };
                    match got {
                        Err(e) => format!("read failed (allowed): {e}"),
                        Ok(m) => {
                            let mut bad = 0;
                            for (k, (weid, legal)) in &must {
                                let v = m.get(k).cloned().flatten();
                                if !legal.contains(&v) {
                                    bad += 1;
                                    found.lock().unwrap().push(format!(
                                        "read-your-writes ({}): {} in process {p}: read {} where its own write of event {weid} was acknowledged before the read",
                                        if scan { "scan" } else { "get" },
                                        String::from_utf8_lossy(&k[3..]),
                                        match &v {
                                            Some(x) => String::from_utf8_lossy(x).chars().take(40).collect::<String>(),
                                            None => "absent".into(),
                                        }
                                    ));
                                }
                            }
                            format!("{} {} rows checked, {bad} wrong", if scan { "scan" } else { "get" }, must.len())
                        }
                    }
                }
                Ev::Flush { .. } => {
                    let Some(t) = g.as_ref() else { return "not open (skipped)".into() };
                    let Some(db) = t.fx_writer_db() else { return "not a writer (skipped)".into() };
                    match db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await {
                        Ok(()) => "flushed".into(),
                        Err(e) => format!("flush failed: {e}"),
                    }
                }
                Ev::Publish { .. } => {
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if !t.is_writer() {
                        return "not a writer (skipped)".into();
                    }
                    // the first publish waits out E1's announcement (rule 26), as the server's next writes would
                    for _ in 0..10 {
                        match t.fx_publish(&build_opts_for(kp), None).await {
                            Ok(Some(r)) => return format!("published (x_d {})", r.x_d),
                            Ok(None) if t.envelope_seq().is_none() => tokio::time::sleep(Duration::from_millis(T_PIN_MS / 3)).await,
                            Ok(None) => return "nothing published".into(),
                            Err(e) => return format!("publish failed: {}", short(&e)),
                        }
                    }
                    "nothing published (E1 not yet passed)".into()
                }
                Ev::FirstStart { .. } => {
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if !t.is_writer() {
                        return "not a writer (skipped)".into();
                    }
                    for _ in 0..10 {
                        match t.fx_first_start(&build_opts_for(kp)).await {
                            Ok(true) => return "first build started".into(),
                            Ok(false) if t.envelope_seq().is_none() && t.fx_view().is_none() => tokio::time::sleep(Duration::from_millis(T_PIN_MS / 3)).await,
                            Ok(false) => return "no first build to start".into(),
                            Err(e) => return format!("first start failed: {}", short(&e)),
                        }
                    }
                    "no first build (E1 not yet passed)".into()
                }
                Ev::FirstPoll { .. } => {
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    match t.fx_first_poll(false).await {
                        Ok(Some(r)) => format!("first version installed (x_d {})", r.x_d),
                        Ok(None) => "no first version yet".into(),
                        Err(e) => format!("first poll failed: {}", short(&e)),
                    }
                }
                Ev::CompStart { .. } => {
                    // as the server's `Entry::maybe_start_fxc`: a writer with a version, no first build running, the
                    // database claimed, and a catch-up publish first when an ungated pass may have folded rows out of L0
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if comp.is_some() {
                        return "compactor already running".into();
                    }
                    if !t.is_writer() || t.fx_view().is_none() || t.fx_first_running() {
                        return "no writer, no version, or a first build running (skipped)".into();
                    }
                    if !t.fx_claim() {
                        return "claim refused".into();
                    }
                    let mut caught = String::new();
                    match manifest::needs_catch_up(&store, &id).await {
                        Ok(false) => {}
                        Ok(true) => {
                            let scan_ms = manifest::now_ms();
                            let mut o = t.fx_auto_opts();
                            o.reseal = Reseal::None;
                            match t.fx_publish(&o, None).await {
                                Ok(Some(_)) => {
                                    caught = "after a catch-up publish; ".into();
                                    if let Err(e) = manifest::note_caught_up(&store, &id, scan_ms).await {
                                        caught = format!("after a catch-up publish (mark not written: {}); ", short(&e));
                                    }
                                }
                                Ok(None) => {
                                    t.fx_unclaim();
                                    return "catch-up published nothing: no compactor yet".into();
                                }
                                Err(e) => {
                                    t.fx_unclaim();
                                    return format!("catch-up publish failed: {}", short(&e));
                                }
                            }
                        }
                        Err(e) => {
                            t.fx_unclaim();
                            return format!("marks unreadable: {}", short(&e));
                        }
                    }
                    match Compactor::start(t, Arc::clone(&store), knobs_for(kp), pool::Pool::new(2), tokio::runtime::Handle::current()).await {
                        Ok(c) => {
                            c.run();
                            *compl.lock().unwrap() = Some(c);
                            format!("compactor started {caught}")
                        }
                        Err(e) => {
                            t.fx_unclaim();
                            format!("compactor start failed: {}", short(&e))
                        }
                    }
                }
                Ev::CompDrain { .. } => {
                    let Some(c) = comp else { return "no compactor (skipped)".into() };
                    drop(g);
                    let r = c.drain(false).await;
                    let mut g = tl.lock().await;
                    if let Some(t) = g.as_mut() {
                        t.fx_sync();
                    }
                    match r {
                        Ok(()) => format!("drained (version {})", c.current().await.version),
                        Err(e) => format!("drain failed: {}", short(&e)),
                    }
                }
                Ev::DropThrough { .. } => {
                    let Some(c) = comp else { return "no compactor (skipped)".into() };
                    // the tenant serves the compactor's newest version first, so its own holds do not keep the old floor
                    if let Some(t) = g.as_mut() {
                        t.fx_sync();
                    }
                    drop(g);
                    let x = c.current().await.x_d;
                    match c.drop_through(x, Duration::from_secs(40)).await {
                        Ok((a, b)) => format!("dropped through {x} ({a}, {b})"),
                        Err(e) => format!("drop failed: {}", short(&e)),
                    }
                }
                Ev::CompStop { .. } => {
                    let c = compl.lock().unwrap().take();
                    let Some(c) = c else { return "no compactor (skipped)".into() };
                    c.stop().await;
                    "compactor stopped".into()
                }
                Ev::Gc { .. } => {
                    let Some(c) = comp else { return "no compactor (skipped)".into() };
                    drop(g);
                    match gc::gc_once(&c).await {
                        Ok(()) => "gc pass".into(),
                        Err(e) => format!("gc failed: {}", short(&e)),
                    }
                }
                Ev::ErasePass { .. } => {
                    let Some(c) = comp else { return "no compactor (skipped)".into() };
                    drop(g);
                    let out = erase_pass(&store, &id, &c).await;
                    let mut g = tl.lock().await;
                    if let Some(t) = g.as_mut() {
                        t.fx_sync();
                    }
                    match out {
                        Ok(s) => s,
                        Err(e) => format!("erase pass failed: {}", short(&e)),
                    }
                }
                Ev::Retire { .. } => {
                    let served = g.as_ref().and_then(|t| t.fx_served_version());
                    drop(g);
                    match retire(&store, &id, served).await {
                        Ok(s) => s,
                        Err(e) => format!("retire failed: {}", short(&e)),
                    }
                }
                Ev::GcClosedServer { min_age_ms, .. } => {
                    let a = match crate::lifecycle::gc_tenant_closed(&store, id.as_str(), Duration::from_millis(min_age_ms)).await {
                        Ok(r) => format!("slate closed gc: {} deletes", r.requests.delete),
                        Err(e) => format!("slate closed gc failed: {}", short(&e)),
                    };
                    let b = match gc::gc_closed(&store, &id).await {
                        Ok(_) => "fx closed gc pass".to_string(),
                        Err(e) => format!("fx closed gc failed: {}", short(&e)),
                    };
                    format!("{a}; {b}")
                }
                Ev::GcClosed { .. } => match gc::gc_closed(&store, &id).await {
                    Ok(_) => "closed gc pass".into(),
                    Err(e) => format!("closed gc failed: {}", short(&e)),
                },
                Ev::ClosePass { .. } => match crate::lifecycle::compact_tenant(&store, &store, &id).await {
                    Ok(r) => format!("close-time pass: {}", if r.is_some() { "ran" } else { "nothing" }),
                    Err(e) => format!("close-time pass failed: {}", short(&e)),
                },
                Ev::Close { .. } => {
                    let c = compl.lock().unwrap().take();
                    if let Some(c) = c {
                        c.stop().await;
                    }
                    let Some(t) = g.take() else { return "not open (skipped)".into() };
                    match t.close().await {
                        Ok(()) => "closed".into(),
                        Err(e) => format!("close failed: {}", short(&e)),
                    }
                }
                Ev::Abandon { .. } => {
                    let c = compl.lock().unwrap().take();
                    if let Some(c) = c {
                        c.stop().await;
                    }
                    let Some(t) = g.take() else { return "not open (skipped)".into() };
                    format!("abandoned (close done {})", t.abandon().await)
                }
                Ev::Kill { .. } | Ev::Hold { .. } | Ev::Release { .. } | Ev::Sleep { .. } | Ev::LogCopy { .. } | Ev::LogLeader => unreachable!(),
                _ => unreachable!("sim-test-2 events run in ext.rs"),
            }
        })
    }

    /// Settle: every hold released, faults off, in-flight work ended, every process stopped.
    async fn settle(&mut self) {
        self.net.step.store(u32::MAX as u64, SeqCst);
        self.net.release_all();
        self.net.set_calm();
        let t0 = tokio::time::Instant::now();
        for (eid, h) in std::mem::take(&mut self.inflight) {
            let left = Duration::from_secs(30).saturating_sub(t0.elapsed());
            if tokio::time::timeout(left, h).await.is_err() {
                self.note(format!("settle: event {eid} did not end"));
            }
        }
        // sim-test-3: the log's copies back, a restart parked at `hold_kept_records` goes on, kept records retried
        self.x3_settle().await;
        for p in 0..SLOTS {
            let s = &self.slots[p];
            let c = s.comp.lock().unwrap().take();
            if let Some(c) = c {
                let _ = tokio::time::timeout(Duration::from_secs(20), c.stop()).await;
            }
            let t = match tokio::time::timeout(Duration::from_secs(20), s.t.lock()).await {
                Ok(mut g) => g.take(),
                Err(_) => {
                    self.note(format!("settle: slot {p} still busy"));
                    None
                }
            };
            if let Some(t) = t {
                // only the database's current writer closes cleanly; every other process gives up its tenant
                let writer = t.is_writer();
                let r = if writer {
                    tokio::time::timeout(Duration::from_secs(20), t.close()).await.map(|r| r.map_err(|e| short(&e)))
                } else {
                    tokio::time::timeout(Duration::from_secs(20), async move {
                        t.abandon().await;
                        Ok(())
                    })
                    .await
                };
                self.note(format!("settle: slot {p} {} {:?}", if writer { "close" } else { "abandon" }, r));
            }
        }
        // tasks a close left behind (an abandon's close goes on in the background)
        for _ in 0..200 {
            if !crate::lifecycle::close_pending(&self.id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// One erasure pass: every open erasure whose deletes the compactor's version absorbed is scrubbed in one batch,
/// committed with the compactor, and recorded (as `ctl/erase.rs` `run_pass_scrub` does, without its agent).
async fn erase_pass(store: &Arc<dyn ObjectStore>, id: &str, c: &Arc<Compactor>) -> anyhow::Result<String> {
    let base = c.current().await;
    let mut all = erase::list(store, id).await?;
    let mut ready = Vec::new();
    for (i, e) in all.iter().enumerate() {
        if e.state == "pending" && e.s_e <= base.x_d {
            if let Some(t) = erase::targets_of(store, id, e).await? {
                ready.push((i, t));
            }
        }
    }
    if ready.is_empty() {
        return Ok("no erasure ready".into());
    }
    let items: Vec<(&Targets, u64)> = ready.iter().map(|(i, t)| (t, all[*i].s_e)).collect();
    let batch = Batch::new(&items);
    let v = view::View::new(Arc::clone(store), base.clone(), None)?;
    // as the server: no merge or reseal of the compactor starts meanwhile
    let _pause = c.pause_rewrites();
    let s = erase::scrub_version(store, id, &v, base.version + 1, base.writer_epoch, &batch, &erase::ScrubOpts::default()).await?;
    let published = match &s.version {
        None => Some(base.clone()),
        Some(_) => c.commit_scrub(&base, &s, batch.max_s_e).await?,
    };
    drop(_pause);
    let Some(newv) = published else {
        for (_, _, n) in &s.sealed {
            if let Some(l) = n {
                erase::delete_run(store, id, &l.path).await?;
            }
        }
        for (_, n) in &s.delta {
            if let Some(l) = n {
                erase::delete_run(store, id, &l.path).await?;
            }
        }
        return Ok(format!("scrub on version {} not published (the version moved)", base.version));
    };
    for (i, t) in &ready {
        erase::record_scrub(store, id, &mut all[*i], &s, newv.version, t, &newv).await?;
    }
    Ok(format!("scrub of {} erasures published as version {} (base {}, {} rows, {} runs retired)", ready.len(), newv.version, base.version, s.rows, s.retired.len()))
}

/// The retire step of every scrubbed erasure, as `ctl/erase.rs` runs it: newest and served versions from the process's
/// tenant, or the store's newest when it has none.
async fn retire(store: &Arc<dyn ObjectStore>, id: &str, served: Option<u64>) -> anyhow::Result<String> {
    let mut all = erase::list(store, id).await?;
    let top = all.iter().filter_map(|e| e.scrubbed_version).max().unwrap_or(0);
    let listed = manifest::list_versions(store, id).await?.last().copied().unwrap_or(top);
    let (newest, served) = match served {
        Some(s) => (listed.max(top), s),
        None => (listed, listed),
    };
    let mut open: Vec<&mut erase::Erasure> = all.iter_mut().filter(|e| e.done_ms.is_none() && e.scrubbed_version.is_some()).collect();
    if open.is_empty() {
        return Ok("nothing to retire".into());
    }
    let before: Vec<String> = open.iter().map(|e| serde_json::to_string(&**e).unwrap_or_default()).collect();
    erase::retire_all(store, id, &mut open, newest, served, GC_GRACE_MS).await?;
    let mut deleted = 0;
    for (e, b) in open.iter().zip(before) {
        deleted += e.retired.iter().filter(|r| r.deleted_ms.is_some()).count();
        if serde_json::to_string(&**e).unwrap_or_default() != b {
            erase::save(store, id, e).await?;
        }
    }
    Ok(format!("retire (newest {newest}, served {served}): {deleted} retired runs deleted so far"))
}

// ---------------------------------------------------------------------------------------------------------------------
// The oracle

async fn read_all(t: &Tenant) -> BTreeMap<Vec<u8>, Result<Option<Vec<u8>>, String>> {
    let mut out = BTreeMap::new();
    for c in 0..CUSTOMERS {
        for i in 0..PER_CUSTOMER {
            let k = ckey(c, i);
            let r = t.get(&k).await.map(|v| v.map(|b| b.to_vec())).map_err(|e| short(&e));
            out.insert(k, r);
        }
    }
    out
}

fn show(k: &[u8]) -> String {
    String::from_utf8_lossy(&k[3..]).into_owned()
}

fn show_v(v: &Result<Option<Vec<u8>>, String>) -> String {
    match v {
        Ok(Some(v)) => String::from_utf8_lossy(v).into_owned(),
        Ok(None) => "absent".into(),
        Err(e) => format!("error {e}"),
    }
}

/// Check one set of reads against the model. Returns the violations.
fn check_reads(what: &str, reads: &BTreeMap<Vec<u8>, Result<Option<Vec<u8>>, String>>, model: &Model) -> Vec<String> {
    let mut v = Vec::new();
    for (k, got) in reads {
        let got = match got {
            Ok(g) => g,
            Err(e) => {
                v.push(format!("{what}: read error on {}: {e}", show(k)));
                continue;
            }
        };
        let empty = Vec::new();
        let ws = model.writes.get(k).unwrap_or(&empty);
        // the last acknowledged write, then every later write whose outcome is unknown, are the legal answers
        let last_ack = ws.iter().rposition(|w| w.status.load(SeqCst) == ACKED);
        let mut legal: Vec<Option<Vec<u8>>> = Vec::new();
        match last_ack {
            Some(i) => legal.push(ws[i].value.clone()),
            None => legal.push(None),
        }
        let from = last_ack.map(|i| i + 1).unwrap_or(0);
        for w in &ws[from..] {
            let s = w.status.load(SeqCst);
            if s == PENDING || s == FAILED {
                legal.push(w.value.clone());
            }
        }
        // writes issued before the last acknowledged one but not ended when it started may be ordered after it
        if let Some(i) = last_ack {
            for w in ws[..i].iter().filter(|w| w.status.load(SeqCst) != FAILED_DEFINITE && w.overlaps(ws[i].issue)) {
                legal.push(w.value.clone());
            }
        }
        if legal.contains(got) {
            continue;
        }
        // name the case
        let definite = ws.iter().find(|w| w.status.load(SeqCst) == FAILED_DEFINITE && w.value.as_ref() == got.as_ref() && got.is_some());
        let older = ws.iter().find(|w| w.value.as_ref() == got.as_ref());
        let msg = if let Some(w) = definite {
            format!("a write that failed with a definite error is read (event {}, slot {})", w.eid, w.slot)
        } else if let Some(i) = last_ack {
            let a = &ws[i];
            match (got, older) {
                (_, Some(o)) => format!(
                    "acknowledged write lost: expected {} (event {}, slot {}, issue {}), read the older {} (event {}, slot {})",
                    show_v(&Ok(a.value.clone())),
                    a.eid,
                    a.slot,
                    a.issue,
                    show_v(&Ok(got.clone())),
                    o.eid,
                    o.slot
                ),
                (None, None) => format!("acknowledged write lost: expected {} (event {}, slot {}), read absent", show_v(&Ok(a.value.clone())), a.eid, a.slot),
                (Some(_), None) => format!("read a value never written: {}", show_v(&Ok(got.clone()))),
            }
        } else {
            format!("read {} but no write of it was acknowledged", show_v(&Ok(got.clone())))
        };
        let erased = a_delete_acked(ws, last_ack);
        v.push(format!("{what}: {} {}{}", show(k), msg, if erased { " [a deleted or erased row is readable]" } else { "" }));
    }
    v
}

fn a_delete_acked(ws: &[WriteRec], last_ack: Option<usize>) -> bool {
    last_ack.is_some_and(|i| ws[i].value.is_none())
}

async fn oracle(w: &World) -> Vec<String> {
    let mut v = Vec::new();
    if !w.created {
        return v;
    }
    view::clear_cache();
    let fresh = w.net.proc_store(1_000_000 + w.next_life);
    let lc = Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&fresh))), None);
    let mut t = match tokio::time::timeout(Duration::from_secs(60), lc.wake(&w.id)).await {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            v.push(format!("fresh opening failed: {}", short(&e)));
            return v;
        }
        Err(_) => {
            v.push("fresh opening did not return in 60 s".into());
            return v;
        }
    };
    let model = w.model.lock().unwrap().snapshot();
    let first = read_all(&t).await;
    let next = read_all(&t).await;
    for (k, a) in &first {
        if next.get(k) != Some(a) {
            v.push(format!("FIRST and NEXT differ on {}: first {}, next {}", show(k), show_v(a), show_v(next.get(k).unwrap())));
        }
    }
    v.extend(check_reads("reader", &first, &model));
    let up = tokio::time::timeout(Duration::from_secs(60), t.upgrade()).await;
    match up {
        Ok(Ok(())) => {
            let wr = read_all(&t).await;
            for (k, a) in &first {
                if wr.get(k) != Some(a) {
                    v.push(format!("reader and writer differ on {}: reader {}, writer {}", show(k), show_v(a), show_v(wr.get(k).unwrap())));
                }
            }
            v.extend(check_reads("writer", &wr, &model));
        }
        Ok(Err(e)) => v.push(format!("fresh upgrade failed: {}", short(&e))),
        Err(_) => v.push("fresh upgrade did not return in 60 s".into()),
    }
    // no live run deleted: every run of the version the fresh opening serves exists
    if let Some(n) = t.fx_served_version() {
        match manifest::load(&w.raw, &w.id, n).await {
            Ok(tv) => {
                for path in tv.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| r.path.clone())).chain(tv.delta.iter().map(|d| d.path.clone())) {
                    if let Err(object_store::Error::NotFound { .. }) = w.raw.head(&object_store::path::Path::from(path.as_str())).await {
                        v.push(format!("live run deleted: version {} (served by a fresh opening) lists {path}", tv.version));
                    }
                }
            }
            Err(e) => v.push(format!("the served version {n} is unreadable: {}", short(&e))),
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(20), t.close()).await;
    v
}

// ---------------------------------------------------------------------------------------------------------------------
// Running a script

static RUNS: AtomicU64 = AtomicU64::new(0);

/// The tenants (a world's id and its forks, `{id}-fork{tag}`) that run with a restore window: the server sets
/// `lifecycle::WAL_RETAIN` for a database with restore points, so the scripts that take points keep their WAL as the
/// server does (`ext.rs` `SlateGc` already passes the same window to SlateDB's GC). Without it the server's closed pass
/// (`GcClosedServer`, `lifecycle::gc_tenant_closed`) deletes the WAL below the replay boundary and a restore point's
/// restore answers retention_gap, which a server with points never does (sim-all-on-nh, gen 2 seed 14).
static RETAINED: LazyLock<Mutex<Vec<String>>> = LazyLock::new(Default::default);
pub const SIM_WAL_RETAIN: Duration = Duration::from_secs(3600);

fn sim_wal_retain(name: &str) -> Duration {
    let name = name.split('@').next().unwrap_or(name);
    if RETAINED.lock().unwrap().iter().any(|id| name == id || name.strip_prefix(id.as_str()).is_some_and(|r| r.starts_with("-fork"))) {
        SIM_WAL_RETAIN
    } else {
        Duration::ZERO
    }
}

/// Removes a world's tenants from `RETAINED` when its run ends.
struct Retained(Option<String>);
impl Drop for Retained {
    fn drop(&mut self) {
        if let Some(id) = self.0.take() {
            RETAINED.lock().unwrap().retain(|x| *x != id);
        }
    }
}

pub async fn run_script(s: &Script) -> Run {
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let net = Net::new(Arc::clone(&raw), s.seed, s.profile);
    let n = RUNS.fetch_add(1, SeqCst);
    let _ = crate::lifecycle::WAL_RETAIN.set(sim_wal_retain);
    let _retained = Retained(s.events.iter().any(|(_, e)| matches!(e, Ev::Point { .. })).then(|| {
        let id = format!("sim-{}-{n}", s.seed);
        RETAINED.lock().unwrap().push(id.clone());
        id
    }));
    let mut w = World {
        id: format!("sim-{}-{n}", s.seed),
        net: Arc::clone(&net),
        raw,
        slots: (0..SLOTS as u64).map(|l| new_slot(&net, l)).collect(),
        next_life: SLOTS as u64 - 1,
        created: false,
        model: Default::default(),
        inflight: Vec::new(),
        log: Default::default(),
        knobs: s.knobs,
        x: ext::XState::new(s.profile),
        x3: ext3::X3::new(&s.ext, Arc::clone(&net)),
        opened_at: Arc::new(Mutex::new(vec![u32::MAX; SLOTS])),
    };
    w.note(format!("seed {} knobs {} profile {:?}", s.seed, s.knobs, s.profile));
    for (eid, e) in &s.events {
        w.step(*eid, e).await;
    }
    w.settle().await;
    // sim-test-3: with the log, a fresh process takes the database over (its takeover reads the log back) before the
    // oracle; a database dropped or erased last is checked as absent
    ext3::settle_absent(&w).await;
    let mut violations = ext3::before_oracle(&w).await;
    if w.x3.is_absent() {
        violations.extend(ext3::oracle_absent(&w).await);
    } else {
        violations.extend(oracle(&w).await);
    }
    violations.extend(ext::oracle_ext(&w).await);
    if w.x3.strict_versions {
        violations.extend(ext3::strict_versions(&w).await);
    }
    let (issued, acked) = {
        let model = w.model.lock().unwrap();
        (model.issued, model.writes.values().flatten().filter(|x| x.status.load(SeqCst) == ACKED).count())
    };
    let stats = format!("events {} writes issued {issued} key-writes acked {acked} requests {} faults {}{}{}", s.events.len(), net.requests.load(SeqCst), net.faults.load(SeqCst), ext::coverage(&w), ext3::coverage(&w));
    let trace = net.trace();
    // the slots' own processes' state for this database (views, holds) is process-wide: forget it for the next seed
    view::clear_cache();
    Run { seed: s.seed, violations, trace, stats }
}

/// A short form of a script (for the report and the minimal trace).
pub fn describe(s: &Script) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "seed {} knobs {} profile {:?}", s.seed, s.knobs, s.profile);
    for (eid, e) in &s.events {
        let _ = writeln!(out, "  {eid:>3} {e:?}");
    }
    out
}

/// Whether a run keeps the violation being minimized: any violation, or (`SIM_MIN_KEEP`) one that contains that text
/// (for example `writer:` to keep a loss the writer sees, not only a reader).
fn keeps(r: &Run) -> bool {
    match std::env::var("SIM_MIN_KEEP") {
        Ok(k) if !k.is_empty() => r.violations.iter().any(|v| v.contains(&k)),
        _ => !r.violations.is_empty(),
    }
}

/// Remove events while the violation stays (each candidate run `tries` times; a run that shows any violation counts).
pub async fn minimize(s: &Script, tries: usize, budget: usize) -> (Script, Run) {
    let mut best = s.clone();
    let mut best_run: Option<Run> = None;
    let mut runs = 0;
    let mut chunk = (best.events.len() / 2).max(1);
    while chunk >= 1 && runs < budget {
        let mut i = 0;
        let mut progressed = false;
        while i < best.events.len() && runs < budget {
            let mut cand = best.clone();
            let end = (i + chunk).min(cand.events.len());
            // keep the first opening (a database must exist)
            if i == 0 {
                i = 1;
                continue;
            }
            cand.events.drain(i..end);
            let mut failed = None;
            for _ in 0..tries {
                runs += 1;
                let r = run_script(&cand).await;
                if keeps(&r) {
                    failed = Some(r);
                    break;
                }
            }
            if let Some(r) = failed {
                best = cand;
                best_run = Some(r);
                progressed = true;
            } else {
                i += chunk;
            }
        }
        if !progressed {
            if chunk == 1 {
                break;
            }
            chunk /= 2;
        }
    }
    let run = match best_run {
        Some(r) => r,
        None => run_script(&best).await,
    };
    (best, run)
}

/// Which slots a script uses (for the trace header).
pub fn slots_used(s: &Script) -> Vec<usize> {
    let mut m: HashMap<usize, ()> = HashMap::new();
    for (_, e) in &s.events {
        if let Some(p) = e.slot() {
            m.insert(p, ());
        }
    }
    let mut v: Vec<usize> = m.into_keys().collect();
    v.sort();
    v
}

#[cfg(test)]
mod oracle_tests {
    use super::*;

    fn read(m: &Model, k: &[u8], v: &str) -> Vec<String> {
        let mut r = BTreeMap::new();
        r.insert(k.to_vec(), Ok(Some(v.as_bytes().to_vec())));
        check_reads("t", &r, &m.snapshot())
    }

    /// Seed 43032's rule: an earlier write still in flight when a later one is acknowledged may land after it; one that
    /// ended before the later one started may not.
    #[test]
    fn overlapping_writes_land_in_either_order_ended_ones_do_not() {
        let k = ckey(2, 13);
        let kv = |v: &str| vec![(k.clone(), Some(v.as_bytes().to_vec()))];
        // in flight: a issued, b issued and acked, a acked after
        let mut m = Model::default();
        let a = m.issue(83, 0, &kv("a"));
        let b = m.issue(84, 1, &kv("b"));
        b.store(ACKED, SeqCst);
        a.store(ACKED, SeqCst);
        assert!(read(&m, &k, "a").is_empty(), "an overlapping write is a legal answer");
        assert!(read(&m, &k, "b").is_empty());
        // still pending when read: legal too
        let mut m = Model::default();
        let _a = m.issue(83, 0, &kv("a"));
        m.issue(84, 1, &kv("b")).store(ACKED, SeqCst);
        assert!(read(&m, &k, "a").is_empty());
        // ended before b started: reading a loses b
        let mut m = Model::default();
        m.issue(83, 0, &kv("a")).store(ACKED, SeqCst);
        m.issue(84, 1, &kv("b")).store(ACKED, SeqCst);
        assert_eq!(read(&m, &k, "a").len(), 1, "a write that ended before an acknowledged one started must not come back");
        // failed before b started (indeterminate): still not legal after b
        let mut m = Model::default();
        m.issue(83, 0, &kv("a")).store(FAILED, SeqCst);
        m.issue(84, 1, &kv("b")).store(ACKED, SeqCst);
        assert_eq!(read(&m, &k, "a").len(), 1);
        // a definite failure is never legal, even overlapping
        let mut m = Model::default();
        let a = m.issue(83, 0, &kv("a"));
        m.issue(84, 1, &kv("b")).store(ACKED, SeqCst);
        a.store(FAILED_DEFINITE, SeqCst);
        assert_eq!(read(&m, &k, "a").len(), 1);
    }
}
