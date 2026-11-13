//! Compaction 2 (way (b)): our manifest, the merged reader and, later, our compactor, over SlateDB's public API.
//!
//! - `manifest.rs`: versioned manifest objects with X_d, the groups with their X_g and sealed runs, the delta runs, E_env.
//! - `view.rs`: an opened version and the read rule over its runs.
//! - `read.rs`: the merged read (SlateDB above X_d, our runs at or below it) for the writer, a reader and a
//!   transaction snapshot; the control layer's raw readers.
//! - `hold.rs`: holds on a version (the drop floor of step 4, the hold life).
//! - `build.rs`: sealed and delta runs built offline from a loaded database (step 3; the compactor is step 4).
//! - `pair.rs`: checkpoints paired with a version, and the control layer's readers through that pair (step 7a: export,
//!   PITR).
//! - `droptest.rs`: a stand-in for step 4's drop job, for tests of the paths that must read through our runs.
//! - `counter.rs`: the waiter's cache of our runs: physical slices, the new-user filter's counters, the staged publish
//!   of a reseal (step 6).
//!
//! Off by default (`--sealed-runs`). With it off, no code here runs and the server reads SlateDB as before. With it on, a
//! database reads through the merged reader once it has a published version.
//!
//! Every direct SlateDB reader of the engine is in this module or in `lifecycle.rs`, or named with its reason in
//! `judge/raw_readers.sh` (plan round 2, fix 3, and its review).

pub mod build;
pub mod codec;
pub mod compactor;
pub mod counter;
pub mod derive;
pub mod drop;
pub mod gc;
pub mod droptest;
pub mod erase;
pub mod floor;
pub mod hold;
pub mod local;
pub mod pool;
pub mod manifest;
pub mod pair;
pub mod read;
pub mod seqfilter;
pub mod view;
#[cfg(test)]
mod counter_bench;
#[cfg(test)]
mod fix6_tests;
#[cfg(test)]
mod close_pass_gate_tests;
#[cfg(test)]
mod review6_tests;
#[cfg(test)]
mod counter_tests;
#[cfg(test)]
mod big_speed_tests;
#[cfg(test)]
pub(crate) mod tests;
#[cfg(test)]
mod review_tests;
#[cfg(test)]
mod fix_tests;
#[cfg(test)]
mod compactor_tests;
#[cfg(test)]
mod review4_tests;
#[cfg(test)]
mod fix4_tests;
#[cfg(test)]
mod tests_7a;
#[cfg(test)]
mod derive_tests;
#[cfg(test)]
mod tests_7b;
#[cfg(test)]
mod i4_joint_tests;
#[cfg(test)]
mod long_reads_tests;
#[cfg(test)]
mod tests_gen;
#[cfg(test)]
mod review_long_reads_tests;
#[cfg(test)]
mod perf_erase_tests;
#[cfg(test)]
mod write_stalls_tests;
#[cfg(test)]
mod review_final_tests;
#[cfg(test)]
pub mod sim;

use std::sync::atomic::{AtomicBool, AtomicU64};

/// `--sealed-runs`: read through our manifest and runs.
pub static ENABLED: AtomicBool = AtomicBool::new(false);
/// With `--sealed-runs N`: the server publishes a version every N write batches of a database (0: never by itself).
pub static PUBLISH_EVERY: AtomicU64 = AtomicU64::new(0);
/// Every how many publishes all groups are resealed (the others write a delta run only).
pub static RESEAL_EVERY: AtomicU64 = AtomicU64::new(3);
/// Raw bytes per group of a database's first version (`--sealed-group-mb`).
pub static GROUP_BYTES: AtomicU64 = AtomicU64::new(1 << 30);
/// T_pin in milliseconds: a reader in reader mode refreshes its version at least this often and refuses to read with a
/// version older than this (rule 6).
pub static T_PIN_MS: AtomicU64 = AtomicU64::new(30_000);
/// How long after E1's announcement envelopes start (rule 26: at least T_pin). `u64::MAX`: T_pin. Tests and the judge's
/// short-lived databases set it lower; with leases no reader in another process serves a database, so nothing can
/// miss the announcement there.
pub static E1_WAIT_MS: AtomicU64 = AtomicU64::new(u64::MAX);

/// Set by `--sealed-test-short-e1-wait` (or FENVIX_SEALED_TEST_SHORT_E1_WAIT=1), tests only: [`check_config`] then
/// accepts an E1 wait shorter than T_pin (review of step 7a, note 4).
pub static E1_WAIT_TEST: AtomicBool = AtomicBool::new(false);

pub fn e1_wait() -> std::time::Duration {
    use std::sync::atomic::Ordering::Relaxed;
    let w = E1_WAIT_MS.load(Relaxed);
    std::time::Duration::from_millis(if w == u64::MAX { T_PIN_MS.load(Relaxed) } else { w })
}

/// F6 (review round 2 of step 0): a manifest PUT whose answer has not arrived within this timeout, retries included,
/// counts as timed out (outcome unknown: the fence of rule 25 follows). It must stay shorter than the lease minus its
/// margin, and the remote lease and the GC grace must each exceed the hold life plus it ([`check_config`]).
pub static PUT_TIMEOUT_MS: AtomicU64 = AtomicU64::new(20_000);
/// The ordinary lease of a version for the drop floor (rule 8: T_pin plus a margin) and its margin.
pub static LEASE_MS: AtomicU64 = AtomicU64::new(60_000);
pub static LEASE_MARGIN_MS: AtomicU64 = AtomicU64::new(30_000);
/// The time-only lease of a drop job outside the waiter's process (rule 10).
pub static REMOTE_LEASE_MS: AtomicU64 = AtomicU64::new(180_000);
/// GC's grace before it deletes a retired or orphaned run (rule 23; the plan's 10 minutes).
pub static GC_GRACE_MS: AtomicU64 = AtomicU64::new(600_000);
/// How long GC keeps a version of our manifest that nothing holds, serves or names (`--sealed-version-retention-secs`).
/// P-perf-bytes: 0 by default (was 7 days). Such a version cannot be read once the drop is on (SlateDB no longer holds
/// the rows between its X_d and the newest one), and restore points, exports and forks keep their versions through
/// their pairs and references; a retired run still waits the GC grace for reads in flight.
pub static VERSION_RETENTION_MS: AtomicU64 = AtomicU64::new(0);

/// The timing rules the drop floor (step 4) relies on, checked at startup: a bad combination is refused.
/// - the PUT timeout is shorter than the lease minus its margin (F6), and the margin is shorter than the lease;
/// - the lease is longer than T_pin (rule 8: T_pin plus a margin);
/// - the remote lease (rule 10) exceeds the hold life plus the PUT timeout (F6);
/// - the GC grace (rule 23) exceeds T_pin plus the hold life plus the PUT timeout (integration-4: a reader may hold a
///   version up to T_pin old when its read step starts, and the step may last the hold life, so a run that version lists
///   must outlive both; review of c2-fix-long-reads, `review_lr_shortest_legal_grace`).
pub fn check_config() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let put = PUT_TIMEOUT_MS.load(Relaxed);
    let lease = LEASE_MS.load(Relaxed);
    let margin = LEASE_MARGIN_MS.load(Relaxed);
    let t_pin = T_PIN_MS.load(Relaxed);
    let hold = hold::HOLD_LIFE_MS.load(Relaxed);
    let remote = REMOTE_LEASE_MS.load(Relaxed);
    let grace = GC_GRACE_MS.load(Relaxed);
    anyhow::ensure!(put > 0, "sealed runs: the manifest PUT timeout must be above 0");
    anyhow::ensure!(margin < lease, "sealed runs: the lease margin ({margin} ms) must be shorter than the lease ({lease} ms)");
    anyhow::ensure!(put < lease - margin, "sealed runs: the manifest PUT timeout ({put} ms) must be shorter than the lease minus its margin ({lease} - {margin} ms) (F6)");
    anyhow::ensure!(lease > t_pin, "sealed runs: the lease ({lease} ms) must be longer than T_pin ({t_pin} ms) (rule 8)");
    anyhow::ensure!(remote > hold + put, "sealed runs: the remote lease ({remote} ms) must exceed the hold life plus the PUT timeout ({hold} + {put} ms) (rule 10, F6)");
    anyhow::ensure!(grace > t_pin + hold + put, "sealed runs: the GC grace ({grace} ms) must exceed T_pin plus the hold life plus the PUT timeout ({t_pin} + {hold} + {put} ms) (rule 23, F6, integration-4)");
    // review of step 7a, note 4: envelopes must not start before every follower has taken E1's announcement (rule 26)
    let wait = E1_WAIT_MS.load(Relaxed);
    anyhow::ensure!(wait == u64::MAX || wait >= t_pin || E1_WAIT_TEST.load(Relaxed), "sealed runs: E1's wait ({wait} ms) must be at least T_pin ({t_pin} ms) (rule 26); a shorter one is for tests only (--sealed-test-short-e1-wait)");
    Ok(())
}

/// Negative control of rule 25's fence-pending state, tests only: reads take holds while a fence is pending.
#[cfg(test)]
pub static CONTROL_NO_FENCE_PENDING: AtomicBool = AtomicBool::new(false);

/// c2-fix-long-reads: renewals of a reader's state made by a read step under the shared lock (diagnostics, tests).
pub static RENEWALS: AtomicU64 = AtomicU64::new(0);
/// Negative control of c2-fix-long-reads, tests only: a read step never renews its state (the code before the fix),
/// so a reader's state ages with the command and the T_pin guard refuses its steps.
#[cfg(test)]
pub static CONTROL_NO_RENEW: AtomicBool = AtomicBool::new(false);

/// Versions published and installed, builds that failed, fence versions written at open (diagnostics).
pub static PUBLISHES: AtomicU64 = AtomicU64::new(0);
pub static PUBLISH_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static FENCES: AtomicU64 = AtomicU64::new(0);

/// Negative controls of step 7a, tests only (`fx/tests_7a.rs`, `FX_7A_CONTROL`): the X_d check only on the first version
/// `load_fx` reads; no operation holds; release E1 writes its marker at once.
#[cfg(test)]
pub static CONTROL_NO_XD_LOOP_CHECK: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
pub static CONTROL_E1_AT_ONCE: AtomicBool = AtomicBool::new(false);

/// Negative control of review note 5, tests only: a refresh's T_pin window starts when its answer arrives, as before.
#[cfg(test)]
pub static CONTROL_LOADED_AFTER_ANSWER: AtomicBool = AtomicBool::new(false);

/// The instant a refresh that started at `t0` counts from (rule 6 with note 5: its start).
pub fn loaded_at(t0: std::time::Instant) -> std::time::Instant {
    #[cfg(test)]
    if CONTROL_LOADED_AFTER_ANSWER.load(std::sync::atomic::Ordering::Relaxed) {
        return std::time::Instant::now();
    }
    t0
}

/// A retryable read error: a revoked hold, or a reader whose version is older than T_pin. The server answers it with a
/// retryable code and the TransientTransactionError label.
#[derive(Debug)]
pub struct Retry(pub String);

impl std::fmt::Display for Retry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Retry {}

/// A publish whose conditional create found its slot taken: the outcome is known, the publisher reloads (rule 24).
#[derive(Debug)]
pub struct LostRace(pub String);

impl std::fmt::Display for LostRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LostRace {}

/// A publish that stopped at its cut: the WAL flush did not end within the F6 timeout (the store does not answer the
/// writer's WAL PUTs). Nothing of the publish was written yet, so the outcome is known: no fence (c2-fix-hangs).
#[derive(Debug)]
pub struct CutNotDurable(pub String);

impl std::fmt::Display for CutNotDurable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CutNotDurable {}

/// A publish whose conditional create failed with an unknown outcome (an error or the F6 timeout): the PUT may still
/// land. It carries the version it tried, so the fence can take that same slot even when the store is still empty.
#[derive(Debug)]
pub struct UnknownPublish {
    pub version: Box<manifest::Version>,
    pub msg: String,
}

impl std::fmt::Display for UnknownPublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "publish of version {} has an unknown outcome: {}", self.version.version, self.msg)
    }
}

impl std::error::Error for UnknownPublish {}

/// Negative control of the first-publish fence (review round 2 of step 3), tests only: an empty store clears the
/// pending fence, as before the fix.
#[cfg(test)]
pub static CONTROL_NO_FIRST_FENCE: AtomicBool = AtomicBool::new(false);

/// Whether an error says this process's SlateDB writer was fenced by a newer one (rule 28: then it publishes nothing more).
pub fn is_fenced(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<slatedb::Error>().is_some_and(|s| s.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)))
}
