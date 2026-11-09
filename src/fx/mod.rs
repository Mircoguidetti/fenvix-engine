//! Compaction 2 (way (b)): our manifest, the merged reader and, later, our compactor, over SlateDB's public API.
//!
//! - `manifest.rs`: versioned manifest objects with X_d, the groups with their X_g and sealed runs, the delta runs, E_env.
//! - `view.rs`: an opened version and the read rule over its runs.
//! - `read.rs`: the merged read (SlateDB above X_d, our runs at or below it) for the writer, a reader and a
//!   transaction snapshot; the control layer's raw readers.
//! - `hold.rs`: holds on a version (the drop floor of step 4, the hold life).
//! - `build.rs`: sealed and delta runs built offline from a loaded database (step 3; the compactor is step 4).
//! - `counter.rs`: the waiter's cache of our runs: physical slices, the new-user filter's counters, the staged publish
//!   of a reseal (step 6).
//!
//! Off by default (`--sealed-runs`). With it off, no code here runs and the server reads SlateDB as before. With it on, a
//! database reads through the merged reader once it has a published version.
//!
//! Every direct SlateDB reader of the engine is in this module or in `lifecycle.rs`, or named with its reason in
//! `judge/raw_readers.sh` (plan round 2, fix 3, and its review).

pub mod build;
pub mod counter;
pub mod hold;
pub mod manifest;
pub mod read;
pub mod view;
#[cfg(test)]
mod counter_bench;
#[cfg(test)]
mod counter_tests;
#[cfg(test)]
mod tests;

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

pub fn e1_wait() -> std::time::Duration {
    use std::sync::atomic::Ordering::Relaxed;
    let w = E1_WAIT_MS.load(Relaxed);
    std::time::Duration::from_millis(if w == u64::MAX { T_PIN_MS.load(Relaxed) } else { w })
}

/// Versions published and installed, builds that failed, fence versions written at open (diagnostics).
pub static PUBLISHES: AtomicU64 = AtomicU64::new(0);
pub static PUBLISH_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static FENCES: AtomicU64 = AtomicU64::new(0);

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

/// Whether an error says this process's SlateDB writer was fenced by a newer one (rule 28: then it publishes nothing more).
pub fn is_fenced(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<slatedb::Error>().is_some_and(|s| s.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)))
}
