//! Holds: a reader's hold on one version of our manifest (plan round 2, fix 1; RULES-FOR-THE-CODE rules 1, 4, 5).
//!
//! In code these are "holds", not "pins": `ctl/pin.rs` already names the keep-awake pins (review round 2 note).
//!
//! - A hold keeps its version (and so its runs) alive in this process, and counts in the drop floor while it lives
//!   (rule 4). The floor is for step 4's drop job: in this step nothing drops SlateDB rows, so the floor is only
//!   computed and tested.
//! - A hold lives at most the hold life (120 s proposed). Past it, the hold is revoked: it leaves the floor, and the
//!   reader, which checks its hold after every step, fails with a retryable error (rule 5).
//!
//! Who takes a hold: a transaction, at begin, for its whole life (it never re-reads the manifest, rule 1). A plain
//! operation (a find, an update's reads) reads the tenant's current version under the database's lock, and that version
//! only changes under the exclusive lock (`Tenant::fx_install`), so every step of one operation reads the same version;
//! it takes a short hold per read, so that the floor sees it.

use super::view::View;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Hold life in milliseconds (rule 5).
pub static HOLD_LIFE_MS: AtomicU64 = AtomicU64::new(120_000);

pub fn hold_life() -> Duration {
    Duration::from_millis(HOLD_LIFE_MS.load(Relaxed))
}

/// The marker text of the retryable error of a revoked hold (the server labels it TransientTransactionError).
pub const REVOKED: &str = "fx hold revoked";

/// The holds of one database in this process.
#[derive(Default)]
pub struct Holds {
    next: AtomicU64,
    live: Mutex<HashMap<u64, Entry>>,
}

struct Entry {
    version: u64,
    x_d: u64,
    since: Instant,
    revoked: Arc<AtomicBool>,
}

pub struct Hold {
    pub view: Arc<View>,
    id: u64,
    holds: Arc<Holds>,
    since: Instant,
    revoked: Arc<AtomicBool>,
}

impl Holds {
    pub fn take(self: &Arc<Self>, view: Arc<View>) -> Hold {
        let id = self.next.fetch_add(1, Relaxed);
        let since = Instant::now();
        let revoked = Arc::new(AtomicBool::new(false));
        self.live.lock().unwrap().insert(id, Entry { version: view.version, x_d: view.x_d, since, revoked: revoked.clone() });
        Hold { view, id, holds: Arc::clone(self), since, revoked }
    }

    /// Revoke every hold older than the hold life: it leaves the floor, and its reader fails at its next check.
    pub fn revoke_expired(&self) -> usize {
        let life = hold_life();
        let mut n = 0;
        let mut m = self.live.lock().unwrap();
        m.retain(|_, e| {
            if e.since.elapsed() > life {
                e.revoked.store(true, Relaxed);
                n += 1;
                false
            } else {
                true
            }
        });
        n
    }

    /// The drop floor's share of the holds: the smallest X_d of a live, unrevoked hold (None: no hold). Step 4's drop
    /// job takes the smaller of this and the X of the newest version published at least one lease ago (rule 7, 8).
    pub fn floor(&self) -> Option<u64> {
        self.revoke_expired();
        self.live.lock().unwrap().values().map(|e| e.x_d).min()
    }

    /// The oldest version held (diagnostics, GC in step 4).
    pub fn oldest_version(&self) -> Option<u64> {
        self.live.lock().unwrap().values().map(|e| e.version).min()
    }

    pub fn count(&self) -> usize {
        self.live.lock().unwrap().len()
    }
}

impl Hold {
    /// After every step of a read: an error once the hold was revoked or outlived the hold life (rule 5).
    pub fn check(&self) -> anyhow::Result<()> {
        if self.revoked.load(Relaxed) || self.since.elapsed() > hold_life() {
            self.revoked.store(true, Relaxed);
            return Err(anyhow::Error::new(super::Retry(format!("{REVOKED}: the read held version {} for more than {:?}; retry", self.view.version, hold_life()))));
        }
        Ok(())
    }

    pub fn age(&self) -> Duration {
        self.since.elapsed()
    }

    /// Revoke now (tests; an operator's kill).
    pub fn revoke(&self) {
        self.revoked.store(true, Relaxed);
        self.holds.live.lock().unwrap().remove(&self.id);
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.holds.live.lock().unwrap().remove(&self.id);
    }
}

/// The holds of every database of this process, one `Holds` per database name, shared by every `Tenant` that serves it
/// (a reopen keeps the holds of the reads still running on the old one) and by the control layer's readers (an export
/// page, PITR's diff). Entries go away with their last user.
static REGISTRY: Mutex<Option<HashMap<String, std::sync::Weak<Holds>>>> = Mutex::new(None);

pub fn for_db(id: &str) -> Arc<Holds> {
    let mut g = REGISTRY.lock().unwrap();
    let m = g.get_or_insert_with(HashMap::new);
    if let Some(h) = m.get(id).and_then(|w| w.upgrade()) {
        return h;
    }
    m.retain(|_, w| w.strong_count() > 0);
    let h = Arc::new(Holds::default());
    m.insert(id.to_string(), Arc::downgrade(&h));
    h
}

tokio::task_local! {
    /// The holds of the operation running on this task (`op_scope`): one per database and version it read with.
    static OP: std::cell::RefCell<Vec<Hold>>;
}

/// Run one operation (one server command) as one scope of holds: every read of the operation, through any number of
/// `Dbs::read` calls, keeps the version it read with in the drop floor until the operation ends (rules 4, 7 and 8; the
/// step 3 review's note on `$lookup`, whose foreign collections are read under new `Dbs::read` calls, between which a
/// publish can land). A read outside any scope keeps only its own short hold.
pub async fn op_scope<F: std::future::Future>(f: F) -> F::Output {
    OP.scope(std::cell::RefCell::new(Vec::new()), f).await
}

/// A read of the current operation is about to read `view` of the database whose holds are `holds`: the operation
/// keeps a hold on that version until it ends (one per database and version).
pub fn op_note(holds: &Arc<Holds>, view: &Arc<View>) {
    #[cfg(test)]
    if super::CONTROL_NO_OP_HOLDS.load(Relaxed) {
        return;
    }
    let _ = OP.try_with(|op| {
        let mut op = op.borrow_mut();
        if !op.iter().any(|h| Arc::ptr_eq(&h.holds, holds) && h.view.version == view.version) {
            op.push(holds.take(Arc::clone(view)));
        }
    });
}

/// After every step of a read: the operation's holds are still alive (rule 5: an operation that outlived the hold life
/// fails with the retryable error).
pub fn op_check() -> anyhow::Result<()> {
    OP.try_with(|op| op.borrow().iter().try_for_each(|h| h.check())).unwrap_or(Ok(()))
}
