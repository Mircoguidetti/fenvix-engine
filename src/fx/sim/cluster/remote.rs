//! P-B3 (2026-11-13): the remote compactor in the simulator (`SIM_REMOTE=1`; the whole batch, since the waiter's switch
//! `fx::remote::WAITER_DEFERS` is process-wide).
//!
//! The scripts of `generate` (or `ext::generate2` with `SIM_GEN=2`) are rewritten:
//! - the owner's compactor start becomes `Defer` (the server's `maybe_start_fxc` under `--sealed-remote-compactor`: a
//!   catch-up publish when needed, then the writer hands its compaction over and follows the store) and `RStart` on the
//!   seed's remote slot (sometimes another slot: a second remote compactor that must wait for the claim);
//! - the owner's drains, GC passes, drops and stops go to the remote slot's compactor (the existing events, on that slot);
//! - `Read` after some writes: the owner reads every key with its state (renewed when due, as the server does before a
//!   command) and the answer is checked against the model at once (a refused read is no violation; a wrong one is);
//! - the late remote publish: the remote's claim renewals and its next version create are held, it works, its claim
//!   lapses, another slot takes the claim over and compacts, the owner writes, then the held requests are let go.
//!
//! A remote compactor runs in a slot as the server's `--compactor-only` process does (`mongo-server/src/remotec.rs`):
//! it takes the claim with the slot's own store (kills and holds reach it), starts `Compactor::start_remote`, renews the
//! claim every lease / 6, and starts a new compactor on the newest version when one stops by itself while the claim
//! holds (a newer writer's version). A kill of the slot stops it; the claim then lapses.

use super::*;
use crate::fx::remote::{self, Claim};
use std::sync::atomic::AtomicU64;

pub const CLAIM_LEASE_MS: u64 = 2000;
/// Longer than the PUT timeout of the simulated world (F6).
pub const CLAIM_MARGIN_MS: u64 = 1200;
/// How long an `RStart` waits for the claim before it gives up.
pub const CLAIM_WAIT_MS: u64 = 6000;

/// Coverage of a remote batch (process-wide, every seed of the batch): remote starts, claims refused, owner reads
/// checked and the keys they read, refused reads.
pub static STARTS: AtomicU64 = AtomicU64::new(0);
pub static CLAIMS_REFUSED: AtomicU64 = AtomicU64::new(0);
pub static READS: AtomicU64 = AtomicU64::new(0);
pub static READ_KEYS: AtomicU64 = AtomicU64::new(0);
pub static READS_REFUSED: AtomicU64 = AtomicU64::new(0);
pub static DEFERS: AtomicU64 = AtomicU64::new(0);

pub fn coverage() -> String {
    format!(
        "remote starts {} (claims not taken {}), takeovers of a claim {}, defers {}, owner reads {} ({} keys checked, {} refused), installs by following {}, acts refused past the deadline {}",
        STARTS.load(SeqCst),
        CLAIMS_REFUSED.load(SeqCst),
        remote::TAKEOVERS.load(SeqCst),
        DEFERS.load(SeqCst),
        READS.load(SeqCst),
        READ_KEYS.load(SeqCst),
        READS_REFUSED.load(SeqCst),
        remote::FOLLOW_INSTALLS.load(SeqCst),
        remote::PAST_DEADLINE.load(SeqCst)
    )
}

pub fn on() -> bool {
    std::env::var("SIM_REMOTE").as_deref() == Ok("1")
}

pub fn is_remote(e: &Ev) -> bool {
    matches!(e, Ev::RStart { .. } | Ev::RStop { .. } | Ev::Defer { .. } | Ev::Read { .. })
}

/// The rewrite of a script for the remote compactor (see the module text).
pub fn generate_remote(base: Script) -> Script {
    let mut r = StdRng::seed_from_u64(base.seed ^ 0xb3b3_b3b3);
    let rs = r.gen_range(0..SLOTS);
    let mut out: Vec<Ev> = Vec::new();
    let mut rid = 900_000u64;
    let mut owner: Option<usize> = None;
    let mut late_done = false;
    for (_, e) in base.events {
        match e {
            Ev::Open { p, writer: true } => {
                owner = Some(p);
                out.push(e);
            }
            Ev::CompStart { p } => {
                out.push(Ev::Defer { p });
                let q = if r.gen_bool(0.15) { (rs + 1 + r.gen_range(0..SLOTS - 1)) % SLOTS } else { rs };
                out.push(Ev::RStart { p: q });
            }
            Ev::CompDrain { .. } => out.push(Ev::CompDrain { p: rs }),
            Ev::Gc { .. } => out.push(Ev::Gc { p: rs }),
            Ev::DropThrough { .. } => out.push(Ev::DropThrough { p: rs }),
            Ev::CompStop { .. } => out.push(Ev::RStop { p: rs }),
            Ev::Write { p, .. } => {
                out.push(e);
                if Some(p) == owner && r.gen_bool(0.3) {
                    out.push(Ev::Read { p });
                }
            }
            other => out.push(other),
        }
        // the late remote publish, once per seed at most, after the first compactor start
        if !late_done && out.iter().any(|e| matches!(e, Ev::RStart { .. })) && r.gen_bool(0.06) {
            late_done = true;
            let o = owner.unwrap_or(0);
            let q = (rs + 1 + r.gen_range(0..SLOTS - 1)) % SLOTS;
            let (a, b) = (rid, rid + 1);
            rid += 2;
            let land = r.gen_bool(0.7);
            // the remote's claim renewals (fx/compactor) and its next version create are held
            out.push(Ev::Hold { p: rs, class: Class::FxOther, op: Some(NOp::Put), skip: 0, land, all: true, rid: a });
            out.push(Ev::Write { p: o, ops: vec![(r.gen_range(0..CUSTOMERS), r.gen_range(0..PER_CUSTOMER), false), (r.gen_range(0..CUSTOMERS), r.gen_range(0..PER_CUSTOMER), false)] });
            out.push(Ev::Flush { p: o });
            out.push(Ev::Hold { p: rs, class: Class::FxVersion, op: Some(NOp::Put), skip: 0, land: r.gen_bool(0.7), all: false, rid: b });
            out.push(Ev::CompDrain { p: rs });
            out.push(Ev::Sleep { ms: CLAIM_LEASE_MS + CLAIM_MARGIN_MS + 400 });
            out.push(Ev::RStart { p: q });
            for _ in 0..r.gen_range(2..5) {
                out.push(match r.gen_range(0..4) {
                    0 | 1 => Ev::Write { p: o, ops: vec![(r.gen_range(0..CUSTOMERS), r.gen_range(0..PER_CUSTOMER), r.gen_bool(0.2))] },
                    2 => Ev::Flush { p: o },
                    _ => Ev::CompDrain { p: q },
                });
            }
            out.push(Ev::Read { p: o });
            out.push(Ev::Release { rid: b });
            out.push(Ev::Release { rid: a });
            out.push(Ev::Gc { p: q });
            out.push(Ev::Read { p: o });
        }
    }
    Script { events: out.into_iter().enumerate().map(|(i, e)| (i as u32, e)).collect(), ..base }
}

/// Start a remote compactor on slot `p` with `claim`, and its supervisor (renewals, restarts).
async fn start_on(id: String, store: Arc<dyn ObjectStore>, compl: Arc<Mutex<Option<Arc<Compactor>>>>, rcl: Arc<Mutex<Option<Arc<Claim>>>>, claim: Arc<Claim>, kp: u8) -> anyhow::Result<u64> {
    let at = crate::lifecycle::DbAt::new(&id, crate::lifecycle::read_incarnation(&store, &id).await?);
    let c = Compactor::start_remote(Arc::clone(&store), at.clone(), knobs_for(kp), pool::Pool::new(2), tokio::runtime::Handle::current(), Arc::clone(&claim), Duration::from_millis(30)).await?;
    c.run();
    let v = c.current().await.version;
    *compl.lock().unwrap() = Some(Arc::clone(&c));
    *rcl.lock().unwrap() = Some(Arc::clone(&claim));
    tokio::spawn(async move {
        let mut c = c;
        loop {
            tokio::time::sleep(claim.lease / 6).await;
            // the slot's compactor is no longer this one (a kill or an `RStop` took it): the supervisor ends
            if !compl.lock().unwrap().as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) {
                return;
            }
            let _ = claim.renew().await;
            if claim.lost() || !claim.valid() {
                let mine = { let mut g = compl.lock().unwrap(); if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) { g.take() } else { None } };
                if let Some(c) = mine {
                    c.stop().await;
                }
                return;
            }
            if c.stopped.load(SeqCst) {
                c.stop().await;
                match Compactor::start_remote(Arc::clone(&store), at.clone(), knobs_for(kp), pool::Pool::new(2), tokio::runtime::Handle::current(), Arc::clone(&claim), Duration::from_millis(30)).await {
                    Ok(n) => {
                        n.run();
                        let mut g = compl.lock().unwrap();
                        if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) {
                            *g = Some(Arc::clone(&n));
                            c = n;
                        } else {
                            drop(g);
                            let n2 = n;
                            tokio::spawn(async move { n2.stop().await });
                            return;
                        }
                    }
                    Err(_) => {
                        // tried again at the next round, the old one stays listed as stopped
                    }
                }
            }
        }
    });
    Ok(v)
}

impl World {
    pub(super) fn remote_op(&mut self, eid: u32, e: Ev) -> futures::future::BoxFuture<'static, String> {
        let p = e.slot().unwrap();
        let id = self.id.clone();
        let s = &self.slots[p];
        let (store, tl, compl, rcl, life) = (Arc::clone(&s.store), Arc::clone(&s.t), Arc::clone(&s.comp), Arc::clone(&s.rc), s.life);
        let model = Arc::clone(&self.model);
        let found = Arc::clone(&self.x.found);
        let kp = self.knobs;
        Box::pin(async move {
            match e {
                Ev::RStart { .. } => {
                    if compl.lock().unwrap().is_some() {
                        return "a compactor already runs in this process (skipped)".into();
                    }
                    let at = match crate::lifecycle::read_incarnation(&store, &id).await {
                        Ok(inc) => crate::lifecycle::DbAt::new(&id, inc),
                        Err(e) => return format!("incarnation unreadable: {}", short(&e)),
                    };
                    match manifest::load_newest(&store, &at).await {
                        Ok(Some(_)) => {}
                        Ok(None) => return "no version yet (skipped)".into(),
                        Err(e) => return format!("versions unreadable: {}", short(&e)),
                    }
                    let holder = format!("slot{p}-life{life}");
                    let claim = match Claim::acquire(&store, &at, &holder, Duration::from_millis(CLAIM_LEASE_MS), Duration::from_millis(CLAIM_MARGIN_MS), Duration::from_millis(CLAIM_WAIT_MS)).await {
                        Ok(c) => c,
                        Err(e) => {
                            CLAIMS_REFUSED.fetch_add(1, SeqCst);
                            return format!("claim not taken: {}", short(&e));
                        }
                    };
                    let n = claim.doc().claim;
                    match start_on(id, store, compl, rcl, Arc::clone(&claim), kp).await {
                        Ok(v) => {
                            STARTS.fetch_add(1, SeqCst);
                            format!("remote compactor started (claim {n}, version {v})")
                        }
                        Err(e) => {
                            claim.release().await;
                            format!("remote start failed (claim {n} released): {}", short(&e))
                        }
                    }
                }
                Ev::RStop { .. } => {
                    let c = compl.lock().unwrap().take();
                    let cl = rcl.lock().unwrap().take();
                    let Some(c) = c else { return "no compactor (skipped)".into() };
                    c.stop().await;
                    if let Some(cl) = cl {
                        cl.release().await;
                    }
                    "remote compactor stopped, claim released".into()
                }
                Ev::Defer { .. } => {
                    // as `Entry::maybe_start_fxc` with `--sealed-remote-compactor`
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if t.fx_remote() {
                        return "already deferring".into();
                    }
                    if !t.is_writer() || t.fx_view().is_none() || t.fx_first_running() {
                        return "no writer, no version, or a first build running (skipped)".into();
                    }
                    if !t.fx_claim() {
                        return "claim of this process refused".into();
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
                                    return "catch-up published nothing".into();
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
                    if t.fx_set_remote() {
                        DEFERS.fetch_add(1, SeqCst);
                        format!("writer defers to the remote compactor {caught}")
                    } else {
                        t.fx_unclaim();
                        format!("defer refused {caught}")
                    }
                }
                Ev::Read { .. } => {
                    let Ok(mut g) = tl.try_lock() else { return "busy (skipped)".into() };
                    let Some(t) = g.as_mut() else { return "not open (skipped)".into() };
                    if !t.fx_remote() {
                        return "not deferring (skipped)".into();
                    }
                    // as the server before a command: renew a due state under the exclusive lock (`Entry::ensure_open`)
                    if t.fx_refresh_due() && !crate::fx::remote::CONTROL_WRITER_NO_T_PIN.load(SeqCst) {
                        if let Err(e) = t.fx_refresh().await {
                            return format!("refresh failed: {}", short(&e));
                        }
                    }
                    let snap = model.lock().unwrap().snapshot();
                    let reads = read_all(t).await;
                    let refused = reads.values().filter(|r| r.is_err()).count();
                    let ok: BTreeMap<Vec<u8>, Result<Option<Vec<u8>>, String>> = reads.into_iter().filter(|(_, r)| r.is_ok()).collect();
                    let v = check_reads(&format!("event {eid}: the deferring writer (slot {p}) reads"), &ok, &snap);
                    let n = v.len();
                    READS.fetch_add(1, SeqCst);
                    READ_KEYS.fetch_add(ok.len() as u64, SeqCst);
                    READS_REFUSED.fetch_add(refused as u64, SeqCst);
                    found.lock().unwrap().extend(v);
                    format!("read {} keys ({refused} refused), {n} wrong", ok.len())
                }
                _ => unreachable!(),
            }
        })
    }
}

/// Process-wide settings of a remote batch (`SIM_REMOTE=1`): the waiters defer (`WAITER_DEFERS`); restored on drop.
pub struct RemoteGuard(bool);

impl RemoteGuard {
    pub fn set() -> RemoteGuard {
        let old = remote::WAITER_DEFERS.swap(on(), SeqCst);
        RemoteGuard(old)
    }
}

impl Drop for RemoteGuard {
    fn drop(&mut self) {
        remote::WAITER_DEFERS.store(self.0, SeqCst);
    }
}
