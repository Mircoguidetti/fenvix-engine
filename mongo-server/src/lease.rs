//! Single ownership of a tenant across server processes.
//!
//! `tenants/{id}/_lease` holds `{holder, expiresMs, seq}`. It is created with `PutMode::Create`
//! (S3 `If-None-Match: *`) and replaced with `PutMode::Update` (S3 `If-Match: <etag>`), the only two
//! conditional modes the S3 implementation of object_store 0.14.2 supports (`S3ConditionalPut::ETagMatch`;
//! there is no conditional delete, so a lease is released by a conditional update that expires it).
//!
//! A process may open a tenant, as reader or writer, only while it holds the tenant's lease. The lease
//! is renewed in the background while the tenant is open. A process that does not hold it gets
//! [`LeaseHeld`], which the server turns into a retryable MongoDB error. When a holder crashes its
//! lease expires; the next process takes it over by conditional update and, because a writer deletes
//! the `_clean` marker when it opens, takes the unclean (WAL replay) path.

use anyhow::{anyhow, Result};
use bson::{doc, Document};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The clock of every lease deadline: CLOCK_BOOTTIME, which (unlike the monotonic clock of `std::time::Instant`) keeps
/// counting while the machine or VM is suspended. A process resumed after a suspend longer than the ttl then sees its
/// leases as lapsed at once, instead of serving as owner while another process has already taken its databases over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(Duration);

impl Instant {
    pub fn now() -> Self {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: clock_gettime only writes the timespec it is given.
        let r = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
        assert_eq!(r, 0, "clock_gettime(CLOCK_BOOTTIME) failed");
        Instant(Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32))
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, d: Duration) -> Instant {
        Instant(self.0 + d)
    }
}

/// The tenant is owned by another process (or this process lost its lease).
#[derive(Debug)]
pub struct LeaseHeld {
    pub tenant: String,
    pub holder: String,
    pub remaining_ms: u64,
}

impl std::fmt::Display for LeaseHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tenant {} is owned by server process {} (lease valid for another {} ms); retry", self.tenant, self.holder, self.remaining_ms)
    }
}

impl std::error::Error for LeaseHeld {}

/// This process's own lease in per-process mode (`processes/{holder}/_lease`). One object for all the tenants the
/// process owns: renewing it keeps every one of them valid, so an idle open tenant costs no request.
struct ProcLease {
    /// Unique id of this lease generation; written into every owner record this process creates.
    inc: String,
    /// Bumped whenever the lease lapsed before it was renewed. Owner records and held tenants of an older
    /// generation are void: another process may have taken those tenants over while this one was frozen.
    gen: u64,
    etag: Option<String>,
    version: Option<String>,
    seq: u64,
    valid_until: Instant,
    renew_at: Instant,
}

struct Held {
    /// Process-lease generation this tenant was acquired under (per-process mode only).
    gen: u64,
    etag: Option<String>,
    version: Option<String>,
    /// Local monotonic time until which this process may still act as the owner.
    valid_until: Instant,
    /// When this lease is next renewed. Leases are renewed individually, a third of a ttl after they were
    /// last written, so the renewals of thousands of tenants spread out instead of arriving in one burst.
    renew_at: Instant,
    seq: u64,
}

pub struct Leases {
    store: Arc<dyn ObjectStore>,
    pub holder: String,
    pub ttl: Duration,
    /// Extra time after expiry before a lease may be taken over (clock skew allowance).
    skew: Duration,
    held: Mutex<HashMap<String, Held>>,
    /// Tenants another process was just seen to own: refused without asking the store again for a moment.
    denied: Mutex<HashMap<String, (Instant, String, u64)>>,
    pub stats: Mutex<LeaseStats>,
    /// Test control only: never treat a lease as expired or lost (`--unsafe-skip-ownership-checks`).
    pub unsafe_skip: std::sync::atomic::AtomicBool,
    /// Per-process lease mode (`--process-lease`): see [`ProcLease`]. All processes sharing a bucket must use the same mode.
    pub process_mode: std::sync::atomic::AtomicBool,
    proc: Mutex<Option<ProcLease>>,
    proc_init: tokio::sync::Mutex<()>,
    /// Source of process-lease generation numbers (never reused within this process).
    proc_gen_floor: std::sync::atomic::AtomicU64,
    /// No leases at all (single process, for object stores without conditional update such as the local-directory store).
    pub disabled: std::sync::atomic::AtomicBool,
    /// A renewal of the process lease is in flight: no second one is started (two concurrent renewals with the same etag
    /// made the slower one fail its condition and the process drop every database it owned, whenever S3 was slow).
    proc_renewing: std::sync::atomic::AtomicBool,
}

#[derive(Default, Debug, Clone)]
pub struct LeaseStats {
    pub acquired: u64,
    pub takeovers: u64,
    pub refused: u64,
    pub renewals: u64,
    pub renew_failures: u64,
    pub lost: u64,
}

pub fn lease_path(id: &str) -> Path {
    Path::from(format!("tenants/{id}/_lease"))
}

fn now_ms() -> u64 {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    (ms + wall_offset_ms()) as u64
}

/// Test only: `MONGO_TEST_WALL_CLOCK_OFFSET_MS` shifts this process's wall clock as seen by the leases, to reproduce clock
/// skew between machines. Never set in production.
fn wall_offset_ms() -> i64 {
    static OFF: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *OFF.get_or_init(|| std::env::var("MONGO_TEST_WALL_CLOCK_OFFSET_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
}

fn body(holder: &str, expires_ms: u64, seq: u64) -> Bytes {
    body_inc(holder, expires_ms, seq, "")
}

/// `inc` (per-process mode) is the process-lease generation an owner record belongs to, or the generation of a
/// process lease itself; absent in per-tenant records.
fn body_inc(holder: &str, expires_ms: u64, seq: u64, inc: &str) -> Bytes {
    let mut d = doc! { "holder": holder, "expiresMs": expires_ms as i64, "seq": seq as i64 };
    if !inc.is_empty() {
        d.insert("inc", inc);
    }
    Bytes::from(d.to_vec().unwrap())
}

fn parse(b: &[u8]) -> Result<(String, u64, u64)> {
    parse_inc(b).map(|(h, e, s, _)| (h, e, s))
}

fn parse_inc(b: &[u8]) -> Result<(String, u64, u64, String)> {
    let d = Document::from_reader(b)?;
    Ok((d.get_str("holder")?.to_string(), d.get_i64("expiresMs")? as u64, d.get_i64("seq").unwrap_or(0) as u64, d.get_str("inc").unwrap_or("").to_string()))
}

/// Expiry written into an owner record in per-process mode: the record itself never expires, the owner's process
/// lease decides. A per-tenant-mode process reading it refuses the tenant, which is the safe direction.
const OWNER_NEVER_EXPIRES_MS: u64 = u64::MAX / 4;

pub fn process_path(holder: &str) -> Path {
    Path::from(format!("processes/{holder}/_lease"))
}

fn fresh_inc() -> String {
    format!("{:016x}", rand::random::<u64>())
}

fn far() -> Instant {
    Instant::now() + Duration::from_secs(86400 * 365)
}

/// Log line parsed by the S4 checker: `LEASE <event> tenant=<t> holder=<h> unix_ms=<ms> ...`.
pub fn log(event: &str, tenant: &str, holder: &str, extra: &str) {
    eprintln!("LEASE {event} tenant={tenant} holder={holder} unix_ms={} {extra}", now_ms());
}

impl Leases {
    pub fn new(store: Arc<dyn ObjectStore>, holder: String, ttl: Duration) -> Arc<Self> {
        Arc::new(Self { store, holder, ttl, skew: Duration::from_millis(1500), held: Mutex::new(HashMap::new()), denied: Mutex::new(HashMap::new()), stats: Mutex::new(LeaseStats::default()), unsafe_skip: std::sync::atomic::AtomicBool::new(false), process_mode: std::sync::atomic::AtomicBool::new(false), proc: Mutex::new(None), proc_init: tokio::sync::Mutex::new(()), proc_gen_floor: std::sync::atomic::AtomicU64::new(0), disabled: std::sync::atomic::AtomicBool::new(false), proc_renewing: std::sync::atomic::AtomicBool::new(false) })
    }

    pub fn held_count(&self) -> usize {
        self.held.lock().unwrap().len()
    }

    /// True while this process may act as the tenant's owner (it holds the lease and has renewed it recently enough).
    pub fn valid(&self, id: &str) -> bool {
        if self.unsafe_skip.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        if self.process_mode.load(std::sync::atomic::Ordering::Relaxed) {
            // Owned while this process's lease is valid and still of the generation the tenant was acquired under.
            // Lock order: proc, then held (as in renew_process).
            let p = self.proc.lock().unwrap();
            let held = self.held.lock().unwrap();
            return match (held.get(id), p.as_ref()) {
                (Some(h), Some(p)) => h.gen == p.gen && Instant::now() + self.ttl / 6 < p.valid_until,
                _ => false,
            };
        }
        self.held.lock().unwrap().get(id).map_or(false, |h| Instant::now() + self.ttl / 6 < h.valid_until)
    }

    /// Fails with a retryable error if this process no longer owns `id` (checked after every read).
    pub fn check_owner(&self, id: &str) -> Result<()> {
        if self.valid(id) {
            return Ok(());
        }
        self.stats.lock().unwrap().refused += 1;
        Err(LeaseHeld { tenant: id.to_string(), holder: "(ownership lost during the request)".into(), remaining_ms: 0 }.into())
    }

    pub fn is_held(&self, id: &str) -> bool {
        self.held.lock().unwrap().contains_key(id)
    }

    /// Take (or renew) the lease. Returns the previous holder when an expired lease was taken over.
    pub async fn acquire(&self, id: &str) -> Result<Option<String>> {
        if self.disabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.record(id, None, None, Instant::now() + Duration::from_secs(86400 * 365), 0);
            return Ok(None);
        }
        let path = lease_path(id);
        if let Some((until, holder, remaining)) = self.denied.lock().unwrap().get(id).cloned() {
            if Instant::now() < until {
                self.stats.lock().unwrap().refused += 1;
                return Err(LeaseHeld { tenant: id.to_string(), holder, remaining_ms: remaining }.into());
            }
        }
        if self.process_mode.load(std::sync::atomic::Ordering::Relaxed) {
            return self.acquire_owner(id).await;
        }
        for _ in 0..4 {
            let started = Instant::now();
            let expires = now_ms() + self.ttl.as_millis() as u64;
            match self.store.get(&path).await {
                Err(object_store::Error::NotFound { .. }) => {
                    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
                    match self.store.put_opts(&path, PutPayload::from(body(&self.holder, expires, 1)), opts).await {
                        Ok(r) => {
                            self.record(id, r.e_tag, r.version, started, 1);
                            return Ok(None);
                        }
                        Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
                Ok(got) => {
                    let (etag, version) = (got.meta.e_tag.clone(), got.meta.version.clone());
                    let bytes = got.bytes().await?;
                    let (holder, exp, seq, rec_inc) = parse_inc(&bytes)?;
                    let now = now_ms();
                    let mine = holder == self.holder;
                    let free = holder.is_empty() || exp == 0;
                    let mut expired = (exp + self.skew.as_millis() as u64) < now;
                    if !rec_inc.is_empty() && !mine && !free && !expired {
                        // An owner record of a process in per-process mode (it never expires by itself): owned while that
                        // process's lease is live and of the record's generation. Without this, going back to
                        // `--process-lease false` after such a process crashed would refuse the database for ever.
                        expired = match self.store.get(&process_path(&holder)).await {
                            Err(object_store::Error::NotFound { .. }) => true,
                            Err(e) => return Err(e.into()),
                            Ok(pg) => {
                                let (_, pexp, _, pinc) = parse_inc(&pg.bytes().await?)?;
                                pinc != rec_inc || pexp + (self.skew.as_millis() as u64) < now
                            }
                        };
                    }
                    if !(mine || free || expired) {
                        self.stats.lock().unwrap().refused += 1;
                        let remaining = exp.saturating_sub(now);
                        self.denied.lock().unwrap().insert(id.to_string(), (Instant::now() + Duration::from_millis(500).min(Duration::from_millis(remaining)), holder.clone(), remaining));
                        return Err(LeaseHeld { tenant: id.to_string(), holder, remaining_ms: remaining }.into());
                    }
                    let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: etag, version }), ..Default::default() };
                    match self.store.put_opts(&path, PutPayload::from(body(&self.holder, expires, seq + 1)), opts).await {
                        Ok(r) => {
                            self.record(id, r.e_tag, r.version, started, seq + 1);
                            let took = (!mine && !free).then_some(holder);
                            if took.is_some() {
                                self.stats.lock().unwrap().takeovers += 1;
                            }
                            return Ok(took);
                        }
                        Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Err(anyhow!("could not settle the lease of tenant {id} (contended)"))
    }

    /// Per-process mode: make sure this process has its own lease; returns its (inc, gen).
    async fn ensure_process(&self) -> Result<(String, u64)> {
        if let Some(p) = self.proc.lock().unwrap().as_ref() {
            return Ok((p.inc.clone(), p.gen));
        }
        let _init = self.proc_init.lock().await;
        if let Some(p) = self.proc.lock().unwrap().as_ref() {
            return Ok((p.inc.clone(), p.gen));
        }
        let path = process_path(&self.holder);
        let gen = self.proc_gen_floor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        for _ in 0..4 {
            let started = Instant::now();
            let inc = fresh_inc();
            let expires = now_ms() + self.ttl.as_millis() as u64;
            let (put, seq) = match self.store.get(&path).await {
                Err(object_store::Error::NotFound { .. }) => (self.store.put_opts(&path, PutPayload::from(body_inc(&self.holder, expires, 1, &inc)), PutOptions { mode: PutMode::Create, ..Default::default() }).await, 1),
                Err(e) => return Err(e.into()),
                Ok(got) => {
                    // The same holder id is the same process slot (a restart): its old generation is dead.
                    let (etag, version) = (got.meta.e_tag.clone(), got.meta.version.clone());
                    let seq = parse(&got.bytes().await?).map(|(_, _, s)| s).unwrap_or(0) + 1;
                    let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: etag, version }), ..Default::default() };
                    (self.store.put_opts(&path, PutPayload::from(body_inc(&self.holder, expires, seq, &inc)), opts).await, seq)
                }
            };
            match put {
                Ok(r) => {
                    *self.proc.lock().unwrap() = Some(ProcLease { inc: inc.clone(), gen, etag: r.e_tag, version: r.version, seq, valid_until: started + self.ttl, renew_at: started + self.ttl / 3 });
                    self.stats.lock().unwrap().acquired += 1;
                    log("process_lease", "-", &self.holder, &format!("inc={inc} gen={gen}"));
                    return Ok((inc, gen));
                }
                Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(anyhow!("could not settle the process lease of {} (contended)", self.holder))
    }

    /// Per-process mode: take ownership of a tenant by writing its owner record (`tenants/{id}/_lease`, written once
    /// and not renewed). Another process's record stays valid while that process's own lease is live and of the
    /// generation the record names; otherwise the tenant is taken over with a conditional update.
    async fn acquire_owner(&self, id: &str) -> Result<Option<String>> {
        let (inc, gen) = self.ensure_process().await?;
        let path = lease_path(id);
        for _ in 0..4 {
            match self.store.get(&path).await {
                Err(object_store::Error::NotFound { .. }) => {
                    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
                    match self.store.put_opts(&path, PutPayload::from(body_inc(&self.holder, OWNER_NEVER_EXPIRES_MS, 1, &inc)), opts).await {
                        Ok(r) => {
                            self.record_owner(id, r.e_tag, r.version, 1, gen);
                            return Ok(None);
                        }
                        Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
                Ok(got) => {
                    let (etag, version) = (got.meta.e_tag.clone(), got.meta.version.clone());
                    let (holder, exp, seq, rec_inc) = parse_inc(&got.bytes().await?)?;
                    let mine = holder == self.holder;
                    let free = holder.is_empty() || exp == 0;
                    if !(mine || free) {
                        let now = now_ms();
                        let skew = self.skew.as_millis() as u64;
                        // When is the record's owner no longer the owner (None: it still is, and until when)?
                        let owner_gone = if rec_inc.is_empty() {
                            // A per-tenant-mode record: it expires by itself.
                            (exp + skew < now).then_some(0).ok_or(exp.saturating_sub(now))
                        } else {
                            match self.store.get(&process_path(&holder)).await {
                                Err(object_store::Error::NotFound { .. }) => Ok(0),
                                Err(e) => return Err(e.into()),
                                Ok(pg) => {
                                    let (petag, pversion) = (pg.meta.e_tag.clone(), pg.meta.version.clone());
                                    let (_, pexp, pseq, pinc) = parse_inc(&pg.bytes().await?)?;
                                    if pinc != rec_inc {
                                        Ok(0) // that process restarted since: the record's generation is dead
                                    } else if pexp + skew < now {
                                        // Revoke the old process lease first (conditional on the version just read), so a
                                        // renewal of it still in flight fails its condition and that process drops every
                                        // database it holds, instead of becoming valid again after this takeover.
                                        let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: petag, version: pversion }), ..Default::default() };
                                        let revoked = format!("revoked-by-{}-{}", self.holder, pinc);
                                        match self.store.put_opts(&process_path(&holder), PutPayload::from(body_inc(&holder, 0, pseq + 1, &revoked)), opts).await {
                                            Ok(_) => {
                                                log("process_lease_revoked", id, &self.holder, &format!("of={holder}"));
                                                Ok(0)
                                            }
                                            // it renewed (or was revoked) meanwhile: read everything again
                                            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => continue,
                                            Err(e) => return Err(e.into()),
                                        }
                                    } else {
                                        Err(pexp.saturating_sub(now))
                                    }
                                }
                            }
                        };
                        if let Err(remaining) = owner_gone {
                            self.stats.lock().unwrap().refused += 1;
                            self.denied.lock().unwrap().insert(id.to_string(), (Instant::now() + Duration::from_millis(500).min(Duration::from_millis(remaining)), holder.clone(), remaining));
                            return Err(LeaseHeld { tenant: id.to_string(), holder, remaining_ms: remaining }.into());
                        }
                    }
                    let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: etag, version }), ..Default::default() };
                    match self.store.put_opts(&path, PutPayload::from(body_inc(&self.holder, OWNER_NEVER_EXPIRES_MS, seq + 1, &inc)), opts).await {
                        Ok(r) => {
                            self.record_owner(id, r.e_tag, r.version, seq + 1, gen);
                            let took = (!mine && !free).then_some(holder);
                            if took.is_some() {
                                self.stats.lock().unwrap().takeovers += 1;
                            }
                            return Ok(took);
                        }
                        Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Err(anyhow!("could not settle the owner record of tenant {id} (contended)"))
    }

    fn record_owner(&self, id: &str, etag: Option<String>, version: Option<String>, seq: u64, gen: u64) {
        self.denied.lock().unwrap().remove(id);
        self.held.lock().unwrap().insert(id.to_string(), Held { gen, etag, version, valid_until: far(), renew_at: far(), seq });
        self.stats.lock().unwrap().acquired += 1;
    }

    /// Per-process mode: renew the process lease. If it had already lapsed (this process was frozen or starved for
    /// longer than the ttl) another process may have taken tenants over, so every tenant held under the old
    /// generation is abandoned first and the lease only becomes valid again as a new generation.
    async fn renew_process(&self, lost: &tokio::sync::mpsc::UnboundedSender<String>) {
        let unsafe_skip = self.unsafe_skip.load(std::sync::atomic::Ordering::Relaxed);
        let started = Instant::now();
        let (etag, version, seq, inc, prev_valid, lapsed_at_start) = {
            let mut guard = self.proc.lock().unwrap();
            let Some(p) = guard.as_mut() else { return };
            let prev_valid = p.valid_until;
            let lapsed_at_start = started > p.valid_until && !unsafe_skip;
            if lapsed_at_start {
                p.gen = self.proc_gen_floor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                p.inc = fresh_inc();
                let ids: Vec<String> = self.held.lock().unwrap().drain().map(|(k, _)| k).collect();
                log("process_lease_lapsed", "-", &self.holder, &format!("gen={} abandoned={}", p.gen, ids.len()));
                self.stats.lock().unwrap().lost += ids.len() as u64;
                for id in ids {
                    log("lost", &id, &self.holder, "process_lease_lapsed");
                    let _ = lost.send(id);
                }
            }
            (p.etag.clone(), p.version.clone(), p.seq, p.inc.clone(), prev_valid, lapsed_at_start)
        };
        let expires = now_ms() + self.ttl.as_millis() as u64;
        let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: etag, version }), ..Default::default() };
        match self.store.put_opts(&process_path(&self.holder), PutPayload::from(body_inc(&self.holder, expires, seq + 1, &inc)), opts).await {
            Ok(r) => {
                let completed = Instant::now();
                // The PUT landed after the previous deadline: for a while the stored lease was expired, and another
                // process may have taken databases over in between (the PUT itself carried no proof it had not). Every
                // database of this generation is dropped, and the next renewal starts a new generation.
                let late = !lapsed_at_start && !unsafe_skip && completed > prev_valid;
                let mut dropped = Vec::new();
                if let Some(p) = self.proc.lock().unwrap().as_mut() {
                    p.etag = r.e_tag;
                    p.version = r.version;
                    p.seq = seq + 1;
                    if late {
                        p.valid_until = completed;
                        p.renew_at = completed;
                        dropped = self.held.lock().unwrap().drain().map(|(k, _)| k).collect();
                    } else {
                        p.valid_until = started + self.ttl;
                        p.renew_at = started + self.ttl / 3;
                    }
                }
                if late {
                    log("process_lease_late", "-", &self.holder, &format!("abandoned={}", dropped.len()));
                    self.stats.lock().unwrap().lost += dropped.len() as u64;
                    for id in dropped {
                        log("lost", &id, &self.holder, "process_lease_late");
                        let _ = lost.send(id);
                    }
                }
                self.stats.lock().unwrap().renewals += 1;
            }
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) if !unsafe_skip => {
                // Someone else wrote our process lease (the same holder id started twice): we are not the owner of
                // anything. Abandon everything and start over with a new lease.
                let ids: Vec<String> = self.held.lock().unwrap().drain().map(|(k, _)| k).collect();
                *self.proc.lock().unwrap() = None;
                self.stats.lock().unwrap().lost += ids.len() as u64;
                log("process_lease_lost", "-", &self.holder, &format!("abandoned={}", ids.len()));
                for id in ids {
                    log("lost", &id, &self.holder, "process_lease_lost");
                    let _ = lost.send(id);
                }
            }
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => {
                self.stats.lock().unwrap().renew_failures += 1;
                eprintln!("process lease renewal failed: {e}");
            }
        }
    }

    /// Clean shutdown in per-process mode: expire the process lease so nothing waits for it.
    pub async fn release_process(&self) {
        let Some(p) = self.proc.lock().unwrap().take() else { return };
        let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: p.etag, version: p.version }), ..Default::default() };
        let _ = self.store.put_opts(&process_path(&self.holder), PutPayload::from(body_inc(&self.holder, 0, p.seq + 1, &p.inc)), opts).await;
    }

    fn record(&self, id: &str, etag: Option<String>, version: Option<String>, started: Instant, seq: u64) {
        self.denied.lock().unwrap().remove(id);
        self.held.lock().unwrap().insert(id.to_string(), Held { gen: 0, etag, version, valid_until: started + self.ttl, renew_at: started + self.ttl / 3, seq });
        self.stats.lock().unwrap().acquired += 1;
    }

    /// Release at clean close: expire the lease with a conditional update (there is no conditional delete).
    /// If the update loses a race with an in-flight renewal (the etag moved), read the lease again and, while
    /// it is still ours, retry: a renewal that landed after the release would otherwise keep the tenant
    /// locked for a whole ttl.
    pub async fn release(&self, id: &str) {
        let Some(h) = self.held.lock().unwrap().remove(id) else { return };
        if self.disabled.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Logged before the write: after this line the process no longer acts as the owner.
        log("release_begin", id, &self.holder, "");
        let path = lease_path(id);
        let (mut etag, mut version, mut seq) = (h.etag, h.version, h.seq);
        let mut ok = false;
        for _ in 0..4 {
            let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: etag.clone(), version: version.clone() }), ..Default::default() };
            match self.store.put_opts(&path, PutPayload::from(body("", 0, seq + 1)), opts).await {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(object_store::Error::Precondition { .. }) => match self.store.get(&path).await {
                    Ok(got) => {
                        etag = got.meta.e_tag.clone();
                        version = got.meta.version.clone();
                        match got.bytes().await.ok().and_then(|b| parse(&b).ok()) {
                            Some((holder, exp, s)) if holder == self.holder && exp > 0 => seq = s,
                            _ => break, // someone else owns it now, or it is already free
                        }
                    }
                    Err(_) => break,
                },
                Err(_) => break,
            }
        }
        log("release", id, &self.holder, &format!("ok={ok}"));
    }

    /// Forget a lease without touching storage (the tenant never existed, or ownership was lost).
    pub fn forget(&self, id: &str) {
        self.held.lock().unwrap().remove(id);
    }

    /// Delete the lease object of a tenant that turned out not to exist.
    pub async fn discard(&self, id: &str) {
        self.forget(id);
        if self.disabled.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let _ = self.store.delete(&lease_path(id)).await;
    }

    /// Renew each held lease `ttl / 3` after it was last written. Returns (through `lost`) the tenants whose lease
    /// could not be renewed because another process took them over, so the server can abandon them.
    pub fn spawn_renewer(self: &Arc<Self>, lost: tokio::sync::mpsc::UnboundedSender<String>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let sem = Arc::new(tokio::sync::Semaphore::new(32));
            let mut tick = tokio::time::interval(Duration::from_millis(200));
            loop {
                tick.tick().await;
                let now = Instant::now();
                if me.process_mode.load(std::sync::atomic::Ordering::Relaxed) {
                    if me.proc_renewing.load(std::sync::atomic::Ordering::Acquire) {
                        continue;
                    }
                    let due = me.proc.lock().unwrap().as_mut().map_or(false, |p| {
                        let due = p.renew_at <= now;
                        if due {
                            // Pushed out so a slow renewal is not started twice; success sets the real next time.
                            p.renew_at = now + me.ttl / 12;
                        }
                        due
                    });
                    if due {
                        me.proc_renewing.store(true, std::sync::atomic::Ordering::Release);
                        let (me, lost) = (Arc::clone(&me), lost.clone());
                        tokio::spawn(async move {
                            me.renew_process(&lost).await;
                            me.proc_renewing.store(false, std::sync::atomic::Ordering::Release);
                        });
                    }
                    continue;
                }
                let due: Vec<String> = {
                    let mut held = me.held.lock().unwrap();
                    let mut due = Vec::new();
                    for (id, h) in held.iter_mut() {
                        if h.renew_at <= now {
                            // Pushed out so a slow renewal is not started twice; success sets the real next time.
                            h.renew_at = now + me.ttl / 12;
                            due.push(id.clone());
                        }
                    }
                    due
                };
                for id in due {
                    let me = Arc::clone(&me);
                    let lost = lost.clone();
                    let Ok(permit) = Arc::clone(&sem).acquire_owned().await else { break };
                    tokio::spawn(async move {
                        let _p = permit;
                        me.renew_one(&id, &lost).await;
                    });
                }
            }
        });
    }

    async fn renew_one(&self, id: &str, lost: &tokio::sync::mpsc::UnboundedSender<String>) {
        let (etag, version, seq) = match self.held.lock().unwrap().get(id) {
            Some(h) => (h.etag.clone(), h.version.clone(), h.seq),
            None => return,
        };
        let started = Instant::now();
        let expires = now_ms() + self.ttl.as_millis() as u64;
        let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: etag, version }), ..Default::default() };
        match self.store.put_opts(&lease_path(id), PutPayload::from(body(&self.holder, expires, seq + 1)), opts).await {
            Ok(r) => {
                if let Some(h) = self.held.lock().unwrap().get_mut(id) {
                    h.etag = r.e_tag;
                    h.version = r.version;
                    h.valid_until = started + self.ttl;
                    h.renew_at = started + self.ttl / 3;
                    h.seq = seq + 1;
                }
                self.stats.lock().unwrap().renewals += 1;
            }
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) if self.unsafe_skip.load(std::sync::atomic::Ordering::Relaxed) => {}
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => {
                // The etag moved. That is a lost lease only if someone else now holds it; our own release or
                // re-acquire racing with this renewal also moves it.
                let still_mine = match self.store.get(&lease_path(id)).await {
                    Ok(got) => {
                        let (e, v) = (got.meta.e_tag.clone(), got.meta.version.clone());
                        match got.bytes().await.ok().and_then(|b| parse(&b).ok()) {
                            Some((holder, exp, s)) if holder == self.holder && exp > 0 => Some((e, v, s)),
                            _ => None,
                        }
                    }
                    Err(_) => None,
                };
                let mut held = self.held.lock().unwrap();
                match (held.get_mut(id), still_mine) {
                    (Some(h), Some((e, v, s))) => {
                        // Adopt the lease as it stands; the next tick renews it with the right etag.
                        h.etag = e;
                        h.version = v;
                        h.seq = s;
                    }
                    (Some(_), None) => {
                        held.remove(id);
                        drop(held);
                        self.stats.lock().unwrap().lost += 1;
                        log("lost", id, &self.holder, "");
                        let _ = lost.send(id.to_string());
                    }
                    (None, _) => {} // released meanwhile
                }
            }
            Err(e) => {
                // Transient store error: keep the lease, it simply stops being renewed (and becomes invalid
                // locally once `valid_until` passes).
                self.stats.lock().unwrap().renew_failures += 1;
                eprintln!("lease renewal of {id} failed: {e}");
            }
        }
    }
}
