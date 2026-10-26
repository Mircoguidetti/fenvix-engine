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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

struct Held {
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
    /// No leases at all (single process, for object stores without conditional update such as the local-directory store).
    pub disabled: std::sync::atomic::AtomicBool,
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn body(holder: &str, expires_ms: u64, seq: u64) -> Bytes {
    let d = doc! { "holder": holder, "expiresMs": expires_ms as i64, "seq": seq as i64 };
    Bytes::from(d.to_vec().unwrap())
}

fn parse(b: &[u8]) -> Result<(String, u64, u64)> {
    let d = Document::from_reader(b)?;
    Ok((d.get_str("holder")?.to_string(), d.get_i64("expiresMs")? as u64, d.get_i64("seq").unwrap_or(0) as u64))
}

/// Log line parsed by the S4 checker: `LEASE <event> tenant=<t> holder=<h> unix_ms=<ms> ...`.
pub fn log(event: &str, tenant: &str, holder: &str, extra: &str) {
    eprintln!("LEASE {event} tenant={tenant} holder={holder} unix_ms={} {extra}", now_ms());
}

impl Leases {
    pub fn new(store: Arc<dyn ObjectStore>, holder: String, ttl: Duration) -> Arc<Self> {
        Arc::new(Self { store, holder, ttl, skew: Duration::from_millis(1500), held: Mutex::new(HashMap::new()), denied: Mutex::new(HashMap::new()), stats: Mutex::new(LeaseStats::default()), unsafe_skip: std::sync::atomic::AtomicBool::new(false), disabled: std::sync::atomic::AtomicBool::new(false) })
    }

    pub fn held_count(&self) -> usize {
        self.held.lock().unwrap().len()
    }

    /// True while this process may act as the tenant's owner (it holds the lease and has renewed it recently enough).
    pub fn valid(&self, id: &str) -> bool {
        if self.unsafe_skip.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
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
                    let (holder, exp, seq) = parse(&bytes)?;
                    let now = now_ms();
                    let mine = holder == self.holder;
                    let free = holder.is_empty() || exp == 0;
                    let expired = (exp + self.skew.as_millis() as u64) < now;
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

    fn record(&self, id: &str, etag: Option<String>, version: Option<String>, started: Instant, seq: u64) {
        self.denied.lock().unwrap().remove(id);
        self.held.lock().unwrap().insert(id.to_string(), Held { etag, version, valid_until: started + self.ttl, renew_at: started + self.ttl / 3, seq });
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
