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
        Arc::new(Self { store, holder, ttl, skew: Duration::from_millis(1500), held: Mutex::new(HashMap::new()), denied: Mutex::new(HashMap::new()), stats: Mutex::new(LeaseStats::default()) })
    }

    pub fn held_count(&self) -> usize {
        self.held.lock().unwrap().len()
    }

    /// True while this process may act as the tenant's owner (it holds the lease and has renewed it recently enough).
    pub fn valid(&self, id: &str) -> bool {
        self.held.lock().unwrap().get(id).map_or(false, |h| Instant::now() + self.ttl / 6 < h.valid_until)
    }

    pub fn is_held(&self, id: &str) -> bool {
        self.held.lock().unwrap().contains_key(id)
    }

    /// Take (or renew) the lease. Returns the previous holder when an expired lease was taken over.
    pub async fn acquire(&self, id: &str) -> Result<Option<String>> {
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
        self.held.lock().unwrap().insert(id.to_string(), Held { etag, version, valid_until: started + self.ttl, seq });
        self.stats.lock().unwrap().acquired += 1;
    }

    /// Release at clean close: expire the lease with a conditional update (there is no conditional delete).
    pub async fn release(&self, id: &str) {
        let Some(h) = self.held.lock().unwrap().remove(id) else { return };
        // Logged before the write: after this line the process no longer acts as the owner.
        log("release_begin", id, &self.holder, "");
        let opts = PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: h.etag, version: h.version }), ..Default::default() };
        let r = self.store.put_opts(&lease_path(id), PutPayload::from(body("", 0, h.seq + 1)), opts).await;
        log("release", id, &self.holder, &format!("ok={}", r.is_ok()));
    }

    /// Forget a lease without touching storage (the tenant never existed, or ownership was lost).
    pub fn forget(&self, id: &str) {
        self.held.lock().unwrap().remove(id);
    }

    /// Delete the lease object of a tenant that turned out not to exist.
    pub async fn discard(&self, id: &str) {
        self.forget(id);
        let _ = self.store.delete(&lease_path(id)).await;
    }

    /// Renew every held lease every `ttl / 3`. Returns the tenants whose lease could not be renewed
    /// (taken over by another process) so the server can abandon them.
    pub fn spawn_renewer(self: &Arc<Self>, lost: tokio::sync::mpsc::UnboundedSender<String>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.ttl / 3);
            tick.tick().await;
            loop {
                tick.tick().await;
                let ids: Vec<String> = me.held.lock().unwrap().keys().cloned().collect();
                let sem = Arc::new(tokio::sync::Semaphore::new(32));
                let mut tasks = Vec::new();
                for id in ids {
                    let me = Arc::clone(&me);
                    let lost = lost.clone();
                    let permit = Arc::clone(&sem).acquire_owned().await.unwrap();
                    tasks.push(tokio::spawn(async move {
                        let _p = permit;
                        me.renew_one(&id, &lost).await;
                    }));
                }
                for t in tasks {
                    let _ = t.await;
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
                    h.seq = seq + 1;
                }
                self.stats.lock().unwrap().renewals += 1;
            }
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::NotFound { .. }) => {
                self.held.lock().unwrap().remove(id);
                self.stats.lock().unwrap().lost += 1;
                log("lost", id, &self.holder, "");
                let _ = lost.send(id.to_string());
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
