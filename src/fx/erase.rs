//! Erasure (compaction 2, step 7b; plan round 2, fix 5; DECISIONS 2026-11-07 "Ordinary deletes and whole-customer
//! deletes").
//!
//! Two kinds of delete:
//! - an ordinary delete (deleteOne, a deleteMany that is not a whole customer, an update): its older rows leave the
//!   delta runs at the next merge of the tier that holds them and the sealed run at the group's next reseal, and the
//!   restore points after 30 days. Nothing here runs for it.
//! - an erasure: a command that deletes all of one customer's data in a customer-keyed collection (a deleteMany whose
//!   filter is exactly the customer field, `mongo-server/src/cmd_data.rs`), or a whole database (`ctl/erase.rs`, which
//!   deletes every object of the database at once, `fx/` included). For a customer, the rows leave our live runs, L0
//!   and SlateDB's sorted runs within the deadline ([`DEADLINE_MS`], 24 hours), and the restore points after 30 days.
//!
//! A customer erasure is recorded when its deletes commit, under the database's exclusive lock, in
//! `{db}/fx/erasures/{id}.json`, with its cut S_e (the database's last seq after the deletes) and its targets in
//! `{id}.keys`: the physical key range of the customer in that collection (documents and co-located index entries,
//! `u/{customer}{collection}/`) and every other key the deletes removed (the locators, the index entries outside the
//! customer's range, the natural-order entries). The targets hold the customer's own values (its id, indexed fields), so
//! that object is deleted as soon as the runs are scrubbed. A row is erased when its key is a target and its seq is at or
//! below S_e: a document written again after the erasure (seq above S_e) is new data and stays.
//!
//! The pass ([`crate::lifecycle::Tenant::fx_erase_pass`], under the exclusive lock of the writer):
//! 1. our runs must hold every row at or below S_e: if X_d < S_e, a version is published first (step 3's builder now;
//!    step 4's absorption later);
//! 2. scrub: every sealed and delta run of the newest version that may hold a target row at or below S_e is rewritten
//!    without those rows, and a version of kind `erase` lists the new runs (same X_d, X_g and groups). The read rule
//!    then answers every target key from SlateDB above X_d (the delete envelopes, or newer writes) or not at all;
//! 3. SlateDB: every target row is at or below S_e <= X_d, so the drop (step 4; `droptest.rs` in the tests) removes
//!    them from L0 and the sorted runs, and SlateDB's GC deletes the old SSTs (`min_age` under 24 hours). WAL objects are
//!    kept for the restore window on purpose (`WAL_RETAIN`), so they leave with the restore points, after 30 days;
//! 4. retire: the runs the scrub replaced are deleted once the GC grace has passed (rule 23: no hold can still read
//!    them) and no pair (a restore point, an export) or fork reference lists them; old manifest versions that list them
//!    or name a target key go the same way. A run kept for a restore point is reported with the checkpoint that keeps it.
//!
//! An erasure is `live_done` once its targets are gone from the runs of the newest version (the 24-hour target, with the
//! drop of step 4), and `done` once no object of ours holds them (the 30-day target).
//!
//! Cost (ESTIMATED): one rewrite of each run that holds the customer, so the customer's group and the delta runs whose
//! key range holds it; an index outside the customer's range spreads its entries over the keyspace, so its groups are
//! rewritten too (the plan's estimate counts only the customer's own group).

use super::build::line;
use super::manifest::{self, hex, now_ms, root, RunLine, Version};
use super::view::{self, RunH, View};
use crate::runfmt::{RunOptions, RunWriter};
use anyhow::{bail, ensure, Context, Result};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// How long after its request an erasure must have left the live runs, L0 and SlateDB (24 hours, DECISIONS
/// 2026-11-07); tests set it shorter (`--sealed-erasure-deadline-ms`).
pub static DEADLINE_MS: AtomicU64 = AtomicU64::new(24 * 3600 * 1000);
/// Erasures recorded, scrub versions published, rows erased from runs, runs retired (diagnostics).
pub static REGISTERED: AtomicU64 = AtomicU64::new(0);
/// audit-fencing: retired runs kept because the store's newest version still lists them.
pub static RETIRE_REFUSED_LIVE: AtomicU64 = AtomicU64::new(0);
pub static SCRUBS: AtomicU64 = AtomicU64::new(0);
pub static ROWS_ERASED: AtomicU64 = AtomicU64::new(0);
pub static RUNS_RETIRED: AtomicU64 = AtomicU64::new(0);
/// perf-erase: runs whose key span met a batch but whose probed blocks held no target (not read whole, not rewritten);
/// batches scrubbed and the erasures they served; scrubs abandoned because the version changed under them.
pub static PROBE_SKIPS: AtomicU64 = AtomicU64::new(0);
pub static BATCHES: AtomicU64 = AtomicU64::new(0);
pub static BATCHED_ERASURES: AtomicU64 = AtomicU64::new(0);
pub static SCRUB_ABANDONS: AtomicU64 = AtomicU64::new(0);
/// erase-cost-1tb: stored bytes the scrub wrote and runs it rewrote, run bytes the scrub and its probe read (block GETs),
/// erasures found clean after ordinary rewrites alone ("rode" them) and erasures whose deadline forced work.
pub static SCRUB_BYTES: AtomicU64 = AtomicU64::new(0);
pub static SCRUB_RUNS: AtomicU64 = AtomicU64::new(0);
pub static SCRUB_READ_BYTES: AtomicU64 = AtomicU64::new(0);
pub static RODE: AtomicU64 = AtomicU64::new(0);
pub static FORCED: AtomicU64 = AtomicU64::new(0);
/// Negative control of perf-erase: no probe, every run that meets a batch is read whole (the code before).
pub static CONTROL_NO_PROBE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Negative control of erase-cost-1tb: the first phase of a scrub in parts drops the envelopes too (the code before).
pub static CONTROL_PARTS_DROP_ENVELOPES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Negative controls of the tests (`fx/tests_7b.rs`, `FX_7B_CONTROL`): no scrub at all; delta runs not scrubbed; target
/// rows erased whatever their seq (a document written again after the erasure is lost); retired runs never deleted.
/// Only the tests set them.
pub static CONTROL_NO_SCRUB: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static CONTROL_NO_DELTA_SCRUB: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static CONTROL_NO_SEQ_BOUND: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static CONTROL_NO_RETIRE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Negative control of the step 7b fix round (FIX 1): the retire step deletes a run whatever database owns it.
pub static CONTROL_NO_OWNERSHIP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Trace of the erasure path (perf-erase): one `fx-erase-trace {json}` line per step on stderr when the environment
/// variable FX_ERASE_TRACE is set. Diagnostics only: it changes nothing else.
pub fn trace_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("FX_ERASE_TRACE").is_some())
}

pub fn trace(v: serde_json::Value) {
    if trace_on() {
        eprintln!("fx-erase-trace {v}");
    }
}

#[inline]
fn ctl(f: &std::sync::atomic::AtomicBool) -> bool {
    f.load(Relaxed)
}

/// The physical keys an erasure removes: whole ranges [lo, hi) and single keys.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Targets {
    pub ranges: Vec<(Vec<u8>, Vec<u8>)>,
    pub keys: BTreeSet<Vec<u8>>,
}

impl Targets {
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty() && self.keys.is_empty()
    }

    /// Add a key unless a range already covers it.
    pub fn add_key(&mut self, k: Vec<u8>) {
        if !self.in_range(&k) {
            self.keys.insert(k);
        }
    }

    fn in_range(&self, k: &[u8]) -> bool {
        self.ranges.iter().any(|(lo, hi)| &lo[..] <= k && k < &hi[..])
    }

    pub fn hit(&self, k: &[u8]) -> bool {
        self.in_range(k) || self.keys.contains(k)
    }

    /// Whether a run whose keys are within [first, last] may hold a target.
    pub fn meets(&self, first: &[u8], last: &[u8]) -> bool {
        self.ranges.iter().any(|(lo, hi)| &lo[..] <= last && first < &hi[..]) || self.keys.range(first.to_vec()..=last.to_vec()).next().is_some()
    }

    /// Whether a target lies in [lo, hi) (hi None: no upper bound), a group's key span.
    pub fn meets_span(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        let below = |k: &[u8]| hi.is_none_or(|h| k < h);
        self.ranges.iter().any(|(a, z)| &z[..] > lo && below(a))
            || match hi {
                Some(h) => self.keys.range(lo.to_vec()..h.to_vec()).next().is_some(),
                None => self.keys.range(lo.to_vec()..).next().is_some(),
            }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let put = |out: &mut Vec<u8>, b: &[u8]| {
            out.extend((b.len() as u32).to_le_bytes());
            out.extend(b);
        };
        for (lo, hi) in &self.ranges {
            out.push(1);
            put(&mut out, lo);
            put(&mut out, hi);
        }
        for k in &self.keys {
            out.push(0);
            put(&mut out, k);
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<Targets> {
        let mut t = Targets::default();
        let mut i = 0;
        let take = |i: &mut usize| -> Result<Vec<u8>> {
            ensure!(*i + 4 <= b.len(), "erasure targets: truncated");
            let n = u32::from_le_bytes(b[*i..*i + 4].try_into().unwrap()) as usize;
            *i += 4;
            ensure!(*i + n <= b.len(), "erasure targets: truncated");
            let v = b[*i..*i + n].to_vec();
            *i += n;
            Ok(v)
        };
        while i < b.len() {
            let tag = b[i];
            i += 1;
            match tag {
                1 => {
                    let lo = take(&mut i)?;
                    let hi = take(&mut i)?;
                    t.ranges.push((lo, hi));
                }
                0 => {
                    let k = take(&mut i)?;
                    t.keys.insert(k);
                }
                x => bail!("erasure targets: bad tag {x}"),
            }
        }
        Ok(t)
    }
}

/// Several erasures scrubbed together (perf-erase): one rewrite of each run serves every erasure of the batch. A row is
/// a target of the batch when it is a target of one of its erasures at or below that erasure's S_e (a document written
/// again after its own erasure is new data and stays, whatever the other erasures' S_e).
#[derive(Clone, Debug, Default)]
pub struct Batch {
    /// Disjoint key segments [lo, hi), in key order, each with the highest S_e of the erasure ranges covering it.
    segs: Vec<(Vec<u8>, Vec<u8>, u64)>,
    /// Single keys, with the highest S_e of the erasures naming them.
    keys: std::collections::BTreeMap<Vec<u8>, u64>,
    /// The highest S_e of the batch (X_d must reach it before the scrub).
    pub max_s_e: u64,
    pub erasures: usize,
}

impl Batch {
    pub fn new(items: &[(&Targets, u64)]) -> Batch {
        let mut b = Batch { erasures: items.len(), ..Default::default() };
        let mut bounds: Vec<&[u8]> = Vec::new();
        for (t, s_e) in items {
            b.max_s_e = b.max_s_e.max(*s_e);
            for (lo, hi) in &t.ranges {
                if lo < hi {
                    bounds.push(lo);
                    bounds.push(hi);
                }
            }
            for k in &t.keys {
                let e = b.keys.entry(k.clone()).or_insert(*s_e);
                *e = (*e).max(*s_e);
            }
        }
        bounds.sort();
        bounds.dedup();
        for w in bounds.windows(2) {
            let (lo, hi) = (w[0], w[1]);
            // the highest S_e of the ranges that cover [lo, hi) (bounds are every range's ends: a range covers the
            // whole segment or none of it)
            let s = items.iter().flat_map(|(t, s_e)| t.ranges.iter().filter(|(a, z)| &a[..] <= lo && hi <= &z[..]).map(move |_| *s_e)).max();
            if let Some(s) = s {
                b.segs.push((lo.to_vec(), hi.to_vec(), s));
            }
        }
        b
    }

    pub fn single(t: &Targets, s_e: u64) -> Batch {
        Batch::new(&[(t, s_e)])
    }

    /// The highest S_e at or below which a row of key `k` is erased, if `k` is a target.
    pub fn bound(&self, k: &[u8]) -> Option<u64> {
        let i = self.segs.partition_point(|(lo, _, _)| &lo[..] <= k);
        let r = i.checked_sub(1).map(|i| &self.segs[i]).filter(|(_, hi, _)| k < &hi[..]).map(|s| s.2);
        match (r, self.keys.get(k)) {
            (Some(a), Some(b)) => Some(a.max(*b)),
            (a, b) => a.or(b.copied()),
        }
    }

    pub fn hit(&self, k: &[u8], seq: u64) -> bool {
        self.bound(k).is_some_and(|s| seq <= s)
    }

    /// Whether a run whose keys are within [first, last] may hold a target.
    pub fn meets(&self, first: &[u8], last: &[u8]) -> bool {
        let i = self.segs.partition_point(|(_, hi, _)| &hi[..] <= first);
        (i < self.segs.len() && &self.segs[i].0[..] <= last) || self.keys.range(first.to_vec()..=last.to_vec()).next().is_some()
    }

    /// The key spans of the batch that meet [first, last]: segments and single keys (as [k, k + 0x00)).
    fn spans_in(&self, first: &[u8], last: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let i = self.segs.partition_point(|(_, hi, _)| &hi[..] <= first);
        for (lo, hi, _) in self.segs[i..].iter().take_while(|(lo, _, _)| &lo[..] <= last) {
            out.push((lo.clone(), hi.clone()));
        }
        for k in self.keys.range(first.to_vec()..=last.to_vec()).map(|(k, _)| k) {
            let mut hi = k.clone();
            hi.push(0);
            out.push((k.clone(), hi));
        }
        out
    }
}

/// A run the scrub replaced, and why it still exists (empty once deleted).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Retired {
    pub path: String,
    pub deleted_ms: Option<u64>,
    /// A run of another database (an ancestor, for a `clone` fork): never deleted here; once nothing of this database
    /// needs it, this database's fork reference in the owner stops naming it (released by reference).
    #[serde(default)]
    pub released_ms: Option<u64>,
    /// Why it is kept: "grace", "restore point {checkpoint}", "fork {db}".
    pub kept_by: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Erasure {
    pub id: String,
    pub kind: String,
    pub collection: String,
    /// A hash of the customer's key component (the component itself is the customer's data; the hash lets an audit
    /// match the erasure to a request).
    pub customer_hash: String,
    pub s_e: u64,
    pub requested_ms: u64,
    pub deadline_ms: u64,
    pub ranges: u64,
    pub keys: u64,
    /// "pending" until the scrub version is published, then "scrubbed".
    pub state: String,
    pub scrubbed_version: Option<u64>,
    pub scrubbed_ms: Option<u64>,
    pub rows_erased: u64,
    pub runs_rewritten: u64,
    pub retired: Vec<Retired>,
    /// Old manifest versions deleted because they listed a retired run or named a target key, and those kept (by a pair
    /// or a fork reference, or the grace).
    pub versions_deleted: u64,
    pub versions_kept: Vec<String>,
    /// A group boundary of the newest version that is a target key (the manifest names it): reported, step 4's split and
    /// merge move it.
    pub boundaries_on_targets: u64,
    /// Live runs clean (the 24-hour target, with step 4's drop at or above S_e) and nothing of ours left (30 days).
    pub live_done_ms: Option<u64>,
    pub done_ms: Option<u64>,
    /// perf-erase: the targets (hex of [`Targets::encode`]) in the record itself until the scrub, so registering is one
    /// PUT; records of earlier releases keep them in `{id}.keys`. They are the customer's data: never shown (`to_json`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub targets: Option<String>,
    /// perf-erase: the batch that scrubbed it (its first erasure's id) and how many erasures that batch served; the rows
    /// and runs above are the batch's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<String>,
    #[serde(default)]
    pub batch_erasures: u64,
    /// erase-cost-1tb ("one reseal serves many"): the version from which every group that may hold the customer had an
    /// X_g at or above S_e (its sealed runs hold none of the rows and the delta runs only dead ones), and the delta runs
    /// listed then that may still hold dead rows of the customer. Each leaves when a merge, a cleanse or a retire replaces
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_clear_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delta_owed: Vec<String>,
    /// erase-cost-1tb: how the live runs became clean: "ordinary" (reseals and merges that ran anyway), "forced" (the
    /// deadline scheduler resealed groups or cleansed delta runs for it), "scrub" (the batch scrub of perf-erase).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleaned_by: Option<String>,
    /// erase-cost-1tb: groups resealed and delta runs cleansed by the deadline scheduler while this erasure was due (shared
    /// with the other erasures it served).
    #[serde(default)]
    pub forced_reseals: u64,
    #[serde(default)]
    pub forced_cleanses: u64,
    /// audit-fencing round 2 (item 15): the entity tag of the record as this process last read or wrote it; `save`
    /// writes only over that version (If-Match), so a process that lost the database cannot overwrite newer progress.
    #[serde(skip)]
    pub tag: RecTag,
}

impl Erasure {
    pub fn overdue(&self, now: u64) -> bool {
        self.live_done_ms.map_or(now > self.deadline_ms, |d| d > self.deadline_ms)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(self).unwrap_or_default();
        if let Some(o) = v.as_object_mut() {
            o.remove("targets");
        }
        v["overdue"] = serde_json::json!(self.overdue(now_ms()));
        v["drop_floor_needed"] = serde_json::json!(self.s_e);
        v
    }
}

pub fn dir(id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Path {
    Path::from(format!("{}/erasures", root(id)))
}

fn rec_path(id: &(impl crate::lifecycle::DbRef + ?Sized), eid: &str) -> Path {
    Path::from(format!("{}/erasures/{eid}.json", root(id)))
}

pub fn keys_path(id: &(impl crate::lifecycle::DbRef + ?Sized), eid: &str) -> Path {
    Path::from(format!("{}/erasures/{eid}.keys", root(id)))
}

/// The fork references of a database: `{db}/fx/forks/{fork}` names the version and runs a fork shares (`pair.rs`).
pub fn forks_dir(id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Path {
    Path::from(format!("{}/forks", root(id)))
}

fn customer_hash(c: &[u8]) -> String {
    format!("{:016x}", twox_hash::XxHash64::oneshot(0x6572_6173, c))
}

/// Record an erasure whose deletes committed at or below `s_e` (the caller holds the database's exclusive lock, so no
/// other write of these keys lies between the deletes and S_e). perf-erase: one PUT, the record with its targets in it;
/// `also`, the path of an index object of the erasure (the server's `control/erasures/{db}/{id}`), is written at the same
/// time: a crash that lands only the index leaves an index with no record, which only makes the agent look once more.
pub async fn register(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), collection: &str, customer: &[u8], targets: &Targets, s_e: u64, also: Option<&(dyn Fn(&str) -> Path + Sync)>) -> Result<Erasure> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let now = now_ms();
    let r: u32 = rand::random();
    let eid = format!("{s_e:020}-{r:08x}");
    let e = Erasure {
        id: eid,
        kind: "customer".into(),
        collection: collection.to_string(),
        customer_hash: customer_hash(customer),
        s_e,
        requested_ms: now,
        deadline_ms: now + DEADLINE_MS.load(Relaxed),
        ranges: targets.ranges.len() as u64,
        keys: targets.keys.len() as u64,
        state: "pending".into(),
        scrubbed_version: None,
        scrubbed_ms: None,
        rows_erased: 0,
        runs_rewritten: 0,
        retired: Vec::new(),
        versions_deleted: 0,
        versions_kept: Vec::new(),
        boundaries_on_targets: 0,
        live_done_ms: None,
        done_ms: None,
        targets: Some(hex(&targets.encode())),
        batch: None,
        batch_erasures: 0,
        groups_clear_version: None,
        delta_owed: Vec::new(),
        cleaned_by: None,
        forced_reseals: 0,
        forced_cleanses: 0,
        tag: RecTag::default(),
    };
    let rec = async { save(store, id, &e).await };
    match also {
        Some(f) => {
            let p = f(&e.id);
            let idx = async { store.put(&p, PutPayload::from_static(b"{}")).await.map(|_| ()).map_err(anyhow::Error::from) };
            futures::try_join!(rec, idx)?;
        }
        None => rec.await?,
    }
    REGISTERED.fetch_add(1, Relaxed);
    Ok(e)
}

/// The version of a stored record this process holds (shared by the clones of an [`Erasure`]); equality ignores it.
#[derive(Clone, Debug, Default)]
pub struct RecTag(Arc<std::sync::Mutex<Option<object_store::UpdateVersion>>>);

impl PartialEq for RecTag {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for RecTag {}

impl RecTag {
    fn get(&self) -> Option<object_store::UpdateVersion> {
        self.0.lock().unwrap().clone()
    }
    fn set(&self, e_tag: Option<String>, version: Option<String>) {
        *self.0.lock().unwrap() = (e_tag.is_some() || version.is_some()).then_some(object_store::UpdateVersion { e_tag, version });
    }
}

/// A conditional write of a record found the stored one changed since this process read it (another process wrote it):
/// nothing written; the caller's pass ends and the next one reads the record again.
#[derive(Debug)]
pub struct RecordChanged(pub String);

impl std::fmt::Display for RecordChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for RecordChanged {}

/// Negative control of the audit-fencing tests (round 2, items 15 and 18): records and fork references written
/// unconditionally, as before.
pub static CONTROL_UNCONDITIONAL_RECORDS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Conditional writes of records and fork references refused because another process wrote first (diagnostics).
pub static RECORD_CONFLICTS: AtomicU64 = AtomicU64::new(0);

/// Write an erasure record: a new one with a conditional create, a read one only over the version read (If-Match).
pub async fn save(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), e: &Erasure) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let body = PutPayload::from(serde_json::to_vec(e)?);
    if ctl(&CONTROL_UNCONDITIONAL_RECORDS) {
        store.put(&rec_path(id, &e.id), body).await?;
        return Ok(());
    }
    let mode = match e.tag.get() {
        Some(v) => object_store::PutMode::Update(v),
        None => object_store::PutMode::Create,
    };
    match store.put_opts(&rec_path(id, &e.id), body.clone(), object_store::PutOptions { mode, ..Default::default() }).await {
        Ok(r) => {
            e.tag.set(r.e_tag, r.version);
            Ok(())
        }
        Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::AlreadyExists { .. }) => {
            RECORD_CONFLICTS.fetch_add(1, Relaxed);
            Err(anyhow::Error::new(RecordChanged(format!("erasure record {} of {id} changed since this process read it: not written", e.id))))
        }
        // a store without conditional updates (the local file system of some tools): as before
        Err(object_store::Error::NotImplemented { .. }) => {
            store.put(&rec_path(id, &e.id), body).await?;
            Ok(())
        }
        Err(x) => Err(x.into()),
    }
}

/// perf-erase: where the records of erasures that are done go (`archive`), so a pass reads only the open ones.
pub fn done_dir(id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Path {
    Path::from(format!("{}/erasures-done", root(id)))
}

async fn read_records(store: &Arc<dyn ObjectStore>, dir: &Path) -> Result<Vec<Erasure>> {
    use futures::StreamExt;
    let metas: Vec<_> = store.list(Some(dir)).try_collect().await?;
    let mut paths: Vec<Path> = metas.into_iter().map(|m| m.location).filter(|p| p.as_ref().ends_with(".json")).collect();
    paths.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    // perf-erase: the GETs in parallel (in order), not one after the other
    let got: Vec<Result<Option<Erasure>>> = futures::stream::iter(paths)
        .map(|p| async move {
            match store.get(&p).await {
                Ok(g) => {
                    let (e_tag, version) = (g.meta.e_tag.clone(), g.meta.version.clone());
                    let e: Erasure = serde_json::from_slice(&g.bytes().await?).with_context(|| format!("erasure record {p}"))?;
                    e.tag.set(e_tag, version);
                    Ok(Some(e))
                }
                // moved to the archive meanwhile
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .buffered(32)
        .collect()
        .await;
    let mut out = Vec::new();
    for r in got {
        out.extend(r?);
    }
    Ok(out)
}

/// The erasures of a database not archived yet (every open one, and done ones the last pass did not move yet), oldest
/// first.
pub async fn list(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<Vec<Erasure>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    read_records(store, &dir(id)).await
}

/// Every erasure of a database, archived ones included (the status API), oldest first.
pub async fn list_all(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<Vec<Erasure>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let mut out = read_records(store, &done_dir(id)).await?;
    let open = list(store, id).await?;
    out.retain(|d| !open.iter().any(|o| o.id == d.id));
    out.extend(open);
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

/// Move the record of an erasure that is done to the archive (written there first: a crash between the two leaves it
/// in both places, and the next pass moves it again).
pub async fn archive(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), e: &Erasure) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    store.put(&Path::from(format!("{}/{}.json", done_dir(id), e.id)), PutPayload::from(serde_json::to_vec(e)?)).await?;
    match store.delete(&rec_path(id, &e.id)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(x) => Err(x.into()),
    }
}

/// Whether a database has an erasure not yet done (cheap: one LIST).
pub async fn any_open(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<bool> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    Ok(list(store, id).await?.iter().any(|e| e.done_ms.is_none()))
}

/// The targets of an erasure: in its record (perf-erase), else in `{id}.keys` (records of earlier releases).
pub async fn targets_of(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), e: &Erasure) -> Result<Option<Targets>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    if let Some(h) = &e.targets {
        return Ok(Some(Targets::decode(&manifest::unhex(h)?)?));
    }
    load_targets(store, id, &e.id).await
}

pub async fn load_targets(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), eid: &str) -> Result<Option<Targets>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    match store.get(&keys_path(id, eid)).await {
        Ok(g) => Ok(Some(Targets::decode(&g.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The run options of the scrub's rewrites (the runs keep their tier's options).
#[derive(Clone, Debug)]
pub struct ScrubOpts {
    pub sealed: RunOptions,
    pub delta: RunOptions,
    /// perf-erase: start no new run rewrite once this much time has passed (the scrub is then partial: `complete`
    /// false, and the caller commits what it did and scrubs again). The runs it wrote must be published well within the
    /// GC grace, which counts from their write (`gc.rs`). None: no limit.
    pub budget: Option<std::time::Duration>,
    /// erase-cost-1tb: keep the target rows that are delete envelopes (the first phase of a scrub in parts). A part
    /// that removed a customer's envelopes while an older run, not rewritten yet, still held its live rows published a
    /// version in which the erased documents were readable again until the next part
    /// (`perf_erase_tests::no_part_of_a_scrub_makes_an_erased_document_readable`). So a scrub in parts first removes
    /// every other target row (the envelopes keep hiding the keys), and only then, once no run holds an older row of
    /// any target, the envelopes, in any order.
    pub keep_envelopes: bool,
}

impl Default for ScrubOpts {
    fn default() -> Self {
        let b = super::build::BuildOpts::default();
        ScrubOpts { sealed: b.sealed, delta: b.delta, budget: None, keep_envelopes: false }
    }
}

/// What a scrub did.
#[derive(Clone, Debug, Default)]
pub struct Scrub {
    pub version: Option<Version>,
    pub retired: Vec<String>,
    pub rows: u64,
    pub runs: u64,
    /// What changed, for a commit on a newer version (`Compactor::commit_scrub`): (group id, old sealed run, new one),
    /// and (old delta run, new one); None when every row was a target.
    pub sealed: Vec<(u32, String, Option<RunLine>)>,
    pub delta: Vec<(String, Option<RunLine>)>,
    /// Every run of the version the scrub started from (rewritten, found clean, or left for the next scrub when it
    /// stopped at its budget): a commit on a newer version checks the runs that are not in it.
    pub examined: BTreeSet<String>,
    /// Whether every run that may hold a target was rewritten or found clean (false: stopped at the budget).
    pub complete: bool,
}

/// Whether the blocks of run `h` that may hold a target of the batch hold one (a row `hit` names that is not a message
/// row of a derived thread: those go only with the documents that use them, `derive::scrub_unit`).
async fn probe_hits(store: &Arc<dyn ObjectStore>, h: &RunH, t: &Batch, hit: &(dyn Fn(&[u8], u64) -> bool + Sync)) -> Result<bool> {
    let run = h.open(store).await?;
    let mut want: BTreeSet<usize> = BTreeSet::new();
    for (lo, hi) in t.spans_in(&h.first, &h.last) {
        if let Some((a, z)) = run.fence.blocks_for(&lo, Some(&hi)) {
            want.extend(a..=z);
        }
    }
    let want: Vec<usize> = want.into_iter().collect();
    let mut i = 0;
    while i < want.len() {
        // consecutive blocks in one range GET, at most 64 at a time (as the rewrite reads)
        let mut z = i;
        while z + 1 < want.len() && want[z + 1] == want[z] + 1 && z + 1 - i < 64 {
            z += 1;
        }
        for blk in view::blocks(store, h, want[i], want[z]).await? {
            for r in 0..blk.len() {
                if blk.env(r).first() != Some(&crate::runfmt::PART) && hit(blk.key(r), blk.seq(r)) {
                    return Ok(true);
                }
            }
        }
        i = z + 1;
    }
    Ok(false)
}

/// Rewrite one run without the batch's target rows. None: the run holds none (nothing written). Some(None): every row
/// was a target (the run leaves the version).
async fn rewrite_run(store: &Arc<dyn ObjectStore>, h: &RunH, path: String, opts: &RunOptions, t: &Batch, dropped: &mut u64, keep_env: bool) -> Result<Option<Option<RunLine>>> {
    let n = view::block_count(store, h).await?;
    let any_seq = ctl(&CONTROL_NO_SEQ_BOUND);
    let hit = |k: &[u8], seq: u64| if any_seq { t.bound(k).is_some() } else { t.hit(k, seq) };
    let keep_env = keep_env && !ctl(&CONTROL_PARTS_DROP_ENVELOPES);
    // perf-erase: read first only the blocks that may hold a target (the run's fence); a run that holds none is not
    // read whole nor rewritten (its key span only met the batch's)
    if !ctl(&CONTROL_NO_PROBE) && !probe_hits(store, h, t, &hit).await? {
        PROBE_SKIPS.fetch_add(1, Relaxed);
        return Ok(None);
    }
    // the new run keeps the codec and the derived flag of the run it replaces (review of c2-delta-log, FIX 2): a run of
    // derived documents is rewritten as one
    let run = h.open(store).await?.clone();
    let mut opts = opts.clone();
    if !super::derive::fix_control(2) {
        opts.codec = run.codec;
        opts.derived = run.derived;
    }
    let mut w: Option<RunWriter> = None;
    let mut erased = 0u64;
    // a derived thread (`derive.rs`): its message rows, then the rest of its customer's rows, scrubbed as one unit
    let mut unit: Option<(Vec<u8>, Vec<super::derive::OutRow>)> = None;
    async fn put(w: &mut Option<RunWriter>, store: &Arc<dyn ObjectStore>, path: &str, opts: &RunOptions, k: &[u8], seq: u64, env: &[u8]) -> Result<()> {
        if w.is_none() {
            *w = Some(RunWriter::new(store, Path::from(path), opts.clone()).await?);
        }
        w.as_mut().unwrap().add(k, seq, env).await
    }
    macro_rules! flush_unit {
        () => {
            if let Some((p, rows)) = unit.take() {
                let rows = match super::derive::scrub_unit(&p, rows.clone(), &hit)? {
                    Some((out, e)) => {
                        erased += e;
                        out
                    }
                    None => rows,
                };
                for (k, seq, env) in rows {
                    put(&mut w, store, &path, &opts, &k, seq, &env).await?;
                }
            }
        };
    }
    let mut b = 0;
    while b < n {
        let z = (b + 63).min(n - 1);
        for blk in view::blocks(store, h, b, z).await? {
            for i in 0..blk.len() {
                let k = blk.key(i);
                if unit.as_ref().is_some_and(|(p, _)| !k.starts_with(p)) {
                    flush_unit!();
                }
                if unit.is_none() && blk.env(i).first() == Some(&crate::runfmt::PART) && !super::derive::fix_control(2) {
                    unit = Some((super::derive::parse_msg_key(k)?.0.to_vec(), Vec::new()));
                }
                if let Some((_, rows)) = unit.as_mut() {
                    rows.push((k.to_vec(), blk.seq(i), blk.env(i).to_vec()));
                    continue;
                }
                if hit(k, blk.seq(i)) && !(keep_env && blk.env(i).first() == Some(&crate::runfmt::DELETED)) {
                    erased += 1;
                    continue;
                }
                put(&mut w, store, &path, &opts, k, blk.seq(i), blk.env(i)).await?;
            }
        }
        b = z + 1;
    }
    flush_unit!();
    if erased == 0 {
        if let Some(w) = w {
            w.abort().await?;
        }
        return Ok(None);
    }
    *dropped += erased;
    Ok(Some(match w {
        Some(w) => {
            let l = line(&w.finish().await?);
            SCRUB_BYTES.fetch_add(l.size, Relaxed);
            SCRUB_RUNS.fetch_add(1, Relaxed);
            Some(l)
        }
        None => None,
    }))
}

/// The scrub of version `cur`: rewrite every run that may hold a target row at or below `s_e`, and build the version
/// that lists the new runs (number `next`, kind `erase`; same X_d, groups and X_g). `version` None: no run held one.
pub async fn scrub_version(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), cur: &View, next: u64, epoch: Option<u64>, t: &Batch, opts: &ScrubOpts) -> Result<Scrub> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let s_e = t.max_s_e;
    ensure!(cur.x_d >= s_e, "database {id}: an erasure at {s_e} needs X_d at or above it (X_d {})", cur.x_d);
    let mut out = Scrub::default();
    if ctl(&CONTROL_NO_SCRUB) {
        return Ok(out);
    }
    out.examined = cur.groups.iter().filter_map(|g| g.sealed.as_ref().map(|h| h.line.path.clone())).chain(cur.delta.iter().map(|h| h.line.path.clone())).collect();
    if ctl(&CONTROL_NO_DELTA_SCRUB) {
        for h in &cur.delta {
            out.examined.remove(&h.line.path);
        }
    }
    let mut v = cur.meta.clone();
    let tag = format!("{next:010}");
    let t_start = std::time::Instant::now();
    // a part stops only after it rewrote a run (runs found clean by the probe are cheap and do not count), so every
    // part moves on, however short the budget
    let over = |out: &mut Scrub| {
        let o = !out.retired.is_empty() && opts.budget.is_some_and(|b| t_start.elapsed() >= b);
        if o {
            out.complete = false;
        }
        o
    };
    out.complete = true;
    for (gi, g) in cur.groups.iter().enumerate() {
        let Some(h) = &g.sealed else { continue };
        if h.line.min_seq > s_e || !t.meets(&h.first, &h.last) {
            continue;
        }
        if over(&mut out) {
            continue;
        }
        let path = manifest::run_path(id, &format!("g{}e{tag}", g.id), g.x_g, epoch);
        let t0 = std::time::Instant::now();
        let rows0 = out.rows;
        let rw = rewrite_run(store, h, path, &opts.sealed, t, &mut out.rows, opts.keep_envelopes).await?;
        trace(serde_json::json!({"ev": "scrub_run", "db": id.name.as_str(), "kind": "sealed", "group": g.id, "size": h.line.size, "raw": h.line.raw, "rows_erased": out.rows - rows0, "rewritten": rw.is_some(), "ms": t0.elapsed().as_millis() as u64}));
        if let Some(new) = rw {
            out.retired.push(h.line.path.clone());
            out.runs += new.is_some() as u64;
            out.sealed.push((g.id, h.line.path.clone(), new.clone()));
            v.groups[gi].sealed = new;
        }
    }
    if !ctl(&CONTROL_NO_DELTA_SCRUB) {
        let mut delta = Vec::with_capacity(cur.delta.len());
        for h in &cur.delta {
            if h.line.min_seq > s_e || !t.meets(&h.first, &h.last) || over(&mut out) {
                delta.push(h.line.clone());
                continue;
            }
            let path = manifest::run_path(id, &format!("delta-e{tag}"), h.line.max_seq, epoch);
            let t0 = std::time::Instant::now();
            let rows0 = out.rows;
            let rw = rewrite_run(store, h, path, &opts.delta, t, &mut out.rows, opts.keep_envelopes).await?;
            trace(serde_json::json!({"ev": "scrub_run", "db": id.name.as_str(), "kind": "delta", "size": h.line.size, "raw": h.line.raw, "rows_erased": out.rows - rows0, "rewritten": rw.is_some(), "ms": t0.elapsed().as_millis() as u64}));
            match rw {
                None => delta.push(h.line.clone()),
                Some(new) => {
                    out.retired.push(h.line.path.clone());
                    out.runs += new.is_some() as u64;
                    out.delta.push((h.line.path.clone(), new.clone()));
                    delta.extend(new);
                }
            }
        }
        // newest first by max seq (a rewritten run can end lower than the next one)
        delta.sort_by(|a, b| b.max_seq.cmp(&a.max_seq));
        v.delta = delta;
    }
    if out.retired.is_empty() {
        return Ok(out);
    }
    v.version = next;
    v.kind = "erase".into();
    v.writer_epoch = epoch;
    v.published_ms = now_ms();
    v.check()?;
    out.version = Some(v);
    Ok(out)
}

/// The run paths and keys a version names (lines, group boundaries).
fn names(v: &Version) -> (Vec<&str>, Vec<Vec<u8>>) {
    let mut runs = Vec::new();
    let mut keys = Vec::new();
    for g in &v.groups {
        keys.extend(manifest::unhex(&g.lo).ok());
        if let Some(r) = &g.sealed {
            runs.push(r.path.as_str());
            keys.extend(manifest::unhex(&r.first).ok());
            keys.extend(manifest::unhex(&r.last).ok());
        }
    }
    for r in &v.delta {
        runs.push(r.path.as_str());
        keys.extend(manifest::unhex(&r.first).ok());
        keys.extend(manifest::unhex(&r.last).ok());
    }
    (runs, keys)
}

/// Whether database `id` owns the object at `path` (it lives under `tenants/{id}/`). A database deletes only objects it
/// owns: a `clone` fork's versions list runs of its ancestors, which those ancestors' versions may list too (review of
/// step 7b, FIX 1). Step 4's GC, reseals and merges must delete runs only through [`delete_run`].
pub fn owns(id: &(impl crate::lifecycle::DbRef + ?Sized), path: &str) -> bool {
    path.starts_with(&format!("{}/", crate::lifecycle::tenant_path(id)))
}

/// Delete a run of database `id`: refused for a run another database owns.
pub async fn delete_run(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), path: &str) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    ensure!(owns(id, path) || ctl(&CONTROL_NO_OWNERSHIP), "database {id} does not own run {path}: never deleted by it");
    match store.delete(&Path::from(path)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {
            // review of big-speed, error 4: a deleted run leaves nothing on the local disk floor
            super::floor::run_deleted(path);
            Ok(())
        }
        Err(x) => Err(x.into()),
    }
}

/// This database stops naming `path` in its fork reference in the run's owner (the shared run is released by
/// reference; the owner's own versions and GC decide its life).
///
/// audit-fencing round 2 (item 18): a read-modify-write of the reference, written only over the version read (If-Match)
/// and done again when another writer came first, so two releases (two erasures of the fork, or a process that lost it)
/// never undo each other.
pub(crate) async fn release_ref(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), path: &str) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    // fix-incarnation: the owner's incarnation is the one in the run's path (the reference sits next to that run)
    let Some(owner) = crate::lifecycle::db_of_path(path) else { return Ok(()) };
    let p = super::pair::fork_ref_path(&owner, &id.name);
    for _ in 0..16 {
        let (b, e_tag, version) = match store.get(&p).await {
            Ok(g) => {
                let (t, v) = (g.meta.e_tag.clone(), g.meta.version.clone());
                (g.bytes().await?, t, v)
            }
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut r: super::pair::ForkRef = serde_json::from_slice(&b)?;
        let n = r.runs.len();
        r.runs.retain(|x| x != path);
        if r.runs.len() == n {
            return Ok(());
        }
        let body = PutPayload::from(serde_json::to_vec(&r)?);
        if ctl(&CONTROL_UNCONDITIONAL_RECORDS) {
            pause_release().await;
            store.put(&p, body).await?;
            return Ok(());
        }
        let opts = object_store::PutOptions { mode: object_store::PutMode::Update(object_store::UpdateVersion { e_tag, version }), ..Default::default() };
        pause_release().await;
        match store.put_opts(&p, body.clone(), opts).await {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. }) => {
                RECORD_CONFLICTS.fetch_add(1, Relaxed);
                continue; // another writer came first: read it again
            }
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(object_store::Error::NotImplemented { .. }) => {
                store.put(&p, body).await?;
                return Ok(());
            }
            Err(x) => return Err(x.into()),
        }
    }
    bail!("fork reference {p}: too many concurrent writers")
}

/// Tests: a release waits here (between its read and its write) while the notify is set.
pub static PAUSE_RELEASE: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>> = std::sync::Mutex::new(None);

async fn pause_release() {
    let n = PAUSE_RELEASE.lock().unwrap().clone();
    if let Some(n) = n {
        n.notified().await;
    }
}

/// What keeps objects of a database alive besides the newest version: the versions named by pairs (restore points,
/// exports) and fork references, with the name of what keeps them.
pub async fn referenced_versions(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized)) -> Result<Vec<(u64, String, Vec<String>)>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let mut out = Vec::new();
    let pairs: Vec<_> = store.list(Some(&Path::from(format!("{}/pairs", root(id))))).try_collect().await?;
    for m in pairs {
        let b = match store.get(&m.location).await {
            Ok(g) => g.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let p: super::pair::Pair = serde_json::from_slice(&b)?;
        if let Some(n) = p.version {
            out.push((n, format!("restore point or export {}", p.checkpoint), Vec::new()));
        }
    }
    let forks: Vec<_> = store.list(Some(&forks_dir(id))).try_collect().await?;
    for m in forks {
        let b = match store.get(&m.location).await {
            Ok(g) => g.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let r: super::pair::ForkRef = serde_json::from_slice(&b)?;
        out.push((r.version, format!("fork {}", r.fork), r.runs));
    }
    Ok(out)
}

/// Retire what the scrub replaced: delete each retired run once the grace has passed and nothing keeps it, and the old
/// versions that list a retired run or name a target key. `t`: the targets while they are still known (None after).
pub async fn retire(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), e: &mut Erasure, newest: u64, grace_ms: u64) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let Some(scrubbed) = e.scrubbed_version else { return Ok(()) };
    let now = now_ms();
    let in_grace = e.scrubbed_ms.map_or(true, |s| now < s + grace_ms);
    let refs = referenced_versions(store, id).await?;
    // the runs each referenced version lists (a fork reference carries its own list: the version may live in the parent)
    let mut kept: Vec<(String, Vec<String>)> = Vec::new();
    for (n, why, runs) in &refs {
        let listed: Vec<String> = if !runs.is_empty() {
            runs.clone()
        } else {
            // fix-retire-read-error: only a version that is gone lists nothing; a failed read fails the pass
            listed_runs(store, id, *n).await?.unwrap_or_default()
        };
        kept.push((why.clone(), listed));
    }
    let no_retire = ctl(&CONTROL_NO_RETIRE);
    for r in e.retired.iter_mut().filter(|r| r.deleted_ms.is_none() && r.released_ms.is_none()) {
        let mut by: Vec<String> = kept.iter().filter(|(_, l)| l.iter().any(|p| *p == r.path)).map(|(w, _)| w.clone()).collect();
        if in_grace {
            by.push("grace".into());
        }
        if no_retire {
            by.push("control".into());
        }
        if by.is_empty() {
            if owns(id, &r.path) || ctl(&CONTROL_NO_OWNERSHIP) {
                delete_run(store, id, &r.path).await?;
                r.deleted_ms = Some(now);
                RUNS_RETIRED.fetch_add(1, Relaxed);
            } else {
                // a run of an ancestor: the rows in it are the ancestor's own data, erased (or not) by the ancestor
                release_ref(store, id, &r.path).await?;
                r.released_ms = Some(now);
                by.push(format!("owned by database {}: released by reference, not deleted", r.path.strip_prefix("tenants/").and_then(|x| x.split('/').next()).unwrap_or("?")));
            }
        }
        r.kept_by = by;
    }
    // old versions: they list retired runs, and their lines name keys (first, last, group boundaries)
    let retired: BTreeSet<&str> = e.retired.iter().map(|r| r.path.as_str()).collect();
    let mut versions_kept = Vec::new();
    for n in manifest::list_versions(store, id).await? {
        if n >= scrubbed || n >= newest {
            continue;
        }
        let v = match manifest::load(store, id, n).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let (runs, _) = names(&v);
        if !runs.iter().any(|p| retired.contains(p)) {
            continue;
        }
        let mut by: Vec<String> = refs.iter().filter(|(m, _, _)| *m == n).map(|(_, w, _)| w.clone()).collect();
        if in_grace {
            by.push("grace".into());
        }
        if by.is_empty() && !no_retire {
            match store.delete(&manifest::manifest_path(id, n)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(x) => return Err(x.into()),
            }
            e.versions_deleted += 1;
        } else {
            versions_kept.push(format!("version {n}: {}", by.join(", ")));
        }
    }
    e.versions_kept = versions_kept;
    if e.live_done_ms.is_none() {
        e.live_done_ms = Some(now);
    }
    if e.retired.iter().all(|r| r.deleted_ms.is_some() || r.released_ms.is_some()) && e.versions_kept.is_empty() {
        e.done_ms.get_or_insert(now);
    }
    Ok(())
}

/// perf-erase: the retire step of every erasure in `es` at once: the references (pairs, forks) and the old versions are
/// read once for all of them (the old step read them again for each erasure). `served`: the version the database's
/// writer serves; a run of an erasure scrubbed in a newer version is kept until the writer installed it.
pub async fn retire_all(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), es: &mut [&mut Erasure], newest: u64, served: u64, grace_ms: u64) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let es: Vec<&mut &mut Erasure> = es.iter_mut().filter(|e| e.scrubbed_version.is_some() && e.done_ms.is_none()).collect();
    if es.is_empty() {
        return Ok(());
    }
    let now = now_ms();
    let refs = referenced_versions(store, id).await?;
    let mut kept: Vec<(String, Vec<String>)> = Vec::new();
    for (n, why, runs) in &refs {
        let listed: Vec<String> = if !runs.is_empty() {
            runs.clone()
        } else {
            // fix-retire-read-error: only a version that is gone lists nothing; a failed read fails the pass
            listed_runs(store, id, *n).await?.unwrap_or_default()
        };
        kept.push((why.clone(), listed));
    }
    // the old versions and the runs they list, read once (only those older than the newest scrub of the batch)
    let top = es.iter().filter_map(|e| e.scrubbed_version).max().unwrap_or(0);
    let mut old: Vec<(u64, Vec<String>)> = Vec::new();
    for n in manifest::list_versions(store, id).await? {
        if n >= top || n >= newest {
            continue;
        }
        if let Some(l) = listed_runs(store, id, n).await? {
            old.push((n, l));
        }
    }
    let no_retire = ctl(&CONTROL_NO_RETIRE);
    // audit-fencing: a run the store's newest version lists is live, whatever a record says (a scrub that a process which
    // lost the database took as published, in a slot below the newest): never deleted. The erasure is then not done.
    let live_now: BTreeSet<String> = if manifest::CONTROL_NO_MAIN_LINE.load(Relaxed) {
        BTreeSet::new()
    } else {
        // the store's newest version and the one readers trust (fix-compactor-start: an older writer's late version on
        // top is passed over by readers, so both may be read)
        match manifest::load_newest(store, id).await? {
            Some(v) => {
                let mut l: BTreeSet<String> = names(&v).0.into_iter().map(|s| s.to_string()).collect();
                let t = manifest::trusted_at(store, id, v).await?;
                l.extend(names(&t).0.into_iter().map(|s| s.to_string()));
                l
            }
            None => BTreeSet::new(),
        }
    };
    let mut deleted_versions: BTreeSet<u64> = BTreeSet::new();
    let mut stray: Option<Vec<String>> = None;
    for e in es {
        let scrubbed = e.scrubbed_version.unwrap();
        let in_grace = e.scrubbed_ms.map_or(true, |s| now < s + grace_ms);
        for r in e.retired.iter_mut().filter(|r| r.deleted_ms.is_none() && r.released_ms.is_none()) {
            let mut by: Vec<String> = kept.iter().filter(|(_, l)| l.iter().any(|p| *p == r.path)).map(|(w, _)| w.clone()).collect();
            if in_grace {
                by.push("grace".into());
            }
            if served < scrubbed {
                by.push(format!("served version {served}"));
            }
            if live_now.contains(&r.path) {
                RETIRE_REFUSED_LIVE.fetch_add(1, Relaxed);
                by.push("listed by the newest version: the scrub is not on the main line".into());
            }
            if no_retire {
                by.push("control".into());
            }
            if by.is_empty() {
                if owns(id, &r.path) || ctl(&CONTROL_NO_OWNERSHIP) {
                    delete_run(store, id, &r.path).await?;
                    r.deleted_ms = Some(now);
                    RUNS_RETIRED.fetch_add(1, Relaxed);
                } else {
                    release_ref(store, id, &r.path).await?;
                    r.released_ms = Some(now);
                    by.push(format!("owned by database {}: released by reference, not deleted", r.path.strip_prefix("tenants/").and_then(|x| x.split('/').next()).unwrap_or("?")));
                }
            }
            r.kept_by = by;
        }
        let retired: BTreeSet<&str> = e.retired.iter().map(|r| r.path.as_str()).collect();
        let mut versions_kept = Vec::new();
        for (n, runs) in old.iter().filter(|(n, _)| *n < scrubbed) {
            if !runs.iter().any(|p| retired.contains(p.as_str())) {
                continue;
            }
            if deleted_versions.contains(n) {
                continue;
            }
            let mut by: Vec<String> = refs.iter().filter(|(m, _, _)| m == n).map(|(_, w, _)| w.clone()).collect();
            if in_grace {
                by.push("grace".into());
            }
            if served <= *n {
                by.push("served".into());
            }
            if by.is_empty() && !no_retire {
                match store.delete(&manifest::manifest_path(id, *n)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(x) => return Err(x.into()),
                }
                deleted_versions.insert(*n);
                e.versions_deleted += 1;
            } else {
                versions_kept.push(format!("version {n}: {}", by.join(", ")));
            }
        }
        // perf-erase: the versions older than the scrub that GC keeps for the restore window (the compactor's version
        // retention, 7 days by default; DECISIONS 2026-11-07 allows restore points 30 days) still list runs written
        // before the scrub, which may hold the erased rows: the erasure is not done while one is left
        let older = old.iter().filter(|(n, _)| *n < scrubbed && !deleted_versions.contains(n)).count();
        if older > 0 {
            versions_kept.push(format!("{older} older versions in the restore window (GC deletes them and their runs when it ends)"));
        }
        e.versions_kept = versions_kept;
        if e.live_done_ms.is_none() && served >= scrubbed && !e.retired.iter().any(|r| live_now.contains(&r.path)) {
            e.live_done_ms = Some(now);
        }
        if e.retired.iter().all(|r| r.deleted_ms.is_some() || r.released_ms.is_some()) && e.versions_kept.is_empty() && e.live_done_ms.is_some() {
            // GC deletes an old version first and its runs at a later pass: a run object that no version from the
            // scrub on lists (nor a pair or a fork) may still hold the rows
            if stray.is_none() {
                stray = Some(stray_runs(store, id, scrubbed, &kept).await?);
            }
            let n = stray.as_ref().unwrap().len();
            if n > 0 {
                e.versions_kept.push(format!("{n} runs of older versions not deleted by GC yet"));
            } else {
                e.done_ms.get_or_insert(now);
            }
        }
    }
    Ok(())
}

/// fix-retire-read-error (RULE 8; gen-2 seeds 104673, 200400, 202173 of the night-head gate): the runs version `n`
/// lists, None when the version is gone. Any other read error is an error: the retire, the stray-run sweep and their
/// reference sets used to take a version they could not read (a store timeout, a 503) as listing nothing, and deleted
/// runs a restore point's version still listed.
async fn listed_runs(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), n: u64) -> Result<Option<Vec<String>>> {
    match manifest::load(store, id, n).await {
        Ok(v) => Ok(Some(names(&v).0.into_iter().map(|s| s.to_string()).collect())),
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) && !CONTROL_RETIRE_READ_ERRORS_AS_GONE.load(Relaxed) => Ok(None),
        Err(_) if CONTROL_RETIRE_READ_ERRORS_AS_GONE.load(Relaxed) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Negative control of fix-retire-read-error, tests only in effect: every error reading a version counts as "gone" (the
/// code before).
pub static CONTROL_RETIRE_READ_ERRORS_AS_GONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Run objects of the database that no version from `from` on lists, nor a pair or a fork (`kept`).
async fn stray_runs(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), from: u64, kept: &[(String, Vec<String>)]) -> Result<Vec<String>> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    let mut listed: BTreeSet<String> = kept.iter().flat_map(|(_, l)| l.iter().cloned()).collect();
    for n in manifest::list_versions(store, id).await?.into_iter().filter(|n| *n >= from) {
        if let Some(l) = listed_runs(store, id, n).await? {
            listed.extend(l);
        }
    }
    let metas: Vec<_> = store.list(Some(&Path::from(format!("{}/run", root(id))))).try_collect().await?;
    Ok(metas.into_iter().map(|m| m.location.to_string()).filter(|p| !listed.contains(p)).collect())
}

/// Group boundaries of a version that are target keys.
pub fn boundaries_on(v: &Version, t: &Targets) -> u64 {
    v.groups.iter().filter_map(|g| manifest::unhex(&g.lo).ok()).filter(|k| !k.is_empty() && t.hit(k)).count() as u64
}

/// perf-erase: add what a scrub (a partial one, or the last of several) replaced to the erasure's record: its runs
/// are retired like a whole scrub's. The caller saves the record before anything else can lose track of them.
pub fn record_partial(e: &mut Erasure, s: &Scrub) {
    e.rows_erased += s.rows;
    e.runs_rewritten += s.runs;
    for p in &s.retired {
        if !e.retired.iter().any(|r| &r.path == p) {
            e.retired.push(Retired { path: p.clone(), deleted_ms: None, released_ms: None, kept_by: vec!["grace".into()] });
        }
    }
}

/// After the scrub: the record of what it did (the targets object goes, its keys are the customer's data).
pub async fn record_scrub(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), e: &mut Erasure, s: &Scrub, version: u64, t: &Targets, newest: &Version) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    e.state = "scrubbed".into();
    e.scrubbed_version = Some(version);
    e.scrubbed_ms = Some(now_ms());
    record_partial(e, s);
    e.boundaries_on_targets = boundaries_on(newest, t);
    let inline = e.targets.take().is_some();
    save(store, id, e).await?;
    if !inline {
        match store.delete(&keys_path(id, &e.id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(x) => return Err(x.into()),
        }
    }
    ROWS_ERASED.fetch_add(s.rows, Relaxed);
    SCRUBS.fetch_add(s.version.is_some() as u64, Relaxed);
    let _ = hex(&[]);
    Ok(())
}

// ---- erase-cost-1tb: one reseal serves many --------------------------------------------------------------------------
//
// The perf-erase batch rewrote every run that held a target, once per batch (hourly by default). At 1 TB, with
// customers spread over the key space and their locators spread by `_id`, that is most of the groups every hour. Here
// the physical erasure rides the rewrites that happen anyway, and the deadline forces only what they did not do:
//
// - A reseal of a group at X_cut >= S_e (rule 14) writes the newest row of each key at or below X_cut and drops every
//   delete envelope: every key the erasure deleted (its newest row at or below S_e is the delete's envelope) leaves the
//   group's sealed run, with every older row of it. Its rows left in delta runs are then at or below the group's X_g:
//   dead for every read (rule 13), and a merge, a retire by the lowest X_g, or a cleanse drops them. A row written again
//   after the erasure (seq above S_e) is new data: the reseal keeps it.
// - So an erasure is clean in the live runs once (1) every group that may hold a target has X_g >= S_e
//   ([`owing_groups`] empty: the manifest says so, no read) and (2) every delta run listed when (1) first held, that may
//   hold a target row at or below S_e ([`owed_delta`]), has left the version. A delta run listed later holds no target
//   row at or below S_e: a merge drops rows at or below X_g, an absorption holds rows above X_d >= S_e only.
// - Then a probe of every run of the newest version that may hold a target confirms it ([`confirm_clean`]); only then
//   is the erasure "scrubbed" and its targets forgotten.
// - The deadline scheduler (`ctl/erase.rs`) forces, for the erasures due, a reseal of each group still owing and then a
//   cleanse of each owed delta run that still holds a target row. Both drop only what ordinary jobs drop, through the
//   compactor's commits: no step can make a row readable that was not readable before (a forced reseal is a reseal; a
//   cleanse drops only rows at or below X_g).

/// The groups of version `v` that may still hold a row of `t` at or below `s_e` the reads can see: X_g below `s_e` and a
/// target in their key span.
pub fn owing_groups(v: &Version, t: &Targets, s_e: u64) -> Result<Vec<u32>> {
    let mut out = Vec::new();
    for (i, g) in v.groups.iter().enumerate() {
        if g.x_g >= s_e {
            continue;
        }
        let lo = manifest::unhex(&g.lo)?;
        let hi = v.groups.get(i + 1).map(|n| manifest::unhex(&n.lo)).transpose()?;
        if t.meets_span(&lo, hi.as_deref()) {
            out.push(g.id);
        }
    }
    Ok(out)
}

/// The delta runs of `v` that may hold a row of `t` at or below `s_e` (live or dead).
pub fn owed_delta(v: &Version, t: &Targets, s_e: u64) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for d in &v.delta {
        if d.min_seq > s_e {
            continue;
        }
        let (first, last) = (manifest::unhex(&d.first)?, manifest::unhex(&d.last)?);
        if t.meets(&first, &last) {
            out.push(d.path.clone());
        }
    }
    Ok(out)
}

/// The target rows (key, seq) of `t` in the blocks of run `h` that may hold one (message rows of derived threads are
/// not counted: a reseal re-encodes them from the documents left; [`probe_hits`] does the same).
pub async fn probe_rows(store: &Arc<dyn ObjectStore>, h: &RunH, t: &Batch, limit: usize) -> Result<Vec<(Vec<u8>, u64)>> {
    let run = h.open(store).await?;
    let mut want: BTreeSet<usize> = BTreeSet::new();
    for (lo, hi) in t.spans_in(&h.first, &h.last) {
        if let Some((a, z)) = run.fence.blocks_for(&lo, Some(&hi)) {
            want.extend(a..=z);
        }
    }
    let want: Vec<usize> = want.into_iter().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < want.len() {
        let mut z = i;
        while z + 1 < want.len() && want[z + 1] == want[z] + 1 && z + 1 - i < 64 {
            z += 1;
        }
        for blk in view::blocks(store, h, want[i], want[z]).await? {
            for r in 0..blk.len() {
                if blk.env(r).first() != Some(&crate::runfmt::PART) && t.hit(blk.key(r), blk.seq(r)) {
                    out.push((blk.key(r).to_vec(), blk.seq(r)));
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
        }
        i = z + 1;
    }
    Ok(out)
}

/// For each erasure of `items` (targets, S_e): whether no run of version `cur` holds one of its target rows at or below
/// its S_e. Every run that may hold one is probed once for all of them (only the blocks that may hold a target).
/// `only`: probe only these runs (None: every run of the version).
pub async fn confirm_clean(store: &Arc<dyn ObjectStore>, cur: &View, items: &[(&Targets, u64)], only: Option<&BTreeSet<String>>) -> Result<Vec<bool>> {
    let mut clean = vec![true; items.len()];
    if items.is_empty() {
        return Ok(clean);
    }
    let all = Batch::new(items);
    let singles: Vec<Batch> = items.iter().map(|(t, s)| Batch::single(t, *s)).collect();
    let runs = cur.groups.iter().filter_map(|g| g.sealed.as_ref()).chain(cur.delta.iter());
    for h in runs {
        if only.is_some_and(|o| !o.contains(&h.line.path)) {
            continue;
        }
        if h.line.min_seq > all.max_s_e || !all.meets(&h.first, &h.last) {
            continue;
        }
        let rows = probe_rows(store, h, &all, 100_000).await?;
        for (i, b) in singles.iter().enumerate() {
            if clean[i] && rows.iter().any(|(k, s)| b.hit(k, *s)) {
                clean[i] = false;
            }
        }
    }
    Ok(clean)
}

/// The record of an erasure whose live runs became clean without a scrub (ridden, or forced through reseals and
/// cleanses): "scrubbed" at `version`, its targets forgotten (they are the customer's data). The retire step then waits
/// for the runs and versions the compactor replaced to leave (GC) as for a scrub.
pub async fn record_clean(store: &Arc<dyn ObjectStore>, id: &(impl crate::lifecycle::DbRef + ?Sized), e: &mut Erasure, version: u64, how: &str) -> Result<()> {
    let id = &crate::lifecycle::DbRef::at(id); // fix-incarnation: one incarnation for the whole call
    e.state = "scrubbed".into();
    e.scrubbed_version = Some(version);
    e.scrubbed_ms = Some(now_ms());
    e.cleaned_by = Some(how.to_string());
    e.delta_owed.clear();
    let inline = e.targets.take().is_some();
    save(store, id, e).await?;
    if !inline {
        match store.delete(&keys_path(id, &e.id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(x) => return Err(x.into()),
        }
    }
    if how == "ordinary" {
        RODE.fetch_add(1, Relaxed);
    }
    Ok(())
}

/// erase-cost-1tb, review fix: the plan of one scrub pass in parts. The batch is frozen at its first part: an erasure
/// whose deletes are absorbed while the pass runs waits for the next pass, so every erasure of the batch goes through
/// phase 1 (every target row but the envelopes) before phase 2 drops its envelopes. Before the fix an erasure absorbed
/// during phase 1 joined the batch in phase 2, and its envelopes could leave in one part while its live rows were still
/// in an older run (`perf_erase_tests::review_an_erasure_joining_in_phase_two_is_not_protected`).
#[derive(Clone, Debug)]
pub struct PartsPlan {
    frozen: Option<BTreeSet<String>>,
    /// phase 1 (keep the envelopes) until it completes
    pub keep_envelopes: bool,
}

/// Negative control of the review fix: the batch is not frozen (an erasure absorbed during phase 1 joins phase 2).
pub static CONTROL_UNFROZEN_BATCH: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

impl Default for PartsPlan {
    fn default() -> Self {
        PartsPlan { frozen: None, keep_envelopes: true }
    }
}

impl PartsPlan {
    /// The erasures (by id) this part may scrub, of those `ready` now: the first call freezes the batch.
    pub fn admit(&mut self, ready: &[String]) -> Vec<String> {
        if ctl(&CONTROL_UNFROZEN_BATCH) {
            return ready.to_vec();
        }
        match &self.frozen {
            None => {
                if !ready.is_empty() {
                    self.frozen = Some(ready.iter().cloned().collect());
                }
                ready.to_vec()
            }
            Some(f) => ready.iter().filter(|e| f.contains(*e)).cloned().collect(),
        }
    }

    /// After a part: a complete phase 1 starts phase 2.
    pub fn after_part(&mut self, complete: bool) {
        if complete {
            self.keep_envelopes = false;
        }
    }
}
