//! Per-database usage: object-store requests and bytes attributed to a database by object path, Mongo commands per
//! day, wake events with the latency of the first request, storage bytes, last activity.
//!
//! Counters live in memory and are written to `control/usage/{db}/{day}/{holder}-{nonce}.json` (cumulative for
//! that process and day, so a lost write loses nothing already written and several processes never overwrite each
//! other). A `kill -9` loses at most `--usage-flush-secs` of counters. Reading a database's usage sums the files of
//! all processes (this process's own file is replaced by its live counters).

use super::rec::{self, now_ms};
use super::{ApiError, ApiResult, Ctl};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OsResult,
    UploadPart,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DAY_MS: i64 = 86_400_000;

/// Test hook: `CTL_USAGE_DAY_OFFSET_MS` shifts the clock that decides which UTC day a counter belongs to, so a test can
/// cross midnight without waiting for it. Unset in production.
fn day_now() -> i64 {
    static OFFSET: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    let off = *OFFSET.get_or_init(|| std::env::var("CTL_USAGE_DAY_OFFSET_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0));
    (now_ms() + off) / DAY_MS
}
const MAX_WAKE_EVENTS: usize = 200;

#[derive(Default, Clone, Debug)]
pub struct Counts {
    pub get: u64,
    pub get_range: u64,
    pub head: u64,
    pub put: u64,
    pub copy: u64,
    pub list: u64,
    pub delete: u64,
    pub get_bytes: u64,
    pub put_bytes: u64,
}

impl Counts {
    pub fn add(&mut self, o: &Counts) {
        self.get += o.get;
        self.get_range += o.get_range;
        self.head += o.head;
        self.put += o.put;
        self.copy += o.copy;
        self.list += o.list;
        self.delete += o.delete;
        self.get_bytes += o.get_bytes;
        self.put_bytes += o.put_bytes;
    }

    pub fn minus(&self, o: &Counts) -> Counts {
        Counts {
            get: self.get - o.get,
            get_range: self.get_range - o.get_range,
            head: self.head - o.head,
            put: self.put - o.put,
            copy: self.copy - o.copy,
            list: self.list - o.list,
            delete: self.delete - o.delete,
            get_bytes: self.get_bytes - o.get_bytes,
            put_bytes: self.put_bytes - o.put_bytes,
        }
    }

    /// S3 bills GET, range GET and HEAD at the GET tier; PUT, COPY and LIST at the PUT tier.
    pub fn get_tier(&self) -> u64 {
        self.get + self.get_range + self.head
    }

    pub fn put_tier(&self) -> u64 {
        self.put + self.copy + self.list
    }

    pub fn to_json(&self) -> Value {
        json!({
            "get": self.get, "get_range": self.get_range, "head": self.head, "put": self.put, "copy": self.copy, "list": self.list, "delete": self.delete,
            "get_tier_requests": self.get_tier(), "put_tier_requests": self.put_tier(), "get_bytes": self.get_bytes, "put_bytes": self.put_bytes,
        })
    }

    pub fn from_json(v: &Value) -> Counts {
        let g = |k: &str| v[k].as_u64().unwrap_or(0);
        Counts { get: g("get"), get_range: g("get_range"), head: g("head"), put: g("put"), copy: g("copy"), list: g("list"), delete: g("delete"), get_bytes: g("get_bytes"), put_bytes: g("put_bytes") }
    }
}

#[derive(Default, Clone, Debug)]
struct Day {
    counts: Counts,
    commands: u64,
    by_cmd: BTreeMap<String, u64>,
    wake_total: u64,
    wakes: Vec<(i64, u64, String)>,
    storage: Option<(i64, u64)>,
    last_active_ms: i64,
    dirty: bool,
}

#[derive(Default)]
struct Inner {
    days: HashMap<(String, i64), Day>,
    last_active: HashMap<String, i64>,
    seen_wakes: HashMap<String, u64>,
    /// Counters per database as of the start of the current measurement window (see [`Usage::mark`]).
    marks: HashMap<String, Counts>,
}

pub struct Usage {
    inner: Mutex<Inner>,
    nonce: u64,
    holder: Mutex<String>,
}

/// Which database an object path belongs to (`None`: shared, e.g. the tenant listing).
pub fn db_of(p: &Path) -> Option<String> {
    let mut parts = p.parts();
    let first = parts.next()?;
    match first.as_ref() {
        "tenants" | "ttl-hints" => parts.next().map(|s| s.as_ref().to_string()),
        "control" => {
            let kind = parts.next()?;
            match kind.as_ref() {
                "dbs" | "pins" | "forks" | "staging" | "retention" | "exports" => parts.next().map(|s| s.as_ref().trim_end_matches(".json").to_string()),
                // The usage records themselves are not metered: writing them would make the counters dirty again.
                "ops" => parts.next().map(|s| s.as_ref().trim_end_matches(".json").to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

impl Usage {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { inner: Mutex::new(Inner::default()), nonce: rand::random::<u32>() as u64, holder: Mutex::new(String::new()) })
    }

    pub fn set_holder(&self, h: &str) {
        *self.holder.lock().unwrap() = h.to_string();
    }

    fn with_day<R>(&self, db: &str, f: impl FnOnce(&mut Day) -> R) -> R {
        let mut g = self.inner.lock().unwrap();
        let d = g.days.entry((db.to_string(), day_now())).or_default();
        d.dirty = true;
        f(d)
    }

    fn record(&self, p: &Path, f: impl FnOnce(&mut Counts)) {
        if let Some(db) = db_of(p) {
            self.with_day(&db, |d| f(&mut d.counts));
        }
    }

    /// Note activity on a database (a command, an admin operation).
    pub fn touch(&self, db: &str) {
        let now = now_ms();
        self.inner.lock().unwrap().last_active.insert(db.to_string(), now);
        self.with_day(db, |d| d.last_active_ms = now);
    }

    pub fn last_active_ms(&self, db: &str) -> Option<i64> {
        self.inner.lock().unwrap().last_active.get(db).copied()
    }

    /// A Mongo command finished. `woke` carries the latency of the request that found the database asleep.
    pub fn command(&self, db: &str, name: &str, woke: Option<Duration>) {
        let now = now_ms();
        self.inner.lock().unwrap().last_active.insert(db.to_string(), now);
        self.with_day(db, |d| {
            d.commands += 1;
            *d.by_cmd.entry(name.to_string()).or_default() += 1;
            d.last_active_ms = now;
            if let Some(l) = woke {
                d.wake_total += 1;
                if d.wakes.len() < MAX_WAKE_EVENTS {
                    d.wakes.push((now, l.as_millis() as u64, name.to_string()));
                }
            }
        });
    }

    /// True once per wake of a database: `wakes` is the entry's wake counter after the command.
    pub fn claim_wake(&self, db: &str, wakes_before: u64, wakes_after: u64) -> bool {
        if wakes_after <= wakes_before {
            return false;
        }
        let mut g = self.inner.lock().unwrap();
        let seen = g.seen_wakes.entry(db.to_string()).or_default();
        if wakes_after > *seen {
            *seen = wakes_after;
            true
        } else {
            false
        }
    }

    /// Forget a database's counters (it was erased; its usage records went with it).
    pub fn drop_db(&self, db: &str) {
        let mut g = self.inner.lock().unwrap();
        g.days.retain(|(d, _), _| d != db);
        g.last_active.remove(db);
        g.seen_wakes.remove(db);
    }

    pub fn note_storage(&self, db: &str, bytes: u64) {
        let now = now_ms();
        self.with_day(db, |d| d.storage = Some((now, bytes)));
    }

    /// Counters of one database today (for measuring the cost of an operation: take before and after).
    pub fn today(&self, db: &str) -> Counts {
        let g = self.inner.lock().unwrap();
        g.days.get(&(db.to_string(), day_now())).map(|d| d.counts.clone()).unwrap_or_default()
    }

    /// Sum of today's counters over several databases (an operation touches its source, target and control records).
    pub fn today_sum(&self, dbs: &[&str]) -> Counts {
        let mut c = Counts::default();
        for d in dbs {
            c.add(&self.today(d));
        }
        c
    }

    pub fn mark(&self, db: &str) {
        let c = self.today(db);
        self.inner.lock().unwrap().marks.insert(db.to_string(), c);
    }

    fn doc_of(&self, db: &str, day: i64, d: &Day) -> Value {
        json!({
            "db": db, "day": day_string(day), "holder": *self.holder.lock().unwrap(), "nonce": self.nonce,
            "object_store": d.counts.to_json(),
            "commands": d.commands, "by_command": d.by_cmd,
            "wake_total": d.wake_total,
            "wakes": d.wakes.iter().map(|(at, l, c)| json!({ "at_ms": at, "first_request_latency_ms": l, "command": c })).collect::<Vec<_>>(),
            "storage": d.storage.map(|(at, b)| json!({ "sampled_at_ms": at, "bytes": b })),
            "last_active_ms": d.last_active_ms,
        })
    }

    /// Write every dirty (db, day) record. Returns the number written.
    pub async fn flush(&self, store: &rec::Store, ops: &super::OpLocks) -> usize {
        let holder = self.holder.lock().unwrap().clone();
        let todo: Vec<(String, i64, Value)> = {
            let mut g = self.inner.lock().unwrap();
            let mut out = Vec::new();
            for ((db, day), d) in g.days.iter_mut() {
                if d.dirty {
                    d.dirty = false;
                    out.push((db.clone(), *day, self.doc_of(db, *day, d)));
                }
            }
            out
        };
        let mut n = 0;
        for (db, day, doc) in todo {
            // Not while an operation (an erase above all) holds the database: its records must not reappear.
            let Some(_op) = ops.try_lock(&db) else {
                if let Some(d) = self.inner.lock().unwrap().days.get_mut(&(db, day)) {
                    d.dirty = true;
                }
                continue;
            };
            let name = format!("{holder}-{:08x}.json", self.nonce);
            let p = rec::path(&["control", "usage", &db, &day_string(day), &name]);
            match rec::put(store, &p, &doc).await {
                Ok(()) => n += 1,
                Err(e) => {
                    eprintln!("usage flush of {db}: {e:#}");
                    if let Some(d) = self.inner.lock().unwrap().days.get_mut(&(db, day)) {
                        d.dirty = true;
                    }
                }
            }
        }
        // Forget days that are over and written.
        let cutoff = day_now() - 1;
        self.inner.lock().unwrap().days.retain(|(_, day), d| d.dirty || *day >= cutoff);
        n
    }
}

pub fn day_string(day: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(day * DAY_MS).map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default()
}

fn parse_day(s: &str) -> Option<i64> {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok().map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis() / DAY_MS)
}

pub fn spawn_flusher(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    ctl.usage.set_holder(&ctl.holder);
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_secs(ctl.args.usage_flush_secs.max(1)));
        every.tick().await;
        loop {
            every.tick().await;
            // Sample the storage of databases that were active, at most hourly.
            let due: Vec<String> = {
                let g = ctl.usage.inner.lock().unwrap();
                g.days
                    .iter()
                    .filter(|((_, day), d)| *day == day_now() && d.dirty && d.storage.map_or(true, |(at, _)| now_ms() - at > 3_600_000))
                    .map(|((db, _), _)| db.clone())
                    .collect()
            };
            for db in due {
                if let Ok(fp) = super::meta::footprint(&ctl, &super::meta::tenant_prefix(&db)).await {
                    if fp.objects > 0 {
                        ctl.usage.note_storage(&db, fp.bytes);
                    }
                }
            }
            ctl.usage.flush(&ctl.store, &ctl.ops).await;
        }
    });
}

/// Usage of one database over a range of days (UTC), summed over every process that wrote counters.
pub async fn query(ctl: &Arc<Ctl>, db: &str, from: Option<&str>, to: Option<&str>, with_storage: bool) -> ApiResult<Value> {
    super::meta::check_name(db)?;
    if !super::meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    let today = day_now();
    let to_day = match to {
        Some(s) => parse_day(s).ok_or_else(|| ApiError::bad("to must be YYYY-MM-DD"))?,
        None => today,
    };
    let from_day = match from {
        Some(s) => parse_day(s).ok_or_else(|| ApiError::bad("from must be YYYY-MM-DD"))?,
        None => to_day - 6,
    };
    if from_day > to_day {
        return Err(ApiError::bad("from is after to"));
    }
    // The own process's counters come from memory (newer than its file); everyone else's from the bucket.
    let own_suffix = format!("-{:08x}.json", ctl.usage.nonce);
    let mut docs: Vec<Value> = Vec::new();
    let dir = rec::path(&["control", "usage", db]);
    let mut it = ctl.store.list(Some(&dir));
    while let Some(m) = it.next().await {
        let m = m.map_err(anyhow::Error::from)?;
        let parts: Vec<String> = m.location.parts().map(|p| p.as_ref().to_string()).collect();
        let (Some(day), Some(file)) = (parts.get(3).and_then(|d| parse_day(d)), parts.get(4)) else { continue };
        if day < from_day || day > to_day || file.ends_with(&own_suffix) {
            continue;
        }
        if let Some(v) = rec::get(&ctl.store, &m.location).await? {
            docs.push(v);
        }
    }
    {
        let g = ctl.usage.inner.lock().unwrap();
        for ((d, day), x) in g.days.iter() {
            if d == db && *day >= from_day && *day <= to_day {
                docs.push(ctl.usage.doc_of(db, *day, x));
            }
        }
    }
    let mut by_day: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for d in docs {
        by_day.entry(d["day"].as_str().unwrap_or("").to_string()).or_default().push(d);
    }
    let mut rows = Vec::new();
    let mut total = Counts::default();
    let (mut total_cmds, mut total_wakes) = (0u64, 0u64);
    for (day, ds) in by_day {
        let mut c = Counts::default();
        let (mut cmds, mut wake_total) = (0u64, 0u64);
        let mut by_cmd: BTreeMap<String, u64> = BTreeMap::new();
        let mut wakes = Vec::new();
        let mut storage: Option<(i64, u64)> = None;
        for d in &ds {
            c.add(&Counts::from_json(&d["object_store"]));
            cmds += d["commands"].as_u64().unwrap_or(0);
            wake_total += d["wake_total"].as_u64().unwrap_or(0);
            if let Some(m) = d["by_command"].as_object() {
                for (k, v) in m {
                    *by_cmd.entry(k.clone()).or_default() += v.as_u64().unwrap_or(0);
                }
            }
            if let Some(w) = d["wakes"].as_array() {
                for e in w {
                    wakes.push(json!({ "at": rec::rfc3339(e["at_ms"].as_i64().unwrap_or(0)), "at_ms": e["at_ms"], "first_request_latency_ms": e["first_request_latency_ms"], "command": e["command"] }));
                }
            }
            if let (Some(at), Some(b)) = (d["storage"]["sampled_at_ms"].as_i64(), d["storage"]["bytes"].as_u64()) {
                if storage.map_or(true, |(a, _)| at > a) {
                    storage = Some((at, b));
                }
            }
        }
        wakes.sort_by_key(|w| w["at_ms"].as_i64().unwrap_or(0));
        total.add(&c);
        total_cmds += cmds;
        total_wakes += wake_total;
        rows.push(json!({
            "day": day,
            "requests": { "commands": cmds, "by_command": by_cmd },
            "object_store": c.to_json(),
            "wakes": { "count": wake_total, "events_listed": wakes.len(), "events": wakes },
            "storage_bytes_sampled": storage.map(|(at, b)| json!({ "at": rec::rfc3339(at), "bytes": b })),
            "processes": ds.len(),
        }));
    }
    let storage_now = if with_storage { Some(super::meta::footprint(ctl, &super::meta::tenant_prefix(db)).await?.bytes) } else { None };
    Ok(json!({
        "database": db, "from": day_string(from_day), "to": day_string(to_day), "days": rows,
        "totals": { "commands": total_cmds, "wakes": total_wakes, "object_store": total.to_json() },
        "storage_bytes_now": storage_now,
        "note": "counters of the running process are live; other processes' counters are as of their last flush",
    }))
}

// ---- the counting object store ---------------------------------------------------------------

/// Wraps the object store and attributes every request to a database by path.
pub struct UsageStore {
    inner: Arc<dyn ObjectStore>,
    usage: Arc<Usage>,
}

pub fn wrap(inner: Arc<dyn ObjectStore>, usage: Arc<Usage>) -> Arc<dyn ObjectStore> {
    Arc::new(UsageStore { inner, usage })
}

impl fmt::Debug for UsageStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UsageStore({})", self.inner)
    }
}

impl fmt::Display for UsageStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UsageStore({})", self.inner)
    }
}

struct UsageUpload {
    inner: Box<dyn MultipartUpload>,
    usage: Arc<Usage>,
    path: Path,
}

impl fmt::Debug for UsageUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UsageUpload({})", self.path)
    }
}

#[async_trait]
impl MultipartUpload for UsageUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let n = data.content_length() as u64;
        self.usage.record(&self.path, |c| {
            c.put += 1;
            c.put_bytes += n;
        });
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        self.usage.record(&self.path, |c| c.put += 1);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for UsageStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> OsResult<PutResult> {
        let n = payload.content_length() as u64;
        self.usage.record(location, |c| {
            c.put += 1;
            c.put_bytes += n;
        });
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> OsResult<Box<dyn MultipartUpload>> {
        self.usage.record(location, |c| c.put += 1);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(UsageUpload { inner, usage: Arc::clone(&self.usage), path: location.clone() }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let (head, ranged) = (options.head, options.range.is_some());
        let r = self.inner.get_opts(location, options).await;
        let bytes = r.as_ref().map(|r| r.range.end - r.range.start).unwrap_or(0);
        self.usage.record(location, |c| {
            if head {
                c.head += 1;
            } else {
                if ranged {
                    c.get_range += 1;
                } else {
                    c.get += 1;
                }
                c.get_bytes += bytes;
            }
        });
        r
    }

    fn delete_stream(&self, locations: BoxStream<'static, OsResult<Path>>) -> BoxStream<'static, OsResult<Path>> {
        let usage = Arc::clone(&self.usage);
        let locations = locations
            .inspect(move |p| {
                if let Ok(p) = p {
                    usage.record(p, |c| c.delete += 1);
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let (usage, p) = (Arc::clone(&self.usage), prefix.cloned());
        if let Some(p) = &p {
            usage.record(p, |c| c.list += 1);
        }
        let mut seen = 0u64;
        self.inner
            .list(prefix)
            .inspect(move |_| {
                seen += 1;
                if seen > 1000 && (seen - 1) % 1000 == 0 {
                    if let Some(p) = &p {
                        usage.record(p, |c| c.list += 1);
                    }
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        if let Some(p) = prefix {
            self.usage.record(p, |c| c.list += 1);
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        let _ = from;
        self.usage.record(to, |c| c.copy += 1);
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OsResult<()> {
        self.usage.record(to, |c| c.copy += 1);
        self.usage.record(from, |c| c.delete += 1);
        self.inner.rename_opts(from, to, options).await
    }
}

// ---- the command hook ---------------------------------------------------------------------

/// Commands that do not touch a tenant (not recorded against a database).
fn tenantless(name: &str) -> bool {
    matches!(
        name,
        "hello" | "isMaster" | "ismaster" | "ping" | "buildInfo" | "buildinfo" | "getParameter" | "endSessions" | "killSessions" | "refreshSessions" | "whatsmyuri" | "connectionStatus" | "listDatabases" | "tenantStats"
    )
}

pub struct Probe {
    db: String,
    name: String,
    wakes: Option<u64>,
    t0: std::time::Instant,
}

/// Called before a command is handled (see `handle_connection`). Cheap: a map lookup and a try-lock.
pub async fn before(db: &str, command: &bson::Document) -> Option<Probe> {
    let ctl = super::get()?;
    let name = command.keys().next()?.clone();
    if tenantless(&name) || !crate::dbs::valid_db_name(db) {
        return None;
    }
    let wakes = match ctl.dbs.lock_handle(db).try_read() {
        Ok(g) => Some(g.wakes),
        Err(_) => None,
    };
    Some(Probe { db: db.to_string(), name, wakes, t0: std::time::Instant::now() })
}

pub async fn after(p: Option<Probe>) {
    let Some(p) = p else { return };
    let Some(ctl) = super::get() else { return };
    let took = p.t0.elapsed();
    let mut woke = None;
    if let Some(before) = p.wakes {
        let h = ctl.dbs.lock_handle(&p.db);
        let after = match tokio::time::timeout(Duration::from_millis(100), h.read()).await {
            Ok(g) => Some(g.wakes),
            Err(_) => None,
        };
        if let Some(a) = after {
            if ctl.usage.claim_wake(&p.db, before, a) {
                woke = Some(took);
            }
        }
    }
    ctl.usage.command(&p.db, &p.name, woke);
}
