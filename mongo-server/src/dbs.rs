//! One tenant per MongoDB database, driven through the results3 lifecycle: wake as a reader,
//! upgrade to a writer on the first write, clean close after an idle period.

use crate::layout;
use anyhow::{bail, Result};
use bson::{doc, Bson, Document};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tenant_bench::lifecycle::{Lifecycle, Op, Tenant};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

pub struct Dbs {
    lc: Arc<Lifecycle>,
    raw: Arc<dyn ObjectStore>,
    entries: Mutex<HashMap<String, Arc<AsyncMutex<Entry>>>>,
    idle: Duration,
}

pub struct Entry {
    name: String,
    lc: Arc<Lifecycle>,
    raw: Arc<dyn ObjectStore>,
    tenant: Option<Tenant>,
    pub last_used: Instant,
    pub wakes: u64,
    pub closes: u64,
}

/// MongoDB database names may not be empty, contain `/\. "$*<>:|?` or a NUL byte.
pub fn valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.len() < 64 && !name.chars().any(|c| "/\\. \"$*<>:|?\0".contains(c))
}

impl Dbs {
    pub fn new(lc: Arc<Lifecycle>, raw: Arc<dyn ObjectStore>, idle: Duration) -> Arc<Self> {
        Arc::new(Self { lc, raw, entries: Mutex::new(HashMap::new()), idle })
    }

    /// Lock a database's entry. Commands on one database run one at a time.
    pub async fn lock(&self, name: &str) -> OwnedMutexGuard<Entry> {
        let e = {
            let mut m = self.entries.lock().unwrap();
            m.entry(name.to_string())
                .or_insert_with(|| {
                    Arc::new(AsyncMutex::new(Entry {
                        name: name.to_string(),
                        lc: Arc::clone(&self.lc),
                        raw: Arc::clone(&self.raw),
                        tenant: None,
                        last_used: Instant::now(),
                        wakes: 0,
                        closes: 0,
                    }))
                })
                .clone()
        };
        e.lock_owned().await
    }

    /// Cumulative object-store requests made through the lifecycle (not maintenance passes):
    /// (PUT/LIST-tier, GET-tier, DELETE), priced like S3's request tiers.
    pub fn request_counts(&self) -> (u64, u64, u64) {
        let s = self.lc.counting.counters.snapshot();
        (s.billed_put_tier(), s.billed_get_tier(), s.delete)
    }

    /// Close tenants that have been idle for `idle`. Runs until the process exits.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let entries: Vec<Arc<AsyncMutex<Entry>>> = me.entries.lock().unwrap().values().cloned().collect();
                for e in entries {
                    if let Ok(mut g) = e.try_lock() {
                        if g.tenant.is_some() && g.last_used.elapsed() >= me.idle {
                            if let Err(err) = g.close().await {
                                eprintln!("idle close of {} failed: {err:#}", g.name);
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn close_all(&self) {
        let entries: Vec<Arc<AsyncMutex<Entry>>> = self.entries.lock().unwrap().values().cloned().collect();
        for e in entries {
            let mut g = e.lock().await;
            if let Err(err) = g.close().await {
                eprintln!("close of {} failed: {err:#}", g.name);
            }
        }
    }

    /// Database names (tenants) present in object storage.
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let r = self.raw.list_with_delimiter(Some(&Path::from("tenants"))).await?;
        let mut names: Vec<String> = r
            .common_prefixes
            .iter()
            .filter_map(|p| p.parts().nth(1).map(|s| s.as_ref().to_string()))
            .filter(|n| valid_db_name(n))
            .collect();
        names.sort();
        Ok(names)
    }
}

impl Entry {
    pub fn mode(&self) -> &'static str {
        match &self.tenant {
            None => "closed",
            Some(t) if t.is_writer() => "writer",
            Some(_) => "reader",
        }
    }

    pub fn unclean_wake(&self) -> Option<bool> {
        self.tenant.as_ref().map(|t| t.unclean)
    }

    async fn db_exists(&self) -> Result<bool> {
        let prefix = Path::from(format!("tenants/{}", self.name));
        Ok(self.raw.list(Some(&prefix)).next().await.is_some())
    }

    /// Wake the tenant as a reader if it is closed. `Ok(false)` if the database does not exist.
    pub async fn ensure_open(&mut self) -> Result<bool> {
        self.last_used = Instant::now();
        if self.tenant.is_some() {
            return Ok(true);
        }
        match self.lc.wake(&self.name).await {
            Ok(t) => {
                self.tenant = Some(t);
                self.wakes += 1;
                Ok(true)
            }
            Err(e) => {
                if self.db_exists().await? { Err(e) } else { Ok(false) }
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        if let Some(t) = self.tenant.take() {
            t.close().await?;
            self.closes += 1;
        }
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.ensure_open().await? {
            return Ok(None);
        }
        self.tenant.as_ref().unwrap().get(key).await
    }

    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.ensure_open().await? {
            return Ok(vec![]);
        }
        self.tenant.as_ref().unwrap().scan_prefix(prefix).await
    }

    /// Apply writes atomically and durably, creating the database on its first write.
    pub async fn apply(&mut self, ops: Vec<Op>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if !self.ensure_open().await? {
            self.tenant = Some(self.lc.create(&self.name).await?);
            self.wakes += 1;
        }
        self.last_used = Instant::now();
        self.tenant.as_mut().unwrap().apply(ops).await
    }

    /// Close the tenant and delete every object of the database. Holds the tenant's maintenance
    /// lock so no compaction or GC pass can write into it while (or after) it is being deleted:
    /// a pass that finds no manifest does nothing. Repeats the listing until the prefix is empty.
    pub async fn drop_database(&mut self) -> Result<()> {
        let lock = tenant_bench::lifecycle::maint_lock(&self.lc.maint_locks, &self.name);
        let _held = lock.lock().await;
        self.close().await?;
        let prefix = Path::from(format!("tenants/{}", self.name));
        for _ in 0..5 {
            let objs: Vec<Path> = self.raw.list(Some(&prefix)).map(|m| m.map(|m| m.location)).try_collect().await?;
            if objs.is_empty() {
                break;
            }
            let mut del = self.raw.delete_stream(futures::stream::iter(objs.into_iter().map(Ok)).boxed());
            while let Some(r) = del.next().await {
                r?;
            }
        }
        self.lc.touched.lock().unwrap().remove(&self.name);
        Ok(())
    }

    // ---- document-level helpers --------------------------------------------------------

    pub async fn collections(&mut self) -> Result<Vec<String>> {
        let Some(v) = self.get(layout::META_COLLECTIONS).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("names")?.iter().filter_map(|b| b.as_str().map(str::to_string)).collect())
    }

    pub fn collections_op(names: &[String]) -> Result<Op> {
        let d = doc! { "names": names.iter().map(|n| Bson::String(n.clone())).collect::<Vec<_>>() };
        Ok(Op::Put(layout::META_COLLECTIONS.to_vec(), d.to_vec()?))
    }

    /// User-created indexes of a collection (the implicit `_id_` index is not stored).
    pub async fn indexes(&mut self, coll: &str) -> Result<Vec<Document>> {
        let Some(v) = self.get(&layout::index_key(coll)).await? else { return Ok(vec![]) };
        let d = Document::from_reader(&v[..])?;
        Ok(d.get_array("indexes")?.iter().filter_map(|b| b.as_document().cloned()).collect())
    }

    pub fn indexes_op(coll: &str, idx: &[Document]) -> Result<Op> {
        let d = doc! { "indexes": idx.iter().map(|i| Bson::Document(i.clone())).collect::<Vec<_>>() };
        Ok(Op::Put(layout::index_key(coll), d.to_vec()?))
    }

    pub async fn scan_collection(&mut self, coll: &str) -> Result<Vec<Document>> {
        let rows = self.scan(&layout::doc_prefix(coll)).await?;
        rows.into_iter().map(|(_, v)| Ok(Document::from_reader(&v[..])?)).collect()
    }

    pub async fn get_doc(&mut self, coll: &str, id: &Bson) -> Result<Option<Document>> {
        match self.get(&layout::doc_key(coll, id)).await? {
            Some(v) => Ok(Some(Document::from_reader(&v[..])?)),
            None => Ok(None),
        }
    }

    pub fn put_doc_op(coll: &str, d: &Document) -> Result<Op> {
        let id = match d.get("_id") {
            Some(id) => id,
            None => bail!("document without _id"),
        };
        Ok(Op::Put(layout::doc_key(coll, id), d.to_vec()?))
    }

    pub fn delete_doc_op(coll: &str, id: &Bson) -> Op {
        Op::Delete(layout::doc_key(coll, id))
    }
}
