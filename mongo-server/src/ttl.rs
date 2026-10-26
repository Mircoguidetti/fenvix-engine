//! TTL indexes: expired documents are hidden on read (see `index::is_expired`), removed by a sweep while a
//! tenant is open as a writer, and removed for sleeping tenants by a maintenance pass that finds due tenants
//! from the `ttl-hints/{db}/{expiry ms}` objects written at close, with one LIST and no wake.

use crate::coll::{decode, Writes};
use crate::dbs::{Dbs, Entry};
use crate::index::{self, entry_id, State};
use crate::keyenc::prefix_end;
use crate::layout::{doc_prefix, encode_sortable};
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct TtlStats {
    /// Maintenance passes (one LIST of the hint prefix each).
    pub passes: AtomicU64,
    pub hints_listed: AtomicU64,
    /// Tenants found due by a pass.
    pub due: AtomicU64,
    /// Due tenants that were open already (their writer sweep handles them).
    pub skipped_open: AtomicU64,
    /// Due tenants not woken because they were woken for expiry less than the minimum gap ago.
    pub skipped_gap: AtomicU64,
    /// Sleeping tenants woken by the maintenance pass only to expire documents.
    pub wakes: AtomicU64,
    /// Sweeps run inside writer sessions.
    pub session_sweeps: AtomicU64,
    pub docs_expired: AtomicU64,
    pub last_wake: Mutex<HashMap<String, Instant>>,
}

const SWEEP_BATCH: usize = 1000;

/// Delete expired documents of every collection of the tenant. Returns the number removed.
pub async fn sweep_entry(g: &mut Entry, now_ms: i64, max_passes: usize) -> Result<usize> {
    let mut total = 0;
    for coll in g.collections().await? {
        total += sweep_coll(g, &coll, now_ms, max_passes).await?;
    }
    Ok(total)
}

/// Delete expired documents of one collection (at most `max_passes` batches of [`SWEEP_BATCH`]).
pub async fn sweep_coll(g: &mut Entry, coll: &str, now_ms: i64, max_passes: usize) -> Result<usize> {
    let mut total = 0;
    let coll = coll.to_string();
    {
        for _ in 0..max_passes {
            let indexes = g.indexes(&coll).await?;
            let specs = index::ttl_specs(&indexes);
            if specs.is_empty() {
                break;
            }
            let dp = doc_prefix(&coll);
            let mut ids: Vec<Vec<u8>> = Vec::new();
            for idx in indexes.iter().filter(|i| i.state == State::Ready && i.ttl.is_some() && i.key.len() == 1) {
                let prefix = idx.prefix(&coll);
                let asc = idx.key[0].1;
                let b = if asc { 10u8 } else { !10u8 };
                let mut start = prefix.clone();
                start.push(b);
                let end = if asc {
                    // entries with a date at or before now - secs
                    let limit = bson::DateTime::from_millis(now_ms.saturating_sub(idx.ttl.unwrap().saturating_mul(1000)));
                    let mut e = prefix.clone();
                    e.extend(crate::keyenc::encode(&bson::Bson::DateTime(limit)));
                    prefix_end(&e)
                } else {
                    let mut e = prefix.clone();
                    e.push(b + 1);
                    Some(e)
                };
                let rows = g.scan_range(&start, end.as_deref(), false, Some(SWEEP_BATCH), 64 << 10).await?;
                ids.extend(rows.iter().filter_map(|(k, _)| entry_id(k).map(|x| x.to_vec())));
            }
            ids.sort();
            ids.dedup();
            if ids.is_empty() {
                break;
            }
            let keys: Vec<Vec<u8>> = ids.iter().map(|id| [dp.as_slice(), id.as_slice()].concat()).collect();
            let vals = g.get_many(&keys).await?;
            let mut w = Writes::new();
            let mut n = 0;
            for v in vals.into_iter().flatten() {
                let d = decode(&v).map_err(|e| anyhow::anyhow!("{e}"))?;
                if index::is_expired(&d, &specs, now_ms) {
                    w.delete(g, &g.name().to_string(), &coll, &indexes, &d).map_err(|e| anyhow::anyhow!("{e}"))?;
                    n += 1;
                }
            }
            let ops = std::mem::take(&mut w.ops);
            let events = std::mem::take(&mut w.events);
            g.apply(ops).await?;
            g.changes.publish(events);
            total += n;
            if ids.len() < SWEEP_BATCH {
                break;
            }
        }
    }
    let _ = encode_sortable;
    Ok(total)
}

impl Dbs {
    /// Sweep the tenants that are open as writers (the writer session's sweep).
    pub fn spawn_ttl_session_sweeper(self: &Arc<Self>, every: Duration) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                for e in me.entries_snapshot() {
                    let Ok(mut g) = e.try_lock() else { continue };
                    if g.is_writer_open() && g.ttl_seen {
                        match sweep_entry(&mut g, crate::cmd_data::now_ms(), 10).await {
                            Ok(n) => {
                                me.ttl.session_sweeps.fetch_add(1, Relaxed);
                                me.ttl.docs_expired.fetch_add(n as u64, Relaxed);
                            }
                            Err(err) => eprintln!("ttl sweep of {} failed: {err:#}", g.name()),
                        }
                    }
                }
            }
        });
    }

    /// Find sleeping tenants with due documents from the hint objects (one LIST per pass) and wake at most
    /// `max_wakes` of them per pass, none more often than `min_gap`.
    pub fn spawn_ttl_maintenance(self: &Arc<Self>, every: Duration, min_gap: Duration, max_wakes: usize) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(e) = me.ttl_pass(min_gap, max_wakes).await {
                    eprintln!("ttl maintenance pass failed: {e:#}");
                }
            }
        });
    }

    pub async fn ttl_pass(self: &Arc<Self>, min_gap: Duration, max_wakes: usize) -> Result<()> {
        let raw = self.raw_store();
        let st = &self.ttl;
        st.passes.fetch_add(1, Relaxed);
        let metas: Vec<object_store::path::Path> = raw.list(Some(&object_store::path::Path::from("ttl-hints"))).map(|m| m.map(|m| m.location)).try_collect().await?;
        st.hints_listed.fetch_add(metas.len() as u64, Relaxed);
        let now = crate::cmd_data::now_ms();
        let mut due: HashMap<String, i64> = HashMap::new();
        for p in metas {
            let parts: Vec<String> = p.parts().map(|s| s.as_ref().to_string()).collect();
            if parts.len() == 3 {
                if let Ok(ms) = parts[2].parse::<i64>() {
                    let e = due.entry(parts[1].clone()).or_insert(ms);
                    *e = (*e).min(ms);
                }
            }
        }
        let mut woken = 0;
        let mut names: Vec<(String, i64)> = due.into_iter().filter(|(_, ms)| *ms <= now).collect();
        names.sort_by_key(|(_, ms)| *ms);
        for (db, _) in names {
            st.due.fetch_add(1, Relaxed);
            let handle = self.lock_handle(&db);
            let Ok(mut g) = handle.try_lock() else {
                st.skipped_open.fetch_add(1, Relaxed);
                continue;
            };
            if g.is_open() {
                st.skipped_open.fetch_add(1, Relaxed);
                continue;
            }
            let recently = st.last_wake.lock().unwrap().get(&db).map(|t| t.elapsed() < min_gap).unwrap_or(false);
            if recently {
                st.skipped_gap.fetch_add(1, Relaxed);
                continue;
            }
            if woken >= max_wakes {
                continue;
            }
            woken += 1;
            st.wakes.fetch_add(1, Relaxed);
            st.last_wake.lock().unwrap().insert(db.clone(), Instant::now());
            g.ttl_seen = true;
            match sweep_entry(&mut g, now, 20).await {
                Ok(n) => {
                    st.docs_expired.fetch_add(n as u64, Relaxed);
                }
                Err(e) => eprintln!("ttl wake of {db} failed: {e:#}"),
            }
            // Back to sleep right away: this wake was only for expiry (the close rewrites the hint).
            if let Err(e) = g.close().await {
                eprintln!("close of {db} after ttl wake failed: {e:#}");
            }
        }
        Ok(())
    }
}
