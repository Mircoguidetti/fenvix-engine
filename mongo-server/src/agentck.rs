//! Agent checkpoint collections (compaction 2, `c2-delta-log`): which collections the compactor stores as a thread
//! message log (`fx/derive.rs`). A registered collection is a LangGraph MongoDB-saver collection whose customer key is
//! `thread_id` (`--customer-key checkpoints=thread_id,checkpoint_writes=thread_id`), so one customer is one thread and its
//! documents sit in one key range of layout v2 (`custkey.rs`).
//!
//! `--agent-checkpoints [db.]collection,...` registers collections (default: `checkpoints,checkpoint_writes`, the
//! saver's own names, in every database). Nothing changes unless `--sealed-derive` (the release flag E1 of derived
//! documents) is on: then reseals write those threads as message rows plus derived documents. Reads always understand
//! them (release E0). Correctness does not depend on the registration: every rewrite is checked byte for byte before
//! it is written, so a collection that is not a checkpoint collection is only rewritten for nothing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

static CONFIG: OnceLock<Vec<(Option<String>, String)>> = OnceLock::new();

/// Registered collections whose customer key is known, by (database, escaped collection): true when it is `thread_id`
/// (review of c2-delta-log, FIX 3: a collection named like the saver's but keyed by another field is never rewritten).
/// Filled by every write of a customer-keyed collection (`custkey::for_write`), so a collection is known before its
/// compactor reseals it; an unknown one is left as written.
static THREAD_KEYED: std::sync::LazyLock<Mutex<HashMap<(String, Vec<u8>), bool>>> = std::sync::LazyLock::new(Default::default);

/// The registered collections of database `db`.
pub fn registered(db: &str) -> Vec<String> {
    CONFIG.get().map(|c| c.iter().filter(|(d, _)| d.as_deref().map_or(true, |d| d == db)).map(|(_, c)| c.clone()).collect()).unwrap_or_default()
}

/// Note the customer key of `db.coll` (None: no customer key).
pub fn note_key(db: &str, coll: &str, field: Option<&str>) {
    let Some(cfg) = CONFIG.get() else { return };
    if !cfg.iter().any(|(d, c)| c == coll && d.as_deref().map_or(true, |d| d == db)) {
        return;
    }
    THREAD_KEYED.lock().unwrap().insert((db.to_string(), crate::layout::escape_collection(coll)), field == Some("thread_id"));
}

/// Parse `--agent-checkpoints`.
pub fn configure(spec: &str) -> anyhow::Result<()> {
    let mut v = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (db, coll) = match part.split_once('.') {
            Some((d, c)) => (Some(d.to_string()), c.to_string()),
            None => (None, part.to_string()),
        };
        anyhow::ensure!(!coll.is_empty() && !coll.contains('\0'), "--agent-checkpoints: bad collection in '{part}'");
        v.push((db, coll));
    }
    // the reader's prefetch of a thread's message blocks (any database: it only saves a GET)
    let all: Vec<Vec<u8>> = v.iter().map(|(_, c)| crate::layout::escape_collection(c)).collect();
    if !all.is_empty() {
        tenant_bench::fx::derive::set_read_spec(Some(Arc::new(AgentSpec { db: None, colls: all })));
    }
    let _ = CONFIG.set(v);
    Ok(())
}

/// The key spec of database `db` (None: no registered collection there).
pub fn spec_for(db: &str) -> Option<tenant_bench::fx::derive::SpecRef> {
    let colls: Vec<Vec<u8>> = CONFIG.get()?.iter().filter(|(d, _)| d.as_deref().map_or(true, |d| d == db)).map(|(_, c)| crate::layout::escape_collection(c)).collect();
    if colls.is_empty() {
        return None;
    }
    Some(tenant_bench::fx::derive::SpecRef(Arc::new(AgentSpec { db: Some(db.to_string()), colls })))
}

/// Layout v2 keys: `u/{customer component}{escaped collection}/d/{_id}` for the documents of a customer-keyed
/// collection.
struct AgentSpec {
    /// The database of a compactor's spec (the reader's prefetch spec has none: it only saves a GET).
    db: Option<String>,
    colls: Vec<Vec<u8>>,
}

impl tenant_bench::fx::derive::Spec for AgentSpec {
    fn customer_prefix(&self, key: &[u8]) -> Option<usize> {
        let rest = key.strip_prefix(crate::custkey::U)?;
        Some(crate::custkey::U.len() + crate::keyenc::skip(rest)?)
    }

    fn is_checkpoint_doc(&self, key: &[u8]) -> bool {
        let Some(p) = self.customer_prefix(key) else { return false };
        let rest = &key[p..];
        let Some(c) = self.colls.iter().find(|c| rest.len() > c.len() + 3 && rest.starts_with(c) && &rest[c.len()..c.len() + 3] == b"/d/") else { return false };
        match &self.db {
            // a compactor's spec: only a collection whose customer key is known to be `thread_id`
            Some(db) => THREAD_KEYED.lock().unwrap().get(&(db.clone(), c.clone())).copied().unwrap_or(false),
            None => true,
        }
    }
}
