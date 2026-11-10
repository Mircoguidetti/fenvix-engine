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

use std::sync::{Arc, OnceLock};

static CONFIG: OnceLock<Vec<(Option<String>, String)>> = OnceLock::new();

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
        tenant_bench::fx::derive::set_read_spec(Some(Arc::new(AgentSpec { colls: all })));
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
    Some(tenant_bench::fx::derive::SpecRef(Arc::new(AgentSpec { colls })))
}

/// Layout v2 keys: `u/{customer component}{escaped collection}/d/{_id}` for the documents of a customer-keyed
/// collection.
struct AgentSpec {
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
        self.colls.iter().any(|c| rest.len() > c.len() + 3 && rest.starts_with(c) && &rest[c.len()..c.len() + 3] == b"/d/")
    }
}
