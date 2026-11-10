//! Test support: a stand-in for step 4's drop job, so the paths that must read through our runs (export, PITR, holds)
//! can be tested before the compactor exists.
//!
//! It does what RULES-FOR-THE-CODE rule 12 says a drop does, once: it folds every L0 SST and every SlateDB sorted run
//! into one sorted run and drops every row with seq <= `floor` (SlateDB's compaction filter, the plan's probe 3). The
//! caller picks the floor (normally the X_d of the newest version); nothing here computes a floor from holds or leases
//! (rules 7-10 are step 4's); it refuses to start while a checkpoint holds the drop lease (rule 11, `pair.rs`). One
//! difference from the real job, on purpose:
//! the envelope marker row is kept: the server reads E_env from SlateDB at open (`Tenant::load_env`), and a drop that
//! removed it would make a reopened writer decode every envelope as data. Step 4 must keep it the same way, or take
//! E_env from the version (noted in the step 7a report).
//!
//! The database must be closed in this process (the caller holds its lock): it opens a writer of its own, which fences
//! any other writer.

use anyhow::{ensure, Result};
use async_trait::async_trait;
use object_store::ObjectStore;
use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::config::{CompactorOptions, FlushOptions, FlushType};
use slatedb::{CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext, RowEntry};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

/// What one drop did.
#[derive(Debug, Default, Clone)]
pub struct DropReport {
    pub floor: u64,
    pub dropped: u64,
    pub kept: u64,
}

#[derive(Clone)]
struct Job {
    floor: u64,
    armed: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    dropped: Arc<AtomicU64>,
    kept: Arc<AtomicU64>,
}

impl CompactionSchedulerSupplier for Job {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.clone())
    }
}

impl CompactionScheduler for Job {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if !self.armed.load(SeqCst) || self.started.load(SeqCst) {
            return vec![];
        }
        if let Some(c) = state.compactions() {
            if c.recent_compactions().any(|c| c.active()) {
                return vec![];
            }
        }
        let m = state.manifest();
        let mut src: Vec<SourceId> = m.l0().iter().map(|v| SourceId::SstView(v.id)).collect();
        // newest run first, as SlateDB orders its sources (`tiered::logical_order`)
        let mut runs: Vec<u32> = m.compacted().iter().map(|r| r.id).collect();
        runs.sort_unstable_by(|a, b| b.cmp(a));
        let dest = runs.last().copied().unwrap_or(0);
        src.extend(runs.iter().map(|&r| SourceId::SortedRun(r)));
        if src.is_empty() {
            self.started.store(true, SeqCst);
            self.done.store(true, SeqCst);
            return vec![];
        }
        self.started.store(true, SeqCst);
        vec![CompactionSpec::new(src, dest)]
    }
}

struct Filter(Job);

#[async_trait]
impl CompactionFilter for Filter {
    async fn filter(&mut self, entry: &RowEntry) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if entry.seq <= self.0.floor && entry.key.as_ref() != crate::envelope::MARKER {
            self.0.dropped.fetch_add(1, SeqCst);
            Ok(CompactionFilterDecision::Drop)
        } else {
            self.0.kept.fetch_add(1, SeqCst);
            Ok(CompactionFilterDecision::Keep)
        }
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        self.0.done.store(true, SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CompactionFilterSupplier for Job {
    async fn create_compaction_filter(&self, _c: &CompactionJobContext) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(Filter(self.clone())))
    }
}

/// Drop every SlateDB row of database `id` with seq <= `floor` (the envelope marker excepted), folding everything
/// into one sorted run.
pub async fn drop_at_or_below(store: &Arc<dyn ObjectStore>, id: &str, floor: u64) -> Result<DropReport> {
    ensure!(!super::pair::lease_held(store, id).await?, "database {id}: a checkpoint holds the drop lease (rule 11)");
    let job = Job { floor, armed: Default::default(), started: Default::default(), done: Default::default(), dropped: Default::default(), kept: Default::default() };
    let mut s = crate::lifecycle::writer_settings();
    s.compactor_options = None;
    s.manifest_poll_interval = Duration::from_millis(50); // the writer's view follows the job's output on its poll
    let mut co = CompactorOptions::default();
    co.poll_interval = Duration::from_millis(20);
    let path = crate::lifecycle::tenant_path(id);
    let owned = crate::counting::owned(store, id);
    let cb = slatedb::CompactorBuilder::new(path.clone(), Arc::clone(&owned)).with_options(co).with_scheduler_supplier(Arc::new(job.clone())).with_compaction_filter_supplier(Arc::new(job.clone()));
    let db = slatedb::Db::builder(path, owned).with_settings(s).with_compactor_builder(cb).build().await?;
    db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
    job.armed.store(true, SeqCst);
    // the compactor sees the L0 SST of that flush on its next poll; wait for the job and for the writer's view of it
    let mut ok = false;
    for _ in 0..1500 {
        if job.done.load(SeqCst) {
            let m = db.manifest();
            if m.l0().is_empty() && m.compacted().len() <= 1 {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (l0, srs) = (db.manifest().l0().len(), db.manifest().compacted().len());
    db.close().await?;
    ensure!(ok, "database {id}: the drop job did not finish in 30 s (started {}, done {}, L0 {l0}, runs {srs}, dropped {}, kept {})", job.started.load(SeqCst), job.done.load(SeqCst), job.dropped.load(SeqCst), job.kept.load(SeqCst));
    Ok(DropReport { floor, dropped: job.dropped.load(SeqCst), kept: job.kept.load(SeqCst) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_stand_in_drops_rows_at_or_below_the_floor() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let l = crate::lifecycle::Lifecycle::new(Arc::new(crate::counting::CountingStore::new(Arc::clone(&store))), None);
        let mut t = l.create("dropt").await.unwrap();
        for i in 0..50u32 {
            t.apply(vec![crate::lifecycle::Op::Put(format!("k{i:03}").into_bytes(), b"v".to_vec())]).await.unwrap();
        }
        let floor = t.txn_snapshot().await.unwrap().seq();
        for i in 50..60u32 {
            t.apply(vec![crate::lifecycle::Op::Put(format!("k{i:03}").into_bytes(), b"w".to_vec())]).await.unwrap();
        }
        t.close().await.unwrap();
        let r = drop_at_or_below(&store, "dropt", floor).await.unwrap();
        assert_eq!(r.dropped, 50, "{r:?}");
        let t = l.wake("dropt").await.unwrap();
        assert!(t.get(b"k001").await.unwrap().is_none());
        assert_eq!(t.get(b"k055").await.unwrap().as_deref(), Some(&b"w"[..]));
        t.close().await.unwrap();
    }
}
