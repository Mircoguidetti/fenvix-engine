//! Test hooks of the admin interface, off unless the environment variable `FENVIX_TEST_DROP=1` is set.
//!
//! `POST /v1/test/databases/{db}/drop` runs a stand-in for step 4's drop job on the database (`fx/droptest.rs`): the
//! database is quiesced like an export (closed cleanly, its lease held, no compaction pass), then every SlateDB row with
//! seq <= the X_d of its newest version is dropped. Body `{"floor": N}` sets another floor. The paths that must read
//! through our runs (export, PITR) are tested with it (`ctl-tests/test_47_c2_step7a_paths.py`).

use super::{fork, meta, ApiError, ApiResult, Ctl};
use serde_json::{json, Value};
use std::sync::Arc;
use tenant_bench::lifecycle::maint_lock;

pub fn enabled() -> bool {
    std::env::var("FENVIX_TEST_DROP").map(|v| v == "1").unwrap_or(false)
}

pub async fn drop_rows(ctl: &Arc<Ctl>, db: &str, body: &Value) -> ApiResult<Value> {
    if !enabled() {
        return Err(ApiError::new(404, "no_route", "test hooks are off"));
    }
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    let mut g = ctl.dbs.lock(db).await;
    fork::quiesce(ctl, &mut g, db).await?;
    let _preempt = tenant_bench::lifecycle::preempt_compaction(db);
    let lock = maint_lock(&ctl.lc.maint_locks, db);
    let _maint = lock.lock().await;
    ctl.dbs.leases.acquire(db).await?;
    let r = async {
        let newest = tenant_bench::fx::manifest::load_newest(&ctl.store, db).await?;
        let floor = match (body["floor"].as_u64(), &newest) {
            (Some(f), _) => f,
            (None, Some(v)) => v.x_d,
            (None, None) => anyhow::bail!("database {db} has no version of our manifest: nothing to drop"),
        };
        let rep = tenant_bench::fx::droptest::drop_at_or_below(&ctl.store, db, floor).await?;
        Ok::<_, anyhow::Error>(json!({ "database": db, "floor": rep.floor, "dropped": rep.dropped, "kept": rep.kept, "version": newest.map(|v| v.version) }))
    }
    .await;
    ctl.dbs.leases.release(db).await;
    drop(g);
    Ok(r?)
}

/// The test hook between two reads of one command (`dbs::interleave`, step 7b fix round, option (b)): publish a version
/// of `db` with what SlateDB holds above X_d, then drop SlateDB's rows at or below its X_d, as `drop_rows` does.
pub async fn publish_and_drop(db: &str) -> ApiResult<Value> {
    let Some(ctl) = super::get() else { return Err(ApiError::new(404, "no_route", "no control layer")) };
    if !enabled() || !tenant_bench::fx::ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(ApiError::new(404, "no_route", "test hooks or sealed runs are off"));
    }
    {
        let mut g = ctl.dbs.lock(db).await;
        g.fx_test_publish().await?;
    }
    drop_rows(ctl, db, &json!({})).await
}
