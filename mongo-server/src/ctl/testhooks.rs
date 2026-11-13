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
    // P-perf-bytes: with the compactor on (the product default), the real publish and the real drop job, never the
    // stand-in (the compactor is then the only publisher)
    if tenant_bench::fx::compactor::SERVER_ON.load(std::sync::atomic::Ordering::Relaxed) {
        return publish_and_real_drop(ctl, db).await;
    }
    // review of big-speed (2026-11-13, the reviewer's false alarm): the stand-in drop removes SlateDB's rows at or below
    // X_d, but the in-process builder still reseals from SlateDB's rows when `--sealed-runs` / `--sealed-reseal-every`
    // let it publish again, and its reseal then lacks the dropped rows. The stand-in is valid only with the builder held
    // off (test_50's `--sealed-runs 1000000 --sealed-reseal-every 1000000`); any other set-up is refused, not run.
    {
        use std::sync::atomic::Ordering::Relaxed;
        let (every, reseal) = (tenant_bench::fx::PUBLISH_EVERY.load(Relaxed), tenant_bench::fx::RESEAL_EVERY.load(Relaxed));
        if every < 1_000_000 || reseal < 1_000_000 {
            return Err(ApiError::new(409, "bad_setup", &format!("publish-drop without the compactor needs --sealed-runs 1000000 --sealed-reseal-every 1000000 (now {every} and {reseal}): the builder would reseal from dropped rows")));
        }
    }
    {
        let mut g = ctl.dbs.lock(db).await;
        g.fx_test_publish().await?;
    }
    drop_rows(ctl, db, &json!({})).await
}

/// `PUT /v1/test/interleave {"database": D, "at": K}`: before the K-th storage access of every command on D, a publish
/// and a drop (`dbs::interleave`). `DELETE` clears it.
pub fn set_interleave(body: Option<&Value>) -> ApiResult<Value> {
    if !enabled() {
        return Err(ApiError::new(404, "no_route", "test hooks are off"));
    }
    let v = match body {
        Some(b) => {
            let db = b["database"].as_str().ok_or_else(|| ApiError::bad("body needs database"))?.to_string();
            let at = b["at"].as_u64().ok_or_else(|| ApiError::bad("body needs at"))? as u32;
            Some((db, at))
        }
        None => None,
    };
    *crate::dbs::INTERLEAVE_SET.lock().unwrap() = v.clone();
    Ok(json!({ "interleave": v.map(|(d, k)| json!({ "database": d, "at": k })), "interleaved": crate::dbs::INTERLEAVED.load(std::sync::atomic::Ordering::Relaxed) }))
}

/// The real path of [`publish_and_drop`] (P-perf-bytes, DECISIONS 2026-11-09 "Long commands keep working"): the
/// database's compactor absorbs everything and publishes, the tenant installs that version under the exclusive lock (as
/// the server does at its next exclusive step), then the hook waits until the real drop job (`fx/drop.rs`, SlateDB's
/// compaction filter in the live waiter) has removed SlateDB's rows at or below the new X_d. The floor passes the old
/// X_d only once no read in flight holds it, so a read that kept the old version across this hook would miss rows.
async fn publish_and_real_drop(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    if !tenant_bench::fx::compactor::DROP_DEFAULT.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(ApiError::new(409, "drop_off", "the real drop is off (--sealed-drop false): nothing to interleave"));
    }
    let r = {
        let mut g = ctl.dbs.lock(db).await;
        g.fx_publish_now().await?
    };
    let Some((c, x_d)) = r else { return Err(ApiError::new(409, "no_compactor", format!("database {db} has no compactor running"))) };
    let (dropped, floor) = c.drop_through(x_d, std::time::Duration::from_secs(60)).await?;
    Ok(json!({ "database": db, "real": true, "x_d": x_d, "floor": floor, "dropped": dropped }))
}

