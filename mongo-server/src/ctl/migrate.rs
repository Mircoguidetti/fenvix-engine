//! Migration to compaction 2 and its rollback, through the control layer (step 8; `tenant_bench::fx::migrate` has the
//! mechanism and the reasons).
//!
//! - `POST /v1/databases/{db}/migration`: begin (envelopes first, then the cut). The agent drives it from there.
//! - `GET /v1/databases/{db}/migration`: the state.
//! - `POST /v1/databases/{db}/migration/run`: drive it now, until it is published or `budget_ms` runs out (operators,
//!   tests). Body: `{"budget_ms": n, "groups": n}`.
//! - `POST /v1/databases/{db}/migration/rollback`: back to release E0's way of reading (SlateDB alone); refused once
//!   committed.
//! - `POST /v1/databases/{db}/migration/commit`: the drop goes on, one-way. Before the window ends only with
//!   `{"force": true}`.
//!
//! The agent: every `POLL_MS`, and at once when a migration begins, it drives every database listed under
//! `control/migrations/`: groups are built and catch-up rounds run outside the database's lock; the publish takes the
//! lock for one short last round, and the compactor starts before the lock is released. A migration whose window ended
//! is committed. A process that dies anywhere leaves the state in the store; whichever process holds the database next
//! resumes it.

use super::{meta, rec, ApiError, ApiResult, Ctl};
use serde_json::{json, Value};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tenant_bench::fx::migrate::{self, Phase};

static WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();
pub static POLL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(10_000);
/// Groups built per pass of the agent before it looks at the other databases again.
pub static GROUPS_PER_STEP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(4);
/// Tests only (`--sealed-test-migrate-all`): the agent begins a migration of every database it holds that has none, so
/// whole test suites run while their databases migrate.
pub static MIGRATE_ALL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Tests only: a pause after each group built, so a test's takeovers land while a small database is still building.
pub static TEST_STEP_DELAY_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Rounds outside the lock at most before the last one runs under it whatever its size.
const MAX_ROUNDS_BEFORE_FINISH: usize = 6;

fn index_path(db: &str) -> object_store::path::Path {
    rec::path(&["control", "migrations", db])
}

fn err(e: anyhow::Error) -> ApiError {
    ApiError::from(e)
}

fn state_json(db: &str, s: Option<&migrate::State>) -> Value {
    match s {
        Some(s) => json!({ "database": db, "migration": s.to_json() }),
        None => json!({ "database": db, "migration": null }),
    }
}

/// `POST /v1/databases/{db}/migration`.
pub async fn begin(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    if !tenant_bench::fx::ENABLED.load(Relaxed) {
        return Err(ApiError::bad("the migration needs --sealed-runs (release E1)"));
    }
    let _op = ctl.ops.lock(db).await?;
    // release E1 announces envelopes and starts them one T_pin later (rule 26): wait for it here
    let wait = tenant_bench::fx::e1_wait() + std::time::Duration::from_secs(10);
    let t0 = std::time::Instant::now();
    let s = loop {
        let r = {
            let mut g = ctl.dbs.lock(db).await;
            g.fx_migrate_begin().await.map_err(err)?
        };
        match r {
            Some(s) => break s,
            None if t0.elapsed() < wait => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            None => return Err(ApiError::conflict("not_ready", format!("database {db}: envelopes have not started yet; begin again"))),
        }
    };
    rec::put(&ctl.store, &index_path(db), &json!({})).await?;
    WAKE.notify_one();
    Ok(state_json(db, Some(&s)))
}

/// `GET /v1/databases/{db}/migration`.
pub async fn status(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let s = migrate::load(&ctl.store, db).await.map_err(err)?;
    Ok(state_json(db, s.as_ref()))
}

/// Drive the migration of `db` until it is published (or committed, rolled back), or `budget` runs out.
pub async fn drive(ctl: &Arc<Ctl>, db: &str, budget: std::time::Duration, groups: usize) -> ApiResult<Option<migrate::State>> {
    let t0 = std::time::Instant::now();
    let mut rounds = 0usize;
    loop {
        let Some(s) = migrate::load(&ctl.store, db).await.map_err(err)? else {
            let _ = rec::delete(&ctl.store, &index_path(db)).await; // the database went
            return Ok(None);
        };
        match s.phase {
            Phase::Building if !s.built => {
                migrate::build_step(&ctl.store, db, groups, &migrate::sealed_opts(), None).await.map_err(err)?;
                let d = TEST_STEP_DELAY_MS.load(Relaxed);
                if d > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(d)).await;
                }
            }
            Phase::Building => {
                let w = {
                    let mut g = ctl.dbs.lock(db).await;
                    g.fx_migrate_writer().await.map_err(err)?
                };
                let Some(w) = w else { return Ok(Some(s)) };
                let rep = migrate::round(&ctl.store, db, &w, &migrate::delta_opts(), None).await.map_err(err)?;
                rounds += 1;
                if rep.sst_bytes <= migrate::FINISH_ROUND_BYTES.load(Relaxed) || rounds >= MAX_ROUNDS_BEFORE_FINISH {
                    let t = std::time::Instant::now();
                    let v = {
                        let mut g = ctl.dbs.lock(db).await;
                        g.fx_migrate_finish().await.map_err(err)?
                    };
                    if let Some(v) = v {
                        eprintln!("migration of {db}: version {v} published; the lock was held {:?}", t.elapsed());
                        migrate::LOCK_US.fetch_max(t.elapsed().as_micros() as u64, Relaxed);
                    }
                }
            }
            Phase::Published if tenant_bench::fx::manifest::now_ms() >= s.window_ends_ms() => {
                commit(ctl, db, false, "the rollback window ended").await?;
                return Ok(migrate::load(&ctl.store, db).await.map_err(err)?);
            }
            _ => return Ok(Some(s)),
        }
        if t0.elapsed() >= budget {
            return Ok(migrate::load(&ctl.store, db).await.map_err(err)?);
        }
    }
}

/// `POST /v1/databases/{db}/migration/run`.
pub async fn run_now(ctl: &Arc<Ctl>, db: &str, body: &Value) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    let budget = std::time::Duration::from_millis(body["budget_ms"].as_u64().unwrap_or(600_000));
    let groups = body["groups"].as_u64().unwrap_or(GROUPS_PER_STEP.load(Relaxed)) as usize;
    let s = drive(ctl, db, budget, groups).await?;
    Ok(state_json(db, s.as_ref()))
}

/// `POST /v1/databases/{db}/migration/rollback`.
pub async fn rollback(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    let r = {
        let mut g = ctl.dbs.lock(db).await;
        g.fx_migrate_rollback("rollback requested through the control API").await
    };
    match r {
        Ok(s) => {
            rec::delete(&ctl.store, &index_path(db)).await?;
            Ok(state_json(db, Some(&s)))
        }
        Err(e) if format!("{e:#}").contains("rollback refused") => Err(ApiError::conflict("rollback_refused", format!("{e:#}"))),
        Err(e) => Err(err(e)),
    }
}

/// `POST /v1/databases/{db}/migration/commit`.
pub async fn commit(ctl: &Arc<Ctl>, db: &str, force: bool, note: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let s = migrate::commit(&ctl.store, db, force, note).await.map_err(|e| ApiError::conflict("not_committed", format!("{e:#}")))?;
    {
        let mut g = ctl.dbs.lock(db).await;
        g.fx_migrate_committed().await.map_err(err)?;
    }
    rec::delete(&ctl.store, &index_path(db)).await?;
    Ok(state_json(db, Some(&s)))
}

/// The migration agent.
pub fn spawn_agent(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    tokio::spawn(async move {
        loop {
            let poll = std::time::Duration::from_millis(POLL_MS.load(Relaxed).max(10));
            let _ = tokio::time::timeout(poll, WAKE.notified()).await;
            if !tenant_bench::fx::ENABLED.load(Relaxed) {
                continue;
            }
            let Some(_gate) = super::agent_gate().await else { return };
            use futures::StreamExt;
            if MIGRATE_ALL.load(Relaxed) {
                for db in meta::names(&ctl).await.unwrap_or_default() {
                    if matches!(db.as_str(), "admin" | "config" | "local") || !ctl.dbs.leases.is_held(&db) {
                        continue;
                    }
                    if matches!(migrate::load(&ctl.store, &db).await, Ok(None)) {
                        if let Err(e) = begin(&ctl, &db).await {
                            eprintln!("migration of {db}: begin: {}", e.message);
                        }
                    }
                }
            }
            let listed: Vec<object_store::path::Path> = ctl.store.list(Some(&rec::path(&["control", "migrations"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
            let dbs: Vec<String> = listed.iter().filter_map(|p| p.parts().nth(2).map(|x| x.as_ref().to_string())).collect();
            for db in dbs {
                // only the process that holds the database drives it (a takeover moves the drive with the database)
                if !ctl.dbs.leases.is_held(&db) {
                    continue;
                }
                let Ok(_op) = ctl.ops.lock(&db).await else { continue };
                match drive(&ctl, &db, std::time::Duration::from_secs(5), GROUPS_PER_STEP.load(Relaxed) as usize).await {
                    Ok(Some(s)) if matches!(s.phase, Phase::Committed | Phase::RolledBack) => {
                        let _ = rec::delete(&ctl.store, &index_path(&db)).await;
                    }
                    Ok(Some(s)) if s.phase == Phase::Building => WAKE.notify_one(), // more to do: come back at once
                    Ok(_) => {}
                    Err(e) => eprintln!("migration of {db}: {}", e.message),
                }
            }
        }
    });
}
