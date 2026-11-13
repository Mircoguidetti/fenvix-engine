//! Deleting a tenant's objects in an order that is safe to interrupt, shared by erase and fork rollback.
//!
//! Order: (1) release the checkpoints this tenant pinned in its ancestors (it is a shallow fork), (2) delete its
//! manifests, which makes it unopenable at once (a half-deleted tenant is then "missing", never "corrupt": a reader
//! needs a manifest), (3) everything else, (4) the lease. Every step can be repeated, so a crashed purge is finished
//! by running it again.

use super::rec::{self, Store};
use super::Ctl;
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStoreExt;
use slatedb::admin::Admin;
use std::sync::Arc;

#[derive(Default, Debug, Clone)]
pub struct PurgeReport {
    pub deleted: u64,
    pub passes: u32,
    pub checkpoints_released: u32,
    /// Incomplete multipart uploads under the database's prefix, aborted (`uploads.rs`; review D1).
    pub uploads_aborted: u64,
}

/// The database's current incarnation's prefix (audit-fencing round 2: `lifecycle::tenant_path`).
pub fn tenant_path(db: &str) -> String {
    tenant_bench::lifecycle::tenant_path(db)
}

/// Every incarnation of the name: what an erase deletes.
pub fn name_prefix(db: &str) -> String {
    format!("tenants/{db}")
}

/// The ancestors (paths) whose SSTs this tenant's latest manifest reads, with the checkpoint that pins each.
pub async fn ancestors(store: &Store, db: &str) -> Result<Vec<(String, Option<uuid::Uuid>)>> {
    ancestors_at(store, &tenant_path(db)).await
}

/// Like [`ancestors`] for a slatedb database at any path (a restore's scratch database).
pub async fn ancestors_at(store: &Store, path: &str) -> Result<Vec<(String, Option<uuid::Uuid>)>> {
    let admin = Admin::builder(path.to_string(), Arc::clone(store)).build();
    let Some(m) = admin.read_manifest(None).await? else { return Ok(vec![]) };
    Ok(m.external_dbs().iter().map(|e| (e.path.clone(), e.final_checkpoint_id)).collect())
}

/// Remove the pins this tenant holds in its ancestors. Ancestors that no longer exist need nothing.
pub async fn release_pins(store: &Store, db: &str) -> Result<u32> {
    release_pins_at(store, &tenant_path(db)).await
}

pub async fn release_pins_at(store: &Store, path_of_db: &str) -> Result<u32> {
    let mut n = 0;
    for (path, cp) in ancestors_at(store, path_of_db).await? {
        let Some(cp) = cp else { continue };
        let parent = Admin::builder(path.clone(), Arc::clone(store)).build();
        if parent.read_manifest(None).await?.is_none() {
            continue;
        }
        parent.delete_checkpoint(cp).await?;
        n += 1;
    }
    Ok(n)
}

pub async fn delete_prefix(store: &Store, prefix: &Path) -> Result<u64> {
    let objs = list(store, prefix).await?;
    delete_all(store, objs).await
}

async fn list(store: &Store, prefix: &Path) -> Result<Vec<Path>> {
    Ok(store.list(Some(prefix)).map_ok(|m| m.location).try_collect().await?)
}

async fn delete_all(store: &Store, paths: Vec<Path>) -> Result<u64> {
    let n = paths.len() as u64;
    let mut del = store.delete_stream(futures::stream::iter(paths.into_iter().map(Ok)).boxed());
    while let Some(r) = del.next().await {
        match r {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(n)
}

/// Delete every object under `tenants/{db}/` (lease and marker included) and `ttl-hints/{db}/`.
pub async fn purge_tenant(ctl: &Ctl, db: &str) -> Result<PurgeReport> {
    let mut rep = PurgeReport::default();
    rep.checkpoints_released = release_pins(&ctl.store, db).await?;
    let prefix = Path::from(name_prefix(db));
    for pass in 0..6 {
        rep.passes = pass + 1;
        let objs = list(&ctl.store, &prefix).await?;
        if objs.is_empty() {
            break;
        }
        let (manifests, rest): (Vec<Path>, Vec<Path>) = objs.into_iter().partition(|p| p.as_ref().contains("/manifest/"));
        rep.deleted += delete_all(&ctl.store, manifests).await?;
        super::failpoint("erase.after_manifests").await;
        let (lease, rest): (Vec<Path>, Vec<Path>) = rest.into_iter().partition(|p| p.filename() == Some("_lease"));
        rep.deleted += delete_all(&ctl.store, rest).await?;
        rep.deleted += delete_all(&ctl.store, lease).await?;
    }
    let hints = list(&ctl.store, &rec::path(&["ttl-hints", db])).await?;
    rep.deleted += delete_all(&ctl.store, hints).await?;
    // the parts of runs and SSTs whose upload was never completed (a cancelled compaction job, a process that exited
    // mid-upload): the database's rows, invisible to LIST (review of d9dbc7c, D1)
    rep.uploads_aborted = crate::uploads::abort_all(&format!("{}/", name_prefix(db))).await?.unwrap_or(0);
    Ok(rep)
}

/// Every object still present for a database, from a fresh listing: the proof that an erase is complete. Prefixes
/// are listed as directories; the single-object records (pin, retention marker, intent) are looked up by name.
pub async fn leftovers(ctl: &Ctl, db: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for prefix in [
        rec::path(&["tenants", db]),
        rec::path(&["ttl-hints", db]),
        rec::path(&["control", "dbs", db]),
        rec::path(&["control", "usage", db]),
        rec::path(&["control", "forks", db]),
        rec::path(&["control", "exports", db]),
        rec::path(&["control", "staging", db]),
        rec::path(&["control", "erasures", db]),
    ] {
        out.extend(list(&ctl.store, &prefix).await?.into_iter().map(|p| p.to_string()));
    }
    // incomplete multipart uploads hold data too, and LIST does not show them
    // (P-fix-slow-store item 6: without the ghosts a listing may show after their abort, which hold no data)
    if let Some(ups) = crate::uploads::list_real(&format!("{}/", name_prefix(db))).await? {
        out.extend(ups.into_iter().map(|u| format!("incomplete upload {} ({})", u.key, u.upload_id)));
    }
    for exact in [rec::path(&["control", "pins", db]), rec::path(&["control", "retention", db]), rec::path(&["control", "ops", &format!("{db}.json")])] {
        match ctl.store.head(&exact).await {
            Ok(_) => out.push(exact.to_string()),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(out)
}

/// The checkpoints in a database's latest manifest (diagnostics: fork pins, restore points, export snapshots).
pub async fn checkpoints(store: &Store, db: &str) -> Result<Vec<serde_json::Value>> {
    let admin = Admin::builder(tenant_path(db), Arc::clone(store)).build();
    Ok(admin
        .list_checkpoints(None)
        .await?
        .into_iter()
        .map(|c| {
            serde_json::json!({
                "id": c.id.to_string(), "name": c.name, "manifest_id": c.manifest_id,
                "created_at_ms": c.create_time.timestamp_millis(), "expires_at_ms": c.expire_time.map(|t| t.timestamp_millis()),
            })
        })
        .collect())
}

/// The latest manifest in brief (diagnostics).
pub async fn manifest_summary(store: &Store, db: &str, id: Option<u64>) -> Result<Option<serde_json::Value>> {
    let admin = Admin::builder(tenant_path(db), Arc::clone(store)).build();
    Ok(admin.read_manifest(id).await?.map(|m| {
        serde_json::json!({
            "id": m.id(), "replay_after_wal_id": m.replay_after_wal_id(), "next_wal_sst_id": m.next_wal_sst_id(),
            "l0_ssts": m.l0().len(), "sorted_runs": m.compacted().len(), "writer_epoch": m.writer_epoch(),
            "external_dbs": m.external_dbs().iter().map(|e| e.path.clone()).collect::<Vec<_>>(),
            "checkpoints": m.checkpoints().len(),
        })
    }))
}
