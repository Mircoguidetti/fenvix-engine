//! Fork: an instant copy of a database as a new database with its own state.
//!
//! Two modes:
//!
//! * `clone` (default): slatedb's shallow clone. The new tenant's manifest points at the source's SSTs (no data is
//!   copied) and the source gets a checkpoint that stops its garbage collector deleting them; only the WAL objects
//!   still outside an SST are copied, and the source is closed cleanly first so there are none. Cost: a handful of
//!   requests, independent of the data size. The fork reads the source's objects until its own compaction has
//!   rewritten them, so the source cannot be erased meanwhile (`control/forks/{source}/{fork}` records the
//!   dependency; erase answers 409 `has_forks`).
//! * `copy`: server-side copy of every object (one COPY request each, no data through this process). The fork is
//!   fully independent, at the price of one request per object and a second copy of the bytes in the bucket.
//!
//! Either way writes to one side never reach the other: both sides are separate slatedb databases with their own
//! manifests, WALs and leases, and the shared SSTs are immutable.

use super::purge;
use super::rec::{self, now_ms};
use super::usage::Counts;
use super::{meta, ops, ApiError, ApiResult, Ctl};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStoreExt;
use serde_json::{json, Value};
use slatedb::admin::Admin;
use std::sync::Arc;
use tenant_bench::lifecycle::{maint_lock, Op};

/// Branches being created by a fork in this process, with the allow list they will get (their source's; `None` when the
/// source has none). Until the branch's registry record exists the allow-list check reads it from here, so the branch is
/// never open to addresses its source refuses, even while it has no record of its own.
static FORKING: std::sync::Mutex<Option<std::collections::HashMap<String, Option<Arc<Vec<crate::netguard::Net>>>>>> = std::sync::Mutex::new(None);

/// The allow list of `db` while a fork creates it (see [`FORKING`]); `None` when no fork is creating it.
pub fn list_while_forking(db: &str) -> Option<Option<Arc<Vec<crate::netguard::Net>>>> {
    FORKING.lock().unwrap().as_ref().and_then(|m| m.get(db).cloned())
}

/// Registers the branch in [`FORKING`] for as long as it lives.
struct Forking(String);

impl Forking {
    fn new(db: &str, list: Option<Vec<crate::netguard::Net>>) -> Forking {
        FORKING.lock().unwrap().get_or_insert_with(Default::default).insert(db.to_string(), list.map(Arc::new));
        Forking(db.to_string())
    }
}

impl Drop for Forking {
    fn drop(&mut self) {
        if let Some(m) = FORKING.lock().unwrap().as_mut() {
            m.remove(&self.0);
        }
    }
}

pub fn fork_marker(ancestor: &str, db: &str) -> Path {
    rec::path(&["control", "forks", ancestor, db])
}

fn db_of_path(p: &str) -> Option<String> {
    p.strip_prefix("tenants/").map(|s| s.to_string())
}

/// Forks that still read this database's objects (stale records of erased forks are removed on the way).
pub async fn dependents(ctl: &Ctl, db: &str) -> anyhow::Result<Vec<String>> {
    let prefix = rec::path(&["control", "forks", db]);
    let mut out = Vec::new();
    let marks: Vec<Path> = ctl.store.list(Some(&prefix)).map_ok(|m| m.location).try_collect().await?;
    for m in marks {
        let child = m.filename().unwrap_or("").to_string();
        if meta::exists(ctl, &child).await? {
            out.push(child);
        } else if ops::get(ctl, &child).await?.is_none() {
            rec::delete(&ctl.store, &m).await?;
        }
    }
    Ok(out)
}

/// Make the source's latest manifest the complete truth: flushed, marker present, no tenant open on it.
/// The caller holds the entry lock.
pub(crate) async fn quiesce(ctl: &Arc<Ctl>, g: &mut crate::dbs::Entry, src: &str) -> ApiResult<()> {
    if g.is_open() {
        g.close().await?;
    }
    // fix-sim-holes, hole Z: a marker counts only when it names the stored manifest's writer epoch (a process that lost
    // the database without noticing may have closed it after the new owner's writes, which may be only in the WAL)
    let clean = tenant_bench::lifecycle::marker_clean(&ctl.store, src).await.map_err(ApiError::from)?;
    if !clean {
        // A writer died without closing: its last writes are in the WAL beyond the manifest. Open it as a writer
        // (replays the WAL), rewrite a value with itself to make that real, and close it cleanly.
        let cur = g.get(crate::layout::META_COLLECTIONS).await?;
        let Some(cur) = cur else { return Err(ApiError::new(500, "internal", format!("database {src} has no collection list"))) };
        g.apply(vec![Op::Put(crate::layout::META_COLLECTIONS.to_vec(), cur.to_vec())]).await?;
        g.close().await?;
        if !tenant_bench::lifecycle::marker_clean(&ctl.store, src).await.map_err(ApiError::from)? {
            return Err(ApiError::new(500, "internal", format!("database {src} could not be closed cleanly")));
        }
    }
    Ok(())
}

pub async fn fork(ctl: &Arc<Ctl>, src: &str, target: &str, mode: &str) -> ApiResult<Value> {
    meta::check_name(src)?;
    meta::check_name(target)?;
    // A fork that leaves no branch (its source turned out erased, a refusal, a rollback) made requests under the target's
    // name (the intent record, the target's lease, existence checks): they are held back while the target does not exist,
    // or the usage flusher writes a record of a database that does not exist (fix-erase-fork-usage; ctl test_05, an erase
    // racing a fork).
    let probe = ctl.usage.begin_probe(target);
    let r = fork_probed(ctl, src, target, mode).await;
    let present = r.is_ok() || ctl.dbs.is_open_hint(target) || meta::exists(ctl, target).await.unwrap_or(true);
    ctl.usage.end_probe(probe, present);
    r
}

async fn fork_probed(ctl: &Arc<Ctl>, src: &str, target: &str, mode: &str) -> ApiResult<Value> {
    if src == target {
        return Err(ApiError::bad("source and target are the same database"));
    }
    if mode != "clone" && mode != "copy" {
        return Err(ApiError::bad("mode must be \"clone\" or \"copy\""));
    }
    let (_g1, _g2) = ctl.ops.lock2(src, target).await?;
    // An existing database is never forked over: refused before its allow list is replaced by the source's below. (The
    // branch's `ops` lock is held: no registry record or list can be written for it meanwhile; a database made implicitly
    // by a write has no list. The check is repeated under the entry lock below.)
    if meta::exists(ctl, target).await? {
        return Err(ApiError::conflict("exists", format!("database {target} already exists")));
    }
    // The branch gets its source's allow list. From here on, before anything else, the check at authentication reads it
    // from [`FORKING`] until the branch's own record exists: a user created on the branch after the check below cannot
    // come in from an address the source refuses (one created before is seen by the check, which refuses the fork). The
    // source's `ops` lock is held: its list cannot change meanwhile. A stored list that does not parse fails the fork
    // rather than giving an open fork.
    let src_meta = rec::get(&ctl.store, &meta::meta_path(src)).await?;
    let list = src_meta.as_ref().and_then(crate::netguard::stored_entries).transpose().map_err(|e| anyhow::anyhow!("the allow list of {src} is not valid: {e}"))?;
    let _forking = Forking::new(target, list.clone());
    // (before the entry locks below: it reads the admin database)
    meta::ensure_no_auth(ctl, target).await?;
    // The entry locks of the source and of the branch, in name order like a rename across databases (xdb.rs), so the
    // two can never deadlock. The source's is held while it is closed and copied: no command runs on it meanwhile. The
    // branch's is held from before the check that the name is free until the fork is complete or rolled back: no
    // command creates a database with that name in between (the rollback would delete it), and none opens the branch
    // (or writes to it) before it is complete.
    let (g, gt) = if src < target {
        let g = ctl.dbs.lock(src).await;
        (g, ctl.dbs.lock(target).await)
    } else {
        let gt = ctl.dbs.lock(target).await;
        (ctl.dbs.lock(src).await, gt)
    };
    // The entry lock works within this process only. Another process on the same bucket (a handover between two
    // machines, a deploy) could still create the branch's name and write to it, and the rollback would delete those
    // writes. The branch's lease is taken too, before the check that the name is free and until the fork is complete or
    // rolled back: another process asking for the branch meanwhile gets "owned elsewhere" (code 134, retry) instead of
    // writing. A process that already owns it refuses the fork (409 `owned_elsewhere`). When this process already held it
    // (an entry of its own is open on the name: the check below then finds the database), it is not released here.
    let ours = !ctl.dbs.leases.is_held(target);
    // Before the branch's lease is taken: a record that this process may hold it for a fork. A kill between the lease
    // and the intent record (`fork_leased`) leaves a lease object of a database that does not exist, which no intent
    // names; the recovery at the next start removes it through this record ([`recover_leases`]; review of step 7b, the
    // `_lease` leftover of test_02b_fork_crash).
    if ours {
        rec::put(&ctl.store, &forking_path(target), &json!({ "holder": ctl.dbs.leases.holder, "source": src, "at_ms": now_ms() })).await?;
    }
    if let Err(e) = ctl.dbs.leases.acquire(target).await {
        if ours {
            rec::delete(&ctl.store, &forking_path(target)).await?;
        }
        return Err(e.into());
    }
    super::failpoint("fork.after_lease").await;
    let r = fork_leased(ctl, g, src, target, mode, &list, std::time::Instant::now()).await;
    if ours {
        // A fork that leaves no branch (refused, or rolled back) leaves no lease object either, like a database that turned
        // out not to exist (dbs.rs): an erase finds nothing left under the name. Otherwise the lease is released. (After a
        // rollback the object is already gone with the rest of the branch.)
        let none = r.is_err() && !meta::exists(ctl, target).await.unwrap_or(true);
        if none {
            ctl.dbs.leases.discard(target).await;
        } else {
            ctl.dbs.leases.release(target).await;
        }
    }
    if ours {
        rec::delete(&ctl.store, &forking_path(target)).await?;
    }
    // The branch's entry lock last: no command of this process opens the branch while the lease is being released.
    drop(gt);
    r
}

fn forking_path(target: &str) -> Path {
    rec::path(&["control", "forking", &format!("{target}.json")])
}

/// At start, after the interrupted operations were finished (`ops::recover`): a fork killed between taking its branch's
/// lease and writing its intent record left the lease of a database that does not exist. Remove it when this process
/// took it (same holder), the branch has no registry record and no intent; then the record goes.
/// A negative control of the step 7b fix round (tests only): the environment variable names a fix to switch off.
pub(crate) fn control(name: &str) -> bool {
    std::env::var(name).map(|v| v == "1").unwrap_or(false)
}

pub async fn recover_leases(ctl: &Arc<Ctl>) {
    if control("FENVIX_CONTROL_NO_LEASE_RECOVERY") {
        return;
    }
    let listed: Vec<Path> = match ctl.store.list(Some(&rec::path(&["control", "forking"]))).map_ok(|m| m.location).try_collect().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ctl recovery: fork lease records not listed: {e}");
            return;
        }
    };
    for p in listed {
        let target = p.filename().unwrap_or("").trim_end_matches(".json").to_string();
        let r: anyhow::Result<()> = async {
            let Some(m) = rec::get(&ctl.store, &p).await? else { return Ok(()) };
            if m["holder"].as_str() != Some(ctl.dbs.leases.holder.as_str()) {
                return Ok(()); // another process's fork: its own recovery handles it
            }
            if !meta::exists(ctl, &target).await? && ops::get(ctl, &target).await?.is_none() && crate::lease::holder_of(&ctl.store, &target).await?.as_deref() == Some(ctl.dbs.leases.holder.as_str()) {
                // only a lone lease object: anything else under the name is data, never touched here
                let objs: Vec<Path> = ctl.store.list(Some(&Path::from(purge::tenant_path(&target)))).map_ok(|m| m.location).try_collect().await?;
                if !objs.is_empty() && objs.iter().all(|o| o.filename() == Some("_lease")) {
                    for o in objs {
                        rec::delete(&ctl.store, &o).await?;
                    }
                    eprintln!("ctl recovery: removed the lease of {target} left by a fork interrupted before its intent record");
                }
            }
            rec::delete(&ctl.store, &p).await?;
            Ok(())
        }
        .await;
        if let Err(e) = r {
            eprintln!("ctl recovery: fork lease record of {target}: {e:#}");
        }
    }
}

/// The name of the checkpoint `fork_paired` takes in the source for branch `target`: the fork's rollback finds it by it
/// (review of step 7b, FIX 2).
fn pairing_name(target: &str) -> String {
    format!("fork-pairing:{target}")
}

/// Remove what a paired fork into `target` may have left in `src` (a kill between the pairing and its cleanup): the
/// pairing checkpoint, found by its name, its pair, and any pair of `src` whose checkpoint no longer exists.
async fn drop_pairing_leftovers(ctl: &Arc<Ctl>, src: &str, target: &str) -> anyhow::Result<()> {
    let admin = Admin::builder(purge::tenant_path(src), Arc::clone(&ctl.store)).build();
    if admin.read_manifest(None).await?.is_none() {
        return Ok(());
    }
    let cps = admin.list_checkpoints(None).await?;
    let name = pairing_name(target);
    for c in cps.iter().filter(|c| c.name.as_deref() == Some(name.as_str())) {
        admin.delete_checkpoint(c.id).await?;
        tenant_bench::fx::pair::delete_pair(&ctl.store, src, &c.id).await?;
    }
    let live: std::collections::HashSet<String> = admin.list_checkpoints(None).await?.into_iter().map(|c| c.id.to_string()).collect();
    let pairs: Vec<Path> = ctl.store.list(Some(&Path::from(format!("{}/pairs", tenant_bench::fx::manifest::root(src))))).map_ok(|m| m.location).try_collect().await?;
    for p in pairs {
        if !live.contains(p.filename().unwrap_or("")) {
            rec::delete(&ctl.store, &p).await?;
        }
    }
    Ok(())
}

/// [`fork`] once the locks and the branch's lease are held: `g` is the entry lock of the source (the caller holds the
/// branch's until after this returns).
async fn fork_leased(ctl: &Arc<Ctl>, mut g: tokio::sync::OwnedRwLockWriteGuard<crate::dbs::Entry>, src: &str, target: &str, mode: &str, list: &Option<Vec<crate::netguard::Net>>, t0: std::time::Instant) -> ApiResult<Value> {
    if !meta::exists(ctl, src).await? {
        return Err(ApiError::not_found(format!("database {src} does not exist")));
    }
    if meta::exists(ctl, target).await? || ops::get(ctl, target).await?.is_some() {
        return Err(ApiError::conflict("exists", format!("database {target} already exists")));
    }
    let before = ctl.usage.today_sum(&[src, target]);
    let ancestors: Vec<String> = purge::ancestors(&ctl.store, src).await?.into_iter().filter_map(|(p, _)| db_of_path(&p)).collect();
    if mode == "copy" && !ancestors.is_empty() {
        return Err(ApiError::conflict("source_is_shallow_fork", format!("database {src} is itself a shallow fork of {ancestors:?}; fork it with mode \"clone\" (or erase its parents' dependency first)")));
    }
    let mut chain: Vec<String> = vec![src.to_string()];
    chain.extend(ancestors);
    ops::begin(ctl, target, "fork", json!({ "source": src, "mode": mode, "ancestors": chain })).await?;
    super::failpoint("fork.after_intent").await;
    let result = do_fork(ctl, &mut g, src, target, mode, &chain, list).await;
    drop(g);
    let source_manifest = match result {
        Ok(m) => m,
        Err(e) => {
            // All or nothing: undo what was done (the branch still locked, its lease still held). If this fails too, the intent record makes
            // the next start do it.
            if let Err(r) = rollback_locked(ctl, target, &json!({ "ancestors": chain })).await {
                eprintln!("fork {src} -> {target}: rollback failed ({r}); it is retried at the next start");
            }
            return Err(e);
        }
    };
    // The intent record is deleted before the branch's entry lock and lease are released (fork()): a crash between the two would otherwise leave a complete
    // branch, maybe with confirmed writes, that the recovery at the next start rolls back (deletes).
    ops::finish(ctl, target).await?;
    super::failpoint("fork.finished").await;
    ctl.usage.touch(src);
    ctl.usage.touch(target);
    let cost = ctl.usage.today_sum(&[src, target]).minus(&before);
    let mut d = meta::describe(ctl, target, true).await?;
    d["fork"] = json!({ "source": src, "mode": mode, "source_manifest_id": source_manifest, "elapsed_ms": t0.elapsed().as_millis() as u64, "cost": cost.to_json() });
    Ok(d)
}

/// The fork itself; the caller holds the entry locks of the source (`g`) and of the branch.
async fn do_fork(ctl: &Arc<Ctl>, g: &mut crate::dbs::Entry, src: &str, target: &str, mode: &str, chain: &[String], list: &Option<Vec<crate::netguard::Net>>) -> ApiResult<Option<u64>> {
    // `list`: the source's allow list, read by fork() (and served for the branch from [`FORKING`] since then).
    quiesce(ctl, g, src).await?;
    // a compaction pass running (maybe a long merge) stops instead of making this wait for it
    let _preempt = tenant_bench::lifecycle::preempt_compaction(src);
    let lock = maint_lock(&ctl.lc.maint_locks, src);
    let _maint = lock.lock().await;
    ctl.dbs.leases.acquire(src).await?;
    let r = async {
        // Dependencies are recorded before the first byte is shared, so a crash cannot leave an unrecorded one.
        if mode == "clone" {
            for a in chain {
                rec::put(&ctl.store, &fork_marker(a, target), &json!({ "fork": target, "source": src, "at_ms": now_ms() })).await?;
            }
        }
        super::failpoint("fork.after_markers").await;
        ops::phase(ctl, target, "copying").await?;
        let manifest_id = if paired() { fork_paired(ctl, src, target, mode).await? } else {
            match mode {
                "clone" => {
                    let admin = Admin::builder(purge::tenant_path(target), Arc::clone(&ctl.store)).build();
                    admin.create_clone_builder_from_source(slatedb::CloneSourceSpec::new(purge::tenant_path(src))).build().await.map_err(anyhow::Error::from)?;
                    Admin::builder(purge::tenant_path(src), Arc::clone(&ctl.store)).build().read_manifest(None).await.map_err(anyhow::Error::from)?.map(|m| m.id())
                }
                _ => {
                    copy_objects(ctl, src, target).await?;
                    None
                }
            }
        };
        super::failpoint("fork.after_clone").await;
        tenant_bench::lifecycle::mark_clean_stored(&ctl.store, target).await?;
        super::failpoint("fork.after_marker").await;
        let fm = json!({ "database": src, "at_ms": now_ms(), "mode": mode });
        let m = meta::default_meta(target, now_ms(), "fork");
        let mut m = m;
        m["forked_from"] = fm;
        // A fresh record (no users, no pin), but never fewer network restrictions than the source: its allow list
        // (read above) is copied (TLS is per listener, so the fork has the same TLS mode).
        if let Some(l) = list {
            m["ip_allowlist"] = json!(l.iter().map(|n| n.to_string()).collect::<Vec<_>>());
        }
        rec::put(&ctl.store, &meta::meta_path(target), &m).await?;
        crate::netguard::get().set_cached(target, list.clone());
        // Like a change of the list (allowlist.rs): a connection that authenticated on the branch from an address
        // outside the list loses that authentication.
        if let Some(l) = list {
            ctl.server.drop_auth_outside(target, l);
        }
        // The source's TTL hints (written at its clean close): the maintenance pass finds the fork's due documents too.
        let hints: Vec<Path> = ctl.store.list(Some(&rec::path(&["ttl-hints", src]))).map_ok(|o| o.location).try_collect().await?;
        for h in hints {
            if let Some(f) = h.filename() {
                ctl.store.copy(&h, &rec::path(&["ttl-hints", target, f])).await?;
            }
        }
        // A resume token of the source (or any token from before the fork) is not a point in the fork's history.
        ctl.dbs.changes.mark_born(target);
        super::failpoint("fork.after_meta").await;
        Ok::<_, anyhow::Error>(manifest_id)
    }
    .await;
    ctl.dbs.leases.release(src).await;
    Ok(r?)
}

/// Forks read through a pair (compaction 2 step 7b; RULES-FOR-THE-CODE rule 20): with `--sealed-runs` on, unless the
/// test control `FENVIX_CONTROL_RAW_PATHS=1` asks for the paths of before. With it off nothing changes: no checkpoint,
/// no extra request.
fn paired() -> bool {
    tenant_bench::fx::ENABLED.load(std::sync::atomic::Ordering::Relaxed) && !tenant_bench::fx::pair::control_raw()
}

/// The fork through a pair (rule 20). Once step 4 drops SlateDB rows at or below X_d, the source's SlateDB alone lacks
/// rows our runs hold, so:
/// 1. a checkpoint of the (quiesced) source, paired with a version under the drop lease (`fx/pair.rs`, rule 18);
/// 2. `clone`: SlateDB's clone of that checkpoint (it adds its own permanent checkpoint in the source); `copy`: a copy of
///    every SlateDB object of the source (quiesced: its latest state is the checkpoint's);
/// 3. the fork's first version of our manifest: the paired version, its runs shared by reference with a fork reference
///    in each database that holds them (`clone`), or copied under the fork (`copy`); E1's announcement copied;
/// 4. the checkpoint of step 1 and its pair are deleted (the clone's own checkpoint keeps SlateDB's objects; the fork
///    reference keeps our runs).
async fn fork_paired(ctl: &Arc<Ctl>, src: &str, target: &str, mode: &str) -> anyhow::Result<Option<u64>> {
    use tenant_bench::fx::pair;
    let store = &ctl.store;
    let src_admin = Admin::builder(purge::tenant_path(src), Arc::clone(store)).build();
    // named, so that the rollback of a fork killed before the cleanup below finds and deletes it (review of 7b, FIX 2)
    let opts = slatedb::config::CheckpointOptions { name: Some(pairing_name(target)), ..Default::default() };
    let (cp, p) = pair::checkpoint_paired(store, src, || async { Ok(src_admin.create_detached_checkpoint(&opts).await?) }).await?;
    let r = async {
        super::failpoint("fork.after_pairing").await;
        if mode == "clone" {
            let admin = Admin::builder(purge::tenant_path(target), Arc::clone(store)).build();
            admin.create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint(purge::tenant_path(src), cp.id)).build().await?;
        } else {
            copy_objects(ctl, src, target).await?;
            // the copy carries the source's checkpoint list; this one is the fork step's own, not a restore point
            let _ = Admin::builder(purge::tenant_path(target), Arc::clone(store)).build().delete_checkpoint(cp.id).await;
        }
        if let Some(p) = &p {
            let runs = if mode == "clone" { pair::ForkRuns::Shared } else { pair::ForkRuns::Copied };
            pair::seed_fork(store, src, target, p, runs).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // the pairing checkpoint goes in every case (a failed fork must not leave it in the source)
    let d = src_admin.delete_checkpoint(cp.id).await;
    pair::delete_pair(store, src, &cp.id).await?;
    r?;
    d?;
    Ok(Some(cp.manifest_id))
}

/// Server-side copy of every data object of `src` to `target` (not the lease or the marker).
async fn copy_objects(ctl: &Arc<Ctl>, src: &str, target: &str) -> anyhow::Result<()> {
    let from = Path::from(purge::tenant_path(src));
    let metas: Vec<object_store::ObjectMeta> = ctl.store.list(Some(&from)).try_collect().await?;
    let pre_from = format!("{}/", purge::tenant_path(src));
    let pre_to = format!("{}/", purge::tenant_path(target));
    // compaction 2: our manifest and runs are not copied here: a copy would read with the parent's newest version, not
    // one paired with the fork's state (RULES-FOR-THE-CODE rule 20, ForkNewestCut). `fork_paired` copies the paired
    // version and its runs (step 7b).
    let fx = format!("{pre_from}fx/");
    let copies = metas.into_iter().filter(|m| !matches!(m.location.filename(), Some("_lease") | Some("_clean") | Some(".deleting")) && !m.location.as_ref().starts_with(&fx)).map(|m| {
        let to = Path::from(format!("{pre_to}{}", m.location.as_ref().strip_prefix(&pre_from).unwrap_or(m.location.as_ref())));
        let store = Arc::clone(&ctl.store);
        async move { store.copy(&m.location, &to).await }
    });
    futures::stream::iter(copies).buffer_unordered(16).try_collect::<Vec<()>>().await?;
    Ok(())
}

/// Undo a fork: release the pins it took, delete its objects, its dependency records and its registry record.
pub async fn rollback(ctl: &Arc<Ctl>, target: &str, op: &Value) -> ApiResult<()> {
    let _g = ctl.dbs.lock(target).await;
    rollback_locked(ctl, target, op).await
}

/// [`rollback`] for a caller that holds the branch's entry lock.
async fn rollback_locked(ctl: &Arc<Ctl>, target: &str, op: &Value) -> ApiResult<()> {
    let chain: Vec<String> = op["ancestors"].as_array().map(|a| a.iter().filter_map(|a| a.as_str().map(|s| s.to_string())).collect()).unwrap_or_default();
    if let Some(src) = op["source"].as_str() {
        if !control("FENVIX_CONTROL_NO_PAIRING_CLEANUP") {
            drop_pairing_leftovers(ctl, src, target).await?;
        }
    }
    let owners = tenant_bench::fx::pair::fork_ref_owners(&ctl.store, target, &chain).await?;
    purge::purge_tenant(ctl, target).await?;
    for a in &chain {
        rec::delete(&ctl.store, &fork_marker(a, target)).await?;
    }
    tenant_bench::fx::pair::drop_fork_refs(&ctl.store, &owners, target).await?;
    rec::delete(&ctl.store, &meta::meta_path(target)).await?;
    ops::finish(ctl, target).await?;
    Ok(())
}

/// Called by the Mongo `dropDatabase` command before it deletes a database's objects: a database that forks still
/// read cannot be dropped, and a fork that is dropped releases the pins it holds in its source. `owner` says whether this
/// process still owns the database: it is checked right before the first change (the reads before it can take long, and
/// a process that lost the database meanwhile must not release the pins of a fork another process now serves).
pub async fn on_drop(db: &str, owner: &(dyn Fn() -> bool + Send + Sync)) -> anyhow::Result<()> {
    let Some(ctl) = super::get() else { return Ok(()) };
    let deps = dependents(ctl, db).await?;
    if !deps.is_empty() {
        anyhow::bail!("database {db} cannot be dropped: databases {deps:?} are forks that still read its data");
    }
    let ancestors = purge::ancestors(&ctl.store, db).await?;
    if !owner() {
        return Err(crate::lease::LeaseHeld { tenant: db.to_string(), holder: "(ownership lost during the drop)".into(), remaining_ms: 0 }.into());
    }
    let names: Vec<String> = ancestors.iter().filter_map(|(p, _)| db_of_path(p)).collect();
    let owners = tenant_bench::fx::pair::fork_ref_owners(&ctl.store, db, &names).await?;
    for a in &names {
        rec::delete(&ctl.store, &fork_marker(a, db)).await?;
    }
    tenant_bench::fx::pair::drop_fork_refs(&ctl.store, &owners, db).await?;
    purge::release_pins(&ctl.store, db).await?;
    Ok(())
}

#[allow(dead_code)]
fn _unused(_: Counts) {}
