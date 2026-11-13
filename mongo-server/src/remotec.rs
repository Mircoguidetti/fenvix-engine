//! P-B3 (prototype): `mongo-server --compactor-only DB`, the compactor of one database outside its waiter (tenant_bench
//! `fx/remote.rs` has the protocol). The supervisor:
//! 1. waits until the database has a version of our manifest and is caught up (the waiter, run with
//!    `--sealed-remote-compactor`, publishes the first version and the catch-up, then follows the store);
//! 2. takes the database's compactor claim (`Claim::acquire`);
//! 3. starts the compactor on the store (`Compactor::start_remote`) and renews the claim every lease / 6;
//! 4. stops the compactor when the claim is lost or its renewal fails past the deadline, and starts a new one on the
//!    newest version when the compactor stopped by itself (a newer writer's version: rule 28) while the claim holds;
//! 5. on SIGTERM or Ctrl-C stops the compactor and releases the claim.
//! Customer keys come from `--customer-key` / `--agent-checkpoints` (a waiter also reads them from the stored collection
//! options; this process has no catalog).
use anyhow::Result;
use object_store::ObjectStore;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tenant_bench::fx::{compactor, manifest, pool, remote};

fn note_keys(db: &str) {
    for coll in crate::agentck::registered(db) {
        let key = crate::custkey::configured(db, &coll);
        crate::agentck::note_key(db, &coll, key.as_ref().map(|k| k.field.as_str()));
    }
}

fn line(c: &compactor::Compactor, claim: &remote::Claim) -> String {
    format!(
        "db={} claim={} publishes={} absorbs={} merges={} reseals={} errors={} l0={} slate_reads={} past_deadline={}",
        c.id(),
        claim.doc().claim,
        compactor::G_PUBLISHES.load(Relaxed),
        compactor::G_ABSORBS.load(Relaxed),
        compactor::G_MERGES.load(Relaxed),
        compactor::G_RESEALS.load(Relaxed),
        compactor::G_ERRORS.load(Relaxed),
        c.l0_count(),
        c.slate_reads(),
        remote::PAST_DEADLINE.load(Relaxed)
    )
}

pub async fn run(store: Arc<dyn ObjectStore>, db: &str, holder: &str, lease: Duration, margin: Duration) -> Result<()> {
    anyhow::ensure!(margin > Duration::from_millis(tenant_bench::fx::PUT_TIMEOUT_MS.load(Relaxed)), "--compactor-claim-margin-ms must exceed the manifest PUT timeout (F6)");
    note_keys(db);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    eprintln!("compactor-only: database {db}, holder {holder}, claim lease {lease:?}, margin {margin:?}");
    'outer: loop {
        // 1. a version and the catch-up mark, under the store's current incarnation
        let id = loop {
            let inc = tenant_bench::lifecycle::read_incarnation(&store, db).await?;
            let id = tenant_bench::lifecycle::DbAt::new(db, inc);
            let ready = manifest::load_newest(&store, &id).await?.is_some() && !manifest::needs_catch_up(&store, &id).await?;
            if ready {
                break id;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = tokio::signal::ctrl_c() => return Ok(()),
                _ = term.recv() => return Ok(()),
            }
        };
        // 2. the claim
        let claim = tokio::select! {
            c = remote::Claim::acquire(&store, &id, holder, lease, margin, Duration::from_secs(365 * 86400)) => c?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = term.recv() => return Ok(()),
        };
        eprintln!("compactor-only: claim {} of {id} taken", claim.doc().claim);
        // 3. compactors, one after the other, while the claim holds
        loop {
            let mut k = compactor::server_knobs();
            k.derive = crate::agentck::spec_for(db);
            let c = match compactor::Compactor::start_remote(Arc::clone(&store), id.clone(), k, pool::global(), pool::compaction_runtime(), Arc::clone(&claim), Duration::from_millis(200)).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("compactor-only: start on {id}: {e:#}; again in 2 s");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if !claim.valid() {
                        claim.release().await;
                        continue 'outer;
                    }
                    continue;
                }
            };
            c.run();
            eprintln!("compactor-only: compactor of {id} started on version {}", c.current().await.version);
            let mut last_line = Instant::now();
            let why = loop {
                tokio::select! {
                    _ = tokio::time::sleep(lease / 6) => {}
                    _ = tokio::signal::ctrl_c() => break "stop",
                    _ = term.recv() => break "stop",
                }
                if let Err(e) = claim.renew().await {
                    eprintln!("compactor-only: renewal of the claim of {id}: {e:#}");
                }
                if claim.lost() || !claim.valid() {
                    break "claim";
                }
                if c.stopped.load(Relaxed) {
                    break "restart";
                }
                if tenant_bench::lifecycle::read_incarnation(&store, db).await.ok() != Some(id.inc) {
                    break "incarnation";
                }
                if last_line.elapsed() >= Duration::from_secs(10) {
                    last_line = Instant::now();
                    eprintln!("compactor-only: {}", line(&c, &claim));
                }
            };
            eprintln!("compactor-only: stopping the compactor of {id} ({why}): {}", line(&c, &claim));
            c.stop().await;
            match why {
                "stop" => {
                    claim.release().await;
                    return Ok(());
                }
                "restart" => continue,
                _ => {
                    claim.release().await;
                    continue 'outer;
                }
            }
        }
    }
}
