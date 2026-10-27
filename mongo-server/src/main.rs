//! A MongoDB wire-protocol server whose databases are slatedb tenants on object storage.
//!
//! Each MongoDB database is one tenant at `tenants/{dbname}` managed by the results3 lifecycle
//! (reader wake, upgrade on first write, clean close after an idle period, `_clean` marker,
//! standalone compaction and GC).

mod agg;
mod cmd;
mod dbs;
mod error;
mod expr;
mod layout;
mod lease;
mod query;
mod regex;
mod update;
mod wire;

use anyhow::Result;
use clap::Parser;
use cmd::Server;
use dbs::Dbs;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tenant_bench::counting::CountingStore;
use tenant_bench::lifecycle::{CompactionQueue, Lifecycle, Maintenance};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:27018")]
    listen: String,
    /// Close a database's tenant after this many idle seconds.
    #[arg(long, default_value_t = 60)]
    idle_secs: u64,
    /// S3-compatible endpoint (MinIO).
    #[arg(long, default_value = "http://localhost:9000")]
    endpoint: String,
    #[arg(long, default_value = "bench")]
    bucket: String,
    /// Do not run the background compaction and GC loops (also disables the L0 guard).
    #[arg(long)]
    no_maintenance: bool,
    /// Tenant lease time-to-live; leases of open tenants are renewed every third of it, and a crashed
    /// process's tenants can be taken over once it has passed.
    #[arg(long, default_value_t = 30)]
    lease_ttl_secs: u64,
    /// Lease holder id (default: a unique id per process start).
    #[arg(long)]
    holder: Option<String>,
    /// WAL flush interval in milliseconds. 0 (default) keeps the baseline: slatedb's 100 ms periodic flush plus an
    /// explicit flush after every write batch, so a write is acknowledged as soon as its WAL object is durable. A
    /// value above 0 sets the periodic interval and drops the explicit flush: a write is acknowledged (durable) at
    /// the next flush, up to one interval later, and concurrent writes to a tenant share one WAL object.
    #[arg(long, default_value_t = 0)]
    wal_flush_ms: u64,
    /// Acknowledge writes before they are durable (they become durable at the next periodic flush). Unsafe: a crash
    /// can lose acknowledged writes. For measuring the durability trade-off only; use with --wal-flush-ms.
    #[arg(long)]
    ack_before_durable: bool,
    /// Object store: `s3` (the endpoint above), `local` (a directory, see --local-dir) or `memory`.
    #[arg(long, default_value = "s3")]
    store: String,
    #[arg(long, default_value = "/tmp/mongo-server-store")]
    local_dir: String,
    /// Cold wakes allowed at once; more queue (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    max_concurrent_wakes: usize,
    /// Test control only: never treat a lease as expired or lost. Demonstrates what the ownership checks prevent.
    #[arg(long)]
    unsafe_skip_ownership_checks: bool,
    /// Run reads of one tenant one at a time like writes (the behaviour before concurrent reads). By default reads of
    /// one tenant share the entry lock and run concurrently; writes and tenant state changes are exclusive.
    #[arg(long)]
    serialize_reads: bool,
    /// Do not use leases (single process only; for stores without conditional update, e.g. --store local).
    #[arg(long)]
    no_leases: bool,
    /// Wait for each write's own flush while holding the tenant lock (the behaviour before group commit: one WAL
    /// object per write). By default writes arriving while a flush is in flight share the next WAL object.
    #[arg(long)]
    no_group_commit: bool,
    /// Seconds between manifest reads of an open writer (it learns of compactions, and retries a memtable flush that
    /// stalled on a full L0, at this pace). One GET per open writer per interval.
    #[arg(long, default_value_t = 30)]
    writer_poll_secs: u64,
    /// One lease per process (`processes/{holder}/_lease`, renewed every third of the ttl) plus a per-database owner
    /// record written when the database is opened, instead of one lease object renewed per open database: an idle open
    /// database then costs no request. Every process sharing the bucket must use the same setting.
    #[arg(long)]
    process_lease: bool,
    /// Seconds between WAL garbage-collection passes over an open writer that is being written (0 = off, WAL objects
    /// then stay until the tenant has been idle and is collected as a whole).
    #[arg(long, default_value_t = 20)]
    wal_gc_secs: u64,
    /// Group commit window in milliseconds (fractions allowed): the first write of a group waits this long before
    /// flushing so that more writes can share the WAL object. 0 (default) flushes at once.
    #[arg(long, default_value_t = 0.0)]
    group_commit_window_ms: f64,
    /// Reads see only durable data (default true). `--durable-reads false`: reads may see writes that are staged
    /// and waiting for their group flush (they can disappear if the process dies before the flush).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    durable_reads: bool,
    /// Test control: slatedb's `l0_max_ssts` for writers (default 8, 0 keeps it). Lowering it makes the stalled-flush
    /// shutdown fallback reachable with a modest number of writes.
    #[arg(long, default_value_t = 0)]
    l0_max_ssts: usize,
    /// How the WAL cleanup of an open writer works: `bulk` (default: remembers where it stopped, bulk deletes),
    /// `single` (the same with one DELETE per object), `slatedb` (slatedb's own pass: lists the whole WAL directory,
    /// one DELETE per object) or `list-only` (that listing without deleting; profiling only, bounds nothing).
    #[arg(long, default_value = "bulk")]
    wal_gc_mode: String,
    /// Seconds between cleanups of the compacted SSTs and old manifests of an open writer (0 = off: they stay until
    /// the tenant closes).
    #[arg(long, default_value_t = 60)]
    sst_gc_secs: u64,
    /// Seconds an SST or manifest must have been seen unreferenced before it is deleted (must exceed
    /// --writer-poll-secs, the staleness of a live writer's view of the manifest).
    #[arg(long, default_value_t = 120)]
    sst_gc_min_unref_secs: u64,
    /// Test control, unsafe: the SST cleanup also deletes the oldest SST the manifest still references.
    #[arg(long)]
    gc_control_delete_live: bool,
    /// Test control: memtable size at which a writer flushes it to an L0 SST (slatedb default 64 MiB; 0 keeps it).
    #[arg(long, default_value_t = 0)]
    l0_sst_bytes: u64,
    /// With --group-commit-window-ms: wait for the window only while another write of the tenant is still on its way
    /// to the flush; flush at once when none is (so a lone writer never waits). Without it the window is always waited.
    #[arg(long)]
    group_commit_adaptive: bool,
    /// Log every command name and database to stderr.
    #[arg(long)]
    log_commands: bool,
}

async fn handle_connection(server: Arc<Server>, stream: TcpStream, conn_id: i64) {
    let _ = stream.set_nodelay(true);
    let (r, mut w) = stream.into_split();
    let mut r = BufReader::new(r);
    let mut response_id = 1;
    loop {
        match wire::read_request(&mut r).await {
            Ok(Some(req)) => {
                let reply = server.handle(&req.db, req.command.clone(), conn_id).await;
                if req.more_to_come {
                    continue;
                }
                response_id += 1;
                if let Err(e) = wire::write_reply(&mut w, &req, &reply, response_id).await {
                    eprintln!("conn {conn_id}: write failed: {e:#}");
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("conn {conn_id}: {e:#}");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let s3: Arc<dyn ObjectStore> = match args.store.as_str() {
        "s3" => Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(&args.endpoint)
                .with_allow_http(true)
                .with_region("us-east-1")
                .with_bucket_name(&args.bucket)
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .build()?,
        ),
        "local" => {
            std::fs::create_dir_all(&args.local_dir)?;
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&args.local_dir)?)
        }
        "memory" => Arc::new(object_store::memory::InMemory::new()),
        other => anyhow::bail!("unknown --store {other}"),
    };
    tenant_bench::lifecycle::FLUSH_INTERVAL_MS.store(args.wal_flush_ms, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::ACK_BEFORE_DURABLE.store(args.ack_before_durable, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::EXPLICIT_FLUSH.store(args.wal_flush_ms == 0, std::sync::atomic::Ordering::Relaxed);
    dbs::set_wake_limit(args.max_concurrent_wakes);
    tenant_bench::lifecycle::WRITER_POLL_SECS.store(args.writer_poll_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::HOT_GC_EVERY_SECS.store(args.wal_gc_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GROUP_WINDOW_US.store((args.group_commit_window_ms * 1000.0) as u64, std::sync::atomic::Ordering::Relaxed);
    dbs::DURABLE_READS.store(args.durable_reads, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::L0_MAX_SSTS_OVERRIDE.store(args.l0_max_ssts, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::WAL_GC_MODE.store(
        match args.wal_gc_mode.as_str() { "slatedb" => 0, "list-only" => 1, "single" => 2, "bulk" => 3, other => anyhow::bail!("unknown --wal-gc-mode {other}") },
        std::sync::atomic::Ordering::Relaxed,
    );
    tenant_bench::lifecycle::SST_GC_EVERY_SECS.store(args.sst_gc_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::SST_GC_MIN_UNREF_SECS.store(args.sst_gc_min_unref_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GC_CONTROL_DELETE_LIVE.store(args.gc_control_delete_live, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::L0_SST_BYTES_OVERRIDE.store(args.l0_sst_bytes, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GROUP_ADAPTIVE.store(args.group_commit_adaptive, std::sync::atomic::Ordering::Relaxed);
    dbs::GROUP_COMMIT.store(!args.no_group_commit, std::sync::atomic::Ordering::Relaxed);
    dbs::SHARED_READS.store(!args.serialize_reads, std::sync::atomic::Ordering::Relaxed);
    // One counter for every request this process makes (lifecycle, leases, maintenance, listings).
    let global = Arc::new(CountingStore::new(s3));
    let s3: Arc<dyn ObjectStore> = global.clone();
    let counting = Arc::new(CountingStore::new(Arc::clone(&s3)));
    let (queue, rx) = CompactionQueue::new();
    let lc = if args.no_maintenance {
        Lifecycle::with_guard(counting, None, false)
    } else {
        Lifecycle::new(counting, Some(Arc::clone(&queue)))
    };
    let maintenance = (!args.no_maintenance).then(|| {
        Maintenance::start(Arc::clone(&s3), Arc::clone(&s3), queue, rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 2, Duration::from_secs(30), Duration::from_secs(15))
    });
    let holder = args.holder.clone().unwrap_or_else(|| format!("{}-{:08x}", std::process::id(), rand::random::<u32>()));
    let leases = lease::Leases::new(Arc::clone(&lc.store), holder.clone(), Duration::from_secs(args.lease_ttl_secs));
    leases.process_mode.store(args.process_lease, std::sync::atomic::Ordering::Relaxed);
    leases.disabled.store(args.no_leases, std::sync::atomic::Ordering::Relaxed);
    leases.unsafe_skip.store(args.unsafe_skip_ownership_checks, std::sync::atomic::Ordering::Relaxed);
    let dbs = Dbs::new(lc, Arc::clone(&s3), Duration::from_secs(args.idle_secs), Arc::clone(&leases), global);
    dbs.spawn_reaper();
    let (lost_tx, lost_rx) = tokio::sync::mpsc::unbounded_channel();
    leases.spawn_renewer(lost_tx);
    dbs.spawn_lost_handler(lost_rx);
    let server = Server::new(Arc::clone(&dbs), args.log_commands);

    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!("mongo-server listening on {} (idle close after {} s, holder {holder}, lease ttl {} s)", args.listen, args.idle_secs, args.lease_ttl_secs);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let server = Arc::clone(&server);
                let id = server.new_connection_id();
                tokio::spawn(handle_connection(server, stream, id));
            }
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
        }
    }
    eprintln!("shutting down: closing tenants cleanly");
    dbs.close_all().await;
    leases.release_process().await;
    eprintln!("tenants closed; stopping maintenance");
    if let Some(m) = maintenance {
        m.shutdown().await;
    }
    eprintln!("maintenance stopped");
    Ok(())
}
