//! A MongoDB wire-protocol server whose databases are slatedb tenants on object storage.
//!
//! Each MongoDB database is one tenant at `tenants/{dbname}` managed by the results3 lifecycle
//! (reader wake, upgrade on first write, clean close after an idle period, `_clean` marker,
//! standalone compaction and GC).

mod agg;
mod agg_ext;
mod changes;
mod cmd;
mod cmd_changes;
mod coll;
mod cmd_data;
mod cmd_index;
mod dbs;
mod error;
mod expr;
mod index;
mod js;
mod keyenc;
mod layout;
mod lease;
mod query;
mod regex;
mod ttl;
mod txn;
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
    /// Remove expired TTL documents of tenants that are open as writers this often.
    #[arg(long, default_value_t = 60)]
    ttl_sweep_secs: u64,
    /// Every this often, look for sleeping tenants with expired documents (one LIST) and wake due ones.
    #[arg(long, default_value_t = 60)]
    ttl_maintenance_secs: u64,
    /// Do not wake the same sleeping tenant for expiry more often than this.
    #[arg(long, default_value_t = 3600)]
    ttl_min_gap_secs: u64,
    /// At most this many sleeping tenants are woken for expiry per maintenance pass.
    #[arg(long, default_value_t = 20)]
    ttl_max_wakes: usize,
    /// Replica-set name reported by `hello`. Drivers allow multi-document transactions and change streams only on
    /// replica sets, so set it (with `--advertise`) when an application needs them; empty (default) is a standalone
    /// server, which is what clients that connect by another host name or address expect.
    #[arg(long, default_value = "")]
    replica_set: String,
    /// Address advertised in `hello.hosts` / `me`; must match the host:port clients connect to
    /// (default `localhost:<listen port>`).
    #[arg(long)]
    advertise: Option<String>,
    /// Do not run the TTL sweeps (expired documents stay hidden but are not removed).
    #[arg(long)]
    no_ttl: bool,
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
    let s3: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(&args.endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(&args.bucket)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .build()?,
    );
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
    let dbs = Dbs::new(lc, Arc::clone(&s3), Duration::from_secs(args.idle_secs), Arc::clone(&leases), global);
    dbs.spawn_reaper();
    let (lost_tx, lost_rx) = tokio::sync::mpsc::unbounded_channel();
    leases.spawn_renewer(lost_tx);
    dbs.spawn_lost_handler(lost_rx);
    if !args.no_ttl {
        dbs.spawn_ttl_session_sweeper(Duration::from_secs(args.ttl_sweep_secs));
        dbs.spawn_ttl_maintenance(Duration::from_secs(args.ttl_maintenance_secs), Duration::from_secs(args.ttl_min_gap_secs), args.ttl_max_wakes);
    }
    let server = Server::new(Arc::clone(&dbs), args.log_commands);
    if !args.replica_set.is_empty() {
        let port = args.listen.rsplit(':').next().unwrap_or("27017");
        *server.rs.lock().unwrap() = Some((args.replica_set.clone(), args.advertise.clone().unwrap_or_else(|| format!("localhost:{port}"))));
    }
    server.spawn_txn_janitor();

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
    if let Some(m) = maintenance {
        m.shutdown().await;
    }
    Ok(())
}
