#![recursion_limit = "256"] // integration-4: the serverStatus doc! macro grew past the default 128 when both merged counters were kept
//! A MongoDB wire-protocol server whose databases are slatedb tenants on object storage.
//!
//! Each MongoDB database is one tenant at `tenants/{dbname}` managed by the results3 lifecycle
//! (reader wake, upgrade on first write, clean close after an idle period, `_clean` marker,
//! standalone compaction and GC).

/// jemalloc instead of the system allocator (mem-growth branch). Measured with 50 databases at 2 writes/s on MinIO: glibc's
/// malloc held ~5.5 KB of RSS per write for ~1.5 KB of live heap and gave nothing back when the databases closed;
/// jemalloc held ~1.9 KB per write. `background_thread` returns freed pages to the system while the process is idle
/// (without it, pages were only purged by later allocations).
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
// P-big-speed fix 1 (review round, from the session's fallback-memory-bounds.patch): freed pages go back to the system
// within 1 s (jemalloc's default keeps dirty pages 10 s: under the load at 1 GiB about 220 MB more resident than
// allocated, MEASURED by the big-speed session)
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

mod uploads;
mod agg;

/// `--sealed-runs` when neither the flag nor FENVIX_SEALED_RUNS says otherwise (integration-3: sealed runs on).
const DEFAULT_SEALED_RUNS: u64 = 2;
mod ballast;
mod agentlane;
mod agentck;
mod custkey;
mod layout2;
mod opening;
mod custbox;
mod prefetch;
mod hostinfo;
mod auth;
mod authz;
mod scram;
mod tls;
mod agg_ext;
mod agg_more;
mod json_schema;
mod capped;
mod capped_cmds;
mod changes;
mod cmd;
mod cmd_changes;
mod collation;
mod collmod_index;
mod collopts;
mod collstat;
mod coll;
mod cmd_data;
mod cmd_index;
mod cmd_mapreduce;
mod ctl;
mod cs_events;
mod dbs;
mod diag;
mod fields;
mod find_ext;
mod dsum;
mod error;
mod fair;
mod expr;
mod index;
mod js;
mod keyenc;
mod layout;
mod lease;
mod localcas;
mod faults;
mod locallog;
mod qlog;
mod natural;
mod netguard;
mod params;
mod projfind;
mod query;
mod geo;
mod geo_query;
mod rc;
mod regex;
mod retry;
mod search;
mod special_colls;
mod stable_api;
mod stats_scale;
mod search_more;
mod text;
mod wild;
mod ttl;
mod txn;
mod update;
mod validation;
mod views;
mod admin_cmds;
mod repl_cmds;
mod search_idx;
mod wc;
mod wire;
mod xdb;
mod uploads_sweep;

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
use bson::Bson;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:27018")]
    listen: String,
    /// Close a database's tenant after this many idle seconds.
    #[arg(long, default_value_t = 60)]
    idle_secs: u64,
    /// S3 endpoint URL, only for S3-compatible services such as MinIO (also read from AWS_ENDPOINT). Empty: AWS itself.
    /// An `http://` endpoint switches plain HTTP on.
    #[arg(long, default_value = "")]
    endpoint: String,
    /// S3 region (also read from AWS_REGION or AWS_DEFAULT_REGION). With `--store s3` one of the three must be set: the engine
    /// has no region of its own.
    #[arg(long)]
    region: Option<String>,
    #[arg(long, default_value = "bench")]
    bucket: String,
    /// Do not run the background compaction and GC loops (also disables the L0 guard).
    #[arg(long)]
    no_maintenance: bool,
    /// Tenant lease time-to-live; leases of open tenants are renewed every third of it, and a crashed
    /// process's tenants can be taken over once it has passed.
    #[arg(long, default_value_t = 30)]
    lease_ttl_secs: u64,
    /// SIGTERM: the clean close of every database (memtable flush, lease release) gets at most this long; then the
    /// process exits without it (fix-store-errors: a store that refuses or does not answer writes kept the close waiting
    /// for minutes). Nothing acknowledged is lost: an acknowledged write is in a WAL object already, and the next owner
    /// replays it once the leases run out.
    #[arg(long, default_value_t = 25)]
    shutdown_timeout_secs: u64,
    /// P-fix-slow-store item 1: SIGTERM's close of the databases ends this long after the signal (at most
    /// `--shutdown-timeout-secs`). Every step of every close shares this deadline: the optional ones (the compactor's
    /// absorption of the last flush) get at most half of what is left, a flush or SlateDB close that does not fit is cut
    /// into an unclean close (no marker: the next owner replays the WAL, where every acknowledged write is), and a lease
    /// is released only by a close whose SlateDB writer stopped. Then the upload aborts, the usage counters and the
    /// process lease get what is left plus a short grace. Before, a close waited on a slow store step by step: 8.7 s at
    /// 200 ms per request and the whole 25 s at 1 s (MEASURED), and the process then exited with leases unreleased.
    #[arg(long, default_value_t = 8)]
    close_budget_secs: u64,
    /// P-fix-slow-store item 3: a single-document write (an insert, a findAndModify, an update or delete of one document,
    /// outside a transaction) not acknowledged within this many milliseconds is answered with error 262 ExceededTimeLimit;
    /// a retryable write carries the `RetryableWriteError` label, so the driver sends it again (deduplicated by its
    /// session record). The command itself goes on: nothing is cut, and nothing not on 2 of 3 log copies (or in a WAL
    /// object) is ever acknowledged. Before, such a write hung its client for as long as the store did. 0: no deadline
    #[arg(long, default_value_t = 30_000)]
    write_deadline_ms: u64,
    /// Lease holder id (default: a unique id per process start).
    #[arg(long)]
    holder: Option<String>,
    /// Remove expired TTL documents of tenants that are open as writers this often.
    #[arg(long, default_value_t = 60)]
    ttl_sweep_secs: u64,
    /// Wake sleeping tenants to remove their expired documents (off by default). Off: expired documents are hidden on
    /// every read and removed at the tenant's next natural writer session; nothing is woken, no expiry hint is
    /// written at close and no maintenance pass lists anything. On: a maintenance pass (one LIST per pass) wakes
    /// tenants whose documents are due, each at most once per `--ttl-min-gap-secs`.
    #[arg(long)]
    ttl_wake_sleeping: bool,
    /// With `--ttl-wake-sleeping`: every this often, look for sleeping tenants with expired documents and wake due ones.
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
    /// Cold wakes allowed at once; more queue (0 = unlimited). Measured with 500 databases waking together after a restart
    /// (MinIO, 2 CPUs, results-round3.md): first request p50/p99 2.7/16.4 s with 8, 3.9/12.1 s with 32, 6.1/8.6 s with 128,
    /// 6.3/9.5 s without a limit. 128 gives the shortest tail, which is what a customer waits for.
    #[arg(long, default_value_t = 128)]
    max_concurrent_wakes: usize,
    /// Memtable size in MiB (slatedb's own estimate of the data) at which a writer flushes it to an L0 SST (slatedb's default
    /// is 64 MiB per database). The process memory a memtable holds is about 4 times this estimate for 1 KB documents
    /// (results-round3.md). 2 bounded the memory better but, on real S3, a database at 100 writes/s then flushed L0 so
    /// often that its writes stalled on compaction (p99 2-3 s, request cost doubled); 8 had no such stall (results-round3.md,
    /// part E). 0 = slatedb's default.
    /// Default: 8, or less under a small memory limit (`membudget.rs`).
    #[arg(long)]
    memtable_max_mb: Option<u64>,
    /// A writer flushes its memtable to an L0 SST once its oldest unflushed write is this old, in seconds (0 = only when the
    /// memtable is full or after slatedb's 4096 WAL flushes). Measured (mem-growth branch, 50 databases at 2 writes/s): the
    /// memtables of slowly written databases were most of the memory that grew with the writes. A database written fast
    /// fills its memtable before this age and is not affected; a slow one gets at most one more L0 SST per interval.
    #[arg(long, default_value_t = 600)]
    memtable_max_age_secs: u64,
    /// Also keep the data blocks of the L0 SSTs a writer flushes in the shared block cache (slatedb's default). Off: those
    /// blocks filled the cache with data nobody had read and, after the database closed, could never be read again (new
    /// cache scope per open). Blocks are still cached when read.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    cache_flushed_blocks: bool,
    /// After a compaction pass of an open database, read into the shared block cache the new blocks that hold the keys of
    /// the blocks it had cached, before the database switches to the new manifest (`src/warm.rs`). Off: every compaction
    /// cooled the cache, the hot reads went back to object storage (M27: 0% of the hot reads served by the cache after a pass).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compaction_warm: bool,
    /// Longest warm-up after one compaction pass, in milliseconds; past it the warm-up stops where it is (what it read
    /// stays in the cache) and the database switches to the new manifest anyway.
    #[arg(long, default_value_t = 3000)]
    compaction_warm_ms: u64,
    /// Warm-up reads in flight at once per warm-up (`warm_sst` calls of 2 range GETs each). The rate of all warm-ups
    /// together is bounded by `--compaction-warm-gets-per-sec`.
    #[arg(long, default_value_t = 128)]
    compaction_warm_concurrency: u64,
    /// Most MB of hot keys one warm-up keeps in memory; past it only a part of the hot blocks is warmed.
    #[arg(long, default_value_t = 32)]
    compaction_warm_max_key_mb: u64,
    /// Most warm-up GETs per second for the whole process, all compaction warm-ups together (one shared token bucket; 0 =
    /// no limit). Under S3's 5,500 GET/s of one partition with room for the other requests of the same prefixes.
    #[arg(long, default_value_t = 3000)]
    compaction_warm_gets_per_sec: u64,
    /// A cursor (find, aggregate, tailable, change stream) that no getMore used for this long is closed and its buffered
    /// results freed, as mongod's `cursorTimeoutMillis` (10 minutes). Before, an abandoned cursor stayed until the process
    /// stopped. 0 = never.
    #[arg(long, default_value_t = 600)]
    cursor_timeout_secs: u64,
    /// One block cache shared by every open database, in MiB (slatedb's default is a 512 MiB cache per open database, with no
    /// limit across databases: the memory then grew with the data read by each database).
    /// Default: 45% of the caches' share of the memory limit (`membudget.rs`; 0 = none).
    #[arg(long)]
    block_cache_mb: Option<u64>,
    /// One metadata cache (SST indexes and filters) shared by every open database, in MiB (slatedb's default: 128 MiB per database).
    /// Default: 15% of the caches' share of the memory limit (`membudget.rs`; 0 = none).
    #[arg(long)]
    meta_cache_mb: Option<u64>,
    /// The memory the process may use, in MiB, instead of the one detected (the smallest of MemTotal and the cgroup v1
    /// or v2 limit). Every cache and buffer is sized from it (`membudget.rs`, P-big-speed fix 1).
    #[arg(long, default_value_t = 0)]
    memory_limit_mb: u64,
    /// The waiter's tools (T1): the two-floor counter (RAM + local disk) with the maps pinned (SST indexes and filters,
    /// collection indexes, metadata), one trip per cold read, read-ahead on a signal. Replaces --block-cache-mb and
    /// --meta-cache-mb. Off by default until reviewed.
    #[arg(long, default_value_t = false)]
    waiter_tools: bool,
    /// RAM of the counter, in percent of the memory limit (--waiter-tools). Default: the block and metadata caches'
    /// share of the memory budget (`membudget.rs`).
    #[arg(long)]
    counter_ram_pct: Option<u64>,
    /// RAM of the counter in MiB, instead of the percentage (0 = use the percentage).
    #[arg(long, default_value_t = 0)]
    counter_ram_mb: u64,
    /// Directory of the counter's disk floor (local NVMe). Empty = no disk floor. Emptied at start.
    #[arg(long, default_value = "")]
    counter_disk_dir: String,
    /// Size of the disk floor in GiB (0 = 80% of the free space of its file system at start).
    #[arg(long, default_value_t = 0)]
    counter_disk_gb: u64,
    /// Ranged GETs at least this long (KiB) are sent as parallel pieces (0 = never; --waiter-tools).
    #[arg(long, default_value_t = 512)]
    split_get_min_kb: u64,
    /// Size of one piece of a split GET, KiB.
    #[arg(long, default_value_t = 256)]
    split_get_piece_kb: u64,
    /// Hedged reads (--waiter-tools): a ranged GET not answered after this many ms is sent again, first answer wins
    /// (0 = off).
    #[arg(long, default_value_t = 0)]
    hedge_after_ms: u64,
    /// Bloom filter bits per key of new SSTs (slatedb's default 10). More bits, fewer false positives: a cold point read
    /// then waits for one read, not two in a row (T1).
    #[arg(long, default_value_t = 10)]
    filter_bits_per_key: u32,
    /// Next-block read-along (--waiter-tools): a small ranged GET of an SST also brings this many KiB after it (T1:
    /// slatedb reads the block before a key that starts a block first; this keeps both in one trip). 0 = off.
    #[arg(long, default_value_t = 8)]
    next_block_kb: u64,
    /// Read ahead on a signal (T1, prefetch.rs): the first indexed read of a key prefix brings this many more entries'
    /// documents in the background, and a read that goes past them brings the next ones. 0 = off.
    #[arg(long, default_value_t = 200)]
    read_ahead_docs: usize,
    /// Memory the change-stream event log may hold, in MiB (estimated). Older events are dropped first; a resume token older than
    /// the oldest event kept fails with ChangeStreamHistoryLost. The log keeps whole documents, so without this limit every
    /// write grew the process by ~4 KB until 100 000 events were held.
    /// Default: 1/32 of the memory limit, at most 32 (`membudget.rs`).
    #[arg(long)]
    change_log_max_mb: Option<usize>,
    /// Relaxed durability for databases without their own setting (`PUT /v1/databases/{db}/durability`), in milliseconds:
    /// 0 (default) = off, otherwise 1000 to 10000. A write with write concern `{w: 1, j: false}` to such a database is
    /// acknowledged before it is durable and becomes durable within this interval (lost if the process dies first);
    /// `j: true`, `w: "majority"` and writes without a write concern stay durable before the acknowledgement.
    #[arg(long, default_value_t = 0)]
    relaxed_flush_ms: u64,
    /// Adaptive group commit: the first write of a group waits a short window (half the database's average flush time, at
    /// most --adaptive-group-commit-max-us) when more writes of the same database are expected within it, so they share
    /// one WAL object. A database written rarely never waits.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    adaptive_group_commit: bool,
    #[arg(long, default_value_t = 5000)]
    adaptive_group_commit_max_us: u64,
    /// PROTOTYPE, off by default: a durable log in this local directory. Writes are acknowledged after the log's fsync and
    /// reach object storage in blocks every --local-log-upload-secs; a restart applies the records not yet uploaded. Losing
    /// the disk loses up to that interval of acknowledged writes of every database. See locallog.rs.
    #[arg(long, default_value = "")]
    local_log_dir: String,
    #[arg(long, default_value_t = 30)]
    local_log_upload_secs: u64,
    /// Q8d, off by default: the replicated log (quaderno, proto/quaderno) as the durability of every write, at these
    /// three copies (`a:p,b:p,c:p`). A write is acknowledged when 2 copies of 3 have its record; object storage gets it
    /// every --local-log-upload-secs (1 to 60 here), then the log drops it. A restart (on any machine) reads back from the
    /// log what object storage does not have yet. --local-log-dir, if set, only keeps conflicting records. See qlog.rs.
    #[arg(long, default_value = "")]
    quaderno: String,
    /// Name of the log in the markers stored with the data (the same for every engine that uses this log).
    #[arg(long, default_value = "cell1")]
    quaderno_id: String,
    /// PROTOTYPE, off by default: collections on the agent lane, comma-separated `database.collection` patterns (`*` matches
    /// any run of characters), e.g. `*.checkpoints,*.checkpoint_writes`. Their writes skip the local and replicated log and
    /// are acknowledged once the WAL object holding them is in object storage. A collection can also be put on the lane
    /// with `collMod` / `create` and the option `fenvixAgentLane: true`. See agentlane.rs.
    #[arg(long, default_value = "")]
    agent_lane: String,
    /// Agent lane: the batch window (ms). The first write of a batch waits this long for others to share its PUT;
    /// a transaction's commit does not wait.
    #[arg(long, default_value_t = 100)]
    agent_lane_window_ms: u64,
    /// Agent lane: follow these databases' WAL objects in object storage (comma-separated names), as a warm spare does,
    /// so that a takeover replays the last batches from memory instead of reading them one by one. See agentlane.rs.
    #[arg(long, default_value = "")]
    agent_lane_follow: String,
    /// Local log: the restart gives up on replaying one database after this long (its records are kept for the next
    /// restart) so that one stuck database (its S3 requests hanging or retried) does not keep the whole server down.
    #[arg(long, default_value_t = 120)]
    local_log_replay_timeout_secs: u64,
    /// Fair queue between databases, only while the server is saturated: when the runtime runs a 10 ms timer late by more
    /// than this (moving average, ms), new commands wait in their database's queue and start in turn (wakes first) as
    /// others finish; below it every command starts at once. 0 = off. See fair.rs.
    #[arg(long, default_value_t = 2.0)]
    fair_lag_ms: f64,
    /// While saturated (see --fair-lag-ms), at most this many commands run at once; the others wait in turn (0 = 8 per CPU).
    #[arg(long, default_value_t = 0)]
    fair_max_inflight: u64,
    /// While saturated, at most this many commands at once have their written data flushed (the flush is CPU work in the
    /// database's background tasks); the others wait in turn, least used databases first (0 = 4 per CPU).
    #[arg(long, default_value_t = 0)]
    fair_max_flushing: u64,
    /// Safety limit per database, only against runaway clients: commands running or queued at once (0 = none).
    #[arg(long, default_value_t = 10_000)]
    db_max_inflight: u64,
    /// Safety limit per database, only against runaway clients: commands in one second (0 = none).
    #[arg(long, default_value_t = 100_000)]
    db_max_rate: u64,
    /// Quota per database in points per second (the unit of fair.rs: CPU, bytes written and read, S3 requests), for
    /// databases without their own (PUT /v1/databases/{db}/quota). Over it a database's commands wait in its own queue. 0 = none.
    #[arg(long, default_value_t = 0.0)]
    db_quota_pts: f64,
    /// Burst credit per database in points: how far a database may go over its quota for a while (0 = 10 s of quota).
    #[arg(long, default_value_t = 0.0)]
    db_burst_pts: f64,
    /// Commands of one database that may wait over its quota; beyond, a new one gets the retryable error 462 at once.
    #[arg(long, default_value_t = 256)]
    db_quota_queue: u64,
    /// Weights of the unit, points per: CPU ms, KiB written, KiB read, S3 PUT-class request, S3 GET-class request (empty = defaults, see fair.rs).
    #[arg(long, default_value = "")]
    unit_weights: String,
    /// Longest total time one command may spend running JavaScript ($where, $function, $accumulator, mapReduce), in ms; over it the command fails with error 50 MaxTimeMSExpired. 0 = no limit (each call still has internalQueryJavaScriptFnTimeoutMillis).
    #[arg(long, default_value_t = 30_000)]
    js_command_max_ms: u64,
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
    /// stalled on a full L0, at this pace). One LIST and one GET per open writer per interval: 60 s is ~86 000 requests
    /// a month per database open for writing (30 s, the old default, twice that). Must stay below --sst-gc-min-unref-secs.
    #[arg(long, default_value_t = 60)]
    writer_poll_secs: u64,
    /// One lease per process (`processes/{holder}/_lease`, renewed every third of the ttl) plus a per-database owner
    /// record written when the database is opened, instead of one lease object renewed per open database (6 PUT per minute
    /// for every open database with the default ttl): an open database then costs no lease request. On by default;
    /// `--process-lease false` goes back to one lease per database. Every process sharing the bucket should use the same
    /// setting (a mix is safe, see lease.rs: each mode refuses the other's live records).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
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
    /// Keep the collection list, index definitions and collection options of open databases in memory (default true; see
    /// `dbs::META_CACHE`). `--metadata-cache false` reads them from storage on every command, as before.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    metadata_cache: bool,
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
    #[command(flatten)]
    ctl: ctl::CtlArgs,
    #[command(flatten)]
    net: netguard::NetArgs,
    /// Require authentication and authorization for every command (mongod's --auth).
    #[arg(long)]
    auth: bool,
    /// With --auth: the only roles a caller that holds neither root nor clusterAdmin (a provisioner, a customer's user
    /// administrator) may hand out with createUser, updateUser and grantRolesToUser, comma-separated, e.g. `readWrite,read`.
    /// When set, such a caller may manage users only on customer databases (never admin, local, config, $external), may
    /// grant only the listed built-in roles of the database the user is defined on, never one of its own roles, and may not
    /// create or change roles. Only built-in roles of one database without user administration are accepted (read,
    /// readWrite, dbAdmin). Empty: no restriction (mongod's rules).
    #[arg(long, default_value = "")]
    grantable_roles: String,
    /// TLS on the client port: disabled, allowTLS, preferTLS or requireTLS (mongod's net.tls.mode).
    #[arg(long, default_value = "disabled")]
    tls_mode: String,
    /// PEM file with the server certificate chain and its private key (net.tls.certificateKeyFile).
    #[arg(long, default_value = "")]
    tls_certificate_key_file: String,
    /// PEM file with the CA certificates client certificates must chain to (net.tls.CAFile).
    #[arg(long, default_value = "")]
    tls_ca_file: String,
    /// With --tls-ca-file: accept clients that present no certificate (net.tls.allowConnectionsWithoutCertificates).
    #[arg(long)]
    tls_allow_connections_without_certificates: bool,
    /// TLS session resumption: cache (rustls's default: 256 sessions kept in memory), tickets (stateless TLS 1.3 tickets,
    /// nothing kept) or off.
    #[arg(long, default_value = "cache")]
    tls_session_resumption: String,
    /// Customer keys and layout v2 (custkey.rs): comma separated `[db.]collection=field[:timefield]`; a named collection
    /// takes the customer key when it is created, first indexed or first written while empty. Besides these, every
    /// `--agent-checkpoints` collection is keyed by `thread_id` unless named here (since integration-4).
    #[arg(long, default_value = "")]
    customer_key: String,
    /// Envelope values (envelope.rs): `read` (release E0: read envelopes, write them only in a database that already has
    /// them) or `write` (release E1: also start them in a database that has none). Default `read`. Rolling back from
    /// `write` is safe only to a release that has `read`.
    #[arg(long, default_value = "read")]
    envelopes: String,
    /// Compaction 2, step 3 (`fx`): read through our manifest and sealed runs. The value is how many write batches of a
    /// database before its first publish of runs (`fx/build.rs`); with the compactor off, between two publishes (0: none
    /// by the server itself). On by default since integration-3, with 2 (the judge's tested setting); `--no-sealed-runs`
    /// or FENVIX_SEALED_RUNS=off turns it off. A database that published a version has every SlateDB row too while the
    /// drop is off, so turning it off again reads the same data. The environment variable FENVIX_SEALED_RUNS=N does the
    /// same as `--sealed-runs N`.
    #[arg(long)]
    sealed_runs: Option<u64>,
    /// Turn sealed runs (and with them the compactor) off.
    #[arg(long)]
    no_sealed_runs: bool,
    /// Step 9 ballast generator: generate the databases of this plan (JSON, `step9/machine/ballast_plan.py`) straight
    /// into sealed runs and our manifest with this process's options, then exit (`ballast.rs`). Needs `--sealed-runs`.
    #[arg(long)]
    ballast: Option<String>,
    /// Raw MB per group when a database's first version cuts its groups (the plan's 1 GB).
    #[arg(long, default_value_t = 1024)]
    sealed_group_mb: u64,
    /// RAM for decoded blocks of our runs, shared by every database (MB).
    /// Default: 40% of the caches' share of the memory limit (`membudget.rs`).
    #[arg(long)]
    sealed_cache_mb: Option<u64>,
    /// Every how many publishes every group is resealed (the others write a delta run only).
    #[arg(long, default_value_t = 3)]
    sealed_reseal_every: u64,
    /// T_pin in milliseconds (fx, rules 6 and 26): a reader refreshes its version of our manifest (and release E1's
    /// announcement) at least this often, and envelopes start this long after release E1 is announced, with sealed runs
    /// or with `--envelopes write`. FENVIX_SEALED_TPIN_MS does the same.
    #[arg(long, default_value_t = 30_000)]
    sealed_t_pin_ms: u64,
    /// How long after release E1 is announced envelopes start, in milliseconds (default: T_pin, as rule 26 asks; tests
    /// set it lower). FENVIX_SEALED_E1_WAIT_MS does the same.
    #[arg(long)]
    sealed_e1_wait_ms: Option<u64>,
    /// What the counter keeps of a run block fetched by a one-customer read (fx/counter.rs, step 6): `off` (the whole
    /// block), `customer` (the customer's rows) or `area` (the rows of the read's collection documents or index).
    #[arg(long, default_value = "area")]
    sealed_slices: String,
    /// Bits per customer of the customer filter of new runs (the new-user filter).
    #[arg(long, default_value_t = 16)]
    sealed_filter_bits: u64,
    /// The staged publish (step 6): before a version that retires runs becomes current, warm the new runs' ranges of what
    /// the counter holds of the retired ones, for at most this many milliseconds (0: no staging).
    #[arg(long, default_value_t = 10_000)]
    sealed_stage_ms: u64,
    /// A slice is kept whole this many milliseconds after a read made it, then trimmed to what reads relied on.
    #[arg(long, default_value_t = 1_000)]
    sealed_trim_ms: u64,
    /// Tests only: accept `--sealed-e1-wait-ms` below T_pin (rule 26 needs at least T_pin in production; the check at
    /// startup refuses a shorter wait without this flag). FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 does the same.
    #[arg(long)]
    sealed_test_short_e1_wait: bool,
    /// Erasure (compaction 2 step 7b): how long after a whole-customer delete its rows must have left our live runs, L0
    /// and SlateDB (24 hours, DECISIONS 2026-11-07), and how often the erasure agent looks for erasures not done.
    #[arg(long, default_value_t = 24 * 3600 * 1000)]
    sealed_erasure_deadline_ms: u64,
    #[arg(long, default_value_t = 60_000)]
    sealed_erasure_poll_ms: u64,
    /// perf-erase: the physical erasure runs in batches; a database's batch is due when its oldest pending erasure has
    /// waited this long (never more than an eighth of the deadline), so one rewrite of each run serves many erasures. The
    /// delete itself returns at once and its data is unreadable at once. 0: at the agent's next poll.
    #[arg(long, default_value_t = 3_600_000)]
    sealed_erasure_batch_ms: u64,
    /// perf-erase: ... or when this many erasures of the database are pending.
    #[arg(long, default_value_t = 1_000)]
    sealed_erasure_batch_max: u64,
    /// erase-cost-1tb: the physical erasure rides the compactor's ordinary reseals and merges ("one reseal serves many");
    /// only what they did not do by an erasure's force time is forced (a reseal of each group still owing, a cleanse of each
    /// delta run still holding dead rows). false: the perf-erase batch scrub (above).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sealed_erasure_ride: bool,
    /// erase-cost-1tb: an erasure's force time, this long after its request (never later than three quarters of its
    /// deadline). Every erasure whose force time falls within the batch window is forced together.
    #[arg(long, default_value_t = 18 * 3_600_000)]
    sealed_erasure_force_after_ms: u64,
    /// erase-cost-1tb: how often an open database with erasures pending is checked for what ordinary jobs did.
    #[arg(long, default_value_t = 600_000)]
    sealed_erasure_check_ms: u64,
    /// Timing of our manifest (fx; review round 2 of step 0, F6), in milliseconds, checked at startup: a manifest PUT
    /// with no answer within the PUT timeout counts as timed out (a fence follows); it must be shorter than the lease
    /// minus its margin, and the remote lease and the GC grace must each exceed the hold life plus it.
    #[arg(long, default_value_t = 20_000)]
    sealed_put_timeout_ms: u64,
    #[arg(long, default_value_t = 60_000)]
    sealed_lease_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    sealed_lease_margin_ms: u64,
    #[arg(long, default_value_t = 180_000)]
    sealed_remote_lease_ms: u64,
    #[arg(long, default_value_t = 600_000)]
    sealed_gc_grace_ms: u64,
    #[arg(long, default_value_t = 120_000)]
    sealed_hold_life_ms: u64,
    /// At startup (S3 store), abort the bucket's unfinished multipart uploads under `tenants/` started more than this many
    /// seconds ago: what a crash left (uploads.rs, where the bound is explained). 0: no sweep.
    #[arg(long, default_value_t = 3600)]
    abandoned_upload_secs: u64,
    /// Compaction 2, step 4: with sealed runs on, one compactor per open writer (absorption, delta tiers, reseals, GC)
    /// instead of the offline publishes and today's compaction pass. On by default since integration-3 (with sealed runs
    /// on); `--no-sealed-compactor` or FENVIX_SEALED_COMPACTOR=0 keeps step 3's offline publishes. FENVIX_SEALED_COMPACTOR=small
    /// uses small knobs, for tests on tiny databases. (`--sealed-compactor` is kept and does nothing more.)
    #[arg(long)]
    sealed_compactor: bool,
    #[arg(long)]
    no_sealed_compactor: bool,
    /// perf-latency: where the compactors' jobs and SlateDB's fold run. `own` (default): a runtime of their own, at nice
    /// +10 (`tenant_bench::fx::pool::bg_runtime`); `shared`: the server's runtime, as before (measurements only).
    #[arg(long, default_value = "own")]
    compaction_threads: String,
    /// perf-latency: with `--compaction-threads own`, the share of the cores (percent) the background threads may use on
    /// average; the jobs pause when ahead of it (absorption excepted near the writer's L0 stop). 100 = no pause.
    #[arg(long, default_value_t = 50)]
    compaction_cpu_pct: u64,
    /// perf-latency round 2: the background pauses to the end of a 100 ms window once the whole process used this share
    /// (percent) of what its cores allow in it (a CPU quota freezes every thread once used up). 100 = never.
    #[arg(long, default_value_t = 70)]
    compaction_window_pct: u64,
    /// With the compactor: the drop of SlateDB rows our sealed runs already hold (the fold's filter, `fx/drop.rs`). On by
    /// default since P-perf-bytes (before, SlateDB kept a full uncompressed copy of every row: 56% of the stored bytes of
    /// the local benchmark). `--sealed-drop false` or FENVIX_SEALED_DROP=0 keeps SlateDB a full copy.
    #[arg(long, default_value_t = true, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    sealed_drop: bool,
    /// How long GC keeps a version of our manifest that nothing holds, serves or names (pairs, references, forks keep
    /// theirs). 0 by default since P-perf-bytes (was 7 days): with the drop on such a version cannot be read.
    /// integration-4: perf-erase added the same flag (7 days) for the compactors' old versions; one flag now sets both
    /// (GC's `fx::VERSION_RETENTION_MS` and the compactors' `fx::compactor::VERSION_RETENTION_SECS`), with perf-bytes'
    /// default. An erasure is done once the versions older than its scrub are gone, so it is done sooner.
    #[arg(long, default_value_t = 0)]
    sealed_version_retention_secs: u64,
    /// First course (opening.rs, measurement prototype, off by default): keep the key-value pairs of each user's first
    /// query of a visit and answer the next visit's first query from them when the answer is exact.
    #[arg(long, default_value_t = false)]
    first_course: bool,
    /// First course: a query of a user idle for this long starts a visit.
    #[arg(long, default_value_t = 1800)]
    first_course_idle_secs: u64,
    /// First course: an opening not refreshed by a visit for this many days is not used (T).
    #[arg(long, default_value_t = 14)]
    first_course_days: u64,
    /// First course: all openings together, in MiB.
    /// Default: 1/16 of the memory limit (`membudget.rs`).
    #[arg(long)]
    first_course_mb: Option<u64>,
    /// First course: one opening at most, in KiB (trimmed above it, then dropped).
    #[arg(long, default_value_t = 256)]
    first_course_max_kb: u64,
    /// First course: the opening keeps what this many queries of a visit read (1: the first query only). Default 4, the
    /// logical images of compaction 2 step 6 (the first course's review: with 1 query a visit's next query pays the
    /// cold read instead).
    #[arg(long, default_value_t = 4)]
    first_course_queries: u64,
    /// The customer's box (custbox.rs, P-big-speed fix 3): the first find of a customer of a customer-keyed collection
    /// reads the customer's whole key range once, so it and the customer's next requests are served from the caches.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    customer_box: bool,
    /// P-big-speed fix 4: the `_id` check of an insert into a customer-keyed collection without a time field reads the
    /// document key (inside the customer's box) instead of the shared locator (false: the locator, as before).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    id_check_in_box: bool,
    /// P-big-speed fix 3: the local-disk floor under the runs' block cache (the customer's box kept in memory, then on
    /// local disk): its directory (emptied at start; default: `<temp dir>/fenvix-box-floor-<port>`).
    #[arg(long, default_value = "")]
    box_disk_dir: String,
    /// Size of the disk floor in MiB (0 = a quarter of the free space of its file system, at most 4096; with
    /// `--box-disk false` no floor).
    #[arg(long, default_value_t = 0)]
    box_disk_mb: u64,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    box_disk: bool,
    /// A customer's box is warmed again after this many seconds.
    #[arg(long, default_value_t = 600)]
    customer_box_ttl_secs: u64,
    /// Review of big-speed (error 1): the most one customer's box warm-up fetches (stored bytes) and keeps (decoded
    /// bytes), in MiB; a bigger box is read on demand. Default: the runs' cache / 8 (`membudget.rs`; 12 MiB at 1 GiB).
    #[arg(long)]
    customer_box_max_mb: Option<u64>,
    /// The most every box warm-up of the process has in flight together, in MiB. Default: the runs' cache / 4.
    #[arg(long)]
    customer_box_inflight_mb: Option<u64>,
    /// Review of big-speed (error 2): every database's memtables together, in MiB (`memgov.rs`); writes wait past 1.5
    /// times this. Default: 1/8 of the memory limit (128 MiB at 1 GiB). 0 = no process-wide budget.
    #[arg(long)]
    memtables_total_mb: Option<u64>,
    /// With `--sealed-compactor`: the release flag E1 of derived documents (`fx/derive.rs`): reseals store the threads of
    /// the agent checkpoint collections as a message log plus derived documents. Reads understand them always (E0).
    /// On by default since integration-3 for the agent checkpoint collections below (the LangGraph saver's);
    /// `--sealed-derive false` or FENVIX_SEALED_DERIVE=0 turns it off. A release before the delta log cannot read a
    /// database once its reseals wrote derived documents.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sealed_derive: bool,
    /// Agent checkpoint collections (`agentck.rs`): comma separated `[db.]collection`, customer key `thread_id`.
    #[arg(long, default_value = "checkpoints,checkpoint_writes")]
    agent_checkpoints: String,
    /// Compaction 2, step 5: the one-wave reader (`fx/view.rs` `WAVE`): a read through our runs asks SlateDB and every
    /// candidate run at once. On by default (`--sealed-wave false` or FENVIX_SEALED_WAVE=0 turns it off).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sealed_wave: bool,
    /// Block codec of the sealed runs the compactor writes: `zstd` (run format v2; the default since P-perf-bytes: before
    /// launch no older release reads these databases) or `lz4` (run format v1). FENVIX_SEALED_CODEC does the same.
    #[arg(long, default_value = "zstd")]
    sealed_codec: String,
    /// SST block size in KiB, for the writers' flushes and the compaction's output. Default 4 (the stored format of the
    /// releases before); M29 measured 64.
    #[arg(long, default_value_t = 4)]
    sst_block_kb: u32,
    /// SST block compression: lz4 (the default since P-perf-bytes: before launch no older release has to read these
    /// databases; M29: 2.41x on chat-like documents), zstd, or none (the stored format of the releases before).
    #[arg(long, default_value = "lz4")]
    sst_compression: String,
    /// T of the tiered-levels compaction scheduler (M26-M31: 4); 0 = SlateDB's size-tiered scheduler.
    #[arg(long, default_value_t = 4)]
    compaction_tiers: u32,
    /// Fetch tasks of a scan that reads ahead at most 64 KiB (index scans, a find's documents); M29: 1.
    #[arg(long, default_value_t = 1)]
    small_scan_fetch_tasks: usize,
}

async fn handle_connection(server: Arc<Server>, tls: Arc<tls::TlsConfig>, tcp: TcpStream, conn_id: i64) {
    // The client address is the TCP peer: nothing in front of the engine rewrites it, and nothing the client sends changes it.
    let guard = netguard::get();
    let peer = tcp.peer_addr().ok().map(|a| netguard::canonical_ip(a.ip()));
    let Some(slot) = guard.open_connection(peer) else {
        return; // over the per-address limit: closed at once
    };
    let deadline = guard.handshake_timeout().map(|t| tokio::time::Instant::now() + t);
    let accepted = match deadline {
        Some(d) => match tokio::time::timeout_at(d, tls.accept(tcp)).await {
            Ok(r) => r,
            Err(_) => {
                guard.totals.handshake_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                eprintln!("conn {conn_id}: closed: TLS handshake not done within the handshake timeout (peer {})", peer.map(|p| p.to_string()).unwrap_or_default());
                return;
            }
        },
        None => tls.accept(tcp).await,
    };
    let stream = match accepted {
        Ok(s) => s,
        Err(e) => {
            eprintln!("conn {conn_id}: TLS handshake failed: {e:#}");
            return;
        }
    };
    stream.set_nodelay(true);
    server.diag.conn_opened(conn_id, stream.peer_addr().ok().map(|a| std::net::SocketAddr::new(a.ip(), a.port())));
    server.auth_connection_opened(conn_id, stream.peer_addr().ok().map(|a| a.ip()), stream.local_addr().ok().map(|a| a.ip()), stream.client_subject(), Some(slot));
    handle_connection_inner(&server, stream, conn_id, deadline, peer).await;
    server.auth_connection_closed(conn_id);
    server.diag.conn_closed(conn_id);
}

async fn handle_connection_inner(server: &Arc<Server>, stream: tls::Stream, conn_id: i64, first_deadline: Option<tokio::time::Instant>, peer: Option<std::net::IpAddr>) {
    let guard = netguard::get();
    let (r, mut w) = tokio::io::split(stream);
    let mut r = BufReader::new(r);
    let mut response_id = 1;
    let mut first = true;
    loop {
        // The first message must arrive whole before the handshake deadline. With --auth, until the connection has
        // authenticated, a message must arrive whole within the handshake timeout once it started, and may not be larger than
        // the pre-authentication limit, and the next one must start within --unauth-idle-timeout-secs (a driver's
        // monitoring connection never authenticates but sends a hello at least every ~10 s). Idle time between the messages
        // of an authenticated connection is not limited.
        let pre_auth = server.auth.enabled.load(std::sync::atomic::Ordering::Relaxed) && !server.conn_authenticated(conn_id);
        // `idle_wait`: the wait for the first byte is the silence limit (not the handshake deadline)
        let (first_by, rest_within, idle_wait) = match (first, first_deadline) {
            (true, Some(d)) => (Some(d), Some(d.saturating_duration_since(tokio::time::Instant::now())), false),
            _ if pre_auth => (guard.unauth_idle_timeout().map(|t| tokio::time::Instant::now() + t), guard.handshake_timeout(), true),
            _ => (None, None, false),
        };
        let max_len = if pre_auth { guard.pre_auth_max() } else { usize::MAX };
        first = false;
        match wire::read_request_bounded(&mut r, max_len, first_by, rest_within).await {
            Ok(Some(mut req)) => {
                let t_msg = std::time::Instant::now();
                let msg_cmd = if tenant_bench::waits::ON.load(std::sync::atomic::Ordering::Relaxed) { req.command.keys().next().cloned().unwrap_or_default() } else { String::new() };
                let probe = ctl::usage::before(&req.db, &req.command).await;
                server.diag.note_seq(conn_id, &req.seq_keys);
                // the command document is handed over (not copied): the reply needs only `reply_to`
                let mut reply_to = req.reply_to();
                let command = std::mem::take(&mut req.command);
                // an awaitable `hello` the driver allows to stream (its monitoring connection): keep the command to answer it again
                let exhaust = (req.exhaust_allowed && cmd::awaitable_hello(&command)).then(|| command.clone());
                // boxed: the command's future is large (about 22 KB); kept inline it would be part of every connection's
                // task, idle or not
                let t_h = std::time::Instant::now();
                let reply = Box::pin(server.handle(&req.db, command, conn_id)).await;
                let handle_ms = t_h.elapsed().as_secs_f64() * 1000.0;
                ctl::usage::after(probe).await;
                if reply.contains_key("$shutdown") {
                    // `shutdown`: no reply; the connection closes and the process stops
                    repl_cmds::SHUTDOWN.notify_one();
                    break;
                }
                if req.more_to_come {
                    continue;
                }
                response_id += 1;
                let streaming = exhaust.is_some() && matches!(reply.get("ok"), Some(Bson::Double(x)) if *x == 1.0);
                reply_to.more_to_come = streaming;
                let t_w = std::time::Instant::now();
                if let Err(e) = wire::write_reply(&mut w, &reply_to, &reply, response_id).await {
                    eprintln!("conn {conn_id}: write failed: {e:#}");
                    break;
                }
                let total = t_msg.elapsed().as_secs_f64() * 1000.0;
                let write_ms = t_w.elapsed().as_secs_f64() * 1000.0;
                tenant_bench::waits::report_message(&msg_cmd, handle_ms, write_ms, total - handle_ms - write_ms, total);
                if streaming {
                    // exhaust, as mongod: every maxAwaitTimeMS (or at a topology change) another reply flagged moreToCome,
                    // each answering the previous reply, with no request in between; it ends when the driver closes the
                    // connection (the write fails or the read side sees the end).
                    let mut cmd = exhaust.unwrap();
                    let mut last = reply;
                    loop {
                        // the next wait starts from the topology of the last reply, as the driver's next request would:
                        // a request that came with a stale topologyVersion (another process) is answered at once only once
                        if let Ok(tv) = last.get_document("topologyVersion") {
                            cmd.insert("topologyVersion", tv.clone());
                        }
                        let next = tokio::select! {
                            r = Box::pin(server.handle(&req.db, cmd.clone(), conn_id)) => r,
                            _ = r.fill_buf() => break,
                        };
                        reply_to.request_id = response_id;
                        response_id += 1;
                        reply_to.more_to_come = matches!(next.get("ok"), Some(Bson::Double(x)) if *x == 1.0);
                        if wire::write_reply(&mut w, &reply_to, &next, response_id).await.is_err() || !reply_to.more_to_come {
                            break;
                        }
                        last = next;
                    }
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let shown = peer.map(|p| p.to_string()).unwrap_or_default();
                if let Some(t) = e.downcast_ref::<wire::ReadTimeout>() {
                    if t.first_byte && idle_wait {
                        guard.totals.idle_unauthenticated_closed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        guard.note_idle_close(conn_id, &shown);
                    } else {
                        guard.totals.handshake_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        eprintln!("conn {conn_id}: closed: message not received within the handshake timeout (peer {shown})");
                    }
                } else if let Some(t) = e.downcast_ref::<wire::TooLarge>() {
                    guard.totals.oversized_before_auth.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("conn {conn_id}: closed: {t} before authentication (peer {shown})");
                } else {
                    eprintln!("conn {conn_id}: {e:#}");
                }
                break;
            }
        }
    }
}

/// Measurement builds (`--features heap-prof`): every `HEAP_PROF_DUMP_SECS` seconds, print jemalloc's live heap
/// (`allocated`) next to what the allocator holds (`resident`) and, with `prof:true`, write a heap profile
/// (`HEAP_PROF_PREFIX.N.heap`) for `jeprof --base`.
#[cfg(feature = "heap-prof")]
fn heap_prof_thread() {
    let Some(secs) = std::env::var("HEAP_PROF_DUMP_SECS").ok().and_then(|v| v.parse::<u64>().ok()) else { return };
    let prefix = std::env::var("HEAP_PROF_PREFIX").unwrap_or_else(|_| "/tmp/heap".into());
    std::thread::spawn(move || {
        for n in 0u64.. {
            std::thread::sleep(std::time::Duration::from_secs(secs));
            let (a, r) = heap_stats();
            eprintln!("HEAPPROF n={n} allocated={a} resident={r}");
            let path = std::ffi::CString::new(format!("{prefix}.{n:04}.heap")).unwrap();
            unsafe {
                let _ = tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path.as_ptr());
            }
        }
    });
}

/// Measurement builds (`--features cpu-prof`): with `CPU_PROF_FILE` set, the first SIGUSR2 starts a sampling CPU
/// profile of every thread (SIGPROF at 997 Hz) and the next SIGUSR2 writes it to that file as folded stacks
/// (`frame;frame;... count`, root first), then the cycle repeats (`.1`, `.2`, ... appended after the first).
#[cfg(feature = "cpu-prof")]
fn cpu_prof_thread() {
    let Ok(path) = std::env::var("CPU_PROF_FILE") else { return };
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2()).unwrap();
            for n in 0u64.. {
                sig.recv().await;
                let guard = match pprof::ProfilerGuardBuilder::default().frequency(997).build() {
                    Ok(g) => g,
                    Err(e) => {
                        eprintln!("CPUPROF start failed: {e}");
                        continue;
                    }
                };
                eprintln!("CPUPROF started n={n}");
                sig.recv().await;
                let out = if n == 0 { path.clone() } else { format!("{path}.{n}") };
                match guard.report().build() {
                    Ok(rep) => {
                        let mut s = String::new();
                        for (frames, count) in rep.data.iter() {
                            let mut names: Vec<String> = Vec::new();
                            for f in frames.frames.iter().rev() {
                                for sym in f.iter().rev() {
                                    // short names (`{async_fn#0}`, `poll`) get the source file and line to be told apart
                                    let mut n = sym.name().replace(';', ":");
                                    let (fp, line) = (sym.filename(), sym.lineno());
                                    if line > 0 {
                                        let tail: Vec<&str> = fp.rsplitn(3, '/').collect();
                                        let short = if tail.len() >= 2 { format!("{}/{}", tail[1], tail[0]) } else { fp.to_string() };
                                        n = format!("{n}@{short}:{line}");
                                    }
                                    names.push(n);
                                }
                            }
                            s.push_str(&format!("{};{} {}\n", frames.thread_name.replace(';', ":"), names.join(";"), count));
                        }
                        let _ = std::fs::write(&out, s);
                        eprintln!("CPUPROF written {out}");
                    }
                    Err(e) => eprintln!("CPUPROF report failed: {e}"),
                }
            }
        });
    });
}

/// jemalloc's live heap (`stats.allocated`) and the memory it holds (`stats.resident`), in bytes.
pub(crate) fn heap_stats() -> (u64, u64) {
    use tikv_jemalloc_ctl::{epoch, stats};
    let _ = epoch::advance();
    (stats::allocated::read().unwrap_or(0) as u64, stats::resident::read().unwrap_or(0) as u64)
}

fn main() -> Result<()> {
    #[cfg(feature = "heap-prof")]
    heap_prof_thread();
    #[cfg(feature = "cpu-prof")]
    cpu_prof_thread();
    // The futures of the command handlers are large: the workers get stacks that are not the 2 MiB default.
    // ISO_EVENT_INTERVAL (test only, docs/isolamento.md): how many tasks a worker runs between two looks at the network.
    // perf-latency round 2: on as many CPUs as the CPU quota allows (`fx::pool::pin_to_quota`), before any thread starts
    let pinned = tenant_bench::fx::pool::pin_to_quota();
    if let Some(c) = &pinned {
        eprintln!("cpu: the process may use {} core(s) and runs on CPU(s) {c:?}", c.len());
    }
    let mut b = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = std::env::var("ISO_EVENT_INTERVAL").ok().and_then(|v| v.parse::<u32>().ok()) {
        b.event_interval(n);
    }
    // perf-latency: at least MIN_WORKERS worker threads, whatever the CPU quota. tokio sizes its workers to the cores it
    // may use: one in a one-core container, where a command that spends 20-60 ms of CPU on a multi-MB document (parse,
    // copy, re-encode) kept every other command and every log acknowledgement waiting (MEASURED, medium profile: 175 of 673
    // operations over 30 ms waited for a stall outside their own command). With more threads than cores the kernel
    // time-slices them, as it does mongod's threads, and a short command runs beside a long one. FENVIX_WORKER_THREADS
    // overrides (measurements).
    const MIN_WORKERS: usize = 4;
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let workers = std::env::var("FENVIX_WORKER_THREADS").ok().and_then(|v| v.parse::<usize>().ok()).filter(|n| *n > 0).unwrap_or(cores.max(MIN_WORKERS));
    b.worker_threads(workers);
    b.enable_all().thread_stack_size(64 << 20).build()?.block_on(run())
}

async fn run() -> Result<()> {
    let args = Args::parse();
    // perf-latency: per-command wait breakdown and the runtime stall probe (tenant_bench::waits), off unless FX_WAITS is set
    if let Ok(v) = std::env::var("FX_WAITS") {
        tenant_bench::waits::ON.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Ok(ms) = v.parse::<u64>() {
            tenant_bench::waits::SLOW_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
        }
        tenant_bench::waits::GETS.store(std::env::var("FX_WAITS_GETS").is_ok(), std::sync::atomic::Ordering::Relaxed);
        tenant_bench::waits::spawn_probe();
    }
    // The SST cleanup deletes an SST this long after it left the manifest; a reader that polls the manifest less often
    // would read SSTs that are already gone.
    anyhow::ensure!(args.writer_poll_secs < args.sst_gc_min_unref_secs, "--writer-poll-secs ({}) must be below --sst-gc-min-unref-secs ({})", args.writer_poll_secs, args.sst_gc_min_unref_secs);
    anyhow::ensure!(
        !args.unsafe_skip_ownership_checks || std::env::var("MONGO_ALLOW_UNSAFE_TEST_FLAGS").map_or(false, |v| v == "1"),
        "--unsafe-skip-ownership-checks is a test control that lets two processes serve one database; it needs MONGO_ALLOW_UNSAFE_TEST_FLAGS=1"
    );
    netguard::install(&args.net)?;
    tls::check_resumption(&args.tls_session_resumption)?;
    let _ = tls::RESUMPTION.set(args.tls_session_resumption.clone());
    let grantable = authz::parse_grantable(&args.grantable_roles).map_err(|e| anyhow::anyhow!(e))?;
    let s3: Arc<dyn ObjectStore> = match args.store.as_str() {
        "s3" => {
            // The credentials are the standard AWS ones, none is built in: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
            // AWS_SESSION_TOKEN, a web identity token (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN), the container credentials
            // endpoint, or, with none of those set, the instance role through the metadata service (IMDSv2). Region and endpoint
            // come from the flags or from AWS_REGION / AWS_DEFAULT_REGION / AWS_ENDPOINT. Credentials are never on the command line.
            let mut b = AmazonS3Builder::from_env().with_bucket_name(&args.bucket);
            if let Some(r) = &args.region {
                b = b.with_region(r);
            }
            let region_set = args.region.is_some() || ["AWS_REGION", "AWS_DEFAULT_REGION"].iter().any(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false));
            anyhow::ensure!(region_set, "no S3 region: pass --region or set AWS_REGION (the engine has no built-in region)");
            if !args.endpoint.is_empty() {
                b = b.with_endpoint(&args.endpoint);
                if args.endpoint.starts_with("http://") {
                    b = b.with_allow_http(true);
                }
            }
            // S3 lists keys in order: the WAL cleanup finds the oldest WAL object with one LIST.
            tenant_bench::lifecycle::LIST_SORTED.store(true, std::sync::atomic::Ordering::Relaxed);
            let s3 = Arc::new(b.build()?);
            if args.abandoned_upload_secs > 0 {
                uploads_sweep::spawn_sweep(Arc::clone(&s3), std::time::Duration::from_secs(args.abandoned_upload_secs));
            }
            // the erase and the drop list and abort a database's incomplete multipart uploads (uploads.rs)
            uploads::install(Arc::clone(&s3), &args.bucket, &args.region.clone().or_else(|| std::env::var("AWS_REGION").ok().filter(|v| !v.is_empty())).or_else(|| std::env::var("AWS_DEFAULT_REGION").ok()).unwrap_or_default(), &args.endpoint)?;
            s3
        }
        "local" => {
            std::fs::create_dir_all(&args.local_dir)?;
            // LocalFileSystem has no conditional overwrite (PutMode::Update); the wrapper adds it for one process.
            Arc::new(localcas::LocalCas::new(Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&args.local_dir)?)))
        }
        "memory" => Arc::new(object_store::memory::InMemory::new()),
        other => anyhow::bail!("unknown --store {other}"),
    };
    // trip meter (and simulated S3 latency when FENVIX_SIM_S3 is set: measurement only), then large GETs in pieces
    let s3 = tenant_bench::simstore::wrap_sim(s3);
    let s3 = faults::wrap(s3);
    let s3 = tenant_bench::simstore::wrap_split(s3);
    // agent lane: the warm spare keeps the followed databases' new WAL objects in memory (agentlane.rs)
    let s3 = agentlane::wrap_follow(s3, &args.agent_lane_follow);
    tenant_bench::lifecycle::FLUSH_INTERVAL_MS.store(args.wal_flush_ms, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::ACK_BEFORE_DURABLE.store(args.ack_before_durable, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::EXPLICIT_FLUSH.store(args.wal_flush_ms == 0, std::sync::atomic::Ordering::Relaxed);
    dbs::set_wake_limit(args.max_concurrent_wakes);
    // P-big-speed fix 1: every cache and buffer from the memory limit (an explicit flag wins)
    let budget = tenant_bench::membudget::install(args.memory_limit_mb);
    eprintln!("memory budget: limit {} MiB ({}), caches {} MiB (block {}, meta {}, sealed {}), memtable {} MiB, first course {} MiB, change log {} MiB, L0 reads at once {}",
        budget.limit >> 20, budget.source, budget.caches >> 20, budget.block_cache >> 20, budget.meta_cache >> 20, budget.sealed_cache >> 20, budget.memtable >> 20, budget.first_course >> 20, budget.change_log >> 20, budget.l0_read_parallel);
    tenant_bench::lifecycle::MEMTABLE_MAX_BYTES.store(args.memtable_max_mb.map_or(budget.memtable, |m| m << 20), std::sync::atomic::Ordering::Relaxed);
    // integration-4: the LangGraph saver's collections are customer-keyed by thread_id by default (the command line wins)
    custkey::configure(&custkey::with_agent_defaults(&args.customer_key, &args.agent_checkpoints))?;
    opening::ENABLED.store(args.first_course, std::sync::atomic::Ordering::Relaxed);
    custbox::ENABLED.store(args.customer_box, std::sync::atomic::Ordering::Relaxed);
    coll::ID_CHECK_IN_BOX.store(args.id_check_in_box, std::sync::atomic::Ordering::Relaxed);
    {
        let port = args.listen.rsplit(':').next().unwrap_or("0").to_string();
        let dir = if args.box_disk_dir.is_empty() { std::env::temp_dir().join(format!("fenvix-box-floor-{port}")) } else { std::path::PathBuf::from(&args.box_disk_dir) };
        if args.box_disk {
            std::fs::create_dir_all(&dir)?;
            let mb = if args.box_disk_mb > 0 { args.box_disk_mb } else { (tenant_bench::counter::free_disk_bytes(&dir).unwrap_or(0) / 4 >> 20).min(4096) };
            tenant_bench::fx::floor::install(dir.clone(), mb << 20)?;
            eprintln!("box disk floor: {} MiB in {}", mb, dir.display());
        } else {
            // review of big-speed: a disabled floor still empties what an earlier process left in its folder
            tenant_bench::fx::floor::install(dir.clone(), 0)?;
        }
    }
    custbox::TTL_SECS.store(args.customer_box_ttl_secs, std::sync::atomic::Ordering::Relaxed);
    {
        use std::sync::atomic::Ordering::Relaxed;
        // review of big-speed: the box per customer and per process, and the memtables of every database together
        tenant_bench::fx::view::BOX_CUSTOMER_BYTES.store(args.customer_box_max_mb.map_or(budget.box_customer, |m| m << 20), Relaxed);
        tenant_bench::fx::view::BOX_INFLIGHT_BYTES.store(args.customer_box_inflight_mb.map_or(budget.box_inflight, |m| m << 20).max(1 << 20), Relaxed);
        tenant_bench::memgov::BUDGET_BYTES.store(args.memtables_total_mb.map_or(budget.memtables, |m| m << 20), Relaxed);
        eprintln!("memory budget (review round): box {} MiB per customer, {} MiB in flight; memtables of every database {} MiB (writes wait past {} MiB); merge read-ahead {} MiB; absorption {} L0 SSTs",
            tenant_bench::fx::view::BOX_CUSTOMER_BYTES.load(Relaxed) >> 20, tenant_bench::fx::view::BOX_INFLIGHT_BYTES.load(Relaxed) >> 20,
            tenant_bench::memgov::budget() >> 20, tenant_bench::memgov::hard() >> 20, budget.merge_read >> 20, budget.absorb_l0);
    }
    opening::IDLE_SECS.store(args.first_course_idle_secs, std::sync::atomic::Ordering::Relaxed);
    opening::MAX_AGE_SECS.store(args.first_course_days * 86_400, std::sync::atomic::Ordering::Relaxed);
    opening::BUDGET_BYTES.store(args.first_course_mb.map_or(budget.first_course, |m| m << 20), std::sync::atomic::Ordering::Relaxed);
    opening::QUERIES.store(args.first_course_queries.max(1), std::sync::atomic::Ordering::Relaxed);
    opening::MAX_BYTES.store(args.first_course_max_kb << 10, std::sync::atomic::Ordering::Relaxed);
    match args.envelopes.as_str() {
        "read" => {}
        "write" => tenant_bench::envelope::START.store(true, std::sync::atomic::Ordering::Relaxed),
        other => anyhow::bail!("--envelopes must be read or write, not {other}"),
    }
    // FENVIX_SEALED_RUNS=N: the same as --sealed-runs N, for every server a test harness starts (`judge/run.sh` with
    // JUDGE_SEALED_RUNS)
    // Default since integration-3: on, first publish after 2 write batches; off only when asked
    // P-command-cpu: the span scan of a customer's documents (coll.rs docs_by_span), off for measurements and the
    // control of its test only (answers are the same either way)
    if std::env::var_os("FENVIX_NO_SPAN_READS").is_some() {
        crate::coll::SPAN_READS.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    let env_sr = std::env::var("FENVIX_SEALED_RUNS").ok();
    // the command line wins over the environment (a test that passes --sealed-runs N gets it under FENVIX_SEALED_RUNS=off)
    let sealed_runs = if let Some(n) = args.sealed_runs {
        Some(n)
    } else if args.no_sealed_runs || env_sr.as_deref() == Some("off") {
        None
    } else {
        Some(env_sr.as_deref().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_SEALED_RUNS))
    };
    if let Some(n) = sealed_runs {
        use std::sync::atomic::Ordering::Relaxed;
        tenant_bench::fx::ENABLED.store(true, Relaxed);
        tenant_bench::fx::PUBLISH_EVERY.store(n, Relaxed);
        tenant_bench::fx::GROUP_BYTES.store(args.sealed_group_mb.max(1) << 20, Relaxed);
        tenant_bench::fx::view::CACHE_BYTES.store(args.sealed_cache_mb.map_or(budget.sealed_cache, |m| m << 20), Relaxed);
        tenant_bench::fx::RESEAL_EVERY.store(args.sealed_reseal_every.max(1), Relaxed);
        tenant_bench::fx::erase::DEADLINE_MS.store(args.sealed_erasure_deadline_ms, Relaxed);
        ctl::erase::POLL_MS.store(args.sealed_erasure_poll_ms, Relaxed);
        ctl::erase::BATCH_MS.store(args.sealed_erasure_batch_ms, Relaxed);
        ctl::erase::BATCH_MAX.store(args.sealed_erasure_batch_max.max(1), Relaxed);
        ctl::erase::RIDE.store(args.sealed_erasure_ride, Relaxed);
        ctl::erase::FORCE_AFTER_MS.store(args.sealed_erasure_force_after_ms, Relaxed);
        ctl::erase::CHECK_MS.store(args.sealed_erasure_check_ms.max(10), Relaxed);
        tenant_bench::fx::compactor::VERSION_RETENTION_SECS.store(args.sealed_version_retention_secs, Relaxed);
    }
    // negative controls of the step 7b fix round, tests only
    let ctl_env = |k: &str| std::env::var(k).map(|v| v == "1").unwrap_or(false);
    tenant_bench::fx::hold::CONTROL_KEEP_VIEW.store(ctl_env("FENVIX_CONTROL_KEEP_VIEW"), std::sync::atomic::Ordering::Relaxed);
    tenant_bench::fx::hold::CONTROL_OP_HOLDS.store(ctl_env("FENVIX_CONTROL_OP_HOLDS"), std::sync::atomic::Ordering::Relaxed);
    tenant_bench::fx::erase::CONTROL_NO_OWNERSHIP.store(ctl_env("FENVIX_CONTROL_NO_OWNERSHIP"), std::sync::atomic::Ordering::Relaxed);
    tenant_bench::fx::read::CONTROL_FULL_WALK.store(ctl_env("FENVIX_CONTROL_FULL_WALK"), std::sync::atomic::Ordering::Relaxed);
    if sealed_runs.is_some() || tenant_bench::envelope::START.load(std::sync::atomic::Ordering::Relaxed) {
        // T_pin and E1's wait (rules 6 and 26) apply to release E1 with or without sealed runs (`--envelopes write`)
        use std::sync::atomic::Ordering::Relaxed;
        let t_pin = std::env::var("FENVIX_SEALED_TPIN_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(args.sealed_t_pin_ms);
        tenant_bench::fx::T_PIN_MS.store(t_pin.max(1), Relaxed);
        if let Some(w) = args.sealed_e1_wait_ms.or_else(|| std::env::var("FENVIX_SEALED_E1_WAIT_MS").ok().and_then(|v| v.parse().ok())) {
            tenant_bench::fx::E1_WAIT_MS.store(w, Relaxed);
        }
        use tenant_bench::fx::counter::{SliceMode, SLICES, STAGED, STAGE_DEADLINE_MS};
        let mode = match args.sealed_slices.as_str() {
            "off" => SliceMode::Off,
            "customer" => SliceMode::Customer,
            "area" => SliceMode::Area,
            other => anyhow::bail!("--sealed-slices must be off, customer or area, not {other}"),
        };
        SLICES.store(mode as u8, Relaxed);
        tenant_bench::runfmt::FILTER_BITS_PER_CUSTOMER.store(args.sealed_filter_bits.clamp(1, 64), Relaxed);
        STAGED.store(args.sealed_stage_ms > 0, Relaxed);
        STAGE_DEADLINE_MS.store(args.sealed_stage_ms, Relaxed);
        tenant_bench::fx::counter::TRIM_MS.store(args.sealed_trim_ms, Relaxed);
        let short = args.sealed_test_short_e1_wait || std::env::var("FENVIX_SEALED_TEST_SHORT_E1_WAIT").map(|v| v == "1").unwrap_or(false);
        tenant_bench::fx::E1_WAIT_TEST.store(short, Relaxed);
        tenant_bench::fx::PUT_TIMEOUT_MS.store(args.sealed_put_timeout_ms, Relaxed);
        tenant_bench::fx::LEASE_MS.store(args.sealed_lease_ms, Relaxed);
        tenant_bench::fx::LEASE_MARGIN_MS.store(args.sealed_lease_margin_ms, Relaxed);
        tenant_bench::fx::REMOTE_LEASE_MS.store(args.sealed_remote_lease_ms, Relaxed);
        tenant_bench::fx::GC_GRACE_MS.store(args.sealed_gc_grace_ms, Relaxed);
        tenant_bench::fx::VERSION_RETENTION_MS.store(args.sealed_version_retention_secs.saturating_mul(1000), Relaxed);
        tenant_bench::fx::hold::HOLD_LIFE_MS.store(args.sealed_hold_life_ms, Relaxed);
        let env_c = std::env::var("FENVIX_SEALED_COMPACTOR").unwrap_or_default();
        // default since integration-3: on; off with --no-sealed-compactor or FENVIX_SEALED_COMPACTOR=0
        let _ = args.sealed_compactor;
        if !args.no_sealed_compactor && env_c != "0" {
            tenant_bench::fx::compactor::SERVER_ON.store(true, Relaxed);
            tenant_bench::fx::compactor::SERVER_SMALL.store(env_c == "small", Relaxed);
            let drop = match std::env::var("FENVIX_SEALED_DROP").as_deref() {
                Ok("1") => true,
                Ok("0") => false,
                _ => args.sealed_drop,
            };
            tenant_bench::fx::compactor::DROP_DEFAULT.store(drop, Relaxed);
            anyhow::ensure!(matches!(args.compaction_threads.as_str(), "own" | "shared"), "--compaction-threads is own or shared");
            tenant_bench::fx::pool::BG_ON.store(args.compaction_threads == "own", Relaxed);
            anyhow::ensure!((1..=100).contains(&args.compaction_cpu_pct), "--compaction-cpu-pct is 1 to 100");
            // FENVIX_COMPACTION_CPU_PCT overrides the flag (measurements)
            let pct = std::env::var("FENVIX_COMPACTION_CPU_PCT").ok().and_then(|v| v.parse::<u64>().ok()).filter(|p| (1..=100).contains(p)).unwrap_or(args.compaction_cpu_pct);
            tenant_bench::fx::pool::BG_CPU_PCT.store(if pct >= 100 { 100_000 } else { pct }, Relaxed);
            let wpct = std::env::var("FENVIX_COMPACTION_WINDOW_PCT").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(args.compaction_window_pct);
            tenant_bench::fx::pool::WINDOW_PCT.store(wpct.clamp(1, 100), Relaxed);
            let derive = match std::env::var("FENVIX_SEALED_DERIVE").as_deref() {
                Ok("1") => true,
                Ok("0") => false,
                _ => args.sealed_derive,
            };
            tenant_bench::fx::derive::WRITE.store(derive, Relaxed);
            let codec = std::env::var("FENVIX_SEALED_CODEC").unwrap_or_else(|_| args.sealed_codec.clone());
            tenant_bench::fx::compactor::set_sealed_codec(&codec)?;
        }
        agentck::configure(&args.agent_checkpoints)?;
        let wave = args.sealed_wave && std::env::var("FENVIX_SEALED_WAVE").as_deref() != Ok("0");
        tenant_bench::fx::view::WAVE.store(wave, Relaxed);
        tenant_bench::fx::view::WARM_THREAD.store(std::env::var("FENVIX_WARM_THREAD").as_deref() != Ok("0"), Relaxed);
        tenant_bench::fx::pool::WRITE_PATH_ON.store(std::env::var("FENVIX_WRITE_PATH_RUNTIME").as_deref() != Ok("0"), Relaxed);
        // fix-write-stalls: the first build off the database's lock, and the compactor's L0 recognition off it (both on by
        // default; "0" restores the old place, to compare); test pauses inside a build (kill -9 and close tests)
        tenant_bench::lifecycle::FIRST_BUILD_OFF_LOCK.store(std::env::var("FENVIX_FIRST_BUILD_OFF_LOCK").as_deref() != Ok("0"), Relaxed);
        // fix-write-stalls, cause 3: "1" proposes no SlateDB job while one runs, as before (to compare)
        // tests (ctl test_66): every SlateDB job on sorted runs only is held at its first row (a kill -9 mid-merge)
        tenant_bench::fx::drop::TEST_HOLD_MERGE.store(std::env::var("FENVIX_TEST_HOLD_MERGE").as_deref() == Ok("1"), Relaxed);
        tenant_bench::fx::drop::CONTROL_ONE_JOB.store(std::env::var("FENVIX_ONE_SLATE_JOB").as_deref() == Ok("1"), Relaxed);
        tenant_bench::fx::compactor::RECOGNISE_IN_BACKGROUND.store(std::env::var("FENVIX_RECOGNISE_IN_BACKGROUND").as_deref() != Ok("0"), Relaxed);
        for (var, cell) in [("FENVIX_TEST_BUILD_PAUSE_SNAPSHOT_MS", &tenant_bench::fx::build::PAUSE_AFTER_SNAPSHOT_MS), ("FENVIX_TEST_BUILD_PAUSE_CREATE_MS", &tenant_bench::fx::build::PAUSE_BEFORE_CREATE_MS), ("FENVIX_TEST_RECOGNISE_PAUSE_MS", &tenant_bench::fx::compactor::PAUSE_RECOGNISE_MS)] {
            if let Some(ms) = std::env::var(var).ok().and_then(|v| v.parse::<u64>().ok()) {
                cell.store(ms, Relaxed);
            }
        }
        tenant_bench::fx::check_config()?;
    }
    tenant_bench::tiered::SST_BLOCK_KB.store(args.sst_block_kb, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::tiered::set_compression(&args.sst_compression)?;
    tenant_bench::tiered::TIERS.store(args.compaction_tiers, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::tiered::SMALL_SCAN_FETCH_TASKS.store(args.small_scan_fetch_tasks, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::FILTER_BITS.store(args.filter_bits_per_key.clamp(1, 64), std::sync::atomic::Ordering::Relaxed);
    if args.waiter_tools {
        let ram = if args.counter_ram_mb > 0 { args.counter_ram_mb << 20 } else if let Some(p) = args.counter_ram_pct { budget.limit / 100 * p.min(90) } else { budget.counter_ram };
        let (dir, disk) = if args.counter_disk_dir.is_empty() {
            (None, 0)
        } else {
            let d = std::path::PathBuf::from(&args.counter_disk_dir);
            std::fs::create_dir_all(&d)?;
            let gb = if args.counter_disk_gb > 0 { args.counter_disk_gb << 30 } else { tenant_bench::counter::free_disk_bytes(&d).unwrap_or(0) / 10 * 8 };
            (Some(d), gb)
        };
        anyhow::ensure!(tenant_bench::lifecycle::install_counter(ram, dir, disk)?, "a cache is installed already");
        eprintln!("waiter tools on: counter RAM {} MiB, disk floor {} MiB ({})", ram >> 20, disk >> 20, if args.counter_disk_dir.is_empty() { "none" } else { &args.counter_disk_dir });
        tenant_bench::simstore::SPLIT_GET_MIN.store(args.split_get_min_kb << 10, std::sync::atomic::Ordering::Relaxed);
        tenant_bench::simstore::SPLIT_GET_PIECE.store(args.split_get_piece_kb << 10, std::sync::atomic::Ordering::Relaxed);
        tenant_bench::simstore::HEDGE_AFTER_MS.store(args.hedge_after_ms, std::sync::atomic::Ordering::Relaxed);
        tenant_bench::simstore::NEXT_BLOCK_BYTES.store(args.next_block_kb << 10, std::sync::atomic::Ordering::Relaxed);
        prefetch::READ_AHEAD_DOCS.store(args.read_ahead_docs, std::sync::atomic::Ordering::Relaxed);
        dbs::GET_MANY_PARALLEL.store(512, std::sync::atomic::Ordering::Relaxed);
    } else {
        tenant_bench::simstore::SPLIT_GET_MIN.store(0, std::sync::atomic::Ordering::Relaxed);
        let (block, meta) = (args.block_cache_mb.map_or(budget.block_cache, |m| m << 20), args.meta_cache_mb.map_or(budget.meta_cache, |m| m << 20));
        if block > 0 || meta > 0 {
            tenant_bench::lifecycle::install_shared_cache(block, meta);
        }
    }
    changes::MAX_BYTES.store(args.change_log_max_mb.map_or(budget.change_log as usize, |m| m << 20), std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::MEMTABLE_MAX_AGE_SECS.store(args.memtable_max_age_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::CACHE_FLUSHED_BLOCKS.store(args.cache_flushed_blocks, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::warm::COMPACTION_WARM.store(args.compaction_warm, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::warm::COMPACTION_WARM_MS.store(args.compaction_warm_ms, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::warm::COMPACTION_WARM_CONCURRENCY.store(args.compaction_warm_concurrency, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::warm::COMPACTION_WARM_MAX_KEY_BYTES.store(args.compaction_warm_max_key_mb << 20, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::warm::COMPACTION_WARM_GETS_PER_SEC.store(args.compaction_warm_gets_per_sec, std::sync::atomic::Ordering::Relaxed);
    cmd::CURSOR_TIMEOUT_SECS.store(args.cursor_timeout_secs, std::sync::atomic::Ordering::Relaxed);
    cmd::WRITE_DEADLINE_MS.store(args.write_deadline_ms, std::sync::atomic::Ordering::Relaxed);
    ctl::durability::install(args.relaxed_flush_ms)?;
    fair::install(args.fair_lag_ms, args.db_max_inflight, args.db_max_rate, args.fair_max_inflight, args.fair_max_flushing, args.db_quota_pts, args.db_burst_pts, args.db_quota_queue, &args.unit_weights).map_err(|e| anyhow::anyhow!(e))?;
    js::COMMAND_MAX_MS.store(args.js_command_max_ms, std::sync::atomic::Ordering::Relaxed);
    agentlane::configure(&args.agent_lane, args.agent_lane_window_ms)?;
    let local_log_old = if !args.quaderno.is_empty() {
        let dir = if args.local_log_dir.is_empty() { "fenvix-quaderno-kept" } else { args.local_log_dir.as_str() };
        locallog::install_quaderno(&args.quaderno, &args.quaderno_id, dir, args.local_log_upload_secs).await?;
        eprintln!("quaderno: writes are acknowledged by the replicated log at {}", args.quaderno);
        None
    } else if args.local_log_dir.is_empty() {
        None
    } else {
        Some(locallog::install(&args.local_log_dir, args.local_log_upload_secs)?)
    };
    // With the log, reads, acknowledgements and change streams follow the confirmed state (lifecycle::TRACK_VISIBLE).
    tenant_bench::lifecycle::TRACK_VISIBLE.store(locallog::get().is_some(), std::sync::atomic::Ordering::Relaxed);
    let _ = tenant_bench::lifecycle::VIS_IGNORE_KEY.set(locallog::MARKER_KEY.to_vec());
    tenant_bench::lifecycle::ADAPTIVE.store(args.adaptive_group_commit, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::ADAPTIVE_MAX_US.store(args.adaptive_group_commit_max_us, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::WRITER_POLL_SECS.store(args.writer_poll_secs, std::sync::atomic::Ordering::Relaxed);
    if tenant_bench::fx::compactor::SERVER_ON.load(std::sync::atomic::Ordering::Relaxed) {
        // a memtable flush that stalled on a full L0 retries only at the writer's manifest poll; the compactor's folds
        // free L0 within a second or two, so the writer looks at least every 2 s (step 4 report, section 4)
        tenant_bench::lifecycle::WRITER_POLL_SECS.store(args.writer_poll_secs.min(2), std::sync::atomic::Ordering::Relaxed);
    }
    tenant_bench::lifecycle::HOT_GC_EVERY_SECS.store(args.wal_gc_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GROUP_WINDOW_US.store((args.group_commit_window_ms * 1000.0) as u64, std::sync::atomic::Ordering::Relaxed);
    dbs::DURABLE_READS.store(args.durable_reads, std::sync::atomic::Ordering::Relaxed);
    dbs::META_CACHE.store(args.metadata_cache, std::sync::atomic::Ordering::Relaxed);
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
    // Requests are attributed to databases by object path (control layer, usage per database).
    let usage = ctl::usage::Usage::new();
    let s3 = ctl::usage::wrap(s3, Arc::clone(&usage));
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
    {
        // The hot WAL cleanup of an open writer runs only while this process owns the tenant.
        let l = Arc::clone(&leases);
        let _ = tenant_bench::lifecycle::OWNER_VALID.set(Box::new(move |id: &str| l.disabled.load(std::sync::atomic::Ordering::Relaxed) || l.valid(id)));
    }
    if let Some(c) = tenant_bench::counter::COUNTER.get() {
        tenant_bench::maps::start(Arc::clone(c), Arc::clone(&lc.store));
        prefetch::install(Arc::clone(c), Arc::clone(&lc.store));
    }
    let lc_for_ctl = Arc::clone(&lc);
    let dbs = Dbs::new(lc, Arc::clone(&s3), Duration::from_secs(args.idle_secs), Arc::clone(&leases), global);
    dbs.spawn_reaper();
    dbs.spawn_replay_raise_dropper();
    // negative control of ctl test_66 (FENVIX_TEST_NO_FIRST_INSTALLER=1): a finished first build waits for a write
    if std::env::var("FENVIX_TEST_NO_FIRST_INSTALLER").as_deref() != Ok("1") {
        dbs.spawn_first_build_installer();
    }
    {
        // a change-stream reader waiting on an event whose batch was not confirmed yet wakes when it is
        let changes = Arc::clone(&dbs.changes);
        let _ = tenant_bench::lifecycle::ON_VISIBLE.set(Box::new(move || changes.notify.notify_waiters()));
    }
    let (lost_tx, lost_rx) = tokio::sync::mpsc::unbounded_channel();
    leases.spawn_renewer(lost_tx);
    dbs.spawn_lost_handler(lost_rx);
    if !args.no_ttl {
        dbs.spawn_ttl_session_sweeper(Duration::from_secs(args.ttl_sweep_secs));
        if args.ttl_wake_sleeping {
            ttl::WAKE_SLEEPING.store(true, std::sync::atomic::Ordering::Relaxed);
            dbs.spawn_ttl_maintenance(Duration::from_secs(args.ttl_maintenance_secs), Duration::from_secs(args.ttl_min_gap_secs), args.ttl_max_wakes);
        }
    }
    let server = Server::new(Arc::clone(&dbs), args.log_commands);
    server.auth.enabled.store(args.auth, std::sync::atomic::Ordering::Relaxed);
    if let Some(g) = grantable {
        eprintln!("grantable roles (callers without root or clusterAdmin): {}", g.join(","));
        let _ = server.auth.grantable.set(g);
    }
    if !args.replica_set.is_empty() {
        let port = args.listen.rsplit(':').next().unwrap_or("27017");
        *server.rs.lock().unwrap() = Some((args.replica_set.clone(), args.advertise.clone().unwrap_or_else(|| format!("localhost:{port}"))));
    }
    server.spawn_txn_janitor();
    server.spawn_txn_hint_sweeper();
    // The local log is applied before the control layer starts: no erase, restore or fork (resumed or new) may run on a
    // database whose acknowledged writes are still only in the log, and an erase's drop record is then always seen first.
    if let Some(old) = local_log_old {
        let t = std::time::Instant::now();
        let (n, recs) = locallog::recover(&dbs, old, Duration::from_secs(args.local_log_replay_timeout_secs)).await?;
        eprintln!("local log: restart applied {recs} records to {n} databases in {:.1} s", t.elapsed().as_secs_f64());
        locallog::get().unwrap().spawn_uploader();
    }
    if !args.quaderno.is_empty() {
        let t = std::time::Instant::now();
        let (n, recs) = locallog::recover_remote(&dbs, Duration::from_secs(args.local_log_replay_timeout_secs)).await?;
        eprintln!("quaderno: restart applied {recs} records to {n} databases in {:.1} s", t.elapsed().as_secs_f64());
        locallog::get().unwrap().spawn_uploader();
    }
    if let Some(plan) = &args.ballast {
        // the step 9 ballast generator: this process's options, no listener, no control layer
        let r = ballast::run(Arc::clone(&server), plan, scratch_server).await;
        dbs.close_all().await;
        leases.release_process().await;
        return r;
    }
    ctl::start(Arc::clone(&dbs), Arc::clone(&server), lc_for_ctl, holder.clone(), args.ctl.clone(), usage).await?;
    let tls_cfg = Arc::new(tls::load(&tls::Options { mode: &args.tls_mode, certificate_key_file: &args.tls_certificate_key_file, ca_file: &args.tls_ca_file, allow_without_certificates: args.tls_allow_connections_without_certificates })?);
    tls::install(Arc::clone(&tls_cfg));
    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!("mongo-server listening on {} (idle close after {} s, holder {holder}, lease ttl {} s)", args.listen, args.idle_secs, args.lease_ttl_secs);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut last_accept_log: Option<std::time::Instant> = None;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        // Out of open files (EMFILE, ENFILE), memory (ENOBUFS, ENOMEM) or a connection reset before it was
                        // taken: never a reason to stop the process. The pending connections stay in the kernel's queue;
                        // wait a little (no busy loop while the files are out) and go on. One log line a minute at most.
                        netguard::get().totals.accept_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let now = std::time::Instant::now();
                        if last_accept_log.map(|t| now.duration_since(t) >= Duration::from_secs(60)).unwrap_or(true) {
                            last_accept_log = Some(now);
                            eprintln!("accept failed: {e} (open connections {}; the server goes on, next message at most in 60 s)", netguard::get().open_connections());
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let server = Arc::clone(&server);
                let id = server.new_connection_id();
                tokio::spawn(handle_connection(server, Arc::clone(&tls_cfg), stream, id));
            }
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
            _ = repl_cmds::SHUTDOWN.notified() => break,
        }
    }
    eprintln!("shutting down: closing tenants cleanly");
    let bound = Duration::from_secs(args.shutdown_timeout_secs.max(1));
    let t0 = std::time::Instant::now();
    // P-fix-slow-store item 1: one deadline for every database's close, then one short shared deadline for what follows
    let close_deadline = tokio::time::Instant::now() + Duration::from_secs(args.close_budget_secs.max(1)).min(bound);
    let tail_deadline = close_deadline + SHUTDOWN_TAIL;
    let clean = tokio::time::timeout(bound, async {
        ctl::stop_agents().await;
        dbs.close_all_by(Some(close_deadline)).await;
        let t_close = t0.elapsed();
        // the closes stopped the compactors, whose cut jobs abort their run uploads in the background (review D1); an
        // upload whose abort does not end here is left to the next owner (`uploads::abort_fenced`: its key names a writer
        // epoch older than that owner's)
        let tail_left = tail_deadline.saturating_duration_since(tokio::time::Instant::now());
        if !tenant_bench::runfmt::wait_dropped_aborts(tail_left.min(Duration::from_secs(10))).await {
            eprintln!("shutdown: run upload aborts still in flight (the next owner of the database aborts them: their writer epoch is older than its own)");
        }
        let t_aborts = t0.elapsed();
        // the pins, the last usage counters and the process lease, and beside them the maintenance stop, by the tail
        // deadline. Each is cut safely: a usage PUT and a lease PUT are atomic (a counter not written is counted by the
        // next interval from what is stored; a lease not released runs out), and a compaction pass writes only through
        // SlateDB's fenced compactions and conditional version PUTs.
        // (the pins are handed over and the counters written before the process lease goes, as before)
        let usage_then_lease = async {
            if tokio::time::timeout_at(tail_deadline, ctl::shutdown()).await.is_err() {
                eprintln!("shutdown: the pins and the last usage counters were not written in time");
            }
            if tokio::time::timeout_at(tail_deadline, leases.release_process()).await.is_err() {
                eprintln!("shutdown: the process lease was not released in time; it runs out by itself");
            }
        };
        let maint = async {
            if let Some(m) = maintenance {
                if tokio::time::timeout_at(tail_deadline, m.shutdown()).await.is_err() {
                    eprintln!("shutdown: maintenance did not stop in time");
                }
            }
        };
        futures::join!(usage_then_lease, maint);
        eprintln!(
            "TIMING shutdown close_ms={} aborts_ms={} tail_ms={} deadline_cuts={} left_behind={} held_at_deadline={} optional_skipped={}",
            t_close.as_millis(),
            (t_aborts - t_close).as_millis(),
            (t0.elapsed() - t_aborts).as_millis(),
            tenant_bench::lifecycle::CLOSE_DEADLINE_CUTS.load(std::sync::atomic::Ordering::Relaxed),
            tenant_bench::lifecycle::CLOSES_LEFT_BEHIND.load(std::sync::atomic::Ordering::Relaxed),
            dbs::CLOSES_LOCK_PAST_DEADLINE.load(std::sync::atomic::Ordering::Relaxed),
            dbs::CLOSES_OPTIONAL_SKIPPED.load(std::sync::atomic::Ordering::Relaxed),
        );
        eprintln!("tenants closed; stopping maintenance (done with the steps above)");
    })
    .await;
    if clean.is_err() {
        // A close waiting on the store (or on a command stuck on it) would hold SIGTERM for as long as the store refuses.
        // Exit now: the acknowledged writes are in WAL objects; the databases still open are taken over (replayed) by
        // their next owner when their leases run out, as after a crash.
        eprintln!("shutting down: the clean close did not finish in {} s (the store does not answer or refuses writes); exiting without it, acknowledged writes are in the WAL", bound.as_secs());
        std::process::exit(0);
    }
    eprintln!("maintenance stopped ({} ms)", t0.elapsed().as_millis());
    Ok(())
}

/// P-fix-slow-store item 1: what the shutdown steps after the databases' close get past the close deadline, together (the
/// upload aborts, then the usage counters, the process lease and the maintenance stop at once).
const SHUTDOWN_TAIL: Duration = Duration::from_secs(2);

/// A private server for the ballast generator's writers (`ballast.rs`): the same code on an in-memory store, no
/// maintenance, no leases, no listener. A writer creates the database's catalog in it with the same commands as on the
/// real database and runs the insert code against it; nothing it writes leaves the process.
fn scratch_server() -> Arc<Server> {
    let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let global = Arc::new(CountingStore::new(Arc::clone(&mem)));
    let raw: Arc<dyn ObjectStore> = global.clone();
    let counting = Arc::new(CountingStore::new(Arc::clone(&raw)));
    let lc = Lifecycle::with_guard(counting, None, false);
    let leases = lease::Leases::new(Arc::clone(&lc.store), format!("ballast-scratch-{}", std::process::id()), Duration::from_secs(3600));
    leases.disabled.store(true, std::sync::atomic::Ordering::Relaxed);
    let dbs = Dbs::new(lc, raw, Duration::from_secs(1 << 30), leases, global);
    Server::new(dbs, false)
}
