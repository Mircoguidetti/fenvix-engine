//! Per-database control operations for a control-plane service, exposed on a separate admin port (HTTP + bearer
//! token, see [`api`]). Everything here is new code that sits beside the MongoDB wire server: it uses the engine
//! only through `Dbs` (entry locks, leases) and slatedb's admin APIs, and keeps its own state under `control/` in the
//! bucket (never inside `tenants/`, so `listDatabases` and the tenant lifecycle are unaffected).
//!
//! Layout of `control/` (all JSON, written with conditional puts):
//! * `control/dbs/{db}/meta.json`            created_at, forked_from, keep_awake, retention
//! * `control/pins/{db}`                     keep-awake lease `{holder, expires_ms, seq}`
//! * `control/forks/{ancestor}/{db}`         `db` reads SSTs owned by `ancestor` (blocks erasing the ancestor)
//! * `control/ops/{db}.json`                 intent of a fork / restore / erase in progress (resumed after a crash)
//! * `control/exports/{id}.json`             an export snapshot (checkpoint pinned in the database)
//! * `control/usage/{db}/{day}/{holder}-{nonce}.json`   usage counters, one file per process and day
//! * `control/staging/{op}/...`              scratch slatedb databases of a restore

pub mod api;
pub mod erase;
pub mod fork;
pub mod http;
pub mod meta;
pub mod ops;
pub mod purge;
pub mod rec;
pub mod usage;

use crate::dbs::Dbs;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tenant_bench::lifecycle::Lifecycle;

#[derive(clap::Args, Debug, Clone)]
pub struct CtlArgs {
    /// Address of the admin HTTP interface (off when not set). Needs a token; plain HTTP, so bind it to a private
    /// interface or put a TLS terminator in front.
    #[arg(long)]
    pub admin_listen: Option<String>,
    /// Bearer token of the admin interface (at least 16 characters). Prefer `--admin-token-file` or the
    /// `MONGO_ADMIN_TOKEN` environment variable: command lines are visible to other users.
    #[arg(long)]
    pub admin_token: Option<String>,
    #[arg(long)]
    pub admin_token_file: Option<String>,
    /// Retention window (seconds) of databases without their own setting; 0 keeps no restore points.
    #[arg(long, default_value_t = 0)]
    pub retention_window_secs: u64,
    /// A new restore point is taken at most this often per database (and only if it was written since the last one).
    #[arg(long, default_value_t = 600)]
    pub retention_interval_secs: u64,
    /// Usage counters are written to the bucket this often (and at shutdown).
    #[arg(long, default_value_t = 60)]
    pub usage_flush_secs: u64,
    /// Keep-awake leases last this long and are renewed every third of it.
    #[arg(long, default_value_t = 30)]
    pub pin_lease_ttl_secs: u64,
    /// Export snapshots expire after this long (their checkpoint is released then).
    #[arg(long, default_value_t = 86400)]
    pub export_ttl_secs: u64,
    /// Skip the recovery of interrupted fork / restore / erase operations at start (test control only).
    #[arg(long)]
    pub no_ctl_recovery: bool,
}

/// An error with the HTTP status it should be reported with.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into() }
    }
    pub fn bad(message: impl Into<String>) -> Self {
        Self::new(400, "bad_request", message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, "not_found", message)
    }
    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(409, code, message)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        if let Some(l) = e.downcast_ref::<crate::lease::LeaseHeld>() {
            return ApiError::conflict("owned_elsewhere", l.to_string());
        }
        if let Some(a) = e.downcast_ref::<ApiError>() {
            return ApiError { status: a.status, code: a.code, message: a.message.clone() };
        }
        ApiError::new(500, "internal", format!("{e:#}"))
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

/// One async mutex per database name: control operations on a database run one at a time (a second one waits, and
/// gives up with 409 after a while). Mongo commands do not take these; they take the engine's entry lock, which the
/// operations then take second, so the order is always op lock -> entry lock.
#[derive(Default)]
pub struct OpLocks {
    m: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl OpLocks {
    pub async fn lock(&self, name: &str) -> ApiResult<tokio::sync::OwnedMutexGuard<()>> {
        let l = Arc::clone(self.m.lock().unwrap().entry(name.to_string()).or_default());
        match tokio::time::timeout(Duration::from_secs(60), l.lock_owned()).await {
            Ok(g) => Ok(g),
            Err(_) => Err(ApiError::conflict("busy", format!("another operation on database {name} is still running"))),
        }
    }

    /// The lock if no operation holds it right now.
    pub fn try_lock(&self, name: &str) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let l = Arc::clone(self.m.lock().unwrap().entry(name.to_string()).or_default());
        l.try_lock_owned().ok()
    }

    /// Two databases at once (a fork touches its source and its target), always in name order.
    pub async fn lock2(&self, a: &str, b: &str) -> ApiResult<(tokio::sync::OwnedMutexGuard<()>, Option<tokio::sync::OwnedMutexGuard<()>>)> {
        if a == b {
            return Ok((self.lock(a).await?, None));
        }
        let (x, y) = if a < b { (a, b) } else { (b, a) };
        let gx = self.lock(x).await?;
        let gy = self.lock(y).await?;
        Ok((gx, Some(gy)))
    }
}

pub struct Ctl {
    pub dbs: Arc<Dbs>,
    /// The process-wide counting store (every request of this process goes through it).
    pub store: rec::Store,
    pub lc: Arc<Lifecycle>,
    pub holder: String,
    pub args: CtlArgs,
    pub ops: OpLocks,
    pub usage: Arc<usage::Usage>,
    pub start_ms: i64,
}

static CTL: std::sync::OnceLock<Arc<Ctl>> = std::sync::OnceLock::new();

pub fn get() -> Option<&'static Arc<Ctl>> {
    CTL.get()
}

/// Wire the control layer into the process: usage accounting is always on (it is just counters); the admin port,
/// background agents and crash recovery start only when `--admin-listen` is given.
pub async fn start(dbs: Arc<Dbs>, lc: Arc<Lifecycle>, holder: String, args: CtlArgs, usage: Arc<usage::Usage>) -> Result<Arc<Ctl>> {
    let store = dbs.raw_store();
    let ctl = Arc::new(Ctl { dbs, store, lc, holder, args: args.clone(), ops: OpLocks::default(), usage, start_ms: rec::now_ms() });
    let _ = CTL.set(Arc::clone(&ctl));
    if args.admin_listen.is_none() {
        return Ok(ctl);
    }
    let token = match (&args.admin_token_file, &args.admin_token, std::env::var("MONGO_ADMIN_TOKEN")) {
        (Some(f), _, _) => std::fs::read_to_string(f)?.trim().to_string(),
        (None, Some(t), _) => t.clone(),
        (None, None, Ok(t)) => t,
        _ => anyhow::bail!("--admin-listen needs --admin-token-file, --admin-token or MONGO_ADMIN_TOKEN"),
    };
    anyhow::ensure!(token.len() >= 16, "the admin token must be at least 16 characters");
    if !args.no_ctl_recovery {
        ops::recover(&ctl).await;
    }
    usage::spawn_flusher(&ctl);
    let listener = tokio::net::TcpListener::bind(args.admin_listen.as_deref().unwrap()).await?;
    eprintln!("admin interface listening on {} (bearer token required)", args.admin_listen.as_deref().unwrap());
    let handler: http::Handler = {
        let ctl = Arc::clone(&ctl);
        Arc::new(move |req| {
            let ctl = Arc::clone(&ctl);
            Box::pin(async move { api::route(ctl, req).await })
        })
    };
    tokio::spawn(http::serve(listener, token, handler));
    Ok(ctl)
}


/// Test hook: when the environment variable `CTL_FAILPOINT` names this point, the process announces it on stderr
/// and then waits forever, so a test can `kill -9` it at exactly this step of an operation. Unset in production.
pub async fn failpoint(name: &str) {
    if std::env::var("CTL_FAILPOINT").map(|v| v == name).unwrap_or(false) {
        eprintln!("FAILPOINT {name} reached");
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
}
