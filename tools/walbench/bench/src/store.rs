//! The log stores. `append` returns only once the batch is durable in that backend:
//!  - s3 / express: PutObject returned 200 (one object per commit, key <prefix><log>/<seq>)
//!  - ddb: PutItem returned (one item per commit, pk = log, sk = seq)
//!  - file: pwrite + fdatasync of the frame returned (the EBS volume)
//!  - quorum: 2 of the 3 nodes answered that the frame is fdatasync'd on their disk (quorum.rs)
//! SDK retries are disabled: every attempt is counted here, retried by hand (up to 6 attempts).

use crate::codec::{self, Decoded};
use crate::counters::{add, C};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnConsumedCapacity};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
pub struct ReadOut {
    pub batches: Vec<Decoded>,
    pub bad: Vec<String>,
    pub objects: u64,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn append(&self, seq: u64, body: Bytes) -> Result<()>;
    async fn read_all(&self) -> Result<ReadOut>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum Kind {
    S3,
    Express,
    Ddb,
    File,
    Quorum,
}

impl Kind {
    /// Largest batch body a commit may carry (DynamoDB items are limited to 400 KB).
    pub fn max_batch_bytes(self) -> usize {
        match self {
            Kind::Ddb => 350_000,
            Kind::Quorum => 1_000_000,
            _ => 4_000_000,
        }
    }
}

#[derive(clap::Args, Clone, Debug)]
pub struct BackendArgs {
    #[arg(long, value_enum)]
    pub backend: Kind,
    /// S3 bucket (s3) or directory bucket (express, name ends with --x-s3)
    #[arg(long, default_value = "")]
    pub bucket: String,
    #[arg(long, default_value = "wal/")]
    pub prefix: String,
    #[arg(long, default_value = "")]
    pub table: String,
    /// directory of the local log files (file backend: a directory on the EBS volume)
    #[arg(long, default_value = "/mnt/wal/logs")]
    pub dir: String,
    #[arg(long, default_value_t = 64)]
    pub prealloc_mb: u64,
    /// quorum nodes host:port, comma separated, exactly 3
    #[arg(long, default_value = "")]
    pub nodes: String,
    /// recovery: read only these node indexes (e.g. "1,2"); default all three
    #[arg(long, default_value = "")]
    pub read_nodes: String,
}

pub struct Backend {
    pub args: BackendArgs,
    s3: Option<aws_sdk_s3::Client>,
    ddb: Option<aws_sdk_dynamodb::Client>,
}

pub async fn sdk_config() -> aws_config::SdkConfig {
    let timeouts = aws_config::timeout::TimeoutConfig::builder()
        .operation_attempt_timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(3))
        .build();
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .retry_config(aws_config::retry::RetryConfig::disabled())
        .timeout_config(timeouts)
        .load()
        .await
}

pub async fn s3_client() -> aws_sdk_s3::Client {
    let conf = sdk_config().await;
    let mut b = aws_sdk_s3::config::Builder::from(&conf);
    if std::env::var("WALBENCH_PATH_STYLE").is_ok() {
        b = b.force_path_style(true); // local tests against an S3 emulator only
    }
    aws_sdk_s3::Client::from_conf(b.build())
}

impl Backend {
    pub async fn new(args: BackendArgs) -> Result<Arc<Backend>> {
        let (mut s3, mut ddb) = (None, None);
        match args.backend {
            Kind::S3 | Kind::Express => {
                if args.bucket.is_empty() {
                    bail!("--bucket is required");
                }
                if (args.backend == Kind::Express) != args.bucket.ends_with("--x-s3") {
                    bail!("express needs a directory bucket (name ending --x-s3), s3 a general purpose bucket");
                }
                s3 = Some(s3_client().await);
            }
            Kind::Ddb => {
                if args.table.is_empty() {
                    bail!("--table is required");
                }
                ddb = Some(aws_sdk_dynamodb::Client::new(&sdk_config().await));
            }
            Kind::File => std::fs::create_dir_all(&args.dir).with_context(|| format!("create {}", args.dir))?,
            Kind::Quorum => {
                if args.nodes.split(',').filter(|s| !s.is_empty()).count() != 3 {
                    bail!("--nodes needs exactly 3 host:port");
                }
            }
        }
        Ok(Arc::new(Backend { args, s3, ddb }))
    }

    pub async fn log(&self, log_id: &str) -> Result<Arc<dyn Store>> {
        if log_id.is_empty() || !log_id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            bail!("bad log id {log_id:?}");
        }
        Ok(match self.args.backend {
            Kind::S3 | Kind::Express => Arc::new(S3Store {
                c: self.s3.clone().unwrap(),
                bucket: self.args.bucket.clone(),
                prefix: format!("{}{}/", self.args.prefix, log_id),
                express: self.args.backend == Kind::Express,
            }),
            Kind::Ddb => Arc::new(DdbStore { c: self.ddb.clone().unwrap(), table: self.args.table.clone(), log: log_id.to_string() }),
            Kind::File => {
                let path = format!("{}/{}.wal", self.args.dir, log_id);
                let mb = self.args.prealloc_mb;
                Arc::new(tokio::task::spawn_blocking(move || FileStore::open(&path, mb)).await??)
            }
            Kind::Quorum => {
                let nodes: Vec<String> = self.args.nodes.split(',').map(|s| s.to_string()).collect();
                let read: Vec<usize> = if self.args.read_nodes.is_empty() {
                    vec![0, 1, 2]
                } else {
                    self.args.read_nodes.split(',').map(|s| s.parse().unwrap()).collect()
                };
                crate::quorum::QuorumStore::new(nodes, read, log_id.to_string(), self.args.prealloc_mb as u32)
            }
        })
    }
}

async fn backoff(attempt: u32) {
    add(&C.retries, 1);
    tokio::time::sleep(Duration::from_millis(20u64 << attempt.min(6))).await;
}

const ATTEMPTS: u32 = 6;

pub struct S3Store {
    c: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    express: bool,
}

impl S3Store {
    fn count_put(&self, bytes: u64) {
        if self.express {
            add(&C.xz_put, 1);
            add(&C.xz_put_bytes, bytes);
        } else {
            add(&C.s3_put, 1);
            add(&C.s3_put_bytes, bytes);
        }
    }
}

#[async_trait]
impl Store for S3Store {
    async fn append(&self, seq: u64, body: Bytes) -> Result<()> {
        let key = format!("{}{:020}", self.prefix, seq);
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            self.count_put(body.len() as u64);
            let r = self.c.put_object().bucket(&self.bucket).key(&key).body(body.clone().into()).send().await;
            match r {
                Ok(_) => return Ok(()),
                Err(e) => {
                    add(&C.errors, 1);
                    last = Some(format!("{}", aws_sdk_s3::error::DisplayErrorContext(&e)));
                    backoff(attempt).await;
                }
            }
        }
        Err(anyhow!("put {key} failed after {ATTEMPTS} attempts: {}", last.unwrap_or_default()))
    }

    async fn read_all(&self) -> Result<ReadOut> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut ok = None;
            for attempt in 0..ATTEMPTS {
                add(if self.express { &C.xz_list } else { &C.s3_list }, 1);
                let mut req = self.c.list_objects_v2().bucket(&self.bucket).prefix(&self.prefix);
                if let Some(t) = &token {
                    req = req.continuation_token(t);
                }
                match req.send().await {
                    Ok(r) => {
                        ok = Some(r);
                        break;
                    }
                    Err(e) => {
                        add(&C.errors, 1);
                        if attempt + 1 == ATTEMPTS {
                            bail!("list failed: {}", aws_sdk_s3::error::DisplayErrorContext(&e));
                        }
                        backoff(attempt).await;
                    }
                }
            }
            let r = ok.unwrap();
            for o in r.contents() {
                if let Some(k) = o.key() {
                    keys.push(k.to_string());
                }
            }
            // directory buckets do not list in key order and may return a token with an empty page; follow it anyway
            match r.next_continuation_token() {
                Some(t) if r.is_truncated().unwrap_or(false) || !t.is_empty() => token = Some(t.to_string()),
                _ => break,
            }
            if !r.is_truncated().unwrap_or(false) {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        let n = keys.len() as u64;
        let bodies: Vec<Result<(String, Bytes)>> = stream::iter(keys)
            .map(|k| async move {
                for attempt in 0..ATTEMPTS {
                    add(if self.express { &C.xz_get } else { &C.s3_get }, 1);
                    match self.c.get_object().bucket(&self.bucket).key(&k).send().await {
                        Ok(r) => match r.body.collect().await {
                            Ok(b) => {
                                let b = b.into_bytes();
                                if self.express {
                                    add(&C.xz_get_bytes, b.len() as u64);
                                }
                                return Ok((k, b));
                            }
                            Err(_) => add(&C.errors, 1),
                        },
                        Err(_) => add(&C.errors, 1),
                    }
                    backoff(attempt).await;
                }
                Err(anyhow!("get {k} failed"))
            })
            .buffer_unordered(32)
            .collect()
            .await;
        let mut out = ReadOut { objects: n, ..Default::default() };
        for b in bodies {
            let (k, b) = b?;
            match codec::decode(&b) {
                Ok(d) => out.batches.push(d),
                Err(e) => out.bad.push(format!("{k}: {e}")),
            }
        }
        Ok(out)
    }
}

pub struct DdbStore {
    c: aws_sdk_dynamodb::Client,
    table: String,
    log: String,
}

#[async_trait]
impl Store for DdbStore {
    async fn append(&self, seq: u64, body: Bytes) -> Result<()> {
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            add(&C.ddb_put, 1);
            add(&C.ddb_put_bytes, body.len() as u64);
            let r = self
                .c
                .put_item()
                .table_name(&self.table)
                .item("pk", AttributeValue::S(self.log.clone()))
                .item("sk", AttributeValue::N(seq.to_string()))
                .item("d", AttributeValue::B(aws_smithy_types::Blob::new(body.to_vec())))
                .return_consumed_capacity(ReturnConsumedCapacity::Total)
                .send()
                .await;
            match r {
                Ok(o) => {
                    let wcu = o.consumed_capacity().and_then(|c| c.capacity_units()).unwrap_or(0.0);
                    add(&C.ddb_wcu_milli, (wcu * 1000.0).round() as u64);
                    return Ok(());
                }
                Err(e) => {
                    let throttled = e.as_service_error().map(|x| x.is_provisioned_throughput_exceeded_exception() || x.is_request_limit_exceeded()).unwrap_or(false)
                        || e.as_service_error().and_then(|x| aws_sdk_dynamodb::error::ProvideErrorMetadata::code(x)).map(|c| c.contains("Throttl")).unwrap_or(false);
                    if throttled {
                        // throttled writes are rejected and not billed: counted apart, no WCU
                        add(&C.ddb_throttled, 1);
                    } else {
                        // any other failure may still have been written and billed; its WCU are not returned, so
                        // they are counted as the size-based estimate (1 WRU per started KB)
                        add(&C.ddb_wcu_milli, 1000 * ((body.len() as u64 + 64).div_ceil(1024)));
                    }
                    add(&C.errors, 1);
                    last = Some(format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)));
                    backoff(attempt).await;
                }
            }
        }
        Err(anyhow!("put_item seq {seq} failed after {ATTEMPTS} attempts: {}", last.unwrap_or_default()))
    }

    async fn read_all(&self) -> Result<ReadOut> {
        let mut out = ReadOut::default();
        let mut start = None;
        loop {
            let mut resp = None;
            for attempt in 0..ATTEMPTS {
                add(&C.ddb_query, 1);
                let r = self
                    .c
                    .query()
                    .table_name(&self.table)
                    .key_condition_expression("pk = :p")
                    .expression_attribute_values(":p", AttributeValue::S(self.log.clone()))
                    .consistent_read(true)
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
                    .set_exclusive_start_key(start.clone())
                    .send()
                    .await;
                match r {
                    Ok(r) => {
                        resp = Some(r);
                        break;
                    }
                    Err(e) => {
                        add(&C.errors, 1);
                        if attempt + 1 == ATTEMPTS {
                            bail!("query failed: {}", aws_sdk_dynamodb::error::DisplayErrorContext(&e));
                        }
                        backoff(attempt).await;
                    }
                }
            }
            let r = resp.unwrap();
            let rcu = r.consumed_capacity().and_then(|c| c.capacity_units()).unwrap_or(0.0);
            add(&C.ddb_rcu_milli, (rcu * 1000.0).round() as u64);
            for item in r.items() {
                out.objects += 1;
                let sk = item.get("sk").and_then(|v| v.as_n().ok()).cloned().unwrap_or_default();
                match item.get("d").and_then(|v| v.as_b().ok()) {
                    Some(b) => match codec::decode(b.as_ref()) {
                        Ok(d) => out.batches.push(d),
                        Err(e) => out.bad.push(format!("sk {sk}: {e}")),
                    },
                    None => out.bad.push(format!("sk {sk}: no data attribute")),
                }
            }
            match r.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        Ok(out)
    }
}

/// A log file: frames written with pwrite into a pre-zeroed file, then fdatasync. Pre-zeroing means an append does
/// not grow the file, so fdatasync has no size metadata to journal (what a real WAL does with recycled segments).
pub struct FileStore {
    path: String,
    st: Arc<Mutex<(File, u64)>>,
}

impl FileStore {
    /// Opens an existing log (resumes after its last valid frame) or creates a new pre-zeroed one.
    pub fn open(path: &str, prealloc_mb: u64) -> Result<FileStore> {
        let exists = std::path::Path::new(path).exists();
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path).with_context(|| format!("open {path}"))?;
        let off = if exists {
            codec::scan_file(path)?.1 as u64
        } else {
            let zero = vec![0u8; 1 << 20];
            for i in 0..prealloc_mb {
                f.write_all_at(&zero, i << 20)?;
            }
            f.sync_all()?;
            if let Some(dir) = std::path::Path::new(path).parent() {
                File::open(dir)?.sync_all()?;
            }
            0
        };
        Ok(FileStore { path: path.to_string(), st: Arc::new(Mutex::new((f, off))) })
    }

    pub fn append_blocking(st: &Mutex<(File, u64)>, body: &Bytes) -> Result<()> {
        let fr = codec::frame(body);
        let mut g = st.lock().unwrap();
        let off = g.1;
        g.0.write_all_at(&fr, off)?;
        g.0.sync_data()?; // fdatasync
        g.1 = off + fr.len() as u64;
        add(&C.file_syncs, 1);
        add(&C.file_bytes, fr.len() as u64);
        Ok(())
    }
}

#[async_trait]
impl Store for FileStore {
    async fn append(&self, _seq: u64, body: Bytes) -> Result<()> {
        let st = self.st.clone();
        tokio::task::spawn_blocking(move || Self::append_blocking(&st, &body)).await?
    }

    async fn read_all(&self) -> Result<ReadOut> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let (batches, _, stop) = codec::scan_file(&path)?;
            // a torn last frame is expected after a crash (it was never acknowledged); it is reported, not an error
            let mut out = ReadOut { objects: batches.len() as u64, batches, ..Default::default() };
            if let Some(s) = stop {
                out.bad.push(format!("end of log: {s}"));
            }
            Ok(out)
        })
        .await?
    }
}
