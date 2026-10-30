//! journal3z local test bench (phase-2 plan P5 tests, P7, P8): real processes on this machine only.
//!
//! - [`bins`]: builds the node, writer and proxy programs once (test features on: `--faults-file`, `--mutation`).
//! - [`Minio`], [`NodeProc`], [`WriterProc`], [`Proxy`]: start / kill -9 / restart them; writer and node stdout event
//!   lines are collected with the time they arrived.
//! - [`Client`]: load generator speaking the writers' client protocol; every acknowledged write is recorded.
//! - [`check`]: what the object store finally holds against every acknowledgement (0 losses expected), plus the
//!   "late acknowledgement" rule: no acknowledgement from an epoch at or above a newer writer's barrier.

use j3z_writer::meta::{Meta, MetaError};
use object_store::aws::AmazonS3Builder;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as ABufReader};
use tokio::sync::{mpsc, oneshot};

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Write a file atomically (the programs re-read their control files every 50 ms).
pub fn write_atomic(p: &Path, s: &str) {
    let tmp = p.with_extension("tmp");
    std::fs::write(&tmp, s).unwrap();
    std::fs::rename(&tmp, p).unwrap();
}

pub struct Bins {
    pub node: PathBuf,
    pub writer: PathBuf,
    pub proxy: PathBuf,
    pub minio: PathBuf,
}

/// Build the programs (same profile as the caller) with the bench features, once per process.
pub fn bins() -> &'static Bins {
    static B: OnceLock<Bins> = OnceLock::new();
    B.get_or_init(|| {
        let ws = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
        let exe = std::env::current_exe().unwrap();
        let release = exe.components().any(|c| c.as_os_str() == "release");
        let mut cmd = Command::new(env!("CARGO"));
        cmd.current_dir(&ws).args(["build", "-q", "-p", "j3z-node", "-p", "j3z-writer", "-p", "j3z-net", "--bins"]);
        cmd.args(["--features", "j3z-node/faults,j3z-node/mutations,j3z-writer/faults,j3z-writer/mutations"]);
        if release {
            cmd.arg("--release");
        }
        let st = cmd.status().expect("cargo build");
        assert!(st.success(), "building the bench programs failed");
        let dir = ws.join("target").join(if release { "release" } else { "debug" });
        let minio = std::env::var("J3Z_MINIO").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("/root/j3z-tools/bin/minio"));
        Bins { node: dir.join("j3z-node"), writer: dir.join("j3z-writer"), proxy: dir.join("j3z-proxy"), minio }
    })
}

/// Lines a child printed on stdout, with arrival times.
#[derive(Clone, Default)]
pub struct Events(pub Arc<Mutex<Vec<(Instant, String)>>>);

impl Events {
    fn follow(&self, out: std::process::ChildStdout) -> String {
        let mut r = BufReader::new(out);
        let mut first = String::new();
        r.read_line(&mut first).unwrap();
        let ev = self.0.clone();
        std::thread::spawn(move || {
            for l in r.lines() {
                let Ok(l) = l else { return };
                ev.lock().unwrap().push((Instant::now(), l));
            }
        });
        first
    }
    pub fn lines(&self) -> Vec<(Instant, String)> {
        self.0.lock().unwrap().clone()
    }
}

/// `key=value` field of an event line.
pub fn field<'a>(line: &'a str, k: &str) -> Option<&'a str> {
    line.split_whitespace().find_map(|w| w.strip_prefix(k).and_then(|r| r.strip_prefix('=')))
}

fn kill(c: &mut Option<Child>) {
    if let Some(mut c) = c.take() {
        let _ = c.kill(); // SIGKILL
        let _ = c.wait();
    }
}

// ============================================================================================================ MinIO

pub struct Minio {
    child: Option<Child>,
    pub port: u16,
    pub bucket: String,
}

impl Minio {
    /// MinIO on 127.0.0.1 with its data in `dir` and one bucket (created as a directory before start).
    pub fn start(dir: &Path) -> Minio {
        let bucket = "j3z".to_string();
        std::fs::create_dir_all(dir.join(&bucket)).unwrap();
        let port = free_port();
        let child = Command::new(&bins().minio)
            .args(["server", dir.to_str().unwrap(), "--address", &format!("127.0.0.1:{port}"), "--quiet"])
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            .env("MINIO_BROWSER", "off")
            .env("MINIO_UPDATE", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start minio");
        let m = Minio { child: Some(child), port, bucket };
        let t0 = Instant::now();
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(t0.elapsed() < Duration::from_secs(20), "minio did not start");
            std::thread::sleep(Duration::from_millis(100));
        }
        m
    }
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
    pub fn meta(&self) -> Meta {
        let s3 = AmazonS3Builder::new()
            .with_endpoint(self.endpoint())
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(&self.bucket)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .build()
            .unwrap();
        Meta::new(Arc::new(s3))
    }
    /// Wait until the bucket answers (the server listens before it serves).
    pub async fn ready(&self) {
        let meta = self.meta();
        let t0 = Instant::now();
        loop {
            match meta.get::<Value>(&object_store::path::Path::from("probe")).await {
                Ok(_) => return,
                Err(e) => assert!(t0.elapsed() < Duration::from_secs(20), "minio not ready: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Minio {
    fn drop(&mut self) {
        kill(&mut self.child);
    }
}

// ============================================================================================================ nodes

pub struct NodeProc {
    child: Option<Child>,
    pub dir: PathBuf,
    pub port: u16,
    pub faults: PathBuf,
    pub mutation: Option<String>,
    pub id: u32,
    pub events: Events,
}

impl NodeProc {
    pub fn start(dir: &Path, mutation: Option<&str>) -> NodeProc {
        let mut n = NodeProc { child: None, dir: dir.to_path_buf(), port: free_port(), faults: dir.with_extension("faults.json"), mutation: mutation.map(String::from), id: 0, events: Events::default() };
        n.restart();
        n
    }
    pub fn restart(&mut self) {
        kill(&mut self.child);
        let mut cmd = Command::new(&bins().node);
        cmd.args(["--dir", self.dir.to_str().unwrap(), "--listen", &format!("127.0.0.1:{}", self.port), "--faults-file", self.faults.to_str().unwrap(), "--segment-max", "1048576"]);
        if let Some(m) = &self.mutation {
            cmd.args(["--mutation", m]);
        }
        let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let first = self.events.follow(child.stdout.take().unwrap());
        self.id = field(&first, "node").unwrap_or_else(|| panic!("node did not start: {first:?}")).parse().unwrap();
        self.child = Some(child);
    }
    pub fn kill9(&mut self) {
        kill(&mut self.child);
    }
    /// Lose the disk: kill, delete the directory, start again (a new identity).
    pub fn wipe(&mut self) {
        self.kill9();
        let _ = std::fs::remove_dir_all(&self.dir);
        self.restart();
    }
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        kill(&mut self.child);
    }
}

// ============================================================================================================ proxy

/// One proxy process with a route per (writer, node) pair: route `w<i>-n<j>`.
pub struct Proxy {
    child: Option<Child>,
    ctl: PathBuf,
    state: Mutex<BTreeMap<String, Value>>,
    pub listen: BTreeMap<(usize, usize), String>,
}

impl Proxy {
    pub fn start(dir: &Path, writers: usize, nodes: &[String]) -> Proxy {
        std::fs::create_dir_all(dir).unwrap();
        let mut routes = vec![];
        let mut listen = BTreeMap::new();
        for w in 0..writers {
            for (j, t) in nodes.iter().enumerate() {
                let l = format!("127.0.0.1:{}", free_port());
                routes.push(json!({"name": format!("w{w}-n{j}"), "listen": l, "target": t}));
                listen.insert((w, j), l);
            }
        }
        let rp = dir.join("routes.json");
        std::fs::write(&rp, serde_json::to_string(&routes).unwrap()).unwrap();
        let ctl = dir.join("ctl.json");
        write_atomic(&ctl, "{}");
        let mut child = Command::new(&bins().proxy).args(["--routes", rp.to_str().unwrap(), "--ctl", ctl.to_str().unwrap()]).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let first = Events::default().follow(child.stdout.take().unwrap());
        assert_eq!(first.trim(), "READY");
        Proxy { child: Some(child), ctl, state: Mutex::new(BTreeMap::new()), listen }
    }
    /// Set the mode of route writer `w` → node `n` (`pass`, `blackhole`, `cut`, `drop_c2s`, `drop_s2c`).
    pub fn set(&self, w: usize, n: usize, mode: &str) {
        let mut s = self.state.lock().unwrap();
        s.insert(format!("w{w}-n{n}"), json!({"mode": mode}));
        write_atomic(&self.ctl, &serde_json::to_string(&*s).unwrap());
        drop(s);
        std::thread::sleep(Duration::from_millis(120));
    }
    pub fn node_addrs(&self, w: usize, nodes: usize) -> Vec<String> {
        (0..nodes).map(|j| self.listen[&(w, j)].clone()).collect()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        kill(&mut self.child);
    }
}

// =========================================================================================================== writers

pub struct WriterProc {
    child: Option<Child>,
    pub index: usize,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub client: String,
    pub faults: PathBuf,
    pub events: Events,
}

pub struct WriterSpec<'a> {
    pub index: usize,
    pub nwriters: usize,
    pub nodes: Vec<String>,
    pub dbs: u64,
    pub minio: &'a Minio,
    pub config: Value,
    pub dir: &'a Path,
    pub mutation: Option<&'a str>,
    /// extra environment (test-only switches)
    pub env: Vec<(&'a str, &'a str)>,
}

impl WriterProc {
    pub fn start(s: WriterSpec) -> WriterProc {
        std::fs::create_dir_all(s.dir).unwrap();
        let cfg = s.dir.join(format!("w{}.config.json", s.index));
        std::fs::write(&cfg, s.config.to_string()).unwrap();
        let faults = s.dir.join(format!("w{}.faults.json", s.index));
        write_atomic(&faults, "{}");
        let client = format!("127.0.0.1:{}", free_port());
        let mut args: Vec<String> = vec![
            "--holder".into(),
            format!("w{}", s.index),
            "--index".into(),
            s.index.to_string(),
            "--nwriters".into(),
            s.nwriters.to_string(),
            "--me".into(),
            (1000 + s.index).to_string(),
            "--nodes".into(),
            s.nodes.join(","),
            "--dbs".into(),
            s.dbs.to_string(),
            "--client-listen".into(),
            client.clone(),
            "--s3-endpoint".into(),
            s.minio.endpoint(),
            "--bucket".into(),
            s.minio.bucket.clone(),
            "--s3-key".into(),
            "minioadmin".into(),
            "--s3-secret".into(),
            "minioadmin".into(),
            "--config".into(),
            cfg.to_str().unwrap().into(),
            "--faults-file".into(),
            faults.to_str().unwrap().into(),
        ];
        if let Some(m) = s.mutation {
            args.extend(["--mutation".into(), m.into()]);
        }
        let env = s.env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let mut w = WriterProc { child: None, index: s.index, args, env, client, faults, events: Events::default() };
        w.restart();
        w
    }
    pub fn restart(&mut self) {
        kill(&mut self.child);
        let mut child = Command::new(&bins().writer).args(&self.args).envs(self.env.iter().map(|(k, v)| (k, v))).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let first = self.events.follow(child.stdout.take().unwrap());
        assert!(first.starts_with("READY"), "writer did not start: {first:?}");
        self.child = Some(child);
    }
    pub fn kill9(&mut self) {
        kill(&mut self.child);
    }
    pub fn set_clock_offset(&self, ms: i64) {
        write_atomic(&self.faults, &json!({"clock_offset_ms": ms}).to_string());
        std::thread::sleep(Duration::from_millis(120));
    }
    /// `OPEN` events: (time, db, epoch, barrier).
    pub fn opens(&self) -> Vec<(Instant, u64, u64, u64)> {
        self.events
            .lines()
            .into_iter()
            .filter(|(_, l)| l.starts_with("OPEN "))
            .map(|(t, l)| (t, field(&l, "db").unwrap().parse().unwrap(), field(&l, "epoch").unwrap().parse().unwrap(), field(&l, "barrier").unwrap().parse().unwrap()))
            .collect()
    }
}

impl Drop for WriterProc {
    fn drop(&mut self) {
        kill(&mut self.child);
    }
}

// ============================================================================================================ client

#[derive(Clone, Debug)]
pub struct Ack {
    pub db: u64,
    pub id: u64,
    pub idx: u64,
    pub epoch: u64,
    pub writer: usize,
    pub at: Instant,
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>;

/// One connection to a writer's client port, reconnecting; requests matched to answers by id.
struct Conn {
    tx: mpsc::UnboundedSender<(Value, oneshot::Sender<Value>)>,
}

impl Conn {
    fn new(addr: String) -> Conn {
        let (tx, mut rx) = mpsc::unbounded_channel::<(Value, oneshot::Sender<Value>)>();
        tokio::spawn(async move {
            loop {
                let Ok(Ok(s)) = tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(&addr)).await else {
                    // writer down: fail what is queued, try again
                    while let Ok((_, r)) = rx.try_recv() {
                        let _ = r.send(json!({"ok": false, "err": "down"}));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let _ = s.set_nodelay(true);
                let (rd, mut wr) = s.into_split();
                let pending: Pending = Arc::default();
                let p2 = pending.clone();
                let reader = tokio::spawn(async move {
                    let mut lines = ABufReader::new(rd).lines();
                    while let Ok(Some(l)) = lines.next_line().await {
                        let Ok(v) = serde_json::from_str::<Value>(&l) else { continue };
                        if let Some(tx) = p2.lock().unwrap().remove(&v["id"].as_u64().unwrap_or(0)) {
                            let _ = tx.send(v);
                        }
                    }
                });
                loop {
                    tokio::select! {
                        m = rx.recv() => {
                            let Some((req, r)) = m else { return };
                            pending.lock().unwrap().insert(req["id"].as_u64().unwrap(), r);
                            let mut line = req.to_string();
                            line.push('\n');
                            if wr.write_all(line.as_bytes()).await.is_err() { break; }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(200)) => {
                            if reader.is_finished() { break; }
                        }
                    }
                }
                reader.abort();
                for (_, r) in pending.lock().unwrap().drain() {
                    let _ = r.send(json!({"ok": false, "err": "down"}));
                }
            }
        });
        Conn { tx }
    }

    async fn call(&self, req: Value, timeout: Duration) -> Value {
        let (tx, rx) = oneshot::channel();
        if self.tx.send((req, tx)).is_err() {
            return json!({"ok": false, "err": "down"});
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(v)) => v,
            _ => json!({"ok": false, "err": "timeout"}),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct LoadStats {
    pub sent: u64,
    pub acked: u64,
    pub refused: BTreeMap<String, u64>,
}

/// Load generator: `conc` workers write to databases `1..=dbs` round-robin, each to the writer that currently says it
/// owns the database (asked every 100 ms).
pub struct Client {
    conns: Vec<Arc<Conn>>,
    owner: Arc<Mutex<HashMap<u64, usize>>>,
    next_id: Arc<AtomicU64>,
    pub acks: Arc<Mutex<Vec<Ack>>>,
    pub stats: Arc<Mutex<LoadStats>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Client {
    pub fn new(writers: &[String]) -> Arc<Client> {
        let c = Arc::new(Client {
            conns: writers.iter().map(|a| Arc::new(Conn::new(a.clone()))).collect(),
            owner: Arc::default(),
            next_id: Arc::new(AtomicU64::new(1)),
            acks: Arc::default(),
            stats: Arc::default(),
            stop: Arc::default(),
        });
        let c2 = c.clone();
        tokio::spawn(async move {
            loop {
                for (w, conn) in c2.conns.iter().enumerate() {
                    let id = c2.next_id.fetch_add(1, SeqCst);
                    let v = conn.call(json!({"op": "owned", "id": id}), Duration::from_millis(300)).await;
                    if let Some(a) = v["owned"].as_array() {
                        let mut o = c2.owner.lock().unwrap();
                        o.retain(|_, x| *x != w);
                        for p in a {
                            o.insert(p[0].as_u64().unwrap(), w);
                        }
                    }
                }
                if Arc::strong_count(&c2) == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        c
    }

    /// Owner of each database as last reported.
    pub fn owners(&self) -> HashMap<u64, usize> {
        self.owner.lock().unwrap().clone()
    }

    pub async fn write(&self, db: u64, size: usize) -> Result<Ack, String> {
        let Some(w) = self.owner.lock().unwrap().get(&db).copied() else { return Err("no_owner".into()) };
        let id = self.next_id.fetch_add(1, SeqCst);
        self.stats.lock().unwrap().sent += 1;
        let v = self.conns[w].call(json!({"op": "write", "db": db, "id": id, "size": size}), Duration::from_secs(15)).await;
        if v["ok"].as_bool() == Some(true) {
            let a = Ack { db, id, idx: v["idx"].as_u64().unwrap(), epoch: v["epoch"].as_u64().unwrap(), writer: w, at: Instant::now() };
            self.acks.lock().unwrap().push(a.clone());
            self.stats.lock().unwrap().acked += 1;
            Ok(a)
        } else {
            let e = v["err"].as_str().unwrap_or("?").to_string();
            *self.stats.lock().unwrap().refused.entry(e.clone()).or_default() += 1;
            Err(e)
        }
    }

    /// Start `conc` workers until [`Client::stop`].
    pub fn run(self: &Arc<Self>, dbs: u64, conc: usize, size: usize) -> Vec<tokio::task::JoinHandle<()>> {
        (0..conc)
            .map(|k| {
                let c = self.clone();
                tokio::spawn(async move {
                    let mut db = 1 + (k as u64 % dbs);
                    while !c.stop.load(SeqCst) {
                        if c.write(db, size).await.is_err() {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        db = 1 + (db % dbs);
                    }
                })
            })
            .collect()
    }

    pub async fn stop(&self, workers: Vec<tokio::task::JoinHandle<()>>) {
        self.stop.store(true, SeqCst);
        for w in workers {
            let _ = w.await;
        }
    }

    /// Ask the owner of `db` to upload now; returns the store marker.
    pub async fn flush(&self, db: u64) -> Option<u64> {
        let w = *self.owner.lock().unwrap().get(&db)?;
        let id = self.next_id.fetch_add(1, SeqCst);
        self.conns[w].call(json!({"op": "flush", "id": id, "db": db}), Duration::from_secs(10)).await["applied"].as_u64()
    }

    /// Wait until every database `1..=dbs` has an owner, and flush them all.
    pub async fn settle(&self, dbs: u64, limit: Duration) -> bool {
        let t0 = Instant::now();
        'again: while t0.elapsed() < limit {
            tokio::time::sleep(Duration::from_millis(200)).await;
            for db in 1..=dbs {
                if self.flush(db).await.is_none() {
                    continue 'again;
                }
            }
            return true;
        }
        false
    }
}

// =========================================================================================================== checker

#[derive(Debug, Default, Clone)]
pub struct Report {
    pub acked: usize,
    /// acknowledged writes the store does not hold at their index
    pub lost: Vec<Ack>,
    /// acknowledgements at or above the barrier of a newer epoch of the same database
    pub late: Vec<Ack>,
    pub store_records: usize,
}

/// Compare the store's final content with every acknowledgement. `opens`: (db, epoch, barrier) of every writer open.
pub async fn check(meta: &Meta, acks: &[Ack], opens: &[(u64, u64, u64)], dbs: u64) -> Result<Report, MetaError> {
    let mut r = Report { acked: acks.len(), ..Default::default() };
    let mut content = HashMap::new();
    for db in 1..=dbs {
        let (_, c) = meta.content(db).await?;
        r.store_records += c.len();
        content.insert(db, c);
    }
    for a in acks {
        if content[&a.db].get(&a.idx) != Some(&a.id) {
            r.lost.push(a.clone());
        }
        if opens.iter().any(|&(db, e, b)| db == a.db && e > a.epoch && a.idx >= b) {
            r.late.push(a.clone());
        }
    }
    Ok(r)
}

/// A short config for tests (seconds instead of tens of seconds; the production defaults are in `ServerCfg`).
pub fn fast_config() -> Value {
    json!({
        "lease_ttl_ms": 2000, "lease_skew_ms": 300, "upload_every_ms": 500, "release_after_ms": 1000,
        "preferred_poll_ms": 100, "other_poll_ms": 200, "open_timeout_ms": 2000, "tick_ms": 50
    })
}
