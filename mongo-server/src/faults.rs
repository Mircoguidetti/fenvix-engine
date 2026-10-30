//! Test-only fault injection in front of the object store. Active only when the environment variable
//! `MONGO_TEST_FAULTS_FILE` names a file; never set in production. The file is read again when it changes, one rule per line:
//!
//!   hang_put SUBSTR        a PUT to a path containing SUBSTR waits while the rule is in the file, then goes on
//!   delay_put SUBSTR MS    a PUT to such a path is sent after MS milliseconds (a renewal that arrives late)
//!   fail_put SUBSTR        a PUT to such a path fails (an S3 error)
//!   fail_get SUBSTR        a GET (or HEAD) of such a path fails (an S3 error)
//!
//! Every triggered rule prints `FAULT <rule> <path>` on stderr, so a test knows the point was reached.
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OsResult,
};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
enum Rule {
    Hang(String),
    Delay(String, u64),
    Fail(String),
    FailGet(String),
}

pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    file: String,
    cache: Mutex<(Option<SystemTime>, Vec<Rule>)>,
}

/// Wraps the store when `MONGO_TEST_FAULTS_FILE` is set; otherwise returns it unchanged.
pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    match std::env::var("MONGO_TEST_FAULTS_FILE") {
        Ok(f) if !f.is_empty() => {
            eprintln!("TEST FAULT INJECTION ON ({f}): never use this in production");
            Arc::new(FaultStore { inner, file: f, cache: Mutex::new((None, Vec::new())) })
        }
        _ => inner,
    }
}

impl FaultStore {
    fn rules(&self) -> Vec<Rule> {
        let mtime = std::fs::metadata(&self.file).and_then(|m| m.modified()).ok();
        let mut c = self.cache.lock().unwrap();
        if c.0 != mtime || mtime.is_none() {
            let text = std::fs::read_to_string(&self.file).unwrap_or_default();
            let mut rules = Vec::new();
            for line in text.lines() {
                let w: Vec<&str> = line.split_whitespace().collect();
                match w.as_slice() {
                    ["hang_put", s] => rules.push(Rule::Hang(s.to_string())),
                    ["delay_put", s, ms] => rules.push(Rule::Delay(s.to_string(), ms.parse().unwrap_or(0))),
                    ["fail_put", s] => rules.push(Rule::Fail(s.to_string())),
                    ["fail_get", s] => rules.push(Rule::FailGet(s.to_string())),
                    _ => {}
                }
            }
            *c = (mtime, rules);
        }
        c.1.clone()
    }

    async fn before_put(&self, location: &Path) -> OsResult<()> {
        let p = location.as_ref();
        for r in self.rules() {
            match &r {
                Rule::Hang(s) if p.contains(s.as_str()) => {
                    eprintln!("FAULT hang_put {p}");
                    while self.rules().iter().any(|x| matches!(x, Rule::Hang(t) if t == s)) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
                Rule::Delay(s, ms) if p.contains(s.as_str()) => {
                    eprintln!("FAULT delay_put {p} {ms}");
                    tokio::time::sleep(Duration::from_millis(*ms)).await;
                }
                Rule::Fail(s) if p.contains(s.as_str()) => {
                    eprintln!("FAULT fail_put {p}");
                    return Err(object_store::Error::Generic { store: "faults", source: "injected PUT failure".into() });
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> OsResult<PutResult> {
        self.before_put(location).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> OsResult<Box<dyn MultipartUpload>> {
        self.before_put(location).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let p = location.as_ref();
        if self.rules().iter().any(|r| matches!(r, Rule::FailGet(s) if p.contains(s.as_str()))) {
            eprintln!("FAULT fail_get {p}");
            return Err(object_store::Error::Generic { store: "faults", source: "injected GET failure".into() });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, OsResult<Path>>) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.before_put(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OsResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Test only: the number on a line `NAME N` of the faults file (`MONGO_TEST_FAULTS_FILE`), if any. Used by the local log:
/// `stall_local_ack MS` holds every write for MS milliseconds after its record is on disk, before it is acknowledged.
pub fn rule_value(name: &str) -> Option<u64> {
    let f = std::env::var("MONGO_TEST_FAULTS_FILE").ok().filter(|f| !f.is_empty())?;
    let text = std::fs::read_to_string(f).ok()?;
    text.lines().find_map(|l| {
        let w: Vec<&str> = l.split_whitespace().collect();
        (w.len() == 2 && w[0] == name).then(|| w[1].parse().ok()).flatten()
    })
}
