//! Spend guard at the HTTP layer of the S3 client. Every request attempt, retries included, passes through
//! `GuardService::call`, so this sees what S3 bills (the `CountingStore` sits above object_store's retry layer and
//! cannot see retries). Two jobs:
//!
//! * count each request by price class: PUT tier (PUT, COPY, POST, LIST), GET tier (GET, HEAD, other), DELETE (free);
//!   `POST ?delete` (DeleteObjects) is kept apart as "delete_post" and counted at the PUT price in every cap, because
//!   whether it is free was not verified;
//! * limit the rate: a token bucket over all S3 requests of the process. With the rate limit R, the cost that can
//!   accrue between two looks of a spend guard is at most R x interval x price, whatever the workload does.
//!
//! Requests to the instance-metadata service (credentials) are neither counted nor limited.
//! Counters are written to `$S3T_COUNT_DIR/http-<port>.json` once a second and reloaded at start, so a restart of the
//! process continues the totals. An optional per-process cost cap exits the process (code 86) when exceeded.
use async_trait::async_trait;
use object_store::client::{ClientOptions, HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub const PUT_USD: f64 = 5.4e-6; // eu-central-1: $0.0054 per 1,000 (s3-test/prices.json)
pub const GET_USD: f64 = 4.3e-7; // eu-central-1: $0.0043 per 10,000

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Class {
    Put,
    Get,
    Delete,
    DeletePost,
    Metadata,
}

pub fn classify(method: &str, host: &str, query: &str) -> Class {
    if host.starts_with("169.254.") {
        return Class::Metadata;
    }
    let has = |k: &str| query.split('&').any(|p| p == k || p.starts_with(&format!("{k}=")));
    match method {
        "GET" | "HEAD" => {
            if has("list-type") || has("uploads") {
                Class::Put
            } else {
                Class::Get
            }
        }
        "PUT" => Class::Put,
        "POST" => {
            if has("delete") {
                Class::DeletePost
            } else {
                Class::Put
            }
        }
        "DELETE" => Class::Delete,
        _ => Class::Get,
    }
}

#[derive(Debug)]
pub struct S3Guard {
    pub put: AtomicU64,
    pub get: AtomicU64,
    pub delete: AtomicU64,
    pub delete_post: AtomicU64,
    pub metadata: AtomicU64,
    pub throttled: AtomicU64,
    pub waited_us: AtomicU64,
    max_rps: f64,
    bucket: Mutex<(f64, Instant)>,
    process_cap_usd: f64,
}

static GUARD: OnceLock<Arc<S3Guard>> = OnceLock::new();

/// The process-wide guard, if `--store s3-aws` created one.
pub fn global() -> Option<&'static Arc<S3Guard>> {
    GUARD.get()
}

fn field(s: &str, key: &str) -> u64 {
    s.split(&format!("\"{key}\":")).nth(1).map(|r| r.chars().take_while(|c| c.is_ascii_digit()).collect::<String>()).and_then(|d| d.parse().ok()).unwrap_or(0)
}

impl S3Guard {
    pub fn new(max_rps: f64, process_cap_usd: f64) -> S3Guard {
        S3Guard {
            put: AtomicU64::new(0), get: AtomicU64::new(0), delete: AtomicU64::new(0), delete_post: AtomicU64::new(0), metadata: AtomicU64::new(0),
            throttled: AtomicU64::new(0), waited_us: AtomicU64::new(0), max_rps, bucket: Mutex::new((max_rps, Instant::now())), process_cap_usd,
        }
    }

    /// `max_rps` = 0 means unlimited (not used on real S3). `process_cap_usd` = 0 means no cap.
    pub fn install(max_rps: f64, process_cap_usd: f64, port: &str) -> Arc<S3Guard> {
        let g = Arc::new(S3Guard::new(max_rps, process_cap_usd));
        let file = std::env::var("S3T_COUNT_DIR").ok().map(|d| PathBuf::from(d).join(format!("http-{port}.json")));
        if let Some(f) = &file {
            if let Ok(s) = std::fs::read_to_string(f) {
                g.put.store(field(&s, "put"), Relaxed);
                g.get.store(field(&s, "get"), Relaxed);
                g.delete.store(field(&s, "delete"), Relaxed);
                g.delete_post.store(field(&s, "deletePost"), Relaxed);
            }
        }
        let me = Arc::clone(&g);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Some(f) = &file {
                    let tmp = f.with_extension("tmp");
                    if std::fs::write(&tmp, me.json()).is_ok() {
                        let _ = std::fs::rename(&tmp, f);
                    }
                }
                if me.process_cap_usd > 0.0 && me.cost_usd() > me.process_cap_usd {
                    eprintln!("s3guard: process cap of ${} exceeded (${:.4}), exiting", me.process_cap_usd, me.cost_usd());
                    std::process::exit(86);
                }
            }
        });
        let _ = GUARD.set(Arc::clone(&g));
        g
    }

    /// PUT tier with `POST ?delete` counted at the PUT price, GET tier.
    pub fn billed(&self) -> (u64, u64) {
        (self.put.load(Relaxed) + self.delete_post.load(Relaxed), self.get.load(Relaxed))
    }

    pub fn cost_usd(&self) -> f64 {
        let (p, g) = self.billed();
        p as f64 * PUT_USD + g as f64 * GET_USD
    }

    pub fn json(&self) -> String {
        format!(
            "{{\"put\":{},\"get\":{},\"delete\":{},\"deletePost\":{},\"metadata\":{},\"throttled\":{},\"waitedUs\":{}}}",
            self.put.load(Relaxed), self.get.load(Relaxed), self.delete.load(Relaxed), self.delete_post.load(Relaxed),
            self.metadata.load(Relaxed), self.throttled.load(Relaxed), self.waited_us.load(Relaxed)
        )
    }

    /// Token bucket: `max_rps` tokens per second, burst of one second. Returns how long the caller had to wait.
    async fn acquire(&self) -> Duration {
        if self.max_rps <= 0.0 {
            return Duration::ZERO;
        }
        let wait = {
            let mut b = self.bucket.lock().unwrap();
            let now = Instant::now();
            let refilled = (b.0 + now.duration_since(b.1).as_secs_f64() * self.max_rps).min(self.max_rps);
            b.1 = now;
            b.0 = refilled - 1.0; // may go negative: later callers queue behind it
            if b.0 >= 0.0 { Duration::ZERO } else { Duration::from_secs_f64(-b.0 / self.max_rps) }
        };
        if !wait.is_zero() {
            self.throttled.fetch_add(1, Relaxed);
            self.waited_us.fetch_add(wait.as_micros() as u64, Relaxed);
            tokio::time::sleep(wait).await;
        }
        wait
    }
}

#[derive(Debug)]
struct GuardService {
    inner: HttpClient,
    guard: Arc<S3Guard>,
}

#[async_trait]
impl HttpService for GuardService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let class = classify(req.method().as_str(), req.uri().host().unwrap_or(""), req.uri().query().unwrap_or(""));
        let g = &self.guard;
        if class != Class::Metadata {
            g.acquire().await;
        }
        let counter = match class {
            Class::Put => &g.put,
            Class::Get => &g.get,
            Class::Delete => &g.delete,
            Class::DeletePost => &g.delete_post,
            Class::Metadata => &g.metadata,
        };
        counter.fetch_add(1, Relaxed); // counted before the call: a failed attempt is a request too
        self.inner.execute(req).await
    }
}

#[derive(Debug)]
pub struct GuardConnector(pub Arc<S3Guard>);

impl HttpConnector for GuardConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = ReqwestConnector::default().connect(options)?;
        Ok(HttpClient::new(GuardService { inner, guard: Arc::clone(&self.0) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes() {
        assert_eq!(classify("PUT", "b.s3.eu-central-1.amazonaws.com", ""), Class::Put);
        assert_eq!(classify("GET", "b.s3.eu-central-1.amazonaws.com", ""), Class::Get);
        assert_eq!(classify("HEAD", "b.s3.eu-central-1.amazonaws.com", ""), Class::Get);
        assert_eq!(classify("GET", "b.s3.eu-central-1.amazonaws.com", "list-type=2&prefix=x"), Class::Put);
        assert_eq!(classify("POST", "b.s3.eu-central-1.amazonaws.com", "delete"), Class::DeletePost);
        assert_eq!(classify("POST", "b.s3.eu-central-1.amazonaws.com", "uploads"), Class::Put);
        assert_eq!(classify("POST", "b.s3.eu-central-1.amazonaws.com", "uploadId=1"), Class::Put);
        assert_eq!(classify("DELETE", "b.s3.eu-central-1.amazonaws.com", ""), Class::Delete);
        assert_eq!(classify("PUT", "169.254.169.254", ""), Class::Metadata);
        assert_eq!(classify("GET", "169.254.169.254", ""), Class::Metadata);
    }

    #[tokio::test]
    async fn limiter_holds_the_rate() {
        let g = S3Guard::new(200.0, 0.0);
        let t0 = Instant::now();
        for _ in 0..600 {
            g.acquire().await;
        }
        let el = t0.elapsed().as_secs_f64();
        // 600 requests at 200/s with a one-second burst: at least 2 s, and not much more than 3 s.
        assert!(el >= 1.95 && el < 3.5, "elapsed {el}");
    }

    #[test]
    fn json_roundtrip() {
        let g = S3Guard::new(0.0, 0.0);
        g.put.store(7, Relaxed);
        g.delete_post.store(3, Relaxed);
        let s = g.json();
        assert_eq!(field(&s, "put"), 7);
        assert_eq!(field(&s, "deletePost"), 3);
        assert_eq!(field(&s, "get"), 0);
    }
}
