//! Request-counter check against real S3: issues exactly N PUTs and N GETs through the engine's `CountingStore`
//! and compares the counter with N. No endpoint and no keys: credentials come from the instance profile (IMDSv2).
//!
//!     s3probe <bucket> <region> [n=1000] [prefix=_s3test/counter-check]
//!
//! Prints one JSON object; exit code 0 only if put == n, get == n and every other counter is 0 (CountingStore), and the
//! HTTP-level counts are reported next to them: the difference is retries, which only the HTTP layer sees. The cost
//! figures billedPutTier/billedGetTier take the larger of the two.
use anyhow::Result;
use futures::stream::{self, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;
use tenant_bench::counting::CountingStore;

#[path = "../../engine/s3guard.rs"]
mod s3guard;

#[tokio::main]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let bucket = a.get(1).expect("bucket");
    let region = a.get(2).expect("region");
    let n: u64 = a.get(3).map(|s| s.parse()).transpose()?.unwrap_or(1000);
    let prefix = a.get(4).cloned().unwrap_or_else(|| "_s3test/counter-check".into());
    // Same HTTP-level guard as the engine: counts every attempt (retries included) and limits the rate.
    let rps: f64 = std::env::var("S3T_MAX_RPS").ok().and_then(|v| v.parse().ok()).unwrap_or(300.0);
    let guard = s3guard::S3Guard::install(rps, 0.0, "probe");
    let mut b = AmazonS3Builder::new().with_region(region).with_bucket_name(bucket).with_http_connector(s3guard::GuardConnector(guard.clone()));
    // Local check against a fake S3 only (s3-test/local/fake_s3.py); never set in the workflow.
    if let Ok(ep) = std::env::var("S3T_PROBE_ENDPOINT") {
        b = b.with_endpoint(ep).with_allow_http(true).with_access_key_id("test").with_secret_access_key("test");
    }
    let s3: Arc<dyn ObjectStore> = Arc::new(b.build()?);
    let store = Arc::new(CountingStore::new(s3));
    let body = vec![b'x'; 1024];
    let t0 = std::time::Instant::now();
    let puts: Vec<_> = stream::iter(0..n)
        .map(|i| {
            let s = Arc::clone(&store);
            let b = body.clone();
            let key = format!("{prefix}/{i}");
            async move { s.put(&Path::from(key), b.into()).await.map(|_| ()) }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    let put_errors = puts.iter().filter(|r| r.is_err()).count();
    let gets: Vec<_> = stream::iter(0..n)
        .map(|i| {
            let s = Arc::clone(&store);
            let key = format!("{prefix}/{i}");
            async move { s.get(&Path::from(key)).await?.bytes().await.map(|b| b.len()) }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    let get_errors = gets.iter().filter(|r| r.is_err()).count();
    let s = store.counters.snapshot();
    let ok = put_errors == 0 && get_errors == 0 && s.put == n && s.get == n && s.get_range == 0 && s.head == 0 && s.list == 0 && s.copy == 0 && s.delete == 0 && s.multipart_create == 0;
    let (http_put, http_get) = guard.billed();
    println!("{}", serde_json::json!({
        "expected": {"put": n, "get": n}, "counter": s, "putErrors": put_errors, "getErrors": get_errors,
        "http": serde_json::from_str::<serde_json::Value>(&guard.json())?, "httpBilledPutTier": http_put, "httpBilledGetTier": http_get,
        "retriesSeenOnlyAtHttpLayer": {"putTier": http_put as i64 - s.billed_put_tier() as i64, "getTier": http_get as i64 - s.billed_get_tier() as i64},
        "billedPutTier": http_put.max(s.billed_put_tier()), "billedGetTier": http_get.max(s.billed_get_tier()),
        "secs": t0.elapsed().as_secs_f64(), "counterExact": ok,
    }));
    std::process::exit(if ok { 0 } else { 1 })
}
