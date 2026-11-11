//! perf-erase: an independent check of what is left of erased customers in a database's objects.
//!
//! Usage: `fx_erase_proof <endpoint> <bucket> <db> <ids-file>` (MinIO or S3 credentials from AWS_ACCESS_KEY_ID /
//! AWS_SECRET_ACCESS_KEY). `ids-file`: one erased customer value per line (the benchmark's thread ids). Every object
//! under `tenants/{db}/` is read: our runs (`fx/run/`) are opened and every block decoded, and a row whose key or
//! envelope holds an erased id counts; every other object is searched raw for the id and for its hex (manifests name keys
//! in hex). Prints one JSON line: objects and rows found per prefix. Our objects (`fx/`) must reach 0 once every erasure is
//! done; SlateDB's (`wal/`, `compacted/`) are reported as they are (the WAL is kept 30 days on purpose; the drop of
//! absorbed SlateDB rows is off by default).
use anyhow::Result;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use tenant_bench::runfmt;

fn holds(hay: &[u8], needles: &[Vec<u8>]) -> bool {
    needles.iter().any(|n| hay.windows(n.len()).any(|w| w == &n[..]))
}

#[tokio::main]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    anyhow::ensure!(a.len() == 5, "usage: fx_erase_proof <endpoint> <bucket> <db> <ids-file>");
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env().with_endpoint(&a[1]).with_bucket_name(&a[2]).with_allow_http(true).with_region(std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())).build()?,
    );
    let db = &a[3];
    let ids: Vec<Vec<u8>> = std::fs::read_to_string(&a[4])?.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).map(|l| l.as_bytes().to_vec()).collect();
    anyhow::ensure!(!ids.is_empty(), "no ids");
    let hexes: Vec<Vec<u8>> = ids.iter().map(|i| i.iter().map(|b| format!("{b:02x}")).collect::<String>().into_bytes()).collect();
    let mut all = ids.clone();
    all.extend(hexes);
    let metas: Vec<_> = store.list(Some(&Path::from(format!("tenants/{db}/")))).try_collect().await?;
    // per top-level prefix under the tenant: (objects, objects holding an id, rows holding an id, bytes)
    let mut out: BTreeMap<String, (u64, u64, u64, u64)> = BTreeMap::new();
    let mut found: Vec<String> = Vec::new();
    for m in metas {
        let p = m.location.to_string();
        let rest = p.strip_prefix(&format!("tenants/{db}/")).unwrap_or(&p);
        let parts: Vec<&str> = rest.split('/').collect();
        let key = if parts[0] == "fx" && parts.len() > 2 { format!("fx/{}", parts[1]) } else { parts[0].to_string() };
        let e = out.entry(key.clone()).or_default();
        e.0 += 1;
        e.3 += m.size;
        let mut rows = 0u64;
        if key == "fx/run" {
            let (run, _) = runfmt::Run::open(&store, m.location.clone(), m.size).await?;
            let n = run.fence.blocks();
            let mut b = 0;
            while b < n {
                let z = (b + 63).min(n - 1);
                let (lo, hi) = run.fence.range(b, z);
                let data = runfmt::get_range(&store, &run.path, lo, hi).await?;
                for i in b..=z {
                    let (x, y) = run.fence.range(i, i);
                    let blk = runfmt::decode_checked(&run, i, &data[(x - lo) as usize..(y - lo) as usize])?;
                    for r in 0..blk.len() {
                        if holds(blk.key(r), &ids) || holds(blk.env(r), &ids) {
                            rows += 1;
                        }
                    }
                }
                b = z + 1;
            }
        } else {
            let bytes = store.get(&m.location).await?.bytes().await?;
            if holds(&bytes, &all) {
                rows = 1;
            }
        }
        if rows > 0 {
            e.1 += 1;
            e.2 += rows;
            if found.len() < 20 {
                found.push(p.clone());
            }
        }
    }
    let ours: u64 = out.iter().filter(|(k, _)| k.starts_with("fx")).map(|(_, v)| v.1).sum();
    let per: serde_json::Map<String, serde_json::Value> = out.iter().map(|(k, v)| (k.clone(), serde_json::json!({"objects": v.0, "objects_holding": v.1, "rows_holding": v.2, "bytes": v.3}))).collect();
    println!("{}", serde_json::json!({"db": db, "ids": ids.len(), "our_objects_holding": ours, "per_prefix": per, "examples": found}));
    Ok(())
}
