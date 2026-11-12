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

/// Whether `hay` contains one of `needles`: a set lookup at each position whose first byte starts some needle (the
/// benchmark's ids have one length and two first bytes, raw and hex), so one pass over the bytes, not one per needle.
fn holds(hay: &[u8], needles: &[Vec<u8>]) -> bool {
    use std::collections::{HashMap, HashSet};
    thread_local! {
        static IDX: std::cell::RefCell<Option<(usize, [bool; 256], HashMap<usize, HashSet<Vec<u8>>>)>> = const { std::cell::RefCell::new(None) };
    }
    IDX.with(|c| {
        let mut c = c.borrow_mut();
        if c.as_ref().is_none_or(|(n, _, _)| *n != needles.len()) {
            let mut first = [false; 256];
            let mut by_len: HashMap<usize, HashSet<Vec<u8>>> = HashMap::new();
            for n in needles.iter().filter(|n| !n.is_empty()) {
                first[n[0] as usize] = true;
                by_len.entry(n.len()).or_default().insert(n.clone());
            }
            *c = Some((needles.len(), first, by_len));
        }
        let (_, first, by_len) = c.as_ref().unwrap();
        for i in 0..hay.len() {
            if !first[hay[i] as usize] {
                continue;
            }
            for (l, set) in by_len {
                if i + l <= hay.len() && set.contains(&hay[i..i + l]) {
                    return true;
                }
            }
        }
        false
    })
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
    // the runs the newest version of our manifest lists (a run holding an erased id is then readable data, not an orphan)
    let newest = metas.iter().filter(|m| m.location.as_ref().contains("/fx/manifest/")).map(|m| m.location.clone()).max_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let mut live: std::collections::HashSet<String> = Default::default();
    if let Some(p) = &newest {
        let v: tenant_bench::fx::manifest::Version = serde_json::from_slice(&store.get(p).await?.bytes().await?)?;
        live.extend(v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| s.path.clone())));
        live.extend(v.delta.iter().map(|d| d.path.clone()));
    }
    let mut live_holding = 0u64;
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
            // deleted since the listing (a server still running): nothing left to hold
            let run = match runfmt::Run::open(&store, m.location.clone(), m.size).await {
                Ok((r, _)) => r,
                Err(_) if store.head(&m.location).await.is_err() => continue,
                Err(e) => return Err(e),
            };
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
            let bytes = match store.get(&m.location).await {
                Ok(g) => g.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(e.into()),
            };
            if holds(&bytes, &all) {
                rows = 1;
            }
        }
        if rows > 0 {
            if live.contains(&p) {
                live_holding += 1;
            }
            e.1 += 1;
            e.2 += rows;
            if found.len() < 20 {
                found.push(p.clone());
            }
        }
    }
    let ours: u64 = out.iter().filter(|(k, _)| k.starts_with("fx")).map(|(_, v)| v.1).sum();
    let per: serde_json::Map<String, serde_json::Value> = out.iter().map(|(k, v)| (k.clone(), serde_json::json!({"objects": v.0, "objects_holding": v.1, "rows_holding": v.2, "bytes": v.3}))).collect();
    println!("{}", serde_json::json!({"db": db, "ids": ids.len(), "our_objects_holding": ours, "live_runs_holding": live_holding, "newest_version": newest.map(|p| p.to_string()), "per_prefix": per, "examples": found}));
    Ok(())
}
