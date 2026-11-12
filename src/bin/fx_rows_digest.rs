//! Digest of every row a database's newest version of our manifest holds (generator-1tb, the determinism proof): the
//! sealed runs of every group read in key order, with the end-of-run checks of `RunStream`.
//! Prints one JSON line: rows, deleted rows, a 128-bit digest of (key, seq, envelope) of every row in key order, the same
//! without seq, and a digest of the group layout (each group's first key and row count). Two databases with the same
//! `rows_digest` hold the same rows; the same `groups_digest` too means the same groups. Two databases created
//! separately differ in their collection UUIDs (`meta/cuuid/`, random at creation): compare them with
//! `rows_digest_without_collection_uuids`.
//! Usage: fx_rows_digest <endpoint> <bucket> <db>   (MinIO credentials minioadmin; S3 with AWS_* from the environment
//! when endpoint is "s3")
//!        fx_rows_digest <endpoint> <bucket> <db> <other bucket>   prints the first rows that differ between the two
//!        (same database name in both buckets), up to 10, and the count of differing rows
use std::sync::Arc;
use tenant_bench::fx::manifest;
use tenant_bench::runfmt::RunStream;
use twox_hash::XxHash3_128;

fn store_of(endpoint: &str, bucket: &str) -> anyhow::Result<Arc<dyn object_store::ObjectStore>> {
    let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
    if endpoint != "s3" {
        b = b.with_endpoint(endpoint).with_access_key_id("minioadmin").with_secret_access_key("minioadmin").with_region("us-east-1").with_allow_http(true);
    }
    Ok(Arc::new(b.build()?))
}

/// Every row of the newest version, in key order.
struct Rows {
    store: Arc<dyn object_store::ObjectStore>,
    runs: std::collections::VecDeque<(String, u64)>,
    cur: Option<RunStream>,
}

impl Rows {
    async fn open(store: Arc<dyn object_store::ObjectStore>, id: &str) -> anyhow::Result<Rows> {
        let n = *manifest::list_versions(&store, id).await?.last().ok_or_else(|| anyhow::anyhow!("no version of {id}"))?;
        let v = manifest::load(&store, id, n).await?;
        Ok(Rows { store, runs: v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| (r.path.clone(), r.size))).collect(), cur: None })
    }
    async fn next(&mut self) -> anyhow::Result<Option<tenant_bench::runfmt::Row>> {
        loop {
            if let Some(s) = self.cur.as_mut() {
                if let Some(r) = s.next().await? {
                    return Ok(Some(r));
                }
                self.cur = None;
            }
            let Some((p, size)) = self.runs.pop_front() else { return Ok(None) };
            self.cur = Some(RunStream::open(Arc::clone(&self.store), object_store::path::Path::from(p.as_str()), size, 8 << 20).await?);
        }
    }
}

fn show(b: &[u8]) -> String {
    b.iter().map(|c| if c.is_ascii_graphic() { (*c as char).to_string() } else { format!("\\x{c:02x}") }).collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    anyhow::ensure!(a.len() == 4 || a.len() == 5, "usage: fx_rows_digest <endpoint|s3> <bucket> <db> [<other bucket>]");
    let store = store_of(&a[1], &a[2])?;
    let id = &a[3];
    if a.len() == 5 {
        let mut x = Rows::open(store, id).await?;
        let mut y = Rows::open(store_of(&a[1], &a[4])?, id).await?;
        let (mut shown, mut diff, mut n) = (0, 0u64, 0u64);
        loop {
            let (r, s) = (x.next().await?, y.next().await?);
            if r.is_none() && s.is_none() {
                break;
            }
            n += 1;
            let same = match (&r, &s) {
                (Some(r), Some(s)) => r.key == s.key && r.env == s.env && r.seq == s.seq,
                _ => false,
            };
            if !same {
                diff += 1;
                if shown < 10 {
                    shown += 1;
                    for (w, row) in [("A", &r), ("B", &s)] {
                        match row {
                            Some(row) => println!("{w} row {n}: key {} seq {} env({}) {}", show(&row.key), row.seq, row.env.len(), show(&row.env[..row.env.len().min(80)])),
                            None => println!("{w} row {n}: none"),
                        }
                    }
                }
            }
        }
        println!("rows {n}, differing at the same position {diff}");
        return Ok(());
    }
    let versions = manifest::list_versions(&store, id).await?;
    let n = *versions.last().ok_or_else(|| anyhow::anyhow!("no version of {id}"))?;
    let v = manifest::load(&store, id, n).await?;
    anyhow::ensure!(v.delta.is_empty(), "version {n} has delta runs: this digest reads sealed runs only");
    let (mut h, mut h_noseq, mut hg, mut h_nouuid) = (XxHash3_128::new(), XxHash3_128::new(), XxHash3_128::new(), XxHash3_128::new());
    let (mut rows, mut deleted, mut bytes) = (0u64, 0u64, 0u64);
    let mut last: Option<Vec<u8>> = None;
    for g in &v.groups {
        let mut grows = 0u64;
        if let Some(r) = &g.sealed {
            let mut s = RunStream::open(Arc::clone(&store), object_store::path::Path::from(r.path.as_str()), r.size, 8 << 20).await?;
            while let Some(row) = s.next().await? {
                anyhow::ensure!(last.as_ref().is_none_or(|l| l < &row.key), "rows out of order across groups");
                for x in [&(row.key.len() as u32).to_le_bytes()[..], &row.key, &(row.env.len() as u32).to_le_bytes(), &row.env] {
                    h.write(x);
                    h_noseq.write(x);
                }
                h.write(&row.seq.to_le_bytes());
                // collection UUIDs are random at every createIndexes: two separately created databases differ there only
                if !row.key.starts_with(b"meta/cuuid/") {
                    for x in [&(row.key.len() as u32).to_le_bytes()[..], &row.key, &(row.env.len() as u32).to_le_bytes(), &row.env, &row.seq.to_le_bytes()] {
                        h_nouuid.write(x);
                    }
                }
                rows += 1;
                grows += 1;
                deleted += row.is_deleted() as u64;
                bytes += (row.key.len() + row.env.len()) as u64;
                last = Some(row.key);
            }
        }
        hg.write(g.lo.as_bytes());
        hg.write(&grows.to_le_bytes());
    }
    println!(
        "{}",
        serde_json::json!({
            "db": id, "version": n, "x_d": v.x_d, "groups": v.groups.len(), "rows": rows, "deleted": deleted, "bytes": bytes,
            "rows_digest": format!("{:032x}", h.finish_128()), "rows_digest_without_seq": format!("{:032x}", h_noseq.finish_128()),
            "rows_digest_without_collection_uuids": format!("{:032x}", h_nouuid.finish_128()),
            "groups_digest": format!("{:032x}", hg.finish_128()),
        })
    );
    Ok(())
}
