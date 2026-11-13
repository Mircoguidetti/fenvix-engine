//! Reviewer tool (generator-1tb review, 2026-11-13). Built as `src/bin/review_rows_cmp.rs` on generator-1tb 148bee9 in a
//! scratch copy, not committed there. Compares the newest versions of one database in two buckets row by row, by key,
//! ignoring seq and the collection UUID rows (`meta/cuuid/`, random at every createIndexes). Prints, per key prefix, the
//! rows that are the same, only in A, only in B, and with a different stored value (envelope), with examples.
//! Usage: review_rows_cmp <endpoint> <bucket A> <bucket B> <db>   (MinIO credentials minioadmin)
use std::collections::BTreeMap;
use std::sync::Arc;
use tenant_bench::fx::manifest;
use tenant_bench::runfmt::RunStream;

fn store_of(endpoint: &str, bucket: &str) -> anyhow::Result<Arc<dyn object_store::ObjectStore>> {
    let b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket).with_endpoint(endpoint).with_access_key_id("minioadmin").with_secret_access_key("minioadmin").with_region("us-east-1").with_allow_http(true);
    Ok(Arc::new(b.build()?))
}

struct Rows {
    store: Arc<dyn object_store::ObjectStore>,
    runs: std::collections::VecDeque<(String, u64)>,
    cur: Option<RunStream>,
}

impl Rows {
    async fn open(store: Arc<dyn object_store::ObjectStore>, id: &str) -> anyhow::Result<(Rows, String)> {
        let n = *manifest::list_versions(&store, id).await?.last().ok_or_else(|| anyhow::anyhow!("no version of {id}"))?;
        let v = manifest::load(&store, id, n).await?;
        anyhow::ensure!(v.delta.is_empty(), "version {n} has delta runs");
        let info = format!("version {n} x_d {} groups {}", v.x_d, v.groups.len());
        Ok((Rows { store, runs: v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|r| (r.path.clone(), r.size))).collect(), cur: None }, info))
    }
    async fn next(&mut self) -> anyhow::Result<Option<tenant_bench::runfmt::Row>> {
        loop {
            if let Some(s) = self.cur.as_mut() {
                if let Some(r) = s.next().await? {
                    if r.key.starts_with(b"meta/cuuid/") {
                        continue;
                    }
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
    b.iter().take(90).map(|c| if c.is_ascii_graphic() { (*c as char).to_string() } else { format!("\\x{c:02x}") }).collect()
}

fn prefix(k: &[u8]) -> String {
    if k.starts_with(b"u/") {
        return "u/ (customer rows)".into();
    }
    let s = String::from_utf8_lossy(k);
    let mut it = s.splitn(3, '/');
    format!("{}/{}", it.next().unwrap_or(""), it.next().unwrap_or(""))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    anyhow::ensure!(a.len() == 5, "usage: review_rows_cmp <endpoint> <bucket A> <bucket B> <db>");
    let (mut x, ia) = Rows::open(store_of(&a[1], &a[2])?, &a[4]).await?;
    let (mut y, ib) = Rows::open(store_of(&a[1], &a[3])?, &a[4]).await?;
    println!("A {ia}; B {ib}");
    let mut counts: BTreeMap<String, [u64; 4]> = BTreeMap::new(); // same, only A, only B, value differs
    let mut ex: Vec<String> = Vec::new();
    let (mut r, mut s) = (x.next().await?, y.next().await?);
    loop {
        match (&r, &s) {
            (None, None) => break,
            (Some(p), Some(q)) if p.key == q.key => {
                let e = counts.entry(prefix(&p.key)).or_default();
                if p.env == q.env {
                    e[0] += 1;
                } else {
                    e[3] += 1;
                    if ex.len() < 24 {
                        ex.push(format!("VALUE {} | A seq {} {} | B seq {} {}", show(&p.key), p.seq, show(&p.env), q.seq, show(&q.env)));
                    }
                }
                r = x.next().await?;
                s = y.next().await?;
            }
            (Some(p), q) if q.as_ref().is_none_or(|q| p.key < q.key) => {
                counts.entry(prefix(&p.key)).or_default()[1] += 1;
                if ex.len() < 24 {
                    ex.push(format!("ONLY A {} | {}", show(&p.key), show(&p.env)));
                }
                r = x.next().await?;
            }
            (_, Some(q)) => {
                counts.entry(prefix(&q.key)).or_default()[2] += 1;
                if ex.len() < 24 {
                    ex.push(format!("ONLY B {} | {}", show(&q.key), show(&q.env)));
                }
                s = y.next().await?;
            }
            _ => unreachable!(),
        }
    }
    for (p, c) in &counts {
        println!("{p}: same {} only_A {} only_B {} value_differs {}", c[0], c[1], c[2], c[3]);
    }
    for e in ex {
        println!("{e}");
    }
    Ok(())
}
