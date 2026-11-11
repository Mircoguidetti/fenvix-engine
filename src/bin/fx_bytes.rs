//! Byte accounting of one database in the store (P-perf-bytes, 2026-11-11): where every stored byte goes.
//!
//! Read-only: it lists objects, loads our manifest versions, reads every row of the runs the newest version lists, and
//! scans SlateDB through a follow reader (no checkpoint, no write). Prints one JSON document.
//!
//!   fx_bytes --endpoint http://172.31.77.10:9000 --bucket fxbench --db app_demo [--slate] [--retained-rows]
//!
//! Credentials come from the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, MinIO's local ones in the bench).
//!
//! What it reports:
//! - every object under `tenants/{db}/`, by kind (the second path component), and the `fx/` objects split into: runs the
//!   newest version lists (live), runs only older retained versions list (kept for the 7-day version retention or by a
//!   pair or reference), runs nothing lists (waiting for the GC grace), manifests, other;
//! - for the live runs: sealed and delta, codec, derived flag, stored and raw bytes, and per key class (documents,
//!   index entries, locators, message rows...) rows, key bytes and envelope bytes by envelope type;
//! - SlateDB (`--slate`): SSTs the latest manifest lists against `compacted/` objects it does not list; every row through
//!   a follow reader, split by seq at or below the newest version's X_d (absorbed: also held by our runs) or above, with
//!   bytes per key class and what lz4 and zstd-3 would make of them in 4 KiB blocks (SlateDB's block size here).
use anyhow::{Context, Result};
use clap::Parser;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tenant_bench::fx::manifest::{self, Version};
use tenant_bench::runfmt::{RunStream, DELETED, DERIVED, LIVE, PART};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    bucket: String,
    #[arg(long, default_value = "eu-west-1")]
    region: String,
    #[arg(long, default_value = "app_demo")]
    db: String,
    /// Also scan SlateDB's rows (a full read of the database through a follow reader).
    #[arg(long)]
    slate: bool,
    /// Also read the rows of the runs only older versions list.
    #[arg(long)]
    retained_rows: bool,
    /// Print this many example keys per class.
    #[arg(long, default_value_t = 0)]
    sample: usize,
}

/// Class of a physical key. Layout v2 (`mongo-server/src/custkey.rs`, `layout2.rs`): the customer space starts with
/// `u/`; the rest is the plain layout (`doc/`, `idx/`, ...). Message rows of derived documents carry `\0fxm/` after the
/// customer prefix (`fx/derive.rs`). The class is the readable head of the key, digits and binary parts cut away.
fn class(key: &[u8]) -> String {
    if key.windows(5).any(|w| w == b"\0fxm/") {
        return "message-row".into();
    }
    let mut s = String::new();
    let mut slashes = 0;
    for &b in key.iter().take(48) {
        if b == b'/' {
            slashes += 1;
            s.push('/');
            if slashes >= 3 {
                break;
            }
        } else if b.is_ascii_alphabetic() || b == b'_' || b == b'.' || b == b'-' {
            s.push(b as char);
        } else {
            s.push('#');
            break;
        }
    }
    s
}

fn esc(k: &[u8]) -> String {
    k.iter().take(64).map(|&b| if (0x20..0x7f).contains(&b) { (b as char).to_string() } else { format!("\\x{b:02x}") }).collect()
}

#[derive(Default, Clone)]
struct Acc {
    rows: u64,
    key: u64,
    val: u64,
    by_env: BTreeMap<&'static str, (u64, u64)>,
}

impl Acc {
    fn add(&mut self, k: usize, v: usize, env: &'static str) {
        self.rows += 1;
        self.key += k as u64;
        self.val += v as u64;
        let e = self.by_env.entry(env).or_default();
        e.0 += 1;
        e.1 += (k + v) as u64;
    }
    fn json(&self) -> Value {
        json!({"rows": self.rows, "key_bytes": self.key, "value_bytes": self.val,
               "by_envelope": self.by_env.iter().map(|(k, (n, b))| (k.to_string(), json!({"rows": n, "bytes": b}))).collect::<serde_json::Map<_, _>>()})
    }
}

fn env_name(e: &[u8]) -> &'static str {
    match e.first() {
        Some(&LIVE) => "live",
        Some(&DELETED) => "deleted",
        Some(&DERIVED) => "derived",
        Some(&PART) => "message",
        _ => "other",
    }
}

/// What lz4 and zstd-3 make of a stream of rows cut in blocks of `block` bytes.
struct Comp {
    block: usize,
    buf: Vec<u8>,
    raw: u64,
    lz4: u64,
    zstd: u64,
}

impl Comp {
    fn new(block: usize) -> Comp {
        Comp { block, buf: Vec::new(), raw: 0, lz4: 0, zstd: 0 }
    }
    fn push(&mut self, k: &[u8], v: &[u8]) {
        self.buf.extend_from_slice(k);
        self.buf.extend_from_slice(v);
        if self.buf.len() >= self.block {
            self.flush();
        }
    }
    fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        self.raw += self.buf.len() as u64;
        self.lz4 += lz4_flex::block::compress(&self.buf).len() as u64;
        self.zstd += zstd::bulk::compress(&self.buf, 3).map(|c| c.len()).unwrap_or(self.buf.len()) as u64;
        self.buf.clear();
    }
    fn json(&mut self) -> Value {
        self.flush();
        json!({"raw": self.raw, "lz4": self.lz4, "zstd3": self.zstd, "block": self.block})
    }
}

fn runs_of(v: &Version) -> Vec<(String, u64, &'static str)> {
    v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| (s.path.clone(), s.size, "sealed"))).chain(v.delta.iter().map(|d| (d.path.clone(), d.size, "delta"))).collect()
}

async fn run_detail(store: &Arc<dyn ObjectStore>, path: &str, size: u64, sample: usize, samples: &mut HashMap<String, Vec<String>>, classes: &mut BTreeMap<String, Acc>) -> Result<Value> {
    let mut rs = RunStream::open(Arc::clone(store), Path::from(path), size, 16 << 20).await.with_context(|| format!("run {path}"))?;
    let meta = json!({"path": path, "size": size, "raw": rs.run.raw_bytes, "entries": rs.run.entries, "deleted": rs.run.deleted,
        "codec": format!("{:?}", rs.run.codec), "version": rs.run.version, "derived": rs.run.derived, "derived_rows": rs.run.derived_rows,
        "block_bytes": rs.run.block_bytes, "min_seq": rs.run.min_seq, "max_seq": rs.run.max_seq});
    let mut local: BTreeMap<String, Acc> = BTreeMap::new();
    while let Some(r) = rs.next().await? {
        let c = class(&r.key);
        if sample > 0 {
            let s = samples.entry(c.clone()).or_default();
            if s.len() < sample {
                s.push(esc(&r.key));
            }
        }
        let e = env_name(&r.env);
        local.entry(c.clone()).or_default().add(r.key.len(), r.env.len(), e);
        classes.entry(c).or_default().add(r.key.len(), r.env.len(), e);
    }
    let mut m = meta;
    m["classes"] = local.iter().map(|(k, a)| (k.clone(), a.json())).collect::<serde_json::Map<_, _>>().into();
    Ok(m)
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = Args::parse();
    let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&a.bucket).with_region(&a.region).with_endpoint(&a.endpoint);
    if a.endpoint.starts_with("http://") {
        b = b.with_allow_http(true);
    }
    let store: Arc<dyn ObjectStore> = Arc::new(b.build()?);
    let root = format!("tenants/{}", a.db);
    let objs: Vec<_> = store.list(Some(&Path::from(root.as_str()))).try_collect().await?;
    let mut by_kind: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut fx_objs: HashMap<String, u64> = HashMap::new();
    let mut compacted: HashMap<String, u64> = HashMap::new();
    for o in &objs {
        let p = o.location.to_string();
        let rest = p.strip_prefix(&format!("{root}/")).unwrap_or(&p);
        let kind = rest.split('/').next().unwrap_or("").to_string();
        let e = by_kind.entry(kind.clone()).or_default();
        e.0 += 1;
        e.1 += o.size;
        if kind == "fx" {
            fx_objs.insert(p.clone(), o.size);
        }
        if kind == "compacted" {
            compacted.insert(rest.trim_start_matches("compacted/").trim_end_matches(".sst").to_string(), o.size);
        }
    }
    let total: u64 = by_kind.values().map(|x| x.1).sum();
    let mut out = json!({"db": a.db, "total": total, "by_kind": by_kind.iter().map(|(k, (n, s))| (k.clone(), json!({"objects": n, "bytes": s}))).collect::<serde_json::Map<_, _>>()});

    // --- our runs
    let versions = manifest::list_versions(&store, &a.db).await?;
    let mut loaded = Vec::new();
    for &n in &versions {
        if let Ok(v) = manifest::load(&store, &a.db, n).await {
            loaded.push(v);
        }
    }
    let newest = loaded.iter().max_by_key(|v| v.version).cloned();
    let live: Vec<(String, u64, &str)> = newest.as_ref().map(runs_of).unwrap_or_default();
    let live_set: HashSet<String> = live.iter().map(|x| x.0.clone()).collect();
    let mut retained_set: HashSet<String> = HashSet::new();
    for v in &loaded {
        for (p, _, _) in runs_of(v) {
            if !live_set.contains(&p) {
                retained_set.insert(p);
            }
        }
    }
    let run_prefix = format!("{root}/fx/run/");
    let (mut live_b, mut ret_b, mut unref_b, mut mani_b, mut other_b) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut live_n, mut ret_n, mut unref_n) = (0, 0, 0);
    let mut unref_paths = Vec::new();
    let mut retained_paths = Vec::new();
    for (p, s) in &fx_objs {
        if p.starts_with(&run_prefix) {
            if live_set.contains(p) {
                live_b += s;
                live_n += 1;
            } else if retained_set.contains(p) {
                ret_b += s;
                ret_n += 1;
                retained_paths.push((p.clone(), *s));
            } else {
                unref_b += s;
                unref_n += 1;
                unref_paths.push((p.clone(), *s));
            }
        } else if p.contains("/fx/manifest/") || p.contains("/fx/m/") {
            mani_b += s;
        } else {
            other_b += s;
        }
    }
    let oldest_ms = loaded.iter().map(|v| v.published_ms).min().unwrap_or(0);
    out["fx"] = json!({
        "versions": loaded.len(), "newest": newest.as_ref().map(|v| v.version), "x_d": newest.as_ref().map(|v| v.x_d),
        "oldest_version_age_s": (manifest::now_ms().saturating_sub(oldest_ms)) / 1000,
        "live_runs": {"objects": live_n, "bytes": live_b},
        "retained_runs_older_versions_only": {"objects": ret_n, "bytes": ret_b},
        "unreferenced_runs_grace": {"objects": unref_n, "bytes": unref_b},
        "manifests": mani_b, "other": other_b,
        "groups": newest.as_ref().map(|v| v.groups.len()), "delta_runs": newest.as_ref().map(|v| v.delta.len()),
    });
    let mut samples: HashMap<String, Vec<String>> = HashMap::new();
    let mut classes: BTreeMap<String, Acc> = BTreeMap::new();
    let mut runs = Vec::new();
    let (mut sealed_b, mut delta_b, mut sealed_raw, mut delta_raw) = (0u64, 0u64, 0u64, 0u64);
    for (p, s, kind) in &live {
        let mut d = run_detail(&store, p, *s, a.sample, &mut samples, &mut classes).await?;
        let raw = d["raw"].as_u64().unwrap_or(0);
        if *kind == "sealed" {
            sealed_b += s;
            sealed_raw += raw;
        } else {
            delta_b += s;
            delta_raw += raw;
        }
        d["kind"] = json!(kind);
        runs.push(d);
    }
    out["fx"]["live_sealed"] = json!({"bytes": sealed_b, "raw": sealed_raw});
    out["fx"]["live_delta"] = json!({"bytes": delta_b, "raw": delta_raw});
    out["fx"]["live_classes"] = classes.iter().map(|(k, a)| (k.clone(), a.json())).collect::<serde_json::Map<_, _>>().into();
    out["fx"]["live_run_list"] = json!(runs);
    if a.retained_rows {
        let mut rc: BTreeMap<String, Acc> = BTreeMap::new();
        let mut info = Vec::new();
        for (p, s) in &retained_paths {
            match run_detail(&store, p, *s, 0, &mut samples, &mut rc).await {
                Ok(mut d) => {
                    d.as_object_mut().unwrap().remove("classes");
                    info.push(d)
                }
                Err(e) => info.push(json!({"path": p, "error": format!("{e:#}")})),
            }
        }
        out["fx"]["retained_classes"] = rc.iter().map(|(k, a)| (k.clone(), a.json())).collect::<serde_json::Map<_, _>>().into();
        out["fx"]["retained_run_list"] = json!(info);
    }

    // --- SlateDB
    let admin = slatedb::admin::Admin::builder(root.clone(), Arc::clone(&store)).build();
    if let Some(m) = admin.read_manifest(None).await? {
        let mut listed: HashSet<String> = HashSet::new();
        let mut l0_est = 0u64;
        for v in m.l0() {
            listed.insert(v.sst.id.value().to_string());
            l0_est += v.estimate_size();
        }
        let mut srs = Vec::new();
        for r in m.compacted() {
            let mut n = 0;
            for v in r.sst_views() {
                listed.insert(v.sst.id.value().to_string());
                n += 1;
            }
            srs.push(json!({"id": r.id, "ssts": n, "estimate": r.estimate_size()}));
        }
        let (mut lb, mut ub) = (0u64, 0u64);
        for (id, s) in &compacted {
            if listed.contains(id) {
                lb += s;
            } else {
                ub += s;
            }
        }
        out["slate"] = json!({"l0": m.l0().len(), "l0_estimate": l0_est, "sorted_runs": srs, "compacted_listed_bytes": lb, "compacted_unlisted_bytes": ub,
                              "checkpoints": m.checkpoints().iter().map(|c| json!({"id": c.id.to_string(), "name": c.name.clone(), "expire": format!("{:?}", c.expire_time), "manifest_id": c.manifest_id})).collect::<Vec<_>>(),
                              "manifest_id": m.id()});
    }
    if a.slate {
        let x_d = newest.as_ref().map_or(0, |v| v.x_d);
        let r = tenant_bench::fx::read::open_follow_reader(root.clone(), Arc::clone(&store), Default::default()).await?;
        let mut it = r.scan_with_options((std::ops::Bound::Included(Vec::<u8>::new()), std::ops::Bound::Unbounded), &slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, ..Default::default() }).await?;
        let mut below: BTreeMap<String, Acc> = BTreeMap::new();
        let mut above: BTreeMap<String, Acc> = BTreeMap::new();
        let (mut cb, mut ca) = (Comp::new(4096), Comp::new(4096));
        while let Some(kv) = it.next().await? {
            let c = class(&kv.key);
            if a.sample > 0 {
                let s = samples.entry(format!("slate:{c}")).or_default();
                if s.len() < a.sample {
                    s.push(esc(&kv.key));
                }
            }
            if kv.seq <= x_d {
                below.entry(c).or_default().add(kv.key.len(), kv.value.len(), "row");
                cb.push(&kv.key, &kv.value);
            } else {
                above.entry(c).or_default().add(kv.key.len(), kv.value.len(), "row");
                ca.push(&kv.key, &kv.value);
            }
        }
        out["slate_rows"] = json!({
            "x_d": x_d,
            "at_or_below_x_d": below.iter().map(|(k, a)| (k.clone(), a.json())).collect::<serde_json::Map<_, _>>(),
            "above_x_d": above.iter().map(|(k, a)| (k.clone(), a.json())).collect::<serde_json::Map<_, _>>(),
            "compress_at_or_below": cb.json(), "compress_above": ca.json(),
        });
        let _ = r.close().await;
    }
    if a.sample > 0 {
        out["samples"] = json!(samples);
    }
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
