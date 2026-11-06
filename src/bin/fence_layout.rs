//! Fence-layout measurement (fenvix-brain `prompts/P-fence-layout-local.md`). Not part of the server.
//!
//!   fence_layout build   --dump DIR --bucket B [--bucket-gb 1] [--prefix fence/]      runs from bench/fence/dump.py files
//!   fence_layout measure --bucket B --model DIR/model.json --out result.json            cold and hot reads, maps
//!   fence_layout rewrite --bucket B --run I [--young 0.1] [--range-mb 16]               rewrite cost of one bucket
//!
//! The store is the local MinIO (tools/minio-env.sh variables). `measure` wraps it with the server's own simulated S3
//! (`FENVIX_SIM_S3`, for example `5,0,1000000` for a flat 5 ms GET) and trip meter, like the server's measurements.

use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use sha2::Digest;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;
use tenant_bench::fence::{doc_key, Reader, RunMeta, RunWriter, BLOCK_RAW, PART};

const COLL: &str = "messages";

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn store(bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).build()?))
}

fn cpu_secs() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    r.ru_utime.tv_sec as f64 + r.ru_utime.tv_usec as f64 / 1e6 + r.ru_stime.tv_sec as f64 + r.ru_stime.tv_usec as f64 / 1e6
}

struct Rec {
    user: i64,
    oid: [u8; 12],
    val: Vec<u8>,
}

fn read_partition(dir: &str, part: usize) -> anyhow::Result<Vec<Rec>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        if !name.starts_with("part-") || !name.ends_with(&format!("-{part:03}.rec")) {
            continue;
        }
        let mut buf = Vec::new();
        std::fs::File::open(&p)?.read_to_end(&mut buf)?;
        let mut r = &buf[..];
        while !r.is_empty() {
            let len = u32::from_le_bytes(r[0..4].try_into()?) as usize;
            let user = i64::from_le_bytes(r[4..12].try_into()?);
            let oid: [u8; 12] = r[20..32].try_into()?;
            out.push(Rec { user, oid, val: r[32..32 + len].to_vec() });
            r = &r[32 + len..];
        }
    }
    Ok(out)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Manifest {
    runs: Vec<RunMeta>,
    bson_bytes: u64,
    docs: u64,
    build_secs: f64,
    build_cpu_secs: f64,
    puts: u64,
    block_raw: usize,
    part: usize,
}

async fn build(args: &[String]) -> anyhow::Result<()> {
    let dir = arg(args, "--dump").context("--dump")?;
    let bucket = arg(args, "--bucket").context("--bucket")?;
    let prefix = arg(args, "--prefix").unwrap_or_else(|| "fence/".into());
    let bucket_bytes = (arg(args, "--bucket-gb").map(|s| s.parse::<f64>()).transpose()?.unwrap_or(1.0) * (1u64 << 30) as f64) as u64;
    let parts: usize = arg(args, "--parts").map(|s| s.parse()).transpose()?.unwrap_or(64);
    let st = store(&bucket)?;
    let t0 = Instant::now();
    let c0 = cpu_secs();
    let mut runs: Vec<RunMeta> = Vec::new();
    let mut w: Option<RunWriter> = None;
    let (mut docs, mut bson) = (0u64, 0u64);
    for part in 0..parts {
        let mut recs = read_partition(&dir, part)?;
        // customer-first key order: customer (numeric order, as the key encoding), then _id bytes
        recs.sort_unstable_by(|a, b| (a.user, a.oid).cmp(&(b.user, b.oid)));
        let mut prev_user = None;
        for r in &recs {
            // cut a bucket at a customer boundary once it holds bucket_bytes of entries
            if prev_user != Some(r.user) && w.as_ref().is_some_and(|w| w.raw_bytes() >= bucket_bytes) {
                let m = w.take().unwrap().finish().await?;
                eprintln!("run {} {}: {} blocks, {:.0} MiB raw, {:.0} MiB stored, {} PUTs", runs.len(), m.path, m.blocks, m.raw_bytes as f64 / 1048576.0, m.size as f64 / 1048576.0, m.puts);
                runs.push(m);
            }
            if w.is_none() {
                w = Some(RunWriter::new(&st, Path::from(format!("{prefix}run-{:05}", runs.len())), PART, BLOCK_RAW).await?);
            }
            w.as_mut().unwrap().add(&doc_key(COLL, r.user, &r.oid), &r.val).await?;
            prev_user = Some(r.user);
            docs += 1;
            bson += r.val.len() as u64;
        }
    }
    if let Some(w) = w {
        let m = w.finish().await?;
        eprintln!("run {} {}: {} blocks, {:.0} MiB raw, {:.0} MiB stored, {} PUTs", runs.len(), m.path, m.blocks, m.raw_bytes as f64 / 1048576.0, m.size as f64 / 1048576.0, m.puts);
        runs.push(m);
    }
    let puts = runs.iter().map(|r| r.puts).sum();
    let man = Manifest { runs, bson_bytes: bson, docs, build_secs: t0.elapsed().as_secs_f64(), build_cpu_secs: cpu_secs() - c0, puts, block_raw: BLOCK_RAW, part: PART };
    st.put(&Path::from(format!("{prefix}MANIFEST.json")), serde_json::to_vec_pretty(&man)?.into()).await?;
    println!(
        "built {} runs, {} docs, {:.2} GiB of BSON, {} blocks, {:.2} GiB stored, {} PUTs, {:.0} s ({:.0} s CPU)",
        man.runs.len(),
        docs,
        bson as f64 / (1u64 << 30) as f64,
        man.runs.iter().map(|r| r.blocks).sum::<u64>(),
        man.runs.iter().map(|r| r.size).sum::<u64>() as f64 / (1u64 << 30) as f64,
        puts,
        man.build_secs,
        man.build_cpu_secs
    );
    Ok(())
}

async fn manifest(st: &Arc<dyn ObjectStore>, prefix: &str) -> anyhow::Result<Manifest> {
    let b = st.get(&Path::from(format!("{prefix}MANIFEST.json"))).await?.bytes().await?;
    Ok(serde_json::from_slice(&b)?)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn pct(v: &[f64], p: f64) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[((p / 100.0 * (s.len() - 1) as f64).round() as usize).min(s.len() - 1)]
}

/// One read checked against the model: the 20 newest messages, same `_id`s in the same order, same bytes.
fn check(model: &serde_json::Value, user: i64, got: &[(Vec<u8>, Vec<u8>)]) -> bool {
    let want = &model["newest"][user.to_string()];
    let want = want.as_array().cloned().unwrap_or_default();
    if want.len() != got.len() {
        return false;
    }
    want.iter().zip(got).all(|(w, (k, v))| w[0].as_str() == Some(&hex(&k[k.len() - 12..])) && w[1].as_str() == Some(&hex(&sha2::Sha256::digest(v))))
}

async fn measure(args: &[String]) -> anyhow::Result<()> {
    let bucket = arg(args, "--bucket").context("--bucket")?;
    let prefix = arg(args, "--prefix").unwrap_or_else(|| "fence/".into());
    let model: serde_json::Value = serde_json::from_slice(&std::fs::read(arg(args, "--model").context("--model")?)?)?;
    let out = arg(args, "--out").context("--out")?;
    let tail: usize = arg(args, "--tail-blocks").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let cold_mb: usize = arg(args, "--cold-cache-mb").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let hot_mb: usize = arg(args, "--hot-cache-mb").map(|s| s.parse()).transpose()?.unwrap_or(1024);
    let raw = store(&bucket)?;
    let man = manifest(&raw, &prefix).await?;
    let sim = tenant_bench::simstore::wrap_sim(raw.clone());
    let runs: Vec<(String, u64)> = man.runs.iter().map(|r| (r.path.clone(), r.size)).collect();
    let users: Vec<(String, i64)> = ["active", "uniform"].iter().flat_map(|g| model[*g].as_array().unwrap().iter().map(move |u| (g.to_string(), u.as_i64().unwrap()))).collect();
    let gb = man.bson_bytes as f64 / (1u64 << 30) as f64;
    let stored_gb = man.runs.iter().map(|r| r.size).sum::<u64>() as f64 / (1u64 << 30) as f64;

    // maps: open the reader (fence arrays only) and count them in RAM
    let t = Instant::now();
    let (cold, open_gets) = Reader::open(sim.clone(), &runs, cold_mb << 20, tail, COLL).await?;
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    let map_bytes = cold.map_bytes();
    let blocks = cold.blocks();
    println!(
        "maps: {} runs, {} blocks, {} bytes in RAM = {:.1} bytes per block, {:.4} MB per GB of data ({:.2} GiB BSON), {:.4} MB per GB stored ({:.2} GiB); loaded with {} GETs in {:.0} ms",
        runs.len(),
        blocks,
        map_bytes,
        map_bytes as f64 / blocks as f64,
        map_bytes as f64 / 1e6 / gb,
        gb,
        map_bytes as f64 / 1e6 / stored_gb,
        stored_gb,
        open_gets,
        open_ms
    );

    let mut rows = Vec::new();
    let mut wrong = 0;
    // cold: every user once, with a cache of cold_mb (data blocks really cold)
    for (g, u) in &users {
        let tr0 = tenant_bench::simstore::TRIPS.trips.load(std::sync::atomic::Ordering::Relaxed);
        let t = Instant::now();
        let (got, st) = cold.newest(*u, 20).await?;
        let ms = t.elapsed().as_secs_f64() * 1e3;
        let tr = tenant_bench::simstore::TRIPS.trips.load(std::sync::atomic::Ordering::Relaxed) - tr0;
        let ok = check(&model, *u, &got);
        wrong += (!ok) as u32;
        rows.push(serde_json::json!({"pass": "cold", "group": g, "user": u, "messages": model["counts"][u.to_string()], "gets": st.gets, "trips": st.trips, "meter_trips": tr, "bytes": st.bytes, "blocks": st.blocks_fetched, "ms": ms, "ok": ok}));
    }
    // hot: a reader with a big cache; first pass fills it, second pass is timed
    let (hot, _) = Reader::open(sim.clone(), &runs, hot_mb << 20, tail, COLL).await?;
    for (_, u) in &users {
        hot.newest(*u, 20).await?;
    }
    for (g, u) in &users {
        let t = Instant::now();
        let (got, st) = hot.newest(*u, 20).await?;
        let ms = t.elapsed().as_secs_f64() * 1e3;
        let ok = check(&model, *u, &got);
        wrong += (!ok) as u32;
        rows.push(serde_json::json!({"pass": "hot", "group": g, "user": u, "gets": st.gets, "ms": ms, "ok": ok}));
    }
    let hot_cache_used = hot.cache.lock().unwrap().used();
    let mut summary = serde_json::Map::new();
    for pass in ["cold", "hot"] {
        for g in ["active", "uniform", "all"] {
            let rs: Vec<&serde_json::Value> = rows.iter().filter(|r| r["pass"] == pass && (g == "all" || r["group"] == g)).collect();
            let gets: Vec<f64> = rs.iter().map(|r| r["gets"].as_f64().unwrap()).collect();
            let ms: Vec<f64> = rs.iter().map(|r| r["ms"].as_f64().unwrap()).collect();
            let one = rs.iter().filter(|r| r["gets"].as_u64() == Some(1)).count();
            let line = serde_json::json!({
                "n": rs.len(), "gets_mean": gets.iter().sum::<f64>() / gets.len() as f64, "gets_max": gets.iter().cloned().fold(0.0, f64::max),
                "one_trip_share": one as f64 / rs.len() as f64, "ms_p50": pct(&ms, 50.0), "ms_p95": pct(&ms, 95.0), "ms_p99": pct(&ms, 99.0), "ms_max": pct(&ms, 100.0),
                "bytes_mean": rs.iter().map(|r| r["bytes"].as_f64().unwrap_or(0.0)).sum::<f64>() / rs.len() as f64,
            });
            println!("{pass} {g}: {line}");
            summary.insert(format!("{pass}_{g}"), line);
        }
    }
    println!("answers checked against the exact model: {} reads, {} wrong; hot cache used {:.0} MiB", rows.len(), wrong, hot_cache_used as f64 / 1048576.0);
    let res = serde_json::json!({
        "sim": std::env::var("FENVIX_SIM_S3").ok(), "tail_blocks": tail, "cold_cache_mb": cold_mb, "hot_cache_mb": hot_mb, "hot_cache_used": hot_cache_used,
        "runs": runs.len(), "blocks": blocks, "map_bytes": map_bytes, "bson_bytes": man.bson_bytes, "stored_bytes": man.runs.iter().map(|r| r.size).sum::<u64>(),
        "docs": man.docs, "map_mb_per_gb": map_bytes as f64 / 1e6 / gb, "map_open_gets": open_gets, "map_open_ms": open_ms,
        "wrong": wrong, "summary": summary, "rows": rows,
        "build": {"secs": man.build_secs, "cpu_secs": man.build_cpu_secs, "puts": man.puts},
    });
    std::fs::write(&out, serde_json::to_vec_pretty(&res)?)?;
    anyhow::ensure!(wrong == 0, "{wrong} wrong answers");
    Ok(())
}

/// Counting wrapper for the rewrite: GETs and bytes read.
async fn get_counted(st: &Arc<dyn ObjectStore>, p: &Path, a: u64, b: u64, gets: &mut u64) -> anyhow::Result<bytes::Bytes> {
    *gets += 1;
    Ok(st.get_opts(p, GetOptions { range: Some(GetRange::Bounded(a..b)), ..Default::default() }).await?.bytes().await?)
}

/// Rewrite one bucket whole, as idea A does when its young records reach a threshold: read the run in `range_mb`
/// ranges, merge in young records (here: `young` x entries, new messages of the bucket's own customers, newer than
/// everything), write a new run in 64 MiB parts, delete the old one.
async fn rewrite(args: &[String]) -> anyhow::Result<()> {
    let bucket = arg(args, "--bucket").context("--bucket")?;
    let prefix = arg(args, "--prefix").unwrap_or_else(|| "fence/".into());
    let ri: usize = arg(args, "--run").context("--run")?.parse()?;
    let young: f64 = arg(args, "--young").map(|s| s.parse()).transpose()?.unwrap_or(0.1);
    let range = arg(args, "--range-mb").map(|s| s.parse::<u64>()).transpose()?.unwrap_or(16) << 20;
    let st = store(&bucket)?;
    let man = manifest(&st, &prefix).await?;
    let old = man.runs[ri].clone();
    let path = Path::from(old.path.as_str());
    let t0 = Instant::now();
    let c0 = cpu_secs();
    let mut gets = 0u64;
    // the fence (trailer + footer) to know the block boundaries
    let t = get_counted(&st, &path, old.size - 16, old.size, &mut gets).await?;
    let foff = u64::from_le_bytes(t[0..8].try_into()?);
    let flen = u32::from_le_bytes(t[8..12].try_into()?) as u64;
    let f = get_counted(&st, &path, foff, foff + flen, &mut gets).await?;
    let (fence, _, _) = tenant_bench::fence::Fence::parse(&f)?;
    // stream the data region in `range` pieces, decode whole blocks as they complete
    let mut w = RunWriter::new(&st, Path::from(format!("{}.rw", old.path)), PART, BLOCK_RAW).await?;
    let mut buf: Vec<u8> = Vec::new();
    let mut buf_start = 0u64;
    let mut next_block = 0usize;
    let mut pos = 0u64;
    let (mut old_entries, mut young_entries) = (0u64, 0u64);
    let every = if young > 0.0 { (1.0 / young).round() as u64 } else { u64::MAX };
    let mut pending_young: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut cur_cust: Vec<u8> = Vec::new();
    while pos < old.data_end {
        let end = (pos + range).min(old.data_end);
        let b = get_counted(&st, &path, pos, end, &mut gets).await?;
        buf.extend_from_slice(&b);
        pos = end;
        while next_block < fence.blocks() {
            let (x, y) = fence.range(next_block, next_block);
            if y > buf_start + buf.len() as u64 {
                break;
            }
            let blk = tenant_bench::fence::Block::decode(&buf[(x - buf_start) as usize..(y - buf_start) as usize])?;
            for i in 0..blk.len() {
                let k = blk.key(i);
                let cust = &k[..k.len() - 13];
                if cust != &cur_cust[..] {
                    // the customer changed: its young messages (newer than all its old ones) go after them
                    for (yk, yv) in pending_young.drain(..) {
                        w.add(&yk, &yv).await?;
                    }
                    cur_cust = cust.to_vec();
                }
                w.add(k, blk.val(i)).await?;
                old_entries += 1;
                if old_entries % every == 0 {
                    let mut yk = k.to_vec();
                    let n = yk.len();
                    yk[n - 12] |= 0x80; // a later ObjectId time: newer than every old message
                    pending_young.push((yk, blk.val(i).to_vec()));
                    young_entries += 1;
                }
            }
            next_block += 1;
            let keep_from = (y - buf_start) as usize;
            buf.drain(..keep_from);
            buf_start = y;
        }
    }
    pending_young.sort();
    for (yk, yv) in pending_young.drain(..) {
        w.add(&yk, &yv).await?;
    }
    let m = w.finish().await?;
    st.delete(&path).await?;
    let secs = t0.elapsed().as_secs_f64();
    let res = serde_json::json!({
        "run": ri, "old_raw_bytes": old.raw_bytes, "old_stored_bytes": old.size, "old_entries": old_entries, "young_entries": young_entries,
        "new_raw_bytes": m.raw_bytes, "new_stored_bytes": m.size, "gets": gets, "range_bytes": range, "puts": m.puts, "parts": m.parts, "deletes": 1,
        "secs": secs, "cpu_secs": cpu_secs() - c0,
    });
    println!("{res}");
    if let Some(o) = arg(args, "--out") {
        std::fs::write(o, serde_json::to_vec_pretty(&res)?)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("build") => build(&args).await,
        Some("measure") => measure(&args).await,
        Some("rewrite") => rewrite(&args).await,
        _ => anyhow::bail!("usage: fence_layout build|measure|rewrite ... (see the file header)"),
    }
}
