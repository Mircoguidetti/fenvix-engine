//! Agent checkpoints as a thread message log (`fx/derive.rs`): stored bytes, reads and reseal CPU on the documents the
//! LangGraph MongoDB saver really wrote (fenvix-brain `prompts/P-c2-delta-log.md`, task 5).
//!
//! Input: a dump of the saver's documents (`research/c2-delta-log/data-prep`, format FXD1: coll u8, thread id, raw BSON
//! as mongod stored it, in write order). Each document is written to a writer tenant in process (in-memory store) as
//! the server writes a customer-keyed document: the document under `u/{thread}{collection}/d/{_id}`, its entry of the
//! saver's compound index (customer first, so with the document), and its locator in the shared keyspace. Then the
//! first version is built, the compactor starts (drop on), everything is absorbed and every group resealed, with
//! derived documents on or off and lz4 or zstd blocks.
//!
//! Measured: stored bytes of every run of the final version per thread (documents, index entries and locators: keys
//! included); every document read back through the merged reader and compared byte for byte; get_tuple of the latest
//! and of an old checkpoint, cold (no block cached; run footers open, as on a waiter) and warm, with GETs, trips and
//! time behind a simulated S3 latency; the CPU of the reseals per GB.
//!
//! Run: `cargo run --release --bin fx_derive -- DUMP.fxd lz4|zstd on|off [sample threads for reads]`; one JSON line on
//! stdout.

use anyhow::{anyhow, bail, ensure, Result};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tenant_bench::fx::{self, compactor, derive, view};
use tenant_bench::lifecycle::{Lifecycle, Op, Tenant};
use tenant_bench::runfmt::{self, BlockCodec, CustomerKind, RunOptions};

const COLLS: [&str; 2] = ["checkpoints", "checkpoint_writes"];

struct Rec {
    coll: u8,
    thread: String,
    doc: Vec<u8>,
}

fn load(path: &str) -> Result<Vec<Rec>> {
    let d = std::fs::read(path)?;
    ensure!(d.starts_with(b"FXD1"), "{path}: not an FXD1 dump");
    let mut p = 4;
    let mut out = Vec::new();
    while p < d.len() {
        let coll = d[p];
        let tl = u16::from_le_bytes(d[p + 1..p + 3].try_into()?) as usize;
        let thread = String::from_utf8(d[p + 3..p + 3 + tl].to_vec())?;
        p += 3 + tl;
        let n = u32::from_le_bytes(d[p..p + 4].try_into()?) as usize;
        p += 4;
        out.push(Rec { coll, thread, doc: d[p..p + n].to_vec() });
        p += n;
    }
    Ok(out)
}

/// Top-level fields of a BSON document: (name, type, value bytes).
fn fields(d: &[u8]) -> Result<Vec<(String, u8, &[u8])>> {
    let mut out = Vec::new();
    let mut p = 4;
    while p < d.len() - 1 {
        let t = d[p];
        let z = d[p + 1..].iter().position(|&b| b == 0).ok_or_else(|| anyhow!("name"))?;
        let name = String::from_utf8_lossy(&d[p + 1..p + 1 + z]).to_string();
        p += 2 + z;
        let i32_at = |p: usize| i32::from_le_bytes(d[p..p + 4].try_into().unwrap()) as usize;
        let len = match t {
            0x01 | 0x09 | 0x11 | 0x12 => 8,
            0x02 | 0x0D | 0x0E => 4 + i32_at(p),
            0x03 | 0x04 => i32_at(p),
            0x05 => 5 + i32_at(p),
            0x06 | 0x0A => 0,
            0x07 => 12,
            0x08 => 1,
            0x10 => 4,
            0x13 => 16,
            _ => bail!("BSON type {t:#x} in field {name}"),
        };
        out.push((name, t, &d[p..p + len]));
        p += len;
    }
    Ok(out)
}

fn str_of(f: &[(String, u8, &[u8])], n: &str) -> Vec<u8> {
    match f.iter().find(|x| x.0 == n) {
        Some((_, 0x02, v)) => v[4..v.len() - 1].to_vec(),
        Some((_, 0x10, v)) => v.to_vec(),
        Some((_, _, v)) => v.to_vec(),
        None => Vec::new(),
    }
}

/// The order-preserving encoding of a string (index entries), descending when asked: type byte, bytes, terminator.
fn enc(s: &[u8], desc: bool) -> Vec<u8> {
    let mut v = vec![0x3c];
    v.extend(s);
    v.push(0);
    if desc {
        for b in v.iter_mut() {
            *b = !*b;
        }
    }
    v
}

fn comp(thread: &str) -> Vec<u8> {
    let mut c = vec![thread.len() as u8];
    c.extend(thread.as_bytes());
    c
}

fn doc_key(thread: &str, coll: &str, oid: &[u8]) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(thread));
    k.extend(coll.as_bytes());
    k.extend(b"/d/\x07");
    k.extend(oid);
    k
}

fn index_prefix(thread: &str, coll: &str) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.extend(comp(thread));
    k.extend(coll.as_bytes());
    k.extend(b"/x/ix/");
    k.extend(comp(thread));
    k
}

/// The rows the server writes for one saver document: the document, its index entry, its locator.
fn rows_of(r: &Rec, n: u32) -> Result<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let f = fields(&r.doc)?;
    let oid: Vec<u8> = match f.iter().find(|x| x.0 == "_id") {
        Some((_, 0x07, v)) => v.to_vec(),
        _ => bail!("a document without an ObjectId _id"),
    };
    let coll = COLLS[r.coll as usize];
    let dk = doc_key(&r.thread, coll, &oid);
    let mut ik = index_prefix(&r.thread, coll);
    ik.extend(enc(&str_of(&f, "checkpoint_ns"), false));
    ik.extend(enc(&str_of(&f, "checkpoint_id"), r.coll == 0));
    if r.coll == 1 {
        ik.extend(enc(&str_of(&f, "task_id"), false));
        ik.extend(enc(&str_of(&f, "idx"), false));
    }
    ik.push(0x07);
    ik.extend(&oid);
    ik.extend([0u8, 0]);
    let mut lk = format!("loc/{coll}/").into_bytes();
    lk.extend(n.to_be_bytes());
    lk.push(0x07);
    lk.extend(&oid);
    lk.extend(comp(&r.thread));
    let mut suffix = comp(&r.thread);
    suffix.push(0x07);
    suffix.extend(&oid);
    Ok((dk.clone(), vec![(dk, r.doc.clone()), (ik, Vec::new()), (lk, suffix)]))
}

fn knobs(codec: BlockCodec, on: bool) -> compactor::Knobs {
    let mut k = compactor::Knobs::default();
    k.sealed = RunOptions::sealed(CustomerKind::V2);
    k.sealed.codec = codec;
    k.tier_unit = 4 << 20;
    k.absorb_min_l0 = 1;
    k.split_bytes = 256 << 20;
    k.merge_bytes = 64 << 20;
    k.min_seal_bytes = 32 << 20;
    k.local_max_bytes = 0;
    k.lease = Duration::from_millis(400);
    k.remote_lease = Duration::from_millis(4000);
    k.serving_lease = Duration::from_millis(300);
    k.put_timeout = Duration::from_millis(250);
    k.gc_grace = Duration::from_millis(4500);
    k.version_retention = Duration::from_millis(0);
    k.tick = Duration::from_millis(20);
    k.drop = true;
    k.derive = on.then(|| derive::SpecRef(Arc::new(derive::LenSpec { colls: COLLS.iter().map(|c| c.as_bytes().to_vec()).collect() })));
    k
}

async fn flush(t: &Tenant) -> Result<()> {
    let db = t.fx_writer_db().ok_or_else(|| anyhow!("no writer"))?;
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await?;
    Ok(())
}

#[derive(Default, Clone, Copy)]
struct ReadM {
    gets: u64,
    trips: u64,
    us: u64,
}

/// get_tuple as the saver does it: the newest checkpoint of the thread (or checkpoint `cid`) through the index, the
/// document, then its pending writes through their index and their documents. Every document read is checked.
async fn get_tuple(v: &Arc<view::View>, thread: &str, cid: Option<&[u8]>, want: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<ReadM> {
    let g0 = view::RUN_GETS.load(Relaxed);
    let t0 = tenant_bench::simstore::TRIPS.trips.load(Relaxed);
    let start = Instant::now();
    let mut lo = index_prefix(thread, COLLS[0]);
    lo.extend(enc(b"", false));
    if let Some(c) = cid {
        lo.extend(enc(c, true));
    }
    let hi = runfmt::prefix_successor(&lo);
    let mut cur = v.cursor(&lo, hi.as_deref(), false, 256 << 10);
    let e = cur.next().await?.ok_or_else(|| anyhow!("no checkpoint of {thread}"))?;
    let oid = &e.key[e.key.len() - 14..e.key.len() - 2];
    let dk = doc_key(thread, COLLS[0], oid);
    let d = v.get(&dk).await?.ok_or_else(|| anyhow!("checkpoint document missing"))?;
    ensure!(Some(&d.env[1..]) == want.get(&dk).map(|x| &x[..]), "WRONG checkpoint document {thread}");
    let f = fields(&d.env[1..])?;
    let cidv = str_of(&f, "checkpoint_id");
    let mut wlo = index_prefix(thread, COLLS[1]);
    wlo.extend(enc(b"", false));
    wlo.extend(enc(&cidv, false));
    let whi = runfmt::prefix_successor(&wlo);
    let mut wc = v.cursor(&wlo, whi.as_deref(), false, 256 << 10);
    while let Some(e) = wc.next().await? {
        let oid = &e.key[e.key.len() - 14..e.key.len() - 2];
        let k = doc_key(thread, COLLS[1], oid);
        let d = v.get(&k).await?.ok_or_else(|| anyhow!("write document missing"))?;
        ensure!(Some(&d.env[1..]) == want.get(&k).map(|x| &x[..]), "WRONG write document {thread}");
    }
    Ok(ReadM { gets: view::RUN_GETS.load(Relaxed) - g0, trips: tenant_bench::simstore::TRIPS.trips.load(Relaxed) - t0, us: start.elapsed().as_micros() as u64 })
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn summary(ms: &[ReadM]) -> serde_json::Value {
    let n = ms.len().max(1) as f64;
    let mut us: Vec<u64> = ms.iter().map(|m| m.us).collect();
    let mut gets: Vec<u64> = ms.iter().map(|m| m.gets).collect();
    let mut trips: Vec<u64> = ms.iter().map(|m| m.trips).collect();
    json!({
        "n": ms.len(),
        "gets_mean": ms.iter().map(|m| m.gets).sum::<u64>() as f64 / n,
        "gets_max": pct(&mut gets, 1.0),
        "trips_mean": ms.iter().map(|m| m.trips).sum::<u64>() as f64 / n,
        "trips_max": pct(&mut trips, 1.0),
        "ms_p50": pct(&mut us, 0.5) as f64 / 1e3,
        "ms_p99": pct(&mut us, 0.99) as f64 / 1e3,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    ensure!(a.len() >= 4, "usage: fx_derive DUMP lz4|zstd on|off [threads to read]");
    let codec = match a[2].as_str() {
        "lz4" => BlockCodec::Lz4,
        "zstd" => BlockCodec::Zstd,
        x => bail!("codec {x}"),
    };
    let on = a[3] == "on";
    let sample: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(40);
    let recs = load(&a[1])?;
    let mut threads: Vec<String> = recs.iter().map(|r| r.thread.clone()).collect();
    threads.sort();
    threads.dedup();
    fx::ENABLED.store(true, Relaxed);
    fx::PUBLISH_EVERY.store(0, Relaxed);
    fx::T_PIN_MS.store(200, Relaxed);
    fx::E1_WAIT_MS.store(50, Relaxed);
    fx::hold::HOLD_LIFE_MS.store(3000, Relaxed);
    derive::WRITE.store(on, Relaxed);
    // the reader's prefetch of a thread's message blocks (FX_PREFETCH=0: off, two trips for a derived document)
    if std::env::var("FX_PREFETCH").as_deref() != Ok("0") {
        derive::set_read_spec(Some(Arc::new(derive::LenSpec { colls: COLLS.iter().map(|c| c.as_bytes().to_vec()).collect() })));
    }
    tenant_bench::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lc = Lifecycle::new(Arc::new(tenant_bench::counting::CountingStore::new(Arc::clone(&mem))), None);
    let id = "agent";
    let mut t = lc.create(id).await?;
    // writes: one saver document per batch, as the server writes it
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let (mut doc_bytes, mut logical) = (0u64, 0u64);
    let tw = Instant::now();
    for (n, r) in recs.iter().enumerate() {
        let (_, rows) = rows_of(r, n as u32)?;
        doc_bytes += r.doc.len() as u64;
        for (k, v) in &rows {
            logical += (k.len() + v.len()) as u64;
        }
        t.apply(rows.iter().map(|(k, v)| Op::Put(k.clone(), v.clone())).collect()).await?;
        for (k, v) in rows {
            model.insert(k, v);
        }
    }
    flush(&t).await?;
    let write_s = tw.elapsed().as_secs_f64();
    // the first version (step 3's builder), then the compactor: absorb everything, reseal every group
    let bo = fx::build::BuildOpts { reseal: fx::build::Reseal::All, group_bytes: 64 << 20, sealed: RunOptions::sealed(CustomerKind::V2), delta: RunOptions::new(64 << 10, 16 << 20, CustomerKind::V2)? };
    loop {
        if t.fx_publish(&bo, None).await?.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let k = knobs(codec, on);
    let c = compactor::Compactor::start(&mut t, Arc::clone(&mem), k.clone(), fx::pool::Pool::new(2), tokio::runtime::Handle::current()).await?;
    // a last write after the publish, so absorption has an L0 SST to take
    t.apply(vec![Op::Put(b"zz/tick".to_vec(), b"1".to_vec())]).await?;
    model.insert(b"zz/tick".to_vec(), b"1".to_vec());
    flush(&t).await?;
    while c.absorb_once().await? > 0 {}
    while c.merge_once().await? {}
    // reseal passes: the first version's groups cut threads anywhere, so a reseal also takes the neighbour of a
    // boundary inside a thread; passes until nothing more is encoded. The CPU is counted per pass.
    let mut passes = Vec::new();
    let mut reseals = 0;
    let mut prev_enc = u64::MAX;
    for pass in 0..6 {
        let enc0 = c.sh.stats.derive_encoded.load(Relaxed);
        let cpu0 = compactor::process_cpu_us();
        let r0 = Instant::now();
        let n = c.current().await.groups.len();
        let mut gi = 0;
        let mut done = 0;
        while gi < c.current().await.groups.len().max(n) && gi < 10_000 {
            if gi < c.current().await.groups.len() && c.reseal_once(Some(gi)).await? {
                done += 1;
            }
            gi += 1;
        }
        reseals += done;
        let enc = c.sh.stats.derive_encoded.load(Relaxed) - enc0;
        passes.push(json!({"pass": pass, "reseals": done, "encoded": enc, "cpu_s": (compactor::process_cpu_us() - cpu0) as f64 / 1e6, "wall_s": r0.elapsed().as_secs_f64(), "groups": c.current().await.groups.len()}));
        if pass >= 1 && (!on || enc == prev_enc) {
            break;
        }
        prev_enc = enc;
    }
    // the CPU of a reseal of everything once the threads are encoded: the last pass (it rebuilds and re-encodes)
    let last = passes.last().cloned().unwrap();
    let reseal_cpu_us = (last["cpu_s"].as_f64().unwrap() * 1e6) as u64;
    let reseal_s = last["wall_s"].as_f64().unwrap();
    while c.merge_once().await? {}
    t.fx_sync();
    let v = c.current().await;
    // stored bytes of the final version: every run (sealed and delta)
    let mut stored = 0u64;
    let mut raw = 0u64;
    let (mut n_sealed, mut n_delta) = (0, 0);
    let (mut derived_rows, mut part_rows, mut part_raw, mut skel_raw) = (0u64, 0u64, 0u64, 0u64);
    for line in v.groups.iter().filter_map(|g| g.sealed.clone()) {
        stored += line.size;
        raw += line.raw;
        n_sealed += 1;
    }
    for line in &v.delta {
        stored += line.size;
        raw += line.raw;
        n_delta += 1;
    }
    for line in v.groups.iter().filter_map(|g| g.sealed.clone()).chain(v.delta.iter().cloned()) {
        for row in compactor::run_rows(&mem, &line).await? {
            match row.env.first() {
                Some(&runfmt::DERIVED) => {
                    derived_rows += 1;
                    skel_raw += (row.key.len() + row.env.len() + 8) as u64;
                }
                Some(&runfmt::PART) => {
                    part_rows += 1;
                    part_raw += (row.key.len() + row.env.len() + 8) as u64;
                }
                _ => {}
            }
        }
    }
    // every document read back through the merged reader, byte for byte
    view::clear_cache();
    let mut wrong = 0u64;
    let mut checked = 0u64;
    for (key, want) in &model {
        let got = t.get(key).await?;
        checked += 1;
        if got.as_deref() != Some(&want[..]) {
            wrong += 1;
        }
    }
    let scan = t.scan_range(b"u/", Some(b"u0"), false, None, 4 << 20).await?;
    let scan_want: Vec<(&Vec<u8>, &Vec<u8>)> = model.range(b"u/".to_vec()..b"u0".to_vec()).collect();
    let scan_ok = scan.len() == scan_want.len() && scan.iter().zip(&scan_want).all(|(a, b)| &a.0[..] == &b.0[..] && &a.1[..] == &b.1[..]);
    // reads behind a simulated S3 (25 ms median, sigma 0.6, 90 MB/s): every run opened first (footers stay in RAM on a
    // waiter), then cold (block cache cleared before each read) and warm (the same read again)
    std::env::set_var("FENVIX_SIM_S3", "25,0.6,90");
    let sim = tenant_bench::simstore::wrap_sim(Arc::clone(&mem));
    let rv = Arc::new(view::View::new(Arc::clone(&sim), v.clone(), None)?);
    for g in &rv.groups {
        if let Some(s) = &g.sealed {
            s.open(&sim).await?;
        }
    }
    for d in &rv.delta {
        d.open(&sim).await?;
    }
    let step = (threads.len() / sample.max(1)).max(1);
    let picked: Vec<&String> = threads.iter().step_by(step).take(sample).collect();
    let (mut latest_cold, mut latest_warm, mut old_cold, mut old_warm) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for th in &picked {
        // the thread's checkpoint ids, oldest first
        let mut cids: Vec<Vec<u8>> = recs.iter().filter(|r| &r.thread == *th && r.coll == 0).map(|r| fields(&r.doc).map(|f| str_of(&f, "checkpoint_id"))).collect::<Result<_>>()?;
        cids.sort();
        let old = cids[cids.len() / 4].clone();
        view::clear_cache();
        tokio::time::sleep(Duration::from_millis(15)).await;
        latest_cold.push(get_tuple(&rv, th, None, &model).await?);
        latest_warm.push(get_tuple(&rv, th, None, &model).await?);
        view::clear_cache();
        tokio::time::sleep(Duration::from_millis(15)).await;
        old_cold.push(get_tuple(&rv, th, Some(&old), &model).await?);
        old_warm.push(get_tuple(&rv, th, Some(&old), &model).await?);
    }
    let st = &c.sh.stats;
    let out = json!({
        "dump": a[1], "codec": a[2], "derive": on, "prefetch": std::env::var("FX_PREFETCH").as_deref() != Ok("0"),
        "threads": threads.len(), "documents": recs.len(),
        "checkpoints": recs.iter().filter(|r| r.coll == 0).count(), "writes": recs.iter().filter(|r| r.coll == 1).count(),
        "saver_bson_bytes": doc_bytes, "logical_row_bytes": logical,
        "stored_bytes": stored, "stored_bytes_per_thread": stored as f64 / threads.len() as f64,
        "raw_row_bytes": raw, "sealed_runs": n_sealed, "delta_runs": n_delta,
        "derived_rows": derived_rows, "message_rows": part_rows, "message_raw_bytes": part_raw, "skeleton_raw_bytes": skel_raw,
        "encoded": st.derive_encoded.load(Relaxed), "checkpoint_docs_seen": st.derive_docs.load(Relaxed), "fallbacks": st.derive_fallbacks.load(Relaxed),
        "reseals": reseals, "reseal_passes": passes, "reseal_cpu_s": reseal_cpu_us as f64 / 1e6, "reseal_wall_s": reseal_s,
        "reseal_cpu_s_per_logical_gb": reseal_cpu_us as f64 / 1e6 / (logical as f64 / 1e9),
        "reseal_cpu_s_per_saver_gb": reseal_cpu_us as f64 / 1e6 / (doc_bytes as f64 / 1e9),
        "write_s": write_s,
        "checked": checked, "wrong": wrong, "full_scan_equal": scan_ok,
        "reads_threads": picked.len(),
        "latest_cold": summary(&latest_cold), "latest_warm": summary(&latest_warm),
        "old_cold": summary(&old_cold), "old_warm": summary(&old_warm),
    });
    println!("{out}");
    c.stop().await;
    let _ = t.abandon().await;
    ensure!(wrong == 0 && scan_ok, "WRONG answers: {wrong} of {checked}, full scan equal {scan_ok}");
    Ok(())
}
