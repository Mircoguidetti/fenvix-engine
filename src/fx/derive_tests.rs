//! Tests of derived documents (agent checkpoints as a thread message log, `derive.rs`): every document read back equal
//! to the one written, byte for byte, through every path (point reads, range reads both ways, held transactions,
//! paired checkpoint readers of PITR and forks), with the compactor running reseals that encode, re-encode and decode
//! threads, while agents append, summarise, edit and remove messages; with crashes in the middle of a reseal, a
//! takeover, erasures (no message row or skeleton of an erased thread left in any run), the run format v2 refused by a
//! v1 reader, the rollback to plain documents, and a planted bug (a skeleton referencing the wrong message) caught.
//!
//! The documents are LangGraph MongoDB-saver documents built here (BSON with a msgpack `checkpoint` / `value` binary
//! field whose message list is an array of msgpack ext values, as the saver writes them). The real trajectories of the
//! checkpoint-delta review run through the bench `fx_derive` (it reads the saver's own documents dumped from mongod).

use super::compactor::{self, Compactor, Knobs};
use super::derive::{self, LenSpec, SpecRef};
use super::tests::*;
use super::*;
use crate::lifecycle::{Op, Tenant};
use crate::runfmt::{self, BlockCodec, CustomerKind, RunOptions};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HOLD_MS: u64 = 3000;

// ---- LangGraph MongoDB-saver documents

fn mp_str(out: &mut Vec<u8>, s: &[u8]) {
    match s.len() {
        0..=31 => out.push(0xa0 | s.len() as u8),
        32..=255 => out.extend([0xd9, s.len() as u8]),
        _ => {
            out.push(0xda);
            out.extend((s.len() as u16).to_be_bytes());
        }
    }
    out.extend(s);
}

/// A message as the saver's serializer writes it: an ext value (type 5) wrapping the message's own msgpack.
fn message(rng: &mut StdRng, size: usize, tag: u64) -> Vec<u8> {
    let mut inner = vec![0x94];
    mp_str(&mut inner, b"langchain_core.messages");
    mp_str(&mut inner, if tag % 3 == 0 { b"HumanMessage" } else if tag % 3 == 1 { b"AIMessage" } else { b"ToolMessage" });
    inner.push(0x82);
    mp_str(&mut inner, b"id");
    mp_str(&mut inner, format!("run-{tag:016x}").as_bytes());
    mp_str(&mut inner, b"content");
    let words = ["the ", "agent ", "tool ", "returns ", "data ", "for ", "user ", "query ", "order ", "flight ", "\n", "def f(x):", " return x"];
    let mut text = Vec::with_capacity(size);
    while text.len() < size {
        text.extend(words[rng.gen_range(0..words.len())].as_bytes());
    }
    text.truncate(size.max(1));
    mp_str(&mut inner, &text);
    let mut m = Vec::with_capacity(inner.len() + 6);
    if inner.len() <= 255 {
        m.extend([0xc7, inner.len() as u8, 5]);
    } else {
        m.push(0xc8);
        m.extend((inner.len() as u16).to_be_bytes());
        m.push(5);
    }
    m.extend(inner);
    m
}

fn mp_array_header(out: &mut Vec<u8>, n: usize) {
    if n < 16 {
        out.push(0x90 | n as u8);
    } else {
        out.push(0xdc);
        out.extend((n as u16).to_be_bytes());
    }
}

struct Bson(Vec<u8>);

impl Bson {
    fn new() -> Bson {
        Bson(Vec::new())
    }
    fn name(&mut self, t: u8, n: &str) {
        self.0.push(t);
        self.0.extend(n.as_bytes());
        self.0.push(0);
    }
    fn oid(mut self, n: &str, v: u64) -> Self {
        self.name(0x07, n);
        self.0.extend([0x65, 0x4a, 0, 0]);
        self.0.extend(v.to_be_bytes());
        self
    }
    fn str(mut self, n: &str, v: &str) -> Self {
        self.name(0x02, n);
        self.0.extend(((v.len() + 1) as i32).to_le_bytes());
        self.0.extend(v.as_bytes());
        self.0.push(0);
        self
    }
    fn int(mut self, n: &str, v: i32) -> Self {
        self.name(0x10, n);
        self.0.extend(v.to_le_bytes());
        self
    }
    fn bin(mut self, n: &str, v: &[u8]) -> Self {
        self.name(0x05, n);
        self.0.extend((v.len() as i32).to_le_bytes());
        self.0.push(0);
        self.0.extend(v);
        self
    }
    fn done(self) -> Vec<u8> {
        let mut d = ((self.0.len() + 5) as i32).to_le_bytes().to_vec();
        d.extend(self.0);
        d.push(0);
        d
    }
}

/// How an agent changes its message list.
#[derive(Clone, Copy, Debug)]
enum Policy {
    /// append only (the default `add_messages`)
    Append,
    /// past 12 messages, all but the last 2 replaced by one summary
    Summarise,
    /// each step replaces the message 3 back with a short stub under the same id
    Edit,
    /// every 5th step removes the 2 oldest messages
    Remove,
}

struct Thread {
    name: String,
    policy: Policy,
    msgs: Vec<Vec<u8>>,
    step: u32,
}

fn tkey(t: &str, coll: &str, kind: &str, n: u64) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.push(t.len() as u8);
    k.extend(t.as_bytes());
    k.extend(format!("{coll}/{kind}/{n:010}").as_bytes());
    k
}

/// The customer range of thread `t`.
fn trange(t: &str) -> (Vec<u8>, Vec<u8>) {
    let mut lo = b"u/".to_vec();
    lo.push(t.len() as u8);
    lo.extend(t.as_bytes());
    let hi = runfmt::prefix_successor(&lo).unwrap();
    (lo, hi)
}

fn spec() -> SpecRef {
    SpecRef(Arc::new(LenSpec { colls: vec![b"ck".to_vec(), b"cw".to_vec()] }))
}

/// One step of a thread: its new messages, the checkpoint document and its pending-write document (keys and BSON).
fn step(rng: &mut StdRng, th: &mut Thread, oid: &mut u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    th.step += 1;
    let n_new = rng.gen_range(1..4);
    let mut new = Vec::new();
    for _ in 0..n_new {
        let size = match rng.gen_range(0..20) {
            0..=1 => rng.gen_range(5..30), // short: stays literal
            2 => rng.gen_range(3000..12000),
            _ => rng.gen_range(80..900),
        };
        *oid += 1;
        new.push(message(rng, size, *oid));
    }
    th.msgs.extend(new.iter().cloned());
    match th.policy {
        Policy::Append => {}
        Policy::Summarise if th.msgs.len() > 12 => {
            let keep = th.msgs.split_off(th.msgs.len() - 2);
            *oid += 1;
            th.msgs = vec![message(rng, 600, *oid)];
            th.msgs.extend(keep);
        }
        Policy::Summarise => {}
        Policy::Edit => {
            if th.msgs.len() > 3 {
                let i = th.msgs.len() - 4;
                *oid += 1;
                th.msgs[i] = message(rng, 60, *oid);
            }
        }
        Policy::Remove => {
            if th.step % 5 == 0 && th.msgs.len() > 4 {
                th.msgs.drain(..2);
            }
        }
    }
    let mut ck = vec![0x84];
    mp_str(&mut ck, b"v");
    ck.push(4);
    mp_str(&mut ck, b"id");
    mp_str(&mut ck, format!("1f0-{:08}", th.step).as_bytes());
    mp_str(&mut ck, b"channel_values");
    ck.push(0x82);
    mp_str(&mut ck, b"messages");
    mp_array_header(&mut ck, th.msgs.len());
    for m in &th.msgs {
        ck.extend(m);
    }
    mp_str(&mut ck, b"branch:to:agent");
    ck.push(0xc0);
    mp_str(&mut ck, b"channel_versions");
    ck.push(0x81);
    mp_str(&mut ck, b"messages");
    mp_str(&mut ck, format!("{:032}", th.step).as_bytes());
    let mut meta = vec![0x82];
    mp_str(&mut meta, b"source");
    mp_str(&mut meta, b"loop");
    mp_str(&mut meta, b"step");
    meta.push((th.step % 100) as u8);
    *oid += 1;
    let cid = format!("1f0-{:08}", th.step);
    let ck_doc = Bson::new()
        .oid("_id", *oid)
        .str("thread_id", &th.name)
        .str("checkpoint_ns", "")
        .str("checkpoint_id", &cid)
        .str("parent_checkpoint_id", &format!("1f0-{:08}", th.step - 1))
        .str("type", "msgpack")
        .bin("checkpoint", &ck)
        .bin("metadata", &meta)
        .done();
    let mut val = Vec::new();
    mp_array_header(&mut val, new.len());
    for m in &new {
        val.extend(m);
    }
    *oid += 1;
    let wr_doc = Bson::new()
        .oid("_id", *oid)
        .str("thread_id", &th.name)
        .str("checkpoint_ns", "")
        .str("checkpoint_id", &cid)
        .str("task_id", &format!("task-{}", th.step))
        .int("idx", 0)
        .str("channel", "messages")
        .str("type", "msgpack")
        .bin("value", &val)
        .str("task_path", "~__pregel_pull, agent")
        .done();
    vec![(tkey(&th.name, "ck", "d", th.step as u64), ck_doc), (tkey(&th.name, "cw", "d", th.step as u64), wr_doc)]
}

fn new_thread(i: usize) -> Thread {
    let policy = match i % 4 {
        0 => Policy::Append,
        1 => Policy::Summarise,
        2 => Policy::Edit,
        _ => Policy::Remove,
    };
    Thread { name: format!("th{i:03}"), policy, msgs: Vec::new(), step: 0 }
}

// ---- setup

fn knobs(codec: BlockCodec) -> Knobs {
    let mut sealed = RunOptions::unchecked(4096, 1 << 20, CustomerKind::V2);
    sealed.codec = codec;
    Knobs {
        theta: 0.25,
        r3: true,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 8 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        local_max_bytes: 16 << 10,
        split_bytes: 400 << 10,
        merge_bytes: 60 << 10,
        min_seal_bytes: 32 << 10,
        sealed,
        delta: RunOptions::unchecked(2048, 1 << 20, CustomerKind::V2),
        drop: true,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(400),
        remote_lease: Duration::from_millis(HOLD_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(HOLD_MS + 800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_millis(500),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: Some(spec()),
    }
}

fn setup() {
    enable();
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(16 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    local::set_store(Some(Arc::new(InMemory::new())));
    view::clear_cache();
    derive::WRITE.store(true, Relaxed);
    // FX_WAVE=1: the tests of this file with the one-wave reader (step 5), as the server runs by default
    view::WAVE.store(std::env::var("FX_WAVE").is_ok(), Relaxed);
    // review: FX_D_PLANT=n runs a test with planted bug n, FX_D_NOCRC=1 without the reader's CRC check
    derive::CHECK_CRC.store(std::env::var("FX_D_NOCRC").is_err(), Relaxed);
    derive::PLANT.store(std::env::var("FX_D_PLANT").ok().and_then(|s| s.parse().ok()).unwrap_or(0), Relaxed);
    derive::REVIEW_PREV.lock().unwrap().clear();
    derive::FAIL_AT_UNIT.store(0, Relaxed);
    // fix round: FX_FIX_CONTROL=1|2|3 removes one fix (its review test must then fail)
    derive::FIX_CONTROL.store(std::env::var("FX_FIX_CONTROL").ok().and_then(|s| s.parse().ok()).unwrap_or(0), Relaxed);
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    local::set_store(None);
    derive::WRITE.store(false, Relaxed);
    view::WAVE.store(false, Relaxed);
    derive::CHECK_CRC.store(true, Relaxed);
    derive::PLANT.store(0, Relaxed);
    derive::FAIL_AT_UNIT.store(0, Relaxed);
}

async fn start(t: &mut Tenant, store: &Arc<dyn ObjectStore>, k: &Knobs) -> Arc<Compactor> {
    let c = Compactor::start(t, Arc::clone(store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c.run();
    c
}

/// Every row of every run of the newest version (key, seq, envelope byte).
async fn all_run_rows(store: &Arc<dyn ObjectStore>, v: &manifest::Version) -> Vec<compactor::Row> {
    let mut out = Vec::new();
    for line in v.groups.iter().filter_map(|g| g.sealed.clone()).chain(v.delta.iter().cloned()) {
        out.extend(compactor::run_rows(store, &line).await.unwrap());
    }
    out
}

/// Reseal every group of the compactor's newest version (the background planner reseals on its own too).
async fn reseal_all(c: &Compactor) -> u64 {
    let mut n = 0;
    let v = c.current().await;
    for gi in 0..v.groups.len() {
        let cur = c.current().await;
        if gi < cur.groups.len() && c.reseal_once(Some(gi)).await.unwrap_or(false) {
            n += 1;
        }
    }
    n
}

async fn flush(t: &Tenant) {
    let db = t.fx_writer_db().unwrap();
    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
}

/// Flush the memtable and absorb every L0 SST now.
async fn absorb_all(t: &mut Tenant, c: &Compactor) {
    flush(t).await;
    for _ in 0..100 {
        match c.absorb_once().await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break, // a job of the background planner holds the absorption: it absorbs instead
        }
    }
    t.fx_sync();
}

#[derive(Default, Debug)]
struct St {
    ops: u64,
    steps: u64,
    gets: u64,
    ranges: u64,
    txn_reads: u64,
    wrong: u64,
    retries: u64,
    crashes: u64,
    mid_reseal_crashes: u64,
    erasures: u64,
    forced_reseals: u64,
}

/// The model test: agents of the four policies write steps; reads of every kind are compared byte for byte with the
/// model while the compactor (drop on, derived documents on) absorbs, merges, reseals (encoding, re-encoding) and
/// drops; waiters crash, some in the middle of a reseal; threads are erased. `FX_D_OPS`, `FX_D_SEED`, `FX_D_CODEC=lz4`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_model_with_crashes_and_erasures() {
    let _s = SERIAL.lock().await;
    setup();
    let n_ops: u64 = std::env::var("FX_D_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(4_000);
    let seed: u64 = std::env::var("FX_D_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(20261110);
    let codec = if std::env::var("FX_D_CODEC").as_deref() == Ok("lz4") { BlockCodec::Lz4 } else { BlockCodec::Zstd };
    let mut rng = StdRng::seed_from_u64(seed);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-derive-model";
    let mut t = lc(&store).create(id).await.unwrap();
    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut threads: Vec<Thread> = (0..24).map(new_thread).collect();
    let mut oid = 0u64;
    // history before the first version, plus keys of a collection that is not registered and the shared keyspace
    for th in threads.iter_mut() {
        for _ in 0..3 {
            let rows = step(&mut rng, th, &mut oid);
            apply(&mut t, &mut m, rows.into_iter().map(|(k, v)| (k, Some(v))).collect()).await;
        }
        let other = tkey(&th.name, "notes", "d", 1);
        apply(&mut t, &mut m, vec![(other, Some(vec![b'n'; 200]))]).await;
    }
    for i in 0..50 {
        apply(&mut t, &mut m, vec![(format!("doc/plain/{i:05}").into_bytes(), Some(vec![b'p'; 100]))]).await;
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    m.insert(crate::envelope::MARKER.to_vec(), b"E1".to_vec());
    T_PIN_MS.store(350, Relaxed);
    let k = knobs(codec);
    let mut c = start(&mut t, &store, &k).await;
    let mut st = St::default();
    let mut snaps: Vec<(Arc<read::Snap>, BTreeMap<Vec<u8>, Vec<u8>>, Instant)> = Vec::new();
    let mut erased: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    let mut agg = compactor::Stats::default();
    let take = |c: &Compactor, agg: &mut compactor::Stats| {
        let s = &c.sh.stats;
        for (a, b) in [
            (&agg.derive_docs, &s.derive_docs),
            (&agg.derive_encoded, &s.derive_encoded),
            (&agg.derive_messages, &s.derive_messages),
            (&agg.derive_rebuilt, &s.derive_rebuilt),
            (&agg.derive_fallbacks, &s.derive_fallbacks),
            (&agg.reseals, &s.reseals),
        ] {
            a.fetch_add(b.load(Relaxed), Relaxed);
        }
    };
    macro_rules! check {
        ($what:expr, $got:expr, $want:expr) => {
            if $got != $want {
                st.wrong += 1;
                if st.wrong <= 5 {
                    eprintln!("WRONG at op {}: {}", st.ops, $what);
                }
            }
        };
    }
    let rebuilds0 = derive::REBUILDS.load(Relaxed);
    for op in 0..n_ops {
        st.ops = op + 1;
        t.fx_sync();
        let r = rng.gen_range(0..1000);
        let live: Vec<usize> = (0..threads.len()).filter(|&i| threads[i].step > 0).collect();
        if r < 380 {
            // agents write a step each
            let n = rng.gen_range(1..4);
            for _ in 0..n {
                let i = rng.gen_range(0..threads.len());
                if threads[i].step >= 40 {
                    // the conversation ends; a new thread takes its slot (the old one stays readable)
                    let gen = threads[i].name.len();
                    threads[i] = Thread { name: format!("{}n{gen}", threads[i].name), ..new_thread(i) };
                }
                let rows = step(&mut rng, &mut threads[i], &mut oid);
                apply(&mut t, &mut m, rows.into_iter().map(|(k, v)| (k, Some(v))).collect()).await;
                st.steps += 1;
            }
        } else if r < 400 && !live.is_empty() {
            // the saver rewrites an old checkpoint document (an upsert of the same checkpoint id)
            let th = &threads[live[rng.gen_range(0..live.len())]];
            let s = rng.gen_range(1..=th.step) as u64;
            let key = tkey(&th.name, "ck", "d", s);
            if let Some(old) = m.get(&key).cloned() {
                let mut v = old.clone();
                let n = v.len();
                v[n - 2] ^= 1; // the metadata's last byte changes
                apply(&mut t, &mut m, vec![(key, Some(v))]).await;
            }
        } else if r < 600 && !live.is_empty() {
            // get_tuple: the latest checkpoint of a thread, or an old one, and its writes
            let th = &threads[live[rng.gen_range(0..live.len())]];
            let s = if rng.gen_bool(0.5) { th.step } else { rng.gen_range(1..=th.step) } as u64;
            for key in [tkey(&th.name, "ck", "d", s), tkey(&th.name, "cw", "d", s)] {
                match t.get(&key).await {
                    Ok(got) => check!(format!("get {:?}", String::from_utf8_lossy(&key)), got.map(|b| b.to_vec()), m.get(&key).cloned()),
                    Err(e) if e.downcast_ref::<Retry>().is_some() => st.retries += 1,
                    Err(e) => panic!("get: {e:#}"),
                }
                st.gets += 1;
            }
        } else if r < 720 {
            // range reads: one thread (list), a slice of threads, both ways, with limits
            let (lo, hi) = if rng.gen_bool(0.6) && !live.is_empty() {
                trange(&threads[live[rng.gen_range(0..live.len())]].name)
            } else {
                let a = trange(&threads[rng.gen_range(0..threads.len())].name).0;
                let b = trange(&threads[rng.gen_range(0..threads.len())].name).1;
                if a < b {
                    (a, b)
                } else {
                    (b.clone(), a.clone())
                }
            };
            let rev = rng.gen_bool(0.4);
            let limit = if rng.gen_bool(0.5) { Some(rng.gen_range(1..12)) } else { None };
            match t.scan_range(&lo, Some(&hi), rev, limit, 64 << 10).await {
                Ok(got) => check!("range", as_vec(got), model_range(&m, &lo, Some(&hi), rev, limit)),
                Err(e) if e.downcast_ref::<Retry>().is_some() => st.retries += 1,
                Err(e) => panic!("scan: {e:#}"),
            }
            st.ranges += 1;
        } else if r < 820 {
            // held transactions
            if snaps.len() < 3 && rng.gen_bool(0.5) {
                let s = t.txn_snapshot().await.unwrap();
                snaps.push((s, m.clone(), Instant::now()));
            } else if !snaps.is_empty() {
                let i = rng.gen_range(0..snaps.len());
                let (s, sm, at) = &snaps[i];
                let keys: Vec<&Vec<u8>> = sm.keys().collect();
                let key = keys[rng.gen_range(0..keys.len())].clone();
                match s.get(&key).await {
                    Ok(got) => check!("txn get", got.map(|b| b.to_vec()), sm.get(&key).cloned()),
                    Err(e) if e.downcast_ref::<Retry>().is_some() => {
                        st.retries += 1;
                        snaps.remove(i);
                        continue;
                    }
                    Err(e) => panic!("txn get: {e:#}"),
                }
                st.txn_reads += 1;
                if at.elapsed() > Duration::from_millis(HOLD_MS - 500) {
                    snaps.remove(i);
                }
            }
        } else if r < 828 && !live.is_empty() {
            // erasure of a thread's customer: every key deleted, then the range erased
            let i = live[rng.gen_range(0..live.len())];
            let (lo, hi) = trange(&threads[i].name);
            let gone: Vec<Vec<u8>> = m.range(lo.clone()..hi.clone()).map(|(k, _)| k.clone()).collect();
            if gone.is_empty() {
                continue;
            }
            let commit = t.stage(gone.iter().map(|k| Op::Delete(k.clone())).collect()).await.unwrap().unwrap();
            let seq = commit.seqnum();
            commit.wait().await.unwrap();
            for key in &gone {
                m.remove(key);
            }
            c.erase(lo.clone(), hi.clone(), seq);
            erased.push((lo, hi, seq));
            threads[i] = Thread { name: format!("{}e", threads[i].name), ..new_thread(i) };
            st.erasures += 1;
        } else if r < 860 {
            // reseal everything now (the planner reseals on its own as well)
            absorb_all(&mut t, &c).await;
            st.forced_reseals += reseal_all(&c).await;
            t.fx_sync();
        } else if r < 866 && op > 200 && op < n_ops * 3 / 4 {
            // a crash: the waiter dies, half the time in the middle of a reseal (its run never completes)
            let mid = rng.gen_bool(0.5);
            if mid {
                absorb_all(&mut t, &c).await;
                derive::FAIL_AT_UNIT.store(rng.gen_range(1..4), Relaxed);
                let v = c.current().await;
                let gi = rng.gen_range(0..v.groups.len());
                let r = c.reseal_once(Some(gi)).await;
                derive::FAIL_AT_UNIT.store(0, Relaxed);
                st.mid_reseal_crashes += r.is_err() as u64;
            }
            c.stop().await;
            take(&c, &mut agg);
            snaps.clear();
            t.abandon().await;
            t = lc(&store).wake(id).await.unwrap();
            t.upgrade().await.unwrap();
            c = start(&mut t, &store, &k).await;
            for (lo, hi, seq) in &erased {
                c.erase(lo.clone(), hi.clone(), *seq);
            }
            st.crashes += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(rng.gen_range(0..3))).await;
        }
    }
    // finish: erasures done, everything absorbed and resealed, then check every key and full scans
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(30) {
        t.fx_sync();
        let pending = c.sh.erasures.lock().unwrap().iter().filter(|e| e.done.is_none()).count();
        if pending == 0 {
            break;
        }
        apply(&mut t, &mut m, vec![(b"doc/plain/00000".to_vec(), Some(b"tick".to_vec()))]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    absorb_all(&mut t, &c).await;
    reseal_all(&c).await;
    t.fx_sync();
    view::clear_cache();
    for rev in [false, true] {
        let all = t.scan_range(b"", None, rev, None, 1 << 20).await.unwrap();
        check!("full scan", as_vec(all), model_range(&m, b"", None, rev, None));
    }
    for (key, want) in &m {
        let got = t.get(key).await.unwrap();
        check!("final get", got.as_deref(), Some(&want[..]));
    }
    // the newest version: derived rows exist, and no row of an erased thread (message rows and skeletons included)
    let v = c.current().await;
    let rows = all_run_rows(&store, &v).await;
    let derived = rows.iter().filter(|r| r.env[0] == runfmt::DERIVED).count();
    let parts = rows.iter().filter(|r| r.env[0] == runfmt::PART).count();
    let mut leftovers = 0;
    for row in &rows {
        for (lo, hi, seq) in &erased {
            if row.key >= *lo && row.key < *hi && (row.seq <= *seq || matches!(row.env[0], runfmt::DERIVED | runfmt::PART)) && v.x_d >= *seq {
                leftovers += 1;
            }
        }
    }
    let pending = c.sh.erasures.lock().unwrap().iter().filter(|e| e.done.is_none()).count();
    // GC, then every read again
    gc::gc_once(&c).await.unwrap();
    tokio::time::sleep(k.gc_grace + Duration::from_millis(300)).await;
    gc::gc_once(&c).await.unwrap();
    view::clear_cache();
    t.fx_sync();
    let all = t.scan_range(b"", None, false, None, 1 << 20).await.unwrap();
    check!("full scan after GC", as_vec(all), model_range(&m, b"", None, false, None));
    c.stop().await;
    take(&c, &mut agg);
    t.abandon().await;
    let rebuilds = derive::REBUILDS.load(Relaxed) - rebuilds0;
    teardown();
    eprintln!("derived model ({codec:?}): {st:?}");
    eprintln!(
        "derived model: reseals {}, checkpoint docs seen by reseals {}, encoded {}, message rows written {}, derived rebuilt by reseals {}, fallbacks {}, reader rebuilds {rebuilds}; newest version: {derived} derived rows, {parts} message rows; erasure leftovers {leftovers}, erasures pending {pending}",
        agg.reseals.load(Relaxed),
        agg.derive_docs.load(Relaxed),
        agg.derive_encoded.load(Relaxed),
        agg.derive_messages.load(Relaxed),
        agg.derive_rebuilt.load(Relaxed),
        agg.derive_fallbacks.load(Relaxed)
    );
    assert_eq!(st.wrong, 0, "wrong answers");
    assert_eq!(leftovers, 0, "rows of erased threads left in live runs");
    assert_eq!(pending, 0, "erasures not done");
    assert!(derived > 0 && parts > 0 && rebuilds > 0, "derived documents were written and read");
    assert!(agg.derive_rebuilt.load(Relaxed) > 0, "a reseal re-encoded derived documents");
    assert_eq!(agg.derive_fallbacks.load(Relaxed), 0, "every document cut exactly");
    assert!(st.crashes > 0 && st.erasures > 0, "crashes and erasures ran");
}

/// A written version of threads, every group resealed with derived documents on: the returned map is the model.
async fn derived_db(store: &Arc<dyn ObjectStore>, id: &str, threads: usize, steps: usize, k: &Knobs) -> (Tenant, Arc<Compactor>, BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut rng = StdRng::seed_from_u64(7);
    let mut t = lc(store).create(id).await.unwrap();
    let mut m = BTreeMap::new();
    let mut ths: Vec<Thread> = (0..threads).map(new_thread).collect();
    let mut oid = 0;
    for _ in 0..steps {
        for th in ths.iter_mut() {
            let rows = step(&mut rng, th, &mut oid);
            apply(&mut t, &mut m, rows.into_iter().map(|(k, v)| (k, Some(v))).collect()).await;
        }
    }
    publish(&mut t, &small_opts(build::Reseal::All)).await;
    let c = start(&mut t, store, k).await;
    absorb_all(&mut t, &c).await;
    // the first version's groups cut threads anywhere; reseals merge the neighbours and split at thread boundaries
    for _ in 0..4 {
        reseal_all(&c).await;
    }
    t.fx_sync();
    assert!(c.sh.stats.derive_encoded.load(Relaxed) > 0 || !derive::WRITE.load(Relaxed), "nothing was encoded");
    (t, c, m)
}

async fn count_wrong(t: &Tenant, m: &BTreeMap<Vec<u8>, Vec<u8>>) -> (u64, u64, String) {
    let (mut wrong, mut errors, mut first) = (0, 0, String::new());
    for (key, want) in m {
        match t.get(key).await {
            Ok(got) => wrong += (got.as_deref() != Some(&want[..])) as u64,
            Err(e) => {
                if first.is_empty() {
                    first = format!("{e:#}");
                }
                errors += 1
            }
        }
    }
    (wrong, errors, first)
}

/// A planted bug (each skeleton's second reference points at the next message) is caught: with the reader's CRC check,
/// every affected read is an error, never a wrong document; with the check off, the byte comparison of this test finds
/// the wrong documents. Without the plant: 0 wrong, 0 errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planted_wrong_reference_is_caught() {
    let _s = SERIAL.lock().await;
    let mut results = Vec::new();
    for (plant, crc) in [(0, true), (1, true), (1, false)] {
        setup();
        derive::PLANT.store(plant, Relaxed);
        derive::CHECK_CRC.store(crc, Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let k = knobs(BlockCodec::Zstd);
        let (t, c, m) = derived_db(&store, &format!("fx-plant-{plant}-{crc}"), 8, 12, &k).await;
        view::clear_cache();
        let (wrong, errors, first) = count_wrong(&t, &m).await;
        let scan_ok = t.scan_range(b"u/", Some(b"u0"), false, None, 1 << 20).await.map(|r| as_vec(r) == model_range(&m, b"u/", Some(b"u0"), false, None));
        eprintln!("plant {plant} crc check {crc}: wrong {wrong}, errors {errors} ({first}); full scan equal: {scan_ok:?}; encoded {}", c.sh.stats.derive_encoded.load(Relaxed));
        results.push((plant, crc, wrong, errors, scan_ok.ok()));
        c.stop().await;
        t.abandon().await;
        teardown();
    }
    assert_eq!(results[0].2 + results[0].3, 0, "no plant: 0 wrong, 0 errors");
    assert_eq!(results[0].4, Some(true));
    assert!(results[1].2 == 0 && results[1].3 > 0, "plant with the CRC check: errors, never a wrong answer");
    assert_eq!(results[1].4, None, "the scan fails");
    assert!(results[2].2 > 0, "plant without the CRC check: the comparison finds wrong documents");
}

/// Format v2: written only for zstd or derived rows (else v1, as before); a reader that knows v1 only refuses it at the
/// trailer. The rollback: with the release flag off, the next reseal writes the same documents plain, in v1 runs again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_reader_refuses_v2_and_rollback_writes_plain() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut k = knobs(BlockCodec::Zstd);
    let (mut t, c, m) = derived_db(&store, "fx-v2", 6, 10, &k).await;
    let v = c.current().await;
    let mut v2 = 0;
    let mut refused = 0;
    for g in v.groups.iter().filter_map(|g| g.sealed.clone()) {
        let path = object_store::path::Path::from(g.path.as_str());
        let tail = runfmt::get_range(&store, &path, g.size - runfmt::TRAILER, g.size).await.unwrap();
        let (run, _) = runfmt::Run::open(&store, path, g.size).await.unwrap();
        if run.version == 2 {
            v2 += 1;
            refused += runfmt::parse_trailer_known(&tail, g.size, 1).is_err() as u32;
            assert!(runfmt::parse_trailer(&tail, g.size).is_ok());
        }
    }
    assert!(v2 > 0 && refused == v2, "every v2 run is refused by a v1 reader ({refused} of {v2})");
    let (wrong, errors, _) = count_wrong(&t, &m).await;
    assert_eq!((wrong, errors), (0, 0));
    // rollback: the release flag off, lz4 again; the next reseal writes plain documents in v1 runs
    c.stop().await;
    derive::WRITE.store(false, Relaxed);
    k.sealed.codec = BlockCodec::Lz4;
    let c2 = Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    reseal_all(&c2).await;
    t.fx_sync();
    let v = c2.current().await;
    let rows = all_run_rows(&store, &v).await;
    let derived = rows.iter().filter(|r| matches!(r.env[0], runfmt::DERIVED | runfmt::PART)).count();
    let mut versions = Vec::new();
    for g in v.groups.iter().filter_map(|g| g.sealed.clone()) {
        versions.push(runfmt::Run::open(&store, object_store::path::Path::from(g.path.as_str()), g.size).await.unwrap().0.version);
    }
    view::clear_cache();
    let (wrong, errors, _) = count_wrong(&t, &m).await;
    eprintln!("v2 runs {v2}, refused by a v1 reader {refused}; after the rollback: derived rows {derived}, sealed run versions {versions:?}, wrong {wrong}, errors {errors}, rebuilt by the reseal {}", c2.sh.stats.derive_rebuilt.load(Relaxed));
    assert_eq!(derived, 0);
    assert!(versions.iter().all(|&x| x == 1));
    assert_eq!((wrong, errors), (0, 0));
    assert!(c2.sh.stats.derive_rebuilt.load(Relaxed) > 0);
    c2.stop().await;
    t.abandon().await;
    teardown();
}

/// Takeover: waiter A holds a transaction over derived documents; B takes the database over, writes new steps and
/// reseals (re-encoding the threads) and folds. A's transaction reads its snapshot or fails, never a wrong document; B
/// reads every document exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_while_threads_are_reencoded() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let k = knobs(BlockCodec::Zstd);
    let id = "fx-derive-take";
    let (mut a, ca, mut m) = derived_db(&store, id, 6, 8, &k).await;
    a.set_serving_until(Some(Instant::now() + k.serving_lease));
    let txn = a.txn_snapshot().await.unwrap();
    let at = m.clone();
    let mut b = lc(&store).wake(id).await.unwrap();
    b.fx_own_holds();
    b.upgrade().await.unwrap();
    let cb = start(&mut b, &store, &k).await;
    let mut rng = StdRng::seed_from_u64(11);
    let mut ths: Vec<Thread> = (0..6).map(|i| Thread { step: 100, ..new_thread(i) }).collect();
    let mut oid = 1 << 40;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(HOLD_MS - 800) {
        for th in ths.iter_mut() {
            let rows = step(&mut rng, th, &mut oid);
            apply(&mut b, &mut m, rows.into_iter().map(|(k, v)| (k, Some(v))).collect()).await;
        }
        absorb_all(&mut b, &cb).await;
        reseal_all(&cb).await;
        cb.drop.request_refold();
        b.fx_sync();
    }
    let (mut wrong, mut errors) = (0, 0);
    for (key, want) in &at {
        match txn.get(key).await {
            Ok(got) => wrong += (got.as_deref() != Some(&want[..])) as u64,
            Err(_) => errors += 1,
        }
    }
    view::clear_cache();
    let (bw, be, first) = count_wrong(&b, &m).await;
    eprintln!("takeover: A's transaction wrong {wrong}, errors {errors} of {}; B wrong {bw}, errors {be} ({first}); B encoded {}, rebuilt {}", at.len(), cb.sh.stats.derive_encoded.load(Relaxed), cb.sh.stats.derive_rebuilt.load(Relaxed));
    assert_eq!(wrong + bw + be, 0);
    assert!(cb.sh.stats.derive_rebuilt.load(Relaxed) > 0);
    ca.stop().await;
    cb.stop().await;
    b.abandon().await;
    let _ = a.abandon().await;
    teardown();
}

/// PITR and forks read a SlateDB checkpoint through the version paired with it (step 7a). A checkpoint taken while the
/// threads are derived documents keeps answering the documents of its time, byte for byte, after the threads moved on,
/// were re-encoded, dropped from SlateDB and garbage collected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_checkpoint_reads_exact_documents() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let k = knobs(BlockCodec::Zstd);
    let id = "fx-derive-pitr";
    let (mut t, c, mut m) = derived_db(&store, id, 6, 10, &k).await;
    let at = m.clone();
    let admin = slatedb::admin::Admin::builder(crate::lifecycle::tenant_path(id), Arc::clone(&store)).build();
    let (cp, pair) = pair::checkpoint_paired(&store, id, || async { Ok(admin.create_detached_checkpoint(&slatedb::config::CheckpointOptions::default()).await?) }).await.unwrap();
    assert!(pair.as_ref().and_then(|p| p.version).is_some(), "{pair:?}");
    // the threads move on: new steps, re-encoded, folded, GC
    let mut rng = StdRng::seed_from_u64(5);
    let mut ths: Vec<Thread> = (0..6).map(|i| Thread { step: 200, ..new_thread(i) }).collect();
    let mut oid = 1 << 41;
    for _ in 0..6 {
        for th in ths.iter_mut() {
            let rows = step(&mut rng, th, &mut oid);
            apply(&mut t, &mut m, rows.into_iter().map(|(k, v)| (k, Some(v))).collect()).await;
        }
        absorb_all(&mut t, &c).await;
        reseal_all(&c).await;
    }
    c.drop.request_refold();
    tokio::time::sleep(k.gc_grace + Duration::from_millis(300)).await;
    gc::gc_once(&c).await.unwrap();
    view::clear_cache();
    let mut o = slatedb::config::DbReaderOptions::default();
    o.skip_wal_replay = true;
    let r = pair::PairedReader::open_checkpoint(&store, id, cp.id, o).await.unwrap();
    let rows = as_vec(r.scan(b"u/", Some(b"u0"), false, None, 1 << 20).await.unwrap());
    let want = model_range(&at, b"u/", Some(b"u0"), false, None);
    let mut wrong = (rows != want) as u64;
    let mut n = 0;
    for (key, v) in &at {
        n += 1;
        wrong += (r.get(key).await.unwrap().as_deref() != Some(&v[..])) as u64;
    }
    r.close().await.unwrap();
    let (lw, le, _) = count_wrong(&t, &m).await;
    eprintln!("paired checkpoint: {} rows scanned, {n} gets, wrong {wrong}; live database wrong {lw}, errors {le}", rows.len());
    assert_eq!(wrong + lw + le, 0);
    c.stop().await;
    t.abandon().await;
    teardown();
}

// ---- review (fenvix-brain prompts/R-review-c2-delta-log.md)

/// Review, check 1: three more planted bugs, each placed after the encoder's own check (as a bug in the writing would
/// be): 2, a literal range one byte short; 3, references that resolve to another thread's messages (a thread's message
/// rows hold the message table of the thread encoded before it); 4, a message row missing while a skeleton refers to
/// it. With the reader's checks each one is an error, never a wrong document. With the CRC check off, the byte
/// comparison finds 2 and 3, and 4 is still an error (a missing message cannot be assembled).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_more_planted_bugs_are_caught() {
    let _s = SERIAL.lock().await;
    let mut out = Vec::new();
    for (plant, crc) in [(2, true), (2, false), (3, true), (3, false), (4, true), (4, false)] {
        setup();
        derive::PLANT.store(plant, Relaxed);
        derive::CHECK_CRC.store(crc, Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let k = knobs(BlockCodec::Zstd);
        let (t, c, m) = derived_db(&store, &format!("fx-rplant-{plant}-{crc}"), 8, 12, &k).await;
        view::clear_cache();
        let (wrong, errors, first) = count_wrong(&t, &m).await;
        let scan = t.scan_range(b"u/", Some(b"u0"), false, None, 1 << 20).await.map(|r| as_vec(r) == model_range(&m, b"u/", Some(b"u0"), false, None));
        eprintln!("review plant {plant} crc check {crc}: {} docs, wrong {wrong}, errors {errors} ({first}); full scan equal: {scan:?}; encoded {}", m.len(), c.sh.stats.derive_encoded.load(Relaxed));
        out.push((plant, crc, wrong, errors, scan.ok()));
        c.stop().await;
        t.abandon().await;
        teardown();
    }
    for &(plant, crc, wrong, errors, scan) in &out {
        if crc || plant == 4 {
            assert!(wrong == 0 && errors > 0 && scan.is_none(), "plant {plant} crc {crc}: errors, never a wrong document ({wrong} wrong, {errors} errors, scan {scan:?})");
        } else {
            assert!(wrong > 0 && scan == Some(false), "plant {plant} without the CRC: the comparison finds it ({wrong} wrong, scan {scan:?})");
        }
    }
}

/// Review, check 1: a follower (another process: its own lifecycle, reader mode) reads every derived document exactly,
/// point reads and both scan directions with limits, before and after the writer re-encodes every thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_follower_reads_derived_documents_exactly() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let k = knobs(BlockCodec::Zstd);
    let id = "fx-review-follower";
    let (mut t, c, mut m) = derived_db(&store, id, 8, 12, &k).await;
    let mut f = lc(&store).wake(id).await.unwrap();
    f.fx_refresh().await.unwrap();
    let mut wrong = 0u64;
    let mut reads = 0u64;
    let rb0 = derive::REBUILDS.load(Relaxed);
    for round in 0..2 {
        view::clear_cache();
        let (w, e, first) = count_wrong(&f, &m).await;
        assert_eq!(e, 0, "follower errors: {first}");
        wrong += w;
        reads += m.len() as u64;
        for rev in [false, true] {
            for limit in [None, Some(3), Some(40)] {
                let got = as_vec(f.scan_range(b"u/", Some(b"u0"), rev, limit, 64 << 10).await.unwrap());
                wrong += (got != model_range(&m, b"u/", Some(b"u0"), rev, limit)) as u64;
                reads += 1;
            }
        }
        if round == 0 {
            // the writer moves on: new steps on every thread, absorbed and re-encoded; the follower refreshes
            let mut rng = StdRng::seed_from_u64(99);
            let mut ths: Vec<Thread> = (0..8).map(|i| Thread { step: 300, ..new_thread(i) }).collect();
            let mut oid = 1 << 42;
            for th in ths.iter_mut() {
                let rows = step(&mut rng, th, &mut oid);
                apply(&mut t, &mut m, rows.into_iter().map(|(k, v)| (k, Some(v))).collect()).await;
            }
            absorb_all(&mut t, &c).await;
            reseal_all(&c).await;
            t.fx_sync();
            f.fx_refresh().await.unwrap();
        }
    }
    let rebuilds = derive::REBUILDS.load(Relaxed) - rb0;
    eprintln!("review follower: {reads} reads, wrong {wrong}, follower rebuilds {rebuilds}, writer rebuilt by reseals {}", c.sh.stats.derive_rebuilt.load(Relaxed));
    assert_eq!(wrong, 0);
    assert!(rebuilds > 0 && c.sh.stats.derive_rebuilt.load(Relaxed) > 0);
    c.stop().await;
    let _ = f.abandon().await;
    t.abandon().await;
    teardown();
}

/// Review, check 3: what the encoder rewrites in a registered collection. The prompt (P-c2-delta-log task 3, R-review
/// check 3): only LangGraph Mongo-saver checkpoint documents; a collection that is not registered, or a document of
/// another shape, is left untouched. The encoder looks at the collection name only (`LenSpec` here, `AgentSpec` in the
/// server: the same name test), then cuts any msgpack array inside any binary field. Each case is encoded alone, as
/// one thread; every encoded document must still rebuild exactly.
#[test]
fn review_near_miss_shapes_are_left_untouched() {
    use derive::Spec as _;
    let spec = LenSpec { colls: vec![b"ck".to_vec(), b"cw".to_vec()] };
    let mut rng = StdRng::seed_from_u64(3);
    let mut th = new_thread(0);
    let mut oid = 0u64;
    let mut saver = Vec::new();
    for _ in 0..4 {
        saver = step(&mut rng, &mut th, &mut oid);
    }
    let msgs: Vec<Vec<u8>> = (0..4).map(|i| message(&mut rng, 300, 900 + i)).collect();
    let mut arr = Vec::new();
    mp_array_header(&mut arr, msgs.len());
    for m in &msgs {
        arr.extend(m);
    }
    let sub = |inner: &[u8]| {
        let mut b = Bson::new().oid("_id", 7).str("thread_id", "t").str("checkpoint_id", "1f0").str("type", "bson");
        b.name(0x03, "checkpoint");
        b.0.extend(inner);
        b.done()
    };
    let json: String = format!("{{\"v\":4,\"channel_values\":{{\"messages\":[{}]}}}}", (0..4).map(|i| format!("{{\"type\":\"ai\",\"content\":\"{}\"}}", "x".repeat(200 + i))).collect::<Vec<_>>().join(","));
    let mut trailing = arr.clone();
    trailing.push(0);
    let cases: Vec<(&str, Vec<u8>, &str, bool)> = vec![
        ("LangGraph saver checkpoint (control)", saver[0].1.clone(), "ck", true),
        ("LangGraph saver write (control)", saver[1].1.clone(), "cw", true),
        ("the same saver document in a collection that is not registered", saver[0].1.clone(), "notes", false),
        ("LangGraph JS saver (type json)", Bson::new().oid("_id", 1).str("thread_id", "t").str("type", "json").bin("checkpoint", json.as_bytes()).done(), "ck", false),
        ("a saver version with the checkpoint as a BSON subdocument", sub(&Bson::new().bin("messages", &arr).done()), "ck", false),
        ("msgpack followed by a stray byte", Bson::new().oid("_id", 2).str("thread_id", "t").str("type", "msgpack").bin("checkpoint", &trailing).done(), "ck", false),
        ("another app's document in a collection named like the saver's", Bson::new().oid("_id", 3).str("user_id", "u1").str("kind", "game-save").bin("state", &arr).done(), "ck", false),
        ("a saver-like document without its type field", Bson::new().oid("_id", 4).str("thread_id", "t").str("checkpoint_id", "1f0").bin("checkpoint", &arr).done(), "ck", false),
    ];
    let mut bad = Vec::new();
    for (i, (what, doc, coll, want_encoded)) in cases.iter().enumerate() {
        let key = tkey(&format!("t{i}"), coll, "d", 1);
        let p = spec.customer_prefix(&key).unwrap();
        let mut env = vec![runfmt::LIVE];
        env.extend(doc);
        let mut st = derive::EncStats::default();
        let out = derive::encode_customer(&spec, &key[..p], vec![(key.clone(), 1, env.clone())], &mut st).unwrap();
        let parts: std::collections::HashMap<u32, Vec<u8>> = out.rows.iter().filter(|r| r.2[0] == runfmt::PART).map(|r| (derive::parse_msg_key(&r.0).unwrap().1, r.2[1..].to_vec())).collect();
        let d = out.rows.iter().find(|r| r.0 == key).unwrap();
        let rebuilt = if d.2[0] == runfmt::DERIVED { derive::assemble(&derive::parse_skeleton(&d.2).unwrap(), &|i| parts.get(&i).cloned()).unwrap() } else { d.2[1..].to_vec() };
        assert_eq!(&rebuilt, doc, "{what}: rebuilt exactly");
        let encoded = st.encoded > 0;
        eprintln!("review near-miss: {what}: encoded {encoded} (message rows {}), expected {want_encoded}", parts.len());
        if encoded != *want_encoded {
            bad.push(*what);
        }
    }
    assert!(bad.is_empty(), "rewritten although not a LangGraph Mongo-saver document: {bad:?}");
}

/// Review, check 2: the rollback to E0 (release flag off). Derived rows leave a group only when it is resealed: with no
/// new writes, the compactor has nothing due, so a quiet group keeps them, readable by this release and refused (never
/// misread) by a release before it. Facts only: the test passes and prints the counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_rollback_leaves_derived_rows_in_quiet_groups() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut k = knobs(BlockCodec::Zstd);
    let id = "fx-review-rollback";
    let (mut t, c, m) = derived_db(&store, id, 8, 12, &k).await;
    c.stop().await;
    derive::WRITE.store(false, Relaxed);
    k.sealed.codec = BlockCodec::Lz4;
    let c2 = Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    c2.drain(true).await.unwrap();
    c2.run();
    tokio::time::sleep(Duration::from_secs(3)).await;
    t.fx_sync();
    let v = c2.current().await;
    let rows = all_run_rows(&store, &v).await;
    let derived = rows.iter().filter(|r| matches!(r.env[0], runfmt::DERIVED | runfmt::PART)).count();
    let (mut v2, mut refused) = (0, 0);
    for g in v.groups.iter().filter_map(|g| g.sealed.clone()) {
        let path = object_store::path::Path::from(g.path.as_str());
        let tail = runfmt::get_range(&store, &path, g.size - runfmt::TRAILER, g.size).await.unwrap();
        if runfmt::Run::open(&store, path, g.size).await.unwrap().0.version == 2 {
            v2 += 1;
            refused += runfmt::parse_trailer_known(&tail, g.size, 1).is_err() as u32;
        }
    }
    view::clear_cache();
    let (wrong, errors, _) = count_wrong(&t, &m).await;
    eprintln!("review rollback: flag off, compactor idle for 3 s: reseals {}, derived rows still in runs {derived}, v2 sealed runs {v2} (a v1 reader refuses {refused}), wrong {wrong}, errors {errors}", c2.sh.stats.reseals.load(Relaxed));
    assert_eq!((wrong, errors), (0, 0));
    assert_eq!(refused, v2);
    c2.stop().await;
    t.abandon().await;
    teardown();
}

// ---- review trial merges (research/c2-delta-log/review/trial-merges), taken unchanged in the fix round

/// Review, check 7 (trial merge with c2-step5b-reader): step 5's one-wave reader (`view::WAVE`, `View::get_wave` and
/// `read::get`'s wave branch) reads a sealed run's row without `derived_row`, so a point read of a derived document
/// returns its skeleton as the document. Every point read must equal the model, wave on or off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_wave_reader_rebuilds_derived_documents() {
    let _s = SERIAL.lock().await;
    let mut res = Vec::new();
    for wave in [false, true] {
        setup();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let k = knobs(BlockCodec::Zstd);
        let (t, c, m) = derived_db(&store, &format!("fx-review-wave-{wave}"), 8, 12, &k).await;
        view::clear_cache();
        view::WAVE.store(wave, Relaxed);
        let (wrong, errors, first) = count_wrong(&t, &m).await;
        let scan = t.scan_range(b"u/", Some(b"u0"), false, None, 1 << 20).await.map(|r| as_vec(r) == model_range(&m, b"u/", Some(b"u0"), false, None));
        view::WAVE.store(false, Relaxed);
        eprintln!("review wave {wave}: {} point reads, wrong {wrong}, errors {errors} ({first}); range scan equal {scan:?}; encoded {}", m.len(), c.sh.stats.derive_encoded.load(Relaxed));
        res.push((wave, wrong, errors, scan.ok()));
        c.stop().await;
        t.abandon().await;
        teardown();
    }
    for (wave, wrong, errors, scan) in res {
        assert_eq!((wrong, errors, scan), (0, 0, Some(true)), "wave {wave}");
    }
}

/// Review, check 4 (trial merge into c2-step8-migration, which carries step 7b's erasure): a thread's customer is erased
/// the way the server does it (`cmd_data.rs` `erasure_targets`: one erasure per collection, target range
/// `u/{customer}{collection}/` plus the deleted keys). The pass must finish, and afterwards no row of the thread may be
/// left in any run of the newest version: message rows (`u/{customer}\0fxm/...`) and skeletons included. Run with the
/// scrub's default options (as the server passes them) and with sealed runs opened for derived rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_erasure_of_a_derived_thread() {
    let _s = SERIAL.lock().await;
    let mut res = Vec::new();
    for derived_opts in [false, true] {
        setup();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let k = knobs(BlockCodec::Zstd);
        let id = format!("fx-review-erase-{derived_opts}");
        let (t0, c, mut m) = derived_db(&store, &id, 8, 12, &k).await;
        c.stop().await;
        t0.abandon().await;
        // the erasure pass runs on a writer without a compactor (step 8's lifecycle refuses it otherwise)
        let mut t = lc(&store).wake(&id).await.unwrap();
        t.upgrade().await.unwrap();
        t.fx_refresh().await.unwrap();
        let name = "th000";
        let mut comp = vec![name.len() as u8];
        comp.extend(name.as_bytes());
        let (tlo, thi) = trange(name);
        let mut ids = Vec::new();
        for coll in ["ck", "cw"] {
            let mut lo = b"u/".to_vec();
            lo.extend(&comp);
            lo.extend(coll.as_bytes());
            lo.push(b'/');
            let hi = runfmt::prefix_successor(&lo).unwrap();
            let gone: Vec<Vec<u8>> = m.range(lo.clone()..hi.clone()).map(|(k, _)| k.clone()).collect();
            assert!(!gone.is_empty());
            t.apply(gone.iter().map(|k| Op::Delete(k.clone())).collect()).await.unwrap();
            for key in &gone {
                m.remove(key);
            }
            let mut tg = erase::Targets::default();
            tg.ranges.push((lo, hi));
            for key in &gone {
                tg.add_key(key.clone());
            }
            ids.push(t.fx_erasure_register(coll, &comp, &tg, None).await.unwrap().expect("recorded").id);
        }
        let mut opts = erase::ScrubOpts { sealed: k.sealed.clone(), delta: k.delta.clone(), budget: None };
        opts.sealed.derived = true;
        if !derived_opts {
            opts = erase::ScrubOpts::default();
        }
        let pass = t.fx_erase_pass(&opts).await;
        let pass_s = match &pass {
            Ok(all) => format!("ok, states {:?}", all.iter().filter(|e| ids.contains(&e.id)).map(|e| e.state.clone()).collect::<Vec<_>>()),
            Err(e) => format!("error: {e:#}"),
        };
        t.fx_refresh().await.unwrap();
        let v = t.fx_view().unwrap().meta.clone();
        let rows = all_run_rows(&store, &v).await;
        let left: Vec<&compactor::Row> = rows.iter().filter(|r| r.key >= tlo && r.key < thi).collect();
        let left_msgs = left.iter().filter(|r| r.env[0] == runfmt::PART).count();
        let left_skel = left.iter().filter(|r| r.env[0] == runfmt::DERIVED).count();
        let left_plain = left.len() - left_msgs - left_skel;
        view::clear_cache();
        let (wrong, errors, first) = count_wrong(&t, &m).await;
        eprintln!("review erasure (scrub opens sealed runs for derived rows: {derived_opts}): pass {pass_s}; rows of the erased thread left in the newest version: {left_msgs} message rows, {left_skel} skeletons, {left_plain} other; reads of the other threads: wrong {wrong}, errors {errors} {first}");
        res.push((derived_opts, pass.is_ok(), left.len(), wrong + errors));
        t.abandon().await;
        teardown();
    }
    for (o, ok, left, bad) in res {
        assert!(ok && left == 0 && bad == 0, "derived opts {o}: pass ok {ok}, rows left {left}, bad reads {bad}");
    }
}

/// Fix round, FIX 2: step 7b's erasure of one collection of a derived thread. Erasing only `ck` (the checkpoints) drops
/// its documents and the messages only they used; the messages `cw` (the pending writes) still uses stay, and every `cw`
/// document and every other thread reads back exactly. Then erasing `cw` too leaves 0 rows under the thread's prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix2_erasure_of_one_collection_keeps_the_messages_still_used() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let k = knobs(BlockCodec::Zstd);
    let id = "fx-fix2-erase-one";
    let (t0, c, mut m) = derived_db(&store, id, 8, 12, &k).await;
    c.stop().await;
    t0.abandon().await;
    let mut t = lc(&store).wake(id).await.unwrap();
    t.upgrade().await.unwrap();
    t.fx_refresh().await.unwrap();
    let name = "th001";
    let mut comp = vec![name.len() as u8];
    comp.extend(name.as_bytes());
    let (tlo, thi) = trange(name);
    let mut left_after = Vec::new();
    for coll in ["ck", "cw"] {
        let mut lo = b"u/".to_vec();
        lo.extend(&comp);
        lo.extend(coll.as_bytes());
        lo.push(b'/');
        let hi = runfmt::prefix_successor(&lo).unwrap();
        let gone: Vec<Vec<u8>> = m.range(lo.clone()..hi.clone()).map(|(k, _)| k.clone()).collect();
        assert!(!gone.is_empty());
        t.apply(gone.iter().map(|k| Op::Delete(k.clone())).collect()).await.unwrap();
        for key in &gone {
            m.remove(key);
        }
        let mut tg = erase::Targets::default();
        tg.ranges.push((lo, hi));
        for key in &gone {
            tg.add_key(key.clone());
        }
        let e = t.fx_erasure_register(coll, &comp, &tg, None).await.unwrap().expect("recorded");
        let all = t.fx_erase_pass(&erase::ScrubOpts::default()).await.unwrap();
        let state = all.iter().find(|x| x.id == e.id).map(|x| x.state.clone());
        t.fx_refresh().await.unwrap();
        let v = t.fx_view().unwrap().meta.clone();
        let rows = all_run_rows(&store, &v).await;
        let left: Vec<&compactor::Row> = rows.iter().filter(|r| r.key >= tlo && r.key < thi).collect();
        let msgs = left.iter().filter(|r| r.env[0] == runfmt::PART).count();
        let skel = left.iter().filter(|r| r.env[0] == runfmt::DERIVED).count();
        let of_coll = left.iter().filter(|r| r.key.starts_with(&[&tlo[..], coll.as_bytes()].concat())).count();
        view::clear_cache();
        let (wrong, errors, first) = count_wrong(&t, &m).await;
        let scan_ok = as_vec(t.scan_range(b"u/", Some(b"u0"), false, None, 1 << 20).await.unwrap()) == model_range(&m, b"u/", Some(b"u0"), false, None);
        eprintln!("fix2 erasure of {coll}: state {state:?}; rows of the thread left: {} ({msgs} message rows, {skel} skeletons), of the erased collection {of_coll}; reads wrong {wrong}, errors {errors} {first}; full scan equal {scan_ok}", left.len());
        assert_eq!(state.as_deref(), Some("scrubbed"));
        assert_eq!(of_coll, 0, "rows of the erased collection left");
        assert_eq!((wrong, errors, scan_ok), (0, 0, true));
        left_after.push((left.len(), msgs));
    }
    assert!(left_after[0].1 > 0, "the messages the writes still use stay after erasing the checkpoints only");
    assert_eq!(left_after[1].0, 0, "nothing of the thread is left once both collections are erased");
    t.abandon().await;
    teardown();
}

/// Fix round, the adapter rerun's finding: after a restart the server learns which collections are keyed by thread_id
/// only when they are written, so a reseal before that saw a spec that knew no checkpoint collection and rewrote every
/// derived thread plain (exact, but the saving lost). A thread that is derived stays derived through a reseal whose
/// spec does not know its collection; `FX_FIX_CONTROL=4` (the code before) loses every derived row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix_derived_threads_stay_derived_when_the_spec_does_not_know_them() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut k = knobs(BlockCodec::Zstd);
    let (mut t, c, m) = derived_db(&store, "fx-fix-unknown-spec", 8, 12, &k).await;
    let v0 = c.current().await;
    let before = all_run_rows(&store, &v0).await.iter().filter(|r| r.env[0] == runfmt::DERIVED).count();
    let mut young = 0;
    for d in &v0.delta {
        young += compactor::run_rows(&store, d).await.unwrap().iter().filter(|r| r.key.starts_with(b"u/") && r.seq > v0.groups.iter().map(|g| g.x_g).min().unwrap_or(0)).count();
    }
    let ckdocs = all_run_rows(&store, &v0).await.iter().filter(|r| r.key.starts_with(b"u/") && r.env[0] == runfmt::LIVE).count();
    eprintln!("fix unknown spec: before the restart: {young} customer rows in delta runs above the lowest X_g, {ckdocs} plain customer rows in runs");
    c.stop().await;
    // a compactor whose spec registers no collection (the server right after a restart)
    if std::env::var("FX_DBG_SAME_SPEC").is_err() {
        k.derive = Some(SpecRef(Arc::new(LenSpec { colls: vec![] })));
    }
    let c2 = Compactor::start(&mut t, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    for _ in 0..2 {
        reseal_all(&c2).await;
    }
    t.fx_sync();
    let after = all_run_rows(&store, &c2.current().await).await.iter().filter(|r| r.env[0] == runfmt::DERIVED).count();
    view::clear_cache();
    let (wrong, errors, first) = count_wrong(&t, &m).await;
    eprintln!("fix unknown spec: derived rows before {before}, after reseals by a compactor that knows no collection {after}; reseals {}, re-encoded {}; wrong {wrong}, errors {errors} {first}", c2.sh.stats.reseals.load(Relaxed), c2.sh.stats.derive_encoded.load(Relaxed));
    assert_eq!((wrong, errors), (0, 0));
    assert!(c2.sh.stats.reseals.load(Relaxed) > 0);
    assert_eq!(after, before, "derived threads were rewritten plain");
    c2.stop().await;
    t.abandon().await;
    teardown();
}
