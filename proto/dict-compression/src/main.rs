//! Local test of Fable's idea 3: zstd with a dictionary trained per kind of document, on single log records,
//! batches of 16 records and 64 KB blocks, against zstd without a dictionary and lz4 (today's boxes).
//! Contract and method: fenvix-brain `prompts/P-dictionary-compression-local.md`. Spend $0, local only.
//!
//! Run: ./fetch_traces.sh && cargo run --release -- [--kinds chat,agent,orders,agent_emb_bin,agent_emb_arr] [--reps 3]
//! Output: results/raw.jsonl (one line per measure) and a readable summary on stdout.

mod agent;
mod synth;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde_json::json;
use std::io::Write;
use std::time::Instant;
use synth::{Doc, Gen, Oids, Vocab};
use zstd::bulk::{Compressor, Decompressor};
use zstd::dict::{DecoderDictionary, EncoderDictionary};
use zstd::zstd_safe::{CParameter, DParameter, FrameFormat};

const TRAIN: usize = 5_000;
const MEASURE: usize = 50_000;
const DRIFT: usize = 10_000;
const DICT_SIZES: [usize; 3] = [32 * 1024, 64 * 1024, 112 * 1024];
const BLOCK: usize = 64 * 1024;

struct Kind {
    name: &'static str,
    train: Vec<Doc>,
    measure: Vec<Doc>,
    drifts: Vec<(&'static str, Vec<Doc>)>,
}

// ------------------------------------------------------------------------------------------------ building the data

fn synth_kind(name: &'static str, f: fn(&Gen, &Vocab, &mut Oids, &mut SmallRng) -> Doc) -> Kind {
    let day1 = Gen::day1();
    let v = Vocab::new(day1.vocab_seed);
    let gen = |g: &Gen, v: &Vocab, n: usize, seed: u64, fleet: u64| {
        let mut r = SmallRng::seed_from_u64(seed);
        let mut fr = SmallRng::seed_from_u64(fleet);
        let mut o = Oids::new(&mut fr);
        (0..n).map(|_| f(g, v, &mut o, &mut r)).collect::<Vec<_>>()
    };
    // training and measuring: the same app fleet on day 1, two different random samples
    let train = gen(&day1, &v, TRAIN, 11, 5);
    let measure = gen(&day1, &v, MEASURE, 12, 5);
    // drift 1: same app 90 days later, new customers, restarted app fleet, new random seed
    let later = Gen { cust_base: 100_000, t0_ms: day1.t0_ms + 90 * 86_400_000, ..day1.clone() };
    let d_seed = gen(&later, &v, DRIFT, 13, 6);
    // drift 2: as drift 1 and the users write with another vocabulary (extreme: a different language)
    let v2 = Vocab::new(7);
    let d_vocab = gen(&Gen { vocab_seed: 7, ..later.clone() }, &v2, DRIFT, 14, 6);
    // drift 3: day 1 with one added field and one renamed field
    let d_schema = gen(&Gen { added_field: true, renamed_field: true, ..day1.clone() }, &v, DRIFT, 15, 5);
    Kind { name, train, measure, drifts: vec![("new-seed-90-days-later", d_seed), ("new-vocabulary", d_vocab), ("schema-added-and-renamed", d_schema)] }
}

struct AgentPools {
    day1: Vec<agent::Msg>,
    later: Vec<agent::Msg>,
    order: Vec<usize>,
    later_order: Vec<usize>,
}

fn agent_pools() -> AgentPools {
    let t = Instant::now();
    let day1 = agent::load("traces", |n| n.contains("_2026-02-") || n.contains("_2026-03-"));
    let later = agent::load("traces", |n| n.contains("_2026-08-"));
    let mut r = SmallRng::seed_from_u64(1);
    let mut order: Vec<usize> = (0..day1.len()).collect();
    order.shuffle(&mut r);
    let mut later_order: Vec<usize> = (0..later.len()).collect();
    later_order.shuffle(&mut r);
    eprintln!("agent traces: {} day-1 messages, {} later messages, loaded in {:.1} s", day1.len(), later.len(), t.elapsed().as_secs_f64());
    AgentPools { day1, later, order, later_order }
}

fn agent_kind(name: &'static str, p: &AgentPools, emb: Option<agent::Emb>) -> Kind {
    let mut r = SmallRng::seed_from_u64(match emb {
        None => 21,
        Some(agent::Emb::Binary) => 22,
        Some(agent::Emb::Array) => 23,
    });
    let mk = |m: &agent::Msg, schema: bool, r: &mut SmallRng| {
        let d = if schema { agent::drift_schema(&m.doc, r) } else { m.doc.clone() };
        let d = match &emb {
            None => d,
            Some(e) => agent::with_embedding(&d, e, r),
        };
        agent::to_doc(&m.key, &d)
    };
    let train = p.order[..TRAIN].iter().map(|&i| mk(&p.day1[i], false, &mut r)).collect();
    let measure = p.order[TRAIN..TRAIN + MEASURE].iter().map(|&i| mk(&p.day1[i], false, &mut r)).collect();
    let d_later = p.later_order[..DRIFT].iter().map(|&i| mk(&p.later[i], false, &mut r)).collect();
    let d_schema = p.order[TRAIN + MEASURE..TRAIN + MEASURE + DRIFT].iter().map(|&i| mk(&p.day1[i], true, &mut r)).collect();
    Kind { name, train, measure, drifts: vec![("later-runs-newer-models-2026-08", d_later), ("schema-added-and-renamed", d_schema)] }
}

// ------------------------------------------------------------------------------------------------ units

fn singles(docs: &[Doc]) -> Vec<Vec<u8>> {
    docs.iter().map(|d| d.bson.clone()).collect()
}

/// 16 consecutive records of the (random) measuring order, as one log message: records of many customers interleaved.
fn batches(docs: &[Doc], n: usize) -> Vec<Vec<u8>> {
    docs.chunks(n).map(|c| c.iter().flat_map(|d| d.bson.iter().copied()).collect()).collect()
}

/// Records sorted by key (customer, conversation or session, time), packed into blocks of at most 64 KB, as in the
/// boxes laid out per customer. Only the document bytes are counted, not the keys.
fn blocks(docs: &[Doc]) -> Vec<Vec<u8>> {
    let mut idx: Vec<usize> = (0..docs.len()).collect();
    idx.sort_by(|&a, &b| docs[a].key.cmp(&docs[b].key));
    let mut out = Vec::new();
    let mut cur: Vec<u8> = Vec::with_capacity(BLOCK);
    for i in idx {
        let b = &docs[i].bson;
        if !cur.is_empty() && cur.len() + b.len() > BLOCK {
            out.push(std::mem::take(&mut cur));
        }
        cur.extend_from_slice(b);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

// ------------------------------------------------------------------------------------------------ codecs

#[derive(Clone, Copy, PartialEq)]
enum Frame {
    /// standard zstd frame: magic number, dictionary id when a dictionary is used, content size, no checksum
    Standard,
    /// magicless frame, no dictionary id (the dictionary version is named once in the segment or box header)
    Lean,
}

struct Res {
    raw: usize,
    comp: usize,
    comp_ns: f64,
    dec_ns: f64,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn zstd_run(units: &[Vec<u8>], level: i32, dict: Option<&[u8]>, frame: Frame, reps: usize) -> Res {
    let edict = dict.map(|d| EncoderDictionary::copy(d, level));
    let ddict = dict.map(DecoderDictionary::copy);
    let mut c = match &edict {
        Some(e) => Compressor::with_prepared_dictionary(e).unwrap(),
        None => Compressor::new(level).unwrap(),
    };
    let mut d = match &ddict {
        Some(e) => Decompressor::with_prepared_dictionary(e).unwrap(),
        None => Decompressor::new().unwrap(),
    };
    c.set_parameter(CParameter::ChecksumFlag(false)).unwrap();
    c.set_parameter(CParameter::ContentSizeFlag(true)).unwrap();
    if frame == Frame::Lean {
        c.set_parameter(CParameter::Format(FrameFormat::Magicless)).unwrap();
        c.set_parameter(CParameter::DictIdFlag(false)).unwrap();
        d.set_parameter(DParameter::Format(FrameFormat::Magicless)).unwrap();
    }
    let raw: usize = units.iter().map(|u| u.len()).sum();
    let bound: usize = units.iter().map(|u| zstd::zstd_safe::compress_bound(u.len())).sum();
    let maxu = units.iter().map(|u| u.len()).max().unwrap_or(0);
    let mut out = vec![0u8; bound];
    let mut offs = Vec::with_capacity(units.len() + 1);
    let mut back = vec![0u8; maxu];
    let (mut cts, mut dts) = (Vec::new(), Vec::new());
    for rep in 0..reps.max(1) {
        offs.clear();
        offs.push(0usize);
        let mut pos = 0;
        let t = Instant::now();
        for u in units {
            pos += c.compress_to_buffer(u, &mut out[pos..]).unwrap();
            offs.push(pos);
        }
        cts.push(t.elapsed().as_nanos() as f64);
        let t = Instant::now();
        for i in 0..units.len() {
            let n = d.decompress_to_buffer(&out[offs[i]..offs[i + 1]], &mut back[..]).unwrap();
            std::hint::black_box(n);
        }
        dts.push(t.elapsed().as_nanos() as f64);
        if rep == 0 {
            for (i, u) in units.iter().enumerate() {
                let n = d.decompress_to_buffer(&out[offs[i]..offs[i + 1]], &mut back[..]).unwrap();
                assert!(n == u.len() && &back[..n] == &u[..], "round trip failed");
            }
        }
    }
    Res { raw, comp: *offs.last().unwrap(), comp_ns: median(cts), dec_ns: median(dts) }
}

fn lz4_run(units: &[Vec<u8>], reps: usize) -> Res {
    let raw: usize = units.iter().map(|u| u.len()).sum();
    let bound: usize = units.iter().map(|u| lz4_flex::block::get_maximum_output_size(u.len())).sum();
    let maxu = units.iter().map(|u| u.len()).max().unwrap_or(0);
    let mut out = vec![0u8; bound];
    let mut offs = Vec::new();
    let mut back = vec![0u8; maxu];
    let (mut cts, mut dts) = (Vec::new(), Vec::new());
    for rep in 0..reps.max(1) {
        offs.clear();
        offs.push(0usize);
        let mut pos = 0;
        let t = Instant::now();
        for u in units {
            pos += lz4_flex::block::compress_into(u, &mut out[pos..]).unwrap();
            offs.push(pos);
        }
        cts.push(t.elapsed().as_nanos() as f64);
        let t = Instant::now();
        for (i, u) in units.iter().enumerate() {
            let n = lz4_flex::block::decompress_into(&out[offs[i]..offs[i + 1]], &mut back[..u.len()]).unwrap();
            std::hint::black_box(n);
        }
        dts.push(t.elapsed().as_nanos() as f64);
        if rep == 0 {
            for (i, u) in units.iter().enumerate() {
                let n = lz4_flex::block::decompress_into(&out[offs[i]..offs[i + 1]], &mut back[..u.len()]).unwrap();
                assert!(n == u.len() && &back[..n] == &u[..]);
            }
        }
    }
    // 4 bytes per block for the uncompressed length, as compress_prepend_size would store
    Res { raw, comp: *offs.last().unwrap() + 4 * units.len(), comp_ns: median(cts), dec_ns: median(dts) }
}

// ------------------------------------------------------------------------------------------------ main

struct Out {
    f: std::fs::File,
}

impl Out {
    fn line(&mut self, v: serde_json::Value) {
        writeln!(self.f, "{}", v).unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
fn record(o: &mut Out, kind: &str, set: &str, mode: &str, codec: &str, dict: usize, frame: &str, nrec: usize, r: &Res) -> f64 {
    let ratio = r.comp as f64 / r.raw as f64;
    let cus = r.comp_ns / nrec as f64 / 1000.0;
    let dus = r.dec_ns / nrec as f64 / 1000.0;
    o.line(json!({
        "kind": kind, "set": set, "mode": mode, "codec": codec, "dict_bytes": dict, "frame": frame,
        "records": nrec, "raw_bytes": r.raw, "comp_bytes": r.comp, "ratio": ratio,
        "comp_us_per_record": cus, "dec_us_per_record": dus,
        "comp_mb_s": r.raw as f64 / r.comp_ns * 1000.0, "dec_mb_s": r.raw as f64 / r.dec_ns * 1000.0,
    }));
    println!("  {kind:14} {set:32} {mode:8} {codec:6} dict {:>6} {frame:8} ratio {ratio:.3}  comp {cus:7.2} us/rec  dec {dus:6.2} us/rec", dict);
    ratio
}

fn size_stats(o: &mut Out, k: &Kind) {
    let mut s: Vec<usize> = k.measure.iter().map(|d| d.bson.len()).collect();
    s.sort();
    let mean = s.iter().sum::<usize>() as f64 / s.len() as f64;
    let p = |q: f64| s[((s.len() as f64 - 1.0) * q) as usize];
    let tr: usize = k.train.iter().map(|d| d.bson.len()).sum();
    o.line(json!({"kind": k.name, "stat": "sizes", "measure_records": s.len(), "mean_bson_bytes": mean, "p10": p(0.1), "p50": p(0.5), "p90": p(0.9), "p99": p(0.99), "max": s[s.len()-1], "train_records": k.train.len(), "train_bytes": tr}));
    println!("== {}: {} measure records, mean {:.0} B, p10 {} p50 {} p90 {} p99 {} max {}; training {} records, {} bytes", k.name, s.len(), mean, p(0.1), p(0.5), p(0.9), p(0.99), s[s.len() - 1], k.train.len(), tr);
}

fn run_kind(o: &mut Out, k: &Kind, reps: usize) {
    size_stats(o, k);
    let samples: Vec<Vec<u8>> = k.train.iter().map(|d| d.bson.clone()).collect();
    let mut dicts = Vec::new();
    for &sz in &DICT_SIZES {
        let t = Instant::now();
        let d = zstd::dict::from_samples(&samples, sz).unwrap();
        let secs = t.elapsed().as_secs_f64();
        o.line(json!({"kind": k.name, "stat": "dictionary", "max_bytes": sz, "bytes": d.len(), "train_seconds": secs}));
        println!("  dictionary max {} -> {} bytes, trained in {:.2} s", sz, d.len(), secs);
        dicts.push((sz, d));
    }
    let n = k.measure.len();
    let modes: Vec<(&str, Vec<Vec<u8>>)> = vec![("single", singles(&k.measure)), ("batch16", batches(&k.measure, 16)), ("block64k", blocks(&k.measure))];
    for (mode, units) in &modes {
        for level in [1, 3] {
            let codec = format!("zstd{level}");
            let r = zstd_run(units, level, None, Frame::Lean, reps);
            record(o, k.name, "measure", mode, &codec, 0, "lean", n, &r);
            if *mode == "single" || *mode == "batch16" {
                let r = zstd_run(units, level, None, Frame::Standard, 1);
                record(o, k.name, "measure", mode, &codec, 0, "standard", n, &r);
            }
            for (sz, d) in &dicts {
                let r = zstd_run(units, level, Some(d), Frame::Lean, reps);
                record(o, k.name, "measure", mode, &codec, *sz, "lean", n, &r);
                if *mode == "single" || *mode == "batch16" {
                    let r = zstd_run(units, level, Some(d), Frame::Standard, 1);
                    record(o, k.name, "measure", mode, &codec, *sz, "standard", n, &r);
                }
            }
        }
        if *mode == "block64k" {
            let r = lz4_run(units, reps);
            record(o, k.name, "measure", mode, "lz4", 0, "block", n, &r);
        }
    }
    for (name, docs) in &k.drifts {
        let units = singles(docs);
        let mean = units.iter().map(|u| u.len()).sum::<usize>() as f64 / units.len() as f64;
        println!("  drift set {name}: {} records, mean {:.0} B", units.len(), mean);
        for level in [1, 3] {
            let codec = format!("zstd{level}");
            let r = zstd_run(&units, level, None, Frame::Lean, 1);
            record(o, k.name, name, "single", &codec, 0, "lean", units.len(), &r);
            for (sz, d) in &dicts {
                let r = zstd_run(&units, level, Some(d), Frame::Lean, 1);
                record(o, k.name, name, "single", &codec, *sz, "lean", units.len(), &r);
            }
        }
        let b = blocks(docs);
        for (sz, d) in [(0usize, None), (dicts[1].0, Some(&dicts[1].1))] {
            let r = zstd_run(&b, 1, d.map(|x| x.as_slice()), Frame::Lean, 1);
            record(o, k.name, name, "block64k", "zstd1", sz, "lean", units.len(), &r);
        }
    }
}

/// Single records only, zstd 1 and 3, lean frame: no dictionary and the three dictionary sizes. `groups` is a list of
/// (training docs, measured docs); ratios are summed over the groups (one dictionary per group).
fn singles_grouped(o: &mut Out, name: &str, groups: &[(Vec<Doc>, Vec<Doc>)]) {
    for level in [1, 3] {
        let codec = format!("zstd{level}");
        for sz in [0usize, DICT_SIZES[0], DICT_SIZES[1], DICT_SIZES[2]] {
            let mut tot = Res { raw: 0, comp: 0, comp_ns: 0.0, dec_ns: 0.0 };
            let mut n = 0;
            for (train, measure) in groups {
                let units = singles(measure);
                n += units.len();
                let d = if sz > 0 { Some(zstd::dict::from_samples(&singles(train), sz).unwrap()) } else { None };
                let r = zstd_run(&units, level, d.as_deref(), Frame::Lean, 1);
                tot.raw += r.raw;
                tot.comp += r.comp;
                tot.comp_ns += r.comp_ns;
                tot.dec_ns += r.dec_ns;
            }
            record(o, name, "measure", "single", &codec, sz, "lean", n, &tot);
        }
    }
}

/// Two fairness checks on the real agent data. (1) One dictionary per app: the four tau2 domains are four different
/// apps mixed in one collection in kind 2; here each gets its own dictionary (5,000 training and 12,500 measured
/// documents each). (2) A larger training sample: 50,000 documents instead of 5,000 (zstd's guidance is about 100 times
/// the dictionary size), measured on the same 50,000 documents as kind 2.
fn agent_extras(o: &mut Out, p: &AgentPools) {
    let mk = |i: usize| agent::to_doc(&p.day1[i].key, &p.day1[i].doc);
    let mut groups = Vec::new();
    for dom in ["airline", "retail", "telecom", "banking_knowledge"] {
        let idx: Vec<usize> = p.order.iter().copied().filter(|&i| p.day1[i].doc.get_str("domain").unwrap_or("") == dom).collect();
        let train: Vec<Doc> = idx[..TRAIN].iter().map(|&i| mk(i)).collect();
        let measure: Vec<Doc> = idx[TRAIN..TRAIN + 12_500].iter().map(|&i| mk(i)).collect();
        println!("  domain {dom}: {} day-1 messages", idx.len());
        groups.push((train, measure));
    }
    singles_grouped(o, "agent_per_domain", &groups);
    let train: Vec<Doc> = p.order[TRAIN + MEASURE + DRIFT..TRAIN + MEASURE + DRIFT + 50_000].iter().map(|&i| mk(i)).collect();
    let measure: Vec<Doc> = p.order[TRAIN..TRAIN + MEASURE].iter().map(|&i| mk(i)).collect();
    singles_grouped(o, "agent_train50k", &[(train, measure)]);
}

/// Kind 4 only: how much of each record is random (embedding values and 128-bit ids), and how well that part alone
/// compresses.
fn incompressible_share(o: &mut Out, k: &Kind) {
    let mut emb_units = Vec::new();
    let mut id_units = Vec::new();
    let mut emb_raw = 0usize;
    let mut total = 0usize;
    for d in &k.measure {
        total += d.bson.len();
        let doc: bson::Document = bson::from_slice(&d.bson).unwrap();
        let mut ids = Vec::new();
        for f in ["_id", "memoryId"] {
            if let Some(bson::Bson::Binary(b)) = doc.get(f) {
                ids.extend_from_slice(&b.bytes);
            }
        }
        id_units.push(ids);
        let e: Vec<u8> = match doc.get("embedding") {
            Some(bson::Bson::Binary(b)) => b.bytes.clone(),
            Some(bson::Bson::Array(a)) => a.iter().flat_map(|x| x.as_f64().unwrap().to_le_bytes()).collect(),
            _ => vec![],
        };
        emb_raw += e.len();
        emb_units.push(e);
    }
    let re = zstd_run(&emb_units, 1, None, Frame::Lean, 1);
    let ri = zstd_run(&id_units, 1, None, Frame::Lean, 1);
    let ids_raw: usize = id_units.iter().map(|u| u.len()).sum();
    let share = (emb_raw + ids_raw) as f64 / total as f64;
    o.line(json!({"kind": k.name, "stat": "random-part", "record_bytes": total, "embedding_value_bytes": emb_raw, "id_bytes": ids_raw,
        "random_share_of_raw": share, "embedding_values_alone_zstd1_ratio": re.comp as f64 / re.raw as f64,
        "ids_alone_zstd1_ratio": ri.comp as f64 / ri.raw as f64}));
    println!("  random part: {:.1}% of raw bytes are embedding values and 128-bit ids; embedding values alone compress to {:.3} with zstd-1, ids alone {:.3}",
        share * 100.0, re.comp as f64 / re.raw as f64, ri.comp as f64 / ri.raw as f64);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| args.iter().position(|a| a == k).map(|i| args[i + 1].clone()).unwrap_or(d.to_string());
    let kinds = get("--kinds", "chat,agent,orders,agent_emb_bin,agent_emb_arr");
    let reps: usize = get("--reps", "3").parse().unwrap();
    let path = get("--out", "results/raw.jsonl");
    std::fs::create_dir_all("results").unwrap();
    let mut o = Out { f: std::fs::OpenOptions::new().create(true).append(true).open(&path).unwrap() };
    let cpu = std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| s.lines().find(|l| l.starts_with("model name")).map(|l| l.split(':').nth(1).unwrap().trim().to_string())).unwrap_or_default();
    o.line(json!({"stat": "run", "cpu": cpu, "zstd_version": zstd::zstd_safe::version_string(), "reps": reps, "kinds": kinds}));
    println!("CPU {cpu}, zstd {}, single thread, {reps} repetitions (median)", zstd::zstd_safe::version_string());
    let need_agent = kinds.split(',').any(|k| k.starts_with("agent"));
    let pools = if need_agent { Some(agent_pools()) } else { None };
    for k in kinds.split(',') {
        let t = Instant::now();
        if k == "agent_extras" {
            agent_extras(&mut o, pools.as_ref().unwrap());
            println!("  ({} done in {:.0} s)", k, t.elapsed().as_secs_f64());
            continue;
        }
        let kind = match k {
            "chat" => synth_kind("chat", synth::chat),
            "orders" => synth_kind("orders", synth::order),
            "agent" => agent_kind("agent", pools.as_ref().unwrap(), None),
            "agent_emb_bin" => agent_kind("agent_emb_bin", pools.as_ref().unwrap(), Some(agent::Emb::Binary)),
            "agent_emb_arr" => agent_kind("agent_emb_arr", pools.as_ref().unwrap(), Some(agent::Emb::Array)),
            x => panic!("unknown kind {x}"),
        };
        run_kind(&mut o, &kind, reps);
        if k.starts_with("agent_emb") {
            incompressible_share(&mut o, &kind);
        }
        println!("  ({} done in {:.0} s)", k, t.elapsed().as_secs_f64());
    }
}
