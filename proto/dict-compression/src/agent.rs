//! Agent memory from real agent trajectories: the tau2-bench public leaderboard runs that M25 already pulled into the
//! repo (branch `agenti-bancone`, `proto/agenti-bancone/tracce/tau2-classifica`, 27 files, 10 models, 4 domains:
//! airline, retail, telecom, banking_knowledge). One message of a conversation becomes one document, as an agent app
//! stores its memory: who, which session, role, turn, time, text, tool calls with their JSON arguments, tool results.
//!
//! "Day 1" is the February and March 2026 runs (6 models). "Later" is the August 2026 runs (4 newer models, banking
//! only): new models, new phrasing, same app.

use crate::synth::{uuid, Doc, Oids};
use bson::{doc, spec::BinarySubtype, Binary, Bson, DateTime, Document};
use flate2::read::GzDecoder;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, StandardNormal};
use std::io::{BufRead, BufReader};

pub struct Msg {
    pub key: Vec<u8>,
    pub doc: Document,
}

fn fnv(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

fn parse_ts(v: &serde_json::Value) -> Option<i64> {
    let s = v.as_str()?;
    let t = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    Some(t.and_utc().timestamp_millis())
}

/// Load every message of every file whose name passes `keep`. Returns documents in file order.
pub fn load(dir: &str, keep: impl Fn(&str) -> bool) -> Vec<Msg> {
    let mut files: Vec<_> = std::fs::read_dir(dir).expect("traces dir: run ./fetch_traces.sh").filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.to_string_lossy().ends_with(".jsonl.gz")).collect();
    files.sort();
    let mut out = Vec::new();
    let mut r = SmallRng::seed_from_u64(77);
    let mut oids = Oids::new(&mut r);
    for f in files {
        let name = f.file_name().unwrap().to_string_lossy().to_string();
        if !keep(&name) {
            continue;
        }
        // <model>_sierra_<date>__<domain>.jsonl.gz
        let model = name.split("_sierra_").next().unwrap().to_string();
        let domain = name.split("__").nth(1).unwrap().trim_end_matches(".jsonl.gz").to_string();
        let rd = BufReader::new(GzDecoder::new(std::fs::File::open(&f).unwrap()));
        for line in rd.lines() {
            let line = line.unwrap();
            let c: serde_json::Value = serde_json::from_str(&line).unwrap();
            let sid = c["id"].as_str().unwrap_or("").to_string();
            let task = c["task_id"].to_string();
            let user = fnv(&format!("{domain}/{task}"));
            let start = parse_ts(&c["start"]).unwrap_or(1_771_891_200_000);
            for (turn, m) in c["m"].as_array().unwrap().iter().enumerate() {
                let m = m.as_array().unwrap();
                let ts = parse_ts(&m[1]).unwrap_or(start + turn as i64 * 1000);
                let mut d = doc! {
                    "_id": oids.next((ts / 1000) as u32, &mut r),
                    "userId": format!("u_{user:08x}"),
                    "sessionId": sid.clone(),
                    "model": model.clone(),
                    "domain": domain.clone(),
                    "turn": turn as i32,
                    "role": m[0].as_str().unwrap_or("?"),
                    "createdAt": DateTime::from_millis(ts),
                };
                if let Some(s) = m.get(6).and_then(|x| x.as_str()) {
                    d.insert("content", s);
                }
                if let Some(calls) = m.get(2).and_then(|x| x.as_array()) {
                    let calls: Vec<Bson> = calls
                        .iter()
                        .map(|tc| {
                            let tc = tc.as_array().unwrap();
                            let args = bson::to_bson(&tc[2]).unwrap_or(Bson::Null);
                            Bson::Document(doc! { "id": tc[0].as_str().unwrap_or(""), "name": tc[1].as_str().unwrap_or(""), "arguments": args })
                        })
                        .collect();
                    d.insert("toolCalls", calls);
                }
                if let Some(id) = m.get(3).and_then(|x| x.as_str()) {
                    d.insert("toolCallId", id);
                    d.insert("isError", m.get(4).and_then(|x| x.as_bool()).unwrap_or(false));
                }
                let mut key = Vec::new();
                key.extend(user.to_be_bytes());
                key.extend(fnv(&sid).to_be_bytes());
                key.extend((turn as u32).to_be_bytes());
                out.push(Msg { key, doc: d });
            }
        }
    }
    out
}

/// Schema drift for agent memory: one added field (`latencyMs` after `createdAt`), one renamed field
/// (`sessionId` becomes `threadId`), in every document.
pub fn drift_schema(d: &Document, r: &mut SmallRng) -> Document {
    let mut out = Document::new();
    for (k, v) in d {
        if k == "sessionId" {
            out.insert("threadId", v.clone());
        } else {
            out.insert(k.clone(), v.clone());
        }
        if k == "createdAt" {
            out.insert("latencyMs", r.gen_range(80..9000) as i32);
        }
    }
    out
}

pub enum Emb {
    /// 768 float32 as BSON binary (3,072 bytes)
    Binary,
    /// 768 doubles as a BSON array; values printed with 9 significant digits, as embedding APIs return them in JSON,
    /// then parsed, so the 52-bit mantissa is full
    Array,
}

/// The hard case: the same memory document with random 128-bit ids (`_id` and `memoryId` as UUID v4) and a
/// 768-dimension embedding (normal values, unit length, like a real text embedding).
pub fn with_embedding(d: &Document, how: &Emb, r: &mut SmallRng) -> Document {
    let mut out = Document::new();
    out.insert("_id", uuid(r));
    out.insert("memoryId", uuid(r));
    for (k, v) in d {
        if k != "_id" {
            out.insert(k.clone(), v.clone());
        }
    }
    let mut e: Vec<f32> = (0..768).map(|_| StandardNormal.sample(r)).collect();
    let n = e.iter().map(|x| x * x).sum::<f32>().sqrt();
    for x in e.iter_mut() {
        *x /= n;
    }
    match how {
        Emb::Binary => {
            let mut b = Vec::with_capacity(3072);
            for x in &e {
                b.extend(x.to_le_bytes());
            }
            out.insert("embedding", Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: b }));
        }
        Emb::Array => {
            let a: Vec<Bson> = e.iter().map(|x| Bson::Double(format!("{:.8e}", x).parse::<f64>().unwrap())).collect();
            out.insert("embedding", a);
        }
    }
    out
}

pub fn to_doc(key: &[u8], d: &Document) -> Doc {
    Doc { key: key.to_vec(), bson: bson::to_vec(d).unwrap() }
}
