//! Synthetic documents: a port of the M29/M31 generator (`proto/lavaggio-per-cliente/m29/src/documenti.rs`) that
//! writes real BSON instead of truncated JSON text. Same vocabulary (4,000 words built from syllables, zipf frequencies
//! with exponent 1.07, seed 2026), same fields and value ranges for chat messages (collection 0) and orders
//! (collection 2). Two changes, both towards real MongoDB data: `_id` is a real ObjectId (4-byte time, 5 bytes fixed
//! per client process, 3-byte counter, as the drivers make it) instead of 24 random hex characters, and dates are
//! BSON dates instead of ISO strings. Deterministic given the seeds.

use bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Document};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

pub struct Vocab {
    words: Vec<String>,
    cdf: Vec<f64>,
    cities: Vec<String>,
}

impl Vocab {
    /// `seed` 2026 is the M29 vocabulary. Another seed gives a different language with the same statistics.
    pub fn new(seed: u64) -> Vocab {
        let mut r = SmallRng::seed_from_u64(seed);
        let syl = ["ka", "lo", "mi", "ne", "ra", "to", "su", "ve", "di", "an", "er", "is", "on", "ul", "pre", "con", "tra", "ste", "gri", "bla", "the", "ing", "ment", "ar", "or", "el", "us", "io", "qua", "fo"];
        let mut words = Vec::new();
        let mut seen = std::collections::HashSet::new();
        while words.len() < 4000 {
            let n = r.gen_range(1..=4);
            let mut w = String::new();
            for _ in 0..n {
                w.push_str(syl[r.gen_range(0..syl.len())]);
            }
            if seen.insert(w.clone()) {
                words.push(w);
            }
        }
        let mut cdf = Vec::new();
        let mut acc = 0.0;
        for i in 0..words.len() {
            acc += 1.0 / ((i + 1) as f64).powf(1.07);
            cdf.push(acc);
        }
        let cities = (0..60).map(|i| format!("{}{}", words[i * 7 + 3], ["polis", "burg", "ville", "ano", "ia"][i % 5])).collect();
        Vocab { words, cdf, cities }
    }

    fn word(&self, r: &mut SmallRng) -> &str {
        let x = r.gen::<f64>() * self.cdf.last().unwrap();
        &self.words[self.cdf.partition_point(|c| *c < x).min(self.words.len() - 1)]
    }

    fn sentence(&self, s: &mut String, r: &mut SmallRng, n: usize) {
        for i in 0..n {
            if i > 0 {
                s.push(if r.gen_ratio(1, 12) { ',' } else { ' ' });
                if s.ends_with(',') {
                    s.push(' ');
                }
            }
            s.push_str(self.word(r));
        }
        s.push(if r.gen_ratio(1, 6) { '?' } else { '.' });
    }
}

/// Generator settings. `day1()` is the training and measuring population; the drift variants change one thing.
#[derive(Clone)]
pub struct Gen {
    pub vocab_seed: u64,
    pub cust_base: u32,
    pub customers: u32,
    pub t0_ms: i64,
    pub added_field: bool,
    pub renamed_field: bool,
}

impl Gen {
    pub fn day1() -> Gen {
        // 512 customers as the M29 1 GB run; January 2026
        Gen { vocab_seed: 2026, cust_base: 0, customers: 512, t0_ms: 1_767_225_600_000, added_field: false, renamed_field: false }
    }
}

/// ObjectId as the drivers make it: 4-byte seconds, 5 bytes random per process (here 8 app processes), 3-byte counter.
pub struct Oids {
    procs: Vec<[u8; 5]>,
    counter: u32,
}

impl Oids {
    pub fn new(r: &mut SmallRng) -> Oids {
        Oids { procs: (0..8).map(|_| r.gen()).collect(), counter: r.gen::<u32>() & 0xff_ffff }
    }
    pub fn next(&mut self, secs: u32, r: &mut SmallRng) -> ObjectId {
        let mut b = [0u8; 12];
        b[0..4].copy_from_slice(&secs.to_be_bytes());
        b[4..9].copy_from_slice(&self.procs[r.gen_range(0..self.procs.len())]);
        self.counter = (self.counter + 1) & 0xff_ffff;
        b[9..12].copy_from_slice(&self.counter.to_be_bytes()[1..4]);
        ObjectId::from_bytes(b)
    }
}

pub struct Doc {
    pub key: Vec<u8>,
    pub bson: Vec<u8>,
}

fn key(cust: u32, a: u32, ts: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(16);
    k.extend(cust.to_be_bytes());
    k.extend(a.to_be_bytes());
    k.extend(ts.to_be_bytes());
    k
}

/// Chat message (M29 collection 0). Content length uniform 60..250 characters, which puts the mean BSON size near the
/// 341 bytes of the cost model.
pub fn chat(g: &Gen, v: &Vocab, oids: &mut Oids, r: &mut SmallRng) -> Doc {
    let cust = g.cust_base + r.gen_range(0..g.customers);
    let conv = r.gen_range(0..40u32);
    let ts = g.t0_ms + r.gen_range(0..30 * 86_400_000i64);
    let roles = ["user", "assistant", "assistant", "tool", "system"];
    let models = ["gpt-5-mini", "claude-sonnet", "llama-4-70b"];
    let len = r.gen_range(60..250);
    let mut content = String::with_capacity(len + 32);
    while content.len() < len {
        let n = r.gen_range(4..18);
        v.sentence(&mut content, r, n);
        content.push(' ');
    }
    content.truncate(content.trim_end().len());
    let mut d = doc! {
        "_id": oids.next((ts / 1000) as u32, r),
        "userId": format!("u_{cust:08x}"),
        "conversationId": format!("c_{cust:08x}_{conv:03}"),
        "role": roles[r.gen_range(0..roles.len())],
        "model": models[r.gen_range(0..models.len())],
        "createdAt": DateTime::from_millis(ts),
        "tokens": { "in": r.gen_range(10..4000), "out": r.gen_range(5..900) },
    };
    if g.added_field {
        d.insert("lang", ["en", "it", "de", "fr", "es"][r.gen_range(0..5)]);
    }
    d.insert(if g.renamed_field { "text" } else { "content" }, content);
    Doc { key: key(cust, conv, ts), bson: bson::to_vec(&d).unwrap() }
}

/// Order (M29 collection 2): nested shipping address, 1 to 3 item lines with prices, total.
pub fn order(g: &Gen, v: &Vocab, oids: &mut Oids, r: &mut SmallRng) -> Doc {
    let cust = g.cust_base + r.gen_range(0..g.customers);
    let ts = g.t0_ms + r.gen_range(0..30 * 86_400_000i64);
    let states = ["created", "paid", "shipped", "delivered", "returned"];
    let mut items = Vec::new();
    let mut tot = 0.0;
    for _ in 0..r.gen_range(1..=3) {
        let p = (r.gen_range(199..9999) as f64) / 100.0;
        let q = r.gen_range(1..4);
        tot += p * q as f64;
        items.push(Bson::Document(doc! {
            "sku": format!("SKU-{:05}", r.gen_range(0..20000)),
            "name": format!("{} {}", v.word(r), v.word(r)),
            "qty": q,
            "price": p,
        }));
    }
    let shipping = doc! {
        "name": format!("{} {}", v.word(r), v.word(r)),
        "street": format!("Via {} {}", v.word(r), r.gen_range(1..200)),
        "city": v.cities[r.gen_range(0..v.cities.len())].clone(),
        "zip": format!("{:05}", r.gen_range(10000..99999)),
        "country": "IT",
    };
    let mut d = doc! {
        "_id": oids.next((ts / 1000) as u32, r),
        "userId": format!("u_{cust:08x}"),
        "status": states[r.gen_range(0..5)],
        "createdAt": DateTime::from_millis(ts),
        "currency": "EUR",
    };
    d.insert(if g.renamed_field { "shippingAddress" } else { "shipping" }, shipping);
    d.insert("items", items);
    d.insert("total", (tot * 100.0).round() / 100.0);
    if g.added_field {
        d.insert("channel", ["web", "ios", "android", "pos"][r.gen_range(0..4)]);
    }
    Doc { key: key(cust, 0, ts), bson: bson::to_vec(&d).unwrap() }
}

/// A random 128-bit id (UUID v4 as BSON binary subtype 4).
pub fn uuid(r: &mut SmallRng) -> Bson {
    let mut b: [u8; 16] = r.gen();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: b.to_vec() })
}

#[allow(dead_code)]
pub fn insert_front(d: Document, k: &str, v: Bson) -> Document {
    let mut out = Document::new();
    out.insert(k, v);
    for (a, b) in d {
        out.insert(a, b);
    }
    out
}
