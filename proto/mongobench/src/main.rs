//! M16 · carico B1 su mongod, riferimento per il capo del quaderno.
//!
//! Un database, una collezione con `idx` indici secondari (2, 4 o 7: record del quaderno da 3, 5, 8 chiavi).
//! Documenti da circa 1 KB, della stessa forma di `json_doc` del prototipo (id, utente, date, stato, cliente,
//! pagamento, righe d'ordine). Miscela B1: 70% `$set` su 1–2 campi (78% due campi, media circa 1,78), circa metà
//! degli aggiornamenti cambia un campo indicizzato; 25% inserimenti; 5% cancellazioni. `--mix insert`: solo inserimenti.
//!
//! Ciclo chiuso: `clients` connessioni, ognuna manda un `bulkWrite` (comando di MongoDB 8.0, un solo viaggio) da
//! `batch` operazioni non ordinate e aspetta la conferma con il write concern scelto. Stampa una riga JSON:
//! operazioni al secondo nella finestra di misura, p50/p99/max della conferma del lotto.
//!
//! Uso: mongobench --uri URI --idx 7 --mix b1 --clients 16 --batch 32 --secs 30 --warm 10 --preload 200000 --w majority --j 1

use futures::future::join_all;
use hdrhistogram::Histogram;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{Acknowledgment, ClientOptions, WriteConcern};
use mongodb::{Client, Namespace};
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const WORDS: [&str; 64] = [
    "rosso", "verde", "blu", "nuovo", "vecchio", "grande", "piccolo", "pagato", "spedito", "consegnato", "annullato", "reso",
    "milano", "roma", "torino", "napoli", "firenze", "bologna", "genova", "bari", "palermo", "venezia", "verona", "padova",
    "mario", "luca", "giulia", "anna", "marco", "sara", "paolo", "elena", "carta", "bonifico", "paypal", "contanti",
    "libro", "penna", "tazza", "lampada", "sedia", "tavolo", "borsa", "scarpa", "maglia", "giacca", "cappello", "orologio",
    "oro", "argento", "nero", "bianco", "giallo", "app", "web", "ios", "android", "admin", "utente", "ospite", "attivo",
    "sospeso", "chiuso", "aperto",
];

struct Args {
    uri: String,
    idx: usize,
    mix: String,
    clients: usize,
    batch: usize,
    secs: u64,
    warm: u64,
    preload: i64,
    w: String,
    j: bool,
    setup: bool,
}

fn args() -> Args {
    let mut a = Args { uri: "mongodb://127.0.0.1:27017".into(), idx: 7, mix: "b1".into(), clients: 16, batch: 32, secs: 30, warm: 10, preload: 200_000, w: "majority".into(), j: true, setup: true };
    let v: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i + 1 < v.len() {
        let x = v[i + 1].clone();
        match v[i].as_str() {
            "--uri" => a.uri = x,
            "--idx" => a.idx = x.parse().unwrap(),
            "--mix" => a.mix = x,
            "--clients" => a.clients = x.parse().unwrap(),
            "--batch" => a.batch = x.parse().unwrap(),
            "--secs" => a.secs = x.parse().unwrap(),
            "--warm" => a.warm = x.parse().unwrap(),
            "--preload" => a.preload = x.parse().unwrap(),
            "--w" => a.w = x,
            "--j" => a.j = x == "1",
            "--setup" => a.setup = x == "1",
            o => panic!("argomento sconosciuto {o}"),
        }
        i += 2;
    }
    a
}

fn w<R: Rng>(g: &mut R) -> &'static str {
    WORDS[g.gen_range(0..WORDS.len())]
}
fn hex24<R: Rng>(g: &mut R) -> String {
    format!("{:024x}", g.gen::<u128>() & ((1u128 << 96) - 1))
}
fn date<R: Rng>(g: &mut R) -> String {
    format!("2026-{:02}-{:02}T{:02}:{:02}", g.gen_range(1..13), g.gen_range(1..29), g.gen_range(0..24), g.gen_range(0..60))
}

/// valore del campo indicizzato i (1..=7), stesso schema delle voci d'indice di `real_record` (i % 4)
fn ix_val<R: Rng>(g: &mut R, i: usize) -> Bson {
    match i % 4 {
        1 => Bson::String(hex24(g)),
        2 => Bson::String(date(g)),
        3 => Bson::String(w(g).to_string()),
        _ => Bson::Int64(g.gen_range(0..100_000)),
    }
}

/// corpo da circa 1 KB in BSON, stessa forma di `json_doc`
fn body<R: Rng>(g: &mut R) -> Document {
    let mut items = vec![];
    for _ in 0..6 {
        items.push(Bson::Document(doc! {
            "sku": format!("{}-{:06}", w(g), g.gen_range(0..1_000_000)), "name": format!("{} {}", w(g), w(g)),
            "qty": g.gen_range(1..10), "price": g.gen_range(100..50000) as f64 / 100.0, "tags": [w(g), w(g)],
        }));
    }
    doc! {
        "plan": w(g),
        "customer": { "name": format!("{} {}", w(g), w(g)), "email": format!("{}.{}{}@example.com", w(g), w(g), g.gen_range(1..999)), "city": w(g), "zip": format!("{:05}", g.gen_range(0..99999)) },
        "payment": { "method": w(g), "amount": g.gen_range(100..200000) as f64 / 100.0, "currency": "EUR" },
        "items": items,
        "notes": format!("{} {} {}", w(g), w(g), w(g)),
        "version": g.gen_range(1..20),
        "counter": 0i64,
    }
}

fn new_doc<R: Rng>(g: &mut R, id: i64, idx: usize, pool: &[Document]) -> Document {
    let mut d = doc! { "_id": id };
    for i in 1..=idx {
        d.insert(format!("f{i}"), ix_val(g, i));
    }
    for (k, v) in pool[g.gen_range(0..pool.len())].iter() {
        d.insert(k.clone(), v.clone());
    }
    d
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let a = args();
    let mut opts = ClientOptions::parse(&a.uri).await.unwrap();
    opts.max_pool_size = Some(a.clients as u32 + 4);
    opts.min_pool_size = Some(a.clients as u32);
    let client = Client::with_options(opts).unwrap();
    let coll = client.database("bench").collection::<Document>("c");
    let ns = Namespace::new("bench", "c");
    let mut g = rand::thread_rng();
    let pool: Arc<Vec<Document>> = Arc::new((0..4096).map(|_| body(&mut g)).collect());
    if a.setup {
        let _ = coll.drop().await;
        client.database("bench").create_collection("c").await.unwrap();
        for i in 1..=a.idx {
            coll.create_index(mongodb::IndexModel::builder().keys(doc! { format!("f{i}"): 1 }).build()).await.unwrap();
        }
        // carico iniziale (w:1): così gli aggiornamenti e le cancellazioni trovano documenti
        let mut id = 0i64;
        while id < a.preload {
            let n = 1000.min(a.preload - id);
            let docs: Vec<Document> = (0..n).map(|k| new_doc(&mut g, id + k, a.idx, &pool)).collect();
            coll.insert_many(docs).ordered(false).await.unwrap();
            id += n;
        }
    }
    let wc = WriteConcern::builder()
        .w(if a.w == "majority" { Acknowledgment::Majority } else { Acknowledgment::Nodes(a.w.parse().unwrap()) })
        .journal(a.j)
        .build();
    let next_id = Arc::new(AtomicI64::new(a.preload));
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()));
    let mut tasks = vec![];
    for _ in 0..a.clients {
        let (client, ns, pool, wc, next_id, measuring, stop, ops, errs, hist) = (client.clone(), ns.clone(), pool.clone(), wc.clone(), next_id.clone(), measuring.clone(), stop.clone(), ops.clone(), errs.clone(), hist.clone());
        let (idx, batch, mix, lo) = (a.idx, a.batch, a.mix.clone(), 0i64);
        tasks.push(tokio::spawn(async move {
            use mongodb::options::{DeleteOneModel, InsertOneModel, UpdateOneModel, WriteModel};
            let mut g = rand::rngs::StdRng::from_entropy();
            while !stop.load(Relaxed) {
                let hi = next_id.load(Relaxed);
                let mut models: Vec<WriteModel> = Vec::with_capacity(batch);
                for _ in 0..batch {
                    let r: f64 = if mix == "insert" { 0.9 } else { g.gen() };
                    if r < 0.70 {
                        // $set su 1–2 campi; circa metà cambia un campo indicizzato
                        let id = g.gen_range(lo..hi);
                        let mut set = Document::new();
                        if g.gen_bool(0.5) && idx > 0 {
                            let i = g.gen_range(1..=idx);
                            set.insert(format!("f{i}"), ix_val(&mut g, i));
                        } else {
                            set.insert("notes", format!("{} {} {}", w(&mut g), w(&mut g), w(&mut g)));
                        }
                        if g.gen_bool(0.78) {
                            set.insert("payment.amount", g.gen_range(100..200000) as f64 / 100.0);
                        }
                        models.push(WriteModel::UpdateOne(UpdateOneModel::builder().namespace(ns.clone()).filter(doc! { "_id": id }).update(doc! { "$set": set }).build()));
                    } else if r < 0.95 {
                        let id = next_id.fetch_add(1, Relaxed);
                        models.push(WriteModel::InsertOne(InsertOneModel::builder().namespace(ns.clone()).document(new_doc(&mut g, id, idx, &pool)).build()));
                    } else {
                        let id = g.gen_range(lo..hi);
                        models.push(WriteModel::DeleteOne(DeleteOneModel::builder().namespace(ns.clone()).filter(doc! { "_id": id }).build()));
                    }
                }
                let t0 = Instant::now();
                let r = client.bulk_write(models).ordered(false).write_concern(wc.clone()).await;
                let us = t0.elapsed().as_micros() as u64;
                if measuring.load(Relaxed) {
                    match r {
                        Ok(_) => {
                            ops.fetch_add(batch as u64, Relaxed);
                            hist.lock().unwrap().record(us.max(1)).ok();
                        }
                        Err(e) => {
                            if errs.fetch_add(1, Relaxed) < 3 {
                                eprintln!("errore: {e}");
                            }
                        }
                    }
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(a.warm)).await;
    measuring.store(true, Relaxed);
    eprintln!("MISURA_INIZIO");
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(a.secs)).await;
    measuring.store(false, Relaxed);
    let el = t0.elapsed().as_secs_f64();
    stop.store(true, Relaxed);
    join_all(tasks).await;
    let h = hist.lock().unwrap();
    let n = ops.load(Relaxed);
    println!(
        "{}",
        serde_json::json!({
            "idx": a.idx, "mix": a.mix, "clients": a.clients, "batch": a.batch, "w": a.w, "j": a.j,
            "ops": n, "ops_per_s": (n as f64 / el).round(), "errors": errs.load(Relaxed),
            "batch_p50_ms": h.value_at_quantile(0.5) as f64 / 1000.0, "batch_p99_ms": h.value_at_quantile(0.99) as f64 / 1000.0,
            "batch_max_ms": h.max() as f64 / 1000.0,
        })
    );
}
