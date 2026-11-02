//! Dato a margine del passo 2: quanti MB/s fonde un core in compattazione (lettura delle voci, fusione con taglio
//! delle versioni, costruzione dell'SST nuovo), senza I/O. Run come quelle del database caldo: 16 MB, 1,2 KB per voce.

use bytes::Bytes;
use clap::Parser;
use lettura::carico::{valore, COLL};
use lettura::iface::*;
use lettura::scarico::fondi;
use lettura::sst::{self, Entry};
use rand::{Rng, SeedableRng};
use std::time::Instant;

#[derive(Parser)]
struct A {
    #[arg(long, default_value_t = 4)]
    run: usize,
    #[arg(long, default_value_t = 16)]
    mb: usize,
    #[arg(long, default_value_t = 10_000_000)]
    chiavi: u64,
}

fn main() {
    let a = A::parse();
    let mut rng = rand::rngs::SmallRng::seed_from_u64(1);
    let db = db_n(1);
    let per_run = a.mb * 1_000_000 / 1260;
    let mut pos = pos_make(now_ms(), 0);
    let mut objs = Vec::new();
    for _ in 0..a.run {
        let mut es: Vec<Entry> = (0..per_run)
            .map(|_| {
                pos += 1;
                Entry { key: sst::full_key(&db, &doc_key(COLL, rng.gen_range(0..a.chiavi))), pos, kind: Kind::Put, val: valore(&mut rng, 1200) }
            })
            .collect();
        es.sort_by(|x, y| x.key.cmp(&y.key).then(y.pos.cmp(&x.pos)));
        objs.push(Bytes::from(sst::build(es, 16 << 10).data));
    }
    let byte_in: usize = objs.iter().map(|o| o.len()).sum();
    let t = Instant::now();
    let mut es = Vec::new();
    for o in &objs {
        es.extend(sst::all_entries(o).unwrap());
    }
    let es = fondi(es, Some(pos), false);
    let b = sst::build(es, 16 << 10);
    let s = t.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::json!({"run": a.run, "mb_ingresso": byte_in as f64 / 1e6, "mb_uscita": b.data.len() as f64 / 1e6, "secondi_un_core": s, "mb_s_un_core": byte_in as f64 / 1e6 / s})
    );
}
