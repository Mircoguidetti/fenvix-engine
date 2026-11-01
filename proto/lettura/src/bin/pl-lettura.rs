//! Passo 3: latenza della lettura composta (finestra del registro via TCP + SST dal manifest), con i blocchi in cache
//! RAM, in cache su disco locale, o senza cache (ogni lettura da MinIO). Durante la misura il carico di scrittura e lo
//! scarico continuano, così le letture trovano sia versioni nella finestra sia run nuove negli SST. Alla fine
//! (facoltativo) ferma le scritture e confronta ogni chiave letta con il modello.

use bytes::Bytes;
use clap::Parser;
use lettura::cache::Cache;
use lettura::carico::{precarica, put_req, valore, COLL};
use lettura::iface::*;
use lettura::lettura::{CfgLettura, Lettore};
use lettura::manifest::Raggruppa;
use lettura::net::{self, RegistroRemoto};
use lettura::registro::RegistroSim;
use lettura::scarico::{CfgScarico, Worker};
use lettura::store::Store;
use lettura::util::{hist, perc};
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
struct A {
    #[arg(long, default_value_t = 4)]
    db: u64,
    #[arg(long, default_value_t = 50_000)]
    docs: u64,
    #[arg(long, default_value_t = 1024)]
    vsize: usize,
    /// scritture al secondo in tutto durante la misura
    #[arg(long, default_value_t = 2000.0)]
    rate: f64,
    #[arg(long, default_value_t = 200_000)]
    letture: u64,
    #[arg(long, default_value_t = 8)]
    conc: usize,
    /// ram | disco | nessuna
    #[arg(long, default_value = "ram")]
    cache: String,
    #[arg(long, default_value_t = 2048)]
    cache_mb: u64,
    #[arg(long, default_value = "/tmp/claude-0/cache-lettura")]
    cache_dir: String,
    /// frazione di letture su chiavi scritte di recente
    #[arg(long, default_value_t = 0.3)]
    recenti: f64,
    #[arg(long, default_value = "lettura")]
    bucket: String,
    #[arg(long, default_value = "127.0.0.1:7720")]
    addr: String,
    #[arg(long, default_value_t = 30)]
    riscaldo_s: u64,
    #[arg(long, default_value_t = 2000)]
    eta_ms: u64,
    #[arg(long)]
    verifica: bool,
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let st_pre = Store::minio(&a.bucket)?;
    let st = Store::minio(&a.bucket)?;
    let reg = RegistroSim::new(60_000, None)?;
    reg.avvia_potatura();
    {
        let reg = reg.clone();
        let addr = a.addr.clone();
        tokio::spawn(async move { net::serve(&addr, reg, Arc::new(|_: &str| String::new())).await });
    }
    let dbs: Vec<Db> = (0..a.db).map(|i| db_n(1000 + i)).collect();
    let t = Instant::now();
    let pos0 = pos_make(now_ms() - 1000, 0);
    precarica(&st_pre, &reg, Raggruppa::PerDb, &dbs, a.docs, a.vsize, 16 << 10, pos0).await?;
    eprintln!("precarico {:.1} s", t.elapsed().as_secs_f64());
    let wcfg = CfgScarico { eta_ms: a.eta_ms, dim_byte: 4 << 20, intervallo_ms: 200, orizzonte_ms: 120_000, grazia_ms: 60_000, ..Default::default() };
    let w = Worker::new(reg.clone(), st_pre.clone(), wcfg);
    tokio::spawn(w.clone().corri());

    // scrittore con modello: (db, id) -> crc del valore più recente
    let modello: Arc<Mutex<HashMap<(usize, u64), u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let recenti: Arc<Mutex<VecDeque<(usize, u64)>>> = Arc::new(Mutex::new(VecDeque::new()));
    let scrivi = Arc::new(AtomicBool::new(true));
    {
        let (reg, modello, recenti, scrivi, dbs) = (reg.clone(), modello.clone(), recenti.clone(), scrivi.clone(), dbs.clone());
        let (rate, docs, vsize) = (a.rate, a.docs, a.vsize);
        std::thread::spawn(move || {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            let t0 = Instant::now();
            let mut fatti = 0u64;
            while scrivi.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
                let ob = (t0.elapsed().as_secs_f64() * rate) as u64;
                while fatti < ob {
                    let d = rng.gen_range(0..dbs.len());
                    let id = rng.gen_range(0..docs);
                    let v = valore(&mut rng, vsize);
                    let crc = crc32fast::hash(&v);
                    if let CommitResp::Committed(_) = reg.commit_sync(put_req(dbs[d], doc_key(COLL, id), v)) {
                        modello.lock().insert((d, id), crc);
                        let mut r = recenti.lock();
                        r.push_back((d, id));
                        if r.len() > 20_000 {
                            r.pop_front();
                        }
                    }
                    fatti += 1;
                }
            }
        });
    }
    // posizione di lettura: la posizione confermata, aggiornata via TCP ogni 2 ms
    let remoto = RegistroRemoto::new(&a.addr, a.conc + 2);
    let s_pos = Arc::new(AtomicU64::new(0));
    {
        let (remoto, s_pos) = (remoto.clone(), s_pos.clone());
        tokio::spawn(async move {
            loop {
                if let Ok(p) = remoto.posizione_confermata().await {
                    s_pos.store(p, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });
    }
    let cache = match a.cache.as_str() {
        "ram" => Cache::ram(a.cache_mb << 20),
        "disco" => Cache::disco(a.cache_mb << 20, a.cache_dir.clone().into())?,
        _ => Cache::ram(0),
    };
    let lett = Lettore::new(remoto.clone(), st.clone(), cache, CfgLettura::default());
    tokio::time::sleep(Duration::from_secs(a.riscaldo_s)).await;
    if a.cache != "nessuna" {
        let t = Instant::now();
        let s = s_pos.load(Ordering::Relaxed);
        let jobs: Vec<(usize, u64)> = (0..dbs.len()).flat_map(|d| (0..a.docs).map(move |i| (d, i))).collect();
        let lett2 = lett.clone();
        let dbs2 = dbs.clone();
        let sem = Arc::new(tokio::sync::Semaphore::new(32));
        let mut hs = Vec::new();
        for (d, i) in jobs {
            let p = sem.clone().acquire_owned().await?;
            let (l, db) = (lett2.clone(), dbs2[d]);
            hs.push(tokio::spawn(async move {
                let _ = l.get(db, doc_key(COLL, i), s).await;
                drop(p);
            }));
        }
        for h in hs {
            let _ = h.await;
        }
        eprintln!("cache riscaldata in {:.1} s, {} MB", t.elapsed().as_secs_f64(), lett.cache.usati() >> 20);
    }
    // misura
    let c0 = st.c.leggi();
    let l0 = lett.stat();
    let tot = Arc::new(Mutex::new((hist(), hist(), hist())));
    let fatte = Arc::new(AtomicU64::new(0));
    let errori = Arc::new(AtomicU64::new(0));
    let t_mis = Instant::now();
    let mut hs = Vec::new();
    for _ in 0..a.conc {
        let (lett, tot, fatte, errori, s_pos, recenti, dbs) = (lett.clone(), tot.clone(), fatte.clone(), errori.clone(), s_pos.clone(), recenti.clone(), dbs.clone());
        let (n, docs, frac) = (a.letture, a.docs, a.recenti);
        hs.push(tokio::spawn(async move {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            while fatte.fetch_add(1, Ordering::Relaxed) < n {
                let (d, id) = if rng.gen::<f64>() < frac {
                    let r = recenti.lock();
                    if r.is_empty() {
                        (rng.gen_range(0..dbs.len()), rng.gen_range(0..docs))
                    } else {
                        r[rng.gen_range(0..r.len())]
                    }
                } else {
                    (rng.gen_range(0..dbs.len()), rng.gen_range(0..docs))
                };
                let s = s_pos.load(Ordering::Relaxed);
                let t = Instant::now();
                match lett.get_fonte(dbs[d], doc_key(COLL, id), s).await {
                    Ok((_, fin)) => {
                        let us = t.elapsed().as_micros() as u64;
                        let mut g = tot.lock();
                        let _ = g.0.record(us.max(1));
                        let _ = if fin { g.1.record(us.max(1)) } else { g.2.record(us.max(1)) };
                    }
                    Err(e) => {
                        if errori.fetch_add(1, Ordering::Relaxed) < 5 {
                            eprintln!("lettura: {e:#}");
                        }
                    }
                }
            }
        }));
    }
    for h in hs {
        h.await?;
    }
    let secs = t_mis.elapsed().as_secs_f64();
    let c1 = st.c.leggi().meno(&c0);
    let l1 = lett.stat();
    let g = tot.lock();
    let lat_s3 = st.c.lat_get.lock().as_ref().map(perc);
    let mut res = json!({
        "cache": a.cache, "db": a.db, "docs_per_db": a.docs, "vsize": a.vsize, "scritture_s": a.rate, "conc": a.conc,
        "letture": g.0.len(), "secondi": secs, "letture_s": g.0.len() as f64 / secs, "errori": errori.load(Ordering::Relaxed),
        "lat_tutte": perc(&g.0), "lat_dalla_finestra": perc(&g.1), "lat_dagli_sst": perc(&g.2),
        "s3_durante_misura": c1, "lettore_prima": l0, "lettore_dopo": l1, "lat_get_minio_cumulata": lat_s3,
        "worker": w.stat.leggi(),
    });
    drop(g);
    if a.verifica {
        scrivi.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let s = reg.posizione_confermata_sync();
        let m = modello.lock().clone();
        let (mut ok, mut ko) = (0u64, 0u64);
        // tutte le chiavi scritte + 20.000 mai scritte (valore del precarico: si controlla solo che ci sia)
        for ((d, id), crc) in m.iter() {
            match lett.get(dbs[*d], doc_key(COLL, *id), s).await? {
                Some(v) if crc32fast::hash(&v) == *crc => ok += 1,
                _ => ko += 1,
            }
        }
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let mut presenti = 0u64;
        for _ in 0..20_000 {
            let d = rng.gen_range(0..dbs.len());
            let id = rng.gen_range(0..a.docs);
            if !m.contains_key(&(d, id)) {
                if lett.get(dbs[d], doc_key(COLL, id), s).await?.map(|v| v.len() == a.vsize).unwrap_or(false) {
                    presenti += 1;
                } else {
                    ko += 1;
                }
            }
        }
        let _ = Bytes::new();
        res["verifica"] = json!({"chiavi_scritte_giuste": ok, "sbagliate": ko, "chiavi_del_precarico_presenti": presenti});
    }
    let txt = serde_json::to_string_pretty(&res)?;
    println!("{txt}");
    if let Some(o) = a.out {
        std::fs::write(o, txt)?;
    }
    std::process::exit(0);
}
