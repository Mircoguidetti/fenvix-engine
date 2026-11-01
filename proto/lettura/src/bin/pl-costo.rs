//! Passo 5: costo S3 per database piccolo. N database (1.000), ognuno con `wps` scritture e `rps` letture puntuali al
//! secondo da `vsize` byte, per `dur_s` secondi; una cache dei nodi d'ingresso da `cache_mb` (disco locale) condivisa
//! da tutti i database. Conta TUTTE le richieste a MinIO (lato client, per worker e lettori, e lato server dai
//! contatori del bucket) e le converte in dollari al mese per database.
//!
//! Varianti:
//! - `base`: un manifest per database, scarico ogni 60 s (il caso "un PUT al minuto per database");
//! - `a`: un manifest per database, scarico a 1 MB o 10 minuti;
//! - `b`: un oggetto condiviso per server: un solo gruppo con tutti i database, scarico ogni 60 s.

use clap::Parser;
use lettura::cache::Cache;
use lettura::carico::{precarica, put_req, valore, Zipf, COLL};
use lettura::iface::*;
use lettura::lettura::{CfgLettura, Lettore};
use lettura::manifest::Raggruppa;
use lettura::registro::RegistroSim;
use lettura::scarico::{CfgScarico, Worker};
use lettura::store::{metriche_minio, Conteggi, Store};
use lettura::util::{hist, perc};
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
struct A {
    #[arg(long)]
    variante: String,
    #[arg(long, default_value_t = 1000)]
    db: u64,
    #[arg(long, default_value_t = 500)]
    docs: u64,
    #[arg(long, default_value_t = 1024)]
    vsize: usize,
    #[arg(long, default_value_t = 1.0)]
    wps: f64,
    #[arg(long, default_value_t = 2.0)]
    rps: f64,
    #[arg(long, default_value_t = 3600)]
    dur_s: u64,
    #[arg(long, default_value_t = 1024)]
    cache_mb: u64,
    /// disco | ram
    #[arg(long, default_value = "disco")]
    cache: String,
    #[arg(long, default_value_t = 0.99)]
    zipf: f64,
    #[arg(long)]
    bucket: String,
    #[arg(long, default_value_t = 300)]
    istantanea_s: u64,
    #[arg(long)]
    out: std::path::PathBuf,
}

const PUT_1000: f64 = 0.005;
const GET_1000: f64 = 0.0004;
const ORE_MESE: f64 = 720.0;

fn costi(c: &Conteggi, secondi: f64, db: u64) -> serde_json::Value {
    let f = ORE_MESE * 3600.0 / secondi / db as f64;
    let put = c.classe_put() as f64 * f * PUT_1000 / 1000.0;
    let get = c.classe_get() as f64 * f * GET_1000 / 1000.0;
    json!({
        "richieste_al_mese_per_db": {"put": (c.put + c.put_cond) as f64 * f, "list": c.list as f64 * f, "get": c.classe_get() as f64 * f, "delete": c.delete as f64 * f},
        "dollari_mese_per_db": {"put_e_list": put, "get": get, "totale": put + get},
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let (raggruppa, eta_ms, dim, max_batch) = match a.variante.as_str() {
        "base" => (Raggruppa::PerDb, 60_000, 1 << 20, 16 << 20),
        "a" => (Raggruppa::PerDb, 600_000, 1 << 20, 16 << 20),
        "b" => (Raggruppa::Gruppi(1), 60_000, 256 << 20, 256 << 20),
        x => anyhow::bail!("variante sconosciuta {x}"),
    };
    let st_pre = Store::minio(&a.bucket)?;
    let st_w = Store::minio(&a.bucket)?;
    let st_r = Store::minio(&a.bucket)?;
    let reg = RegistroSim::new(60_000, None)?;
    reg.avvia_potatura();
    let dbs: Vec<Db> = (0..a.db).map(|i| db_n(200_000 + i)).collect();
    let t = Instant::now();
    let pos0 = pos_make(now_ms() - 1000, 0);
    precarica(&st_pre, &reg, raggruppa, &dbs, a.docs, a.vsize, 16 << 10, pos0).await?;
    eprintln!("precarico {:.1} s", t.elapsed().as_secs_f64());
    let wcfg = CfgScarico {
        raggruppa,
        eta_ms,
        dim_byte: dim,
        max_batch,
        blocco: 16 << 10,
        intervallo_ms: 1000,
        paralleli: 16,
        compatta: true,
        compatta_k: 4,
        max_run: 12,
        riposo_ms: 0,
        tieni_tutte: false,
        orizzonte_ms: 120_000,
        grazia_ms: 120_000,
        orfani_ogni_ms: 6 * 3600 * 1000,
        orfani_eta_ms: 3600 * 1000,
        crash: 0.0,
        cache_byte: 1 << 30,
        senza_cas: false,
    };
    let w = Worker::new(reg.clone(), st_w.clone(), wcfg);
    let cache = if a.cache == "ram" { Cache::ram(a.cache_mb << 20) } else { Cache::disco(a.cache_mb << 20, format!("/tmp/claude-0/cache-costo-{}", a.variante).into())? };
    let lett = Lettore::new(reg.clone(), st_r.clone(), cache, CfgLettura { raggruppa, intero_max: 1 << 20, fetta_max: 1 << 20, prefetch_meta: false });
    // i contatori di MinIO si aggiornano con qualche secondo di ritardo: il precarico deve esserci già dentro
    tokio::time::sleep(Duration::from_secs(20)).await;
    let minio0 = metriche_minio(&a.bucket).await?;
    tokio::spawn(w.clone().corri());

    let zipf = Arc::new(Zipf::new(a.docs, a.zipf));
    let lat = Arc::new(Mutex::new(hist()));
    let errori = Arc::new(AtomicU64::new(0));
    let sem = Arc::new(tokio::sync::Semaphore::new(512));
    let (mut scritte, mut lette) = (0u64, 0u64);
    let t0 = Instant::now();
    let fine = Duration::from_secs(a.dur_s);
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let mut prossima_ist = Duration::from_secs(a.istantanea_s);
    let mut prossimo_campione = Duration::from_secs(10);
    let (mut ram_max, mut ram_somma, mut ram_n, mut pend_max) = (0u64, 0u64, 0u64, 0u64);
    let mut meta_c: Option<(Conteggi, Conteggi, f64)> = None;
    let mut out_f = std::fs::File::create(&a.out)?;
    use std::io::Write as _;
    while t0.elapsed() < fine {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let el = t0.elapsed();
        let ob_w = (el.as_secs_f64() * a.wps * a.db as f64) as u64;
        while scritte < ob_w {
            let d = rng.gen_range(0..a.db);
            let id = zipf.id(&mut rng, d);
            reg.commit_sync(put_req(dbs[d as usize], doc_key(COLL, id), valore(&mut rng, a.vsize)));
            scritte += 1;
        }
        let ob_r = (el.as_secs_f64() * a.rps * a.db as f64) as u64;
        while lette < ob_r {
            let d = rng.gen_range(0..a.db);
            let id = zipf.id(&mut rng, d);
            let s = reg.posizione_confermata_sync();
            let p = sem.clone().acquire_owned().await?;
            let (lett, lat, errori, db) = (lett.clone(), lat.clone(), errori.clone(), dbs[d as usize]);
            tokio::spawn(async move {
                let t = Instant::now();
                match lett.get(db, doc_key(COLL, id), s).await {
                    Ok(_) => {
                        let _ = lat.lock().record((t.elapsed().as_micros() as u64).max(1));
                    }
                    Err(e) => {
                        if errori.fetch_add(1, Ordering::Relaxed) < 10 {
                            eprintln!("lettura: {e:#}");
                        }
                    }
                }
                drop(p);
            });
            lette += 1;
        }
        if el >= prossimo_campione {
            prossimo_campione += Duration::from_secs(10);
            let s = reg.stat();
            ram_max = ram_max.max(s.win_byte);
            ram_somma += s.win_byte;
            ram_n += 1;
            pend_max = pend_max.max(s.pend_byte);
        }
        if el >= prossima_ist {
            prossima_ist += Duration::from_secs(a.istantanea_s);
            let (cw, cr) = (st_w.c.leggi(), st_r.c.leggi());
            if meta_c.is_none() && el.as_secs() >= a.dur_s / 2 {
                meta_c = Some((cw, cr, el.as_secs_f64()));
            }
            let l = json!({"t_s": el.as_secs(), "worker": cw, "lettori": cr, "lettore": lett.stat(), "scarico": w.stat.leggi(), "reg": reg.stat(),
                "rss": lettura::util::rss(0), "cpu_s": lettura::util::cpu_s(0), "lat": perc(&lat.lock()), "errori": errori.load(Ordering::Relaxed)});
            writeln!(out_f, "{l}")?;
            eprintln!("{l}");
        }
    }
    let secondi = t0.elapsed().as_secs_f64();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (cw, cr) = (st_w.c.leggi(), st_r.c.leggi());
    let tot = cw.somma(&cr);
    let minio1 = metriche_minio(&a.bucket).await?;
    let minio: std::collections::BTreeMap<String, u64> = minio1.iter().map(|(k, v)| (k.clone(), v - minio0.get(k).copied().unwrap_or(0))).collect();
    let regime = meta_c.map(|(w0, r0, t_m)| {
        let d = cw.meno(&w0).somma(&cr.meno(&r0));
        json!({"da_s": t_m, "secondi": secondi - t_m, "worker": cw.meno(&w0), "lettori": cr.meno(&r0), "costo": costi(&d, secondi - t_m, a.db)})
    });
    let srv_put = minio.iter().filter(|(k, _)| k.starts_with("put") || k.starts_with("list") || k.starts_with("copy") || k.contains("multipart")).map(|(_, v)| v).sum::<u64>();
    let srv_get = minio.iter().filter(|(k, _)| k.starts_with("get") || k.starts_with("head")).map(|(_, v)| v).sum::<u64>();
    let srv = Conteggi { put: srv_put, get: srv_get, ..Default::default() };
    let r = json!({
        "variante": a.variante, "db": a.db, "docs_per_db": a.docs, "vsize": a.vsize, "scritture_s_per_db": a.wps, "letture_s_per_db": a.rps,
        "zipf": a.zipf, "cache_mb": a.cache_mb, "cache": a.cache, "secondi": secondi, "scritture": scritte, "letture": lette,
        "errori_lettura": errori.load(Ordering::Relaxed),
        "worker": cw, "lettori": cr, "costo_ora_intera": costi(&tot, secondi, a.db), "costo_seconda_meta": regime,
        "minio_lato_server": minio, "costo_dai_contatori_minio": costi(&srv, secondi, a.db),
        "ram_registro_byte": {"media": ram_somma / ram_n.max(1), "max": ram_max, "non_su_s3_max": pend_max},
        "lettore": lett.stat(), "scarico": w.stat.leggi(), "lat_lettura": perc(&lat.lock()),
        "rss_processo": lettura::util::rss(0), "cpu_s": lettura::util::cpu_s(0),
    });
    let txt = serde_json::to_string_pretty(&r)?;
    writeln!(out_f, "{}", serde_json::to_string(&r)?)?;
    println!("{txt}");
    std::process::exit(0);
}
