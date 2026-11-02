//! Il cameriere con N database conosciuti e un LRU di database aperti, sotto un generatore zipf (il 5% dei database
//! prende il 95% delle letture). Misura, in ordine:
//! 1. RAM per database conosciuto ma chiuso (la riga nome -> id);
//! 2. RAM per database aperto (foglio, indice e bloom della scatola, catalogo, e la scatola se `--dati ram`);
//! 3. letture puntuali: p50/p99 di tutte, di quelle servite da un database già aperto, dei risvegli; GET per risveglio;
//! 4. tempesta di risvegli: LRU svuotato (riavvio del cameriere), poi `--tempesta` database freddi al secondo, ognuno con
//!    una lettura, per `--tempesta-s` secondi (anello aperto).
//! Ogni lettura confronta il valore con quello atteso (crc).

use clap::Parser;
use lettura::iface::*;
use lettura::registro::RegistroSim;
use lettura::store::Store;
use milione::*;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
struct A {
    /// database conosciuti
    #[arg(long)]
    n: u64,
    /// database aperti al massimo (LRU)
    #[arg(long)]
    capienza: usize,
    #[arg(long, value_enum, default_value = "ram")]
    dati: Dati,
    #[arg(long, default_value = "/home/claude/pm-cache")]
    cache_dir: PathBuf,
    /// database da aprire per la misura della RAM per aperto (0 = la capienza)
    #[arg(long, default_value_t = 0)]
    misura_aperti: u64,
    #[arg(long, default_value_t = 20)]
    scalda_s: u64,
    #[arg(long, default_value_t = 60)]
    durata_s: u64,
    #[arg(long, default_value_t = 16)]
    lettori: u64,
    /// database freddi al secondo nella tempesta (0 = niente tempesta)
    #[arg(long, default_value_t = 0)]
    tempesta: u64,
    #[arg(long, default_value_t = 30)]
    tempesta_s: u64,
    /// ritardo artificiale per GET (latenza di S3 Standard sopra il finto S3 locale)
    #[arg(long, default_value_t = 0)]
    ritardo_get_ms: u64,
    #[arg(long, default_value = "milione")]
    bucket: String,
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long, default_value = "")]
    etichetta: String,
}

fn heap() -> (i64, i64) {
    vivi()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let mut st = Store::minio(&a.bucket)?;
    st.ritardo_get = Duration::from_millis(a.ritardo_get_ms);
    let reg = RegistroSim::new(60_000, None)?;
    reg.avanza(pos_make(now_ms(), 0));
    let base = json!({"etichetta": a.etichetta, "n": a.n, "capienza": a.capienza, "dati": a.dati, "ritardo_get_ms": a.ritardo_get_ms, "lettori": a.lettori});

    // zipf prima di tutto (la sua CDF è del banco, non del cameriere)
    let s = Zipf::esponente(a.n, 0.05, 0.95);
    let z = Arc::new(Zipf::new(a.n, s));
    let caldi = ((a.n as f64) * 0.05).round() as u64;

    // 1. conosciuti
    let (h0, n0) = heap();
    let r0 = rss();
    let t = Instant::now();
    let mut mappa: HashMap<Box<str>, Db> = HashMap::new();
    for i in 0..a.n {
        mappa.insert(nome(i).into_boxed_str(), db_id(i));
    }
    let t_conosciuti = t.elapsed().as_secs_f64();
    let (h1, n1) = heap();
    let r1 = rss();
    let per_conosciuto = (h1 - h0) as f64 / a.n as f64;
    let cam = Cameriere::new(st.clone(), reg.clone(), Conosciuti { mappa }, a.capienza, a.dati, a.cache_dir.clone());
    let _ = std::fs::remove_dir_all(&a.cache_dir);
    std::fs::create_dir_all(&a.cache_dir)?;
    scrivi_riga(
        &a.out,
        &json!({"prova": "conosciuti", "base": base, "esponente_zipf": s, "quota_letture_primo_5pc": z.quota_primi(caldi),
                "byte_heap_per_conosciuto": per_conosciuto, "alloc_per_conosciuto": (n1 - n0) as f64 / a.n as f64,
                "rss_per_conosciuto": (r1 as f64 - r0 as f64) / a.n as f64, "secondi_per_costruire": t_conosciuti}),
    );

    // 2. RAM per aperto: si aprono i primi M per rango (i più caldi), 64 in parallelo
    let m = if a.misura_aperti > 0 { a.misura_aperti } else { a.capienza as u64 }.min(a.n).min(a.capienza as u64);
    let (h2, n2) = heap();
    let r2 = rss();
    let c0 = st.c.leggi();
    let t = Instant::now();
    {
        use futures::StreamExt;
        let n = a.n;
        let res: Vec<anyhow::Result<()>> = futures::stream::iter(0..m)
            .map(|r| {
                let cam = cam.clone();
                async move {
                    cam.apri(mescola(r, n)).await?;
                    Ok(())
                }
            })
            .buffer_unordered(64)
            .collect()
            .await;
        for r in res {
            r?;
        }
    }
    let t_apri = t.elapsed().as_secs_f64();
    let (h3, n3) = heap();
    let r3 = rss();
    let c = st.c.leggi().meno(&c0);
    let aperti = cam.n_aperti() as f64;
    let un = cam.aperti.lock().peek(&mescola(0, a.n)).cloned();
    let scatola = un.as_ref().map(|x| x.meta.bytes).unwrap_or(0);
    scrivi_riga(
        &a.out,
        &json!({"prova": "aperti", "base": base, "aperti": aperti, "byte_heap_per_aperto": (h3 - h2) as f64 / aperti, "alloc_per_aperto": (n3 - n2) as f64 / aperti,
                "rss_per_aperto": (r3 as f64 - r2 as f64) / aperti, "secondi": t_apri, "risvegli_al_s": aperti / t_apri,
                "get_per_risveglio": c.classe_get() as f64 / aperti, "list": c.list, "byte_get_per_risveglio": c.get_byte as f64 / aperti, "byte_meta_sst": scatola,
                "scomposizione_di_uno": un.as_ref().map(|x| scomponi(x))}),
    );

    // 3. letture zipf
    let fine = Arc::new(AtomicBool::new(false));
    let misura = Arc::new(AtomicBool::new(false));
    let tutte = Arc::new(parking_lot::Mutex::new(hist()));
    let hit = Arc::new(parking_lot::Mutex::new(hist()));
    let sveglie = Arc::new(parking_lot::Mutex::new(hist()));
    let mut hs = Vec::new();
    for l in 0..a.lettori {
        let (cam, z, fine, misura, tutte, hit, sveglie) = (cam.clone(), z.clone(), fine.clone(), misura.clone(), tutte.clone(), hit.clone(), sveglie.clone());
        let n = a.n;
        hs.push(tokio::spawn(async move {
            let mut rng = rand::rngs::SmallRng::seed_from_u64(l * 7919 + 1);
            let (mut ht, mut hh, mut hw) = (hist(), hist(), hist());
            let mut misurando = false;
            while !fine.load(Relaxed) {
                if !misurando && misura.load(Relaxed) {
                    misurando = true;
                    ht.reset();
                    hh.reset();
                    hw.reset();
                }
                let idx = mescola(z.rango(&mut rng), n);
                let d = rng.gen_range(0..DOCS);
                let t = Instant::now();
                match cam.leggi(idx, d).await {
                    Ok((v, sv)) => {
                        let us = t.elapsed().as_micros() as u64;
                        let _ = ht.record(us.max(1));
                        let _ = if sv { hw.record(us.max(1)) } else { hh.record(us.max(1)) };
                        if v != Some(crc_atteso(idx, d)) {
                            cam.stat.sbagliate.fetch_add(1, Relaxed);
                        }
                    }
                    Err(e) => {
                        if cam.stat.errori.fetch_add(1, Relaxed) < 5 {
                            eprintln!("lettura {idx}: {e:#}");
                        }
                    }
                }
            }
            tutte.lock().add(&ht).unwrap();
            hit.lock().add(&hh).unwrap();
            sveglie.lock().add(&hw).unwrap();
        }));
    }
    tokio::time::sleep(Duration::from_secs(a.scalda_s)).await;
    let l0 = cam.stat.letture.load(Relaxed);
    let w0 = cam.stat.risvegli.load(Relaxed);
    let g0 = cam.stat.get_risveglio.load(Relaxed);
    let c0 = st.c.leggi();
    let cpu0 = cpu_s();
    misura.store(true, Relaxed);
    let t = Instant::now();
    tokio::time::sleep(Duration::from_secs(a.durata_s)).await;
    fine.store(true, Relaxed);
    let dt = t.elapsed().as_secs_f64();
    for h in hs {
        h.await?;
    }
    let cpu = cpu_s() - cpu0;
    let c = st.c.leggi().meno(&c0);
    let letture = cam.stat.letture.load(Relaxed) - l0;
    let risvegli = cam.stat.risvegli.load(Relaxed) - w0;
    let (h4, _) = heap();
    scrivi_riga(
        &a.out,
        &json!({"prova": "letture", "base": base, "secondi": dt, "letture": letture, "letture_al_s": letture as f64 / dt, "cpu_core": cpu / dt,
                "tutte": perc(&tutte.lock()), "da_aperto": perc(&hit.lock()), "con_risveglio": perc(&sveglie.lock()),
                "risvegli": risvegli, "quota_risvegli": risvegli as f64 / letture.max(1) as f64,
                "get_per_risveglio": (cam.stat.get_risveglio.load(Relaxed) - g0) as f64 / risvegli.max(1) as f64, "richieste": c,
                "errori": cam.stat.errori.load(Relaxed), "sbagliate": cam.stat.sbagliate.load(Relaxed), "aperti_alla_fine": cam.n_aperti(),
                "sfratti": cam.stat.sfratti.load(Relaxed), "heap_mb": h4 as f64 / 1e6, "rss_mb": rss() as f64 / 1e6}),
    );

    // 4. tempesta di risvegli
    if a.tempesta > 0 {
        cam.svuota();
        let _ = std::fs::remove_dir_all(&a.cache_dir);
        std::fs::create_dir_all(&a.cache_dir)?;
        let n = a.n;
        let tot = a.tempesta * a.tempesta_s;
        anyhow::ensure!(tot <= n - caldi, "non ci sono abbastanza database freddi distinti");
        let lat = Arc::new(parking_lot::Mutex::new(hist()));
        let fatte = Arc::new(AtomicU64::new(0));
        let err = Arc::new(AtomicU64::new(0));
        let max_volo = Arc::new(AtomicU64::new(0));
        let in_volo = Arc::new(AtomicU64::new(0));
        let w0 = cam.stat.risvegli.load(Relaxed);
        let g0 = cam.stat.get_risveglio.load(Relaxed);
        let c0 = st.c.leggi();
        let cpu0 = cpu_s();
        let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / a.tempesta));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let t0 = Instant::now();
        let mut hs = Vec::new();
        let mut ritardo_max_ms = 0u128;
        for k in 0..tot {
            tick.tick().await;
            let atteso = Duration::from_micros(k * 1_000_000 / a.tempesta);
            ritardo_max_ms = ritardo_max_ms.max(t0.elapsed().saturating_sub(atteso).as_millis());
            // freddi: ranghi oltre il 5% caldo, distinti
            let idx = mescola(caldi + k, n);
            let (cam, lat, fatte, err, in_volo, max_volo) = (cam.clone(), lat.clone(), fatte.clone(), err.clone(), in_volo.clone(), max_volo.clone());
            let v = in_volo.fetch_add(1, Relaxed) + 1;
            max_volo.fetch_max(v, Relaxed);
            hs.push(tokio::spawn(async move {
                let t = Instant::now();
                match cam.leggi(idx, 1).await {
                    Ok((v, _)) if v == Some(crc_atteso(idx, 1)) => {
                        fatte.fetch_add(1, Relaxed);
                        let _ = lat.lock().record(t.elapsed().as_micros() as u64);
                    }
                    Ok(_) => {
                        cam.stat.sbagliate.fetch_add(1, Relaxed);
                    }
                    Err(e) => {
                        if err.fetch_add(1, Relaxed) < 5 {
                            eprintln!("tempesta {idx}: {e:#}");
                        }
                    }
                }
                in_volo.fetch_sub(1, Relaxed);
            }));
        }
        let t_gen = t0.elapsed().as_secs_f64();
        for h in hs {
            h.await?;
        }
        let dt = t0.elapsed().as_secs_f64();
        let c = st.c.leggi().meno(&c0);
        let w = cam.stat.risvegli.load(Relaxed) - w0;
        let fatte = fatte.load(Relaxed);
        scrivi_riga(
            &a.out,
            &json!({"prova": "tempesta", "base": base, "ritmo_chiesto": a.tempesta, "secondi": a.tempesta_s, "risvegli": w, "letture_giuste": fatte,
                    "errori": err.load(Relaxed), "sbagliate": cam.stat.sbagliate.load(Relaxed), "durata_s": dt, "durata_generazione_s": t_gen,
                    "ritardo_max_generatore_ms": ritardo_max_ms, "ritmo_ottenuto": fatte as f64 / dt, "latenza": perc(&lat.lock()), "max_in_volo": max_volo.load(Relaxed),
                    "get_per_risveglio": (cam.stat.get_risveglio.load(Relaxed) - g0) as f64 / w.max(1) as f64, "richieste": c, "cpu_core": (cpu_s() - cpu0) / dt}),
        );
    }
    Ok(())
}
