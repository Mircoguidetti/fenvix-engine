//! Creazione dei database piccoli (variante a2: una scatola SST e un foglio per database).
//!
//! - `diretto`: precarico, scrive i file direttamente nella cartella del finto S3 su disco (stessa forma degli oggetti
//!   S3, nessuna richiesta: serve solo a mettere un milione di database sul disco in tempo ragionevole);
//! - `http`: la prova "1.000 creazioni al secondo": ritmo fisso (anello aperto), per ogni database PUT della scatola e
//!   PUT condizionale del foglio (If-None-Match: *), con il calcolo SCRAM vero del server (PBKDF2 a 15.000 iterazioni)
//!   o senza (`--iter 0`); conta richieste, tempi, CPU del processo, errori;
//! - `cpu`: CPU di una creazione, pezzo per pezzo, su un core.

use bytes::Bytes;
use clap::{Parser, Subcommand};
use lettura::iface::*;
use lettura::manifest;
use lettura::store::{Cas, Store};
use milione::*;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
struct A {
    #[command(subcommand)]
    c: C,
    #[arg(long, default_value = "milione")]
    bucket: String,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Subcommand)]
enum C {
    Diretto {
        #[arg(long)]
        root: PathBuf,
        #[arg(long, default_value_t = 0)]
        da: u64,
        #[arg(long)]
        a: u64,
        #[arg(long, default_value_t = 2)]
        thread: u64,
    },
    Http {
        #[arg(long)]
        da: u64,
        #[arg(long, default_value_t = 1000)]
        ritmo: u64,
        #[arg(long, default_value_t = 30)]
        secondi: u64,
        #[arg(long, default_value_t = 15000)]
        iter: u32,
        #[arg(long, default_value_t = 256)]
        max_in_volo: u64,
    },
    Cpu {
        #[arg(long, default_value_t = 200)]
        n: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let pos = pos_make(now_ms() - 60_000, 0);
    match a.c {
        C::Diretto { root, da, a: fino, thread } => {
            let t0 = Instant::now();
            let fatti = Arc::new(AtomicU64::new(0));
            let byte = Arc::new(AtomicU64::new(0));
            let mut hs = Vec::new();
            for t in 0..thread {
                let (root, b, fatti, byte) = (root.clone(), a.bucket.clone(), fatti.clone(), byte.clone());
                hs.push(std::thread::spawn(move || -> anyhow::Result<()> {
                    let mut i = da + t;
                    while i < fino {
                        let (id, sst, m) = oggetti_db(i, pos, 0, 16 << 10);
                        let g = db_hex(&db_id(i));
                        scrivi_file(&file_di(&root, &b, &manifest::path_sst(&g, &id)), &sst)?;
                        let mj = serde_json::to_vec(&m)?;
                        scrivi_file(&file_di(&root, &b, &manifest::path_manifest(&g)), &mj)?;
                        byte.fetch_add((sst.len() + mj.len()) as u64, Relaxed);
                        let n = fatti.fetch_add(1, Relaxed) + 1;
                        if n % 100_000 == 0 {
                            eprintln!("{n} database, {:.0} s", t0.elapsed().as_secs_f64());
                        }
                        i += thread;
                    }
                    Ok(())
                }));
            }
            for h in hs {
                h.join().unwrap()?;
            }
            let n = fatti.load(Relaxed);
            scrivi_riga(&a.out, &json!({"prova": "precarico_diretto", "da": da, "a": fino, "database": n, "secondi": t0.elapsed().as_secs_f64(), "byte": byte.load(Relaxed), "byte_per_db": byte.load(Relaxed) / n.max(1)}));
        }
        C::Cpu { n } => {
            // un core: PBKDF2 a 15.000 e a 0, costruzione degli oggetti e del JSON del foglio
            let t = Instant::now();
            for i in 0..n {
                std::hint::black_box(scram(&format!("password-{i}"), b"sale-di-28-byte-1234567890ab", ITER_SCRAM));
            }
            let pbkdf2_ms = t.elapsed().as_secs_f64() * 1000.0 / n as f64;
            let t = Instant::now();
            let mut dim = 0;
            for i in 0..n * 10 {
                let (_, s, m) = oggetti_db(i, pos, 0, 16 << 10);
                dim = s.len();
                std::hint::black_box(serde_json::to_vec(&m)?);
            }
            let ogg_us = t.elapsed().as_secs_f64() * 1e6 / (n * 10) as f64;
            scrivi_riga(&a.out, &json!({"prova": "cpu_creazione", "pbkdf2_15000_ms": pbkdf2_ms, "oggetti_us": ogg_us, "dim_scatola": dim}));
        }
        C::Http { da, ritmo, secondi, iter, max_in_volo } => {
            let st = Store::minio(&a.bucket)?;
            let fatti = Arc::new(AtomicU64::new(0));
            let errori = Arc::new(AtomicU64::new(0));
            let in_volo = Arc::new(AtomicU64::new(0));
            let saltati = Arc::new(AtomicU64::new(0));
            let lat = Arc::new(parking_lot::Mutex::new(hist()));
            let c0 = st.c.leggi();
            let cpu0 = cpu_s();
            let t0 = Instant::now();
            let tot = ritmo * secondi;
            let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / ritmo));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut hs = Vec::new();
            for k in 0..tot {
                tick.tick().await;
                if in_volo.load(Relaxed) >= max_in_volo {
                    // il banco non regge il ritmo: si conta e non si accumula una coda infinita
                    saltati.fetch_add(1, Relaxed);
                    continue;
                }
                in_volo.fetch_add(1, Relaxed);
                let (st, fatti, errori, in_volo, lat) = (st.clone(), fatti.clone(), errori.clone(), in_volo.clone(), lat.clone());
                let i = da + k;
                hs.push(tokio::spawn(async move {
                    let t = Instant::now();
                    let r: anyhow::Result<()> = async {
                        let (id, sst, m) = tokio::task::spawn_blocking(move || oggetti_db(i, pos, iter, 16 << 10)).await?;
                        let g = db_hex(&db_id(i));
                        st.put(&manifest::path_sst(&g, &id), sst).await?;
                        match st.put_cond(&manifest::path_manifest(&g), Bytes::from(serde_json::to_vec(&m)?), None).await {
                            Cas::Ok(_) => Ok(()),
                            x => anyhow::bail!("foglio: {x:?}"),
                        }
                    }
                    .await;
                    match r {
                        Ok(()) => {
                            fatti.fetch_add(1, Relaxed);
                            let _ = lat.lock().record(t.elapsed().as_micros() as u64);
                        }
                        Err(e) => {
                            if errori.fetch_add(1, Relaxed) < 5 {
                                eprintln!("creazione {i}: {e:#}");
                            }
                        }
                    }
                    in_volo.fetch_sub(1, Relaxed);
                }));
            }
            let t_gen = t0.elapsed().as_secs_f64();
            for h in hs {
                let _ = h.await;
            }
            let dt = t0.elapsed().as_secs_f64();
            let c = st.c.leggi().meno(&c0);
            let n = fatti.load(Relaxed);
            scrivi_riga(
                &a.out,
                &json!({"prova": "creazione_http", "ritmo_chiesto": ritmo, "secondi": secondi, "iter_scram": iter, "creati": n, "errori": errori.load(Relaxed), "saltati_banco_pieno": saltati.load(Relaxed),
                        "durata_s": dt, "durata_generazione_s": t_gen, "ritmo_ottenuto": n as f64 / dt, "latenza": perc(&lat.lock()),
                        "richieste": c, "put_per_db": (c.put + c.put_cond) as f64 / n.max(1) as f64, "list": c.list, "get": c.get,
                        "cpu_processo_s": cpu_s() - cpu0, "cpu_ms_per_db": (cpu_s() - cpu0) * 1000.0 / n.max(1) as f64}),
            );
        }
    }
    Ok(())
}
