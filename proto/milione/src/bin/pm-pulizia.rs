//! Pulizia delle scatole orfane (GC) su un milione di database, contata con un inventario S3 simulato.
//!
//! - `sporca`: mette orfani noti sul disco del finto S3: scatole vecchie non nel foglio (scarico morto dopo il PUT
//!   della scatola), scatole giovani non nel foglio (PUT in volo: non vanno toccate), creazioni morte prima del foglio
//!   (vecchie: da togliere; giovani: da lasciare), scatole in `da_cancellare` (le toglie chi scarica, non la pulizia);
//! - `inventario`: fa quello che fa S3 Inventory (un elenco di tutto il bucket una volta al giorno, fuori dalle richieste
//!   del cliente): un CSV `chiave,byte,ultima modifica` scritto come oggetto nel bucket;
//! - `giro`: la pulizia nuova. Legge l'inventario (GET a pezzi da 16 MB), trova i database sospetti (più di una
//!   scatola, o scatole senza foglio) e solo per loro fa un GET del foglio e i DELETE. Nessun LIST;
//! - `verifica`: controlla sul disco che nessuna scatola di un foglio sia sparita, che gli orfani vecchi non ci siano
//!   più e che quelli giovani e quelli in `da_cancellare` ci siano ancora;
//! - `vecchio`: la pulizia di oggi di proto-lettura (`Worker::orfani`: GET del foglio, LIST del prefisso, PUT del
//!   foglio per ogni database) su un campione, per contare le sue richieste per database.

use clap::{Parser, Subcommand};
use lettura::iface::*;
use lettura::manifest::{self, DaCancellare, Manifest};
use lettura::registro::RegistroSim;
use lettura::scarico::{CfgScarico, Worker};
use lettura::store::Store;
use milione::*;
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

#[derive(Parser)]
struct A {
    #[command(subcommand)]
    c: C,
    #[arg(long, default_value = "milione")]
    bucket: String,
    #[arg(long, default_value = "/home/claude/s3disco")]
    root: PathBuf,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Subcommand)]
enum C {
    Sporca {
        #[arg(long)]
        n: u64,
    },
    Inventario,
    Giro {
        #[arg(long, default_value_t = 300_000)]
        eta_ms: u64,
        #[arg(long, default_value_t = 64)]
        paralleli: usize,
    },
    Verifica {
        #[arg(long)]
        n: u64,
    },
    Vecchio {
        #[arg(long, default_value_t = 10_000)]
        quanti: u64,
        /// primo database del campione
        #[arg(long, default_value_t = 0)]
        da: u64,
    },
}

const INV: &str = "_inventario/inventario.csv";

fn invecchia(p: &std::path::Path, secondi: u64) -> anyhow::Result<()> {
    let f = std::fs::File::options().write(true).open(p)?;
    f.set_modified(SystemTime::now() - Duration::from_secs(secondi))?;
    Ok(())
}

/// Orfani messi da `sporca` (stessa regola in `verifica`).
fn ruolo(i: u64) -> Option<&'static str> {
    if i % 100 == 7 {
        Some("vecchio")
    } else if i % 1000 == 13 {
        Some("giovane")
    } else if i % 2000 == 21 {
        Some("da_cancellare")
    } else {
        None
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let b = a.bucket.clone();
    match a.c {
        C::Sporca { n } => {
            let mut conti: BTreeMap<&str, u64> = BTreeMap::new();
            let pos = pos_make(now_ms() - 60_000, 0);
            for i in 0..n {
                let Some(r) = ruolo(i) else { continue };
                let g = db_hex(&db_id(i));
                let (_, sst, _) = oggetti_db(i, pos, 0, 16 << 10);
                let id = format!("orfano-{r}-{i}");
                let p = file_di(&a.root, &b, &manifest::path_sst(&g, &id));
                scrivi_file(&p, &sst)?;
                match r {
                    "vecchio" => invecchia(&p, 7200)?,
                    "da_cancellare" => {
                        invecchia(&p, 7200)?;
                        let pm = file_di(&a.root, &b, &manifest::path_manifest(&g));
                        let mut m: Manifest = serde_json::from_slice(&std::fs::read(&pm)?)?;
                        m.da_cancellare.push(DaCancellare { id, dal_ms: now_ms() });
                        scrivi_file(&pm, &serde_json::to_vec(&m)?)?;
                    }
                    _ => {}
                }
                *conti.entry(r).or_default() += 1;
            }
            // creazioni morte prima del foglio: database nuovi oltre n, solo la scatola
            for k in 0..100u64 {
                let i = n + 1_000_000 + k;
                let g = db_hex(&db_id(i));
                let (id, sst, _) = oggetti_db(i, pos, 0, 16 << 10);
                let p = file_di(&a.root, &b, &manifest::path_sst(&g, &id));
                scrivi_file(&p, &sst)?;
                if k % 2 == 0 {
                    invecchia(&p, 7200)?;
                    *conti.entry("creazione_morta_vecchia").or_default() += 1;
                } else {
                    *conti.entry("creazione_in_volo_giovane").or_default() += 1;
                }
            }
            scrivi_riga(&a.out, &json!({"prova": "sporca", "n": n, "messi": conti}));
        }
        C::Inventario => {
            let t = Instant::now();
            let base = a.root.join(&b);
            let mut out = String::new();
            let mut n = 0u64;
            let mut byte = 0u64;
            let mut teste: Vec<_> = std::fs::read_dir(&base)?.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect();
            teste.sort();
            for t in teste {
                if t.starts_with('_') {
                    continue;
                }
                for e in std::fs::read_dir(base.join(&t))? {
                    let e = e?;
                    let f = e.file_name().to_string_lossy().to_string();
                    if f.contains(".tmp") {
                        continue;
                    }
                    let md = e.metadata()?;
                    let mt = md.modified()?.duration_since(SystemTime::UNIX_EPOCH)?.as_millis();
                    out.push_str(&format!("{},{},{}\n", chiave_di_file(&t, &f), md.len(), mt));
                    n += 1;
                    byte += md.len();
                }
            }
            scrivi_file(&file_di(&a.root, &b, INV), out.as_bytes())?;
            scrivi_riga(
                &a.out,
                &json!({"prova": "inventario", "oggetti": n, "byte_oggetti": byte, "byte_inventario": out.len(), "secondi": t.elapsed().as_secs_f64(),
                        "costo_usd_stimato": n as f64 * 0.0025 / 1e6}),
            );
        }
        C::Giro { eta_ms, paralleli } => {
            let st = Store::minio(&b)?;
            let c0 = st.c.leggi();
            let t0 = Instant::now();
            // 1. l'inventario, a pezzi da 16 MB (come i file CSV di S3 Inventory)
            let dim = std::fs::metadata(file_di(&a.root, &b, INV))?.len();
            let mut testo = Vec::with_capacity(dim as usize);
            let mut off = 0u64;
            let mut pezzi = 0;
            while off < dim {
                let e = (off + (16 << 20)).min(dim);
                let x = st.get_range(INV, off, e).await?.ok_or_else(|| anyhow::anyhow!("inventario assente"))?;
                testo.extend_from_slice(&x);
                off = e;
                pezzi += 1;
            }
            let t_inv = t0.elapsed().as_secs_f64();
            // 2. per database: foglio presente? scatole con età
            struct Db0 {
                foglio: bool,
                sst: Vec<(String, u64)>,
            }
            let ora = now_ms();
            let mut dbs: BTreeMap<String, Db0> = BTreeMap::new();
            let mut righe = 0u64;
            for l in std::str::from_utf8(&testo)?.lines() {
                righe += 1;
                let mut it = l.split(',');
                let (Some(k), Some(_), Some(mt)) = (it.next(), it.next(), it.next()) else { continue };
                let mt: u64 = mt.parse()?;
                let mut p = k.split('/');
                let (Some(_h), Some(g)) = (p.next(), p.next()) else { continue };
                let e = dbs.entry(g.to_string()).or_insert(Db0 { foglio: false, sst: Vec::new() });
                match (p.next(), p.next()) {
                    (Some("manifest"), None) => e.foglio = true,
                    (Some("sst"), Some(f)) => e.sst.push((f.trim_end_matches(".sst").to_string(), mt)),
                    _ => {}
                }
            }
            let sospetti: Vec<(String, Vec<String>)> = dbs
                .iter()
                .filter(|(_, d)| (d.sst.len() > 1 || !d.foglio) && d.sst.iter().any(|(_, mt)| ora.saturating_sub(*mt) >= eta_ms))
                .map(|(g, d)| (g.clone(), d.sst.iter().filter(|(_, mt)| ora.saturating_sub(*mt) >= eta_ms).map(|(i, _)| i.clone()).collect()))
                .collect();
            let n_db = dbs.len();
            drop(dbs);
            drop(testo);
            let t_sosp = t0.elapsed().as_secs_f64();
            // 3. GET del foglio dei soli sospetti, DELETE degli orfani vecchi
            use futures::StreamExt;
            let res: Vec<anyhow::Result<u64>> = futures::stream::iter(sospetti.clone())
                .map(|(g, vecchie)| {
                    let st = st.clone();
                    async move {
                        let noti: HashSet<String> = match manifest::carica(&st, &g).await? {
                            Some((m, _)) => m.run.iter().map(|r| r.id.clone()).chain(m.da_cancellare.iter().map(|d| d.id.clone())).collect(),
                            None => HashSet::new(),
                        };
                        let mut n = 0;
                        for id in vecchie {
                            if !noti.contains(&id) {
                                st.delete(&manifest::path_sst(&g, &id)).await?;
                                n += 1;
                            }
                        }
                        Ok(n)
                    }
                })
                .buffer_unordered(paralleli)
                .collect()
                .await;
            let mut tolti = 0;
            for r in res {
                tolti += r?;
            }
            let c = st.c.leggi().meno(&c0);
            let dt = t0.elapsed().as_secs_f64();
            let costo = c.classe_put() as f64 * 0.005 / 1000.0 + c.classe_get() as f64 * 0.0004 / 1000.0;
            scrivi_riga(
                &a.out,
                &json!({"prova": "giro_pulizia_inventario", "database_nell_inventario": n_db, "righe": righe, "pezzi_inventario": pezzi, "sospetti": sospetti.len(), "scatole_tolte": tolti,
                        "richieste": c, "list": c.list, "list_ogni_1000_db": c.list as f64 * 1000.0 / n_db.max(1) as f64, "get_fogli": c.get, "delete": c.delete,
                        "costo_richieste_usd": costo, "secondi": dt, "secondi_inventario": t_inv, "secondi_sospetti": t_sosp - t_inv}),
            );
        }
        C::Verifica { n } => {
            let t = Instant::now();
            let (mut fogli, mut scatole_mancanti, mut vecchi_rimasti, mut giovani, mut da_canc, mut morte_vecchie, mut in_volo) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
            for i in 0..n {
                let g = db_hex(&db_id(i));
                let pm = file_di(&a.root, &b, &manifest::path_manifest(&g));
                let m: Manifest = serde_json::from_slice(&std::fs::read(&pm)?)?;
                fogli += 1;
                for r in &m.run {
                    if !file_di(&a.root, &b, &manifest::path_sst(&g, &r.id)).exists() {
                        scatole_mancanti += 1;
                    }
                }
                if let Some(r) = ruolo(i) {
                    let ok = file_di(&a.root, &b, &manifest::path_sst(&g, &format!("orfano-{r}-{i}"))).exists();
                    match (r, ok) {
                        ("vecchio", true) => vecchi_rimasti += 1,
                        ("giovane", true) => giovani += 1,
                        ("da_cancellare", true) => da_canc += 1,
                        _ => {}
                    }
                }
            }
            for k in 0..100u64 {
                let i = n + 1_000_000 + k;
                let g = db_hex(&db_id(i));
                let pref = format!("{}~sst~", g);
                let dir = file_di(&a.root, &b, &manifest::path_manifest(&g)).parent().unwrap().to_path_buf();
                let c = std::fs::read_dir(&dir)?.filter_map(|e| e.ok()).filter(|e| e.file_name().to_string_lossy().starts_with(&pref)).count() as u64;
                if k % 2 == 0 {
                    morte_vecchie += c;
                } else {
                    in_volo += c;
                }
            }
            scrivi_riga(
                &a.out,
                &json!({"prova": "verifica_pulizia", "fogli": fogli, "scatole_dei_fogli_mancanti": scatole_mancanti, "orfani_vecchi_rimasti": vecchi_rimasti,
                        "orfani_giovani_rimasti": giovani, "da_cancellare_rimasti": da_canc, "creazioni_morte_vecchie_rimaste": morte_vecchie, "creazioni_in_volo_rimaste": in_volo,
                        "secondi": t.elapsed().as_secs_f64()}),
            );
        }
        C::Vecchio { quanti, da } => {
            let st = Store::minio(&b)?;
            let reg = RegistroSim::new(60_000, None)?;
            let w = Worker::new(reg, st.clone(), CfgScarico::default());
            let c0 = st.c.leggi();
            let t = Instant::now();
            let mut tolti = 0;
            for i in da..da + quanti {
                tolti += w.orfani(&db_hex(&db_id(i))).await?;
            }
            let c = st.c.leggi().meno(&c0);
            let costo = c.classe_put() as f64 * 0.005 / 1000.0 + c.classe_get() as f64 * 0.0004 / 1000.0;
            scrivi_riga(
                &a.out,
                &json!({"prova": "giro_pulizia_vecchio", "database": quanti, "tolti": tolti, "richieste": c, "list_per_db": c.list as f64 / quanti as f64,
                        "get_per_db": c.get as f64 / quanti as f64, "put_per_db": (c.put + c.put_cond) as f64 / quanti as f64,
                        "costo_usd_per_milione_di_db": costo / quanti as f64 * 1e6, "secondi": t.elapsed().as_secs_f64()}),
            );
        }
    }
    Ok(())
}
