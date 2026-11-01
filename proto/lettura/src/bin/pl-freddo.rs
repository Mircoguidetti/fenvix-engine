//! Passo 4: prima risposta di un database mai aperto. Per ogni configurazione (dimensione del database, numero di run
//! nel manifest) prepara dei database su MinIO e, per ciascuno, crea un nodo d'ingresso nuovo (cache vuota, nessun
//! manifest), fa una lettura puntuale e conta le richieste S3 e il tempo. Le richieste dipendenti (manifest -> meta ->
//! blocco) sono in serie: la stima su S3 Standard è (GET in serie) x 20-40 ms.

use clap::Parser;
use lettura::cache::Cache;
use lettura::carico::{precarica, put_req, valore, COLL};
use lettura::iface::*;
use lettura::lettura::{CfgLettura, Lettore};
use lettura::manifest::{gruppo_di, Raggruppa};
use lettura::registro::RegistroSim;
use lettura::scarico::{CfgScarico, Worker};
use lettura::store::Store;
use rand::Rng;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
struct A {
    #[arg(long, default_value = "freddo")]
    bucket: String,
    /// configurazioni: documenti_per_db:run_in_più:numero_di_db
    #[arg(long, default_value = "100:0:30,100:2:30,10000:0:20,10000:2:20,100000:0:5,100000:2:5")]
    conf: String,
    #[arg(long, default_value_t = 1024)]
    vsize: usize,
    /// ritardo artificiale per GET (ms) per una seconda misura dei tempi
    #[arg(long, default_value_t = 30)]
    ritardo_ms: u64,
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let st_pre = Store::minio(&a.bucket)?;
    // finestra 0: i record scaricati escono subito dal registro, la lettura va agli SST
    let reg = RegistroSim::new(0, None)?;
    let mut risultati = Vec::new();
    let mut base_db = 50_000u64;
    for c in a.conf.split(',') {
        let p: Vec<u64> = c.split(':').map(|x| x.parse().unwrap()).collect();
        let (docs, extra, n) = (p[0], p[1], p[2]);
        let dbs: Vec<Db> = (0..n).map(|i| db_n(base_db + i)).collect();
        base_db += 1000;
        let pos0 = pos_make(now_ms() - 1000, 0);
        precarica(&st_pre, &reg, Raggruppa::PerDb, &dbs, docs, a.vsize, 16 << 10, pos0).await?;
        if extra > 0 {
            let w = Worker::new(reg.clone(), st_pre.clone(), CfgScarico { compatta: false, ..Default::default() });
            let mut rng = rand::thread_rng();
            for _ in 0..extra {
                for d in &dbs {
                    for _ in 0..50 {
                        let id = rng.gen_range(0..docs);
                        reg.commit_sync(put_req(*d, doc_key(COLL, id), valore(&mut rng, a.vsize)));
                    }
                }
                let pend = reg.pendenti(1_000_000).await?;
                for p in pend {
                    w.scarica(&gruppo_di(&p.db, Raggruppa::PerDb), vec![p]).await?;
                }
            }
            reg.pota();
        }
        for prefetch in [false, true] {
            let mut reqs = Vec::new();
            let mut tempi = Vec::new();
            let mut seconde = Vec::new();
            let mut per_tipo = lettura::store::Conteggi::default();
            let mut tempi_rit = Vec::new();
            for d in &dbs {
                // con il ritardo artificiale: un nodo d'ingresso nuovo anche qui
                {
                    let mut st = Store::minio(&a.bucket)?;
                    st.ritardo_get = std::time::Duration::from_millis(a.ritardo_ms);
                    let lett = Lettore::new(reg.clone(), st, Cache::ram(1 << 30), CfgLettura { prefetch_meta: prefetch, ..Default::default() });
                    let id = rand::thread_rng().gen_range(0..docs);
                    let t = Instant::now();
                    lett.get(*d, doc_key(COLL, id), reg.posizione_confermata_sync()).await?;
                    tempi_rit.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                let st = Store::minio(&a.bucket)?;
                let lett = Lettore::new(reg.clone(), st.clone(), Cache::ram(1 << 30), CfgLettura { prefetch_meta: prefetch, ..Default::default() });
                let id = rand::thread_rng().gen_range(0..docs);
                let s = reg.posizione_confermata_sync();
                let t = Instant::now();
                let v = lett.get(*d, doc_key(COLL, id), s).await?;
                let dt = t.elapsed().as_secs_f64() * 1000.0;
                anyhow::ensure!(v.is_some(), "documento del precarico non trovato");
                let c = st.c.leggi();
                per_tipo = per_tipo.somma(&c);
                reqs.push(c.get + c.get_range + c.list + c.put + c.put_cond);
                tempi.push(dt);
                let id2 = rand::thread_rng().gen_range(0..docs);
                lett.get(*d, doc_key(COLL, id2), s).await?;
                let c2 = st.c.leggi().meno(&c);
                seconde.push(c2.get + c2.get_range);
            }
            reqs.sort();
            tempi.sort_by(|a, b| a.partial_cmp(b).unwrap());
            tempi_rit.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let runs = 1 + extra;
            // GET in serie: manifest, poi meta (in serie per run senza prefetch, insieme con prefetch), poi il blocco
            // se l'SST è grande (gli SST piccoli arrivano interi con il meta)
            let dim_db = docs * (a.vsize as u64 + 30);
            let grande = dim_db > (1 << 20);
            let in_serie_peggiore = if prefetch { 1 + 1 + grande as u64 } else { 1 + runs + grande as u64 };
            let r = json!({
                "documenti_per_db": docs, "byte_per_db_circa": dim_db, "run_nel_manifest": runs, "prefetch_meta": prefetch, "db_provati": n,
                "richieste_s3_prima_lettura": {"min": reqs[0], "max": reqs[reqs.len()-1], "media": reqs.iter().sum::<u64>() as f64 / reqs.len() as f64},
                "richieste_per_tipo_tutte_le_prime_letture": per_tipo,
                "richieste_s3_seconda_lettura_media": seconde.iter().sum::<u64>() as f64 / seconde.len() as f64,
                "ms_minio_locale": {"mediana": tempi[tempi.len()/2], "max": tempi[tempi.len()-1]},
                "get_in_serie_caso_peggiore": in_serie_peggiore,
                "ms_con_ritardo_artificiale": {"ritardo_per_get_ms": a.ritardo_ms, "mediana": tempi_rit[tempi_rit.len()/2], "max": tempi_rit[tempi_rit.len()-1]},
                "stima_s3_standard_ms": [in_serie_peggiore * 20, in_serie_peggiore * 40],
            });
            println!("{r}");
            risultati.push(r);
        }
    }
    let txt = serde_json::to_string_pretty(&risultati)?;
    if let Some(o) = a.out {
        std::fs::write(o, &txt)?;
    }
    let _ = Arc::new(());
    Ok(())
}
