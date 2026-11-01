//! N3 (misura-lettore): µs per lettura puntuale del lettore di proto-lettura con i blocchi in RAM, senza il giro TCP,
//! su 1 core, contro `Db::get` di SlateDB 0.17 sugli stessi dati.
//!
//! Tutto in un processo con un runtime tokio a un thread (`current_thread`), da lanciare con `taskset -c 0`.
//! Lo store è `object_store::memory::InMemory` per entrambi: dopo il riscaldamento nessuna lettura tocca lo store
//! (lo si controlla con i contatori), quindi si misura solo il lavoro di CPU della lettura.
//!
//! Dati: `docs` documenti da `vsize` byte, chiavi `doc/c/{id}` come nel motore (`lettura::iface::doc_key`). I primi 8
//! byte del valore sono l'id: ogni lettura controlla che il valore sia quello giusto.
//!
//! Lettore nostro, tre misure:
//! - `nostro_get`: `Lettore::get` (finestra del registro simulato nello stesso processo, poi SST) = il percorso intero;
//! - `nostro_sst`: `Lettore::sst_get` (solo SST: manifest in cache, meta, bloom, blocco dalla cache, ricerca nel blocco);
//! - `nostro_nudo`: lo stesso lavoro chiamato a mano senza async (meta già in mano, cache dei blocchi, `block_lookup`),
//!   per vedere quanto costa l'involucro.
//! SlateDB: `slate_get` = `Db::get` con le impostazioni del motore (`writer_settings`, cache foyer condivisa) dopo una
//! compattazione completa e la riapertura senza compattatore, come il writer del motore.
//!
//! I giri alternano le misure (A, B, C, D, A, B, ...) e si riporta la mediana dei giri.

use bytes::Bytes;
use clap::Parser;
use lettura::cache::Cache;
use lettura::iface::*;
use lettura::lettura::{CfgLettura, Lettore};
use lettura::manifest::{self, Manifest, Run};
use lettura::registro::RegistroSim;
use lettura::sst::{self, Entry, Lookup};
use lettura::store::{Contatori, Store};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use rand::{Rng, SeedableRng};
use serde_json::json;
use slatedb::config::{FlushOptions, FlushType, PutOptions, Settings, WriteOptions};
use slatedb::Db;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
struct A {
    #[arg(long, default_value_t = 200_000)]
    docs: u64,
    #[arg(long, default_value_t = 1000)]
    vsize: usize,
    /// dimensione dei blocchi dei nostri SST (SlateDB: 4 KiB, il suo default, se non si dà --slate-blocco)
    #[arg(long, default_value_t = 4096)]
    blocco: usize,
    /// 4096 | 16384 (blocchi di SlateDB)
    #[arg(long, default_value_t = 4096)]
    slate_blocco: usize,
    /// run nei nostri SST: 1 = compattato; n > 1 = base + (n-1) run nuove, ognuna con il `frazione` delle chiavi
    #[arg(long, default_value_t = 1)]
    run: usize,
    #[arg(long, default_value_t = 0.02)]
    frazione: f64,
    #[arg(long, default_value_t = 200_000)]
    letture: u64,
    #[arg(long, default_value_t = 7)]
    giri: usize,
    /// misure da fare, separate da virgola
    #[arg(long, default_value = "nostro_get,nostro_sst,nostro_nudo,slate_get")]
    misure: String,
    /// cache dei blocchi di SlateDB (MiB) e dei metadati (MiB)
    #[arg(long, default_value_t = 2048)]
    slate_block_mb: u64,
    #[arg(long, default_value_t = 512)]
    slate_meta_mb: u64,
    /// memtable di SlateDB come nel motore (8 MiB, `--memtable-max-mb`)
    #[arg(long, default_value_t = 8)]
    memtable_mb: usize,
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

fn valore(id: u64, versione: u8, n: usize) -> Bytes {
    let mut r = rand::rngs::SmallRng::seed_from_u64(id.wrapping_mul(31).wrapping_add(versione as u64));
    let mut v = vec![0u8; n];
    r.fill(&mut v[..]);
    v[..8].copy_from_slice(&id.to_le_bytes());
    v[8] = versione;
    Bytes::from(v)
}

fn controlla(id: u64, v: &Option<Bytes>, vsize: usize) {
    let v = v.as_ref().unwrap_or_else(|| panic!("chiave {id} assente"));
    assert_eq!(v.len(), vsize, "lunghezza sbagliata per {id}");
    assert_eq!(u64::from_le_bytes(v[..8].try_into().unwrap()), id, "valore sbagliato per {id}");
}

/// Versione attesa di ogni chiave dopo le run nuove (0 = base).
fn versioni(a: &A) -> Vec<u8> {
    let mut ver = vec![0u8; a.docs as usize];
    let mut r = rand::rngs::SmallRng::seed_from_u64(7);
    for j in 1..a.run {
        for id in 0..a.docs {
            if r.gen_bool(a.frazione) {
                ver[id as usize] = j as u8;
            }
        }
    }
    ver
}

struct Nostro {
    lettore: Arc<Lettore>,
    db: Db_,
    s: Pos,
    potato: Pos,
}
type Db_ = lettura::iface::Db;

async fn prepara_nostro(a: &A, ver: &[u8]) -> anyhow::Result<Nostro> {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let st = Store { inner: mem.clone(), c: Arc::new(Contatori::default()), bucket: "mem".into(), ritardo_get: Duration::ZERO };
    let db = db_n(1);
    let g = db_hex(&db);
    let pos0 = pos_make(1_700_000_000_000, 0);
    let mut m = Manifest { versione: 1, ..Default::default() };
    // dalla run più vecchia (base, j = 0) alla più nuova; nel manifest la più nuova sta in testa
    for j in 0..a.run {
        let pos = pos0 + j as u64;
        let mut es = Vec::new();
        for id in 0..a.docs {
            let in_run = j == 0 || run_ha(a, id, j);
            if in_run {
                es.push(Entry { key: sst::full_key(&db, &doc_key("c", id)), pos, kind: Kind::Put, val: valore(id, j as u8, a.vsize) });
            }
        }
        es.sort_by(|x, y| x.key.cmp(&y.key));
        let b = sst::build(es, a.blocco);
        let id = format!("{:016x}-{:08x}", b.max, j);
        let data = Bytes::from(b.data);
        mem.put(&Path::from(manifest::path_sst(&g, &id)), PutPayload::from(data.clone())).await?;
        m.run.insert(0, Run { id, min: b.min, max: b.max, dim: data.len() as u64, meta_off: b.meta_off, meta_len: b.meta_len, voci: b.voci });
    }
    let potato = pos0 + a.run as u64;
    m.scaricato.insert(g.clone(), potato);
    mem.put(&Path::from(manifest::path_manifest(&g)), PutPayload::from(serde_json::to_vec(&m)?)).await?;
    let reg = RegistroSim::new(60_000, None)?;
    reg.imposta_scaricato(db, potato, None);
    reg.avanza(potato);
    let lettore = Lettore::new(reg, st, Cache::ram(8 << 30), CfgLettura::default());
    Ok(Nostro { lettore, db, s: potato, potato })
}

/// La chiave `id` è stata riscritta nella run `j`? (Ricostruisce le estrazioni di `versioni`.)
fn run_ha(a: &A, id: u64, j: usize) -> bool {
    // ricalcolo deterministico: stessa sequenza di `versioni`
    thread_local! { static TAB: std::cell::RefCell<Option<(u64, usize, Vec<Vec<bool>>)>> = const { std::cell::RefCell::new(None) }; }
    TAB.with(|t| {
        let mut t = t.borrow_mut();
        if t.as_ref().map(|x| x.0 != a.docs || x.1 != a.run).unwrap_or(true) {
            let mut r = rand::rngs::SmallRng::seed_from_u64(7);
            let mut tab = vec![vec![false; a.docs as usize]; a.run];
            for jj in 1..a.run {
                for i in 0..a.docs {
                    tab[jj][i as usize] = r.gen_bool(a.frazione);
                }
            }
            *t = Some((a.docs, a.run, tab));
        }
        t.as_ref().unwrap().2[j][id as usize]
    })
}

async fn prepara_slate(a: &A, ver: &[u8]) -> anyhow::Result<(Db, serde_json::Value)> {
    use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut s = Settings::default();
    s.l0_sst_size_bytes = a.memtable_mb << 20;
    s.max_unflushed_bytes = (s.l0_sst_size_bytes * 4).max(16 << 20);
    s.garbage_collector_options = None;
    let bs = match a.slate_blocco {
        1024 => slatedb::config::SstBlockSize::Block1Kib,
        2048 => slatedb::config::SstBlockSize::Block2Kib,
        4096 => slatedb::config::SstBlockSize::Block4Kib,
        8192 => slatedb::config::SstBlockSize::Block8Kib,
        16384 => slatedb::config::SstBlockSize::Block16Kib,
        x => anyhow::bail!("blocco SlateDB {x} non previsto"),
    };
    let t0 = Instant::now();
    // caricamento con il compattatore acceso (come il motore fa compattare dopo), poi chiusura
    {
        let db = Db::builder("tenant", mem.clone()).with_settings(s.clone()).with_sst_block_size(bs).build().await?;
        let wo = WriteOptions::default();
        for j in 0..a.run {
            let mut n = 0;
            let mut batch = slatedb::WriteBatch::new();
            for id in 0..a.docs {
                let in_run = j == 0 || run_ha(a, id, j);
                if !in_run {
                    continue;
                }
                batch.put(doc_key("c", id), valore(id, j as u8, a.vsize));
                n += 1;
                if n % 500 == 0 {
                    db.write_with_options(std::mem::replace(&mut batch, slatedb::WriteBatch::new()), &wo).await?;
                }
            }
            if n % 500 != 0 {
                db.write_with_options(batch, &wo).await?;
            }
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
        }
        // attende che il compattatore si fermi (stato del manifest uguale per 5 s; L0 può restare non vuoto: il
        // compattatore di default fonde L0 solo da un numero minimo di SST in su)
        let mut fermo = 0;
        let mut ultimo = (usize::MAX, usize::MAX);
        loop {
            let m = db.manifest();
            let stato = (m.l0().len(), m.compacted().len());
            if stato == ultimo {
                fermo += 1;
                if fermo >= 10 {
                    break;
                }
            } else {
                fermo = 0;
            }
            ultimo = stato;
            eprintln!("compattazione: l0 {} sorted run {}", stato.0, stato.1);
            tokio::time::sleep(Duration::from_millis(500)).await;
            anyhow::ensure!(t0.elapsed() < Duration::from_secs(1800), "compattazione di SlateDB non finita in 30 minuti");
        }
        db.close().await?;
    }
    let _ = PutOptions::default();
    // riapertura come il writer del motore: niente compattatore, cache foyer condivisa
    let mut s2 = s.clone();
    s2.compactor_options = None;
    let block: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: a.slate_block_mb << 20, ..Default::default() }));
    let meta: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: a.slate_meta_mb << 20, ..Default::default() }));
    let split: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(slatedb::db_cache::SplitCache::new().with_block_cache(Some(block)).with_meta_cache(Some(meta)).build());
    let db = Db::builder("tenant", mem.clone()).with_settings(s2).with_sst_block_size(bs).with_db_cache(split, 1).build().await?;
    let m = db.manifest();
    let srs: Vec<usize> = m.compacted().iter().map(|r| r.sst_views().len()).collect();
    let info = json!({"l0": m.l0().len(), "sorted_run": m.compacted().len(), "sst_per_sorted_run": srs, "carico_s": t0.elapsed().as_secs_f64()});
    Ok((db, info))
}

fn chiavi(a: &A, seme: u64) -> Vec<u64> {
    let mut r = rand::rngs::SmallRng::seed_from_u64(seme);
    (0..a.letture).map(|_| r.gen_range(0..a.docs)).collect()
}

/// Lavoro del lettore senza async e senza manifest: meta già in mano, per ogni run bloom + indice + blocco dalla cache
/// + ricerca nel blocco (lo stesso ordine di `Lettore::sst_get`).
struct Nudo {
    run: Vec<(Arc<str>, Arc<sst::SstMeta>, Pos)>,
    cache: Arc<Cache>,
    db: Db_,
}

impl Nudo {
    fn get(&self, key: &[u8], s: Pos) -> Option<Bytes> {
        let fk = sst::full_key(&self.db, key);
        for (id, meta, min) in &self.run {
            if *min > s || meta.db_blocks(&self.db).is_none() || !meta.bloom.may_contain(&fk) {
                continue;
            }
            let Some(i) = meta.find_block(&fk) else { continue };
            let b = self.cache.blocco(id, i as u32).expect("blocco non in cache");
            match sst::block_lookup(&b, &fk, s).unwrap() {
                Lookup::NotFound => continue,
                Lookup::Found { val, .. } => return val,
                Lookup::Delta { .. } => unreachable!(),
            }
        }
        None
    }
}

fn mediana(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(corri(a))
}

async fn corri(a: A) -> anyhow::Result<()> {
    let misure: Vec<String> = a.misure.split(',').map(|x| x.trim().to_string()).collect();
    let ver = versioni(&a);
    let t0 = Instant::now();
    let n = prepara_nostro(&a, &ver).await?;
    eprintln!("nostro pronto in {:.1}s", t0.elapsed().as_secs_f64());
    // riscaldamento: ogni chiave letta una volta (manifest, meta e tutti i blocchi in cache)
    for id in 0..a.docs {
        let v = n.lettore.get(n.db, doc_key("c", id), n.s).await?;
        controlla(id, &v, a.vsize);
        assert_eq!(v.as_ref().unwrap()[8], ver[id as usize], "versione sbagliata per {id}");
    }
    let st0 = n.lettore.stat();
    let get0 = n.lettore.st.c.leggi();
    // il nudo prende i meta dalla cache
    let g = db_hex(&n.db);
    let man = manifest::carica(&n.lettore.st, &g).await?.unwrap().0;
    let mut runs = Vec::new();
    for r in &man.run {
        let id: Arc<str> = Arc::from(format!("{g}.{}", r.id).as_str());
        let meta = n.lettore.cache.meta(&id).expect("meta non in cache");
        runs.push((id, meta, r.min));
    }
    let nudo = Nudo { run: runs, cache: n.lettore.cache.clone(), db: n.db };
    let get0 = {
        let _ = get0;
        n.lettore.st.c.leggi()
    };

    let slate = if misure.iter().any(|m| m.starts_with("slate")) {
        let t1 = Instant::now();
        let (db, info) = prepara_slate(&a, &ver).await?;
        eprintln!("slatedb pronto in {:.1}s: {info}", t1.elapsed().as_secs_f64());
        for _ in 0..2 {
            for id in 0..a.docs {
                let v = db.get(doc_key("c", id)).await?;
                controlla(id, &v, a.vsize);
                assert_eq!(v.as_ref().unwrap()[8], ver[id as usize], "versione sbagliata (slatedb) per {id}");
            }
        }
        Some((db, info))
    } else {
        None
    };

    // le chiavi già codificate, come le riceverebbe il lettore dallo strato MongoDB
    let mut risultati: std::collections::BTreeMap<String, Vec<f64>> = Default::default();
    let mut p99: std::collections::BTreeMap<String, Vec<f64>> = Default::default();
    for giro in 0..a.giri {
        let ks = chiavi(&a, 1000 + giro as u64);
        let kb: Vec<Bytes> = ks.iter().map(|id| doc_key("c", *id)).collect();
        for m in &misure {
            let mut h = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
            let t = Instant::now();
            match m.as_str() {
                "nostro_get" => {
                    for (i, k) in kb.iter().enumerate() {
                        let ti = Instant::now();
                        let v = n.lettore.get(n.db, k.clone(), n.s).await?;
                        h.record(ti.elapsed().as_nanos() as u64).ok();
                        controlla(ks[i], &v, a.vsize);
                    }
                }
                "nostro_sst" => {
                    for (i, k) in kb.iter().enumerate() {
                        let ti = Instant::now();
                        let v = n.lettore.sst_get(n.db, k, n.s, n.potato).await?;
                        h.record(ti.elapsed().as_nanos() as u64).ok();
                        controlla(ks[i], &v, a.vsize);
                    }
                }
                "nostro_nudo" => {
                    for (i, k) in kb.iter().enumerate() {
                        let ti = Instant::now();
                        let v = nudo.get(k, n.s);
                        h.record(ti.elapsed().as_nanos() as u64).ok();
                        controlla(ks[i], &v, a.vsize);
                    }
                }
                "slate_get" => {
                    let db = &slate.as_ref().unwrap().0;
                    for (i, k) in kb.iter().enumerate() {
                        let ti = Instant::now();
                        let v = db.get(k).await?;
                        h.record(ti.elapsed().as_nanos() as u64).ok();
                        controlla(ks[i], &v, a.vsize);
                    }
                }
                x => anyhow::bail!("misura sconosciuta {x}"),
            }
            let us = t.elapsed().as_secs_f64() * 1e6 / a.letture as f64;
            eprintln!("giro {giro} {m}: {us:.2} µs/lettura (p50 {:.2}, p99 {:.2})", h.value_at_quantile(0.5) as f64 / 1e3, h.value_at_quantile(0.99) as f64 / 1e3);
            risultati.entry(m.clone()).or_default().push(us);
            p99.entry(m.clone()).or_default().push(h.value_at_quantile(0.99) as f64 / 1e3);
        }
    }
    let st1 = n.lettore.stat();
    let get1 = n.lettore.st.c.leggi();
    let store_letture = (get1.get - get0.get) + (get1.get_range - get0.get_range);
    let mut sintesi = serde_json::Map::new();
    for (m, v) in &risultati {
        let mut v2 = v.clone();
        let mut p = p99[m].clone();
        sintesi.insert(m.clone(), json!({"us_mediana": mediana(&mut v2), "us_giri": v, "p99_us_mediana": mediana(&mut p)}));
    }
    let out = json!({
        "parametri": format!("{a:?}"),
        "risultati": sintesi,
        "slatedb": slate.as_ref().map(|x| x.1.clone()),
        "nostro": {
            "run": man.run.len(),
            "blocchi_per_run": nudo.run.iter().map(|r| r.1.blocks.len()).collect::<Vec<_>>(),
            "get_store_durante_misura": store_letture,
            "get_manifest_durante_misura": st1.get_manifest - st0.get_manifest,
            "cache_miss_durante_misura": st1.cache_miss - st0.cache_miss,
        },
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    if let Some(p) = &a.out {
        std::fs::write(p, serde_json::to_string_pretty(&out)?)?;
    }
    if let Some((db, _)) = slate {
        db.close().await?;
    }
    Ok(())
}
