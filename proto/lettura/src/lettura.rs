//! Lettura composta di un nodo d'ingresso senza stato: valore alla posizione S = versioni recenti dalla finestra del
//! registro, altrimenti SST elencati nel manifest (dalla cache o da S3).
//!
//! Regola di freschezza: se il registro risponde `fuori_finestra(potato_fino_a)`, il manifest usato deve avere
//! `scaricato[db] >= potato_fino_a` (tutto ciò che è uscito dalla finestra è negli SST elencati). Il manifest in cache
//! si rilegge solo quando questa condizione non vale: con lo scarico ogni 10 minuti costa un GET ogni 10 minuti per
//! database, non uno per lettura.
//!
//! Unità di trasferimento da S3 (scelta per contare poche GET):
//! - SST con dimensione <= `intero_max`: un GET dell'oggetto intero (meta + tutti i blocchi);
//! - altrimenti meta con un GET a intervallo (`meta_off`, `meta_len` dal manifest), poi, per un blocco mancante,
//!   l'intera fetta del database nell'SST se <= `fetta_max` (oggetti condivisi), altrimenti il solo blocco.

use crate::cache::Cache;
use crate::iface::*;
use crate::manifest::{self, gruppo_di, Manifest, Raggruppa, Run};
use crate::sst::{self, Lookup, SstMeta};
use crate::store::Store;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct CfgLettura {
    pub raggruppa: Raggruppa,
    pub intero_max: u64,
    pub fetta_max: u64,
    /// al primo uso di un manifest, chiede in parallelo i meta di tutte le run (lettura fredda: 3 GET in serie
    /// invece di 2 + numero di run)
    pub prefetch_meta: bool,
}

impl Default for CfgLettura {
    fn default() -> Self {
        CfgLettura { raggruppa: Raggruppa::PerDb, intero_max: 1 << 20, fetta_max: 256 << 10, prefetch_meta: false }
    }
}

#[derive(Default, Debug)]
pub struct StatLettura {
    pub letture: AtomicU64,
    pub da_finestra: AtomicU64,
    pub da_sst: AtomicU64,
    pub get_manifest: AtomicU64,
    pub get_intero: AtomicU64,
    pub get_meta: AtomicU64,
    pub get_fetta: AtomicU64,
    pub get_blocco: AtomicU64,
    pub run_sparite: AtomicU64,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct StatLetturaV {
    pub letture: u64,
    pub da_finestra: u64,
    pub da_sst: u64,
    pub get_manifest: u64,
    pub get_intero: u64,
    pub get_meta: u64,
    pub get_fetta: u64,
    pub get_blocco: u64,
    pub run_sparite: u64,
    pub cache_hit: u64,
    pub cache_miss: u64,
    pub cache_sfratti: u64,
    pub cache_usati: u64,
}

/// chiave di cache di una run: gruppo + id (gli id sono unici, il gruppo è una garanzia in più)
fn chiave_run(g: &str, run: &Run) -> Arc<str> {
    Arc::from(format!("{g}.{}", run.id).as_str())
}

struct ManCache {
    m: Arc<Manifest>,
}

pub struct Lettore {
    pub reg: Arc<dyn Registro>,
    pub st: Store,
    pub cache: Arc<Cache>,
    pub cfg: CfgLettura,
    pub stat: StatLettura,
    manifests: Mutex<HashMap<String, ManCache>>,
    voli: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Lettore {
    pub fn new(reg: Arc<dyn Registro>, st: Store, cache: Arc<Cache>, cfg: CfgLettura) -> Arc<Lettore> {
        Arc::new(Lettore { reg, st, cache, cfg, stat: Default::default(), manifests: Mutex::new(HashMap::new()), voli: Mutex::new(HashMap::new()) })
    }

    pub fn stat(&self) -> StatLetturaV {
        StatLetturaV {
            letture: self.stat.letture.load(Relaxed),
            da_finestra: self.stat.da_finestra.load(Relaxed),
            da_sst: self.stat.da_sst.load(Relaxed),
            get_manifest: self.stat.get_manifest.load(Relaxed),
            get_intero: self.stat.get_intero.load(Relaxed),
            get_meta: self.stat.get_meta.load(Relaxed),
            get_fetta: self.stat.get_fetta.load(Relaxed),
            get_blocco: self.stat.get_blocco.load(Relaxed),
            run_sparite: self.stat.run_sparite.load(Relaxed),
            cache_hit: self.cache.stat.hit.load(Relaxed),
            cache_miss: self.cache.stat.miss.load(Relaxed),
            cache_sfratti: self.cache.stat.sfratti.load(Relaxed),
            cache_usati: self.cache.usati(),
        }
    }

    /// Dimentica i manifest (simula un nodo d'ingresso appena avviato; la cache dei blocchi resta).
    pub fn dimentica_manifest(&self) {
        self.manifests.lock().clear();
    }

    async fn volo(&self, k: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let m = self.voli.lock().entry(k.to_string()).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).clone();
        m.lock_owned().await
    }

    /// Lettura puntuale alla posizione S. None = assente.
    pub async fn get(&self, db: Db, key: Bytes, s: Pos) -> anyhow::Result<Option<Bytes>> {
        Ok(self.get_fonte(db, key, s).await?.0)
    }

    /// Come `get`, e dice se la risposta è venuta tutta dalla finestra del registro.
    pub async fn get_fonte(&self, db: Db, key: Bytes, s: Pos) -> anyhow::Result<(Option<Bytes>, bool)> {
        self.stat.letture.fetch_add(1, Relaxed);
        let (somma, potato) = match self.reg.get(db, key.clone(), s).await? {
            GetResp::Valore { val, .. } => {
                self.stat.da_finestra.fetch_add(1, Relaxed);
                return Ok((Some(val), true));
            }
            GetResp::Assente { .. } => {
                self.stat.da_finestra.fetch_add(1, Relaxed);
                return Ok((None, true));
            }
            GetResp::Indietro(p) => anyhow::bail!("replica indietro ({p} < {s})"),
            GetResp::Delta { somma, potato_fino_a, .. } => (Some(somma), potato_fino_a),
            GetResp::FuoriFinestra { potato_fino_a, .. } => (None, potato_fino_a),
        };
        self.stat.da_sst.fetch_add(1, Relaxed);
        let base = self.sst_get(db, &key, s, potato).await?;
        Ok((
            match somma {
                None => base,
                Some(x) => Some(Bytes::copy_from_slice(&base.map(|b| delta_val(&b)).unwrap_or(0).wrapping_add(x).to_le_bytes())),
            },
            false,
        ))
    }

    async fn manifest(&self, g: &str, db: &Db, potato: Pos, forza: bool) -> anyhow::Result<Arc<Manifest>> {
        if !forza {
            if let Some(c) = self.manifests.lock().get(g) {
                if c.m.scaricato_db(db) >= potato {
                    return Ok(c.m.clone());
                }
            }
        }
        let _v = self.volo(&format!("m/{g}")).await;
        if let Some(c) = self.manifests.lock().get(g) {
            if c.m.scaricato_db(db) >= potato && !forza {
                return Ok(c.m.clone());
            }
        }
        self.stat.get_manifest.fetch_add(1, Relaxed);
        let m = match manifest::carica(&self.st, g).await? {
            Some((m, _)) => m,
            None => Manifest::default(),
        };
        anyhow::ensure!(m.scaricato_db(db) >= potato, "manifest indietro rispetto al registro");
        let m = Arc::new(m);
        self.manifests.lock().insert(g.to_string(), ManCache { m: m.clone() });
        Ok(m)
    }

    async fn intero(&self, g: &str, run: &Run, id: &Arc<str>) -> anyhow::Result<Option<(Arc<SstMeta>, Bytes)>> {
        self.stat.get_intero.fetch_add(1, Relaxed);
        let Some((b, _)) = self.st.get(&manifest::path_sst(g, &run.id)).await? else { return Ok(None) };
        let meta = Arc::new(SstMeta::from_object(&b)?);
        for (i, bl) in meta.blocks.iter().enumerate() {
            self.cache.metti_blocco(id, i as u32, &b[bl.off as usize..(bl.off + bl.len as u64) as usize]);
        }
        self.cache.metti_meta(id, meta.clone());
        Ok(Some((meta, b)))
    }

    async fn meta(&self, g: &str, run: &Run) -> anyhow::Result<Option<Arc<SstMeta>>> {
        let id = chiave_run(g, run);
        if let Some(m) = self.cache.meta(&id) {
            return Ok(Some(m));
        }
        let _v = self.volo(&id).await;
        if let Some(m) = self.cache.meta(&id) {
            return Ok(Some(m));
        }
        if run.dim <= self.cfg.intero_max {
            return Ok(self.intero(g, run, &id).await?.map(|x| x.0));
        }
        self.stat.get_meta.fetch_add(1, Relaxed);
        let Some(b) = self.st.get_range(&manifest::path_sst(g, &run.id), run.meta_off, run.meta_off + run.meta_len as u64).await? else { return Ok(None) };
        let meta = Arc::new(SstMeta::parse(&b)?);
        self.cache.metti_meta(&id, meta.clone());
        Ok(Some(meta))
    }

    async fn blocco(&self, g: &str, run: &Run, meta: &SstMeta, i: usize, db: &Db) -> anyhow::Result<Option<Bytes>> {
        let id = chiave_run(g, run);
        if let Some(b) = self.cache.blocco(&id, i as u32) {
            return Ok(Some(b));
        }
        let _v = self.volo(&id).await;
        if let Some(b) = self.cache.blocco(&id, i as u32) {
            return Ok(Some(b));
        }
        let path = manifest::path_sst(g, &run.id);
        if run.dim <= self.cfg.intero_max {
            let Some((_, obj)) = self.intero(g, run, &id).await? else { return Ok(None) };
            let bl = &meta.blocks[i];
            return Ok(Some(obj.slice(bl.off as usize..(bl.off + bl.len as u64) as usize)));
        }
        let (a, z) = meta.db_blocks(db).unwrap_or((i as u32, i as u32));
        let (s, e) = meta.span(a, z);
        if e - s <= self.cfg.fetta_max && z > a {
            self.stat.get_fetta.fetch_add(1, Relaxed);
            let Some(b) = self.st.get_range(&path, s, e).await? else { return Ok(None) };
            let mut out = None;
            for j in a..=z {
                let bl = &meta.blocks[j as usize];
                let x = &b[(bl.off - s) as usize..(bl.off - s + bl.len as u64) as usize];
                self.cache.metti_blocco(&id, j, x);
                if j as usize == i {
                    out = Some(Bytes::copy_from_slice(x));
                }
            }
            return Ok(out);
        }
        self.stat.get_blocco.fetch_add(1, Relaxed);
        let bl = &meta.blocks[i];
        let Some(b) = self.st.get_range(&path, bl.off, bl.off + bl.len as u64).await? else { return Ok(None) };
        self.cache.metti_blocco(&id, i as u32, &b);
        Ok(Some(b))
    }

    /// Valore negli SST alla posizione S (il manifest deve coprire almeno `potato`).
    pub async fn sst_get(&self, db: Db, key: &[u8], s: Pos, potato: Pos) -> anyhow::Result<Option<Bytes>> {
        let g = gruppo_di(&db, self.cfg.raggruppa);
        let fk = sst::full_key(&db, key);
        let mut forza = false;
        'tentativi: for _ in 0..3 {
            let m = self.manifest(&g, &db, potato, forza).await?;
            if self.cfg.prefetch_meta && m.run.len() > 1 {
                let _ = futures::future::join_all(m.run.iter().filter(|r| r.min <= s).map(|r| self.meta(&g, r))).await;
            }
            let mut somma: i64 = 0;
            let mut delta = false;
            for run in &m.run {
                if run.min > s {
                    continue;
                }
                let Some(meta) = self.meta(&g, run).await? else {
                    // run cancellata dopo una compattazione: manifest vecchio, si rilegge
                    self.stat.run_sparite.fetch_add(1, Relaxed);
                    forza = true;
                    continue 'tentativi;
                };
                if meta.db_blocks(&db).is_none() || !meta.bloom.may_contain(&fk) {
                    continue;
                }
                let Some(i) = meta.find_block(&fk) else { continue };
                let Some(b) = self.blocco(&g, run, &meta, i, &db).await? else {
                    self.stat.run_sparite.fetch_add(1, Relaxed);
                    forza = true;
                    continue 'tentativi;
                };
                match sst::block_lookup(&b, &fk, s)? {
                    Lookup::NotFound => continue,
                    Lookup::Delta { somma: x, .. } => {
                        somma = somma.wrapping_add(x);
                        delta = true;
                    }
                    Lookup::Found { val, .. } => {
                        if !delta {
                            return Ok(val);
                        }
                        let base = val.map(|v| delta_val(&v)).unwrap_or(0);
                        return Ok(Some(Bytes::copy_from_slice(&base.wrapping_add(somma).to_le_bytes())));
                    }
                }
            }
            return Ok(if delta { Some(Bytes::copy_from_slice(&somma.to_le_bytes())) } else { None });
        }
        anyhow::bail!("run sparite tre volte di seguito")
    }

    /// Scansione [da, a) alla posizione S: finestra del registro fusa con gli SST (la finestra vince).
    pub async fn scan(&self, db: Db, da: Bytes, a: Bytes, s: Pos, limite: usize) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let st = self.reg.stato_scarico(db).await?;
        let win = self.reg.scan(db, da.clone(), a.clone(), s, usize::MAX).await?;
        let g = gruppo_di(&db, self.cfg.raggruppa);
        let m = self.manifest(&g, &db, st.potato_fino_a, false).await?;
        let lo = sst::full_key(&db, &da);
        let hi = sst::full_key(&db, &a);
        let mut sst_vals: std::collections::BTreeMap<Bytes, Option<Bytes>> = std::collections::BTreeMap::new();
        // dalle run più vecchie alle più nuove: la più nuova vince
        for run in m.run.iter().rev() {
            if run.min > s {
                continue;
            }
            let Some(meta) = self.meta(&g, run).await? else { anyhow::bail!("run sparita in scan") };
            let Some((x, y)) = meta.db_blocks(&db) else { continue };
            for i in x..=y {
                let bl = &meta.blocks[i as usize];
                if bl.last < lo || bl.first >= hi {
                    continue;
                }
                let Some(b) = self.blocco(&g, run, &meta, i as usize, &db).await? else { anyhow::bail!("blocco sparito") };
                let mut es = Vec::new();
                sst::block_entries(&b, &mut es)?;
                let mut ultimo: Option<Bytes> = None;
                for e in es {
                    if e.key < lo || e.key >= hi || e.pos > s || ultimo.as_ref() == Some(&e.key) {
                        continue;
                    }
                    ultimo = Some(e.key.clone());
                    let v = match e.kind {
                        Kind::Put => Some(e.val),
                        Kind::Del => None,
                        Kind::Delta => Some(e.val), // semplificazione: delta negli SST letti in scan non composti
                    };
                    sst_vals.insert(e.key.slice(16..), v);
                }
            }
        }
        for (k, r) in win {
            match r {
                GetResp::Valore { val, .. } => {
                    sst_vals.insert(k, Some(val));
                }
                GetResp::Assente { .. } => {
                    sst_vals.insert(k, None);
                }
                _ => {}
            }
        }
        Ok(sst_vals.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))).take(limite).collect())
    }
}
