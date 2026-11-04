//! Il bancone (cache) della prova: tiene separati i blocchi di dati dagli indici dei blocchi, dai filtri e dalle statistiche,
//! così si può svuotare solo la parte dei dati prima di ogni query. Unisce le richieste uguali in volo (come foyer, la cache
//! del motore), così due letture dello stesso blocco nello stesso momento fanno una GET sola.

use async_trait::async_trait;
use slatedb::db_cache::{CacheFetch, CacheLoader, CachedEntry, CachedKey, DbCache};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Tipo {
    Blocco = 0,
    Indice = 1,
    Filtro = 2,
    Stat = 3,
}

#[derive(Default)]
pub struct Bancone {
    mappe: [Mutex<HashMap<CachedKey, CachedEntry>>; 4],
    in_volo: Mutex<HashMap<(Tipo, CachedKey), Arc<OnceCell<CachedEntry>>>>,
    /// Mancate (letture da S3) per tipo, e byte caricati per tipo.
    pub mancate: [AtomicU64; 4],
    pub byte: [AtomicU64; 4],
    /// Se falso, gli indici/filtri/statistiche non si tengono (bancone vuoto anche per loro).
    pub tieni_meta: std::sync::atomic::AtomicBool,
}

impl Bancone {
    pub fn new(tieni_meta: bool) -> Self {
        let b = Self::default();
        b.tieni_meta.store(tieni_meta, Relaxed);
        b
    }

    pub fn svuota_dati(&self) {
        self.mappe[Tipo::Blocco as usize].lock().unwrap().clear();
        if !self.tieni_meta.load(Relaxed) {
            for t in [Tipo::Indice, Tipo::Filtro, Tipo::Stat] {
                self.mappe[t as usize].lock().unwrap().clear();
            }
        }
    }

    pub fn svuota_tutto(&self) {
        for m in &self.mappe {
            m.lock().unwrap().clear();
        }
    }

    pub fn azzera_contatori(&self) {
        for i in 0..4 {
            self.mancate[i].store(0, Relaxed);
            self.byte[i].store(0, Relaxed);
        }
    }

    /// Byte tenuti, per tipo.
    pub fn byte_tenuti(&self, t: Tipo) -> u64 {
        self.mappe[t as usize].lock().unwrap().values().map(|e| e.size() as u64).sum()
    }

    pub fn voci(&self, t: Tipo) -> usize {
        self.mappe[t as usize].lock().unwrap().len()
    }

    fn get(&self, t: Tipo, k: &CachedKey) -> Option<CachedEntry> {
        self.mappe[t as usize].lock().unwrap().get(k).cloned()
    }

    async fn fetch(&self, t: Tipo, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        if let Some(e) = self.get(t, &key) {
            return Ok(CacheFetch::hit(e));
        }
        let cell = {
            let mut m = self.in_volo.lock().unwrap();
            Arc::clone(m.entry((t, key.clone())).or_insert_with(|| Arc::new(OnceCell::new())))
        };
        let mut caricato_qui = false;
        let r = cell
            .get_or_try_init(|| {
                caricato_qui = true;
                loader()
            })
            .await
            .cloned();
        if caricato_qui {
            self.in_volo.lock().unwrap().remove(&(t, key.clone()));
        }
        let e = r?;
        if caricato_qui {
            self.mancate[t as usize].fetch_add(1, Relaxed);
            self.byte[t as usize].fetch_add(e.size() as u64, Relaxed);
            self.mappe[t as usize].lock().unwrap().insert(key, e.clone());
            Ok(CacheFetch::miss(e))
        } else {
            Ok(CacheFetch::hit(e))
        }
    }
}

#[async_trait]
impl DbCache for Bancone {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(Tipo::Blocco, key))
    }
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(Tipo::Indice, key))
    }
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(Tipo::Filtro, key))
    }
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(Tipo::Stat, key))
    }
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        // Fuori da fetch_* SlateDB inserisce i blocchi letti a gruppi (scansioni con read-ahead) e, in scrittura, indici e
        // filtri delle scatole appena scritte. La prova non legge mai dal processo che scrive, quindi qui arrivano blocchi.
        for t in [Tipo::Indice, Tipo::Filtro, Tipo::Stat] {
            let mut m = self.mappe[t as usize].lock().unwrap();
            if m.contains_key(&key) {
                m.insert(key, value);
                return;
            }
        }
        self.byte[Tipo::Blocco as usize].fetch_add(value.size() as u64, Relaxed);
        self.mappe[Tipo::Blocco as usize].lock().unwrap().insert(key, value);
    }
    async fn remove(&self, key: &CachedKey) {
        for m in &self.mappe {
            m.lock().unwrap().remove(key);
        }
    }
    fn entry_count(&self) -> u64 {
        self.mappe.iter().map(|m| m.lock().unwrap().len() as u64).sum()
    }
    async fn fetch_block(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(Tipo::Blocco, key, loader).await
    }
    async fn fetch_index(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(Tipo::Indice, key, loader).await
    }
    async fn fetch_filter(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(Tipo::Filtro, key, loader).await
    }
    async fn fetch_stats(&self, key: CachedKey, loader: CacheLoader) -> Result<CacheFetch, slatedb::Error> {
        self.fetch(Tipo::Stat, key, loader).await
    }
}
