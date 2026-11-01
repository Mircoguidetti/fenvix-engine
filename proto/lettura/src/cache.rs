//! Cache dei nodi d'ingresso: meta degli SST (indice + bloom, già decodificati) e blocchi di dati, con un budget in
//! byte e sfratto LRU. Due supporti: RAM, o "NVMe" (i blocchi in file su disco locale, i meta sempre in RAM e
//! contati nel budget).

use crate::sst::SstMeta;
use bytes::Bytes;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

pub const META: u32 = u32::MAX;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Chiave {
    pub run: Arc<str>,
    pub blocco: u32,
}

#[derive(Clone)]
enum Voce {
    Meta(Arc<SstMeta>),
    Ram(Bytes),
    Disco(u32),
}

impl Voce {
    fn dim(&self) -> u64 {
        match self {
            Voce::Meta(m) => m.bytes as u64,
            Voce::Ram(b) => b.len() as u64 + 64,
            Voce::Disco(n) => *n as u64 + 64,
        }
    }
}

#[derive(Default, Debug)]
pub struct StatCache {
    pub hit: AtomicU64,
    pub miss: AtomicU64,
    pub sfratti: AtomicU64,
}

pub struct Cache {
    budget: u64,
    inner: Mutex<(lru::LruCache<Chiave, Voce>, u64)>,
    disco: Option<PathBuf>,
    pub stat: StatCache,
}

impl Cache {
    pub fn ram(budget: u64) -> Arc<Cache> {
        Arc::new(Cache { budget, inner: Mutex::new((lru::LruCache::unbounded(), 0)), disco: None, stat: Default::default() })
    }

    pub fn disco(budget: u64, dir: PathBuf) -> anyhow::Result<Arc<Cache>> {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        Ok(Arc::new(Cache { budget, inner: Mutex::new((lru::LruCache::unbounded(), 0)), disco: Some(dir), stat: Default::default() }))
    }

    fn file(&self, k: &Chiave) -> PathBuf {
        self.disco.as_ref().unwrap().join(format!("{}_{}", k.run, k.blocco))
    }

    pub fn usati(&self) -> u64 {
        self.inner.lock().1
    }

    pub fn meta(&self, run: &Arc<str>) -> Option<Arc<SstMeta>> {
        let k = Chiave { run: run.clone(), blocco: META };
        let mut g = self.inner.lock();
        match g.0.get(&k) {
            Some(Voce::Meta(m)) => {
                self.stat.hit.fetch_add(1, Relaxed);
                Some(m.clone())
            }
            _ => {
                self.stat.miss.fetch_add(1, Relaxed);
                None
            }
        }
    }

    pub fn blocco(&self, run: &Arc<str>, i: u32) -> Option<Bytes> {
        let k = Chiave { run: run.clone(), blocco: i };
        let v = self.inner.lock().0.get(&k).cloned();
        match v {
            Some(Voce::Ram(b)) => {
                self.stat.hit.fetch_add(1, Relaxed);
                Some(b)
            }
            Some(Voce::Disco(_)) => match std::fs::read(self.file(&k)) {
                Ok(b) => {
                    self.stat.hit.fetch_add(1, Relaxed);
                    Some(Bytes::from(b))
                }
                Err(_) => {
                    self.stat.miss.fetch_add(1, Relaxed);
                    None
                }
            },
            _ => {
                self.stat.miss.fetch_add(1, Relaxed);
                None
            }
        }
    }

    fn metti(&self, k: Chiave, v: Voce) {
        let mut sfrattati = Vec::new();
        {
            let mut g = self.inner.lock();
            g.1 += v.dim();
            if let Some(old) = g.0.put(k, v) {
                g.1 -= old.dim();
            }
            while g.1 > self.budget {
                match g.0.pop_lru() {
                    Some((k, v)) => {
                        g.1 -= v.dim();
                        self.stat.sfratti.fetch_add(1, Relaxed);
                        if matches!(v, Voce::Disco(_)) {
                            sfrattati.push(k);
                        }
                    }
                    None => break,
                }
            }
        }
        for k in sfrattati {
            let _ = std::fs::remove_file(self.file(&k));
        }
    }

    pub fn metti_meta(&self, run: &Arc<str>, m: Arc<SstMeta>) {
        self.metti(Chiave { run: run.clone(), blocco: META }, Voce::Meta(m));
    }

    pub fn metti_blocco(&self, run: &Arc<str>, i: u32, b: &[u8]) {
        let k = Chiave { run: run.clone(), blocco: i };
        if self.disco.is_some() {
            if std::fs::write(self.file(&k), b).is_ok() {
                self.metti(k, Voce::Disco(b.len() as u32));
            }
        } else {
            self.metti(k, Voce::Ram(Bytes::copy_from_slice(b)));
        }
    }
}
