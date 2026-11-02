//! Prova P1 "un milione di fogli" (vedi docs/RAPPORTO-prova-milione.md e il rapporto nel brain).
//!
//! Un database piccolo nella variante a2 di proto-lettura è un foglio (manifest JSON) più una scatola (un solo SST da
//! circa 10 KB) su S3. Dentro la scatola, oltre ai documenti, c'è il catalogo finto chiesto dal revisore: 3 collezioni,
//! 5 definizioni di indice e 1 utente SCRAM-SHA-256, con le stesse chiavi del motore (`meta/collections`,
//! `meta/indexes/{coll}`) più `meta/users/{nome}` (nel motore di oggi gli utenti stanno in `admin.system.users`; nel
//! disegno nuovo un database per inquilino porta i suoi utenti con sé).
//!
//! Qui: costruzione degli oggetti, il cameriere (nodo d'ingresso) con l'LRU dei database aperti, l'allocatore che conta i
//! byte vivi (per la RAM per database), lo zipf, la mappa chiave S3 -> file del finto S3 su disco.

use anyhow::Context;
use bytes::Bytes;
use lettura::iface::*;
use lettura::manifest::{self, Manifest, Run};
use lettura::registro::RegistroSim;
use lettura::sst::{self, Entry, Lookup, SstMeta};
use lettura::store::Store;
use parking_lot::Mutex;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

// ---------------------------------------------------------------- allocatore che conta

/// Conta i byte chiesti e il numero di allocazioni vive. Non conta l'arrotondamento di malloc (circa 8-16 byte per
/// allocazione): per questo si riporta anche il numero di allocazioni.
pub struct Contatore;
pub static VIVI: AtomicI64 = AtomicI64::new(0);
pub static NALLOC: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for Contatore {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            VIVI.fetch_add(l.size() as i64, Relaxed);
            NALLOC.fetch_add(1, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        VIVI.fetch_sub(l.size() as i64, Relaxed);
        NALLOC.fetch_sub(1, Relaxed);
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(l);
        if !p.is_null() {
            VIVI.fetch_add(l.size() as i64, Relaxed);
            NALLOC.fetch_add(1, Relaxed);
        }
        p
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = System.realloc(p, l, n);
        if !q.is_null() {
            VIVI.fetch_add(n as i64 - l.size() as i64, Relaxed);
        }
        q
    }
}

#[global_allocator]
static GLOBALE: Contatore = Contatore;

pub fn vivi() -> (i64, i64) {
    (VIVI.load(Relaxed), NALLOC.load(Relaxed))
}

pub fn rss() -> u64 {
    lettura::util::rss(std::process::id())
}

pub fn cpu_s() -> f64 {
    lettura::util::cpu_s(std::process::id())
}

// ---------------------------------------------------------------- nomi e oggetti

/// Nome MongoDB del database numero `i` (come lo vede il driver).
pub fn nome(i: u64) -> String {
    format!("cliente-{i:07}")
}

pub fn indice_da_nome(n: &str) -> Option<u64> {
    n.strip_prefix("cliente-")?.parse().ok()
}

pub fn db_id(i: u64) -> Db {
    db_n(i)
}

pub const COLL: [&str; 3] = ["utenti", "ordini", "eventi"];
pub const DOCS: u64 = 8;
pub const VAL: usize = 1000;
pub const ITER_SCRAM: u32 = 15_000;

pub fn chiave_doc(i: u64) -> Bytes {
    doc_key(COLL[(i % 3) as usize], i)
}

/// Chiavi SCRAM-SHA-256 derivate come fa il server a `createUser`: PBKDF2-HMAC-SHA-256 con `iter` iterazioni.
/// Con `iter = 0` si saltano i calcoli (precarico veloce: chiavi a caso della stessa forma).
pub fn scram(password: &str, sale: &[u8], iter: u32) -> ([u8; 32], [u8; 32]) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    if iter == 0 {
        let h = Sha256::digest([password.as_bytes(), sale].concat());
        return (h.into(), Sha256::digest(h).into());
    }
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), sale, iter, &mut salted);
    let hm = |k: &[u8], m: &[u8]| -> [u8; 32] {
        let mut x = <Hmac<Sha256>>::new_from_slice(k).unwrap();
        x.update(m);
        x.finalize().into_bytes().into()
    };
    let client = hm(&salted, b"Client Key");
    let stored: [u8; 32] = Sha256::digest(client).into();
    (stored, hm(&salted, b"Server Key"))
}

/// Il catalogo finto in BSON, con le chiavi del motore: (chiave, valore).
pub fn catalogo_bson(i: u64, iter: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    use base64::Engine;
    use bson::doc;
    let b64 = base64::engine::general_purpose::STANDARD;
    let enc = |d: bson::Document| bson::to_vec(&d).unwrap();
    let mut out = Vec::new();
    out.push((b"meta/collections".to_vec(), enc(doc! { "names": COLL.to_vec() })));
    // 5 definizioni oltre agli `_id_` (che il motore non scrive): 2 su utenti, 2 su ordini, 1 su eventi
    let idx: [(usize, bson::Document); 5] = [
        (0, doc! { "v": 2, "key": { "email": 1 }, "name": "email_1", "unique": true }),
        (0, doc! { "v": 2, "key": { "creato": -1 }, "name": "creato_-1" }),
        (1, doc! { "v": 2, "key": { "utente": 1, "data": -1 }, "name": "utente_1_data_-1" }),
        (1, doc! { "v": 2, "key": { "stato": 1 }, "name": "stato_1", "partialFilterExpression": { "stato": { "$exists": true } } }),
        (2, doc! { "v": 2, "key": { "scade": 1 }, "name": "scade_1", "expireAfterSeconds": 86400 }),
    ];
    for c in 0..3 {
        let v: Vec<bson::Document> = idx.iter().filter(|(k, _)| *k == c).map(|(_, d)| d.clone()).collect();
        out.push((format!("meta/indexes/{}", COLL[c]).into_bytes(), enc(doc! { "indexes": v })));
    }
    let sale: [u8; 28] = {
        let h = sst::hash64(&i.to_le_bytes()).to_le_bytes();
        let mut s = [0u8; 28];
        for (j, x) in s.iter_mut().enumerate() {
            *x = h[j % 8] ^ (j as u8);
        }
        s
    };
    let (stored, server) = scram(&format!("password-{i}"), &sale, iter);
    let n = nome(i);
    out.push((
        b"meta/users/app".to_vec(),
        enc(doc! {
            "_id": format!("{n}.app"), "user": "app", "db": n.clone(),
            "credentials": { "SCRAM-SHA-256": { "iterationCount": ITER_SCRAM as i32, "salt": b64.encode(sale), "storedKey": b64.encode(stored), "serverKey": b64.encode(server) } },
            "roles": [ { "role": "readWrite", "db": n } ],
        }),
    ));
    out
}

/// Scatola (SST) e foglio (manifest) del database `i`, alla posizione `pos`.
pub fn oggetti_db(i: u64, pos: Pos, iter: u32, blocco: usize) -> (String, Bytes, Manifest) {
    let db = db_id(i);
    let mut es: Vec<Entry> = Vec::with_capacity(16);
    for (k, v) in catalogo_bson(i, iter) {
        es.push(Entry { key: sst::full_key(&db, &k), pos, kind: Kind::Put, val: Bytes::from(v) });
    }
    for d in 0..DOCS {
        let mut v = vec![0u8; VAL];
        let h = sst::hash64(&[i.to_le_bytes(), d.to_le_bytes()].concat()).to_le_bytes();
        for (j, x) in v.iter_mut().enumerate() {
            *x = h[j % 8].wrapping_add(j as u8);
        }
        es.push(Entry { key: sst::full_key(&db, &chiave_doc(d)), pos, kind: Kind::Put, val: Bytes::from(v) });
    }
    es.sort_by(|a, b| a.key.cmp(&b.key).then(b.pos.cmp(&a.pos)));
    let b = sst::build(es, blocco);
    let id = format!("{:016x}-{:08x}w", b.max, (sst::hash64(&i.to_le_bytes()) as u32));
    let data = Bytes::from(b.data);
    let mut m = Manifest { versione: 1, orfani_ms: now_ms(), ultimo_scarico_ms: now_ms(), ..Default::default() };
    m.run.push(Run { id: id.clone(), min: b.min, max: b.max, dim: data.len() as u64, meta_off: b.meta_off, meta_len: b.meta_len, voci: b.voci });
    m.scaricato.insert(db_hex(&db), pos);
    (id, data, m)
}

/// Valore atteso del documento `d` del database `i` (per la verifica delle letture).
pub fn crc_atteso(i: u64, d: u64) -> u32 {
    let mut v = vec![0u8; VAL];
    let h = sst::hash64(&[i.to_le_bytes(), d.to_le_bytes()].concat()).to_le_bytes();
    for (j, x) in v.iter_mut().enumerate() {
        *x = h[j % 8].wrapping_add(j as u8);
    }
    crc32fast_hash(&v)
}

pub fn crc32fast_hash(b: &[u8]) -> u32 {
    crc32fast::hash(b)
}

/// File del finto S3 su disco per una chiave S3 (stessa regola di scripts/finto_s3_disco.py).
pub fn file_di(root: &Path, bucket: &str, key: &str) -> PathBuf {
    match key.split_once('/') {
        Some((t, r)) => root.join(bucket).join(t).join(r.replace('/', "~")),
        None => root.join(bucket).join("_").join(key),
    }
}

pub fn chiave_di_file(testa: &str, f: &str) -> String {
    if testa == "_" {
        f.to_string()
    } else {
        format!("{testa}/{}", f.replace('~', "/"))
    }
}

pub fn scrivi_file(p: &Path, b: &[u8]) -> anyhow::Result<()> {
    if let Some(d) = p.parent() {
        std::fs::create_dir_all(d)?;
    }
    let tmp = p.with_extension(format!("tmp{}", rand::random::<u32>()));
    std::fs::write(&tmp, b)?;
    std::fs::rename(&tmp, p)?;
    Ok(())
}

// ---------------------------------------------------------------- zipf

/// Distribuzione zipf su `n` ranghi con esponente `s`, per campionamento inverso sulla CDF (n fino a qualche milione).
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: u64, s: f64) -> Zipf {
        let mut cdf = Vec::with_capacity(n as usize);
        let mut t = 0.0;
        for k in 1..=n {
            t += 1.0 / (k as f64).powf(s);
            cdf.push(t);
        }
        for x in cdf.iter_mut() {
            *x /= t;
        }
        Zipf { cdf }
    }
    /// Esponente tale che il primo `quota` dei ranghi prenda `peso` delle estrazioni (bisezione).
    pub fn esponente(n: u64, quota: f64, peso: f64) -> f64 {
        let top = ((n as f64) * quota).round().max(1.0) as u64;
        let frac = |s: f64| {
            let (mut a, mut b) = (0.0, 0.0);
            for k in 1..=n {
                let w = 1.0 / (k as f64).powf(s);
                if k <= top {
                    a += w;
                }
                b += w;
            }
            a / b
        };
        let (mut lo, mut hi) = (0.0f64, 3.0f64);
        for _ in 0..50 {
            let m = (lo + hi) / 2.0;
            if frac(m) < peso {
                lo = m
            } else {
                hi = m
            }
        }
        (lo + hi) / 2.0
    }
    pub fn rango<R: rand::Rng>(&self, r: &mut R) -> u64 {
        let u: f64 = r.gen();
        self.cdf.partition_point(|x| *x < u).min(self.cdf.len() - 1) as u64
    }
    pub fn quota_primi(&self, k: u64) -> f64 {
        self.cdf[(k as usize).saturating_sub(1).min(self.cdf.len() - 1)]
    }
}

/// Permutazione fissa dei ranghi sugli indici dei database (i caldi non sono i primi creati).
pub fn mescola(rango: u64, n: u64) -> u64 {
    // moltiplicazione per un dispari modulo la potenza di 2 sopra n, ripetuta finché si cade sotto n (biiezione)
    let m = n.next_power_of_two();
    let mut x = rango;
    loop {
        x = (x.wrapping_mul(0x9E37_79B9_7F4A_7C15 | 1).wrapping_add(0x632B_E59B_D9B4_E019)) & (m - 1);
        if x < n {
            return x;
        }
    }
}

// ---------------------------------------------------------------- il cameriere

/// Database chiuso ma conosciuto: la riga della regia tenuta dal cameriere (nome -> id).
pub struct Conosciuti {
    pub mappa: HashMap<Box<str>, Db>,
}

/// Catalogo di un database aperto, nella forma che serve al cameriere per rispondere (nomi, indici, utente).
pub struct Indice {
    pub coll: u8,
    pub nome: Box<str>,
    /// definizione intera in BSON (chiave, opzioni): serve a `listIndexes` e al piano delle query
    pub def: Box<[u8]>,
    pub unico: bool,
}

pub struct Utente {
    pub nome: Box<str>,
    pub sale: Box<[u8]>,
    pub iter: u32,
    pub stored: [u8; 32],
    pub server: [u8; 32],
    pub ruoli: Box<[(Box<str>, Box<str>)]>,
}

pub struct Catalogo {
    pub coll: Box<[Box<str>]>,
    pub indici: Box<[Indice]>,
    pub utenti: Box<[Utente]>,
}

impl Catalogo {
    pub fn da_voci(es: &[Entry]) -> anyhow::Result<Catalogo> {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let mut coll: Vec<Box<str>> = Vec::new();
        let mut indici = Vec::new();
        let mut utenti = Vec::new();
        for e in es {
            let k = &e.key[16..];
            if k == b"meta/collections" {
                let d = bson::Document::from_reader(&mut &e.val[..])?;
                coll = d.get_array("names")?.iter().filter_map(|x| x.as_str().map(Box::from)).collect();
            }
        }
        for e in es {
            let k = &e.key[16..];
            if let Some(c) = k.strip_prefix(b"meta/indexes/") {
                let c = std::str::from_utf8(c)?;
                let ci = coll.iter().position(|x| &**x == c).context("indice di collezione sconosciuta")? as u8;
                let d = bson::Document::from_reader(&mut &e.val[..])?;
                for i in d.get_array("indexes")? {
                    let i = i.as_document().context("indice non documento")?;
                    indici.push(Indice { coll: ci, nome: i.get_str("name")?.into(), def: bson::to_vec(i)?.into_boxed_slice(), unico: i.get_bool("unique").unwrap_or(false) });
                }
            } else if k.starts_with(b"meta/users/") {
                let d = bson::Document::from_reader(&mut &e.val[..])?;
                let c = d.get_document("credentials")?.get_document("SCRAM-SHA-256")?;
                let dec32 = |s: &str| -> anyhow::Result<[u8; 32]> { b64.decode(s)?.try_into().map_err(|_| anyhow::anyhow!("chiave SCRAM lunga sbagliata")) };
                let ruoli = d.get_array("roles")?.iter().filter_map(|r| r.as_document()).map(|r| Ok((Box::from(r.get_str("role")?), Box::from(r.get_str("db")?)))).collect::<anyhow::Result<Vec<_>>>()?;
                utenti.push(Utente {
                    nome: d.get_str("user")?.into(),
                    sale: b64.decode(c.get_str("salt")?)?.into_boxed_slice(),
                    iter: c.get_i32("iterationCount")? as u32,
                    stored: dec32(c.get_str("storedKey")?)?,
                    server: dec32(c.get_str("serverKey")?)?,
                    ruoli: ruoli.into_boxed_slice(),
                });
            }
        }
        anyhow::ensure!(coll.len() == 3 && indici.len() == 5 && utenti.len() == 1, "catalogo incompleto: {} {} {}", coll.len(), indici.len(), utenti.len());
        Ok(Catalogo { coll: coll.into_boxed_slice(), indici: indici.into_boxed_slice(), utenti: utenti.into_boxed_slice() })
    }
}

/// Dove tiene i dati (la scatola) un database aperto.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum Dati {
    /// la scatola intera in RAM, dentro il database aperto
    Ram,
    /// la scatola in un file della cache locale (NVMe in produzione), letta a ogni lettura
    Disco,
}

pub struct Aperto {
    pub idx: u64,
    pub db: Db,
    /// dal foglio: l'unica run (variante a2), la posizione scaricata, l'etag del foglio
    pub run_id: Box<str>,
    pub scaricato: Pos,
    pub etag: Box<str>,
    pub meta: SstMeta,
    pub cat: Catalogo,
    pub dati: Option<Bytes>,
}

#[derive(Default)]
pub struct StatCam {
    pub letture: AtomicU64,
    pub risvegli: AtomicU64,
    pub get_risveglio: AtomicU64,
    pub sfratti: AtomicU64,
    pub errori: AtomicU64,
    pub sbagliate: AtomicU64,
    pub attese_in_volo: AtomicU64,
}

pub struct Cameriere {
    pub st: Store,
    pub reg: Arc<RegistroSim>,
    pub conosciuti: Conosciuti,
    pub aperti: Mutex<lru::LruCache<u64, Arc<Aperto>>>,
    pub capienza: usize,
    pub dati: Dati,
    pub cache_dir: PathBuf,
    pub blocco: usize,
    pub stat: StatCam,
    volo: Mutex<HashMap<u64, Arc<tokio::sync::Notify>>>,
}

impl Cameriere {
    pub fn new(st: Store, reg: Arc<RegistroSim>, conosciuti: Conosciuti, capienza: usize, dati: Dati, cache_dir: PathBuf) -> Arc<Cameriere> {
        Arc::new(Cameriere {
            st,
            reg,
            conosciuti,
            aperti: Mutex::new(lru::LruCache::unbounded()),
            capienza,
            dati,
            cache_dir,
            blocco: 16 << 10,
            stat: Default::default(),
            volo: Mutex::new(HashMap::new()),
        })
    }

    pub fn n_aperti(&self) -> usize {
        self.aperti.lock().len()
    }

    /// Svuota l'LRU (come dopo un riavvio del cameriere; i conosciuti restano, la regia li ridà subito).
    pub fn svuota(&self) {
        self.aperti.lock().clear();
    }

    fn file_cache(&self, a: &Aperto) -> PathBuf {
        self.cache_dir.join(format!("{:02x}", a.idx & 0xff)).join(&*a.run_id)
    }

    /// Risveglio: GET del foglio, GET della scatola intera, lettura del catalogo.
    pub async fn risveglia(&self, idx: u64, db: Db) -> anyhow::Result<Arc<Aperto>> {
        self.stat.risvegli.fetch_add(1, Relaxed);
        let g = db_hex(&db);
        self.stat.get_risveglio.fetch_add(1, Relaxed);
        let (m, etag) = manifest::carica(&self.st, &g).await?.context("foglio assente")?;
        anyhow::ensure!(m.run.len() == 1, "variante a2: una sola run attesa, trovate {}", m.run.len());
        let run = &m.run[0];
        self.stat.get_risveglio.fetch_add(1, Relaxed);
        let (obj, _) = self.st.get(&manifest::path_sst(&g, &run.id)).await?.context("scatola assente")?;
        let meta = SstMeta::from_object(&obj)?;
        // il catalogo sta nelle chiavi meta/...: si leggono le voci della scatola (è piccola)
        let es = sst::all_entries(&obj)?;
        let cat = Catalogo::da_voci(&es)?;
        let mut a = Aperto { idx, db, run_id: run.id.as_str().into(), scaricato: m.scaricato_db(&db), etag: etag.into_boxed_str(), meta, cat, dati: None };
        match self.dati {
            Dati::Ram => a.dati = Some(Bytes::copy_from_slice(&obj)),
            Dati::Disco => {
                let f = self.file_cache(&a);
                let f2 = f.clone();
                let o2 = obj.clone();
                tokio::task::spawn_blocking(move || scrivi_file(&f2, &o2)).await??;
            }
        }
        Ok(Arc::new(a))
    }

    /// Il database aperto (dall'LRU, o risvegliato). `true` se è stato risvegliato da questa chiamata.
    pub async fn apri(&self, idx: u64) -> anyhow::Result<(Arc<Aperto>, bool)> {
        // prima l'LRU (chiave numerica), poi la riga dei conosciuti solo se serve un risveglio: così la lettura da
        // un database aperto non tocca la mappa dei conosciuti (un milione di righe) e non costruisce il nome
        if let Some(a) = self.aperti.lock().get(&idx) {
            return Ok((a.clone(), false));
        }
        let n = nome(idx);
        let db = *self.conosciuti.mappa.get(n.as_str()).context("database sconosciuto")?;
        // `atteso`: la lettura ha aspettato il risveglio fatto da un'altra (conta come lettura con risveglio)
        let mut atteso = false;
        loop {
            if let Some(a) = self.aperti.lock().get(&idx) {
                return Ok((a.clone(), atteso));
            }
            // un solo risveglio per database: gli altri aspettano
            let (mio, nt) = {
                let mut v = self.volo.lock();
                match v.get(&idx) {
                    Some(n) => (false, n.clone()),
                    None => {
                        let n = Arc::new(tokio::sync::Notify::new());
                        v.insert(idx, n.clone());
                        (true, n)
                    }
                }
            };
            if !mio {
                self.stat.attese_in_volo.fetch_add(1, Relaxed);
                atteso = true;
                let f = nt.notified();
                if self.aperti.lock().contains(&idx) || !self.volo.lock().contains_key(&idx) {
                    continue;
                }
                f.await;
                continue;
            }
            let r = self.risveglia(idx, db).await;
            let out = match r {
                Ok(a) => {
                    let mut l = self.aperti.lock();
                    l.put(idx, a.clone());
                    while l.len() > self.capienza {
                        if let Some((_, v)) = l.pop_lru() {
                            self.stat.sfratti.fetch_add(1, Relaxed);
                            if self.dati == Dati::Disco {
                                let _ = std::fs::remove_file(self.file_cache(&v));
                            }
                        }
                    }
                    Ok((a, true))
                }
                Err(e) => Err(e),
            };
            self.volo.lock().remove(&idx);
            nt.notify_waiters();
            return out;
        }
    }

    /// Lettura puntuale del documento `d` del database `idx` alla posizione confermata: finestra del registro, poi la
    /// scatola. Ritorna (crc del valore, risvegliato).
    pub async fn leggi(&self, idx: u64, d: u64) -> anyhow::Result<(Option<u32>, bool)> {
        self.stat.letture.fetch_add(1, Relaxed);
        let (a, sveglio) = self.apri(idx).await?;
        let key = chiave_doc(d);
        let s = self.reg.posizione_confermata_sync();
        let potato = match self.reg.get_sync(a.db, &key, s) {
            GetResp::Valore { val, .. } => return Ok((Some(crc32fast_hash(&val)), sveglio)),
            GetResp::Assente { .. } => return Ok((None, sveglio)),
            GetResp::FuoriFinestra { potato_fino_a, .. } => potato_fino_a,
            x => anyhow::bail!("risposta del registro inattesa: {x:?}"),
        };
        anyhow::ensure!(a.scaricato >= potato, "foglio indietro rispetto al registro");
        let fk = sst::full_key(&a.db, &key);
        if !a.meta.bloom.may_contain(&fk) {
            return Ok((None, sveglio));
        }
        let Some(bi) = a.meta.find_block(&fk) else { return Ok((None, sveglio)) };
        let bl = &a.meta.blocks[bi];
        let block = match &a.dati {
            Some(o) => o.slice(bl.off as usize..(bl.off + bl.len as u64) as usize),
            None => {
                use std::os::unix::fs::FileExt;
                let f = std::fs::File::open(self.file_cache(&a))?;
                let mut b = vec![0u8; bl.len as usize];
                f.read_exact_at(&mut b, bl.off)?;
                Bytes::from(b)
            }
        };
        Ok(match sst::block_lookup(&block, &fk, s)? {
            Lookup::Found { val: Some(v), .. } => (Some(crc32fast_hash(&v)), sveglio),
            _ => (None, sveglio),
        })
    }
}

/// Riga di risultato in JSON, su stdout e in coda a un file.
pub fn scrivi_riga(f: &Option<PathBuf>, v: &serde_json::Value) {
    println!("{v}");
    if let Some(p) = f {
        use std::io::Write;
        if let Ok(mut h) = std::fs::OpenOptions::new().create(true).append(true).open(p) {
            let _ = writeln!(h, "{v}");
        }
    }
}

pub fn perc(h: &hdrhistogram::Histogram<u64>) -> serde_json::Value {
    // l'istogramma è in nanosecondi (3 cifre significative); qui in microsecondi con i decimali
    let us = |x: u64| x as f64 / 1000.0;
    serde_json::json!({ "n": h.len(), "p50_us": us(h.value_at_quantile(0.5)), "p99_us": us(h.value_at_quantile(0.99)), "p999_us": us(h.value_at_quantile(0.999)), "max_us": us(h.max()), "media_us": (h.mean() / 100.0).round() / 10.0 })
}

pub fn hist() -> hdrhistogram::Histogram<u64> {
    hdrhistogram::Histogram::new_with_bounds(1, 300_000_000_000, 3).unwrap()
}

/// Scomposizione (contata a mano, byte chiesti all'allocatore) di un database aperto, per sapere dove vanno i byte.
pub fn scomponi(a: &Aperto) -> serde_json::Value {
    use std::mem::size_of;
    let m = &a.meta;
    let meta_indice: usize = m.blocks.len() * size_of::<sst::BlockIdx>() + m.blocks.iter().map(|b| b.first.len() + b.last.len()).sum::<usize>();
    let meta_db = m.dbdir.len() * size_of::<(Db, u32, u32)>();
    let cat_coll: usize = a.cat.coll.len() * size_of::<Box<str>>() + a.cat.coll.iter().map(|c| c.len()).sum::<usize>();
    let cat_idx: usize = a.cat.indici.len() * size_of::<Indice>() + a.cat.indici.iter().map(|i| i.nome.len() + i.def.len()).sum::<usize>();
    let cat_ut: usize = a.cat.utenti.len() * size_of::<Utente>() + a.cat.utenti.iter().map(|u| u.nome.len() + u.sale.len() + u.ruoli.len() * size_of::<(Box<str>, Box<str>)>() + u.ruoli.iter().map(|(x, y)| x.len() + y.len()).sum::<usize>()).sum::<usize>();
    serde_json::json!({
        "struttura_aperto": size_of::<Aperto>() + 16,
        "foglio_run_etag": a.run_id.len() + a.etag.len(),
        "indice_blocchi": meta_indice, "bloom_e_altro_meta_stimato": m.bytes.saturating_sub(meta_indice).saturating_sub(meta_db), "elenco_db": meta_db,
        "catalogo_collezioni": cat_coll, "catalogo_indici": cat_idx, "catalogo_utente": cat_ut,
        "scatola_in_ram": a.dati.as_ref().map(|d| d.len()).unwrap_or(0),
    })
}
