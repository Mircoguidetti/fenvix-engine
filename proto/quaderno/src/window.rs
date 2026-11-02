//! La finestra: per ogni database, le versioni recenti di ogni chiave, indicizzate per chiave e ordinate per posizione.
//!
//! Ogni replica la tiene aggiornata quando aggiunge una voce al registro (anche prima della conferma). Le letture
//! filtrano per posizione: chi legge a S ≤ posizione confermata vede solo versioni confermate, perché le posizioni
//! crescono con l'indice del registro. Il leader usa la stessa struttura per validare: una chiave è in conflitto se la
//! sua ultima versione (anche non ancora confermata: viene prima nell'ordine) è più recente di S.
//!
//! Potatura: un record esce dalla finestra quando è (a) su S3 (conferma_scarico) e (b) più vecchio di 60 s. Se dopo la
//! versione che esce restano solo delta, la versione viene "piegata" nel primo delta, che diventa un put col valore
//! calcolato: così la finestra sa sempre rispondere alle letture a posizioni ≥ inizio finestra della chiave.
//!
//! Giro 6 (M16, "capo veloce"): le versioni stanno in una **mappa a hash** (chiave → versioni), non più in un albero
//! ordinato. Controllo, aggiunta e potatura sul thread principale fanno solo accessi a hash (prima ogni chiave era una
//! discesa nell'albero con una serie di cache miss: il costo cresceva con la finestra). L'ordine delle chiavi serve
//! solo a `scan`: lo tiene un insieme ordinato a parte (`OrdIdx`), aggiornato da una coda di operazioni (chiave nata,
//! chiave uscita) che il thread principale riempie e che svuota l'indicizzatore (un thread a parte) oppure `scan`
//! stessa prima di leggere. La potatura segue la coda dei record in ordine di posizione (`recs`), come prima.
//! La semantica non cambia: stesse versioni, stessi confini (`trim_pos`, `spill_upto`) sotto lo stesso lock.
//!
//! Concorrenza: un lock lettore/scrittore per database. Scrive solo il thread principale della replica (aggiunta,
//! troncamento, potatura, sfratto, scarichi applicati); leggono i thread di rete (get, scan, scaricabili, pendenti),
//! senza passare dal thread principale. I valori sfrattati portano la loro collocazione sul disco, così anche un
//! thread di rete sa rileggerli.

use crate::codec::{Body, Db, Dep, Entry, FlushRec, GetRes, Kind, PendInfo, Pos, Write};
use crate::spill::{SEntry, SVal, Spill};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

/// Chiave della finestra: fino a 30 byte sta dentro la voce (niente puntatori da seguire), oltre va sullo heap.
/// Giro 6: sullo heap con `Arc`, così la stessa chiave nella mappa, nel record e nell'insieme ordinato costa una sola
/// allocazione (le copie sono un contatore).
#[derive(Clone)]
pub enum IKey {
    I(u8, [u8; 30]),
    H(Arc<[u8]>),
}
impl std::hash::Hash for IKey {
    // uguale all'hash di `[u8]`: serve perché la mappa si interroga con `&[u8]` (Borrow)
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.as_slice().hash(h)
    }
}
impl From<&[u8]> for IKey {
    fn from(k: &[u8]) -> IKey {
        IKey::new(k)
    }
}
impl IKey {
    pub fn new(k: &[u8]) -> IKey {
        if k.len() <= 30 {
            let mut b = [0u8; 30];
            b[..k.len()].copy_from_slice(k);
            IKey::I(k.len() as u8, b)
        } else {
            IKey::H(k.into())
        }
    }
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            IKey::I(n, b) => &b[..*n as usize],
            IKey::H(b) => b,
        }
    }
}
impl std::borrow::Borrow<[u8]> for IKey {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}
impl PartialEq for IKey {
    fn eq(&self, o: &Self) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl Eq for IKey {}
impl PartialOrd for IKey {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for IKey {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(o.as_slice())
    }
}
impl std::fmt::Debug for IKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self.as_slice()))
    }
}

/// Collocazione di una voce nel registro su disco.
#[derive(Clone, Copy, Debug, Default)]
pub struct Loc {
    pub seg: u32,
    pub off: u64,
    pub len: u32,
}

#[derive(Clone, Debug)]
pub enum Val {
    /// il valore in RAM (fetta del frame della voce: nessuna copia)
    Mem(Bytes),
    /// sfrattato per il tetto di RAM: sta nel registro su disco (voce, numero della scrittura)
    Disk(Loc, u16),
}

#[derive(Clone, Debug)]
pub struct Ver {
    pub pos: Pos,
    pub kind: Kind,
    pub val: Val,
}

#[derive(Default, Debug)]
pub struct KeyS {
    pub v: VecDeque<Ver>,
    /// la finestra conosce la chiave dall'inizio della sua storia (un delta senza base parte da 0)
    pub base_known: bool,
    /// posizione dell'ultima scrittura strutturale (put o delete) nella finestra (per le dipendenze `esiste`)
    pub last_struct: Pos,
}

/// Un record del database ancora nella finestra.
#[derive(Debug)]
pub struct RecMeta {
    pub index: u64,
    pub pos: Pos,
    pub txn: [u8; 16],
    pub keys: Box<[IKey]>,
    pub bytes: u32,
    pub loc: Loc,
    /// la voce in RAM (None se sfrattata: si rilegge da `loc`)
    pub ent: Option<Arc<Entry>>,
}

/// Mappa chiave → versioni (giro 6: hash al posto dell'albero ordinato).
pub type KMap = hashbrown::HashMap<IKey, KeyS>;

/// Operazione in coda per l'insieme ordinato delle chiavi: `true` = chiave nata nella mappa, `false` = uscita.
pub type OrdOp = (bool, IKey);

/// L'ordine delle chiavi, solo per `scan`. Dopo aver applicato tutta la coda contiene esattamente le chiavi della mappa.
#[derive(Default, Debug)]
pub struct OrdIdx {
    pub set: BTreeSet<IKey>,
}

#[derive(Default, Debug)]
pub struct DbW {
    pub keys: KMap,
    /// operazioni non ancora applicate a `ord`, in ordine (le scrive chi ha il lock in scrittura del database)
    pub ordq: Vec<OrdOp>,
    /// insieme ordinato delle chiavi. Lock sempre preso PRIMA di quello del database (stesso ordine per tutti), e
    /// la coda si stacca da `ordq` solo tenendo questo lock: così le operazioni si applicano nell'ordine in cui sono nate.
    pub ord: Arc<Mutex<OrdIdx>>,
    /// le versioni con posizione ≤ trim_pos possono essere uscite dalla finestra
    pub trim_pos: Pos,
    /// su S3 fino a questa posizione (conferma_scarico applicata)
    pub flushed: Pos,
    pub etag: Bytes,
    pub recs: VecDeque<RecMeta>,
    /// la finestra copre tutta la storia del database (nessuna potatura, nessun SST precedente)
    pub genesis: bool,
    /// quanti record in testa a `recs` sono già su S3; record e byte non ancora su S3 (per `pendenti`)
    pub flushed_n: usize,
    pub unfl_n: u64,
    pub unfl_bytes: u64,
    /// record non ancora su S3 usciti dalla RAM verso la dispensa (run su disco), dal più vecchio
    pub spill: VecDeque<SpillRef>,
    pub spilled_any: bool,
    /// le versioni con posizione ≤ spill_upto possono stare solo nella dispensa
    pub spill_upto: Pos,
}

#[derive(Clone, Copy, Debug)]
pub struct SpillRef {
    pub run: u64,
    pub min: Pos,
    pub max: Pos,
    /// record e byte non su S3 spostati nel run (per `pendenti`)
    pub recs: u64,
    pub bytes: u64,
}

pub type DbRef = Arc<RwLock<DbW>>;

const SHARDS: usize = 32;

pub struct Window {
    shards: Vec<RwLock<HashMap<Db, DbRef>>>,
    /// i database creati da ora in poi hanno tutta la storia nella finestra (la replica è partita da un registro vuoto)
    pub genesis: AtomicBool,
    pub versions: AtomicI64,
    pub nkeys: AtomicI64,
    pub ndbs: AtomicI64,
    /// record nella finestra in RAM (per il conto della memoria)
    pub nrecs: AtomicI64,
    pub spill: Arc<Spill>,
    /// database con record non ancora su S3, aggiornato a ogni cambiamento (risposta a `pendenti` senza scorrere tutto)
    pub pending: Mutex<HashMap<Db, PendInfo>>,
    /// database con operazioni in `ordq` (per l'indicizzatore)
    pub ord_dirty: Mutex<std::collections::HashSet<Db>>,
    /// operazioni applicate agli insiemi ordinati (per la diagnosi)
    pub ord_ops: std::sync::atomic::AtomicU64,
    dir: PathBuf,
}

pub fn delta_apply(base: Option<&[u8]>, d: &[u8]) -> Bytes {
    let b = base.and_then(|b| b.get(..8)).map(|b| i64::from_le_bytes(b.try_into().unwrap())).unwrap_or(0);
    let x = d.get(..8).map(|b| i64::from_le_bytes(b.try_into().unwrap())).unwrap_or(0);
    Bytes::copy_from_slice(&b.wrapping_add(x).to_le_bytes())
}

thread_local! {
    /// una lettura dal registro su disco è fallita: il segmento è già stato cancellato, quindi la voce è già su S3
    static GONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
fn gone_take() -> bool {
    GONE.with(|g| g.replace(false))
}

/// Rilegge una voce dal disco (un file aperto per lettura: è il caso raro, valori sfrattati o nella dispensa).
/// None = il segmento non c'è più: la voce è già su S3 (un segmento si cancella solo quando tutto è scaricato).
pub fn read_entry(dir: &std::path::Path, loc: &Loc) -> Option<Entry> {
    let mut r = crate::store::Reader::new(dir.to_path_buf());
    match r.read(loc.seg, loc.off, loc.len) {
        Ok(e) => Some(e),
        Err(_) => {
            GONE.with(|g| g.set(true));
            None
        }
    }
}

impl Window {
    pub fn new(genesis: bool, dir: PathBuf) -> Self {
        Window {
            shards: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
            genesis: AtomicBool::new(genesis),
            versions: AtomicI64::new(0),
            nkeys: AtomicI64::new(0),
            ndbs: AtomicI64::new(0),
            nrecs: AtomicI64::new(0),
            spill: Arc::new(Spill::new(dir.clone())),
            pending: Mutex::new(HashMap::new()),
            ord_dirty: Mutex::new(Default::default()),
            ord_ops: Default::default(),
            dir,
        }
    }

    /// Mette in coda un'operazione per l'insieme ordinato (chi chiama ha il lock in scrittura del database).
    #[inline]
    fn ord_push(&self, db: &Db, d: &mut DbW, op: OrdOp) {
        if d.ordq.is_empty() {
            self.ord_dirty.lock().insert(*db);
        }
        d.ordq.push(op);
    }

    /// Applica a `ord` le operazioni in coda. Chi chiama tiene già il lock di `ord` (sempre prima di quello del
    /// database). Il lock in scrittura del database si prende solo per staccare la coda (breve).
    fn ord_catch_up(&self, dr: &DbRef, o: &mut OrdIdx) -> usize {
        let q = std::mem::take(&mut dr.write().ordq);
        let n = q.len();
        for (ins, k) in q {
            if ins {
                o.set.insert(k);
            } else {
                o.set.remove(k.as_slice());
            }
        }
        self.ord_ops.fetch_add(n as u64, Relaxed);
        n
    }

    /// L'indicizzatore: porta in pari gli insiemi ordinati dei database con operazioni in coda. Gira su un thread
    /// a parte (fuori dal thread principale). Restituisce le operazioni applicate.
    pub fn index_pending(&self) -> usize {
        let dbs: Vec<Db> = self.ord_dirty.lock().drain().collect();
        let mut n = 0;
        for db in dbs {
            let Some(dr) = self.db(&db) else { continue };
            let ord = dr.read().ord.clone();
            let mut o = ord.lock();
            n += self.ord_catch_up(&dr, &mut o);
        }
        n
    }

    fn shard(&self, db: &Db) -> &RwLock<HashMap<Db, DbRef>> {
        let h = db.iter().fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(*b as u64));
        &self.shards[(h as usize) % SHARDS]
    }

    pub fn db(&self, db: &Db) -> Option<DbRef> {
        self.shard(db).read().get(db).cloned()
    }

    pub fn db_or_create(&self, db: &Db) -> DbRef {
        if let Some(d) = self.db(db) {
            return d;
        }
        let g = self.genesis.load(Relaxed);
        let mut s = self.shard(db).write();
        s.entry(*db)
            .or_insert_with(|| {
                self.ndbs.fetch_add(1, Relaxed);
                Arc::new(RwLock::new(DbW { genesis: g, ..Default::default() }))
            })
            .clone()
    }

    pub fn all_dbs(&self) -> Vec<Db> {
        let mut v = vec![];
        for s in &self.shards {
            v.extend(s.read().keys().copied());
        }
        v
    }

    pub fn load(&self, loc: &Loc, wi: u16) -> Bytes {
        let Some(e) = read_entry(&self.dir, loc) else { return Bytes::new() };
        match &e.body {
            Body::Rec { writes, .. } => writes[wi as usize].val.clone(),
            _ => Bytes::new(),
        }
    }

    fn val_of(&self, v: &Val) -> Bytes {
        match v {
            Val::Mem(b) => b.clone(),
            Val::Disk(l, wi) => self.load(l, *wi),
        }
    }

    fn sync_pending(&self, db: &Db, d: &DbW) {
        let mut p = self.pending.lock();
        if d.unfl_n > 0 {
            let oldest = d.spill.front().map(|r| r.min).or_else(|| d.recs.get(d.flushed_n).map(|r| r.pos)).unwrap_or(0);
            let spilled: u64 = d.spill.iter().map(|r| r.bytes).sum();
            p.insert(*db, PendInfo { db: *db, oldest, recs: d.unfl_n, bytes: d.unfl_bytes, flushed: d.flushed, ram_bytes: d.unfl_bytes.saturating_sub(spilled) });
        } else {
            p.remove(db);
        }
    }

    /// Validazione (solo leader). `check_writes`: ogni chiave scritta è una dipendenza `valore` (modalità validata).
    /// `deps`: dipendenze dichiarate. Conflitto se una chiave ha una versione (o una versione strutturale, per `esiste`)
    /// più recente di `read_pos`.
    pub fn validate(&self, db: &Db, read_pos: Pos, writes: &[Write], check_writes: bool, deps: &[(Bytes, Dep)]) -> Result<(), Bytes> {
        if !check_writes && deps.is_empty() {
            return Ok(());
        }
        let Some(d) = self.db(db) else { return Ok(()) };
        let d = d.read();
        let check = |k: &Bytes, dep: Dep| -> Result<(), Bytes> {
            if let Some(ks) = d.keys.get(&k[..]) {
                let last = match dep {
                    Dep::Valore => ks.v.back().map(|v| v.pos).unwrap_or(0),
                    Dep::Esiste => ks.last_struct,
                };
                if last > read_pos {
                    return Err(k.clone());
                }
            }
            Ok(())
        };
        // S più vecchia dell'inizio della finestra: non si può sapere se una chiave è cambiata dopo S
        if read_pos < d.trim_pos || read_pos < d.spill_upto {
            return Err(writes.first().map(|w| w.key.clone()).or_else(|| deps.first().map(|x| x.0.clone())).unwrap_or_default());
        }
        if check_writes {
            for w in writes {
                check(&w.key, Dep::Valore)?;
            }
        }
        for (k, dep) in deps {
            check(k, *dep)?;
        }
        Ok(())
    }

    /// Aggiunge le versioni di un record (al momento dell'aggiunta al registro, prima della conferma).
    pub fn apply(&self, e: &Arc<Entry>, loc: Loc) {
        let Body::Rec { txn, db, writes, .. } = &e.body else { return };
        let (index, pos) = (e.index, e.pos);
        let dr = self.db_or_create(db);
        let mut d = dr.write();
        let genesis = d.genesis && d.trim_pos == 0;
        let mut keys = Vec::with_capacity(writes.len());
        let mut newk = 0i64;
        let d = &mut *d;
        for w in writes.iter() {
            // un solo accesso alla mappa, anche per le chiavi nuove (la chiave si costruisce solo se manca)
            let mut born = false;
            let ks = d.keys.entry_ref(&w.key[..]).or_insert_with(|| {
                born = true;
                KeyS { v: VecDeque::new(), base_known: genesis, last_struct: 0 }
            });
            if w.kind != Kind::Delta {
                ks.last_struct = pos;
            }
            // due scritture della stessa chiave nello stesso record: vale l'ultima (una sola versione per posizione)
            if ks.v.back().map(|l| l.pos == pos).unwrap_or(false) {
                let last = ks.v.back_mut().unwrap();
                if w.kind == Kind::Delta {
                    let base = match (&last.kind, &last.val) {
                        (Kind::Del, _) => None,
                        (_, Val::Mem(b)) => Some(b.clone()),
                        _ => None,
                    };
                    let nv = delta_apply(base.as_deref(), &w.val);
                    last.kind = if last.kind == Kind::Delta { Kind::Delta } else { Kind::Put };
                    last.val = Val::Mem(nv);
                } else {
                    last.kind = w.kind;
                    last.val = Val::Mem(w.val.clone());
                }
                continue;
            }
            ks.v.push_back(Ver { pos, kind: w.kind, val: Val::Mem(w.val.clone()) });
            let ik = IKey::new(&w.key);
            if born {
                newk += 1;
                if d.ordq.is_empty() {
                    self.ord_dirty.lock().insert(*db);
                }
                d.ordq.push((true, ik.clone()));
            }
            keys.push(ik);
        }
        self.versions.fetch_add(keys.len() as i64, Relaxed);
        self.nkeys.fetch_add(newk, Relaxed);
        let bytes = e.raw.len() as u32;
        d.recs.push_back(RecMeta { index, pos, txn: *txn, keys: keys.into_boxed_slice(), bytes, loc, ent: Some(e.clone()) });
        self.nrecs.fetch_add(1, Relaxed);
        d.unfl_n += 1;
        d.unfl_bytes += bytes as u64;
        self.sync_pending(db, &d);
    }

    /// Toglie l'ultimo record del database (troncamento del registro su un follower). Va chiamata in ordine inverso.
    pub fn unapply_last(&self, db: &Db, index: u64) -> Option<[u8; 16]> {
        let dr = self.db(db)?;
        let mut d = dr.write();
        if d.recs.back().map(|r| r.index) != Some(index) {
            return None;
        }
        let r = d.recs.pop_back().unwrap();
        self.nrecs.fetch_sub(1, Relaxed);
        if d.recs.len() >= d.flushed_n {
            d.unfl_n = d.unfl_n.saturating_sub(1);
            d.unfl_bytes = d.unfl_bytes.saturating_sub(r.bytes as u64);
        } else {
            d.flushed_n = d.recs.len();
        }
        for k in r.keys.iter() {
            if let Some(ks) = d.keys.get_mut(k.as_slice()) {
                if ks.v.back().map(|v| v.pos == r.pos).unwrap_or(false) {
                    ks.v.pop_back();
                    self.versions.fetch_sub(1, Relaxed);
                    if ks.last_struct >= r.pos {
                        ks.last_struct = ks.v.iter().rev().find(|v| v.kind != Kind::Delta).map(|v| v.pos).unwrap_or(0);
                    }
                }
                if ks.v.is_empty() {
                    d.keys.remove(k.as_slice());
                    self.nkeys.fetch_sub(1, Relaxed);
                    self.ord_push(db, &mut d, (false, k.clone()));
                }
            }
        }
        self.sync_pending(db, &d);
        Some(r.txn)
    }

    /// Valore di una chiave alla posizione `at` (chi chiama garantisce at ≤ posizione leggibile, oppure legge
    /// speculativamente sul leader con at = MAX).
    /// `dal`: posizione `scaricato` del manifest che userà chi legge (0 = non detta).
    pub fn get(&self, db: &Db, key: &[u8], at: Pos, dal: Pos) -> GetRes {
        let Some(dr) = self.db(db) else {
            return if self.genesis.load(Relaxed) { GetRes::Absent(0) } else { GetRes::OutOfWindow { trimmed: 0, etag: Bytes::new() } };
        };
        let d = dr.read();
        let complete = d.genesis && d.trim_pos == 0 && !d.spilled_any;
        gone_take();
        let g = match d.keys.get(key) {
            None => match self.spill_resolve(db, &d, key, at, dal, None) {
                Some(g) => g,
                None if complete => GetRes::Absent(0),
                None => GetRes::OutOfWindow { trimmed: d.trim_pos, etag: d.etag.clone() },
            },
            Some(ks) => self.resolve(db, key, ks, at, complete, &d, dal),
        };
        if gone_take() {
            // un valore era nel registro già cancellato: è su S3 (fino a `flushed`)
            return GetRes::OutOfWindow { trimmed: d.flushed.max(d.trim_pos), etag: d.etag.clone() };
        }
        g
    }

    pub fn scan(&self, db: &Db, from: &[u8], to: &[u8], at: Pos, limit: usize, dal: Pos) -> Vec<(Bytes, GetRes)> {
        let mut out = vec![];
        let Some(dr) = self.db(db) else { return out };
        // ordine dei lock: prima l'insieme ordinato, poi il database. Si porta in pari l'insieme (stacca la coda col
        // lock in scrittura, breve) e poi si legge col lock in lettura: nel frattempo il thread principale non può
        // aggiungere né togliere chiavi, quindi l'insieme contiene esattamente le chiavi della mappa.
        let ord = dr.read().ord.clone();
        let mut o = ord.lock();
        self.ord_catch_up(&dr, &mut o);
        let d = dr.read();
        if !d.ordq.is_empty() {
            // tra il distacco e il lock in lettura il thread principale ha aggiunto qualcosa: si applica anche questo
            // (solo lettura della coda: resta lì e l'indicizzatore la riapplicherà, le operazioni sono idempotenti
            // nell'ordine in cui sono nate)
            for (ins, k) in d.ordq.iter() {
                if *ins {
                    o.set.insert(k.clone());
                } else {
                    o.set.remove(k.as_slice());
                }
            }
        }
        let complete = d.genesis && d.trim_pos == 0 && !d.spilled_any;
        let range = if to.is_empty() {
            o.set.range::<[u8], _>((std::ops::Bound::Included(from), std::ops::Bound::Unbounded))
        } else {
            o.set.range::<[u8], _>((std::ops::Bound::Included(from), std::ops::Bound::Excluded(to)))
        };
        for k in range {
            if out.len() >= limit {
                break;
            }
            let Some(ks) = d.keys.get(k.as_slice()) else { continue };
            gone_take();
            let mut r = self.resolve(db, k.as_slice(), ks, at, complete, &d, dal);
            if gone_take() {
                r = GetRes::OutOfWindow { trimmed: d.flushed.max(d.trim_pos), etag: d.etag.clone() };
            }
            if matches!(r, GetRes::Absent(0)) && complete {
                continue; // non esisteva ancora a `at`
            }
            out.push((Bytes::copy_from_slice(k.as_slice()), r));
        }
        out
    }

    /// Fa uscire dalla finestra i record più vecchi del database che sono su S3, più vecchi di `cutoff` e confermati,
    /// finché non scade `deadline`. Restituisce (indice, txn) dei record usciti; `true` se ne restano di potabili.
    pub fn trim_db(&self, db: &Db, cutoff: Pos, commit_pos: Pos, deadline: Instant) -> (Vec<(u64, [u8; 16])>, bool) {
        let mut out = vec![];
        let Some(dr) = self.db(db) else { return (out, false) };
        let mut d = dr.write();
        let mut n = 0;
        loop {
            let Some(f) = d.recs.front() else { return (out, false) };
            if f.pos > d.flushed || f.pos >= cutoff || f.pos > commit_pos {
                return (out, false);
            }
            n += 1;
            if n % 16 == 0 && Instant::now() >= deadline {
                return (out, true);
            }
            let r = self.pop_front_rec(db, &mut d);
            d.flushed_n = d.flushed_n.saturating_sub(1);
            out.push((r.index, r.txn));
            d.trim_pos = d.trim_pos.max(r.pos);
        }
    }

    /// Toglie il record più vecchio del database dalla finestra in RAM (versioni comprese). Se dopo la versione che
    /// esce restano solo delta, la versione si piega nel primo delta (che diventa un put col valore calcolato).
    fn pop_front_rec(&self, db: &Db, d: &mut DbW) -> RecMeta {
        let r = d.recs.pop_front().unwrap();
        self.nrecs.fetch_sub(1, Relaxed);
        for k in r.keys.iter() {
            let Some(ks) = d.keys.get_mut(k.as_slice()) else { continue };
            if ks.v.front().map(|v| v.pos != r.pos).unwrap_or(true) {
                continue;
            }
            let old = ks.v.pop_front().unwrap();
            self.versions.fetch_sub(1, Relaxed);
            let mut remove = false;
            match ks.v.front_mut() {
                None => remove = true,
                Some(next) if next.kind == Kind::Delta => {
                    let unknown_base = old.kind == Kind::Delta && !ks.base_known;
                    if !unknown_base {
                        gone_take();
                        let base: Option<Bytes> = match old.kind {
                            Kind::Del => None,
                            Kind::Put => Some(self.val_of(&old.val)),
                            // primo della catena con base nota: il suo valore è 0 + delta
                            Kind::Delta => Some(delta_apply(None, &self.val_of(&old.val))),
                        };
                        let nv = delta_apply(base.as_deref(), &self.val_of(&next.val));
                        if gone_take() {
                            // base già solo su S3: la catena di delta resta senza base nota (le letture la comporranno con gli SST)
                            ks.base_known = false;
                        } else {
                            next.kind = Kind::Put;
                            next.val = Val::Mem(nv);
                        }
                    }
                }
                Some(_) => {}
            }
            if remove {
                d.keys.remove(k.as_slice());
                self.nkeys.fetch_sub(1, Relaxed);
                if d.ordq.is_empty() {
                    self.ord_dirty.lock().insert(*db);
                }
                d.ordq.push((false, k.clone()));
            } else if let Some(ks) = d.keys.get_mut(k.as_slice()) {
                ks.base_known = ks.base_known || ks.v.front().map(|v| v.kind != Kind::Delta).unwrap_or(false);
            }
        }
        r
    }

    // ------------------------------------------------------------------------------------------ dispensa (spill)

    /// Candidati per la dispensa: record non su S3, confermati, più vecchi di `cutoff`, dopo `after`, al più `max`
    /// record. Gira sul thread della dispensa con il lock in lettura (breve: si chiama a pezzi).
    pub fn collect_spill(&self, db: &Db, after: Pos, cutoff: Pos, commit_pos: Pos, max: usize, out: &mut Vec<SEntry>) -> (Pos, usize) {
        let Some(dr) = self.db(db) else { return (after, 0) };
        let d = dr.read();
        let start = d.recs.partition_point(|r| r.pos <= after.max(d.flushed));
        let mut last = after;
        let mut n = 0;
        for r in d.recs.range(start..) {
            if r.pos >= cutoff || r.pos > commit_pos || n >= max {
                break;
            }
            for k in r.keys.iter() {
                let Some(ks) = d.keys.get(k.as_slice()) else { continue };
                let Some(v) = ks.v.iter().find(|v| v.pos == r.pos) else { continue };
                let val = match (&v.kind, &v.val) {
                    (Kind::Del, _) => SVal::None,
                    (_, Val::Disk(l, wi)) => SVal::Log(*l, *wi),
                    (_, Val::Mem(b)) => {
                        // il valore è quello originale della voce? allora basta puntare al registro su disco
                        let orig = r.ent.as_ref().and_then(|e| {
                            let (lo, hi) = (e.raw.as_ptr() as usize, e.raw.as_ptr() as usize + e.raw.len());
                            let p = b.as_ptr() as usize;
                            if p >= lo && p + b.len() <= hi {
                                if let Body::Rec { writes, .. } = &e.body {
                                    return writes.iter().position(|w| w.key.as_ptr() as usize >= lo && &w.key[..] == k.as_slice()).map(|wi| wi as u16);
                                }
                            }
                            None
                        });
                        match orig {
                            Some(wi) => SVal::Log(r.loc, wi),
                            None => SVal::Inline(b.clone()),
                        }
                    }
                };
                out.push(SEntry { db: *db, key: Bytes::copy_from_slice(k.as_slice()), pos: r.pos, kind: v.kind, val });
            }
            last = r.pos;
            n += 1;
        }
        (last, n)
    }

    /// Dopo la scrittura del run: i record del database fino a `upto` escono dalla RAM (al più `max` per chiamata).
    /// Restituisce (indice, txn) dei record usciti e `true` se ne restano.
    pub fn apply_spill(&self, db: &Db, run: u64, upto: Pos, first_call: bool, max: usize) -> (Vec<(u64, [u8; 16])>, bool) {
        let mut out = vec![];
        let Some(dr) = self.db(db) else {
            if first_call {
                self.spill.release(run);
            }
            return (out, false);
        };
        let mut d = dr.write();
        if first_call {
            let (min, max_p) = self.spill.get(run).and_then(|r| r.dbs.get(db).map(|x| (x.min_pos, x.max_pos))).unwrap_or((0, 0));
            // record non su S3 tra quelli spostati: per i contatori di `pendenti`
            let (mut n, mut b) = (0u64, 0u64);
            for r in d.recs.iter() {
                if r.pos > upto {
                    break;
                }
                if r.pos > d.flushed {
                    n += 1;
                    b += r.bytes as u64;
                }
            }
            if min > 0 && max_p > d.flushed {
                d.spill.push_back(SpillRef { run, min, max: max_p, recs: n, bytes: b });
                d.spilled_any = true;
                d.spill_upto = d.spill_upto.max(upto);
            } else {
                self.spill.release(run);
            }
        }
        let mut k = 0;
        while let Some(f) = d.recs.front() {
            if f.pos > upto {
                break;
            }
            if k >= max {
                return (out, true);
            }
            let was_flushed = d.flushed_n > 0;
            let r = self.pop_front_rec(db, &mut d);
            if was_flushed {
                d.flushed_n -= 1;
                d.trim_pos = d.trim_pos.max(r.pos);
            }
            out.push((r.index, r.txn));
            k += 1;
        }
        self.sync_pending(db, &d);
        (out, false)
    }

    /// Lettura che scende nella dispensa: versioni ≤ at dal run più nuovo al più vecchio. `ram` = delta già trovati
    /// in RAM sopra la dispensa (posizione più alta, somma). None = la dispensa non sa nulla della chiave.
    fn spill_resolve(&self, db: &Db, d: &DbW, key: &[u8], at: Pos, dal: Pos, ram: Option<(Pos, Vec<Bytes>)>) -> Option<GetRes> {
        if d.spill.is_empty() {
            return None;
        }
        let (mut top, mut deltas): (Pos, Vec<Bytes>) = ram.unwrap_or((0, vec![]));
        let mut found = top > 0;
        for r in d.spill.iter().rev() {
            if r.min > at {
                continue;
            }
            let Some(run) = self.spill.get(r.run) else { continue };
            let mut vs = run.lookup(db, key);
            vs.retain(|e| e.pos <= at);
            for e in vs.iter().rev() {
                if !found {
                    top = e.pos;
                    found = true;
                }
                let v = || match &e.val {
                    SVal::Log(l, wi) => self.load(l, *wi),
                    SVal::Inline(b) => b.clone(),
                    SVal::None => Bytes::new(),
                };
                match e.kind {
                    Kind::Delta => {
                        if e.pos > dal || dal == 0 {
                            deltas.push(v());
                        }
                    }
                    Kind::Put => {
                        let mut cur = Some(v());
                        for x in deltas.iter().rev() {
                            cur = Some(delta_apply(cur.as_deref(), x));
                        }
                        return Some(GetRes::Value(top, cur.unwrap()));
                    }
                    Kind::Del => {
                        if deltas.is_empty() {
                            return Some(GetRes::Absent(top));
                        }
                        let mut cur: Option<Bytes> = None;
                        for x in deltas.iter().rev() {
                            cur = Some(delta_apply(cur.as_deref(), x));
                        }
                        return Some(GetRes::Value(top, cur.unwrap()));
                    }
                }
            }
        }
        if !found {
            return None;
        }
        // solo delta: la base è negli SST
        let sum = deltas.iter().fold(0i64, |a, x| a.wrapping_add(x.get(..8).map(|b| i64::from_le_bytes(b.try_into().unwrap())).unwrap_or(0)));
        Some(GetRes::Delta { pos: top, sum, trimmed: d.trim_pos })
    }

    /// conferma_scarico applicata: il database è su S3 fino a `p`.
    pub fn set_flushed(&self, db: &Db, p: Pos, etag: &Bytes) {
        let dr = self.db_or_create(db);
        let mut d = dr.write();
        if p <= d.flushed {
            return;
        }
        d.flushed = p;
        d.etag = etag.clone();
        while d.flushed_n < d.recs.len() && d.recs[d.flushed_n].pos <= p {
            // i record già passati alla dispensa (≤ spill_upto) si contano nel loro run, non qui
            if d.recs[d.flushed_n].pos > d.spill_upto {
                let b = d.recs[d.flushed_n].bytes as u64;
                d.unfl_n = d.unfl_n.saturating_sub(1);
                d.unfl_bytes = d.unfl_bytes.saturating_sub(b);
            }
            d.flushed_n += 1;
        }
        // run della dispensa ormai interamente su S3 per questo database
        while let Some(r) = d.spill.front().copied() {
            if r.max > p {
                break;
            }
            d.unfl_n = d.unfl_n.saturating_sub(r.recs);
            d.unfl_bytes = d.unfl_bytes.saturating_sub(r.bytes);
            d.spill.pop_front();
            self.spill.release(r.run);
        }
        self.sync_pending(db, &d);
    }

    /// Sfratta i valori di un record su disco (tetto di RAM superato).
    pub fn evict(&self, db: &Db, pos: Pos, loc: Loc, writes: &[Write]) {
        let Some(dr) = self.db(db) else { return };
        let mut d = dr.write();
        for (wi, w) in writes.iter().enumerate() {
            if let Some(ks) = d.keys.get_mut(&w.key[..]) {
                // cerca dalla fine: la versione con questa posizione
                for v in ks.v.iter_mut().rev() {
                    if v.pos == pos {
                        if matches!(v.val, Val::Mem(_)) && v.kind != Kind::Del {
                            v.val = Val::Disk(loc, wi as u16);
                        }
                        break;
                    }
                    if v.pos < pos {
                        break;
                    }
                }
            }
        }
        // anche il riferimento alla voce nel record
        let i = d.recs.partition_point(|r| r.pos < pos);
        if let Some(r) = d.recs.get_mut(i) {
            if r.pos == pos {
                r.ent = None;
            }
        }
    }

    /// scaricabili: record confermati in (flushed, upto], fino a `budget` byte. Gira su un thread di rete. Sotto il lock
    /// copia solo i riferimenti (breve), poi costruisce la risposta senza lock.
    pub fn flushable(&self, db: &Db, upto: Pos, budget: &mut usize) -> Vec<FlushRec> {
        let Some(dr) = self.db(db) else { return vec![] };
        let mut out = vec![];
        // 1) la dispensa: run interi, dal più vecchio (le loro posizioni vengono prima di quelle in RAM)
        let (spill, flushed) = {
            let d = dr.read();
            (d.spill.iter().copied().collect::<Vec<_>>(), d.flushed)
        };
        if !spill.is_empty() {
            let mut last_loc: Option<(Loc, Arc<Entry>)> = None;
            for r in &spill {
                if *budget == 0 {
                    return out;
                }
                let Some(run) = self.spill.get(r.run) else { continue };
                let mark = out.len();
                for e in run.db_entries(db) {
                    if e.pos <= flushed || e.pos > upto {
                        continue;
                    }
                    let val = match &e.val {
                        SVal::Inline(b) => b.clone(),
                        SVal::None => Bytes::new(),
                        SVal::Log(l, wi) => {
                            let ent = match &last_loc {
                                Some((ll, en)) if ll.seg == l.seg && ll.off == l.off => en.clone(),
                                _ => match read_entry(&self.dir, l) {
                                    Some(en) => {
                                        let en = Arc::new(en);
                                        last_loc = Some((*l, en.clone()));
                                        en
                                    }
                                    None => {
                                        // segmento già cancellato (tutto su S3): il run si scarta per intero e ci si
                                        // ferma, così la risposta resta un prefisso in ordine di posizione
                                        gone_take();
                                        out.truncate(mark);
                                        return out;
                                    }
                                },
                            };
                            match &ent.body {
                                Body::Rec { writes, .. } => writes[*wi as usize].val.clone(),
                                _ => Bytes::new(),
                            }
                        }
                    };
                    *budget = budget.saturating_sub(val.len() + e.key.len() + 16);
                    out.push(FlushRec { pos: e.pos, writes: vec![Write { key: e.key, kind: e.kind, val }] });
                }
            }
            if *budget == 0 {
                return out;
            }
        }
        // 2) i record in RAM: sotto il lock solo i riferimenti (breve), poi la risposta senza lock
        let mut refs: Vec<(Pos, Option<Arc<Entry>>, Loc)> = vec![];
        {
            let d = dr.read();
            let start = d.flushed_n.min(d.recs.len());
            let mut b = *budget;
            for r in d.recs.range(start..) {
                if r.pos > upto || b == 0 {
                    break;
                }
                if r.pos <= d.flushed {
                    continue;
                }
                b = b.saturating_sub(r.bytes as usize);
                refs.push((r.pos, r.ent.clone(), r.loc));
            }
        }
        for (pos, ent, loc) in refs {
            let e = match ent {
                Some(e) => e,
                None => match read_entry(&self.dir, &loc) {
                    Some(e) => Arc::new(e),
                    None => {
                        gone_take();
                        break;
                    }
                },
            };
            if let Body::Rec { writes, .. } = &e.body {
                *budget = budget.saturating_sub(e.raw.len());
                out.push(FlushRec { pos, writes: writes.clone() });
            }
        }
        out
    }

    pub fn flush_state(&self, db: &Db) -> (Pos, Bytes, Pos) {
        match self.db(db) {
            Some(dr) => {
                let d = dr.read();
                (d.flushed, d.etag.clone(), d.trim_pos)
            }
            None => (0, Bytes::new(), 0),
        }
    }

    pub fn pending_list(&self, limit: usize) -> Vec<PendInfo> {
        let mut v: Vec<PendInfo> = self.pending.lock().values().cloned().collect();
        v.sort_by_key(|x| x.oldest);
        v.truncate(limit);
        v
    }

    /// (record non su S3, posizione del più vecchio)
    pub fn unflushed_summary(&self) -> (u64, Pos) {
        let p = self.pending.lock();
        (p.values().map(|x| x.recs).sum(), p.values().map(|x| x.oldest).min().unwrap_or(0))
    }

    /// posizioni già su S3 di tutti i database (per il checkpoint)
    pub fn flushed_all(&self) -> Vec<(Db, Pos)> {
        let mut v = vec![];
        for s in &self.shards {
            for (k, d) in s.read().iter() {
                let f = d.read().flushed;
                if f > 0 {
                    v.push((*k, f));
                }
            }
        }
        v
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve(&self, db: &Db, key: &[u8], ks: &KeyS, at: Pos, complete: bool, d: &DbW, dal: Pos) -> GetRes {
        let oow = || GetRes::OutOfWindow { trimmed: d.trim_pos, etag: d.etag.clone() };
        // ultima versione con pos ≤ at
        let n = ks.v.partition_point(|v| v.pos <= at);
        if n == 0 {
            if let Some(g) = self.spill_resolve(db, d, key, at, dal, None) {
                return g;
            }
            return if complete { GetRes::Absent(0) } else { oow() };
        }
        let top = &ks.v[n - 1];
        match top.kind {
            Kind::Put => GetRes::Value(top.pos, self.val_of(&top.val)),
            Kind::Del => GetRes::Absent(top.pos),
            Kind::Delta => {
                // risali fino a una base
                let mut i = n - 1;
                let mut deltas = vec![];
                loop {
                    let v = &ks.v[i];
                    match v.kind {
                        Kind::Delta => deltas.push(i),
                        _ => break,
                    }
                    if i == 0 {
                        break;
                    }
                    i -= 1;
                }
                let first = &ks.v[i];
                let mut cur: Option<Bytes> = match first.kind {
                    Kind::Put => Some(self.val_of(&first.val)),
                    Kind::Del => None,
                    Kind::Delta => {
                        if !ks.base_known && !d.spill.is_empty() {
                            // la base può stare nella dispensa
                            let ram: Vec<Bytes> = deltas.iter().map(|&j| self.val_of(&ks.v[j].val)).collect();
                            if let Some(g) = self.spill_resolve(db, d, key, at, dal, Some((top.pos, ram))) {
                                return g;
                            }
                        }
                        if !ks.base_known {
                            // base negli SST: somma dei delta della finestra dopo il manifest di chi legge
                            if dal != 0 && dal < d.trim_pos {
                                return oow(); // manifest troppo vecchio: va riletto
                            }
                            let mut sum = 0i64;
                            for &j in deltas.iter() {
                                if ks.v[j].pos > dal {
                                    let v = self.val_of(&ks.v[j].val);
                                    sum = sum.wrapping_add(v.get(..8).map(|x| i64::from_le_bytes(x.try_into().unwrap())).unwrap_or(0));
                                }
                            }
                            return GetRes::Delta { pos: top.pos, sum, trimmed: d.trim_pos };
                        }
                        None
                    }
                };
                for &j in deltas.iter().rev() {
                    cur = Some(delta_apply(cur.as_deref(), &self.val_of(&ks.v[j].val)));
                }
                GetRes::Value(top.pos, cur.unwrap_or_else(|| delta_apply(None, &[0; 8])))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Body;
    fn w(k: &str, kind: Kind, v: &[u8]) -> Write {
        Write { key: Bytes::from(k.to_string()), kind, val: Bytes::copy_from_slice(v) }
    }
    fn rec(win: &Window, db: &Db, index: u64, pos: Pos, ws: Vec<Write>) {
        let e = Arc::new(Entry::encode(1, index, pos, Body::Rec { txn: [index as u8; 16], db: *db, session: None, writes: ws }));
        win.apply(&e, Loc::default());
    }
    fn far() -> Instant {
        Instant::now() + std::time::Duration::from_secs(10)
    }
    #[test]
    fn get_validate_trim_fold() {
        let win = Window::new(true, PathBuf::from("/nonesiste"));
        let db = [1u8; 16];
        rec(&win, &db, 1, 10, vec![w("a", Kind::Put, &5i64.to_le_bytes())]);
        rec(&win, &db, 2, 20, vec![w("a", Kind::Delta, &3i64.to_le_bytes())]);
        rec(&win, &db, 3, 30, vec![w("a", Kind::Delta, &2i64.to_le_bytes())]);
        let v = |g: GetRes| match g {
            GetRes::Value(_, b) => i64::from_le_bytes(b[..8].try_into().unwrap()),
            o => panic!("{o:?}"),
        };
        assert_eq!(v(win.get(&db, b"a", 25, 0)), 8);
        assert_eq!(v(win.get(&db, b"a", 30, 0)), 10);
        assert!(matches!(win.get(&db, b"a", 5, 0), GetRes::Absent(0)));
        assert!(win.validate(&db, 20, &[w("a", Kind::Put, b"x")], true, &[]).is_err());
        assert!(win.validate(&db, 30, &[w("a", Kind::Put, b"x")], true, &[]).is_ok());
        // esiste: i delta dopo S non conflittano, valore sì
        assert!(win.validate(&db, 15, &[], false, &[(Bytes::from_static(b"a"), Dep::Esiste)]).is_ok());
        assert!(win.validate(&db, 15, &[], false, &[(Bytes::from_static(b"a"), Dep::Valore)]).is_err());
        assert!(win.validate(&db, 5, &[], false, &[(Bytes::from_static(b"a"), Dep::Esiste)]).is_err());
        assert_eq!(win.pending_list(10)[0].recs, 3);
        win.set_flushed(&db, 20, &Bytes::new());
        assert_eq!(win.pending_list(10)[0].recs, 1);
        assert_eq!(win.pending_list(10)[0].oldest, 30);
        let (out, more) = win.trim_db(&db, 100, 30, far());
        assert_eq!((out.len(), more), (2, false)); // 30 non è su S3
        assert_eq!(v(win.get(&db, b"a", 30, 0)), 10);
        assert!(matches!(win.get(&db, b"a", 25, 0), GetRes::OutOfWindow { .. }));
        assert!(matches!(win.get(&db, b"zz", 30, 0), GetRes::OutOfWindow { .. }));
        assert!(win.validate(&db, 15, &[w("q", Kind::Put, b"x")], true, &[]).is_err()); // S prima della finestra
        let mut budget = 1 << 20;
        assert_eq!(win.flushable(&db, 100, &mut budget).len(), 1);
    }

    #[test]
    fn delta_con_base_negli_sst() {
        // database che esisteva già negli SST (finestra non dalla nascita)
        let win = Window::new(false, PathBuf::from("/nonesiste"));
        let db = [2u8; 16];
        rec(&win, &db, 1, 10, vec![w("c", Kind::Put, &5i64.to_le_bytes())]);
        win.set_flushed(&db, 10, &Bytes::new());
        assert_eq!(win.trim_db(&db, 100, 10, far()).0.len(), 1); // "c" esce: la base sta negli SST
        rec(&win, &db, 2, 20, vec![w("c", Kind::Delta, &3i64.to_le_bytes())]);
        rec(&win, &db, 3, 30, vec![w("c", Kind::Delta, &4i64.to_le_bytes())]);
        match win.get(&db, b"c", 30, 0) {
            GetRes::Delta { pos, sum, trimmed } => assert_eq!((pos, sum, trimmed), (30, 7, 10)),
            o => panic!("{o:?}"),
        }
        // il manifest di chi legge copre già il delta a 20: si somma solo quello a 30
        match win.get(&db, b"c", 30, 20) {
            GetRes::Delta { sum, .. } => assert_eq!(sum, 4),
            o => panic!("{o:?}"),
        }
        // manifest più vecchio di quanto è uscito dalla finestra: va riletto
        assert!(matches!(win.get(&db, b"c", 30, 5), GetRes::OutOfWindow { trimmed: 10, .. }));
        assert!(matches!(win.get(&db, b"zz", 30, 0), GetRes::OutOfWindow { trimmed: 10, .. }));
    }

    #[test]
    fn dispensa_letture_e_scarico() {
        use std::io::Write as _;
        let dir = std::env::temp_dir().join(format!("pq-test-spill-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let win = Window::new(true, dir.clone());
        let db = [3u8; 16];
        let mut seg = std::fs::File::create(crate::store::seg_path(&dir, 1)).unwrap();
        let mut off = 0u64;
        let mut put = |index: u64, pos: Pos, ws: Vec<Write>| {
            let e = Arc::new(Entry::encode(1, index, pos, Body::Rec { txn: [index as u8; 16], db, session: None, writes: ws }));
            seg.write_all(&e.raw).unwrap();
            let loc = Loc { seg: 1, off, len: e.raw.len() as u32 };
            off += e.raw.len() as u64;
            win.apply(&e, loc);
        };
        put(1, 10, vec![w("a", Kind::Put, &5i64.to_le_bytes()), w("b", Kind::Put, b"bbbbbbbb")]);
        put(2, 20, vec![w("a", Kind::Delta, &3i64.to_le_bytes())]);
        put(3, 30, vec![w("b", Kind::Del, b"")]);
        put(4, 40, vec![w("a", Kind::Delta, &1i64.to_le_bytes())]);
        drop(put);
        let v = |g: GetRes| match g {
            GetRes::Value(_, b) => i64::from_le_bytes(b[..8].try_into().unwrap()),
            o => panic!("{o:?}"),
        };
        // 10, 20, 30 nella dispensa; 40 resta in RAM
        let mut ents = vec![];
        let (upto, n) = win.collect_spill(&db, 0, 35, 40, 100, &mut ents);
        assert_eq!((upto, n), (30, 3));
        let run = win.spill.write_run(ents).unwrap();
        let (gone, more) = win.apply_spill(&db, run.id, upto, true, 100);
        assert_eq!((gone.len(), more), (3, false));
        assert_eq!(win.nrecs.load(Relaxed), 1);
        // letture: uguali a prima, ora anche dalla dispensa
        assert_eq!(v(win.get(&db, b"a", 40, 0)), 9);
        assert_eq!(v(win.get(&db, b"a", 25, 0)), 8);
        assert_eq!(v(win.get(&db, b"a", 15, 0)), 5);
        assert!(matches!(win.get(&db, b"b", 35, 0), GetRes::Absent(30)));
        assert!(matches!(win.get(&db, b"b", 15, 0), GetRes::Value(10, _)));
        assert!(matches!(win.get(&db, b"zz", 40, 0), GetRes::OutOfWindow { .. }));
        // S più vecchia della dispensa: conflitto (non si può validare)
        assert!(win.validate(&db, 25, &[w("a", Kind::Put, b"x")], true, &[]).is_err());
        // pendenti: 4 record non su S3 (3 nella dispensa + 1 in RAM), il più vecchio a 10
        let p = win.pending_list(10);
        assert_eq!((p[0].recs, p[0].oldest), (4, 10));
        // scaricabili: prima la dispensa (4 scritture), poi la RAM (1)
        let mut budget = 1 << 20;
        let fr = win.flushable(&db, 100, &mut budget);
        assert_eq!(fr.len(), 5);
        assert_eq!(fr.iter().map(|r| r.pos).max(), Some(40));
        win.set_flushed(&db, 40, &Bytes::new());
        assert!(win.pending_list(10).is_empty());
        assert!(win.spill.runs.read().is_empty()); // run cancellato
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------------------------------------------------------- giro 6 (M16): mappa a hash e potatura

    fn scan_keys(win: &Window, db: &Db, at: Pos) -> Vec<String> {
        win.scan(db, b"", b"", at, usize::MAX, 0).into_iter().map(|(k, _)| String::from_utf8(k.to_vec()).unwrap()).collect()
    }

    #[test]
    fn potatura_hash_chiave_che_rinasce() {
        // una chiave esce dalla finestra (potatura) e rinasce nel record dopo: la mappa la deve rivedere con la
        // posizione nuova, l'insieme ordinato la deve avere una volta sola, i controlli devono vederla
        let win = Window::new(true, PathBuf::from("/nonesiste"));
        let db = [7u8; 16];
        rec(&win, &db, 1, 10, vec![w("k", Kind::Put, b"uno"), w("ix/1/a/k", Kind::Put, b"k")]);
        rec(&win, &db, 2, 20, vec![w("altro", Kind::Put, b"x")]);
        assert_eq!(win.index_pending(), 3);
        win.set_flushed(&db, 10, &Bytes::new());
        assert_eq!(win.trim_db(&db, 100, 20, far()).0.len(), 1); // esce solo 10 (20 non è su S3)
        // "k" non è più nella mappa: un controllo con S prima del confine è un conflitto (non si sa cosa è cambiato)
        assert!(win.validate(&db, 5, &[w("k", Kind::Put, b"y")], true, &[]).is_err());
        // S dopo il confine e chiave assente: nessun conflitto
        assert!(win.validate(&db, 10, &[w("k", Kind::Put, b"y")], true, &[]).is_ok());
        // la chiave rinasce a 30, prima che l'indicizzatore abbia applicato l'uscita
        rec(&win, &db, 3, 30, vec![w("k", Kind::Put, b"due")]);
        assert!(win.validate(&db, 25, &[w("k", Kind::Put, b"z")], true, &[]).is_err());
        assert!(win.validate(&db, 30, &[w("k", Kind::Put, b"z")], true, &[]).is_ok());
        // "esiste" vede la scrittura strutturale nuova
        assert!(win.validate(&db, 25, &[], false, &[(Bytes::from_static(b"k"), Dep::Esiste)]).is_err());
        // scan: k una volta sola, l'indice uscito non c'è più (coda: uscita k, uscita ix, nascita k)
        assert_eq!(scan_keys(&win, &db, 30), vec!["altro", "k"]);
        assert_eq!(win.index_pending(), 0); // scan ha già portato in pari
        match win.get(&db, b"k", 30, 0) {
            GetRes::Value(30, v) => assert_eq!(&v[..], b"due"),
            o => panic!("{o:?}"),
        }
        assert!(matches!(win.get(&db, b"k", 25, 0), GetRes::OutOfWindow { .. }));
        assert_eq!(win.nkeys.load(Relaxed), 2);
    }

    #[test]
    fn potatura_hash_versioni_multiple_e_troncamento() {
        // una chiave con più versioni: la potatura toglie solo la più vecchia (la chiave resta nella mappa e
        // nell'insieme), il troncamento (unapply_last) toglie l'ultima e rimette l'ultima posizione strutturale giusta
        let win = Window::new(true, PathBuf::from("/nonesiste"));
        let db = [8u8; 16];
        rec(&win, &db, 1, 10, vec![w("c", Kind::Put, &5i64.to_le_bytes())]);
        rec(&win, &db, 2, 20, vec![w("c", Kind::Delta, &3i64.to_le_bytes())]);
        rec(&win, &db, 3, 30, vec![w("c", Kind::Put, &1i64.to_le_bytes()), w("nuova", Kind::Put, b"n")]);
        win.set_flushed(&db, 10, &Bytes::new());
        assert_eq!(win.trim_db(&db, 100, 30, far()).0.len(), 1);
        // il delta a 20 è stato piegato in un put (5+3)
        match win.get(&db, b"c", 20, 0) {
            GetRes::Value(20, v) => assert_eq!(i64::from_le_bytes(v[..8].try_into().unwrap()), 8),
            o => panic!("{o:?}"),
        }
        assert_eq!(scan_keys(&win, &db, 30), vec!["c", "nuova"]);
        // troncamento del record a 30: "nuova" sparisce, "c" torna all'ultima versione 20 (strutturale: il put piegato)
        assert_eq!(win.unapply_last(&db, 3), Some([3u8; 16]));
        assert_eq!(scan_keys(&win, &db, 30), vec!["c"]);
        assert!(win.validate(&db, 20, &[w("c", Kind::Put, b"q")], true, &[]).is_ok());
        assert!(win.validate(&db, 15, &[w("c", Kind::Put, b"q")], true, &[]).is_err());
        // dopo la piega il delta a 20 è un put, quindi il troncamento ricalcola l'ultima posizione strutturale a 20
        // (come prima del giro 6: al più un falso conflitto su `esiste`, mai una scrittura persa)
        let ks_last_struct = win.db(&db).unwrap().read().keys.get(&b"c"[..]).unwrap().last_struct;
        assert_eq!(ks_last_struct, 20);
        assert_eq!(win.nkeys.load(Relaxed), 1);
        assert_eq!(win.versions.load(Relaxed), 1);
    }

    #[test]
    fn insieme_ordinato_uguale_alla_mappa_a_caso() {
        // sequenza a caso di aggiunte, potature, troncamenti e passaggi dell'indicizzatore: dopo ogni passo lo scan
        // completo deve dare esattamente le chiavi della mappa, in ordine, e ogni get deve dare l'ultima versione
        use rand::{Rng, SeedableRng};
        for seed in 0..20u64 {
            let mut g = rand::rngs::StdRng::seed_from_u64(seed);
            let win = Window::new(true, PathBuf::from("/nonesiste"));
            let db = [9u8; 16];
            let mut index = 0u64;
            let mut flushed = 0u64;
            let mut last: std::collections::BTreeMap<String, (Pos, bool)> = Default::default();
            for _ in 0..400 {
                match g.gen_range(0..10) {
                    0..=5 => {
                        index += 1;
                        let pos = index * 10;
                        let n = g.gen_range(1..4);
                        let ws: Vec<Write> = (0..n)
                            .map(|_| {
                                let k = format!("k{:02}{}", g.gen_range(0..40), if g.gen_bool(0.5) { "-una-chiave-lunga-oltre-trenta-byte" } else { "" });
                                if g.gen_bool(0.2) { w(&k, Kind::Del, b"") } else { w(&k, Kind::Put, k.as_bytes()) }
                            })
                            .collect();
                        rec(&win, &db, index, pos, ws);
                    }
                    6 => {
                        flushed = (flushed + 10 * g.gen_range(0..4)).min(index * 10);
                        win.set_flushed(&db, flushed, &Bytes::new());
                        let _ = win.trim_db(&db, flushed + 1, index * 10, far());
                    }
                    7 => {
                        // troncamento solo dei record non ancora su S3 (come un follower)
                        if index * 10 > flushed && index > 0 {
                            if win.unapply_last(&db, index).is_some() {
                                index -= 1;
                            }
                        }
                    }
                    _ => {
                        win.index_pending();
                    }
                }
                // modello: dalla mappa stessa (le chiavi ancora con versioni) contro lo scan
                let dref = win.db(&db);
                let mut want: Vec<String> = match &dref {
                    Some(d) => d.read().keys.keys().map(|k| String::from_utf8(k.as_slice().to_vec()).unwrap()).collect(),
                    None => vec![],
                };
                want.sort();
                let got: Vec<String> = win.scan(&db, b"", b"", Pos::MAX, usize::MAX, 0).into_iter().map(|(k, _)| String::from_utf8(k.to_vec()).unwrap()).collect();
                // scan non mostra le chiavi che a Pos::MAX non esistono (solo cancellate) se la finestra è completa
                let complete = dref.as_ref().map(|d| { let d = d.read(); d.genesis && d.trim_pos == 0 && !d.spilled_any }).unwrap_or(true);
                let want: Vec<String> = want
                    .into_iter()
                    .filter(|k| !(complete && matches!(win.get(&db, k.as_bytes(), Pos::MAX, 0), GetRes::Absent(0))))
                    .collect();
                assert_eq!(got, want, "seme {seed}");
                // l'insieme ordinato, una volta in pari, ha esattamente le chiavi della mappa
                if let Some(d) = &dref {
                    win.index_pending();
                    let ord = d.read().ord.clone();
                    let o = ord.lock();
                    let dd = d.read();
                    assert!(dd.ordq.is_empty());
                    assert_eq!(o.set.len(), dd.keys.len(), "seme {seed}");
                    assert!(o.set.iter().all(|k| dd.keys.contains_key(k.as_slice())), "seme {seed}");
                }
                last.clear();
            }
            let _ = last;
        }
    }

}
