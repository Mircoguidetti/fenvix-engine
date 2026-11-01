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

use crate::codec::{Db, Dep, GetRes, Kind, Pos, Write};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, VecDeque};

/// Chiave della finestra: fino a 30 byte sta dentro il nodo dell'albero (i confronti non seguono puntatori, che
/// costavano un salto di cache ciascuno: era il 16% della CPU del leader), oltre va sullo heap.
#[derive(Clone)]
pub enum IKey {
    I(u8, [u8; 30]),
    H(Box<[u8]>),
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

#[derive(Clone, Debug)]
pub enum Val {
    /// il valore in RAM (fetta del frame della voce: nessuna copia)
    Mem(Bytes),
    /// sfrattato per il tetto di RAM: sta nel registro su disco (indice della voce, numero della scrittura)
    Disk(u64, u16),
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
}

#[derive(Default, Debug)]
pub struct DbW {
    pub keys: BTreeMap<IKey, KeyS>,
    /// le versioni con posizione ≤ trim_pos possono essere uscite dalla finestra
    pub trim_pos: Pos,
    /// su S3 fino a questa posizione (conferma_scarico applicata)
    pub flushed: Pos,
    pub etag: Bytes,
    pub recs: VecDeque<RecMeta>,
    /// la finestra copre tutta la storia del database (nessuna potatura, nessun SST precedente)
    pub genesis: bool,
}

pub struct Window {
    pub dbs: HashMap<Db, DbW>,
    /// i database creati da ora in poi hanno tutta la storia nella finestra (la replica è partita da un registro vuoto)
    pub genesis: bool,
    pub versions: u64,
}

/// Legge un valore sfrattato su disco.
pub type Loader<'a> = &'a mut dyn FnMut(u64, u16) -> Bytes;

pub fn delta_apply(base: Option<&[u8]>, d: &[u8]) -> Bytes {
    let b = base.and_then(|b| b.get(..8)).map(|b| i64::from_le_bytes(b.try_into().unwrap())).unwrap_or(0);
    let x = d.get(..8).map(|b| i64::from_le_bytes(b.try_into().unwrap())).unwrap_or(0);
    Bytes::copy_from_slice(&b.wrapping_add(x).to_le_bytes())
}

impl Window {
    pub fn new(genesis: bool) -> Self {
        Window { dbs: HashMap::new(), genesis, versions: 0 }
    }

    fn db_mut(&mut self, db: &Db) -> &mut DbW {
        let g = self.genesis;
        self.dbs.entry(*db).or_insert_with(|| DbW { genesis: g, ..Default::default() })
    }

    /// Validazione (solo leader). `check_writes`: ogni chiave scritta è una dipendenza `valore` (modalità validata).
    /// `deps`: dipendenze dichiarate. Conflitto se una chiave ha una versione (o una versione strutturale, per `esiste`)
    /// più recente di `read_pos`.
    pub fn validate(&self, db: &Db, read_pos: Pos, writes: &[Write], check_writes: bool, deps: &[(Bytes, Dep)]) -> Result<(), Bytes> {
        if !check_writes && deps.is_empty() {
            return Ok(());
        }
        let Some(d) = self.dbs.get(db) else { return Ok(()) };
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
        if read_pos < d.trim_pos {
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
    pub fn apply(&mut self, index: u64, pos: Pos, txn: [u8; 16], db: &Db, writes: &[Write], bytes: u32) {
        let d = self.db_mut(db);
        let genesis = d.genesis && d.trim_pos == 0;
        let mut keys = Vec::with_capacity(writes.len());
        for w in writes.iter() {
            // una sola discesa nell'albero, anche per le chiavi nuove
            let ks = d.keys.entry(IKey::new(&w.key)).or_insert_with(|| KeyS { v: VecDeque::new(), base_known: genesis, last_struct: 0 });
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
            keys.push(IKey::new(&w.key));
        }
        self.versions += keys.len() as u64;
        let d = self.dbs.get_mut(db).unwrap();
        d.recs.push_back(RecMeta { index, pos, txn, keys: keys.into_boxed_slice(), bytes });
    }

    /// Toglie l'ultimo record del database (troncamento del registro su un follower). Va chiamata in ordine inverso.
    pub fn unapply_last(&mut self, db: &Db, index: u64) -> Option<[u8; 16]> {
        let d = self.dbs.get_mut(db)?;
        if d.recs.back().map(|r| r.index) != Some(index) {
            return None;
        }
        let r = d.recs.pop_back().unwrap();
        for k in r.keys.iter() {
            if let Some(ks) = d.keys.get_mut(k.as_slice()) {
                if ks.v.back().map(|v| v.pos == r.pos).unwrap_or(false) {
                    ks.v.pop_back();
                    self.versions -= 1;
                    if ks.last_struct >= r.pos {
                        ks.last_struct = ks.v.iter().rev().find(|v| v.kind != Kind::Delta).map(|v| v.pos).unwrap_or(0);
                    }
                }
                if ks.v.is_empty() {
                    d.keys.remove(k.as_slice());
                }
            }
        }
        Some(r.txn)
    }

    /// Valore di una chiave alla posizione `at` (chi chiama garantisce at ≤ posizione confermata, oppure legge
    /// speculativamente sul leader con at = MAX).
    /// `dal`: posizione `scaricato` del manifest che userà chi legge (0 = non detta).
    pub fn get(&self, db: &Db, key: &[u8], at: Pos, dal: Pos, load: Loader) -> GetRes {
        let Some(d) = self.dbs.get(db) else {
            return if self.genesis { GetRes::Absent(0) } else { GetRes::OutOfWindow { trimmed: 0, etag: Bytes::new() } };
        };
        let complete = d.genesis && d.trim_pos == 0;
        let Some(ks) = d.keys.get(key) else {
            return if complete { GetRes::Absent(0) } else { GetRes::OutOfWindow { trimmed: d.trim_pos, etag: d.etag.clone() } };
        };
        resolve(ks, at, complete, d, dal, load)
    }

    pub fn scan(&self, db: &Db, from: &[u8], to: &[u8], at: Pos, limit: usize, dal: Pos, load: Loader) -> Vec<(Bytes, GetRes)> {
        let mut out = vec![];
        let Some(d) = self.dbs.get(db) else { return out };
        let complete = d.genesis && d.trim_pos == 0;
        let range = if to.is_empty() {
            d.keys.range::<[u8], _>((std::ops::Bound::Included(from), std::ops::Bound::Unbounded))
        } else {
            d.keys.range::<[u8], _>((std::ops::Bound::Included(from), std::ops::Bound::Excluded(to)))
        };
        for (k, ks) in range {
            if out.len() >= limit {
                break;
            }
            let r = resolve(ks, at, complete, d, dal, load);
            if matches!(r, GetRes::Absent(0)) && complete {
                continue; // non esisteva ancora a `at`
            }
            out.push((Bytes::copy_from_slice(k.as_slice()), r));
        }
        out
    }

    /// Fa uscire dalla finestra il record più vecchio del database se è su S3, più vecchio di `cutoff` e confermato.
    /// Restituisce il record uscito (per il registro: indice, txn).
    pub fn trim_one(&mut self, db: &Db, cutoff: Pos, commit_pos: Pos, load: Loader) -> Option<RecMeta> {
        let d = self.dbs.get_mut(db)?;
        let f = d.recs.front()?;
        if f.pos > d.flushed || f.pos >= cutoff || f.pos > commit_pos {
            return None;
        }
        let r = d.recs.pop_front().unwrap();
        for k in r.keys.iter() {
            let Some(ks) = d.keys.get_mut(k.as_slice()) else { continue };
            if ks.v.front().map(|v| v.pos != r.pos).unwrap_or(true) {
                continue;
            }
            let old = ks.v.pop_front().unwrap();
            self.versions -= 1;
            match ks.v.front_mut() {
                None => {
                    d.keys.remove(k.as_slice());
                }
                Some(next) if next.kind == Kind::Delta => {
                    // piega la versione che esce nel primo delta
                    let base: Option<Bytes> = match old.kind {
                        Kind::Del => None,
                        Kind::Put => Some(val_of(&old.val, load)),
                        // primo della catena con base nota: il suo valore è 0 + delta
                        Kind::Delta => {
                            if ks.base_known {
                                Some(delta_apply(None, &val_of(&old.val, load)))
                            } else {
                                None
                            }
                        }
                    };
                    let unknown_base = old.kind == Kind::Delta && !ks.base_known;
                    if unknown_base {
                        // la base sta negli SST: resta un delta senza base nota
                    } else {
                        let nv = delta_apply(base.as_deref(), &val_of(&next.val, load));
                        next.kind = Kind::Put;
                        next.val = Val::Mem(nv);
                    }
                }
                Some(_) => {}
            }
            if let Some(ks) = d.keys.get_mut(k.as_slice()) {
                ks.base_known = ks.base_known || ks.v.front().map(|v| v.kind != Kind::Delta).unwrap_or(false);
            }
        }
        d.trim_pos = d.trim_pos.max(r.pos);
        Some(r)
    }

    /// Sfratta i valori di un record su disco (tetto di RAM superato).
    pub fn evict(&mut self, db: &Db, pos: Pos, index: u64, writes: &[Write]) {
        let Some(d) = self.dbs.get_mut(db) else { return };
        for (wi, w) in writes.iter().enumerate() {
            if let Some(ks) = d.keys.get_mut(&w.key[..]) {
                // cerca dalla fine: la versione con questa posizione
                for v in ks.v.iter_mut().rev() {
                    if v.pos == pos {
                        if matches!(v.val, Val::Mem(_)) && v.kind != Kind::Del {
                            v.val = Val::Disk(index, wi as u16);
                        }
                        break;
                    }
                    if v.pos < pos {
                        break;
                    }
                }
            }
        }
    }

    pub fn keys_count(&self) -> u64 {
        self.dbs.values().map(|d| d.keys.len() as u64).sum()
    }
}

fn val_of(v: &Val, load: Loader) -> Bytes {
    match v {
        Val::Mem(b) => b.clone(),
        Val::Disk(i, wi) => load(*i, *wi),
    }
}

fn resolve(ks: &KeyS, at: Pos, complete: bool, d: &DbW, dal: Pos, load: Loader) -> GetRes {
    let oow = || GetRes::OutOfWindow { trimmed: d.trim_pos, etag: d.etag.clone() };
    // ultima versione con pos ≤ at
    let n = ks.v.partition_point(|v| v.pos <= at);
    if n == 0 {
        return if complete { GetRes::Absent(0) } else { oow() };
    }
    let top = &ks.v[n - 1];
    match top.kind {
        Kind::Put => GetRes::Value(top.pos, val_of(&top.val, load)),
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
                Kind::Put => Some(val_of(&first.val, load)),
                Kind::Del => None,
                Kind::Delta => {
                    if !ks.base_known {
                        // base negli SST: somma dei delta della finestra dopo il manifest di chi legge
                        if dal != 0 && dal < d.trim_pos {
                            return oow(); // manifest troppo vecchio: va riletto
                        }
                        let mut sum = 0i64;
                        for &j in deltas.iter() {
                            if ks.v[j].pos > dal {
                                sum = sum.wrapping_add(i64::from_le_bytes(val_of(&ks.v[j].val, load).get(..8).map(|x| x.try_into().unwrap()).unwrap_or([0; 8])));
                            }
                        }
                        return GetRes::Delta { pos: top.pos, sum, trimmed: d.trim_pos };
                    }
                    None
                }
            };
            for &j in deltas.iter().rev() {
                cur = Some(delta_apply(cur.as_deref(), &val_of(&ks.v[j].val, load)));
            }
            GetRes::Value(top.pos, cur.unwrap_or_else(|| delta_apply(None, &[0; 8])))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn w(k: &str, kind: Kind, v: &[u8]) -> Write {
        Write { key: Bytes::from(k.to_string()), kind, val: Bytes::copy_from_slice(v) }
    }
    fn noload() -> impl FnMut(u64, u16) -> Bytes {
        |_, _| panic!("disk")
    }
    #[test]
    fn get_validate_trim_fold() {
        let mut win = Window::new(true);
        let db = [1u8; 16];
        let mut l = noload();
        win.apply(1, 10, [0; 16], &db, &[w("a", Kind::Put, &5i64.to_le_bytes())], 8);
        win.apply(2, 20, [1; 16], &db, &[w("a", Kind::Delta, &3i64.to_le_bytes())], 8);
        win.apply(3, 30, [2; 16], &db, &[w("a", Kind::Delta, &2i64.to_le_bytes())], 8);
        let v = |g: GetRes| match g {
            GetRes::Value(_, b) => i64::from_le_bytes(b[..8].try_into().unwrap()),
            o => panic!("{o:?}"),
        };
        assert_eq!(v(win.get(&db, b"a", 25, 0, &mut l)), 8);
        assert_eq!(v(win.get(&db, b"a", 30, 0, &mut l)), 10);
        assert!(matches!(win.get(&db, b"a", 5, 0, &mut l), GetRes::Absent(0)));
        assert!(win.validate(&db, 20, &[w("a", Kind::Put, b"x")], true, &[]).is_err());
        assert!(win.validate(&db, 30, &[w("a", Kind::Put, b"x")], true, &[]).is_ok());
        // esiste: i delta dopo S non conflittano, valore sì
        assert!(win.validate(&db, 15, &[], false, &[(Bytes::from_static(b"a"), Dep::Esiste)]).is_ok());
        assert!(win.validate(&db, 15, &[], false, &[(Bytes::from_static(b"a"), Dep::Valore)]).is_err());
        assert!(win.validate(&db, 5, &[], false, &[(Bytes::from_static(b"a"), Dep::Esiste)]).is_err());
        win.dbs.get_mut(&db).unwrap().flushed = 20;
        assert!(win.trim_one(&db, 100, 30, &mut l).is_some());
        assert!(win.trim_one(&db, 100, 30, &mut l).is_some());
        assert!(win.trim_one(&db, 100, 30, &mut l).is_none()); // 30 non è su S3
        assert_eq!(v(win.get(&db, b"a", 30, 0, &mut l)), 10);
        assert!(matches!(win.get(&db, b"a", 25, 0, &mut l), GetRes::OutOfWindow { .. }));
        assert!(matches!(win.get(&db, b"zz", 30, 0, &mut l), GetRes::OutOfWindow { .. }));
        assert!(win.validate(&db, 15, &[w("q", Kind::Put, b"x")], true, &[]).is_err()); // S prima della finestra
    }

    #[test]
    fn delta_con_base_negli_sst() {
        // database che esisteva già negli SST (finestra non dalla nascita)
        let mut win = Window::new(false);
        let db = [2u8; 16];
        let mut l = noload();
        win.apply(1, 10, [0; 16], &db, &[w("c", Kind::Put, &5i64.to_le_bytes())], 8);
        win.dbs.get_mut(&db).unwrap().flushed = 10;
        assert!(win.trim_one(&db, 100, 10, &mut l).is_some()); // "c" esce: la base sta negli SST
        win.apply(2, 20, [1; 16], &db, &[w("c", Kind::Delta, &3i64.to_le_bytes())], 8);
        win.apply(3, 30, [2; 16], &db, &[w("c", Kind::Delta, &4i64.to_le_bytes())], 8);
        match win.get(&db, b"c", 30, 0, &mut l) {
            GetRes::Delta { pos, sum, trimmed } => assert_eq!((pos, sum, trimmed), (30, 7, 10)),
            o => panic!("{o:?}"),
        }
        // il manifest di chi legge copre già il delta a 20: si somma solo quello a 30
        match win.get(&db, b"c", 30, 20, &mut l) {
            GetRes::Delta { sum, .. } => assert_eq!(sum, 4),
            o => panic!("{o:?}"),
        }
        // manifest più vecchio di quanto è uscito dalla finestra: va riletto
        assert!(matches!(win.get(&db, b"c", 30, 5, &mut l), GetRes::OutOfWindow { trimmed: 10, .. }));
        assert!(matches!(win.get(&db, b"zz", 30, 0, &mut l), GetRes::OutOfWindow { trimmed: 10, .. }));
    }
}
