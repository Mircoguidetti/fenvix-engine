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

use crate::codec::{Db, GetRes, Kind, Pos, Write};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, VecDeque};

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
}

/// Un record del database ancora nella finestra.
#[derive(Debug)]
pub struct RecMeta {
    pub index: u64,
    pub pos: Pos,
    pub txn: [u8; 16],
    pub keys: Box<[Box<[u8]>]>,
    pub bytes: u32,
}

#[derive(Default, Debug)]
pub struct DbW {
    pub keys: BTreeMap<Box<[u8]>, KeyS>,
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

    /// Validazione (solo leader): Ok se nessuna chiave scritta o letta ha versioni più recenti di `read_pos`.
    pub fn validate(&self, db: &Db, read_pos: Pos, writes: &[Write], reads: &[Bytes]) -> Result<(), Bytes> {
        let Some(d) = self.dbs.get(db) else { return Ok(()) };
        let check = |k: &Bytes| -> Result<(), Bytes> {
            if let Some(ks) = d.keys.get(&k[..]) {
                if let Some(last) = ks.v.back() {
                    if last.pos > read_pos {
                        return Err(k.clone());
                    }
                }
            }
            Ok(())
        };
        // S più vecchia dell'inizio della finestra: non si può sapere se una chiave è cambiata dopo S
        if read_pos < d.trim_pos {
            return Err(writes.first().map(|w| w.key.clone()).or_else(|| reads.first().cloned()).unwrap_or_default());
        }
        for w in writes {
            check(&w.key)?;
        }
        for k in reads {
            check(k)?;
        }
        Ok(())
    }

    /// Aggiunge le versioni di un record (al momento dell'aggiunta al registro, prima della conferma).
    pub fn apply(&mut self, index: u64, pos: Pos, txn: [u8; 16], db: &Db, writes: &[Write], bytes: u32) {
        let d = self.db_mut(db);
        let genesis = d.genesis && d.trim_pos == 0;
        let mut keys = Vec::with_capacity(writes.len());
        for w in writes.iter() {
            let ks = match d.keys.get_mut(&w.key[..]) {
                Some(k) => k,
                None => {
                    d.keys.insert(w.key.to_vec().into_boxed_slice(), KeyS { v: VecDeque::new(), base_known: genesis });
                    d.keys.get_mut(&w.key[..]).unwrap()
                }
            };
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
            keys.push(w.key.to_vec().into_boxed_slice());
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
            if let Some(ks) = d.keys.get_mut(&k[..]) {
                if ks.v.back().map(|v| v.pos == r.pos).unwrap_or(false) {
                    ks.v.pop_back();
                    self.versions -= 1;
                }
                if ks.v.is_empty() {
                    d.keys.remove(&k[..]);
                }
            }
        }
        Some(r.txn)
    }

    /// Valore di una chiave alla posizione `at` (chi chiama garantisce at ≤ posizione confermata, oppure legge
    /// speculativamente sul leader con at = MAX).
    pub fn get(&self, db: &Db, key: &[u8], at: Pos, load: Loader) -> GetRes {
        let Some(d) = self.dbs.get(db) else {
            return if self.genesis { GetRes::Absent(0) } else { GetRes::OutOfWindow };
        };
        let complete = d.genesis && d.trim_pos == 0;
        let Some(ks) = d.keys.get(key) else {
            return if complete { GetRes::Absent(0) } else { GetRes::OutOfWindow };
        };
        resolve(ks, at, complete, load)
    }

    pub fn scan(&self, db: &Db, from: &[u8], to: &[u8], at: Pos, limit: usize, load: Loader) -> Vec<(Bytes, GetRes)> {
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
            let r = resolve(ks, at, complete, load);
            if matches!(r, GetRes::Absent(0)) && complete {
                continue; // non esisteva ancora a `at`
            }
            out.push((Bytes::copy_from_slice(k), r));
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
            let Some(ks) = d.keys.get_mut(&k[..]) else { continue };
            if ks.v.front().map(|v| v.pos != r.pos).unwrap_or(true) {
                continue;
            }
            let old = ks.v.pop_front().unwrap();
            self.versions -= 1;
            match ks.v.front_mut() {
                None => {
                    d.keys.remove(&k[..]);
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
            if let Some(ks) = d.keys.get_mut(&k[..]) {
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

fn resolve(ks: &KeyS, at: Pos, complete: bool, load: Loader) -> GetRes {
    // ultima versione con pos ≤ at
    let n = ks.v.partition_point(|v| v.pos <= at);
    if n == 0 {
        return if complete { GetRes::Absent(0) } else { GetRes::OutOfWindow };
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
                        return GetRes::OutOfWindow;
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
        assert_eq!(v(win.get(&db, b"a", 25, &mut l)), 8);
        assert_eq!(v(win.get(&db, b"a", 30, &mut l)), 10);
        assert!(matches!(win.get(&db, b"a", 5, &mut l), GetRes::Absent(0)));
        assert!(win.validate(&db, 20, &[w("a", Kind::Put, b"x")], &[]).is_err());
        assert!(win.validate(&db, 30, &[w("a", Kind::Put, b"x")], &[]).is_ok());
        win.dbs.get_mut(&db).unwrap().flushed = 20;
        assert!(win.trim_one(&db, 100, 30, &mut l).is_some());
        assert!(win.trim_one(&db, 100, 30, &mut l).is_some());
        assert!(win.trim_one(&db, 100, 30, &mut l).is_none()); // 30 non è su S3
        assert_eq!(v(win.get(&db, b"a", 30, &mut l)), 10);
        assert!(matches!(win.get(&db, b"a", 25, &mut l), GetRes::OutOfWindow));
        assert!(matches!(win.get(&db, b"zz", 30, &mut l), GetRes::OutOfWindow));
        assert!(win.validate(&db, 15, &[w("q", Kind::Put, b"x")], &[]).is_err()); // S prima della finestra
    }
}
