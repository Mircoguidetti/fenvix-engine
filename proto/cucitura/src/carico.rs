//! Carico "realistico MongoDB" per le scatole: inserimenti, aggiornamenti di documenti recenti, cancellazioni.
//!
//! Chiave come nel motore: id del database (16 byte) + `doc/{collezione}/` + `_id` codificato con il byte di tipo
//! BSON in testa (0x07 ObjectId da 12 byte; 0x05 binario sottotipo 4 per l'UUID da 16 byte). L'ObjectId è
//! `secondi (4 byte) | 5 byte casuali fissi del processo | contatore (3 byte)`, come lo genera un driver: crescente.

use bytes::Bytes;
use lettura::iface::{db_n, Kind, Pos};
use lettura::sst::Entry;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum Id {
    Oid,
    Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum Canc {
    /// documento a caso tra tutti quelli vivi
    Uniforme,
    /// tra i documenti recenti (come gli aggiornamenti)
    Recenti,
    /// i più vecchi (come un indice TTL)
    Vecchi,
}

pub struct Gen {
    pub id: Id,
    pub canc: Canc,
    rng: SmallRng,
    pub keys: Vec<Bytes>,
    pub vivo: Vec<bool>,
    pub pos: Pos,
    /// finestra dei "recenti": ultimi N documenti inseriti
    pub recenti: usize,
    pub p_upd: f64,
    pub p_del: f64,
    oid_rand: [u8; 5],
    prossimo_vecchio: usize,
    pub ops: u64,
}

fn chiave(id_bson: &[u8]) -> Bytes {
    let mut k = Vec::with_capacity(16 + 7 + id_bson.len());
    k.extend_from_slice(&db_n(1));
    k.extend_from_slice(b"doc/c/");
    k.extend_from_slice(id_bson);
    Bytes::from(k)
}

impl Gen {
    pub fn new(id: Id, canc: Canc, seed: u64) -> Gen {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut oid_rand = [0u8; 5];
        rng.fill_bytes(&mut oid_rand);
        Gen { id, canc, rng, keys: Vec::new(), vivo: Vec::new(), pos: 1 << 20, recenti: 50_000, p_upd: 0.10, p_del: 0.01, oid_rand, prossimo_vecchio: 0, ops: 0 }
    }

    pub fn nuova_chiave(&mut self) -> Bytes {
        let n = self.keys.len() as u64;
        match self.id {
            Id::Oid => {
                let mut b = [0u8; 13];
                b[0] = 0x07;
                let secs = 1_760_000_000u32 + (n / 500) as u32; // circa 500 inserimenti al secondo
                b[1..5].copy_from_slice(&secs.to_be_bytes());
                b[5..10].copy_from_slice(&self.oid_rand);
                b[10..13].copy_from_slice(&((n as u32) & 0xff_ffff).to_be_bytes()[1..]);
                chiave(&b)
            }
            Id::Uuid => {
                let mut b = [0u8; 18];
                b[0] = 0x05;
                b[1] = 0x04;
                self.rng.fill_bytes(&mut b[2..]);
                chiave(&b)
            }
        }
    }

    fn valore(&mut self) -> Bytes {
        let n = self.rng.gen_range(300..1700);
        let mut v = vec![0u8; n];
        self.rng.fill_bytes(&mut v);
        Bytes::from(v)
    }

    fn scegli_recente(&mut self) -> Option<usize> {
        let n = self.keys.len();
        if n == 0 {
            return None;
        }
        let lo = n.saturating_sub(self.recenti);
        for _ in 0..8 {
            let i = self.rng.gen_range(lo..n);
            if self.vivo[i] {
                return Some(i);
            }
        }
        None
    }

    fn scegli_canc(&mut self) -> Option<usize> {
        let n = self.keys.len();
        if n == 0 {
            return None;
        }
        match self.canc {
            Canc::Recenti => self.scegli_recente(),
            Canc::Uniforme => {
                for _ in 0..16 {
                    let i = self.rng.gen_range(0..n);
                    if self.vivo[i] {
                        return Some(i);
                    }
                }
                None
            }
            Canc::Vecchi => {
                while self.prossimo_vecchio < n && !self.vivo[self.prossimo_vecchio] {
                    self.prossimo_vecchio += 1;
                }
                (self.prossimo_vecchio < n).then_some(self.prossimo_vecchio)
            }
        }
    }

    /// Una scatola: le operazioni di un intervallo di tempo, l'ultima versione di ogni chiave, fino a circa
    /// `byte` di voci. Restituisce le voci in ordine.
    pub fn scatola(&mut self, byte: usize) -> Vec<Entry> {
        let mut m: BTreeMap<Bytes, (Pos, Kind, Bytes)> = BTreeMap::new();
        let mut tot = 0usize;
        while tot < byte {
            self.pos += 1;
            self.ops += 1;
            let r: f64 = self.rng.gen();
            let (k, kind, v) = if r < self.p_del {
                match self.scegli_canc() {
                    Some(i) => {
                        self.vivo[i] = false;
                        (self.keys[i].clone(), Kind::Del, Bytes::new())
                    }
                    None => continue,
                }
            } else if r < self.p_del + self.p_upd {
                match self.scegli_recente() {
                    Some(i) => {
                        let v = self.valore();
                        (self.keys[i].clone(), Kind::Put, v)
                    }
                    None => continue,
                }
            } else {
                let k = self.nuova_chiave();
                self.keys.push(k.clone());
                self.vivo.push(true);
                let v = self.valore();
                (k, Kind::Put, v)
            };
            let add = k.len() + v.len() + 15;
            if let Some(old) = m.insert(k.clone(), (self.pos, kind, v)) {
                tot -= k.len() + old.2.len() + 15;
            }
            tot += add;
        }
        m.into_iter().map(|(key, (pos, kind, val))| Entry { key, pos, kind, val }).collect()
    }

    /// Chiavi per le letture: `n` vive a caso e `n / 9` assenti (circa 10%).
    pub fn chiavi_lettura(&mut self, n: usize) -> Vec<(Bytes, bool)> {
        let mut out = Vec::with_capacity(n + n / 9);
        let tot = self.keys.len();
        while out.len() < n {
            let i = self.rng.gen_range(0..tot);
            if self.vivo[i] {
                out.push((self.keys[i].clone(), true));
            }
        }
        for _ in 0..n / 9 {
            // chiave mai inserita, nello stesso spazio: per l'ObjectId una a caso tra quelle esistenti con il
            // contatore cambiato (cade in mezzo ai blocchi), per l'UUID una casuale
            let k = match self.id {
                Id::Oid => {
                    let i = self.rng.gen_range(0..tot);
                    let mut v = self.keys[i].to_vec();
                    let l = v.len();
                    v[l - 6] ^= 0x5a; // tocca i byte casuali dell'ObjectId: chiave che non esiste
                    Bytes::from(v)
                }
                Id::Uuid => self.nuova_chiave(),
            };
            out.push((k, false));
        }
        out
    }
}
