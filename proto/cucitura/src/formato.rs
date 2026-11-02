//! Formato delle scatole (SST) versione 2, per la cucitura.
//!
//! È il formato di proto-lettura (`proto/lettura/src/sst.rs`) con due cambi, e nient'altro:
//! 1. il filtro di Bloom è **per blocco** (filtri partizionati) invece che uno per tutto l'oggetto: un blocco copiato
//!    lato server si porta dietro il suo filtro, preso dal meta dell'ingresso, senza leggere le sue chiavi;
//! 2. l'indice dà per ogni blocco anche voci, posizione minima e massima e gli id dei database "in mezzo"
//!    (quelli che non sono né il primo né l'ultimo del blocco): così il meta nuovo si scrive senza leggere i blocchi.
//!
//! I blocchi sono identici a quelli di proto-lettura (stesse voci, stesso crc in coda, `lettura::sst::block_lookup`
//! li legge senza cambi). Non hanno puntatori assoluti: per questo si possono copiare a byte in un oggetto nuovo.
//! L'indice dà l'offset assoluto di ogni blocco e **non** richiede che i blocchi siano contigui o in ordine fisico:
//! un oggetto cucito "con le toppe in coda" ha blocchi morti (non indicizzati) e blocchi nuovi dopo quelli copiati.
//!
//! Oggetto: `[zona dati][meta][piè di pagina 64 byte]`.
//! - meta: `n u32`, poi per blocco `off u64, len u32, voci u32, min_pos u64, max_pos u64, prima chiave (u16+byte),
//!   ultima chiave (u16+byte), n_mezzo u16, id 16 byte × n_mezzo, bloom (u16 byte + bit)`.
//! - piè di pagina: `meta_off u64, meta_len u32, voci u64, min_pos u64, max_pos u64, crc_meta u32, versione u32 (=2),
//!   byte_vivi u64, riservato 4 byte, magic u64`.

use bytes::{BufMut, Bytes};
use lettura::iface::{Db, Kind, Pos};
use lettura::sst::{hash64, key_db, Entry};

pub const MAGIC: u64 = 0x4645_4e56_5853_5354; // "FENVXSST", come proto-lettura
pub const FOOTER: usize = 64;
pub const VERSIONE: u32 = 2;
const BLOOM_K: u32 = 7;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    pub off: u64,
    pub len: u32,
    pub voci: u32,
    pub min: Pos,
    pub max: Pos,
    pub first: Bytes,
    pub last: Bytes,
    pub mezzo: Vec<Db>,
    pub bloom: Bytes,
}

impl BlockMeta {
    pub fn may_contain(&self, key: &[u8]) -> bool {
        bloom_may(&self.bloom, key)
    }
    pub fn end(&self) -> u64 {
        self.off + self.len as u64
    }
}

fn bloom_build(hashes: &[u64]) -> Vec<u8> {
    let nbits = ((hashes.len() * 10).max(64) + 7) / 8 * 8;
    let mut bits = vec![0u8; nbits / 8];
    for h in hashes {
        let (h1, h2) = (*h, (*h >> 32) | 1);
        for i in 0..BLOOM_K as u64 {
            let b = (h1.wrapping_add(i.wrapping_mul(h2)) % nbits as u64) as usize;
            bits[b / 8] |= 1 << (b % 8);
        }
    }
    bits
}

pub fn bloom_may(bits: &[u8], key: &[u8]) -> bool {
    let nbits = bits.len() as u64 * 8;
    let h = hash64(key);
    let (h1, h2) = (h, (h >> 32) | 1);
    (0..BLOOM_K as u64).all(|i| {
        let b = (h1.wrapping_add(i.wrapping_mul(h2)) % nbits) as usize;
        bits[b / 8] & (1 << (b % 8)) != 0
    })
}

fn put16(b: &mut Vec<u8>, s: &[u8]) {
    b.put_u16_le(s.len() as u16);
    b.put_slice(s);
}

/// Costruttore di blocchi: produce i byte dei blocchi e i loro meta con offset relativi all'inizio del buffer.
/// Le voci devono arrivare in ordine (chiave crescente, posizione decrescente); le versioni di una chiave non si
/// dividono tra due blocchi (stessa regola di proto-lettura).
pub struct Blocchi {
    pub data: Vec<u8>,
    pub metas: Vec<BlockMeta>,
    block_size: usize,
    cur: Vec<u8>,
    hashes: Vec<u64>,
    first: Option<Bytes>,
    prev: Option<Bytes>,
    voci: u32,
    min: Pos,
    max: Pos,
    dbs: Vec<Db>,
}

impl Blocchi {
    pub fn new(block_size: usize) -> Blocchi {
        Blocchi {
            data: Vec::new(),
            metas: Vec::new(),
            block_size,
            cur: Vec::with_capacity(block_size * 2),
            hashes: Vec::new(),
            first: None,
            prev: None,
            voci: 0,
            min: u64::MAX,
            max: 0,
            dbs: Vec::new(),
        }
    }

    fn chiudi(&mut self) {
        if self.cur.is_empty() {
            return;
        }
        let crc = crc32fast::hash(&self.cur);
        self.cur.put_u32_le(crc);
        let first = self.first.take().unwrap();
        let last = self.prev.clone().unwrap();
        let (fd, ld) = (key_db(&first), key_db(&last));
        let mezzo: Vec<Db> = self.dbs.iter().filter(|d| **d != fd && **d != ld).cloned().collect();
        self.metas.push(BlockMeta {
            off: self.data.len() as u64,
            len: self.cur.len() as u32,
            voci: self.voci,
            min: self.min,
            max: self.max,
            first,
            last,
            mezzo,
            bloom: Bytes::from(bloom_build(&self.hashes)),
        });
        self.data.extend_from_slice(&self.cur);
        self.cur.clear();
        self.hashes.clear();
        self.dbs.clear();
        self.voci = 0;
        self.min = u64::MAX;
        self.max = 0;
    }

    pub fn push(&mut self, e: &Entry) {
        let newkey = self.prev.as_ref().map(|p| p != &e.key).unwrap_or(true);
        if newkey {
            if self.cur.len() >= self.block_size {
                self.chiudi();
            }
            self.hashes.push(hash64(&e.key));
            let db = key_db(&e.key);
            if self.dbs.last() != Some(&db) {
                self.dbs.push(db);
            }
            if self.first.is_none() {
                self.first = Some(e.key.clone());
            }
            self.prev = Some(e.key.clone());
        }
        put16(&mut self.cur, &e.key);
        self.cur.put_u64_le(e.pos);
        self.cur.put_u8(e.kind as u8);
        self.cur.put_u32_le(e.val.len() as u32);
        self.cur.put_slice(&e.val);
        self.voci += 1;
        self.min = self.min.min(e.pos);
        self.max = self.max.max(e.pos);
    }

    /// Chiude l'ultimo blocco. Il prossimo `push` comincia un blocco nuovo.
    pub fn fine(&mut self) {
        self.chiudi();
        self.prev = None;
    }
}

/// Meta e piè di pagina da accodare a un oggetto; `meta_off` è l'offset assoluto in cui comincia il meta.
/// `blocks` in ordine di chiave, con offset assoluti.
pub fn meta_e_piede(blocks: &[BlockMeta], meta_off: u64) -> Vec<u8> {
    let mut meta = Vec::new();
    meta.put_u32_le(blocks.len() as u32);
    let (mut voci, mut min, mut max, mut vivi) = (0u64, u64::MAX, 0u64, 0u64);
    for b in blocks {
        meta.put_u64_le(b.off);
        meta.put_u32_le(b.len);
        meta.put_u32_le(b.voci);
        meta.put_u64_le(b.min);
        meta.put_u64_le(b.max);
        put16(&mut meta, &b.first);
        put16(&mut meta, &b.last);
        meta.put_u16_le(b.mezzo.len() as u16);
        for d in &b.mezzo {
            meta.put_slice(d);
        }
        put16(&mut meta, &b.bloom);
        voci += b.voci as u64;
        min = min.min(b.min);
        max = max.max(b.max);
        vivi += b.len as u64;
    }
    let crc = crc32fast::hash(&meta);
    let meta_len = meta.len() as u32;
    let mut out = meta;
    out.put_u64_le(meta_off);
    out.put_u32_le(meta_len);
    out.put_u64_le(voci);
    out.put_u64_le(if voci == 0 { 0 } else { min });
    out.put_u64_le(max);
    out.put_u32_le(crc);
    out.put_u32_le(VERSIONE);
    out.put_u64_le(vivi);
    out.put_slice(&[0u8; 4]);
    out.put_u64_le(MAGIC);
    out
}

/// Un oggetto intero costruito da voci in ordine (lo scarico, o la riscrittura completa).
pub fn costruisci<'a, I: IntoIterator<Item = &'a Entry>>(entries: I, block_size: usize) -> Vec<u8> {
    let mut b = Blocchi::new(block_size);
    for e in entries {
        b.push(e);
    }
    b.fine();
    let mut data = b.data;
    let tail = meta_e_piede(&b.metas, data.len() as u64);
    data.extend_from_slice(&tail);
    data
}

#[derive(Clone, Debug)]
pub struct Piede {
    pub meta_off: u64,
    pub meta_len: u32,
    pub voci: u64,
    pub min: Pos,
    pub max: Pos,
    pub crc: u32,
    pub vivi: u64,
}

pub fn leggi_piede(f: &[u8]) -> anyhow::Result<Piede> {
    anyhow::ensure!(f.len() == FOOTER, "piè di pagina corto");
    anyhow::ensure!(u64::from_le_bytes(f[56..64].try_into()?) == MAGIC, "magic sbagliato");
    let v = u32::from_le_bytes(f[40..44].try_into()?);
    anyhow::ensure!(v == VERSIONE, "versione {v} non supportata");
    Ok(Piede {
        meta_off: u64::from_le_bytes(f[0..8].try_into()?),
        meta_len: u32::from_le_bytes(f[8..12].try_into()?),
        voci: u64::from_le_bytes(f[12..20].try_into()?),
        min: u64::from_le_bytes(f[20..28].try_into()?),
        max: u64::from_le_bytes(f[28..36].try_into()?),
        crc: u32::from_le_bytes(f[36..40].try_into()?),
        vivi: u64::from_le_bytes(f[44..52].try_into()?),
    })
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        anyhow::ensure!(self.p + n <= self.b.len(), "meta corto");
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    fn u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }
    fn u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn b16(&mut self, src: &Bytes) -> anyhow::Result<Bytes> {
        let n = self.u16()? as usize;
        let s = self.p;
        self.take(n)?;
        Ok(src.slice(s..s + n))
    }
}

#[derive(Clone, Debug)]
pub struct Meta {
    pub blocks: Vec<BlockMeta>,
    pub piede: Piede,
    /// lunghezza dell'oggetto intero
    pub obj_len: u64,
}

impl Meta {
    pub fn parse(meta: Bytes, piede: Piede, obj_len: u64) -> anyhow::Result<Meta> {
        anyhow::ensure!(crc32fast::hash(&meta) == piede.crc, "crc del meta sbagliato");
        let mut c = Cur { b: &meta, p: 0 };
        let n = c.u32()? as usize;
        let mut blocks = Vec::with_capacity(n);
        for _ in 0..n {
            let off = c.u64()?;
            let len = c.u32()?;
            let voci = c.u32()?;
            let min = c.u64()?;
            let max = c.u64()?;
            let first = c.b16(&meta)?;
            let last = c.b16(&meta)?;
            let nm = c.u16()? as usize;
            let mut mezzo = Vec::with_capacity(nm);
            for _ in 0..nm {
                mezzo.push(c.take(16)?.try_into()?);
            }
            let bloom = c.b16(&meta)?;
            blocks.push(BlockMeta { off, len, voci, min, max, first, last, mezzo, bloom });
        }
        Ok(Meta { blocks, piede, obj_len })
    }

    pub fn from_object(obj: &Bytes) -> anyhow::Result<Meta> {
        anyhow::ensure!(obj.len() >= FOOTER, "oggetto corto");
        let p = leggi_piede(&obj[obj.len() - FOOTER..])?;
        let (a, z) = (p.meta_off as usize, p.meta_off as usize + p.meta_len as usize);
        anyhow::ensure!(z + FOOTER == obj.len(), "piè di pagina incoerente");
        Meta::parse(obj.slice(a..z), p, obj.len() as u64)
    }

    /// Indice del solo blocco che può contenere la chiave.
    pub fn find_block(&self, key: &[u8]) -> Option<usize> {
        let i = self.blocks.partition_point(|b| &b.last[..] < key);
        (i < self.blocks.len() && &self.blocks[i].first[..] <= key).then_some(i)
    }

    /// Byte dell'oggetto non indicizzati (blocchi morti lasciati dalla cucitura con le toppe in coda).
    pub fn byte_morti(&self) -> u64 {
        let usati: u64 = self.blocks.iter().map(|b| b.len as u64).sum::<u64>() + self.piede.meta_len as u64 + FOOTER as u64;
        self.obj_len - usati
    }

    pub fn byte_dati(&self) -> u64 {
        self.blocks.iter().map(|b| b.len as u64).sum()
    }
}

/// Ricerca puntuale su un oggetto tenuto intero in memoria (il "bancone" del cameriere).
/// Restituisce il valore (None = cancellato o assente) e quanti blocchi ha aperto (0 se il filtro dice no).
pub fn cerca(obj: &Bytes, m: &Meta, key: &[u8]) -> anyhow::Result<(Option<Bytes>, u32)> {
    let Some(i) = m.find_block(key) else { return Ok((None, 0)) };
    let b = &m.blocks[i];
    if !b.may_contain(key) {
        return Ok((None, 0));
    }
    let bl = obj.slice(b.off as usize..b.end() as usize);
    match lettura::sst::block_lookup(&bl, key, u64::MAX)? {
        lettura::sst::Lookup::Found { val, .. } => Ok((val, 1)),
        _ => Ok((None, 1)),
    }
}

/// Tutte le voci di un oggetto in ordine di chiave (solo blocchi indicizzati).
pub fn tutte_le_voci(obj: &Bytes, m: &Meta) -> anyhow::Result<Vec<Entry>> {
    let mut out = Vec::with_capacity(m.piede.voci as usize);
    for b in &m.blocks {
        lettura::sst::block_entries(&obj.slice(b.off as usize..b.end() as usize), &mut out)?;
    }
    Ok(out)
}

pub fn kind_is_del(k: Kind) -> bool {
    k == Kind::Del
}

#[cfg(test)]
mod t {
    use super::*;
    use lettura::iface::{db_n, doc_key};
    use lettura::sst::full_key;

    #[test]
    fn andata_e_ritorno() {
        let mut es = Vec::new();
        for d in 0..3u64 {
            for k in 0..2000u64 {
                es.push(Entry { key: full_key(&db_n(d), &doc_key("c", k)), pos: 1000 + k, kind: Kind::Put, val: Bytes::from(vec![d as u8; 50 + (k % 7) as usize]) });
            }
        }
        let obj = Bytes::from(costruisci(es.iter(), 4096));
        let m = Meta::from_object(&obj).unwrap();
        assert_eq!(tutte_le_voci(&obj, &m).unwrap(), es);
        assert_eq!(m.byte_morti(), 0);
        for e in es.iter().step_by(37) {
            assert_eq!(cerca(&obj, &m, &e.key).unwrap().0, Some(e.val.clone()));
        }
        let assente = full_key(&db_n(1), &doc_key("c", 99_999));
        assert_eq!(cerca(&obj, &m, &assente).unwrap().0, None);
    }
}
