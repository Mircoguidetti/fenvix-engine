//! Formato SST del prototipo. Stesso schema di SlateDB 0.17 (blocchi di dati, indice dei blocchi, filtro di Bloom,
//! piè di pagina; ogni voce porta una sequenza, qui la posizione del registro), ma con un costruttore pubblico e un
//! "meta" (indice + bloom + elenco dei database) leggibile con un solo GET a intervallo, il cui intervallo sta nel
//! manifest: così una lettura fredda costa GET(manifest) + GET(meta) + GET(blocco), o 2 GET se l'oggetto è piccolo.
//!
//! Oggetto: `[blocchi][meta][piè di pagina 64 byte]`.
//! - voce: `klen u16 | chiave | pos u64 | tipo u8 | vlen u32 | valore`; chiave = id del database (16 byte) + chiave
//!   dello spazio unico del motore. Ordine: chiave crescente, posizione decrescente. Le versioni di una chiave non si
//!   dividono mai tra due blocchi. Ogni blocco finisce con il crc32 delle sue voci.
//! - meta: indice (`n u32`, per blocco `off u64, len u32, prima chiave, ultima chiave`), bloom (`k u8, m u32 byte,
//!   bit`), elenco dei database (`n u32`, per database `id 16 byte, primo blocco u32, ultimo blocco u32`).
//! - piè di pagina: `meta_off u64, meta_len u32, voci u64, min_pos u64, max_pos u64, crc_meta u32, versione u32,
//!   riservato 12 byte, magic u64`.

use crate::iface::*;
use bytes::{BufMut, Bytes};

pub const MAGIC: u64 = 0x4645_4e56_5853_5354; // "FENVXSST"
pub const FOOTER: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Bytes,
    pub pos: Pos,
    pub kind: Kind,
    pub val: Bytes,
}

pub fn full_key(db: &Db, k: &[u8]) -> Bytes {
    let mut v = Vec::with_capacity(16 + k.len());
    v.extend_from_slice(db);
    v.extend_from_slice(k);
    Bytes::from(v)
}

pub fn key_db(k: &[u8]) -> Db {
    k[..16].try_into().unwrap()
}

pub fn hash64(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for x in b {
        h = (h ^ *x as u64).wrapping_mul(0x100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

#[derive(Clone, Debug)]
pub struct Bloom {
    k: u8,
    bits: Vec<u8>,
}

impl Bloom {
    fn build(hashes: &[u64]) -> Bloom {
        let nbits = ((hashes.len() * 10).max(64) + 7) / 8 * 8;
        let mut bits = vec![0u8; nbits / 8];
        let k = 7u8;
        for h in hashes {
            let (h1, h2) = (*h, (*h >> 32) | 1);
            for i in 0..k as u64 {
                let b = (h1.wrapping_add(i.wrapping_mul(h2)) % nbits as u64) as usize;
                bits[b / 8] |= 1 << (b % 8);
            }
        }
        Bloom { k, bits }
    }
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = self.bits.len() as u64 * 8;
        let h = hash64(key);
        let (h1, h2) = (h, (h >> 32) | 1);
        (0..self.k as u64).all(|i| {
            let b = (h1.wrapping_add(i.wrapping_mul(h2)) % nbits) as usize;
            self.bits[b / 8] & (1 << (b % 8)) != 0
        })
    }
}

#[derive(Clone, Debug)]
pub struct BlockIdx {
    pub off: u64,
    pub len: u32,
    pub first: Bytes,
    pub last: Bytes,
}

#[derive(Clone, Debug)]
pub struct SstMeta {
    pub blocks: Vec<BlockIdx>,
    pub bloom: Bloom,
    pub dbdir: Vec<(Db, u32, u32)>,
    pub bytes: usize,
}

pub struct Built {
    pub data: Vec<u8>,
    pub meta_off: u64,
    pub meta_len: u32,
    pub voci: u64,
    pub min: Pos,
    pub max: Pos,
}

fn put16(b: &mut Vec<u8>, s: &[u8]) {
    b.put_u16_le(s.len() as u16);
    b.put_slice(s);
}

/// Costruisce un SST. Le voci devono essere in ordine (chiave crescente, posizione decrescente).
pub fn build<I: IntoIterator<Item = Entry>>(entries: I, block_size: usize) -> Built {
    let mut data: Vec<u8> = Vec::new();
    let mut blocks: Vec<BlockIdx> = Vec::new();
    let mut hashes: Vec<u64> = Vec::new();
    let mut dbdir: Vec<(Db, u32, u32)> = Vec::new();
    let mut cur: Vec<u8> = Vec::with_capacity(block_size * 2);
    let mut first: Option<Bytes> = None;
    let mut prev: Option<Bytes> = None;
    let (mut voci, mut min, mut max) = (0u64, u64::MAX, 0u64);

    fn finish(data: &mut Vec<u8>, blocks: &mut Vec<BlockIdx>, dbdir: &mut [(Db, u32, u32)], cur: &mut Vec<u8>, first: &mut Option<Bytes>, last: &Bytes) {
        let crc = crc32fast::hash(cur);
        cur.put_u32_le(crc);
        if let Some(d) = dbdir.last_mut() {
            d.2 = blocks.len() as u32;
        }
        blocks.push(BlockIdx { off: data.len() as u64, len: cur.len() as u32, first: first.take().unwrap(), last: last.clone() });
        data.extend_from_slice(cur);
        cur.clear();
    }

    for e in entries {
        let newkey = prev.as_ref().map(|p| p != &e.key).unwrap_or(true);
        if newkey {
            if !cur.is_empty() && cur.len() >= block_size {
                let last = prev.clone().unwrap();
                finish(&mut data, &mut blocks, &mut dbdir, &mut cur, &mut first, &last);
            }
            hashes.push(hash64(&e.key));
            let db = key_db(&e.key);
            if dbdir.last().map(|d| d.0 != db).unwrap_or(true) {
                if let Some(d) = dbdir.last_mut() {
                    d.2 = if cur.is_empty() { blocks.len() as u32 - 1 } else { blocks.len() as u32 };
                }
                dbdir.push((db, blocks.len() as u32, blocks.len() as u32));
            }
            if first.is_none() {
                first = Some(e.key.clone());
            }
            prev = Some(e.key.clone());
        }
        put16(&mut cur, &e.key);
        cur.put_u64_le(e.pos);
        cur.put_u8(e.kind as u8);
        cur.put_u32_le(e.val.len() as u32);
        cur.put_slice(&e.val);
        voci += 1;
        min = min.min(e.pos);
        max = max.max(e.pos);
    }
    if !cur.is_empty() {
        let last = prev.clone().unwrap();
        finish(&mut data, &mut blocks, &mut dbdir, &mut cur, &mut first, &last);
    }
    let meta_off = data.len() as u64;
    let mut meta = Vec::new();
    meta.put_u32_le(blocks.len() as u32);
    for b in &blocks {
        meta.put_u64_le(b.off);
        meta.put_u32_le(b.len);
        put16(&mut meta, &b.first);
        put16(&mut meta, &b.last);
    }
    let bl = Bloom::build(&hashes);
    meta.put_u8(bl.k);
    meta.put_u32_le(bl.bits.len() as u32);
    meta.put_slice(&bl.bits);
    meta.put_u32_le(dbdir.len() as u32);
    for (db, a, b) in &dbdir {
        meta.put_slice(db);
        meta.put_u32_le(*a);
        meta.put_u32_le(*b);
    }
    let meta_len = meta.len() as u32;
    let crc = crc32fast::hash(&meta);
    data.extend_from_slice(&meta);
    data.put_u64_le(meta_off);
    data.put_u32_le(meta_len);
    data.put_u64_le(voci);
    data.put_u64_le(if voci == 0 { 0 } else { min });
    data.put_u64_le(max);
    data.put_u32_le(crc);
    data.put_u32_le(1);
    data.put_slice(&[0u8; 12]);
    data.put_u64_le(MAGIC);
    Built { data, meta_off, meta_len, voci, min: if voci == 0 { 0 } else { min }, max }
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        anyhow::ensure!(self.p + n <= self.b.len(), "SST corto");
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
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
}

impl SstMeta {
    pub fn parse(meta: &[u8]) -> anyhow::Result<SstMeta> {
        let mut c = Cur { b: meta, p: 0 };
        let n = c.u32()? as usize;
        let mut blocks = Vec::with_capacity(n);
        for _ in 0..n {
            let off = c.u64()?;
            let len = c.u32()?;
            let fl = c.u16()? as usize;
            let first = Bytes::copy_from_slice(c.take(fl)?);
            let ll = c.u16()? as usize;
            let last = Bytes::copy_from_slice(c.take(ll)?);
            blocks.push(BlockIdx { off, len, first, last });
        }
        let k = c.u8()?;
        let m = c.u32()? as usize;
        let bits = c.take(m)?.to_vec();
        let nd = c.u32()? as usize;
        let mut dbdir = Vec::with_capacity(nd);
        for _ in 0..nd {
            let db: Db = c.take(16)?.try_into()?;
            dbdir.push((db, c.u32()?, c.u32()?));
        }
        let bytes = meta.len() + n * 64 + nd * 24;
        Ok(SstMeta { blocks, bloom: Bloom { k, bits }, dbdir, bytes })
    }

    /// Legge il meta dal piè di pagina di un oggetto intero.
    pub fn from_object(obj: &[u8]) -> anyhow::Result<SstMeta> {
        anyhow::ensure!(obj.len() >= FOOTER, "SST corto");
        let f = &obj[obj.len() - FOOTER..];
        let mut c = Cur { b: f, p: 0 };
        let off = c.u64()? as usize;
        let len = c.u32()? as usize;
        anyhow::ensure!(u64::from_le_bytes(f[FOOTER - 8..].try_into()?) == MAGIC, "magic sbagliato");
        anyhow::ensure!(off + len + FOOTER == obj.len(), "piè di pagina incoerente");
        let meta = &obj[off..off + len];
        let crc = u32::from_le_bytes(f[36..40].try_into()?);
        anyhow::ensure!(crc32fast::hash(meta) == crc, "crc del meta sbagliato");
        SstMeta::parse(meta)
    }

    /// Indice del solo blocco che può contenere la chiave.
    pub fn find_block(&self, key: &[u8]) -> Option<usize> {
        let i = self.blocks.partition_point(|b| &b.last[..] < key);
        (i < self.blocks.len() && &self.blocks[i].first[..] <= key).then_some(i)
    }

    pub fn db_blocks(&self, db: &Db) -> Option<(u32, u32)> {
        let i = self.dbdir.partition_point(|d| &d.0 < db);
        (i < self.dbdir.len() && &self.dbdir[i].0 == db).then(|| (self.dbdir[i].1, self.dbdir[i].2))
    }

    /// Intervallo di byte dei blocchi [a, b].
    pub fn span(&self, a: u32, b: u32) -> (u64, u64) {
        let s = self.blocks[a as usize].off;
        let e = self.blocks[b as usize].off + self.blocks[b as usize].len as u64;
        (s, e)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Lookup {
    NotFound,
    /// versione più recente <= S: valore (None = cancellato), posizione
    Found { pos: Pos, val: Option<Bytes> },
    /// solo delta in questo SST fino alla base: somma da aggiungere al valore degli SST più vecchi
    Delta { pos: Pos, somma: i64 },
}

pub fn check_block(b: &[u8]) -> anyhow::Result<&[u8]> {
    anyhow::ensure!(b.len() >= 4, "blocco corto");
    let (body, crc) = b.split_at(b.len() - 4);
    anyhow::ensure!(crc32fast::hash(body) == u32::from_le_bytes(crc.try_into()?), "crc del blocco sbagliato");
    Ok(body)
}

/// Cerca la chiave alla posizione S in un blocco (con crc).
pub fn block_lookup(block: &Bytes, key: &[u8], s: Pos) -> anyhow::Result<Lookup> {
    let body = check_block(block)?;
    let mut c = Cur { b: body, p: 0 };
    let mut somma: i64 = 0;
    let mut primo: Option<Pos> = None;
    while c.p < body.len() {
        let kl = c.u16()? as usize;
        let k = c.take(kl)?;
        let pos = c.u64()?;
        let kind = Kind::from_u8(c.u8()?).ok_or_else(|| anyhow::anyhow!("tipo sbagliato"))?;
        let vl = c.u32()? as usize;
        let vs = c.p;
        c.take(vl)?;
        match k.cmp(key) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Greater => break,
            std::cmp::Ordering::Equal => {}
        }
        if pos > s {
            continue;
        }
        let p = *primo.get_or_insert(pos);
        match kind {
            Kind::Delta => somma = somma.wrapping_add(delta_val(&body[vs..vs + vl])),
            Kind::Put => {
                let val = if p == pos { block.slice(vs..vs + vl) } else { Bytes::copy_from_slice(&delta_val(&body[vs..vs + vl]).wrapping_add(somma).to_le_bytes()) };
                return Ok(Lookup::Found { pos: p, val: Some(val) });
            }
            Kind::Del => {
                return Ok(if p == pos { Lookup::Found { pos: p, val: None } } else { Lookup::Found { pos: p, val: Some(Bytes::copy_from_slice(&somma.to_le_bytes())) } });
            }
        }
    }
    Ok(match primo {
        Some(p) => Lookup::Delta { pos: p, somma },
        None => Lookup::NotFound,
    })
}

/// Tutte le voci di un blocco.
pub fn block_entries(block: &Bytes, out: &mut Vec<Entry>) -> anyhow::Result<()> {
    let body = check_block(block)?;
    let mut c = Cur { b: body, p: 0 };
    while c.p < body.len() {
        let kl = c.u16()? as usize;
        let ks = c.p;
        c.take(kl)?;
        let pos = c.u64()?;
        let kind = Kind::from_u8(c.u8()?).ok_or_else(|| anyhow::anyhow!("tipo sbagliato"))?;
        let vl = c.u32()? as usize;
        let vs = c.p;
        c.take(vl)?;
        out.push(Entry { key: block.slice(ks..ks + kl), pos, kind, val: block.slice(vs..vs + vl) });
    }
    Ok(())
}

/// Tutte le voci di un oggetto SST intero, in ordine.
pub fn all_entries(obj: &Bytes) -> anyhow::Result<Vec<Entry>> {
    let m = SstMeta::from_object(obj)?;
    let mut out = Vec::new();
    for b in &m.blocks {
        block_entries(&obj.slice(b.off as usize..(b.off + b.len as u64) as usize), &mut out)?;
    }
    Ok(out)
}

#[cfg(test)]
mod t {
    use super::*;
    #[test]
    fn roundtrip() {
        let mut es = Vec::new();
        for d in 0..3u64 {
            for k in 0..500u64 {
                for v in (0..3u64).rev() {
                    es.push(Entry { key: full_key(&db_n(d), &doc_key("c", k)), pos: 1000 + v * 10 + k, kind: Kind::Put, val: Bytes::from(vec![v as u8; 100]) });
                }
            }
        }
        let b = build(es.clone(), 4096);
        let obj = Bytes::from(b.data);
        assert_eq!(all_entries(&obj).unwrap(), es);
        let m = SstMeta::from_object(&obj).unwrap();
        for d in 0..3u64 {
            let (a, z) = m.db_blocks(&db_n(d)).unwrap();
            assert!(a <= z);
            for k in [0u64, 7, 499] {
                let key = full_key(&db_n(d), &doc_key("c", k));
                assert!(m.bloom.may_contain(&key));
                let i = m.find_block(&key).unwrap();
                assert!(i >= a as usize && i <= z as usize);
                let bl = obj.slice(m.blocks[i].off as usize..(m.blocks[i].off + m.blocks[i].len as u64) as usize);
                assert_eq!(block_lookup(&bl, &key, u64::MAX).unwrap(), Lookup::Found { pos: 1020 + k, val: Some(Bytes::from(vec![2u8; 100])) });
                assert_eq!(block_lookup(&bl, &key, 1015 + k).unwrap(), Lookup::Found { pos: 1010 + k, val: Some(Bytes::from(vec![1u8; 100])) });
                assert_eq!(block_lookup(&bl, &key, 999 + k).unwrap(), Lookup::NotFound);
            }
        }
        assert!(m.db_blocks(&db_n(9)).is_none());
    }
}
