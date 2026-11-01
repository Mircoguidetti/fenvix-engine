//! La dispensa (indice ordinato su disco locale) per i record non ancora su S3.
//!
//! Quando la memoria del quaderno supera la soglia e il magazzino (S3) non smaltisce, i record non scaricati più
//! vecchi della finestra (60 s) escono dalla RAM: le loro versioni vanno in un file ordinato per (database, chiave,
//! posizione), un "run". Il run non copia i valori se può evitarlo: punta alla voce nel registro su disco
//! (collocazione + numero della scrittura), che resta finché il record non è su S3. In RAM restano solo un indice rado
//! (una chiave ogni 128 voci) e, per ogni database, l'intervallo di byte e di posizioni che ha nel run.
//!
//! Letture: si cerca nella finestra in RAM; se non basta, nei run dal più nuovo al più vecchio (letture più lente,
//! qualche pread, ma corrette). Scarico: `scaricabili` restituisce prima i run del database (interi, dal più vecchio),
//! poi i record ancora in RAM. Un run si cancella quando tutti i suoi database sono su S3 oltre il suo intervallo.

use crate::codec::{put16, Db, Kind, Pos, Rd, Short};
use crate::window::{IKey, Loc};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write as _;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum SVal {
    /// il valore è la scrittura `wi` della voce in `Loc` nel registro
    Log(Loc, u16),
    /// valore calcolato (delta piegato) o non più nel registro in forma originale
    Inline(Bytes),
    /// cancellazione: nessun valore
    None,
}

#[derive(Clone, Debug)]
pub struct SEntry {
    pub db: Db,
    pub key: Bytes,
    pub pos: Pos,
    pub kind: Kind,
    pub val: SVal,
}

#[derive(Clone, Copy, Debug)]
pub struct DbRange {
    pub start: u64,
    pub end: u64,
    pub min_pos: Pos,
    pub max_pos: Pos,
    pub n: u32,
    /// byte stimati dei valori (per il budget di `scaricabili`)
    pub bytes: u64,
}

pub struct Run {
    pub id: u64,
    path: PathBuf,
    /// (db, chiave, offset) ogni 128 voci
    sparse: Vec<(Db, IKey, u64)>,
    pub dbs: HashMap<Db, DbRange>,
    pub file_len: u64,
    /// database del run non ancora interamente su S3: a zero il run si cancella
    pub live: AtomicUsize,
}

pub struct Spill {
    dir: PathBuf,
    pub runs: RwLock<BTreeMap<u64, Arc<Run>>>,
    next: AtomicU64,
    pub disk_bytes: AtomicI64,
    pub ram_bytes: AtomicI64,
}

fn enc(b: &mut BytesMut, e: &SEntry) {
    b.put_slice(&e.db);
    put16(b, &e.key);
    b.put_u64_le(e.pos);
    b.put_u8(e.kind as u8);
    match &e.val {
        SVal::Log(l, wi) => {
            b.put_u8(0);
            b.put_u32_le(l.seg);
            b.put_u64_le(l.off);
            b.put_u32_le(l.len);
            b.put_u16_le(*wi);
        }
        SVal::Inline(v) => {
            b.put_u8(1);
            b.put_u32_le(v.len() as u32);
            b.put_slice(v);
        }
        SVal::None => b.put_u8(2),
    }
}

fn dec(r: &mut Rd) -> Result<SEntry, Short> {
    let db = r.a16()?;
    let key = r.bytes16()?;
    let pos = r.u64()?;
    let kind = Kind::from(r.u8()?)?;
    let val = match r.u8()? {
        0 => SVal::Log(Loc { seg: r.u32()?, off: r.u64()?, len: r.u32()? }, r.u16()?),
        1 => SVal::Inline(r.bytes32()?),
        _ => SVal::None,
    };
    Ok(SEntry { db, key, pos, kind, val })
}

impl Spill {
    pub fn new(dir: PathBuf) -> Spill {
        // i run di una vita precedente non valgono: dopo il riavvio la finestra si ricostruisce dal registro
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten() {
                if e.file_name().to_string_lossy().starts_with("spill-") {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        Spill { dir, runs: RwLock::new(BTreeMap::new()), next: AtomicU64::new(1), disk_bytes: AtomicI64::new(0), ram_bytes: AtomicI64::new(0) }
    }

    /// Scrive un run (le voci vengono ordinate qui) e lo registra. Restituisce l'id.
    pub fn write_run(&self, mut entries: Vec<SEntry>) -> std::io::Result<Arc<Run>> {
        entries.sort_by(|a, b| (a.db, &a.key[..], a.pos).cmp(&(b.db, &b.key[..], b.pos)));
        let id = self.next.fetch_add(1, Relaxed);
        let path = self.dir.join(format!("spill-{id:08}.run"));
        let mut buf = BytesMut::with_capacity(entries.len() * 64);
        let mut sparse = vec![];
        let mut dbs: HashMap<Db, DbRange> = HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            let off = buf.len() as u64;
            if i % 128 == 0 {
                sparse.push((e.db, IKey::new(&e.key), off));
            }
            enc(&mut buf, e);
            let vb = match &e.val {
                SVal::Log(l, _) => l.len as u64,
                SVal::Inline(v) => v.len() as u64,
                SVal::None => 0,
            };
            let r = dbs.entry(e.db).or_insert(DbRange { start: off, end: off, min_pos: e.pos, max_pos: e.pos, n: 0, bytes: 0 });
            r.end = buf.len() as u64;
            r.min_pos = r.min_pos.min(e.pos);
            r.max_pos = r.max_pos.max(e.pos);
            r.n += 1;
            r.bytes += vb + e.key.len() as u64 + 16;
        }
        let mut f = File::create(&path)?;
        f.write_all(&buf)?;
        f.sync_data()?;
        let ram = (sparse.len() * 64 + dbs.len() * 80) as i64;
        self.ram_bytes.fetch_add(ram, Relaxed);
        self.disk_bytes.fetch_add(buf.len() as i64, Relaxed);
        let run = Arc::new(Run { id, path, sparse, live: AtomicUsize::new(dbs.len()), dbs, file_len: buf.len() as u64 });
        self.runs.write().insert(id, run.clone());
        Ok(run)
    }

    pub fn get(&self, id: u64) -> Option<Arc<Run>> {
        self.runs.read().get(&id).cloned()
    }

    /// Un database del run è su S3 oltre il suo intervallo: quando non ne resta nessuno, il run si cancella.
    pub fn release(&self, id: u64) {
        let Some(run) = self.get(id) else { return };
        if run.live.fetch_sub(1, Relaxed) == 1 {
            self.runs.write().remove(&id);
            let _ = std::fs::remove_file(&run.path);
            self.ram_bytes.fetch_sub((run.sparse.len() * 64 + run.dbs.len() * 80) as i64, Relaxed);
            self.disk_bytes.fetch_sub(run.file_len as i64, Relaxed);
        }
    }
}

impl Run {
    fn read_range(&self, start: u64, end: u64) -> Vec<SEntry> {
        let mut b = vec![0u8; (end - start) as usize];
        let Ok(f) = File::open(&self.path) else { return vec![] };
        if f.read_exact_at(&mut b, start).is_err() {
            return vec![];
        }
        let mut r = Rd::new(Bytes::from(b));
        let mut out = vec![];
        while r.left() > 0 {
            match dec(&mut r) {
                Ok(e) => out.push(e),
                Err(_) => break,
            }
        }
        out
    }

    /// Tutte le voci del database in questo run (ordinate per chiave e posizione).
    pub fn db_entries(&self, db: &Db) -> Vec<SEntry> {
        match self.dbs.get(db) {
            Some(r) => self.read_range(r.start, r.end),
            None => vec![],
        }
    }

    /// Versioni della chiave in questo run, in ordine di posizione.
    pub fn lookup(&self, db: &Db, key: &[u8]) -> Vec<SEntry> {
        let Some(r) = self.dbs.get(db) else { return vec![] };
        // ultimo punto dell'indice rado con (db, chiave) < (db, key): da lì si legge in avanti
        let target = (*db, key);
        let i = self.sparse.partition_point(|(d, k, _)| (*d, k.as_slice()) < target);
        let start = if i == 0 { r.start } else { self.sparse[i - 1].2.max(r.start) };
        // fine: il primo punto dell'indice con (db, chiave) > target, o la fine del database
        let j = self.sparse.partition_point(|(d, k, _)| (*d, k.as_slice()) <= target);
        let end = if j < self.sparse.len() { self.sparse[j].2.min(r.end).max(start) } else { r.end };
        let end = if end <= start { r.end } else { end };
        self.read_range(start, end).into_iter().filter(|e| e.db == *db && &e.key[..] == key).collect()
    }
}

pub fn spill_dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| rd.flatten().filter(|e| e.file_name().to_string_lossy().starts_with("spill-")).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum())
        .unwrap_or(0)
}
