//! Registro su disco di una replica.
//!
//! Cartella:
//! - `seg-NNNNNNNNNN.log`: frame delle voci (formato `codec::frame`), in ordine di indice, aggiunti in coda. Un thread
//!   dedicato scrive tutto quello che trova in coda e fa **un** fdatasync per gruppo (group commit), poi segnala
//!   l'ultimo indice reso durevole. Solo l'ultimo segmento può finire con un frame strappato (crash a metà scrittura):
//!   al recupero viene tagliato.
//! - `state`: mandato e voto di Raft (riscritto in modo atomico: tmp + fsync + rename + fsync della cartella).
//! - `ckpt`: posizioni già su S3 per database e metadati della voce prima del primo segmento rimasto; scritto prima di
//!   cancellare segmenti.

use crate::codec::{read_frame, Entry, FrameRead, Pos};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub enum DiskOp {
    /// scrivi questi frame in coda al segmento `seg` (che inizia o continua); `last` = indice dell'ultima voce
    Append { seg: u32, off: u64, data: Vec<Bytes>, last: u64, gen: u64 },
    /// tronca il segmento `seg` a `off` byte e cancella i segmenti successivi
    Truncate { seg: u32, off: u64 },
    Delete { seg: u32 },
    /// scrivi il checkpoint (con le posizioni su S3 lette ora dalla finestra), poi cancella questi segmenti
    CkptDelete { prev_index: u64, prev_term: u64, prev_pos: Pos, segs: Vec<u32> },
}

pub fn seg_path(dir: &Path, seg: u32) -> PathBuf {
    dir.join(format!("seg-{seg:010}.log"))
}

pub fn sync_dir(dir: &Path) {
    if let Ok(f) = File::open(dir) {
        let _ = f.sync_all();
    }
}

pub fn write_atomic(dir: &Path, name: &str, data: &[u8]) -> std::io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir);
    Ok(())
}

/// Scrive `len` byte a zero da `off` (blocchi da 1 MB).
fn zero_range(f: &File, off: u64, len: u64) -> std::io::Result<()> {
    let z = vec![0u8; 1 << 20];
    let mut p = off;
    while p < off + len {
        let n = ((off + len - p) as usize).min(z.len());
        f.write_all_at(&z[..n], p)?;
        p += n as u64;
    }
    Ok(())
}

/// Crea un file di `size` byte già scritti a zero e sincronizzati: le scritture successive sovrascrivono blocchi già
/// allocati, così fdatasync scrive solo i dati (niente metadati né journal di ext4 a ogni gruppo).
fn prealloc(path: &Path, size: u64) -> std::io::Result<()> {
    let f = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    zero_range(&f, 0, size)?;
    f.sync_all()
}

/// Q8: come `prealloc`, ma a passo: blocchi da 1 MB, fdatasync dopo ogni blocco e al più `mbs` MB/s
/// (`PQ_PREALLOC_MBS`). Prima il segmento di scorta da 64 MB si scriveva tutto insieme con un solo sync alla fine:
/// sullo stesso disco fermava il fdatasync del registro (MISURATO in Q8: un colpo da 60–130 ms a ogni cambio di
/// segmento con tre repliche sullo stesso disco).
fn prealloc_paced(path: &Path, size: u64, mbs: u64) -> std::io::Result<()> {
    let f = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let z = vec![0u8; 1 << 20];
    let t0 = std::time::Instant::now();
    let mut p = 0u64;
    while p < size {
        let n = ((size - p) as usize).min(z.len());
        f.write_all_at(&z[..n], p)?;
        f.sync_data()?;
        p += n as u64;
        let due = std::time::Duration::from_secs_f64(p as f64 / (mbs as f64 * 1048576.0));
        let el = t0.elapsed();
        if due > el {
            std::thread::sleep(due - el);
        }
    }
    f.sync_all()
}

/// Q8: riciclo dei segmenti (`PQ_RICICLA=1`). Un segmento uscito dal registro non si cancella: diventa il segmento
/// di scorta. Si accorcia alla taglia del segmento, si azzerano i primi 64 KB, fdatasync, e si rinomina in
/// `spare.log`. I blocchi restano allocati e già scritti (niente metadati al fdatasync, come con gli zeri), ma il
/// disco non scrive più due volte ogni byte del registro (prima zeri, poi dati) e non fa il `discard` della
/// cancellazione. Il recupero è già sicuro con i resti vecchi: si ferma alla prima voce con indice non contiguo
/// (le voci vecchie hanno indici più bassi dell'inizio del registro) e azzera la coda dell'ultimo segmento.
pub fn recycle_on() -> bool {
    std::env::var("PQ_RICICLA").map(|x| x == "1").unwrap_or(false)
}

fn recycle(path: &Path, spare: &Path, dir: &Path, seg_bytes: u64) -> std::io::Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(seg_bytes)?;
    f.write_all_at(&vec![0u8; 64 << 10], 0)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(path, spare)?;
    sync_dir(dir);
    Ok(())
}

fn prealloc_mbs() -> u64 {
    std::env::var("PQ_PREALLOC_MBS").ok().and_then(|x| x.parse().ok()).unwrap_or(0)
}

fn open_segment(dir: &Path, seg: u32, size: u64) -> File {
    let p = seg_path(dir, seg);
    if !p.exists() {
        let spare = dir.join("spare.log");
        if spare.exists() && std::fs::rename(&spare, &p).is_ok() {
            sync_dir(dir);
        } else {
            prealloc(&p, size).expect("preallocazione segmento");
            sync_dir(dir);
        }
    }
    OpenOptions::new().write(true).open(&p).expect("apri segmento")
}

/// Thread del disco. `done(gen, last)` è chiamata dopo ogni fdatasync con l'ultimo indice reso durevole.
/// Un secondo thread tiene pronto un segmento di scorta già preallocato (`spare.log`).
pub fn spawn_disk(
    dir: PathBuf,
    rx: crossbeam_channel::Receiver<DiskOp>,
    seg_bytes: u64,
    done: impl Fn(u64, u64) + Send + 'static,
    flushed: impl Fn() -> Vec<([u8; 16], Pos)> + Send + 'static,
) {
    {
        let dir = dir.clone();
        std::thread::Builder::new()
            .name("prealloc".into())
            .spawn(move || loop {
                let spare = dir.join("spare.log");
                // con il riciclo si aspetta fino a 3 s un segmento uscito dal registro prima di scriverne uno di zeri
                if !spare.exists() && recycle_on() {
                    for _ in 0..30 {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                        if spare.exists() {
                            break;
                        }
                    }
                }
                if !spare.exists() {
                    let tmp = dir.join("spare.tmp");
                    let mbs = prealloc_mbs();
                    let r = if mbs > 0 { prealloc_paced(&tmp, seg_bytes, mbs) } else { prealloc(&tmp, seg_bytes) };
                    if r.is_ok() && std::fs::rename(&tmp, &spare).is_ok() {
                        sync_dir(&dir);
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            })
            .unwrap();
    }
    let (del_tx, del_rx) = crossbeam_channel::unbounded::<PathBuf>();
    {
        let dir = dir.clone();
        std::thread::Builder::new()
            .name("cancella-seg".into())
            .spawn(move || {
                let rec = recycle_on();
                while let Ok(p) = del_rx.recv() {
                    let spare = dir.join("spare.log");
                    if rec && !spare.exists() && recycle(&p, &spare, &dir, seg_bytes).is_ok() {
                        continue;
                    }
                    let _ = std::fs::remove_file(p);
                }
            })
            .unwrap();
    }
    std::thread::Builder::new()
        .name("disk".into())
        .spawn(move || {
            crate::node::set_thread_nice(-10);
            let mut cur: Option<(u32, File)> = None;
            let mut buf: Vec<u8> = Vec::with_capacity(4 << 20);
            while let Ok(first) = rx.recv() {
                let mut ops = vec![first];
                while let Ok(o) = rx.try_recv() {
                    ops.push(o);
                    if ops.len() > 4096 {
                        break;
                    }
                }
                let mut last_done = None;
                let mut dirty = false;
                for op in ops {
                    match op {
                        DiskOp::Append { seg, off, data, last, gen } => {
                            if cur.as_ref().map(|c| c.0) != Some(seg) {
                                if let Some((_, f)) = cur.take() {
                                    if dirty {
                                        f.sync_data().expect("fdatasync");
                                    }
                                }
                                cur = Some((seg, open_segment(&dir, seg, seg_bytes)));
                            }
                            buf.clear();
                            for d in &data {
                                buf.extend_from_slice(d);
                            }
                            cur.as_mut().unwrap().1.write_all_at(&buf, off).expect("scrittura registro");
                            dirty = true;
                            last_done = Some((gen, last));
                        }
                        DiskOp::Truncate { seg, off } => {
                            // quanto scritto prima del troncamento non conta più come "durevole fino a"
                            last_done = None;
                            if let Some((_, f)) = cur.take() {
                                if dirty {
                                    f.sync_data().expect("fdatasync");
                                    dirty = false;
                                }
                            }
                            // cancella i segmenti dopo `seg`
                            if let Ok(rd) = std::fs::read_dir(&dir) {
                                for e in rd.flatten() {
                                    if let Some(s) = parse_seg(&e.file_name().to_string_lossy()) {
                                        if s > seg {
                                            let _ = std::fs::remove_file(e.path());
                                        }
                                    }
                                }
                            }
                            // azzera da `off` alla fine: nessuna voce troncata deve poter riapparire al recupero
                            if let Ok(f) = OpenOptions::new().write(true).open(seg_path(&dir, seg)) {
                                let len = f.metadata().map(|m| m.len()).unwrap_or(0);
                                if len > off {
                                    zero_range(&f, off, len - off).expect("azzeramento");
                                }
                                f.sync_all().expect("fsync");
                            }
                            sync_dir(&dir);
                        }
                        DiskOp::Delete { seg } => {
                            let _ = std::fs::remove_file(seg_path(&dir, seg));
                        }
                        DiskOp::CkptDelete { prev_index, prev_term, prev_pos, segs } => {
                            // le posizioni su S3 lette adesso sono ≥ di quelle valide al momento della richiesta: vanno bene
                            let ck = Ckpt { prev_index, prev_term, prev_pos, flushed: flushed().into_iter().map(|(d, p)| (hex(&d), p)).collect() };
                            write_atomic(&dir, "ckpt", &serde_json::to_vec(&ck).unwrap()).expect("checkpoint");
                            // la cancellazione dei file (lenta con il `discard` del disco: ~1,4 s per 64 MB) va su un
                            // thread a parte, altrimenti ferma i fdatasync e quindi i commit
                            for s in segs {
                                let _ = del_tx.send(seg_path(&dir, s));
                            }
                        }
                    }
                }
                if dirty {
                    cur.as_ref().unwrap().1.sync_data().expect("fdatasync");
                }
                if let Some((g, l)) = last_done {
                    done(g, l);
                }
            }
        })
        .unwrap();
}

pub fn parse_seg(name: &str) -> Option<u32> {
    name.strip_prefix("seg-")?.strip_suffix(".log")?.parse().ok()
}

/// Lettura casuale di voci già sul disco (sfratto per il tetto di RAM, recupero di un follower in ritardo).
pub struct Reader {
    dir: PathBuf,
    files: HashMap<u32, File>,
}

impl Reader {
    pub fn new(dir: PathBuf) -> Self {
        Reader { dir, files: HashMap::new() }
    }
    pub fn read(&mut self, seg: u32, off: u64, len: u32) -> std::io::Result<Entry> {
        if !self.files.contains_key(&seg) {
            self.files.insert(seg, File::open(seg_path(&self.dir, seg))?);
        }
        let f = &self.files[&seg];
        let mut b = vec![0u8; len as usize];
        f.read_exact_at(&mut b, off)?;
        match read_frame(&b) {
            FrameRead::Ok(_, _) => Entry::decode(Bytes::from(b)).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame non valido")),
        }
    }
    pub fn forget(&mut self, seg: u32) {
        self.files.remove(&seg);
    }
    pub fn forget_all(&mut self) {
        self.files.clear();
    }
}

/// Una voce recuperata dal disco con la sua collocazione.
pub struct Recovered {
    pub entry: Entry,
    pub seg: u32,
    pub off: u64,
}

/// Rilegge tutti i segmenti in ordine. Si ferma al primo frame non valido (zeri della preallocazione, coda strappata)
/// o a una voce che non continua il registro (indice non contiguo, mandato che scende); da lì azzera il segmento e
/// cancella quelli successivi.
pub fn recover(dir: &Path) -> std::io::Result<(Vec<Recovered>, u32, u64)> {
    let mut segs: Vec<u32> = std::fs::read_dir(dir)?.flatten().filter_map(|e| parse_seg(&e.file_name().to_string_lossy())).collect();
    segs.sort();
    let mut out: Vec<Recovered> = vec![];
    let mut last_seg = 0;
    let mut last_len = 0;
    let mut stop = false;
    for &s in segs.iter() {
        if stop {
            std::fs::remove_file(seg_path(dir, s))?;
            continue;
        }
        let data = std::fs::read(seg_path(dir, s))?;
        let mut p = 0usize;
        loop {
            match read_frame(&data[p..]) {
                FrameRead::Ok(_, end) => {
                    let raw = Bytes::copy_from_slice(&data[p..p + end]);
                    let Ok(e) = Entry::decode(raw) else { break };
                    if let Some(prev) = out.last() {
                        if e.index != prev.entry.index + 1 || e.term < prev.entry.term {
                            break;
                        }
                    }
                    out.push(Recovered { entry: e, seg: s, off: p as u64 });
                    p += end;
                }
                _ => break,
            }
        }
        let f = OpenOptions::new().write(true).open(seg_path(dir, s))?;
        let len = data.len() as u64;
        // dopo l'ultimo frame valido: deve essere tutto zero (un frame strappato o vecchio non deve riapparire)
        if data[p..].iter().any(|b| *b != 0) {
            zero_range(&f, p as u64, len - p as u64)?;
            f.sync_all()?;
            stop = true;
        }
        last_seg = s;
        last_len = p as u64;
    }
    Ok((out, last_seg, last_len))
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct RaftState {
    pub term: u64,
    pub voted: i32,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct Ckpt {
    /// metadati della voce che precede la prima voce rimasta
    pub prev_index: u64,
    pub prev_term: u64,
    pub prev_pos: Pos,
    /// (db esadecimale, posizione su S3)
    pub flushed: Vec<(String, Pos)>,
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
pub fn unhex16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut o = [0u8; 16];
    for i in 0..16 {
        o[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(o)
}

#[cfg(test)]
mod tests_riciclo {
    use super::*;
    use crate::codec::{Body, Entry, Kind, Write};

    fn ent(i: u64) -> Bytes {
        let w = vec![Write { key: Bytes::from(format!("doc/{i}")), kind: Kind::Put, val: Bytes::from(vec![7u8; 1000]) }];
        Entry::encode(1, i, i << 16, Body::Rec { txn: [1; 16], db: [2; 16], session: None, writes: w }).raw
    }

    fn write_seg(dir: &Path, seg: u32, from: u64, to: u64, seg_bytes: u64) {
        let p = seg_path(dir, seg);
        if !p.exists() {
            prealloc(&p, seg_bytes).unwrap();
        }
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        let mut off = 0;
        for i in from..=to {
            let r = ent(i);
            f.write_all_at(&r, off).unwrap();
            off += r.len() as u64;
        }
        f.sync_all().unwrap();
    }

    #[test]
    fn recupero_con_segmento_riciclato() {
        let seg_bytes = 256 << 10;
        let dir = std::env::temp_dir().join(format!("pq-riciclo-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        // segmento 1: voci 1..200 (circa 220 KB), segmento 2: voci 201..400
        write_seg(&dir, 1, 1, 200, seg_bytes);
        write_seg(&dir, 2, 201, 400, seg_bytes);
        // il segmento 1 esce dal registro e diventa la scorta, poi il segmento 3
        let spare = dir.join("spare.log");
        recycle(&seg_path(&dir, 1), &spare, &dir, seg_bytes).unwrap();
        // caso A: crollo subito dopo il cambio di nome (niente scritto nel segmento 3)
        std::fs::rename(&spare, seg_path(&dir, 3)).unwrap();
        let (v, last, len) = recover(&dir).unwrap();
        assert_eq!(v.first().unwrap().entry.index, 201);
        assert_eq!(v.last().unwrap().entry.index, 400);
        assert_eq!((last, len), (3, 0));
        // caso B: nel segmento 3 (riciclato e azzerato dal recupero di prima? no: lo ricicliamo di nuovo da sporco)
        std::fs::remove_file(seg_path(&dir, 3)).unwrap();
        write_seg(&dir, 9, 1, 200, seg_bytes);
        recycle(&seg_path(&dir, 9), &spare, &dir, seg_bytes).unwrap();
        std::fs::rename(&spare, seg_path(&dir, 3)).unwrap();
        // scritte solo 100 voci nuove (circa 110 KB, oltre i 64 KB azzerati): dopo restano voci vecchie valide
        write_seg(&dir, 3, 401, 500, seg_bytes);
        let (v, last, _) = recover(&dir).unwrap();
        let idx: Vec<u64> = v.iter().map(|r| r.entry.index).collect();
        assert_eq!(idx, (201..=500).collect::<Vec<_>>(), "nessuna voce vecchia deve riapparire");
        assert_eq!(last, 3);
        // la coda vecchia è stata azzerata: un secondo recupero dà lo stesso risultato
        let (v2, _, _) = recover(&dir).unwrap();
        assert_eq!(v2.len(), 300);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
