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
            // end of the last frame written in the current segment (Q8c: tail zeroing on segment switch)
            let mut cur_end: u64 = 0;
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
                                    // Q8c: zero the tail after the last frame of the segment we leave (a recycled
                                    // segment still has stale bytes there), before its fdatasync and before any byte
                                    // of the next segment is written: on disk only the last segment can have a dirty tail
                                    let flen = f.metadata().map(|m| m.len()).unwrap_or(0);
                                    if recycle_on() && cur_end > 0 && flen > cur_end {
                                        zero_range(&f, cur_end, flen - cur_end).expect("tail zeroing");
                                        dirty = true;
                                    }
                                    if dirty {
                                        f.sync_data().expect("fdatasync");
                                    }
                                }
                                cur = Some((seg, open_segment(&dir, seg, seg_bytes)));
                                cur_end = 0;
                            }
                            buf.clear();
                            for d in &data {
                                buf.extend_from_slice(d);
                            }
                            cur.as_mut().unwrap().1.write_all_at(&buf, off).expect("scrittura registro");
                            cur_end = off + buf.len() as u64;
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
                            cur_end = 0;
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
    recover_after(dir, None)
}

/// Q8d: recovery that knows the checkpoint (`prev_index`: every entry up to it is checkpointed). Entries up to the
/// checkpoint at the start of the log are read and skipped (not returned), so they are never the reference of the
/// contiguity check. Before this, a segment left on disk below the checkpoint (killed between the checkpoint and the
/// deletion of its segments, and then never deleted) followed by a gap made the recovery take the first live segment
/// for stale bytes: it was zeroed and every later segment deleted, losing durable confirmed entries (found by the Q8d
/// rerun of `restart_test.py` under CPU load: a copy restarted with 4,650 confirmed entries missing).
pub fn recover_after(dir: &Path, prev_index: Option<u64>) -> std::io::Result<(Vec<Recovered>, u32, u64)> {
    let mut segs: Vec<u32> = std::fs::read_dir(dir)?.flatten().filter_map(|e| parse_seg(&e.file_name().to_string_lossy())).collect();
    segs.sort();
    let mut out: Vec<Recovered> = vec![];
    // Q8d: entries up to the checkpoint (returned after the live ones, see `recover_after`)
    let mut skipped: Vec<Recovered> = vec![];
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
                    if out.is_empty() && prev_index.map_or(false, |pi| e.index <= pi) {
                        // checkpointed entry before the live log: skipped (Q8d)
                        skipped.push(Recovered { entry: e, seg: s, off: p as u64 });
                        p += end;
                        continue;
                    }
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
        // After the last valid frame everything must be zero (a torn or stale frame must never reappear).
        if data[p..].iter().any(|b| *b != 0) {
            zero_range(&f, p as u64, len - p as u64)?;
            f.sync_all()?;
            // Q8c fix (recycling bug found by the Q8b reviewer): a recycled segment that is NOT the last one can hold
            // stale bytes after its last valid frame (the leader moved to the next segment because the next entry did
            // not fit). Non-last segments are always complete (the disk thread syncs a segment before opening the
            // next one), so the stale tail is not the end of the log when the next segment continues the indices.
            // Only stop (and drop the later segments) when the next segment does not continue the log.
            let next = segs.iter().copied().find(|x| *x > s);
            if !next.map(|n| continues(dir, n, out.last().map(|r| (r.entry.index, r.entry.term)))).unwrap_or(false) {
                stop = true;
            }
        }
        last_seg = s;
        last_len = p as u64;
    }
    // the caller skips entries below its front anyway; returned first so it can register their segments for deletion
    skipped.append(&mut out);
    Ok((skipped, last_seg, last_len))
}

/// True when the first frame of segment `n` is a valid entry that continues the log after `prev` (index + 1, term
/// not lower). With no previous entry (everything before was checkpointed away) any valid first frame continues.
fn continues(dir: &Path, n: u32, prev: Option<(u64, u64)>) -> bool {
    let Ok(data) = std::fs::read(seg_path(dir, n)) else { return false };
    match read_frame(&data) {
        FrameRead::Ok(_, end) => match Entry::decode(Bytes::copy_from_slice(&data[..end])) {
            Ok(e) => prev.map(|(i, t)| e.index == i + 1 && e.term >= t).unwrap_or(true),
            Err(_) => false,
        },
        _ => false,
    }
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

    /// Revisore Q8b: un segmento riciclato che NON è l'ultimo (il registro è passato al segmento dopo perché la voce
    /// successiva non ci stava) ha in coda byte vecchi. Il recupero non deve cancellare i segmenti successivi.
    #[test]
    fn rev_q8b_segmento_riciclato_non_ultimo() {
        let seg_bytes = 256 << 10;
        let dir = std::env::temp_dir().join(format!("pq-riciclo-rev-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let spare = dir.join("spare.log");
        // un segmento vecchio quasi pieno (voci 1..230) diventa la scorta
        write_seg(&dir, 9, 1, 230, seg_bytes);
        recycle(&seg_path(&dir, 9), &spare, &dir, seg_bytes).unwrap();
        std::fs::rename(&spare, seg_path(&dir, 3)).unwrap();
        // nel segmento 3 entrano le voci 401..600 e il registro passa al 4 (qui prima della fine del segmento: la coda
        // del segmento 3 ha ancora le voci vecchie, come quando la voce successiva non ci sta)
        write_seg(&dir, 3, 401, 600, seg_bytes);
        write_seg(&dir, 4, 601, 650, seg_bytes);
        let (v, last, _) = recover(&dir).unwrap();
        let idx: Vec<u64> = v.iter().map(|r| r.entry.index).collect();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(last, 4, "il segmento 4 (voci durevoli e confermate) deve sopravvivere");
        assert_eq!(idx, (401..=650).collect::<Vec<_>>());
    }
    /// Q8d: a segment left below the checkpoint (its deletion did not happen before a kill) and a gap after it (the
    /// segments in between were deleted). Without the checkpoint the recovery zeroes the first live segment and deletes
    /// the later ones; with it every live entry survives.
    #[test]
    fn q8d_leftover_segment_below_checkpoint_then_gap() {
        let seg_bytes = 256 << 10;
        for with_ckpt in [true, false] {
            let dir = std::env::temp_dir().join(format!("pq-q8d-gap-{}", rand::random::<u64>()));
            std::fs::create_dir_all(&dir).unwrap();
            write_seg(&dir, 5, 1001, 1100, seg_bytes); // leftover, all checkpointed
            // segments 6 and 7 (1101..1300) were deleted; the live log is 1301..1500 in 8 and 9, checkpoint 1300
            write_seg(&dir, 8, 1301, 1400, seg_bytes);
            write_seg(&dir, 9, 1401, 1500, seg_bytes);
            let (v, last, _) = recover_after(&dir, if with_ckpt { Some(1300) } else { None }).unwrap();
            let live: Vec<u64> = v.iter().map(|r| r.entry.index).filter(|i| *i > 1300).collect();
            let _ = std::fs::remove_dir_all(&dir);
            if with_ckpt {
                assert_eq!(live, (1301..=1500).collect::<Vec<_>>(), "every live entry must survive");
                assert_eq!(last, 9);
            } else {
                // the old behaviour (negative control): the live segments are lost
                assert!(live.is_empty(), "control: without the checkpoint the gap loses the live log");
            }
        }
    }

    /// Q8c: several recycled segments in a row, none of them the last one, each with stale old entries in its tail.
    /// Recovery keeps every entry, twice in a row (the first recovery zeroes the stale tails).
    #[test]
    fn q8c_many_recycled_segments_not_last() {
        let seg_bytes = 256 << 10;
        let dir = std::env::temp_dir().join(format!("pq-q8c-many-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let spare = dir.join("spare.log");
        let mut next = 1001u64;
        for seg in 3..=7u32 {
            // an old full segment (indices far below the log start) becomes the spare, then segment `seg`
            write_seg(&dir, 90 + seg, 1, 230, seg_bytes);
            recycle(&seg_path(&dir, 90 + seg), &spare, &dir, seg_bytes).unwrap();
            std::fs::rename(&spare, seg_path(&dir, seg)).unwrap();
            // fewer new entries than the old ones: stale entries remain after the last valid frame
            let n = 150 + (seg as u64 * 7) % 40;
            write_seg(&dir, seg, next, next + n - 1, seg_bytes);
            next += n;
        }
        for round in 0..2 {
            let (v, last, _) = recover(&dir).unwrap();
            let idx: Vec<u64> = v.iter().map(|r| r.entry.index).collect();
            assert_eq!(last, 7, "round {round}: the last segment must survive");
            assert_eq!(idx, (1001..next).collect::<Vec<_>>(), "round {round}: no confirmed entry lost, no stale entry back");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Q8c: a stale tail followed by a segment that does NOT continue the indices is still the end of the log
    /// (the old safety rule is kept: later segments are dropped).
    #[test]
    fn q8c_gap_after_dirty_tail_still_stops() {
        let seg_bytes = 256 << 10;
        let dir = std::env::temp_dir().join(format!("pq-q8c-gap-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let spare = dir.join("spare.log");
        write_seg(&dir, 9, 1, 230, seg_bytes);
        recycle(&seg_path(&dir, 9), &spare, &dir, seg_bytes).unwrap();
        std::fs::rename(&spare, seg_path(&dir, 3)).unwrap();
        write_seg(&dir, 3, 401, 600, seg_bytes);
        // segment 4 starts at 700: a hole of 99 entries, not a continuation
        write_seg(&dir, 4, 700, 720, seg_bytes);
        let (v, last, _) = recover(&dir).unwrap();
        let idx: Vec<u64> = v.iter().map(|r| r.entry.index).collect();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(idx, (401..=600).collect::<Vec<_>>());
        assert_eq!(last, 3);
    }

    /// Q8c: the disk thread itself, with recycling on. A dirty spare becomes segment 1; the log moves to segment 2
    /// before segment 1 is full. The tail of segment 1 must be zero on disk (written before segment 2), and recovery
    /// must return every entry.
    #[test]
    fn q8c_disk_thread_zeroes_tail_on_switch() {
        std::env::set_var("PQ_RICICLA", "1");
        let seg_bytes = 256 << 10;
        let dir = std::env::temp_dir().join(format!("pq-q8c-disk-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let spare = dir.join("spare.log");
        write_seg(&dir, 50, 1, 230, seg_bytes);
        recycle(&seg_path(&dir, 50), &spare, &dir, seg_bytes).unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let (dtx, drx) = crossbeam_channel::unbounded::<u64>();
        spawn_disk(dir.clone(), rx, seg_bytes, move |_, l| { let _ = dtx.send(l); }, Vec::new);
        let a: Vec<Bytes> = (1001..=1100).map(ent).collect();
        let end1: u64 = a.iter().map(|b| b.len() as u64).sum();
        tx.send(DiskOp::Append { seg: 1, off: 0, data: a, last: 1100, gen: 0 }).unwrap();
        while drx.recv().unwrap() < 1100 {}
        let b: Vec<Bytes> = (1101..=1150).map(ent).collect();
        tx.send(DiskOp::Append { seg: 2, off: 0, data: b, last: 1150, gen: 0 }).unwrap();
        while drx.recv().unwrap() < 1150 {}
        let d1 = std::fs::read(seg_path(&dir, 1)).unwrap();
        assert!(d1[end1 as usize..].iter().all(|x| *x == 0), "stale tail of segment 1 not zeroed");
        drop(tx);
        std::thread::sleep(std::time::Duration::from_millis(50));
        let (v, last, _) = recover(&dir).unwrap();
        let idx: Vec<u64> = v.iter().map(|r| r.entry.index).collect();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(last, 2);
        assert_eq!(idx, (1001..=1150).collect::<Vec<_>>());
    }
}
