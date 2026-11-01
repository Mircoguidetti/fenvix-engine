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
    Append { seg: u32, data: Vec<Bytes>, last: u64, gen: u64 },
    /// tronca il segmento `seg` a `off` byte e cancella i segmenti successivi
    Truncate { seg: u32, off: u64 },
    Delete { seg: u32 },
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

/// Thread del disco. `done(gen, last)` è chiamata dopo ogni fdatasync con l'ultimo indice reso durevole.
pub fn spawn_disk(dir: PathBuf, rx: crossbeam_channel::Receiver<DiskOp>, done: impl Fn(u64, u64) + Send + 'static) {
    std::thread::Builder::new()
        .name("disk".into())
        .spawn(move || {
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
                        DiskOp::Append { seg, data, last, gen } => {
                            if cur.as_ref().map(|c| c.0) != Some(seg) {
                                if let Some((_, f)) = cur.take() {
                                    if dirty {
                                        f.sync_data().expect("fdatasync");
                                        dirty = false;
                                    }
                                }
                                let f = OpenOptions::new().create(true).append(true).open(seg_path(&dir, seg)).expect("apri segmento");
                                sync_dir(&dir);
                                cur = Some((seg, f));
                            }
                            buf.clear();
                            for d in &data {
                                buf.extend_from_slice(d);
                            }
                            cur.as_mut().unwrap().1.write_all(&buf).expect("scrittura registro");
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
                            if let Ok(f) = OpenOptions::new().write(true).open(seg_path(&dir, seg)) {
                                f.set_len(off).expect("truncate");
                                f.sync_all().expect("fsync");
                            }
                            sync_dir(&dir);
                        }
                        DiskOp::Delete { seg } => {
                            let _ = std::fs::remove_file(seg_path(&dir, seg));
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

/// Rilegge tutti i segmenti in ordine; taglia la coda strappata dell'ultimo.
pub fn recover(dir: &Path) -> std::io::Result<(Vec<Recovered>, u32, u64)> {
    let mut segs: Vec<u32> = std::fs::read_dir(dir)?.flatten().filter_map(|e| parse_seg(&e.file_name().to_string_lossy())).collect();
    segs.sort();
    let mut out = vec![];
    let mut last_seg = 0;
    let mut last_len = 0;
    for (k, &s) in segs.iter().enumerate() {
        let data = std::fs::read(seg_path(dir, s))?;
        let mut p = 0usize;
        loop {
            match read_frame(&data[p..]) {
                FrameRead::Ok(_, end) => {
                    let raw = Bytes::copy_from_slice(&data[p..p + end]);
                    match Entry::decode(raw) {
                        Ok(e) => out.push(Recovered { entry: e, seg: s, off: p as u64 }),
                        Err(_) => break,
                    }
                    p += end;
                }
                _ => break,
            }
        }
        if p < data.len() {
            if k + 1 != segs.len() {
                eprintln!("ATTENZIONE: segmento {s} corrotto a metà ({p}/{}), non è l'ultimo", data.len());
            }
            let f = OpenOptions::new().write(true).open(seg_path(dir, s))?;
            f.set_len(p as u64)?;
            f.sync_all()?;
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
