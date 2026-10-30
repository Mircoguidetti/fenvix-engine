//! Batch encoding shared by every backend.
//!
//! One commit (an S3/Express object, a DynamoDB item, a frame in a local log file) carries one batch:
//!   magic "WB01" | seq u64 | count u32 | count x (db u32, id u64, len u32, data) | crc32 u32 (of everything before)
//! Local log files hold frames `len u32 | batch`; a zero length (pre-zeroed file) or a bad crc ends the log.
//! Write payloads are a pure function of (id, len), so recovery checks the data of every entry, not only the id.

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};

const MAGIC: &[u8; 4] = b"WB01";

#[derive(Clone, Debug)]
pub struct Entry {
    pub db: u32,
    pub id: u64,
    pub data: Bytes,
}

pub fn encode(seq: u64, entries: &[Entry]) -> Bytes {
    let size: usize = 4 + 8 + 4 + entries.iter().map(|e| 16 + e.data.len()).sum::<usize>() + 4;
    let mut b = BytesMut::with_capacity(size);
    b.put_slice(MAGIC);
    b.put_u64_le(seq);
    b.put_u32_le(entries.len() as u32);
    for e in entries {
        b.put_u32_le(e.db);
        b.put_u64_le(e.id);
        b.put_u32_le(e.data.len() as u32);
        b.put_slice(&e.data);
    }
    let crc = crc32fast::hash(&b);
    b.put_u32_le(crc);
    b.freeze()
}

pub struct Decoded {
    pub seq: u64,
    pub entries: Vec<Entry>,
}

pub fn decode(b: &[u8]) -> Result<Decoded> {
    if b.len() < 20 || &b[0..4] != MAGIC {
        bail!("bad magic or short batch ({} bytes)", b.len());
    }
    let body = &b[..b.len() - 4];
    let crc = u32::from_le_bytes(b[b.len() - 4..].try_into().unwrap());
    if crc32fast::hash(body) != crc {
        bail!("crc mismatch");
    }
    let seq = u64::from_le_bytes(body[4..12].try_into().unwrap());
    let n = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let mut p = 16;
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        if p + 16 > body.len() {
            bail!("truncated entry header");
        }
        let db = u32::from_le_bytes(body[p..p + 4].try_into().unwrap());
        let id = u64::from_le_bytes(body[p + 4..p + 12].try_into().unwrap());
        let len = u32::from_le_bytes(body[p + 12..p + 16].try_into().unwrap()) as usize;
        p += 16;
        if p + len > body.len() {
            bail!("truncated entry data");
        }
        entries.push(Entry { db, id, data: Bytes::copy_from_slice(&body[p..p + len]) });
        p += len;
    }
    if p != body.len() {
        bail!("trailing bytes in batch");
    }
    Ok(Decoded { seq, entries })
}

/// Deterministic payload of write `id` (splitmix64 stream), so recovery can check every byte.
pub fn payload(id: u64, len: usize) -> Bytes {
    let mut out = BytesMut::with_capacity(len + 8);
    let mut x = id.wrapping_add(0x9E3779B97F4A7C15);
    while out.len() < len {
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        out.put_u64_le(z);
    }
    out.truncate(len);
    out.freeze()
}

pub fn check_payload(e: &Entry) -> bool {
    payload(e.id, e.data.len()) == e.data
}

/// Parse a local log (frames `len u32 | batch`). Returns the batches and the offset just after the last valid frame.
pub fn parse_frames(buf: &[u8]) -> (Vec<Decoded>, usize, Option<String>) {
    let mut p = 0usize;
    let mut out = Vec::new();
    let mut stop = None;
    while p + 4 <= buf.len() {
        let len = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap()) as usize;
        if len == 0 {
            break;
        }
        if p + 4 + len > buf.len() {
            stop = Some(format!("frame at {p} runs past the end"));
            break;
        }
        match decode(&buf[p + 4..p + 4 + len]) {
            Ok(d) => out.push(d),
            Err(e) => {
                stop = Some(format!("frame at {p}: {e}"));
                break;
            }
        }
        p += 4 + len;
    }
    (out, p, stop)
}

/// Scan a log file from the start, reading only as far as the log goes (a pre-zeroed 256 MB file with 1 MB of log
/// reads ~2 MB), so recovery time reflects the log, not the pre-allocated size.
pub fn scan_file(path: &str) -> std::io::Result<(Vec<Decoded>, usize, Option<String>)> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len() as usize;
    let mut buf = Vec::new();
    let mut chunk = 1usize << 20;
    loop {
        let want = chunk.min(len - buf.len());
        let start = buf.len();
        buf.resize(start + want, 0);
        f.read_exact(&mut buf[start..])?;
        let (b, end, stop) = parse_frames(&buf);
        let at_end = buf.len() == len;
        let zero_next = end + 4 <= buf.len() && buf[end..end + 4] == [0, 0, 0, 0];
        let hard_stop = stop.as_deref().map(|s| !s.contains("runs past the end")).unwrap_or(false);
        if at_end || zero_next || hard_stop {
            return Ok((b, end, stop));
        }
        chunk *= 2;
    }
}

pub fn frame(batch: &Bytes) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + batch.len());
    b.put_u32_le(batch.len() as u32);
    b.put_slice(batch);
    b.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let es: Vec<Entry> = (0..5).map(|i| Entry { db: i, id: 100 + i as u64, data: payload(100 + i as u64, 200) }).collect();
        let b = encode(7, &es);
        let d = decode(&b).unwrap();
        assert_eq!(d.seq, 7);
        assert_eq!(d.entries.len(), 5);
        assert!(d.entries.iter().all(check_payload));
        let mut bad = b.to_vec();
        bad[30] ^= 1;
        assert!(decode(&bad).is_err());
        let mut file = frame(&b).to_vec();
        file.extend_from_slice(&frame(&encode(8, &es[..1])));
        file.extend_from_slice(&[0u8; 64]);
        let (bs, end, stop) = parse_frames(&file);
        assert_eq!(bs.len(), 2);
        assert!(stop.is_none());
        assert_eq!(end, file.len() - 64);
        let torn = &file[..end - 3];
        let (bs, _, stop) = parse_frames(torn);
        assert_eq!(bs.len(), 1);
        assert!(stop.is_some());
    }
}
