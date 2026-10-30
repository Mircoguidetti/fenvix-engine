//! Byte encoding of the journal3z records (disk) and messages (network).
//!
//! Frame (disk and network): `len u32 LE | crc32 u32 LE | body` where `len` is the body length and `crc32` the CRC-32
//! (IEEE) of the body. A frame whose length runs past the available bytes, or whose CRC does not match, is invalid.
//!
//! Record body: `kind u8 | db u64 | fields`. Message body: `version u8 (= WIRE_VERSION) | tag u8 | fields`.
//! All integers little-endian. Decoding never panics on hostile input: every read is bounds-checked.

use bytes::Bytes;
use journal3z_core::{Entry, Msg, Rec};

pub const WIRE_VERSION: u8 = 1;
/// Upper bound for one frame (a batch of messages or one record): refuses absurd lengths from corrupt data.
pub const MAX_FRAME: usize = 64 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Short,
    BadKind(u8),
    BadVersion(u8),
    Trailing(usize),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DecodeError {}

// ------------------------------------------------------------------------------------------------------ frames

pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(body.len() + 8);
    v.extend_from_slice(&(body.len() as u32).to_le_bytes());
    v.extend_from_slice(&crc32fast::hash(body).to_le_bytes());
    v.extend_from_slice(body);
    v
}

/// Result of reading one frame at the start of `buf`.
#[derive(Debug, PartialEq, Eq)]
pub enum FrameRead<'a> {
    /// A valid frame: its body and the total number of bytes it used.
    Ok(&'a [u8], usize),
    /// Not enough bytes for a whole frame (a torn tail, or more data still to come on a stream).
    Incomplete,
    /// Enough bytes but the length is absurd or the CRC does not match.
    Corrupt,
}

pub fn read_frame(buf: &[u8]) -> FrameRead<'_> {
    if buf.len() < 8 {
        return FrameRead::Incomplete;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if len == 0 || len > MAX_FRAME {
        return FrameRead::Corrupt;
    }
    if buf.len() < 8 + len {
        return FrameRead::Incomplete;
    }
    let body = &buf[8..8 + len];
    if crc32fast::hash(body) != crc {
        return FrameRead::Corrupt;
    }
    FrameRead::Ok(body, 8 + len)
}

// ------------------------------------------------------------------------------------------------------ reader

pub struct Rd<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Rd<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Rd { b, p: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.p + n > self.b.len() {
            return Err(DecodeError::Short);
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bool(&mut self) -> Result<bool, DecodeError> {
        Ok(self.u8()? != 0)
    }
    pub fn rest(&self) -> usize {
        self.b.len() - self.p
    }
    pub fn end(&self) -> Result<(), DecodeError> {
        match self.rest() {
            0 => Ok(()),
            n => Err(DecodeError::Trailing(n)),
        }
    }
}

fn put_u64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_le_bytes());
}

fn put_entry(v: &mut Vec<u8>, e: &Entry) {
    put_u64(v, e.idx);
    put_u64(v, e.epoch);
    v.push(e.barrier as u8);
    v.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
    v.extend_from_slice(&e.data);
}

fn get_entry(r: &mut Rd) -> Result<Entry, DecodeError> {
    let idx = r.u64()?;
    let epoch = r.u64()?;
    let barrier = r.bool()?;
    let n = r.u32()? as usize;
    let data = Bytes::copy_from_slice(r.take(n)?);
    Ok(Entry { idx, epoch, barrier, data })
}

fn put_entries(v: &mut Vec<u8>, es: &[Entry]) {
    v.extend_from_slice(&(es.len() as u32).to_le_bytes());
    for e in es {
        put_entry(v, e);
    }
}

fn get_entries(r: &mut Rd) -> Result<Vec<Entry>, DecodeError> {
    let n = r.u32()? as usize;
    // every entry takes at least 21 bytes: refuse a count the remaining bytes cannot hold
    if n > r.rest() / 21 + 1 {
        return Err(DecodeError::Short);
    }
    (0..n).map(|_| get_entry(r)).collect()
}

// ------------------------------------------------------------------------------------------------------ records

pub const R_PROMISE: u8 = 1;
pub const R_TRUNCATE: u8 = 2;
pub const R_APPEND: u8 = 3;
pub const R_TRIM: u8 = 4;
pub const R_ERASE: u8 = 5;

pub fn encode_rec(r: &Rec) -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    match r {
        Rec::Promise { db, epoch } => {
            v.push(R_PROMISE);
            put_u64(&mut v, *db);
            put_u64(&mut v, *epoch);
        }
        Rec::Truncate { db, from } => {
            v.push(R_TRUNCATE);
            put_u64(&mut v, *db);
            put_u64(&mut v, *from);
        }
        Rec::Append { db, e } => {
            v.push(R_APPEND);
            put_u64(&mut v, *db);
            put_entry(&mut v, e);
        }
        Rec::Trim { db, upto } => {
            v.push(R_TRIM);
            put_u64(&mut v, *db);
            put_u64(&mut v, *upto);
        }
        Rec::Erase { db } => {
            v.push(R_ERASE);
            put_u64(&mut v, *db);
        }
    }
    v
}

pub fn decode_rec(b: &[u8]) -> Result<Rec, DecodeError> {
    let mut r = Rd::new(b);
    let kind = r.u8()?;
    let db = r.u64()?;
    let rec = match kind {
        R_PROMISE => Rec::Promise { db, epoch: r.u64()? },
        R_TRUNCATE => Rec::Truncate { db, from: r.u64()? },
        R_APPEND => Rec::Append { db, e: get_entry(&mut r)? },
        R_TRIM => Rec::Trim { db, upto: r.u64()? },
        R_ERASE => Rec::Erase { db },
        k => return Err(DecodeError::BadKind(k)),
    };
    r.end()?;
    Ok(rec)
}

// ------------------------------------------------------------------------------------------------------ messages

const M_FENCE: u8 = 1;
const M_PROMISE: u8 = 2;
const M_REJECT: u8 = 3;
const M_READ: u8 = 4;
const M_ENTRIES: u8 = 5;
const M_APPEND: u8 = 6;
const M_ACK: u8 = 7;
const M_MISMATCH: u8 = 8;
const M_TRIM: u8 = 9;
const M_ERASE: u8 = 10;

pub fn encode_msg(m: &Msg, v: &mut Vec<u8>) {
    let u = |v: &mut Vec<u8>, xs: &[u64]| {
        for x in xs {
            put_u64(v, *x);
        }
    };
    match m {
        Msg::Fence { db, epoch, applied } => {
            v.push(M_FENCE);
            u(v, &[*db, *epoch, *applied]);
        }
        Msg::Promise { db, epoch, last, last_epoch } => {
            v.push(M_PROMISE);
            u(v, &[*db, *epoch, *last, *last_epoch]);
        }
        Msg::Reject { db, epoch, promised } => {
            v.push(M_REJECT);
            u(v, &[*db, *epoch, *promised]);
        }
        Msg::Read { db, epoch, from, to } => {
            v.push(M_READ);
            u(v, &[*db, *epoch, *from, *to]);
        }
        Msg::Entries { db, epoch, ok, entries } => {
            v.push(M_ENTRIES);
            u(v, &[*db, *epoch]);
            v.push(*ok as u8);
            put_entries(v, entries);
        }
        Msg::Append { db, epoch, applied, prev_idx, prev_epoch, entries, commit } => {
            v.push(M_APPEND);
            u(v, &[*db, *epoch, *applied, *prev_idx, *prev_epoch, *commit]);
            put_entries(v, entries);
        }
        Msg::Ack { db, epoch, matched } => {
            v.push(M_ACK);
            u(v, &[*db, *epoch, *matched]);
        }
        Msg::Mismatch { db, epoch, hint } => {
            v.push(M_MISMATCH);
            u(v, &[*db, *epoch, *hint]);
        }
        Msg::Trim { db, epoch, upto } => {
            v.push(M_TRIM);
            u(v, &[*db, *epoch, *upto]);
        }
        Msg::Erase { db } => {
            v.push(M_ERASE);
            u(v, &[*db]);
        }
    }
}

pub fn decode_msg(r: &mut Rd) -> Result<Msg, DecodeError> {
    let tag = r.u8()?;
    Ok(match tag {
        M_FENCE => Msg::Fence { db: r.u64()?, epoch: r.u64()?, applied: r.u64()? },
        M_PROMISE => Msg::Promise { db: r.u64()?, epoch: r.u64()?, last: r.u64()?, last_epoch: r.u64()? },
        M_REJECT => Msg::Reject { db: r.u64()?, epoch: r.u64()?, promised: r.u64()? },
        M_READ => Msg::Read { db: r.u64()?, epoch: r.u64()?, from: r.u64()?, to: r.u64()? },
        M_ENTRIES => {
            let (db, epoch) = (r.u64()?, r.u64()?);
            let ok = r.bool()?;
            Msg::Entries { db, epoch, ok, entries: get_entries(r)? }
        }
        M_APPEND => {
            let (db, epoch, applied, prev_idx, prev_epoch, commit) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?, r.u64()?, r.u64()?);
            Msg::Append { db, epoch, applied, prev_idx, prev_epoch, entries: get_entries(r)?, commit }
        }
        M_ACK => Msg::Ack { db: r.u64()?, epoch: r.u64()?, matched: r.u64()? },
        M_MISMATCH => Msg::Mismatch { db: r.u64()?, epoch: r.u64()?, hint: r.u64()? },
        M_TRIM => Msg::Trim { db: r.u64()?, epoch: r.u64()?, upto: r.u64()? },
        M_ERASE => Msg::Erase { db: r.u64()? },
        k => return Err(DecodeError::BadKind(k)),
    })
}

/// A batch of messages as one frame body: `version | count u32 | messages`.
pub fn encode_batch(msgs: &[Msg]) -> Vec<u8> {
    let mut body = Vec::with_capacity(64 * msgs.len());
    body.push(WIRE_VERSION);
    body.extend_from_slice(&(msgs.len() as u32).to_le_bytes());
    for m in msgs {
        encode_msg(m, &mut body);
    }
    frame(&body)
}

pub fn decode_batch(body: &[u8]) -> Result<Vec<Msg>, DecodeError> {
    let mut r = Rd::new(body);
    let v = r.u8()?;
    if v != WIRE_VERSION {
        return Err(DecodeError::BadVersion(v));
    }
    let n = r.u32()? as usize;
    if n > r.rest() {
        return Err(DecodeError::Short);
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(decode_msg(&mut r)?);
    }
    r.end()?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    fn rand_entry(rng: &mut impl Rng) -> Entry {
        let n = rng.gen_range(0..64);
        Entry { idx: rng.gen(), epoch: rng.gen(), barrier: rng.gen(), data: Bytes::from((0..n).map(|_| rng.gen::<u8>()).collect::<Vec<_>>()) }
    }

    fn rand_rec(rng: &mut impl Rng) -> Rec {
        let db = rng.gen();
        match rng.gen_range(0..5) {
            0 => Rec::Promise { db, epoch: rng.gen() },
            1 => Rec::Truncate { db, from: rng.gen() },
            2 => Rec::Append { db, e: rand_entry(rng) },
            3 => Rec::Trim { db, upto: rng.gen() },
            _ => Rec::Erase { db },
        }
    }

    fn rand_msg(rng: &mut impl Rng) -> Msg {
        let (db, epoch) = (rng.gen(), rng.gen());
        let es = |rng: &mut _| (0..rng_n(rng)).map(|_| rand_entry(rng)).collect();
        fn rng_n(rng: &mut impl Rng) -> usize {
            rng.gen_range(0..4)
        }
        match rng.gen_range(0..10) {
            0 => Msg::Fence { db, epoch, applied: rng.gen() },
            1 => Msg::Promise { db, epoch, last: rng.gen(), last_epoch: rng.gen() },
            2 => Msg::Reject { db, epoch, promised: rng.gen() },
            3 => Msg::Read { db, epoch, from: rng.gen(), to: rng.gen() },
            4 => Msg::Entries { db, epoch, ok: rng.gen(), entries: es(rng) },
            5 => Msg::Append { db, epoch, applied: rng.gen(), prev_idx: rng.gen(), prev_epoch: rng.gen(), entries: es(rng), commit: rng.gen() },
            6 => Msg::Ack { db, epoch, matched: rng.gen() },
            7 => Msg::Mismatch { db, epoch, hint: rng.gen() },
            8 => Msg::Trim { db, epoch, upto: rng.gen() },
            _ => Msg::Erase { db },
        }
    }

    #[test]
    fn records_roundtrip() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        for _ in 0..20_000 {
            let r = rand_rec(&mut rng);
            let b = encode_rec(&r);
            let d = decode_rec(&b).unwrap();
            assert_eq!(format!("{r:?}"), format!("{d:?}"));
            assert_eq!(encode_rec(&d), b);
        }
    }

    #[test]
    fn batches_roundtrip() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        for _ in 0..5_000 {
            let ms: Vec<Msg> = (0..rng.gen_range(0..8)).map(|_| rand_msg(&mut rng)).collect();
            let f = encode_batch(&ms);
            let FrameRead::Ok(body, used) = read_frame(&f) else { panic!("frame") };
            assert_eq!(used, f.len());
            assert_eq!(decode_batch(body).unwrap(), ms);
        }
    }

    /// Negative control for the frame check: every single-bit flip and every truncation of a frame is detected
    /// (never returned as a valid frame with other content).
    #[test]
    fn frames_detect_flips_and_truncation() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        for _ in 0..300 {
            let body = encode_rec(&rand_rec(&mut rng));
            let f = frame(&body);
            for cut in 0..f.len() {
                assert_eq!(read_frame(&f[..cut]), FrameRead::Incomplete, "a truncated frame must never read as valid");
            }
            for bit in 0..f.len() * 8 {
                let mut g = f.clone();
                g[bit / 8] ^= 1 << (bit % 8);
                match read_frame(&g) {
                    FrameRead::Ok(b, _) => panic!("bit {bit} flipped and the frame still reads as valid: {b:?}"),
                    FrameRead::Incomplete | FrameRead::Corrupt => {}
                }
            }
        }
    }

    /// Hostile input never panics.
    #[test]
    fn decoders_never_panic() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(4);
        for _ in 0..100_000 {
            let n = rng.gen_range(0..80);
            let b: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
            let _ = decode_rec(&b);
            let _ = decode_batch(&b);
            let _ = read_frame(&b);
        }
    }

    #[test]
    fn wrong_version_refused() {
        let f = encode_batch(&[Msg::Erase { db: 1 }]);
        let FrameRead::Ok(body, _) = read_frame(&f) else { panic!() };
        let mut b = body.to_vec();
        b[0] = 2;
        assert_eq!(decode_batch(&b), Err(DecodeError::BadVersion(2)));
    }
}
