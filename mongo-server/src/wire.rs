//! MongoDB wire protocol framing: OP_MSG (2013) for commands, OP_QUERY (2004) / OP_REPLY (1)
//! for the legacy `isMaster` handshake drivers send before they know the server's version.

use anyhow::{anyhow, bail, Result};
use bson::{Bson, Document};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_COMPRESSED: i32 = 2012;
pub const OP_MSG: i32 = 2013;

const MAX_MESSAGE: usize = 48_000_000 + 1024 * 1024;
const FLAG_CHECKSUM: u32 = 1;
const FLAG_MORE_TO_COME: u32 = 2;
const FLAG_EXHAUST_ALLOWED: u32 = 1 << 16;

pub struct Request {
    pub request_id: i32,
    pub legacy: bool,
    /// `$db` for OP_MSG, the database part of `db.$cmd` for OP_QUERY.
    pub db: String,
    pub command: Document,
    /// The sender does not want a reply (unacknowledged write).
    pub more_to_come: bool,
    /// The sender accepts a stream of replies to this one request (`exhaustAllowed`): drivers ask it for the awaitable
    /// `hello` of their monitoring connection and expect the replies flagged `moreToCome`.
    pub exhaust_allowed: bool,
    /// Fields that arrived as document sequences (`documents`, `updates`, `deletes`).
    pub seq_keys: Vec<String>,
    /// The compressor (1 snappy, 2 zlib, 3 zstd) of an OP_COMPRESSED request: the reply is compressed the same way.
    pub compressor: Option<u8>,
}

fn read_i32(b: &[u8], pos: &mut usize) -> Result<i32> {
    let s = b.get(*pos..*pos + 4).ok_or_else(|| anyhow!("truncated message"))?;
    *pos += 4;
    Ok(i32::from_le_bytes(s.try_into().unwrap()))
}

fn read_cstring(b: &[u8], pos: &mut usize) -> Result<String> {
    let end = b[*pos..].iter().position(|&c| c == 0).ok_or_else(|| anyhow!("unterminated cstring"))?;
    let s = String::from_utf8(b[*pos..*pos + end].to_vec())?;
    *pos += end + 1;
    Ok(s)
}

fn read_doc(b: &[u8], pos: &mut usize) -> Result<Document> {
    let len = read_i32(b, &mut { *pos })? as usize;
    let bytes = b.get(*pos..*pos + len).ok_or_else(|| anyhow!("truncated document"))?;
    let d = Document::from_reader(bytes)?;
    *pos += len;
    Ok(d)
}

/// Read one message. `Ok(None)` on a clean EOF between messages.
pub async fn read_request<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Request>> {
    read_request_bounded(r, MAX_MESSAGE, None, None).await
}

/// A message did not arrive in time (handshake timeout, a slow sender before authentication, or a connection that has
/// not authenticated and stayed silent too long). `first_byte`: no byte of the message had arrived.
#[derive(Debug)]
pub struct ReadTimeout {
    pub first_byte: bool,
}

impl std::fmt::Display for ReadTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "message not received in time")
    }
}

impl std::error::Error for ReadTimeout {}

/// A message larger than allowed (before authentication), refused from its header: nothing of it is allocated or read.
#[derive(Debug)]
pub struct TooLarge(pub usize, pub usize);

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "message of {} bytes is larger than the {} allowed", self.0, self.1)
    }
}

impl std::error::Error for TooLarge {}

/// Read one message of at most `max_len` bytes (also after decompression). `first_byte_by`: the first byte must arrive by
/// then; `rest_within`: once the first byte has arrived, the rest must arrive within this time.
pub async fn read_request_bounded<R: AsyncRead + Unpin>(r: &mut R, max_len: usize, first_byte_by: Option<tokio::time::Instant>, rest_within: Option<std::time::Duration>) -> Result<Option<Request>> {
    let mut header = [0u8; 16];
    let first = match first_byte_by {
        Some(d) => tokio::time::timeout_at(d, r.read(&mut header[..1])).await.map_err(|_| anyhow::Error::new(ReadTimeout { first_byte: true }))?,
        None => r.read(&mut header[..1]).await,
    };
    match first {
        Ok(0) => return Ok(None),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let rest = async move {
        r.read_exact(&mut header[1..]).await?;
        let len = i32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let request_id = i32::from_le_bytes(header[4..8].try_into().unwrap());
        let opcode = i32::from_le_bytes(header[12..16].try_into().unwrap());
        if !(16..=MAX_MESSAGE).contains(&len) {
            bail!("invalid message length {len}");
        }
        if len > max_len {
            return Err(anyhow::Error::new(TooLarge(len, max_len)));
        }
        let mut body = vec![0u8; len - 16];
        r.read_exact(&mut body).await?;
        parse_body(request_id, opcode, &body, max_len)
    };
    match rest_within {
        Some(t) => tokio::time::timeout(t, rest).await.map_err(|_| anyhow::Error::new(ReadTimeout { first_byte: false }))?,
        None => rest.await,
    }
}

fn parse_body(request_id: i32, opcode: i32, body: &[u8], max_len: usize) -> Result<Option<Request>> {
    match opcode {
        OP_MSG => parse_msg(request_id, body).map(Some),
        OP_QUERY => parse_query(request_id, body).map(Some),
        OP_COMPRESSED => {
            let mut pos = 0;
            let original = read_i32(body, &mut pos)?;
            let size = read_i32(body, &mut pos)? as usize;
            let id = *body.get(pos).ok_or_else(|| anyhow!("truncated message"))?;
            pos += 1;
            if size > MAX_MESSAGE {
                bail!("invalid uncompressed size {size}");
            }
            if size + 16 > max_len {
                return Err(anyhow::Error::new(TooLarge(size + 16, max_len)));
            }
            let plain = decompress(id, &body[pos..], size)?;
            let mut req = match original {
                OP_MSG => parse_msg(request_id, &plain)?,
                OP_QUERY => parse_query(request_id, &plain)?,
                other => bail!("unsupported compressed opcode {other}"),
            };
            req.compressor = Some(id);
            Ok(Some(req))
        }
        other => bail!("unsupported opcode {other}"),
    }
}

fn parse_msg(request_id: i32, body: &[u8]) -> Result<Request> {
    let mut pos = 0;
    let flags = read_i32(body, &mut pos)? as u32;
    let end = if flags & FLAG_CHECKSUM != 0 { body.len().saturating_sub(4) } else { body.len() };
    let body = &body[..end];
    let mut command: Option<Document> = None;
    let mut sequences: Vec<(String, Vec<Bson>)> = Vec::new();
    while pos < body.len() {
        let kind = body[pos];
        pos += 1;
        match kind {
            0 => command = Some(read_doc(body, &mut pos)?),
            1 => {
                let size = read_i32(body, &mut { pos })? as usize;
                let section_end = pos + size;
                pos += 4;
                let ident = read_cstring(body, &mut pos)?;
                let mut docs = Vec::new();
                while pos < section_end {
                    docs.push(Bson::Document(read_doc(body, &mut pos)?));
                }
                sequences.push((ident, docs));
            }
            other => bail!("unsupported OP_MSG section kind {other}"),
        }
    }
    let mut command = command.ok_or_else(|| anyhow!("OP_MSG without a body section"))?;
    let seq_keys: Vec<String> = sequences.iter().map(|(i, _)| i.clone()).collect();
    for (ident, docs) in sequences {
        command.insert(ident, Bson::Array(docs));
    }
    let db = command.get_str("$db").unwrap_or("admin").to_string();
    Ok(Request { request_id, legacy: false, db, command, more_to_come: flags & FLAG_MORE_TO_COME != 0, exhaust_allowed: flags & FLAG_EXHAUST_ALLOWED != 0, seq_keys, compressor: None })
}

fn parse_query(request_id: i32, body: &[u8]) -> Result<Request> {
    let mut pos = 0;
    let _flags = read_i32(body, &mut pos)?;
    let ns = read_cstring(body, &mut pos)?;
    let _skip = read_i32(body, &mut pos)?;
    let _n_return = read_i32(body, &mut pos)?;
    let mut command = read_doc(body, &mut pos)?;
    if let Some(Bson::Document(inner)) = command.get("$query").cloned() {
        command = inner;
    }
    let db = ns.strip_suffix(".$cmd").ok_or_else(|| anyhow!("OP_QUERY on non-command namespace {ns}"))?.to_string();
    Ok(Request { request_id, legacy: true, db, command, more_to_come: false, exhaust_allowed: false, seq_keys: Vec::new(), compressor: None })
}

fn decompress(id: u8, data: &[u8], size: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let out = match id {
        0 => data.to_vec(),
        // every decoder stops one byte past the announced size: a small message cannot expand without bound
        1 => {
            if snap::raw::decompress_len(data).map_err(|e| anyhow!("snappy: {e}"))? != size {
                bail!("uncompressed size mismatch");
            }
            snap::raw::Decoder::new().decompress_vec(data).map_err(|e| anyhow!("snappy: {e}"))?
        }
        2 => {
            let mut v = Vec::with_capacity(size);
            flate2::read::ZlibDecoder::new(data).take(size as u64 + 1).read_to_end(&mut v)?;
            v
        }
        3 => {
            let mut v = Vec::with_capacity(size);
            zstd::stream::read::Decoder::new(data)?.take(size as u64 + 1).read_to_end(&mut v)?;
            v
        }
        other => bail!("unknown compressor {other}"),
    };
    if out.len() != size {
        bail!("uncompressed size mismatch");
    }
    Ok(out)
}

fn compress(id: u8, data: &[u8]) -> Result<Vec<u8>> {
    use std::io::Write;
    Ok(match id {
        1 => snap::raw::Encoder::new().compress_vec(data).map_err(|e| anyhow!("snappy: {e}"))?,
        2 => {
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(data)?;
            e.finish()?
        }
        3 => zstd::stream::encode_all(data, 0)?,
        other => bail!("unknown compressor {other}"),
    })
}

/// The compressors this server offers, by name.
pub const COMPRESSORS: [&str; 3] = ["snappy", "zstd", "zlib"];

/// What the reply to a request needs from it, kept when the command document is handed over to the command.
pub struct ReplyTo {
    pub request_id: i32,
    pub legacy: bool,
    /// The compressor of the reply: the request's, unless its command is one whose reply is never compressed.
    pub compressor: Option<u8>,
    /// Flag the reply `moreToCome`: another reply to the same request follows without a new request (exhaust).
    pub more_to_come: bool,
}

impl Request {
    pub fn reply_to(&self) -> ReplyTo {
        ReplyTo { request_id: self.request_id, legacy: self.legacy, compressor: self.compressor.filter(|_| !self.legacy && !never_compressed(&self.command)), more_to_come: false }
    }
}

/// Commands whose reply is never compressed.
fn never_compressed(cmd: &Document) -> bool {
    matches!(cmd.keys().next().map(|k| k.as_str()), Some("hello" | "isMaster" | "ismaster" | "saslStart" | "saslContinue" | "getnonce" | "authenticate" | "createUser" | "updateUser" | "copydbSaslStart" | "copydbgetnonce" | "copydb"))
}

/// P-command-cpu: BSON of a document appended to `out` in one pass, the way the bson crate encodes it (same bytes; the
/// unit test `wire_enc_tests` compares the two on every type). Returns false, with `out` cut back to where it was, when
/// the document holds something only the bson crate's own encoder may write or refuse (a key or a regex with a NUL
/// byte, a DBPointer, the old binary subtype): the caller then uses that encoder. The bson crate's `to_vec` grows its
/// buffer from empty (each doubling a copy of everything so far) and the frame then copied it once more; replies of
/// whole-history lists are megabytes, so that was most of the reply's CPU (MEASURED, cpu-prof of the dev bench).
pub fn encode_doc_into(d: &Document, out: &mut Vec<u8>) -> bool {
    let mark = out.len();
    if enc_doc(d, out) {
        true
    } else {
        out.truncate(mark);
        false
    }
}

fn enc_doc(d: &Document, out: &mut Vec<u8>) -> bool {
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    for (k, v) in d {
        if k.as_bytes().contains(&0) {
            return false;
        }
        out.push(v.element_type() as u8);
        out.extend_from_slice(k.as_bytes());
        out.push(0);
        if !enc_value(v, out) {
            return false;
        }
    }
    out.push(0);
    let len = (out.len() - start) as i32;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    true
}

fn enc_str(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&((s.len() + 1) as i32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn enc_value(v: &Bson, out: &mut Vec<u8>) -> bool {
    match v {
        Bson::Double(x) => out.extend_from_slice(&x.to_le_bytes()),
        Bson::String(s) | Bson::JavaScriptCode(s) | Bson::Symbol(s) => enc_str(s, out),
        Bson::Document(d) => return enc_doc(d, out),
        Bson::Array(a) => {
            let start = out.len();
            out.extend_from_slice(&[0u8; 4]);
            let mut key = itoa_buf::Buf::new();
            for (i, x) in a.iter().enumerate() {
                out.push(x.element_type() as u8);
                out.extend_from_slice(key.fmt(i));
                out.push(0);
                if !enc_value(x, out) {
                    return false;
                }
            }
            out.push(0);
            let len = (out.len() - start) as i32;
            out[start..start + 4].copy_from_slice(&len.to_le_bytes());
        }
        Bson::Binary(b) => {
            if matches!(b.subtype, bson::spec::BinarySubtype::BinaryOld) {
                return false;
            }
            out.extend_from_slice(&(b.bytes.len() as i32).to_le_bytes());
            out.push(u8::from(b.subtype));
            out.extend_from_slice(&b.bytes);
        }
        Bson::ObjectId(o) => out.extend_from_slice(&o.bytes()),
        Bson::Boolean(b) => out.push(*b as u8),
        Bson::DateTime(t) => out.extend_from_slice(&t.timestamp_millis().to_le_bytes()),
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => {}
        Bson::RegularExpression(r) => {
            out.extend_from_slice(r.pattern.as_str().as_bytes());
            out.push(0);
            out.extend_from_slice(r.options.as_str().as_bytes());
            out.push(0);
        }
        Bson::JavaScriptCodeWithScope(c) => {
            let start = out.len();
            out.extend_from_slice(&[0u8; 4]);
            enc_str(&c.code, out);
            if !enc_doc(&c.scope, out) {
                return false;
            }
            let len = (out.len() - start) as i32;
            out[start..start + 4].copy_from_slice(&len.to_le_bytes());
        }
        Bson::Int32(x) => out.extend_from_slice(&x.to_le_bytes()),
        Bson::Timestamp(t) => {
            out.extend_from_slice(&t.increment.to_le_bytes());
            out.extend_from_slice(&t.time.to_le_bytes());
        }
        Bson::Int64(x) => out.extend_from_slice(&x.to_le_bytes()),
        Bson::Decimal128(x) => out.extend_from_slice(&x.bytes()),
        Bson::DbPointer(_) => return false,
    }
    true
}

/// Decimal keys of array elements without an allocation each.
mod itoa_buf {
    pub struct Buf([u8; 20]);
    impl Buf {
        pub fn new() -> Self {
            Buf([0; 20])
        }
        pub fn fmt(&mut self, mut n: usize) -> &[u8] {
            let mut i = self.0.len();
            loop {
                i -= 1;
                self.0[i] = b'0' + (n % 10) as u8;
                n /= 10;
                if n == 0 {
                    break;
                }
            }
            &self.0[i..]
        }
    }
}

/// The reply document's BSON, encoded straight after the frame's header in `out` (see [`encode_doc_into`]).
fn append_reply_doc(reply: &Document, out: &mut Vec<u8>) -> Result<()> {
    if !encode_doc_into(reply, out) {
        out.extend(reply.to_vec()?);
    }
    Ok(())
}

pub async fn write_reply<W: AsyncWrite + Unpin>(w: &mut W, req: &ReplyTo, reply: &Document, response_id: i32) -> Result<()> {
    let mut out = Vec::with_capacity(crate::query::bson_size(reply) + 64);
    if req.legacy {
        out.extend([0u8; 4]); // the length, set below
        out.extend(response_id.to_le_bytes());
        out.extend(req.request_id.to_le_bytes());
        out.extend(OP_REPLY.to_le_bytes());
        out.extend(8i32.to_le_bytes()); // responseFlags: AwaitCapable
        out.extend(0i64.to_le_bytes()); // cursorID
        out.extend(0i32.to_le_bytes()); // startingFrom
        out.extend(1i32.to_le_bytes()); // numberReturned
    } else {
        out.extend([0u8; 4]); // the length, set below
        out.extend(response_id.to_le_bytes());
        out.extend(req.request_id.to_le_bytes());
        out.extend(OP_MSG.to_le_bytes());
        out.extend((if req.more_to_come { FLAG_MORE_TO_COME } else { 0 }).to_le_bytes()); // flagBits
        out.push(0); // section kind 0
    }
    append_reply_doc(reply, &mut out)?;
    let len = out.len() as i32;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    if let Some(id) = req.compressor {
        // OP_COMPRESSED: header, original opcode, size of the plain message body, compressor, compressed body
        let body = &out[16..];
        let packed = compress(id, body)?;
        let mut c = Vec::with_capacity(packed.len() + 25);
        c.extend(((16 + 9 + packed.len()) as i32).to_le_bytes());
        c.extend(response_id.to_le_bytes());
        c.extend(req.request_id.to_le_bytes());
        c.extend(OP_COMPRESSED.to_le_bytes());
        c.extend(OP_MSG.to_le_bytes());
        c.extend((body.len() as i32).to_le_bytes());
        c.push(id);
        c.extend(packed);
        out = c;
    }
    w.write_all(&out).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod wire_enc_tests {
    use super::*;
    use bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, DateTime, Decimal128, JavaScriptCodeWithScope, Timestamp};

    /// /^a.*b$/imx, by decoding (the bson crate builds a Regex only that way or from its own CString)
    fn regex() -> Bson {
        let mut raw = vec![0u8; 4];
        raw.push(0x0B);
        raw.extend(b"r\0^a.*b$\0imx\0");
        raw.push(0);
        let n = raw.len() as i32;
        raw[0..4].copy_from_slice(&n.to_le_bytes());
        Document::from_reader(&raw[..]).unwrap().remove("r").unwrap()
    }

    fn every_type() -> Document {
        let arr: Vec<Bson> = (0..1205).map(|i| if i % 3 == 0 { Bson::Int32(i) } else { Bson::String(format!("s{i}")) }).collect();
        doc! {
            "d": 1.5f64, "nan": f64::NAN, "s": "héllo", "empty": "", "doc": { "a": { "b": [1, 2, { "c": Bson::Null }] } },
            "arr": arr, "arr_empty": [],
            "bin": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] }),
            "uuid": Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: vec![9; 16] }),
            "userbin": Bson::Binary(Binary { subtype: BinarySubtype::UserDefined(0x80), bytes: vec![] }),
            "undef": Bson::Undefined, "oid": ObjectId::from_bytes([7; 12]), "t": true, "f": false,
            "date": DateTime::from_millis(-1234567), "null": Bson::Null,
            "re": regex(),
            "js": Bson::JavaScriptCode("function(){}".into()),
            "sym": Bson::Symbol("sym".into()),
            "cws": Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope { code: "x+1".into(), scope: doc! { "x": 1, "y": [ "z" ] } }),
            "i32": -7i32, "ts": Timestamp { time: 0x01020304, increment: 0x0a0b0c0d }, "i64": i64::MIN,
            "dec": Decimal128::from_bytes([3; 16]), "min": Bson::MinKey, "max": Bson::MaxKey,
        }
    }

    /// P-command-cpu: the one-pass encoder writes exactly the bson crate's bytes, on every BSON type.
    #[test]
    fn same_bytes_as_the_bson_crate() {
        let d = every_type();
        let mut out = vec![0xAA; 3]; // appends after what is there
        assert!(encode_doc_into(&d, &mut out));
        assert_eq!(&out[..3], &[0xAA; 3]);
        assert_eq!(&out[3..], &d.to_vec().unwrap()[..]);
        let nested = doc! { "cursor": { "firstBatch": [Bson::Document(d.clone()), Bson::Document(d)], "id": 0i64, "ns": "a.b" }, "ok": 1.0 };
        let mut out = Vec::new();
        assert!(encode_doc_into(&nested, &mut out));
        assert_eq!(out, nested.to_vec().unwrap());
        let mut out = Vec::new();
        assert!(encode_doc_into(&Document::new(), &mut out));
        assert_eq!(out, Document::new().to_vec().unwrap());
    }

    /// What only the bson crate may write or refuse is left to it: `out` is cut back, and the reply path falls back.
    #[test]
    fn falls_back_on_what_it_does_not_write() {
        let mut nul_key = Document::new();
        nul_key.insert("a\0b", 1);
        let old = doc! { "o": Bson::Binary(Binary { subtype: BinarySubtype::BinaryOld, bytes: vec![1, 2] }) };
        // a DBPointer, only reachable by decoding: {p: DBPointer("ns", oid)}
        let mut raw = vec![0u8; 4];
        raw.push(0x0C);
        raw.extend(b"p\0");
        raw.extend(3i32.to_le_bytes());
        raw.extend(b"ns\0");
        raw.extend([5u8; 12]);
        raw.push(0);
        let n = raw.len() as i32;
        raw[0..4].copy_from_slice(&n.to_le_bytes());
        let dbp = Document::from_reader(&raw[..]).unwrap();
        for d in [nul_key, old, dbp] {
            let mut out = vec![1u8, 2];
            assert!(!encode_doc_into(&doc! { "x": 1, "in": Bson::Document(d.clone()) }, &mut out));
            assert_eq!(out, vec![1u8, 2]);
            // the reply path gives the bson crate's answer (bytes or error)
            let mut out = Vec::new();
            let r = append_reply_doc(&d, &mut out);
            match d.to_vec() {
                Ok(b) => assert_eq!(out, b),
                Err(_) => assert!(r.is_err()),
            }
        }
    }
}
