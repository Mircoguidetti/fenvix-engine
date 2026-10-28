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

pub struct Request {
    pub request_id: i32,
    pub legacy: bool,
    /// `$db` for OP_MSG, the database part of `db.$cmd` for OP_QUERY.
    pub db: String,
    pub command: Document,
    /// The sender does not want a reply (unacknowledged write).
    pub more_to_come: bool,
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
    let mut header = [0u8; 16];
    match r.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = i32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let request_id = i32::from_le_bytes(header[4..8].try_into().unwrap());
    let opcode = i32::from_le_bytes(header[12..16].try_into().unwrap());
    if !(16..=MAX_MESSAGE).contains(&len) {
        bail!("invalid message length {len}");
    }
    let mut body = vec![0u8; len - 16];
    r.read_exact(&mut body).await?;
    match opcode {
        OP_MSG => parse_msg(request_id, &body).map(Some),
        OP_QUERY => parse_query(request_id, &body).map(Some),
        OP_COMPRESSED => {
            let mut pos = 0;
            let original = read_i32(&body, &mut pos)?;
            let size = read_i32(&body, &mut pos)? as usize;
            let id = *body.get(pos).ok_or_else(|| anyhow!("truncated message"))?;
            pos += 1;
            if size > MAX_MESSAGE {
                bail!("invalid uncompressed size {size}");
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
    Ok(Request { request_id, legacy: false, db, command, more_to_come: flags & FLAG_MORE_TO_COME != 0, seq_keys, compressor: None })
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
    Ok(Request { request_id, legacy: true, db, command, more_to_come: false, seq_keys: Vec::new(), compressor: None })
}

fn decompress(id: u8, data: &[u8], size: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let out = match id {
        0 => data.to_vec(),
        1 => snap::raw::Decoder::new().decompress_vec(data).map_err(|e| anyhow!("snappy: {e}"))?,
        2 => {
            let mut v = Vec::with_capacity(size);
            flate2::read::ZlibDecoder::new(data).read_to_end(&mut v)?;
            v
        }
        3 => zstd::stream::decode_all(data)?,
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

/// Commands whose reply is never compressed.
fn never_compressed(cmd: &Document) -> bool {
    matches!(cmd.keys().next().map(|k| k.as_str()), Some("hello" | "isMaster" | "ismaster" | "saslStart" | "saslContinue" | "getnonce" | "authenticate" | "createUser" | "updateUser" | "copydbSaslStart" | "copydbgetnonce" | "copydb"))
}

pub async fn write_reply<W: AsyncWrite + Unpin>(w: &mut W, req: &Request, reply: &Document, response_id: i32) -> Result<()> {
    let doc = reply.to_vec()?;
    let mut out = Vec::with_capacity(doc.len() + 36);
    if req.legacy {
        let len = 16 + 4 + 8 + 4 + 4 + doc.len();
        out.extend((len as i32).to_le_bytes());
        out.extend(response_id.to_le_bytes());
        out.extend(req.request_id.to_le_bytes());
        out.extend(OP_REPLY.to_le_bytes());
        out.extend(8i32.to_le_bytes()); // responseFlags: AwaitCapable
        out.extend(0i64.to_le_bytes()); // cursorID
        out.extend(0i32.to_le_bytes()); // startingFrom
        out.extend(1i32.to_le_bytes()); // numberReturned
    } else {
        let len = 16 + 4 + 1 + doc.len();
        out.extend((len as i32).to_le_bytes());
        out.extend(response_id.to_le_bytes());
        out.extend(req.request_id.to_le_bytes());
        out.extend(OP_MSG.to_le_bytes());
        out.extend(0u32.to_le_bytes()); // flagBits
        out.push(0); // section kind 0
    }
    out.extend(doc);
    if let (Some(id), false, false) = (req.compressor, req.legacy, never_compressed(&req.command)) {
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
