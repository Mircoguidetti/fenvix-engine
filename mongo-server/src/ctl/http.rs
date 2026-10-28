//! A small HTTP/1.1 server for the admin interface: one request per connection, bearer-token authentication
//! before anything else is read, JSON bodies up to 1 MiB, and chunked streaming responses with `Range: bytes=N-`
//! support for exports. No TLS: terminate it in front (see results-ops.md).

use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 1 << 20;

pub struct Request {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

pub enum Body {
    Bytes(Vec<u8>),
    /// Streamed with chunked transfer encoding (the length is not known in advance).
    Stream(mpsc::Receiver<io::Result<Bytes>>),
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Body,
}

impl Response {
    pub fn json(status: u16, v: &serde_json::Value) -> Response {
        Response { status, headers: vec![("Content-Type".into(), "application/json".into())], body: Body::Bytes(serde_json::to_vec(v).unwrap()) }
    }

    pub fn error(status: u16, code: &str, message: impl Into<String>) -> Response {
        Self::json(status, &serde_json::json!({ "error": { "code": code, "message": message.into() } }))
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        206 => "Partial Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

/// Equal-length-independent constant-time comparison of two secrets.
pub fn token_matches(given: &[u8], expected: &[u8]) -> bool {
    let mut diff = (given.len() ^ expected.len()) as u8;
    for i in 0..expected.len().max(given.len()) {
        let a = given.get(i).copied().unwrap_or(0);
        let b = expected.get(i).copied().unwrap_or(0);
        diff |= a ^ b;
    }
    diff == 0
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Some(v) = s.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn read_request(r: &mut BufReader<tokio::net::tcp::OwnedReadHalf>, token: &str) -> Result<Request, Response> {
    let mut head = Vec::new();
    loop {
        let mut line = Vec::new();
        let n = r.read_until(b'\n', &mut line).await.map_err(|_| Response::error(400, "bad_request", "read failed"))?;
        if n == 0 {
            return Err(Response::error(400, "bad_request", "connection closed"));
        }
        head.extend_from_slice(&line);
        if head.len() > MAX_HEAD {
            return Err(Response::error(400, "bad_request", "header too large"));
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
    }
    let text = String::from_utf8_lossy(&head).into_owned();
    let mut lines = text.lines();
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let (method, target) = match (parts.next(), parts.next()) {
        (Some(m), Some(t)) => (m.to_string(), t.to_string()),
        _ => return Err(Response::error(400, "bad_request", "malformed request line")),
    };
    let mut headers = HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    // Authenticate before looking at the body or the route: an unauthenticated caller learns nothing.
    let given = headers.get("authorization").and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    if !token_matches(given.as_bytes(), token.as_bytes()) {
        let mut resp = Response::error(401, "unauthorized", "missing or invalid bearer token");
        resp.headers.push(("WWW-Authenticate".into(), "Bearer".into()));
        return Err(resp);
    }
    let (path, qs) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.clone(), String::new()),
    };
    let mut query = HashMap::new();
    for kv in qs.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        query.insert(pct_decode(k), pct_decode(v));
    }
    let len: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    if len > MAX_BODY {
        return Err(Response::error(413, "too_large", "body larger than 1 MiB"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await.map_err(|_| Response::error(400, "bad_request", "short body"))?;
    Ok(Request { method, path: pct_decode(&path), query, headers, body })
}

async fn write_response(w: &mut tokio::net::tcp::OwnedWriteHalf, resp: Response) -> io::Result<()> {
    let mut head = format!("HTTP/1.1 {} {}\r\nConnection: close\r\n", resp.status, reason(resp.status));
    for (k, v) in &resp.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    match resp.body {
        Body::Bytes(b) => {
            head.push_str(&format!("Content-Length: {}\r\n\r\n", b.len()));
            w.write_all(head.as_bytes()).await?;
            w.write_all(&b).await?;
        }
        Body::Stream(mut rx) => {
            head.push_str("Transfer-Encoding: chunked\r\n\r\n");
            w.write_all(head.as_bytes()).await?;
            while let Some(chunk) = rx.recv().await {
                match chunk {
                    Ok(c) if c.is_empty() => {}
                    Ok(c) => {
                        w.write_all(format!("{:x}\r\n", c.len()).as_bytes()).await?;
                        w.write_all(&c).await?;
                        w.write_all(b"\r\n").await?;
                    }
                    // A producer error: close without the terminating chunk so the client sees a truncated
                    // (resumable) transfer and not a complete one.
                    Err(e) => return Err(e),
                }
            }
            w.write_all(b"0\r\n\r\n").await?;
        }
    }
    w.flush().await
}

pub type Handler = Arc<dyn Fn(Request) -> futures::future::BoxFuture<'static, Response> + Send + Sync>;

pub async fn serve(listener: TcpListener, token: String, handler: Handler) {
    let token = Arc::new(token);
    loop {
        let Ok((stream, peer)) = listener.accept().await else { continue };
        let (token, handler) = (Arc::clone(&token), Arc::clone(&handler));
        tokio::spawn(async move {
            handle(stream, peer.to_string(), token, handler).await;
        });
    }
}

async fn handle(stream: TcpStream, peer: String, token: Arc<String>, handler: Handler) {
    let _ = stream.set_nodelay(true);
    let (r, mut w) = stream.into_split();
    let mut r = BufReader::new(r);
    let t0 = Instant::now();
    let (line, resp) = match tokio::time::timeout(std::time::Duration::from_secs(30), read_request(&mut r, &token)).await {
        Err(_) => ("(timeout)".to_string(), Response::error(400, "bad_request", "request timed out")),
        Ok(Err(resp)) => ("(rejected)".to_string(), resp),
        Ok(Ok(req)) => {
            let line = format!("{} {}", req.method, req.path);
            (line, handler(req).await)
        }
    };
    let status = resp.status;
    let res = write_response(&mut w, resp).await;
    eprintln!("ADMIN {line} -> {status} {} ms peer={peer}{}", t0.elapsed().as_millis(), if res.is_err() { " (write failed)" } else { "" });
}
