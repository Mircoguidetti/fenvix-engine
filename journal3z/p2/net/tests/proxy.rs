//! The fault proxy verified alone: each mode does what it says. Twin: the same checks against a proxy that ignores
//! its modes (test-only switch J3Z_PROXY_IGNORE_MODES) must fail.
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Proxy {
    child: std::process::Child,
    ctl: std::path::PathBuf,
    _dir: tempfile::TempDir,
    port: u16,
}

impl Proxy {
    fn start(target: u16, ignore_modes: bool) -> Proxy {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let routes = dir.path().join("routes.json");
        std::fs::write(&routes, format!(r#"[{{"name":"r","listen":"127.0.0.1:{port}","target":"127.0.0.1:{target}"}}]"#)).unwrap();
        let ctl = dir.path().join("ctl.json");
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_j3z-proxy"));
        cmd.args(["--routes", routes.to_str().unwrap(), "--ctl", ctl.to_str().unwrap()]).stdout(std::process::Stdio::piped());
        if ignore_modes {
            cmd.env("J3Z_PROXY_IGNORE_MODES", "1");
        }
        let mut child = cmd.spawn().unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "READY");
        Proxy { child, ctl, _dir: dir, port }
    }
    fn set(&self, json: &str) {
        let tmp = self.ctl.with_extension("tmp");
        std::fs::write(&tmp, json).unwrap();
        std::fs::rename(&tmp, &self.ctl).unwrap();
        std::thread::sleep(Duration::from_millis(150));
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

/// Echo server that counts the bytes it receives.
async fn echo(received: Arc<AtomicU64>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            let received = received.clone();
            tokio::spawn(async move {
                let mut b = [0u8; 1024];
                loop {
                    let n = match s.read(&mut b).await { Ok(0) | Err(_) => return, Ok(n) => n };
                    received.fetch_add(n as u64, SeqCst);
                    if s.write_all(&b[..n]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

async fn roundtrip(s: &mut TcpStream, wait: Duration) -> Option<Duration> {
    let t = Instant::now();
    s.write_all(b"ping").await.ok()?;
    let mut b = [0u8; 4];
    match tokio::time::timeout(wait, s.read_exact(&mut b)).await {
        Ok(Ok(_)) => Some(t.elapsed()),
        _ => None,
    }
}

/// Every mode, checked from the outside. Returns the list of checks that failed.
async fn scenario(ignore_modes: bool) -> Vec<&'static str> {
    let received = Arc::new(AtomicU64::new(0));
    let target = echo(received.clone()).await;
    let p = Proxy::start(target, ignore_modes);
    let mut bad = vec![];
    let mut s = TcpStream::connect(("127.0.0.1", p.port)).await.unwrap();
    if roundtrip(&mut s, Duration::from_secs(1)).await.is_none() {
        bad.push("pass: no echo");
    }
    p.set(r#"{"r":{"mode":"blackhole"}}"#);
    let before = received.load(SeqCst);
    if roundtrip(&mut s, Duration::from_millis(400)).await.is_some() || received.load(SeqCst) != before {
        bad.push("blackhole: traffic went through");
    }
    p.set(r#"{"r":{"mode":"pass"}}"#);
    if roundtrip(&mut s, Duration::from_secs(1)).await.is_none() {
        bad.push("blackhole -> pass: the same connection does not work again");
    }
    p.set(r#"{"r":{"mode":"drop_c2s"}}"#);
    let before = received.load(SeqCst);
    let r = roundtrip(&mut s, Duration::from_millis(400)).await;
    if r.is_some() || received.load(SeqCst) != before {
        bad.push("drop_c2s: the server received data");
    }
    p.set(r#"{"r":{"mode":"drop_s2c"}}"#);
    let mut s2 = TcpStream::connect(("127.0.0.1", p.port)).await.unwrap();
    let before = received.load(SeqCst);
    let r = roundtrip(&mut s2, Duration::from_millis(400)).await;
    if r.is_some() || received.load(SeqCst) == before {
        bad.push("drop_s2c: the client got an answer, or the server got nothing");
    }
    p.set(r#"{"r":{"mode":"pass","delay_ms":200}}"#);
    let mut s3 = TcpStream::connect(("127.0.0.1", p.port)).await.unwrap();
    match roundtrip(&mut s3, Duration::from_secs(2)).await {
        Some(d) if d >= Duration::from_millis(390) => {}
        _ => bad.push("delay: round trip shorter than 2 x 200 ms"),
    }
    p.set(r#"{"r":{"mode":"cut"}}"#);
    let mut b = [0u8; 4];
    let closed = matches!(tokio::time::timeout(Duration::from_millis(500), s3.read(&mut b)).await, Ok(Ok(0)) | Ok(Err(_)));
    if !closed {
        bad.push("cut: the open connection was not closed");
    }
    let refused = match TcpStream::connect(("127.0.0.1", p.port)).await {
        Err(_) => true,
        Ok(mut s4) => matches!(tokio::time::timeout(Duration::from_millis(500), s4.read(&mut b)).await, Ok(Ok(0)) | Ok(Err(_))),
    };
    if !refused {
        bad.push("cut: a new connection was accepted and kept");
    }
    bad
}

#[tokio::test]
async fn proxy_modes() {
    let bad = scenario(false).await;
    assert!(bad.is_empty(), "{bad:?}");
}

#[tokio::test]
async fn proxy_modes_twin_ignoring_modes_is_caught() {
    let bad = scenario(true).await;
    println!("twin (proxy ignores modes): {} checks fail: {bad:?}", bad.len());
    assert!(bad.len() >= 5, "a proxy that ignores its modes must fail almost every check, failed only {bad:?}");
}
