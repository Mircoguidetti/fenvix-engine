//! s3delay: a plain TCP proxy for one HTTP/1.1 upstream (MinIO). Every time the client has sent bytes (a request, or the
//! body of a PUT) and the first bytes of the answer come back, those first bytes are held for `delay_ms` before they
//! are forwarded; the rest of the answer flows at once. So each request gets `delay_ms` added to its time to first byte,
//! whatever its size: an approximation of S3's first-byte latency, which a local MinIO does not have.
//! Why not `tc netem`: the sandbox kernel has no sch_netem ("Specified qdisc kind is unknown"); netem would also delay
//! every packet (TCP ACKs of uploads included), which is not what S3 does.
//!
//!   s3delay LISTEN UPSTREAM DELAY_MS        e.g. s3delay 0.0.0.0:9000 127.0.0.1:9002 20
//!
//! Every 10 s it prints the requests delayed so far (stderr), so a run can prove the delay was in the path.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static DELAYED: AtomicU64 = AtomicU64::new(0);
static CONNS: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 4 {
        eprintln!("usage: s3delay LISTEN UPSTREAM DELAY_MS");
        std::process::exit(2);
    }
    let (listen, upstream) = (a[1].clone(), a[2].clone());
    let delay = Duration::from_millis(a[3].parse().expect("DELAY_MS"));
    let l = TcpListener::bind(&listen).await.expect("bind");
    eprintln!("s3delay: {listen} -> {upstream}, {} ms added to the first byte of every response", delay.as_millis());
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            eprintln!("s3delay: connections {} responses delayed {}", CONNS.load(Ordering::Relaxed), DELAYED.load(Ordering::Relaxed));
        }
    });
    loop {
        let (c, _) = match l.accept().await {
            Ok(x) => x,
            Err(e) => {
                eprintln!("accept: {e}");
                continue;
            }
        };
        let up = upstream.clone();
        tokio::spawn(async move {
            let u = match TcpStream::connect(&up).await {
                Ok(u) => u,
                Err(e) => {
                    eprintln!("connect {up}: {e}");
                    return;
                }
            };
            CONNS.fetch_add(1, Ordering::Relaxed);
            let _ = c.set_nodelay(true);
            let _ = u.set_nodelay(true);
            let (mut cr, mut cw) = c.into_split();
            let (mut ur, mut uw) = u.into_split();
            let waiting = Arc::new(AtomicBool::new(false));
            let w1 = waiting.clone();
            let up_task = tokio::spawn(async move {
                let mut b = vec![0u8; 64 * 1024];
                loop {
                    let n = match cr.read(&mut b).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    w1.store(true, Ordering::SeqCst);
                    if uw.write_all(&b[..n]).await.is_err() {
                        break;
                    }
                }
                let _ = uw.shutdown().await;
            });
            let mut b = vec![0u8; 64 * 1024];
            loop {
                let n = match ur.read(&mut b).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if waiting.swap(false, Ordering::SeqCst) {
                    DELAYED.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(delay).await;
                }
                if cw.write_all(&b[..n]).await.is_err() {
                    break;
                }
            }
            let _ = cw.shutdown().await;
            up_task.abort();
        });
    }
}
