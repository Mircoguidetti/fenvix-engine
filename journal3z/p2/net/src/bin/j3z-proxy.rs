//! Fault-injecting TCP proxy for the local test bench (phase-2 plan P4).
//!
//! Usage: `j3z-proxy --routes routes.json --ctl ctl.json`
//! - `routes.json`: `[{"name": "w0-n1", "listen": "127.0.0.1:7101", "target": "127.0.0.1:7001"}, ...]`
//! - `ctl.json` (re-read every 50 ms; absent or unreadable = everything passes):
//!   `{"w0-n1": {"mode": "blackhole"}, "w1-n2": {"mode": "drop_s2c", "delay_ms": 5}}`
//!   modes: `pass`, `blackhole` (drop every byte both ways, keep the connection), `cut` (close connections, refuse new
//!   ones), `drop_c2s` (writer→node bytes dropped), `drop_s2c` (node→writer bytes dropped); `delay_ms` adds a one-way
//!   delay to every chunk.
//! Prints `READY` when every route listens. Write `ctl.json` atomically (temporary file + rename): a half-written
//! file reads as "everything passes".
use j3z_net::{Mode, Route, RouteCtl};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Ctl = Arc<RwLock<HashMap<String, RouteCtl>>>;

fn ctl_of(ctl: &Ctl, name: &str) -> RouteCtl {
    #[cfg(feature = "testbugs")]
    if std::env::var("J3Z_PROXY_IGNORE_MODES").is_ok() {
        return RouteCtl::default();
    }
    ctl.read().unwrap().get(name).cloned().unwrap_or_default()
}

async fn pump(mut from: tokio::net::tcp::OwnedReadHalf, mut to: tokio::net::tcp::OwnedWriteHalf, ctl: Ctl, name: String, c2s: bool) {
    let mut buf = vec![0u8; 64 << 10];
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            n = from.read(&mut buf) => {
                let n = match n { Ok(0) | Err(_) => return, Ok(n) => n };
                let c = ctl_of(&ctl, &name);
                let drop = match c.mode {
                    Mode::Cut => return,
                    Mode::Blackhole => true,
                    Mode::DropC2s => c2s,
                    Mode::DropS2c => !c2s,
                    Mode::Pass => false,
                };
                if drop {
                    continue;
                }
                if c.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(c.delay_ms)).await;
                }
                if to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
            _ = tick.tick() => {
                if ctl_of(&ctl, &name).mode == Mode::Cut {
                    return;
                }
            }
        }
    }
}

async fn serve(route: Route, l: TcpListener, ctl: Ctl) {
    loop {
        let Ok((client, _)) = l.accept().await else { continue };
        if ctl_of(&ctl, &route.name).mode == Mode::Cut {
            drop(client);
            continue;
        }
        let (ctl, name, target) = (ctl.clone(), route.name.clone(), route.target.clone());
        tokio::spawn(async move {
            let Ok(server) = TcpStream::connect(&target).await else { return };
            let _ = client.set_nodelay(true);
            let _ = server.set_nodelay(true);
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            let a = tokio::spawn(pump(cr, sw, ctl.clone(), name.clone(), true));
            let b = tokio::spawn(pump(sr, cw, ctl, name, false));
            // when one direction ends (cut, close), end the other: the connection is gone
            tokio::select! { _ = a => {}, _ = b => {} }
        });
    }
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let arg = |k: &str| a.iter().position(|x| x == k).and_then(|i| a.get(i + 1)).cloned().unwrap_or_else(|| panic!("missing {k}"));
    let routes: Vec<Route> = serde_json::from_str(&std::fs::read_to_string(arg("--routes")).unwrap()).unwrap();
    let ctl_path = arg("--ctl");
    let ctl: Ctl = Arc::new(RwLock::new(HashMap::new()));
    {
        let ctl = ctl.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                tick.tick().await;
                let m: HashMap<String, RouteCtl> = std::fs::read_to_string(&ctl_path).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                *ctl.write().unwrap() = m;
            }
        });
    }
    for r in routes {
        let l = TcpListener::bind(&r.listen).await.unwrap_or_else(|e| panic!("bind {}: {e}", r.listen));
        tokio::spawn(serve(r, l, ctl.clone()));
    }
    println!("READY");
    std::future::pending::<()>().await;
}
