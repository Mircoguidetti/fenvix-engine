//! Proxy TCP che può ritardare e scartare il traffico tra i processi. Ripreso dal proxy di journal3z
//! (`journal3z/p2/net/src/bin/j3z-proxy.rs`, branch journal3z) con l'aggiunta di `loss_pct`.
//!
//! - `routes.json`: `[{"name": "r0->r1", "listen": "127.0.0.1:7301", "target": "127.0.0.1:7101"}, ...]`
//! - `ctl.json` (riletto ogni 50 ms; assente o illeggibile = tutto passa):
//!   `{"r0->r1": {"mode": "blackhole"}, "c->r2": {"mode": "pass", "delay_ms": 1}}`
//!   modi: `pass`; `blackhole` (scarta ogni byte nei due versi, la connessione resta aperta: partizione silenziosa);
//!   `cut` (chiude le connessioni e rifiuta le nuove); `drop_c2s` / `drop_s2c` (scarta un verso solo).
//!   `delay_ms` aggiunge un ritardo a ogni pezzo in entrambi i versi; `loss_pct` scarta a caso quella percentuale di
//!   pezzi (il flusso si corrompe, il CRC se ne accorge e la connessione si chiude e si riapre).
//! Scrivere `ctl.json` in modo atomico (file temporaneo + rename).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Route {
    pub name: String,
    pub listen: String,
    pub target: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Pass,
    Blackhole,
    Cut,
    DropC2s,
    DropS2c,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct RouteCtl {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub delay_ms: u64,
    #[serde(default)]
    pub loss_pct: f64,
}

type Ctl = Arc<parking_lot::RwLock<HashMap<String, RouteCtl>>>;

fn ctl_of(ctl: &Ctl, name: &str) -> RouteCtl {
    ctl.read().get(name).cloned().unwrap_or_default()
}

async fn pump(mut from: tokio::net::tcp::OwnedReadHalf, mut to: tokio::net::tcp::OwnedWriteHalf, ctl: Ctl, name: String, c2s: bool) {
    let mut buf = vec![0u8; 256 << 10];
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
                if drop || (c.loss_pct > 0.0 && rand::random::<f64>() * 100.0 < c.loss_pct) { continue; }
                if c.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(c.delay_ms)).await;
                }
                if to.write_all(&buf[..n]).await.is_err() { return; }
            }
            _ = tick.tick() => {
                if ctl_of(&ctl, &name).mode == Mode::Cut { return; }
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
            tokio::select! { _ = a => {}, _ = b => {} }
        });
    }
}

pub async fn run(routes: Vec<Route>, ctl_path: String) {
    let ctl: Ctl = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    {
        let ctl = ctl.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                tick.tick().await;
                let m: HashMap<String, RouteCtl> = std::fs::read_to_string(&ctl_path).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                *ctl.write() = m;
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

// ================================================================================================ log-on-pair: delay line
/// log-on-pair: a TCP proxy with a fixed, sub-millisecond one-way delay per route (a delay LINE: every chunk is sent
/// `delay_us` after it was read, chunks in flight do not wait for each other, so the delay does not limit throughput)
/// and byte counters per route and direction. Blocking OS threads (two per connection) sleeping with a 1 ns timer
/// slack, because the tokio timer of `run` has a 1 ms resolution and this kernel has no tc netem.
/// `routes.json`: `[{"name": "r0->r1", "listen": "...", "target": "...", "delay_us": 500}, ...]` (no `delay_us` =
/// `--delay-us`). Counters are written to `stats` every 200 ms: `{"r0->r1": [bytes_c2s, bytes_s2c, conns], ...}`.
pub fn run_delay(routes: Vec<DelayRoute>, default_delay_us: u64, stats_path: String) {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    unsafe {
        libc::prctl(libc::PR_SET_TIMERSLACK, 1, 0, 0, 0);
    }
    let counters: Vec<(String, Arc<[AtomicU64; 3]>)> =
        routes.iter().map(|r| (r.name.clone(), Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]))).collect();
    for (r, (_, cnt)) in routes.iter().zip(counters.iter()) {
        let l = std::net::TcpListener::bind(&r.listen).unwrap_or_else(|e| panic!("bind {}: {e}", r.listen));
        let (target, d, cnt) = (r.target.clone(), Duration::from_micros(r.delay_us.unwrap_or(default_delay_us)), cnt.clone());
        std::thread::spawn(move || {
            for c in l.incoming() {
                let Ok(client) = c else { continue };
                let Ok(server) = std::net::TcpStream::connect(&target) else { continue };
                let _ = client.set_nodelay(true);
                let _ = server.set_nodelay(true);
                cnt[2].fetch_add(1, Relaxed);
                for (from, to, k) in [(client.try_clone().unwrap(), server.try_clone().unwrap(), 0usize), (server, client, 1usize)] {
                    let cnt = cnt.clone();
                    let (tx, rx) = crossbeam_channel::unbounded::<(Instant, Vec<u8>)>();
                    let mut from = from;
                    let shut = to.try_clone().unwrap();
                    std::thread::spawn(move || {
                        unsafe {
                            libc::prctl(libc::PR_SET_TIMERSLACK, 1, 0, 0, 0);
                        }
                        let mut buf = vec![0u8; 256 << 10];
                        loop {
                            match from.read(&mut buf) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    cnt[k].fetch_add(n as u64, Relaxed);
                                    if tx.send((Instant::now() + d, buf[..n].to_vec())).is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        let _ = from.shutdown(std::net::Shutdown::Both);
                    });
                    let mut to = to;
                    std::thread::spawn(move || {
                        unsafe {
                            libc::prctl(libc::PR_SET_TIMERSLACK, 1, 0, 0, 0);
                        }
                        while let Ok((at, b)) = rx.recv() {
                            let now = Instant::now();
                            if at > now {
                                std::thread::sleep(at - now);
                            }
                            if to.write_all(&b).is_err() {
                                break;
                            }
                        }
                        let _ = shut.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
    }
    println!("READY");
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let m: serde_json::Map<String, serde_json::Value> =
            counters.iter().map(|(n, c)| (n.clone(), serde_json::json!([c[0].load(Relaxed), c[1].load(Relaxed), c[2].load(Relaxed)]))).collect();
        let tmp = format!("{stats_path}.tmp");
        if std::fs::write(&tmp, serde_json::Value::Object(m).to_string()).is_ok() {
            let _ = std::fs::rename(&tmp, &stats_path);
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DelayRoute {
    pub name: String,
    pub listen: String,
    pub target: String,
    #[serde(default)]
    pub delay_us: Option<u64>,
}
