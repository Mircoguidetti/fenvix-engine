//! journal3z writer process for the local bench (phase-2 plan P5): the engine stand-in + the 3-zone journal client.
//!
//! `j3z-writer --holder <name> --index <i> --nwriters <n> --me <u32> --nodes <a1,a2,a3> --dbs <n>
//!             --client-listen <addr> --s3-endpoint http://127.0.0.1:<port> --bucket <b> --s3-key <k> --s3-secret <s>
//!             [--config <json file>] [--faults-file <file>] [--mutation <name>]`
//!
//! - The object store must be on this machine (loopback endpoint): the bench never talks to a real cloud.
//! - Client protocol on `--client-listen`: one JSON object per line, answers in any order, matched by `id`:
//!   `{"op":"write","db":1,"id":7,"size":100}` → `{"id":7,"ok":true,"idx":12,"epoch":3}` or `{"id":7,"ok":false,"err":"not_owner"}`
//!   `{"op":"owned","id":0}` → `{"id":0,"owned":[[db,epoch],...]}`; `{"op":"flush","id":0,"db":1}` → `{"id":0,"applied":k}`.
//! - `--faults-file` (feature `faults`): JSON re-read every 50 ms, `{"clock_offset_ms": n, "clock_rate_ppm": r,
//!   "mono_jump_ms": j}`: the wall clock jumps by n (the lease does not use it), the monotonic clock runs at 1 + r/1e6
//!   and has jumped forward by j in total (only grows).
//! - `--mutation` (feature `mutations`): a protocol bug from `Mutations::ALL` or a takeover-rule bug from
//!   `LeaseBug::ALL` (negative controls).
//! Prints `READY` on stdout once the client port listens; then the event lines of `standin`.

use j3z_writer::journal::{JournalCfg, ThreeZone};
use j3z_writer::meta::Meta;
use j3z_writer::standin::{Clock, Server, ServerCfg};
use journal3z_core::lease::LeaseBug;
use journal3z_core::Mutations;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

fn die(code: i32, why: &str) -> ! {
    eprintln!("j3z-writer: exit {code}: {why}");
    std::process::exit(code);
}

/// A protocol mutation, or one of the 3 negative controls of the takeover rule (`lease_*`).
fn mutations(name: Option<String>) -> (Mutations, LeaseBug) {
    match name {
        None => (Mutations::default(), LeaseBug::None),
        #[cfg(feature = "mutations")]
        Some(n) => match LeaseBug::parse(&n) {
            Some(b) if n != "none" => (Mutations::default(), b),
            _ => (Mutations::parse(&n).unwrap_or_else(|| die(2, &format!("unknown mutation {n}"))), LeaseBug::None),
        },
        #[cfg(not(feature = "mutations"))]
        Some(_) => die(2, "--mutation needs a build with the `mutations` feature"),
    }
}

#[allow(unused_variables)]
fn clock(faults_file: Option<String>) -> Clock {
    let c = Clock::default();
    #[cfg(feature = "faults")]
    if let Some(path) = faults_file {
        let c2 = c.clone();
        std::thread::spawn(move || loop {
            if let Some(v) = std::fs::read_to_string(&path).ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()) {
                c2.set_wall_offset_ms(v["clock_offset_ms"].as_i64().unwrap_or(0));
                c2.set_rate_ppm(v["clock_rate_ppm"].as_i64().unwrap_or(0));
                c2.set_mono_jump_ms(v["mono_jump_ms"].as_u64().unwrap_or(0));
            }
            std::thread::sleep(Duration::from_millis(50));
        });
        return c;
    }
    #[cfg(not(feature = "faults"))]
    if faults_file.is_some() {
        die(2, "--faults-file needs a build with the `faults` feature");
    }
    c
}

pub fn loopback(endpoint: &str) -> bool {
    let rest = endpoint.strip_prefix("http://").unwrap_or("");
    rest.starts_with("127.0.0.1:") || rest.starts_with("localhost:") || rest.starts_with("[::1]:")
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let opt = |k: &str| a.iter().position(|x| x == k).and_then(|i| a.get(i + 1)).cloned();
    let req = |k: &str| opt(k).unwrap_or_else(|| die(2, &format!("{k} is required")));
    let num = |k: &str| req(k).parse::<u64>().unwrap_or_else(|_| die(2, &format!("bad {k}")));
    let (m, lease_bug) = mutations(opt("--mutation"));
    let clock = clock(opt("--faults-file"));
    let cfg: ServerCfg = match opt("--config") {
        Some(p) => serde_json::from_str(&std::fs::read_to_string(&p).unwrap_or_else(|e| die(2, &format!("{p}: {e}")))).unwrap_or_else(|e| die(2, &format!("{p}: {e}"))),
        None => ServerCfg::default(),
    };
    let endpoint = req("--s3-endpoint");
    if !loopback(&endpoint) {
        die(2, "the object store must be a local one (http://127.0.0.1:<port>): the bench never uses a cloud store");
    }
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_region("us-east-1")
        .with_bucket_name(req("--bucket"))
        .with_access_key_id(req("--s3-key"))
        .with_secret_access_key(req("--s3-secret"))
        // a hung request must not hold the lease loop past the lease: short timeouts, few retries
        .with_client_options(object_store::ClientOptions::new().with_allow_http(true).with_timeout(Duration::from_secs(2)).with_connect_timeout(Duration::from_secs(1)))
        .with_retry(object_store::RetryConfig { max_retries: 2, retry_timeout: Duration::from_secs(4), ..Default::default() })
        .build()
        .unwrap_or_else(|e| die(2, &format!("object store: {e}")));
    let meta = Meta::new(Arc::new(s3));
    let nodes: Vec<String> = req("--nodes").split(',').map(|s| s.to_string()).collect();
    let holder = req("--holder");
    let jcfg = JournalCfg { tick: Duration::from_millis(cfg.tick_ms), open_timeout: Duration::from_millis(cfg.open_timeout_ms), hard_cap_bytes: cfg.hard_cap_bytes };
    let journal = ThreeZone::new(&holder, num("--me") as u32, &nodes, jcfg, m);
    let server = Server::new(&holder, num("--index") as usize, num("--nwriters") as usize, journal, meta, cfg, m, lease_bug, clock, num("--dbs"));
    server.start();
    let listen = req("--client-listen");
    let l = TcpListener::bind(&listen).await.unwrap_or_else(|e| die(2, &format!("bind {listen}: {e}")));
    println!("READY holder={holder} listen={listen}");
    loop {
        let Ok((s, _)) = l.accept().await else { continue };
        let _ = s.set_nodelay(true);
        let server = server.clone();
        tokio::spawn(async move {
            let (rd, mut wr) = s.into_split();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            tokio::spawn(async move {
                while let Some(first) = rx.recv().await {
                    let mut out = first;
                    out.push('\n');
                    while let Ok(more) = rx.try_recv() {
                        out.push_str(&more);
                        out.push('\n');
                    }
                    if wr.write_all(out.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
            let mut lines = BufReader::new(rd).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
                let (server, tx) = (server.clone(), tx.clone());
                tokio::spawn(async move {
                    let id = v["id"].as_u64().unwrap_or(0);
                    let r = match v["op"].as_str().unwrap_or("") {
                        "write" => match server.write(v["db"].as_u64().unwrap_or(0), id, v["size"].as_u64().unwrap_or(8) as usize).await {
                            Ok((idx, epoch)) => serde_json::json!({"id": id, "ok": true, "idx": idx, "epoch": epoch}),
                            Err(e) => serde_json::json!({"id": id, "ok": false, "err": e}),
                        },
                        "owned" => serde_json::json!({"id": id, "owned": server.owned()}),
                        "flush" => serde_json::json!({"id": id, "applied": server.flush(v["db"].as_u64().unwrap_or(0)).await}),
                        _ => serde_json::json!({"id": id, "ok": false, "err": "bad_request"}),
                    };
                    let _ = tx.send(r.to_string());
                });
            }
        });
    }
}
