//! Cliente Rust (driver ufficiale `mongodb`) per la prova M14. Stesso carico dei clienti Python e Node.
//! Uso: client-rust URI SECONDI CONCORRENZA OUT.json
use mongodb::bson::{doc, oid::ObjectId, Document};
use mongodb::event::command::CommandEvent;
use mongodb::event::sdam::SdamEvent;
use mongodb::event::EventHandler;
use mongodb::options::ClientOptions;
use mongodb::Client;
use rand::Rng;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct St {
    per_server: BTreeMap<String, u64>,
    per_sec: BTreeMap<u64, BTreeMap<String, u64>>,
    cmd_fail: BTreeMap<String, u64>,
    app_errors: Vec<Value>,
    topo: Option<String>,
    servers: Vec<String>,
    history: Vec<Value>,
    ops: u64,
    hb_ok: u64,
    hb_fail: BTreeMap<String, u64>,
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (uri, secs, conc, out) = (a[1].clone(), a[2].parse::<f64>().unwrap(), a[3].parse::<usize>().unwrap(), a[4].clone());
    let t0 = Instant::now();
    let st = Arc::new(Mutex::new(St::default()));
    let mut opts = ClientOptions::parse(&uri).await.expect("uri");
    let s1 = st.clone();
    opts.command_event_handler = Some(EventHandler::callback(move |ev: CommandEvent| {
        let mut s = s1.lock().unwrap();
        match ev {
            CommandEvent::Succeeded(e) if e.command_name == "find" || e.command_name == "insert" => {
                let k = e.connection.address.to_string();
                *s.per_server.entry(k.clone()).or_default() += 1;
                *s.per_sec.entry(t0.elapsed().as_secs()).or_default().entry(k).or_default() += 1;
            }
            CommandEvent::Failed(e) => *s.cmd_fail.entry(e.connection.address.to_string()).or_default() += 1,
            _ => {}
        }
    }));
    let s2 = st.clone();
    opts.sdam_event_handler = Some(EventHandler::callback(move |ev: SdamEvent| {
        if let SdamEvent::ServerHeartbeatSucceeded(_) = ev {
            s2.lock().unwrap().hb_ok += 1;
            return;
        }
        if let SdamEvent::ServerHeartbeatFailed(e) = &ev {
            let k: String = e.failure.to_string().chars().take(100).collect();
            *s2.lock().unwrap().hb_fail.entry(k).or_default() += 1;
            return;
        }
        if let SdamEvent::TopologyDescriptionChanged(e) = ev {
            let mut s = s2.lock().unwrap();
            let ty = format!("{:?}", e.new_description.topology_type());
            let mut servers: Vec<String> = e.new_description.servers().iter().map(|(a, d)| format!("{a}={:?}", d.server_type())).collect();
            servers.sort();
            s.history.push(json!([t0.elapsed().as_secs_f64(), ty, servers]));
            if ty != "Unknown" {
                s.topo = Some(ty);
            }
            s.servers = servers;
        }
    }));
    let c = Client::with_options(opts).unwrap();
    let coll = c.database("prova").collection::<Document>("c");
    let stop = Instant::now() + Duration::from_secs_f64(secs);
    let mut hs = vec![];
    for _ in 0..conc {
        let coll = coll.clone();
        let st = st.clone();
        hs.push(tokio::spawn(async move {
            while Instant::now() < stop {
                let is_read = rand::thread_rng().gen_bool(0.8);
                let r: Result<(), mongodb::error::Error> = if is_read {
                    let id = rand::thread_rng().gen_range(0..1000i32);
                    coll.find_one(doc! {"_id": id}).await.map(|_| ())
                } else {
                    coll.insert_one(doc! {"_id": ObjectId::new(), "v": rand::thread_rng().gen::<f64>()}).await.map(|_| ())
                };
                match r {
                    Ok(()) => st.lock().unwrap().ops += 1,
                    Err(e) => {
                        let msg: String = format!("{}{}", if is_read { "find: " } else { "insert: " }, e.to_string().chars().take(200).collect::<String>());
                        st.lock().unwrap().app_errors.push(json!([t0.elapsed().as_secs_f64(), format!("{:?}", e.kind).chars().take(40).collect::<String>(), msg]));
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    c.shutdown().await;
    let s = st.lock().unwrap();
    let per_sec: Map<String, Value> = s.per_sec.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
    let hist: Vec<Value> = s.history.iter().rev().take(8).rev().cloned().collect();
    let res = json!({"driver": "rust", "version": "3", "ops": s.ops, "per_server": s.per_server, "per_sec": per_sec, "cmd_fail": s.cmd_fail, "heartbeats": {"ok": s.hb_ok, "fail": s.hb_fail},
        "app_errors": s.app_errors, "topology": s.topo, "servers": s.servers, "topo_history": hist});
    std::fs::write(&out, serde_json::to_string_pretty(&res).unwrap()).unwrap();
    println!("{}", json!({"ops": s.ops, "per_server": s.per_server, "app_errors": s.app_errors.len(), "topology": s.topo}));
}
