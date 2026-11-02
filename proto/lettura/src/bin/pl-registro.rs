//! Registro simulato servito via TCP, con un generatore di commit interno (al posto dei nodi d'ingresso).
//! Comandi di controllo (Req::Ctl): "stat", "ferma_carico", "riprendi_carico", "flush_log".

use bytes::Bytes;
use clap::Parser;
use lettura::carico::{valore, Zipf, COLL};
use lettura::iface::*;
use lettura::net;
use lettura::registro::RegistroSim;
use rand::{Rng, RngCore, SeedableRng};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Parser, Debug)]
struct A {
    #[arg(long, default_value = "127.0.0.1:7700")]
    addr: String,
    #[arg(long, default_value_t = 60_000)]
    finestra_ms: u64,
    /// log di verifica (ogni record confermato)
    #[arg(long)]
    log: Option<std::path::PathBuf>,
    /// database del carico
    #[arg(long, default_value_t = 0)]
    db: u64,
    /// commit al secondo in tutto
    #[arg(long, default_value_t = 0.0)]
    rate: f64,
    /// scritture per record: da 1 a questo
    #[arg(long, default_value_t = 1)]
    scritture: usize,
    #[arg(long, default_value_t = 1200)]
    val_min: usize,
    #[arg(long, default_value_t = 1200)]
    val_max: usize,
    /// documenti distinti per database
    #[arg(long, default_value_t = 1_000_000)]
    chiavi: u64,
    #[arg(long, default_value_t = 0.0)]
    zipf: f64,
    /// frazione di cancellazioni e di delta (ciascuna)
    #[arg(long, default_value_t = 0.0)]
    altri_tipi: f64,
    #[arg(long, default_value_t = 5000)]
    stat_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let reg = RegistroSim::new(a.finestra_ms, a.log.as_deref())?;
    reg.avvia_potatura();
    let attivo = Arc::new(AtomicBool::new(a.rate > 0.0));
    {
        let reg_c = reg.clone();
        let attivo = attivo.clone();
        let ctl: net::CtlFn = Arc::new(move |c: &str| match c {
            "ferma_carico" => {
                attivo.store(false, Ordering::SeqCst);
                "ok".into()
            }
            "riprendi_carico" => {
                attivo.store(true, Ordering::SeqCst);
                "ok".into()
            }
            "flush_log" => {
                reg_c.flush_log();
                "ok".into()
            }
            _ => "comando sconosciuto".into(),
        });
        let addr = a.addr.clone();
        let reg = reg.clone();
        tokio::spawn(async move {
            if let Err(e) = net::serve(&addr, reg, ctl).await {
                eprintln!("serve: {e:#}");
                std::process::exit(1);
            }
        });
    }
    if a.rate > 0.0 && a.db > 0 {
        let reg = reg.clone();
        let attivo = attivo.clone();
        let a2 = A::parse();
        std::thread::spawn(move || {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            let z = Zipf::new(a2.chiavi, a2.zipf);
            let t0 = std::time::Instant::now();
            let mut fatti: u64 = 0;
            let mut base_t = 0.0f64;
            let mut pausa = false;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(2));
                if !attivo.load(Ordering::Relaxed) {
                    pausa = true;
                    continue;
                }
                let el = t0.elapsed().as_secs_f64();
                if pausa {
                    base_t = el - fatti as f64 / a2.rate;
                    pausa = false;
                }
                let obiettivo = ((el - base_t) * a2.rate) as u64;
                while fatti < obiettivo {
                    let d = rng.gen_range(0..a2.db);
                    let db = db_n(d);
                    let n = rng.gen_range(1..=a2.scritture.max(1));
                    let mut ws: Vec<Write> = Vec::with_capacity(n);
                    for _ in 0..n {
                        let key = doc_key(COLL, z.id(&mut rng, d));
                        if ws.iter().any(|w| w.key == key) {
                            continue;
                        }
                        let u: f64 = rng.gen();
                        let w = if u < a2.altri_tipi {
                            Write { key, kind: Kind::Del, val: Bytes::new() }
                        } else if u < 2.0 * a2.altri_tipi {
                            Write { key, kind: Kind::Delta, val: Bytes::copy_from_slice(&rng.gen_range(-100i64..100).to_le_bytes()) }
                        } else {
                            let len = rng.gen_range(a2.val_min..=a2.val_max.max(a2.val_min));
                            Write { key, kind: Kind::Put, val: valore(&mut rng, len) }
                        };
                        ws.push(w);
                    }
                    let mut txn = [0u8; 16];
                    rng.fill_bytes(&mut txn);
                    reg.commit_sync(CommitReq { txn, db, read_pos: 0, mode: Mode::Blind, writes: ws, reads: vec![], session: None });
                    fatti += 1;
                }
            }
        });
    }
    let t0 = std::time::Instant::now();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(a.stat_ms)).await;
        let s = reg.stat();
        println!("{}", serde_json::json!({"t_s": t0.elapsed().as_secs_f64(), "reg": s, "cpu_s": lettura::util::cpu_s(0), "rss": lettura::util::rss(0)}));
    }
}
