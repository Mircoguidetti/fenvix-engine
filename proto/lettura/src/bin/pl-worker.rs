//! Worker di scarico e compattazione, collegato al registro via TCP. Stampa una riga JSON di statistiche ogni
//! `stat_ms` (anche su file con --stat-file, riscritto a ogni riga: serve al banco di uccisione).

use clap::Parser;
use lettura::manifest::Raggruppa;
use lettura::net::RegistroRemoto;
use lettura::scarico::{CfgScarico, Worker};
use lettura::store::Store;

#[derive(Parser, Debug)]
struct A {
    #[arg(long, default_value = "127.0.0.1:7700")]
    addr: String,
    #[arg(long)]
    bucket: String,
    /// 0 = un manifest per database; n = n gruppi condivisi
    #[arg(long, default_value_t = 0)]
    gruppi: u64,
    #[arg(long, default_value_t = 1000)]
    eta_ms: u64,
    #[arg(long, default_value_t = 8 << 20)]
    dim: u64,
    #[arg(long, default_value_t = 16 << 20)]
    max_batch: u64,
    #[arg(long, default_value_t = 16 << 10)]
    blocco: usize,
    #[arg(long, default_value_t = 100)]
    intervallo_ms: u64,
    #[arg(long, default_value_t = 8)]
    paralleli: usize,
    #[arg(long)]
    no_compatta: bool,
    #[arg(long, default_value_t = 4)]
    compatta_k: usize,
    #[arg(long, default_value_t = 30)]
    max_run: usize,
    #[arg(long, default_value_t = 0)]
    riposo_ms: u64,
    #[arg(long)]
    tieni_tutte: bool,
    #[arg(long, default_value_t = 120_000)]
    orizzonte_ms: u64,
    #[arg(long, default_value_t = 60_000)]
    grazia_ms: u64,
    #[arg(long, default_value_t = 600_000)]
    orfani_ogni_ms: u64,
    #[arg(long, default_value_t = 300_000)]
    orfani_eta_ms: u64,
    #[arg(long, default_value_t = 0.0)]
    crash: f64,
    #[arg(long, default_value_t = 256)]
    cache_mb: u64,
    #[arg(long, default_value_t = 5000)]
    stat_ms: u64,
    #[arg(long)]
    stat_file: Option<std::path::PathBuf>,
    #[arg(long, default_value = "w")]
    nome: String,
    /// SOLO controllo negativo: manifest senza PUT condizionale
    #[arg(long)]
    senza_cas: bool,
    #[arg(long, default_value_t = 0)]
    riscrivi_max: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    let cfg = CfgScarico {
        raggruppa: if a.gruppi == 0 { Raggruppa::PerDb } else { Raggruppa::Gruppi(a.gruppi) },
        eta_ms: a.eta_ms,
        dim_byte: a.dim,
        max_batch: a.max_batch,
        blocco: a.blocco,
        intervallo_ms: a.intervallo_ms,
        paralleli: a.paralleli,
        compatta: !a.no_compatta,
        compatta_k: a.compatta_k,
        max_run: a.max_run,
        riposo_ms: a.riposo_ms,
        tieni_tutte: a.tieni_tutte,
        orizzonte_ms: a.orizzonte_ms,
        grazia_ms: a.grazia_ms,
        orfani_ogni_ms: a.orfani_ogni_ms,
        orfani_eta_ms: a.orfani_eta_ms,
        crash: a.crash,
        cache_byte: a.cache_mb << 20,
        senza_cas: a.senza_cas,
        riscrivi_max: a.riscrivi_max,
    };
    let reg = RegistroRemoto::new(&a.addr, 4);
    let st = Store::minio(&a.bucket)?;
    let w = Worker::new(reg, st.clone(), cfg);
    tokio::spawn(w.clone().corri());
    let t0 = std::time::Instant::now();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(a.stat_ms)).await;
        let l = serde_json::json!({
            "t_s": t0.elapsed().as_secs_f64(), "nome": a.nome, "pid": std::process::id(),
            "stat": w.stat.leggi(), "s3": st.c.leggi(), "cpu_s": lettura::util::cpu_s(0), "rss": lettura::util::rss(0),
            "ms_scarico": w.stat.ns_scarico.load(std::sync::atomic::Ordering::Relaxed) / 1_000_000,
            "ms_compatta": w.stat.ns_compatta.load(std::sync::atomic::Ordering::Relaxed) / 1_000_000,
        })
        .to_string();
        println!("{l}");
        if let Some(f) = &a.stat_file {
            let tmp = f.with_extension("tmp");
            if std::fs::write(&tmp, &l).is_ok() {
                let _ = std::fs::rename(&tmp, f);
            }
        }
    }
}
