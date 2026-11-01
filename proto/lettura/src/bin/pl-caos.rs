//! Passo 1: scarico da chiunque sotto uccisioni. Avvia il registro simulato (con log di ogni record confermato) e N
//! worker sugli stessi database; per la durata data uccide con SIGKILL un worker a caso a intervalli casuali e lo
//! riavvia (in più ogni worker si fa abort() da solo con probabilità `crash` nei punti delicati). Alla fine ferma il
//! carico, aspetta che tutto sia su S3 e verifica record per record: ogni scrittura confermata deve stare in
//! esattamente una voce degli SST elencati nei manifest (zero perse, zero doppie), con tipo e crc del valore giusti.

use clap::Parser;
use lettura::iface::*;
use lettura::manifest::{self, gruppo_di, Raggruppa};
use lettura::net::RegistroRemoto;
use lettura::registro::leggi_log;
use lettura::sst;
use lettura::store::Store;
use rand::Rng;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
struct A {
    #[arg(long, default_value_t = 3600)]
    dur_s: u64,
    #[arg(long, default_value_t = 8)]
    db: u64,
    #[arg(long, default_value_t = 400.0)]
    rate: f64,
    #[arg(long, default_value_t = 3)]
    scritture: usize,
    #[arg(long, default_value_t = 100)]
    val_min: usize,
    #[arg(long, default_value_t = 1500)]
    val_max: usize,
    #[arg(long, default_value_t = 5000)]
    chiavi: u64,
    #[arg(long, default_value_t = 0.05)]
    altri_tipi: f64,
    #[arg(long, default_value_t = 3)]
    worker: usize,
    #[arg(long, default_value_t = 2.0)]
    uccidi_min_s: f64,
    #[arg(long, default_value_t = 15.0)]
    uccidi_max_s: f64,
    #[arg(long, default_value_t = 0.002)]
    crash: f64,
    #[arg(long, default_value_t = 800)]
    eta_ms: u64,
    #[arg(long, default_value_t = 0)]
    gruppi: u64,
    #[arg(long, default_value = "caos")]
    bucket: String,
    #[arg(long, default_value = "127.0.0.1:7701")]
    addr: String,
    #[arg(long)]
    out: PathBuf,
    /// controllo negativo: worker senza PUT condizionale del manifest
    #[arg(long)]
    senza_cas: bool,
    /// solo la verifica (registro ancora vivo e log già scritto)
    #[arg(long)]
    solo_verifica: bool,
}

fn bin(n: &str) -> PathBuf {
    std::env::current_exe().unwrap().parent().unwrap().join(n)
}

fn avvia_worker(a: &A, i: usize, gen: u64) -> Child {
    let out = std::fs::OpenOptions::new().create(true).append(true).open(a.out.join(format!("w{i}.out"))).unwrap();
    let err = out.try_clone().unwrap();
    Command::new(bin("pl-worker"))
        .args(["--addr", &a.addr, "--bucket", &a.bucket, "--gruppi", &a.gruppi.to_string(), "--eta-ms", &a.eta_ms.to_string()])
        .args(["--dim", &(1u64 << 20).to_string(), "--tieni-tutte", "--crash", &a.crash.to_string()])
        .args(["--grazia-ms", "20000", "--orfani-ogni-ms", "60000", "--orfani-eta-ms", "30000", "--compatta-k", "4", "--max-run", "10"])
        .args(["--riposo-ms", "30000", "--stat-ms", "1000", "--paralleli", "4", "--intervallo-ms", "50"])
        .args(["--stat-file", a.out.join(format!("w{i}-{gen}.json")).to_str().unwrap(), "--nome", &format!("w{i}-{gen}")])
        .args(if a.senza_cas { vec!["--senza-cas"] } else { vec![] })
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap()
}

fn uccidi(c: &mut Child) {
    unsafe {
        libc::kill(c.id() as i32, libc::SIGKILL);
    }
    let _ = c.wait();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = A::parse();
    std::fs::create_dir_all(&a.out)?;
    let log = a.out.join("registro.log");
    let mut reg_child = None;
    let reg = RegistroRemoto::new(&a.addr, 2);
    let t_inizio = Instant::now();
    let mut uccisioni = 0u64;
    let mut crash_visti = 0u64;
    if !a.solo_verifica {
        let out = std::fs::File::create(a.out.join("registro.out"))?;
        reg_child = Some(
            Command::new(bin("pl-registro"))
                .args(["--addr", &a.addr, "--log", log.to_str().unwrap(), "--db", &a.db.to_string(), "--rate", &a.rate.to_string()])
                .args(["--scritture", &a.scritture.to_string(), "--val-min", &a.val_min.to_string(), "--val-max", &a.val_max.to_string()])
                .args(["--chiavi", &a.chiavi.to_string(), "--altri-tipi", &a.altri_tipi.to_string(), "--stat-ms", "10000"])
                .stdout(Stdio::from(out))
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        for _ in 0..100 {
            if reg.ctl("stat").await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let mut gen = 0u64;
        let mut ws: Vec<Child> = (0..a.worker).map(|i| avvia_worker(&a, i, { gen += 1; gen })).collect();
        let mut rng = rand::thread_rng();
        let fine = Instant::now() + Duration::from_secs(a.dur_s);
        let mut prossima = Instant::now() + Duration::from_secs_f64(rng.gen_range(a.uccidi_min_s..a.uccidi_max_s));
        let mut ultimo_rapporto = Instant::now();
        while Instant::now() < fine {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for i in 0..ws.len() {
                if let Ok(Some(_)) = ws[i].try_wait() {
                    crash_visti += 1;
                    gen += 1;
                    ws[i] = avvia_worker(&a, i, gen);
                }
            }
            if Instant::now() >= prossima {
                let i = rng.gen_range(0..ws.len());
                uccidi(&mut ws[i]);
                uccisioni += 1;
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..1000))).await;
                gen += 1;
                ws[i] = avvia_worker(&a, i, gen);
                prossima = Instant::now() + Duration::from_secs_f64(rng.gen_range(a.uccidi_min_s..a.uccidi_max_s));
            }
            if ultimo_rapporto.elapsed() >= Duration::from_secs(60) {
                ultimo_rapporto = Instant::now();
                let s = reg.stat().await?;
                println!("{}", json!({"t_s": t_inizio.elapsed().as_secs(), "uccisioni": uccisioni, "crash": crash_visti, "reg": s}));
            }
        }
        // fine del carico: si svuota il registro (i worker possono ancora fare abort() da soli)
        reg.ctl("ferma_carico").await?;
        let t_drain = Instant::now();
        let mut zeri = 0;
        while t_drain.elapsed() < Duration::from_secs(600) {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for i in 0..ws.len() {
                if let Ok(Some(_)) = ws[i].try_wait() {
                    crash_visti += 1;
                    gen += 1;
                    ws[i] = avvia_worker(&a, i, gen);
                }
            }
            let s = reg.stat().await?;
            zeri = if s.pend_rec == 0 { zeri + 1 } else { 0 };
            if zeri >= 6 {
                break;
            }
        }
        println!("svuotato in {:.1} s", t_drain.elapsed().as_secs_f64());
        tokio::time::sleep(Duration::from_secs(5)).await;
        for w in ws.iter_mut() {
            uccidi(w);
        }
        reg.ctl("flush_log").await?;
    }
    let v = verifica(&a, &log, &reg).await?;
    let mut tot = BTreeMap::new();
    for f in std::fs::read_dir(&a.out)? {
        let p = f?.path();
        let n = p.file_name().unwrap().to_string_lossy().to_string();
        if n.starts_with('w') && n.ends_with(".json") {
            if let Ok(s) = std::fs::read_to_string(&p) {
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(&s) {
                    for sez in ["stat", "s3"] {
                        if let Some(o) = j[sez].as_object() {
                            for (k, x) in o {
                                *tot.entry(format!("{sez}.{k}")).or_insert(0u64) += x.as_u64().unwrap_or(0);
                            }
                        }
                    }
                }
            }
        }
    }
    let r = json!({
        "durata_s": a.dur_s, "uccisioni_sigkill": uccisioni, "abort_da_soli": crash_visti, "verifica": v,
        "worker_somma_ultimo_stato": tot, "parametri": format!("{a:?}"),
        "minio": lettura::store::metriche_minio(&a.bucket).await.unwrap_or_default(),
    });
    std::fs::write(a.out.join("risultato.json"), serde_json::to_string_pretty(&r)?)?;
    println!("{}", serde_json::to_string_pretty(&r)?);
    if let Some(mut c) = reg_child {
        uccidi(&mut c);
    }
    Ok(())
}

async fn verifica(a: &A, log: &std::path::Path, reg: &RegistroRemoto) -> anyhow::Result<serde_json::Value> {
    let t0 = Instant::now();
    let rows = leggi_log(log)?;
    let mut attese: HashMap<(Db, Pos, Vec<u8>), (u8, u32, u32)> = HashMap::with_capacity(rows.len() * 2);
    let mut max_db: HashMap<Db, Pos> = HashMap::new();
    let mut nrec = 0u64;
    for (db, pos, ws) in rows {
        nrec += 1;
        let m = max_db.entry(db).or_insert(0);
        *m = (*m).max(pos);
        for (k, kind, crc) in ws {
            attese.insert((db, pos, k), (kind, crc, 0));
        }
    }
    let raggruppa = if a.gruppi == 0 { Raggruppa::PerDb } else { Raggruppa::Gruppi(a.gruppi) };
    let st = Store::minio(&a.bucket)?;
    let gruppi: HashSet<String> = max_db.keys().map(|d| gruppo_di(d, raggruppa)).collect();
    let (mut trovate, mut duplicate, mut estranee, mut corrotte, mut run_mancanti, mut nrun, mut byte) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let mut manifest_indietro = Vec::new();
    let mut referenziati: HashSet<String> = HashSet::new();
    let mut da_cancellare = 0u64;
    for g in &gruppi {
        let Some((m, _)) = manifest::carica(&st, g).await? else {
            manifest_indietro.push(format!("{g}: manifest assente"));
            continue;
        };
        for d in &m.da_cancellare {
            referenziati.insert(manifest::path_sst(g, &d.id));
            da_cancellare += 1;
        }
        for r in &m.run {
            nrun += 1;
            let p = manifest::path_sst(g, &r.id);
            referenziati.insert(p.clone());
            let Some((b, _)) = st.get(&p).await? else {
                run_mancanti += 1;
                continue;
            };
            byte += b.len() as u64;
            for e in sst::all_entries(&b)? {
                let db = sst::key_db(&e.key);
                let k = (db, e.pos, e.key[16..].to_vec());
                match attese.get_mut(&k) {
                    None => estranee += 1,
                    Some(x) => {
                        x.2 += 1;
                        if x.2 == 1 {
                            trovate += 1;
                        } else {
                            duplicate += 1;
                        }
                        if x.0 != e.kind as u8 || x.1 != crc32fast::hash(&e.val) {
                            corrotte += 1;
                        }
                    }
                }
            }
        }
        for (dh, p) in &m.scaricato {
            let db = db_from_hex(dh).unwrap();
            let atteso = max_db.get(&db).copied().unwrap_or(0);
            let reg_s = reg.stato_scarico(db).await?.scaricato;
            if *p != atteso || reg_s != atteso {
                manifest_indietro.push(format!("{dh}: manifest {p} registro {reg_s} atteso {atteso}"));
            }
        }
    }
    let perse = attese.values().filter(|x| x.2 == 0).count() as u64;
    let tutti = st.list("").await?;
    let orfani = tutti.iter().filter(|o| o.location.as_ref().ends_with(".sst") && !referenziati.contains(o.location.as_ref())).count();
    Ok(json!({
        "record_confermati": nrec, "scritture_attese": attese.len(), "trovate": trovate, "perse": perse,
        "duplicate": duplicate, "estranee": estranee, "corrotte": corrotte, "run_mancanti": run_mancanti,
        "run": nrun, "byte_sst": byte, "manifest_o_registro_non_allineati": manifest_indietro,
        "oggetti_nel_bucket": tutti.len(), "sst_orfani_rimasti": orfani, "sst_in_attesa_di_cancellazione": da_cancellare,
        "secondi_verifica": t0.elapsed().as_secs_f64(),
        "esito": if perse == 0 && duplicate == 0 && estranee == 0 && corrotte == 0 && run_mancanti == 0 && manifest_indietro.is_empty() { "OK" } else { "FALLITO" },
    }))
}
