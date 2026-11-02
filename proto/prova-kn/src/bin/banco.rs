//! Il banco: carica gli oggetti sul finto S3 (attraverso il proxy) e misura le letture con ogni strategia.
//!
//! Strategie (per una scatola di S byte):
//! - `normale`: 1 GET dell'oggetto intero.
//! - `pezzi4`: 4 GET a intervallo in parallelo sullo stesso oggetto, si aspetta che arrivino tutte e 4 (controllo:
//!   parallelismo senza parità, per separare il guadagno della "gara" da quello del "trasferimento in parallelo").
//! - `doppia:<ms>`: 1 GET; se dopo <ms> non è finita se ne manda una seconda uguale; vince la prima che finisce, l'altra
//!   viene annullata (hedged request).
//! - `copie:<c>`: c GET uguali subito, vince la prima (richiesta doppia "a parità di GET").
//! - `kn:<k>+<m>`: n = k+m GET a intervallo sull'oggetto codificato, si usano le prime k che arrivano, le altre vengono
//!   annullate; se tra le prime k c'è una fetta di parità si decodifica.
//!
//! Ogni lettura è controllata byte per byte contro i dati attesi (rigenerati dal seme).

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::stream::{FuturesUnordered, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use prova_kn::rs;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Arg {
    #[arg(long, default_value = "http://127.0.0.1:9100")]
    endpoint: String,
    #[arg(long, default_value = "prova-kn")]
    bucket: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// scrive gli oggetti (normali e codificati) e li fa entrare nella memoria del proxy
    Carica {
        #[arg(long, value_delimiter = ',')]
        taglie: Vec<usize>,
        #[arg(long, default_value_t = 32)]
        oggetti: usize,
    },
    /// misura una strategia
    Leggi {
        #[arg(long)]
        strategia: String,
        #[arg(long)]
        taglia: usize,
        #[arg(long, default_value_t = 32)]
        oggetti: usize,
        #[arg(long)]
        letture: usize,
        #[arg(long, default_value_t = 8)]
        conc: usize,
        #[arg(long)]
        uscita: Option<String>,
        #[arg(long, default_value = "")]
        etichetta: String,
    },
}

fn store(a: &Arg) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&a.endpoint)
        .with_allow_http(true)
        .with_bucket_name(&a.bucket)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_region("us-east-1")
        .with_client_options(ClientOptions::new().with_allow_http(true).with_timeout(Duration::from_secs(60)).with_connect_timeout(Duration::from_secs(3)).with_pool_max_idle_per_host(256))
        .with_retry(RetryConfig { max_retries: 0, retry_timeout: Duration::from_secs(60), ..Default::default() })
        .build()?;
    Ok(Arc::new(s3))
}

/// I dati attesi dell'oggetto `i` di taglia `s`: pseudo-casuali dal seme, così non serve tenerli da parte.
fn dati(s: usize, i: usize) -> Vec<u8> {
    let mut r = SmallRng::seed_from_u64((s as u64) << 20 | i as u64);
    let mut v = vec![0u8; s];
    r.fill_bytes(&mut v);
    v
}

fn nome_normale(s: usize, i: usize) -> String {
    format!("normale/{s}/{i}")
}
fn nome_kn(k: usize, m: usize, s: usize, i: usize) -> String {
    format!("kn{k}+{m}/{s}/{i}")
}

const CODICI: [(usize, usize); 2] = [(4, 1), (8, 2)];

#[derive(Default)]
struct Conta {
    get: AtomicU64,
    byte_richiesti: AtomicU64,
    byte_arrivati: AtomicU64,
    decodifiche: AtomicU64,
    us_decodifica: AtomicU64,
    errori: AtomicU64,
}

async fn get(st: &dyn ObjectStore, c: &Conta, p: &str, r: Option<(u64, u64)>) -> anyhow::Result<Bytes> {
    c.get.fetch_add(1, Relaxed);
    let opts = GetOptions { range: r.map(|(a, b)| GetRange::Bounded(a..b)), ..Default::default() };
    if let Some((a, b)) = r {
        c.byte_richiesti.fetch_add(b - a, Relaxed);
    }
    let b = st.get_opts(&Path::from(p), opts).await?.bytes().await?;
    if r.is_none() {
        c.byte_richiesti.fetch_add(b.len() as u64, Relaxed);
    }
    c.byte_arrivati.fetch_add(b.len() as u64, Relaxed);
    Ok(b)
}

#[derive(Clone, Debug)]
enum Strategia {
    Normale,
    Pezzi4,
    Doppia(u64),
    Copie(usize),
    Kn(usize, usize),
}

fn strategia(s: &str) -> anyhow::Result<Strategia> {
    Ok(match s.split_once(':') {
        None if s == "normale" => Strategia::Normale,
        None if s == "pezzi4" => Strategia::Pezzi4,
        Some(("doppia", ms)) => Strategia::Doppia(ms.parse()?),
        Some(("copie", c)) => Strategia::Copie(c.parse()?),
        Some(("kn", km)) => {
            let (k, m) = km.split_once('+').ok_or_else(|| anyhow::anyhow!("kn:k+m"))?;
            Strategia::Kn(k.parse()?, m.parse()?)
        }
        _ => anyhow::bail!("strategia sconosciuta {s}"),
    })
}

async fn leggi_una(st: &dyn ObjectStore, c: &Conta, strat: &Strategia, s: usize, i: usize) -> anyhow::Result<Vec<u8>> {
    match strat {
        Strategia::Normale => Ok(get(st, c, &nome_normale(s, i), None).await?.to_vec()),
        Strategia::Pezzi4 => {
            let p = nome_normale(s, i);
            let f = s.div_ceil(4);
            let fs: Vec<_> = (0..4).map(|j| {
                let a = (j * f).min(s) as u64;
                let b = ((j + 1) * f).min(s) as u64;
                get(st, c, &p, Some((a, b)))
            }).collect();
            let parti = futures::future::try_join_all(fs).await?;
            Ok(parti.concat())
        }
        Strategia::Copie(n) => {
            let p = nome_normale(s, i);
            let mut fu: FuturesUnordered<_> = (0..*n).map(|_| get(st, c, &p, None)).collect();
            let mut ultimo = None;
            while let Some(r) = fu.next().await {
                match r {
                    Ok(b) => return Ok(b.to_vec()),
                    Err(e) => ultimo = Some(e),
                }
            }
            Err(ultimo.unwrap())
        }
        Strategia::Doppia(ms) => {
            let p = nome_normale(s, i);
            let prima = get(st, c, &p, None);
            tokio::pin!(prima);
            tokio::select! {
                r = &mut prima => return Ok(r?.to_vec()),
                _ = tokio::time::sleep(Duration::from_millis(*ms)) => {}
            }
            let seconda = get(st, c, &p, None);
            tokio::pin!(seconda);
            tokio::select! {
                r = &mut prima => Ok(r?.to_vec()),
                r = &mut seconda => Ok(r?.to_vec()),
            }
        }
        Strategia::Kn(k, m) => {
            let (k, m) = (*k, *m);
            let p = nome_kn(k, m, s, i);
            let f = rs::fetta(s, k) as u64;
            let mut fu: FuturesUnordered<_> = (0..k + m).map(|j| {
                let p = p.clone();
                async move { (j, get(st, c, &p, Some((j as u64 * f, (j as u64 + 1) * f))).await) }
            }).collect();
            let mut arrivate = Vec::with_capacity(k);
            while let Some((j, r)) = fu.next().await {
                if let Ok(b) = r {
                    arrivate.push((j, b));
                    if arrivate.len() == k {
                        break;
                    }
                }
            }
            drop(fu); // annulla le GET ancora in volo
            anyhow::ensure!(arrivate.len() == k, "meno di k fette");
            let t0 = Instant::now();
            let (d, decod) = rs::ricostruisci(&arrivate, k, m, s)?;
            if decod {
                c.decodifiche.fetch_add(1, Relaxed);
                c.us_decodifica.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
            }
            Ok(d)
        }
    }
}

fn cpu_s() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

#[derive(Serialize)]
struct Esito {
    etichetta: String,
    strategia: String,
    taglia: usize,
    letture: u64,
    conc: usize,
    errori: u64,
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
    media_ms: f64,
    get_per_lettura: f64,
    byte_richiesti_per_lettura: f64,
    byte_arrivati_per_lettura: f64,
    decodifiche: u64,
    us_per_decodifica: f64,
    cpu_us_per_lettura: f64,
    secondi: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Arg::parse();
    let st = store(&a)?;
    match &a.cmd {
        Cmd::Carica { taglie, oggetti } => {
            for &s in taglie {
                let n_ogg = if s >= 8 << 20 { (*oggetti).min(8) } else { *oggetti };
                for i in 0..n_ogg {
                    let d = dati(s, i);
                    st.put(&Path::from(nome_normale(s, i)), PutPayload::from(d.clone())).await?;
                    for (k, m) in CODICI {
                        let t0 = Instant::now();
                        let o = rs::codifica(&d, k, m)?;
                        let us = t0.elapsed().as_micros();
                        if i == 0 {
                            eprintln!("codifica {k}+{m} di {s} byte: {us} µs (prima, a freddo)");
                        }
                        st.put(&Path::from(nome_kn(k, m, s, i)), PutPayload::from(o)).await?;
                    }
                }
                eprintln!("caricati {n_ogg} oggetti da {s} byte");
            }
        }
        Cmd::Leggi { strategia: sn, taglia, oggetti, letture, conc, uscita, etichetta } => {
            let s = *taglia;
            let n_ogg = if s >= 8 << 20 { (*oggetti).min(8) } else { *oggetti };
            let strat = strategia(sn)?;
            let attesi: Arc<Vec<Vec<u8>>> = Arc::new((0..n_ogg).map(|i| dati(s, i)).collect());
            let c = Arc::new(Conta::default());
            let hist = Arc::new(parking_lot::Mutex::new(hdrhistogram::Histogram::<u64>::new_with_bounds(1, 600_000_000, 3)?));
            let prossima = Arc::new(AtomicU64::new(0));
            let tot = *letture as u64;
            let cpu0 = cpu_s();
            let t0 = Instant::now();
            let mut w = Vec::new();
            for wi in 0..*conc {
                let (st, c, hist, prossima, attesi, strat) = (st.clone(), c.clone(), hist.clone(), prossima.clone(), attesi.clone(), strat.clone());
                w.push(tokio::spawn(async move {
                    let mut r = SmallRng::seed_from_u64(wi as u64 * 7919 + 1);
                    while prossima.fetch_add(1, Relaxed) < tot {
                        let i = r.gen_range(0..attesi.len());
                        let t = Instant::now();
                        match leggi_una(&*st, &c, &strat, s, i).await {
                            Ok(d) if d == attesi[i] => {
                                let _ = hist.lock().record(t.elapsed().as_micros().max(1) as u64);
                            }
                            Ok(_) => {
                                eprintln!("DATI SBAGLIATI oggetto {i}");
                                c.errori.fetch_add(1, Relaxed);
                            }
                            Err(e) => {
                                eprintln!("errore: {e}");
                                c.errori.fetch_add(1, Relaxed);
                            }
                        }
                    }
                }));
            }
            for h in w {
                h.await?;
            }
            let secondi = t0.elapsed().as_secs_f64();
            let cpu = cpu_s() - cpu0;
            let h = hist.lock();
            let n = h.len().max(1) as f64;
            let q = |x: f64| h.value_at_quantile(x) as f64 / 1000.0;
            let e = Esito {
                etichetta: etichetta.clone(),
                strategia: sn.clone(),
                taglia: s,
                letture: h.len(),
                conc: *conc,
                errori: c.errori.load(Relaxed),
                p50_ms: q(0.5),
                p90_ms: q(0.9),
                p95_ms: q(0.95),
                p99_ms: q(0.99),
                p999_ms: q(0.999),
                max_ms: h.max() as f64 / 1000.0,
                media_ms: h.mean() / 1000.0,
                get_per_lettura: c.get.load(Relaxed) as f64 / n,
                byte_richiesti_per_lettura: c.byte_richiesti.load(Relaxed) as f64 / n,
                byte_arrivati_per_lettura: c.byte_arrivati.load(Relaxed) as f64 / n,
                decodifiche: c.decodifiche.load(Relaxed),
                us_per_decodifica: c.us_decodifica.load(Relaxed) as f64 / c.decodifiche.load(Relaxed).max(1) as f64,
                cpu_us_per_lettura: cpu * 1e6 / n,
                secondi,
            };
            let j = serde_json::to_string(&e)?;
            println!("{j}");
            if let Some(u) = uscita {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new().create(true).append(true).open(u)?;
                writeln!(f, "{j}")?;
            }
        }
    }
    Ok(())
}
