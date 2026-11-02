//! Banco della cucitura.
//!
//! `cu-prova prova`: genera N scatole, le lava a cucitura (in uno o due modi) e riscrivendo tutto, controlla che
//! le uscite dicano le stesse cose e misura le letture puntuali su tutte e due.
//! `cu-prova giri`: molti giri di cucitura uno dopo l'altro (frammentazione), con letture a intervalli.

use bytes::Bytes;
use clap::{Parser, Subcommand};
use cucitura::carico::{Canc, Gen, Id};
use cucitura::cucitura::{cuci, riscrivi, Cfg, Esito, Modo};
use cucitura::formato::{self, Meta};
use cucitura::s3::S3;
use lettura::iface::Kind;
use serde::Serialize;
use std::time::Instant;

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    endpoint: String,
    #[arg(long, default_value = "cucitura")]
    bucket: String,
    #[arg(long)]
    out: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Prova {
        #[arg(long, default_value_t = 8)]
        scatole: usize,
        #[arg(long, default_value_t = 128)]
        mb: usize,
        #[arg(long, value_enum, default_value = "oid")]
        id: Id,
        #[arg(long, value_enum, default_value = "uniforme")]
        canc: Canc,
        #[arg(long, value_delimiter = ',', default_value = "coda,ordine")]
        modi: Vec<Modo>,
        #[arg(long, default_value_t = 16)]
        blocco_kb: usize,
        #[arg(long, default_value_t = 200_000)]
        letture: usize,
        #[arg(long, default_value_t = 7)]
        seme: u64,
        /// finestra dei documenti "recenti" (ultimi N inseriti) per aggiornamenti e cancellazioni recenti
        #[arg(long, default_value_t = 50_000)]
        recenti: usize,
        #[arg(long, default_value_t = 0.7)]
        vivo_min: f64,
    },
    Giri {
        #[arg(long, default_value_t = 8)]
        scatole: usize,
        #[arg(long, default_value_t = 32)]
        mb: usize,
        #[arg(long, default_value_t = 32)]
        mb_giro: usize,
        #[arg(long, default_value_t = 30)]
        giri: usize,
        #[arg(long, default_value_t = 10)]
        ogni: usize,
        #[arg(long, value_enum, default_value = "oid")]
        id: Id,
        #[arg(long, value_enum, default_value = "uniforme")]
        canc: Canc,
        #[arg(long, default_value_t = 0.7)]
        vivo_min: f64,
        #[arg(long, default_value_t = 16)]
        blocco_kb: usize,
        #[arg(long, default_value_t = 100_000)]
        letture: usize,
        #[arg(long, default_value_t = 11)]
        seme: u64,
        #[arg(long, default_value_t = 50_000)]
        recenti: usize,
    },
}

#[derive(Serialize, Clone, Debug)]
struct Letture {
    n: usize,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    media_ns: f64,
    blocchi_aperti: u64,
}

fn leggi(obj: &Bytes, m: &Meta, keys: &[(Bytes, bool)]) -> anyhow::Result<Letture> {
    let mut h = hdrhistogram::Histogram::<u64>::new(3)?;
    let mut aperti = 0u64;
    // riscaldamento
    for (k, _) in keys.iter().take(keys.len() / 10) {
        std::hint::black_box(formato::cerca(obj, m, k)?);
    }
    for (k, _) in keys {
        let t = Instant::now();
        let (v, a) = formato::cerca(obj, m, k)?;
        let ns = t.elapsed().as_nanos() as u64;
        std::hint::black_box(v);
        aperti += a as u64;
        h.record(ns.max(1))?;
    }
    Ok(Letture { n: keys.len(), p50_ns: h.value_at_quantile(0.5), p99_ns: h.value_at_quantile(0.99), p999_ns: h.value_at_quantile(0.999), media_ns: h.mean(), blocchi_aperti: aperti })
}

#[derive(Serialize, Debug, Default)]
struct Verifica {
    documenti_vivi: u64,
    uguali: bool,
    cancellazioni_rimaste: u64,
    versioni_vecchie_rimaste: u64,
    letture_giuste: u64,
    letture_sbagliate: u64,
}

/// Confronta il contenuto logico di due uscite (documenti vivi, ultima versione) e le letture delle chiavi.
fn verifica(a: &Bytes, ma: &Meta, b: &Bytes, mb: &Meta, keys: &[(Bytes, bool)]) -> anyhow::Result<Verifica> {
    let logico = |obj: &Bytes, m: &Meta| -> anyhow::Result<(Vec<lettura::sst::Entry>, u64, u64)> {
        let es = formato::tutte_le_voci(obj, m)?;
        let mut out = Vec::with_capacity(es.len());
        let (mut del, mut vecchie) = (0, 0);
        let mut prev: Option<Bytes> = None;
        for e in es {
            if prev.as_ref() == Some(&e.key) {
                vecchie += 1;
                continue;
            }
            prev = Some(e.key.clone());
            if e.kind == Kind::Del {
                del += 1;
                continue;
            }
            out.push(e);
        }
        Ok((out, del, vecchie))
    };
    let (la, da, va) = logico(a, ma)?;
    let (lb, _, _) = logico(b, mb)?;
    let mut v = Verifica { documenti_vivi: la.len() as u64, uguali: la == lb, cancellazioni_rimaste: da, versioni_vecchie_rimaste: va, ..Default::default() };
    for (k, vivo) in keys {
        let (x, _) = formato::cerca(a, ma, k)?;
        let (y, _) = formato::cerca(b, mb, k)?;
        if x == y && x.is_some() == *vivo {
            v.letture_giuste += 1;
        } else {
            v.letture_sbagliate += 1;
        }
    }
    Ok(v)
}

#[derive(Serialize)]
struct UscitaProva {
    scatole: usize,
    mb: usize,
    id: Id,
    canc: Canc,
    blocco_kb: usize,
    recenti: usize,
    vivo_min: f64,
    operazioni: u64,
    ms_generazione: u128,
    byte_scatole: u64,
    riscrittura: Esito,
    cuciture: Vec<(Modo, Esito, Verifica, u64)>,
    letture_riscritta: Vec<Letture>,
    letture_cucite: Vec<(Modo, Vec<Letture>)>,
}

async fn genera(s3: &S3, g: &mut Gen, n: usize, mb: usize, blocco: usize, pref: &str) -> anyhow::Result<(Vec<String>, u64)> {
    let mut chiavi = Vec::new();
    let mut tot = 0;
    for i in 0..n {
        let es = g.scatola(mb << 20);
        let obj = Bytes::from(formato::costruisci(es.iter(), blocco));
        tot += obj.len() as u64;
        let k = format!("{pref}/{i:03}.sst");
        s3.put_grande(&k, obj).await?;
        chiavi.push(k);
    }
    Ok((chiavi, tot))
}

fn scrivi<T: Serialize>(path: &str, x: &T) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_string_pretty(x)?)?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let s3 = S3::new(&cli.endpoint, &cli.bucket).await?;
    match cli.cmd {
        Cmd::Prova { scatole, mb, id, canc, modi, blocco_kb, letture, seme, recenti, vivo_min } => {
            let blocco = blocco_kb << 10;
            let pref = format!("prova-{:?}-{:?}-{}", id, canc, seme).to_lowercase();
            let mut g = Gen::new(id, canc, seme);
            g.recenti = recenti;
            let t = Instant::now();
            let (ingressi, byte_scatole) = genera(&s3, &mut g, scatole, mb, blocco, &format!("{pref}/in")).await?;
            let ms_generazione = t.elapsed().as_millis();
            eprintln!("generate {} scatole, {} MB, {} operazioni, {} ms", scatole, byte_scatole >> 20, g.ops, ms_generazione);
            let keys = g.chiavi_lettura(letture);
            let cfg = Cfg { block_size: blocco, ..Default::default() };
            let k_risc = format!("{pref}/riscritta.sst");
            let risc = riscrivi(&s3, &ingressi, &k_risc, &cfg).await?;
            eprintln!("riscrittura: {:?}", risc);
            let obj_r = s3.get_all(&k_risc).await?;
            let m_r = Meta::from_object(&obj_r)?;
            let mut cuciture = Vec::new();
            let mut letture_cucite = Vec::new();
            let mut letture_riscritta = Vec::new();
            for modo in modi {
                let cfg = Cfg { modo, block_size: blocco, vivo_min, ..Default::default() };
                let k = format!("{pref}/cucita-{:?}.sst", modo).to_lowercase();
                let es = cuci(&s3, &ingressi, &k, &cfg).await?;
                eprintln!("cucitura {:?}: {:?}", modo, es);
                let obj = s3.get_all(&k).await?;
                let m = Meta::from_object(&obj)?;
                let v = verifica(&obj, &m, &obj_r, &m_r, &keys)?;
                eprintln!("verifica: {:?}", v);
                anyhow::ensure!(v.uguali && v.letture_sbagliate == 0, "le uscite non dicono le stesse cose");
                let mut ls = Vec::new();
                for _ in 0..5 {
                    ls.push(leggi(&obj, &m, &keys)?);
                    letture_riscritta.push(leggi(&obj_r, &m_r, &keys)?);
                }
                eprintln!("letture cucita {:?}: {:?}", modo, ls);
                cuciture.push((modo, es, v, m.byte_morti()));
                letture_cucite.push((modo, ls));
                s3.delete(&k).await?;
            }
            eprintln!("letture riscritta: {:?}", letture_riscritta);
            s3.delete(&k_risc).await?;
            for k in &ingressi {
                s3.delete(k).await?;
            }
            let u = UscitaProva { scatole, mb, id, canc, blocco_kb, recenti, vivo_min, operazioni: g.ops, ms_generazione, byte_scatole, riscrittura: risc, cuciture, letture_riscritta, letture_cucite };
            scrivi(&cli.out, &u)?;
        }
        Cmd::Giri { scatole, mb, mb_giro, giri, ogni, id, canc, vivo_min, blocco_kb, letture, seme, recenti } => {
            #[derive(Serialize)]
            struct Giro {
                giro: usize,
                esito: Esito,
                byte_oggetto: u64,
                byte_morti: u64,
                blocchi: usize,
                byte_meta: u64,
                letture_cucita: Option<Letture>,
                letture_riscritta: Option<Letture>,
                byte_riscritta: Option<u64>,
                verifica: Option<Verifica>,
            }
            let blocco = blocco_kb << 10;
            let pref = format!("giri-{:?}-{:?}-{}", id, canc, seme).to_lowercase();
            let mut g = Gen::new(id, canc, seme);
            g.recenti = recenti;
            let cfg = Cfg { modo: Modo::Coda, block_size: blocco, vivo_min, ..Default::default() };
            let (ingressi, _) = genera(&s3, &mut g, scatole, mb, blocco, &format!("{pref}/base")).await?;
            let mut corrente = format!("{pref}/run-000.sst");
            let es0 = cuci(&s3, &ingressi, &corrente, &cfg).await?;
            for k in &ingressi {
                s3.delete(k).await?;
            }
            let mut righe: Vec<Giro> = Vec::new();
            let misura = |righe: &mut Vec<Giro>, giro: usize, es: Esito, m: &Meta, extra: Option<(Letture, Letture, u64, Verifica)>| {
                let (lc, lr, br, v) = match extra {
                    Some((a, b, c, d)) => (Some(a), Some(b), Some(c), Some(d)),
                    None => (None, None, None, None),
                };
                eprintln!("giro {giro}: copiata {:.3}, morti {} MB su {} MB, {:?} {:?}", es.frazione_copiata, m.byte_morti() >> 20, m.obj_len >> 20, lc.as_ref().map(|l| l.p99_ns), lr.as_ref().map(|l| l.p99_ns));
                righe.push(Giro { giro, esito: es, byte_oggetto: m.obj_len, byte_morti: m.byte_morti(), blocchi: m.blocks.len(), byte_meta: m.piede.meta_len as u64, letture_cucita: lc, letture_riscritta: lr, byte_riscritta: br, verifica: v });
            };
            let m0 = s3.meta(&corrente).await?;
            misura(&mut righe, 0, es0, &m0, None);
            for r in 1..=giri {
                let nuova = format!("{pref}/nuova-{r:03}.sst");
                let es = g.scatola(mb_giro << 20);
                s3.put_grande(&nuova, Bytes::from(formato::costruisci(es.iter(), blocco))).await?;
                let prossimo = format!("{pref}/run-{r:03}.sst");
                let esito = cuci(&s3, &[corrente.clone(), nuova.clone()], &prossimo, &cfg).await?;
                s3.delete(&corrente).await?;
                s3.delete(&nuova).await?;
                corrente = prossimo;
                let m = s3.meta(&corrente).await?;
                let extra = if r % ogni == 0 || r == giri {
                    let keys = g.chiavi_lettura(letture);
                    let kr = format!("{pref}/riscritta-{r:03}.sst");
                    riscrivi(&s3, &[corrente.clone()], &kr, &cfg).await?;
                    let obj = s3.get_all(&corrente).await?;
                    let mm = Meta::from_object(&obj)?;
                    let obj_r = s3.get_all(&kr).await?;
                    let m_r = Meta::from_object(&obj_r)?;
                    let v = verifica(&obj, &mm, &obj_r, &m_r, &keys)?;
                    anyhow::ensure!(v.uguali && v.letture_sbagliate == 0, "giro {r}: le uscite non dicono le stesse cose");
                    // tre ripetizioni alternate, si tiene quella con il p99 mediano
                    let mut lc = Vec::new();
                    let mut lr = Vec::new();
                    for _ in 0..3 {
                        lc.push(leggi(&obj, &mm, &keys)?);
                        lr.push(leggi(&obj_r, &m_r, &keys)?);
                    }
                    lc.sort_by_key(|l| l.p99_ns);
                    lr.sort_by_key(|l| l.p99_ns);
                    s3.delete(&kr).await?;
                    Some((lc[1].clone(), lr[1].clone(), obj_r.len() as u64, v))
                } else {
                    None
                };
                misura(&mut righe, r, esito, &m, extra);
                scrivi(&cli.out, &righe)?;
            }
            s3.delete(&corrente).await?;
            scrivi(&cli.out, &righe)?;
        }
    }
    Ok(())
}
