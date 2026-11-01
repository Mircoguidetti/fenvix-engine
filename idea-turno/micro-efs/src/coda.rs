//! Letture dal libro da un'altra istanza (aggiunta di Fable, giro 2, misura 2).
//!
//! Mentre un'istanza scrive il libro (`scrivi --nome-libro X --intervallo-ms 2`),
//! questa istanza tiene il file aperto (non lo riapre) e:
//!   - fa pread casuali da 4 KiB (con O_DIRECT o senza) sulla parte già scritta:
//!     p50, p99, p99,9 della lettura;
//!   - segue la coda: appena un lotto nuovo è leggibile e valido, misura il ritardo
//!     tra l'ora scritta nel lotto (prima della scrittura) e l'ora in cui lo vede.
//! Il ritardo include l'fsync dello scrittore e lo scarto tra gli orologi delle due
//! istanze (Amazon Time Sync: di solito sotto il millisecondo; da riportare).
//! "Con e senza noac" si misura lanciando lo stesso comando su due mount diversi.

use crate::comune::*;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const BLOCCO: u64 = 4096;

fn leggi_a(f: &File, buf: &mut Allineato, off: u64, len: usize) -> std::io::Result<usize> {
    // offset e lunghezza allineati a 4 KiB (O_DIRECT); restituisce i byte letti
    let b = buf.buf();
    let len = len.min(b.len());
    let mut letti = 0;
    while letti < len {
        let n = f.read_at(&mut b[letti..len], off + letti as u64)?;
        if n == 0 {
            break;
        }
        letti += n;
    }
    Ok(letti)
}

pub fn leggi_coda(a: &Args) {
    let libro = PathBuf::from(a.req("libro"));
    let durata = Duration::from_secs_f64(a.f("durata-s", 60.0));
    let letture_s = a.f("letture-s", 500.0);
    let diretto = a.si("odirect", true);
    let poll = Duration::from_secs_f64(a.f("poll-ms", 1.0) / 1000.0);
    let etichetta = a.s("fase", "coda");
    let fasi = a.s("fasi", "");
    if !aspetta_segnale(&libro, Duration::from_secs(a.u("attesa-s", 120))) {
        fail(&format!("il libro {libro:?} non è comparso"));
    }
    let f = apri_lettura(&libro, diretto).unwrap_or_else(|e| fail(&format!("apro {libro:?}: {e}")));
    let mut buf = Allineato::new(512 * 1024);
    let mut buf_r = Allineato::new(4096);
    let (mut lat, mut ritardo) = (Campioni::default(), Campioni::default());
    let mut tail = 0u64;
    let mut seq_atteso = 1u64;
    let (mut visti, mut errori, mut buchi) = (0u64, 0u64, 0u64);
    let mut seed = ora_us() | 1;
    let inizio = ora_ms();
    let t0 = Instant::now();
    let passo_letture = if letture_s > 0.0 { Duration::from_secs_f64(1.0 / letture_s) } else { Duration::MAX };
    let mut prossima_lettura = t0;
    while t0.elapsed() < durata {
        // 1. la coda: tutti i lotti nuovi già visibili
        loop {
            let base = tail / BLOCCO * BLOCCO;
            let n = match leggi_a(&f, &mut buf, base, 64 * 1024) {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("lettura della coda: {e}");
                    errori += 1;
                    break;
                }
            };
            let dentro = (tail - base) as usize;
            if n < dentro + TESTA {
                break;
            }
            let Some(len) = lunghezza_da_testa(&buf.buf()[dentro..n]) else { break };
            let serve = (dentro + len).div_ceil(BLOCCO as usize) * BLOCCO as usize;
            let n = if serve > n {
                match leggi_a(&f, &mut buf, base, serve) {
                    Ok(n) => n,
                    Err(_) => {
                        errori += 1;
                        break;
                    }
                }
            } else {
                n
            };
            if n < dentro + len {
                break;
            }
            let Some((seq, l, t_scritto)) = valida_record(&buf.buf()[dentro..dentro + len]) else { break };
            let ora = ora_us();
            if t_scritto > 0 && visti > 0 {
                // il primo lotto può essere vecchio (scritto prima che partissimo): non conta
                ritardo.add(Duration::from_micros(ora.saturating_sub(t_scritto)));
            }
            if seq != seq_atteso {
                buchi += 1;
            }
            seq_atteso = seq + 1;
            visti += 1;
            tail += l as u64;
        }
        // 2. le letture casuali sulla parte già scritta
        while Instant::now() >= prossima_lettura && tail >= BLOCCO {
            let blocchi = tail / BLOCCO;
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let off = (seed % blocchi) * BLOCCO;
            let t = Instant::now();
            match leggi_a(&f, &mut buf_r, off, 4096) {
                Ok(_) => lat.add(t.elapsed()),
                Err(e) => {
                    eprintln!("pread: {e}");
                    errori += 1;
                }
            }
            prossima_lettura += passo_letture;
        }
        std::thread::sleep(poll);
    }
    let fine = ora_ms();
    let (p50, p99, p999, max) = lat.riassunto();
    let (r50, r99, r999, rmax) = ritardo.riassunto();
    let riga = json(&[
        ("fase", js(&etichetta)),
        ("host", js(&host_id())),
        ("modo", js(if diretto { "lettura-odirect" } else { "lettura-cache" })),
        ("lotto_kib", "4".into()),
        ("inizio_ms", inizio.to_string()),
        ("fine_ms", fine.to_string()),
        ("lotti", "0".into()),
        ("letture", lat.us.len().to_string()),
        ("lotti_visti_in_coda", visti.to_string()),
        ("buchi_di_sequenza", buchi.to_string()),
        ("errori", errori.to_string()),
        ("lettura_p50_ms", format!("{p50:.3}")),
        ("lettura_p99_ms", format!("{p99:.3}")),
        ("lettura_p999_ms", format!("{p999:.3}")),
        ("lettura_max_ms", format!("{max:.3}")),
        ("ritardo_visibilita_p50_ms", format!("{r50:.3}")),
        ("ritardo_visibilita_p99_ms", format!("{r99:.3}")),
        ("ritardo_visibilita_p999_ms", format!("{r999:.3}")),
        ("ritardo_visibilita_max_ms", format!("{rmax:.3}")),
    ]);
    if !fasi.is_empty() {
        aggiungi_riga(Path::new(&fasi), &riga).unwrap_or_else(|e| fail(&format!("scrivo {fasi}: {e}")));
    }
    println!("{riga}");
}
