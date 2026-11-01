//! Banco di latenza: scritture in coda con fsync o con O_DSYNC, lotti di varie
//! dimensioni, da 1 a 64 file concorrenti, controllo del turno dopo ogni lotto.
//!
//! Tre varianti del controllo (`--controllo`):
//!   no        nessun controllo (latenza dell'fsync da solo)
//!   si        dopo l'fsync del lotto k, la domanda al server; poi si scrive k+1
//!   pipeline  dopo l'fsync di k la domanda parte su un altro thread e intanto si
//!             scrive k+1; k è confermato all'arrivo della sua risposta
//! Tre modi di fare la domanda (`--controllo-modo`):
//!   open      apre per nome epoche/<io>/<e+1> (LOOKUP/OPEN al server con lookupcache=none)
//!   stat      stat dello stesso nome
//!   odirect   pread di 4 KiB con O_DIRECT da un file di controllo (deve restare a zero)
//! Con `--solo-controllo si` si fanno solo le domande, senza scrivere: serve a misurare
//! quanti KiB fattura una domanda.

use crate::comune::*;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Config {
    pub dir: String,
    pub controllo_dir: String,
    pub modo: String,     // "fsync" | "dsync"
    pub lotto_kib: u64,
    pub file: u64,
    pub durata_s: f64,
    pub lotti_max: u64,   // 0 = senza limite (vale la durata)
    pub controllo: String, // "no" | "si" | "pipeline"
    pub controllo_modo: String, // "open" | "stat" | "odirect"
    pub solo_controllo: bool,
    pub intervallo_ms: f64, // 0 = a ciclo chiuso; altrimenti un lotto ogni N ms per file
    pub prealloca_mib: u64,
    pub nome_libro: String, // se c'è: libro fisso dir/coda/<nome>-<i>.log, tenuto alla fine
    pub etichetta: String,
    pub fasi: String,
}

pub struct Esito {
    pub riga: String,
}

/// La domanda vera al server. Ok(true) = turno perso.
struct Controllore {
    modo: String,
    segnale: PathBuf,
    file: Option<File>,
    buf: Allineato,
}

impl Controllore {
    fn new(modo: &str, segnale: PathBuf) -> Controllore {
        let mut file = None;
        if modo == "odirect" {
            // file di controllo da 4 KiB di zeri: chi prende il turno ci scriverebbe dentro
            let p = segnale.with_file_name("controllo.bin");
            if let Some(d) = p.parent() {
                std::fs::create_dir_all(d).unwrap();
            }
            if !p.exists() {
                let f = File::create(&p).unwrap();
                f.write_all_at(&[0u8; 4096], 0).unwrap();
                f.sync_all().unwrap();
            }
            file = Some(apri_lettura(&p, true).unwrap_or_else(|e| fail(&format!("O_DIRECT su {p:?}: {e}"))));
        }
        Controllore { modo: modo.to_string(), segnale, file, buf: Allineato::new(4096) }
    }
    fn chiedi(&mut self) -> std::io::Result<bool> {
        match self.modo.as_str() {
            "open" => esiste_dal_server(&self.segnale),
            "stat" => match std::fs::metadata(&self.segnale) {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e),
            },
            "odirect" => {
                let f = self.file.as_ref().unwrap();
                let b = self.buf.buf();
                f.read_exact_at(&mut b[..4096], 0)?;
                Ok(b[..4096].iter().any(|&x| x != 0))
            }
            m => fail(&format!("--controllo-modo open|stat|odirect, non {m}")),
        }
    }
}

struct Risultato {
    tot: Campioni,
    sync: Campioni,
    ctl: Campioni,
    lotti: u64,
    controlli: u64,
    byte: u64,
    errori: u64,
    libro: PathBuf,
}

fn un_file(c: &Config, i: u64, run: &str, host: &str, fine_prevista: Instant) -> Risultato {
    let dsync = c.modo == "dsync";
    let lotto = (c.lotto_kib * 1024) as usize;
    let libro = if c.nome_libro.is_empty() {
        pb(&c.dir, &["scrivi", host, run, &format!("{i}.log")])
    } else {
        pb(&c.dir, &["coda", &format!("{}-{i}.log", c.nome_libro)])
    };
    // il file che direbbe "hai perso il turno": non esiste mai in questo banco,
    // ma la domanda al server si fa davvero, come nel protocollo.
    let segnale = pb(&c.controllo_dir, &["scrivi", host, run, "epoche", &i.to_string(), "2"]);
    let mut ctl = Controllore::new(&c.controllo_modo, segnale.clone());
    let mut r = Risultato {
        tot: Campioni::default(),
        sync: Campioni::default(),
        ctl: Campioni::default(),
        lotti: 0,
        controlli: 0,
        byte: 0,
        errori: 0,
        libro: libro.clone(),
    };
    let passo = Duration::from_secs_f64(c.intervallo_ms / 1000.0);
    let t_partenza = Instant::now();
    let finito = |n: u64| Instant::now() >= fine_prevista || (c.lotti_max > 0 && n >= c.lotti_max);

    if c.solo_controllo {
        while !finito(r.controlli) {
            let t0 = Instant::now();
            match ctl.chiedi() {
                Ok(false) => {}
                Ok(true) => fail("il segnale di turno perso esiste: banco sporco"),
                Err(e) => {
                    eprintln!("controllo fallito: {e}");
                    r.errori += 1;
                    break;
                }
            }
            r.ctl.add(t0.elapsed());
            r.tot.add(t0.elapsed());
            r.controlli += 1;
            if c.intervallo_ms > 0.0 {
                let prossimo = t_partenza + passo * r.controlli as u32;
                if let Some(d) = prossimo.checked_duration_since(Instant::now()) {
                    std::thread::sleep(d);
                }
            }
        }
        return r;
    }

    let f = apri_libro(&libro, dsync, c.prealloca_mib * 1024 * 1024).unwrap_or_else(|e| fail(&format!("apro {libro:?}: {e}")));

    // in pipeline le domande le fa un altro thread, nell'ordine dei lotti
    let pipeline = c.controllo == "pipeline";
    let (tx, rx) = mpsc::sync_channel::<(Instant, Instant)>(256);
    let controllore = if pipeline {
        let mut ctl2 = Controllore::new(&c.controllo_modo, segnale.clone());
        Some(std::thread::spawn(move || {
            let (mut tot, mut ctl) = (Campioni::default(), Campioni::default());
            let mut errori = 0u64;
            for (t0, t1) in rx {
                let a = Instant::now();
                match ctl2.chiedi() {
                    Ok(false) => {}
                    Ok(true) => fail("il segnale di turno perso esiste: banco sporco"),
                    Err(e) => {
                        eprintln!("controllo fallito: {e}");
                        errori += 1;
                        break;
                    }
                }
                let b = Instant::now();
                // conferma di k: dall'inizio della scrittura di k all'arrivo della sua risposta
                tot.add(b - t0);
                ctl.add(b - a.max(t1));
            }
            (tot, ctl, errori)
        }))
    } else {
        drop(rx);
        None
    };

    let mut off = 0u64;
    let mut seq = 0u64;
    while !finito(seq) {
        seq += 1;
        let rec = record(seq, lotto);
        let t0 = Instant::now();
        if let Err(e) = f.write_all_at(&rec, off) {
            // dopo un errore non si ritenta lo stesso libro (fsyncgate): si ferma
            eprintln!("scrittura fallita: {e}");
            r.errori += 1;
            break;
        }
        if !dsync {
            if let Err(e) = f.sync_all() {
                eprintln!("fsync fallita: {e}");
                r.errori += 1;
                break;
            }
        }
        let t1 = Instant::now();
        r.sync.add(t1 - t0);
        match c.controllo.as_str() {
            "si" => {
                match ctl.chiedi() {
                    Ok(false) => {}
                    Ok(true) => fail("il segnale di turno perso esiste: banco sporco"),
                    Err(e) => {
                        eprintln!("controllo fallito: {e}");
                        r.errori += 1;
                        break;
                    }
                }
                let t2 = Instant::now();
                r.ctl.add(t2 - t1);
                r.tot.add(t2 - t0);
                r.controlli += 1;
            }
            "pipeline" => {
                if tx.send((t0, t1)).is_err() {
                    r.errori += 1;
                    break;
                }
                r.controlli += 1;
            }
            _ => r.tot.add(t1 - t0),
        }
        off += rec.len() as u64;
        if c.intervallo_ms > 0.0 {
            let prossimo = t_partenza + passo * seq as u32;
            if let Some(d) = prossimo.checked_duration_since(Instant::now()) {
                std::thread::sleep(d);
            }
        }
    }
    r.lotti = seq;
    r.byte = off;
    drop(tx);
    if let Some(h) = controllore {
        let (tot, ctl, e) = h.join().unwrap();
        r.tot = tot;
        r.ctl = ctl;
        r.errori += e;
    }
    r
}

pub fn esegui(c: &Config) -> Esito {
    let host = host_id();
    if !["fsync", "dsync"].contains(&c.modo.as_str()) {
        fail(&format!("--modo fsync|dsync, non {}", c.modo));
    }
    if !["no", "si", "pipeline"].contains(&c.controllo.as_str()) {
        fail(&format!("--controllo no|si|pipeline, non {}", c.controllo));
    }
    if (c.lotto_kib * 1024) as usize <= TESTA + 8 {
        fail("lotto troppo piccolo");
    }
    let run = format!("{}-{}", c.etichetta, ora_ms());
    let inizio = ora_ms();
    let t_inizio = Instant::now();
    let fine_prevista = t_inizio + Duration::from_secs_f64(c.durata_s);
    let cfg = Arc::new(c.clone());
    let mut handles = Vec::new();
    for i in 0..c.file {
        let (cfg, run, host) = (cfg.clone(), run.clone(), host.clone());
        handles.push(std::thread::spawn(move || un_file(&cfg, i, &run, &host, fine_prevista)));
    }
    let (mut tot, mut sync, mut ctl) = (Campioni::default(), Campioni::default(), Campioni::default());
    let (mut lotti, mut controlli, mut byte, mut errori) = (0u64, 0u64, 0u64, 0u64);
    let mut libri = Vec::new();
    for h in handles {
        let r = h.join().expect("thread");
        tot.unisci(&r.tot);
        sync.unisci(&r.sync);
        ctl.unisci(&r.ctl);
        lotti += r.lotti;
        controlli += r.controlli;
        byte += r.byte;
        errori += r.errori;
        libri.push(r.libro);
    }
    let fine = ora_ms();
    let secondi = t_inizio.elapsed().as_secs_f64();
    let (p50, p99, p999, max) = tot.riassunto();
    let (s50, s99, s999, _) = sync.riassunto();
    let (c50, c99, c999, _) = ctl.riassunto();
    let conferme = tot.us.len() as u64;
    let riga = json(&[
        ("fase", js(&c.etichetta)),
        ("host", js(&host)),
        ("modo", js(if c.solo_controllo { "solo-controllo" } else { &c.modo })),
        ("lotto_kib", if c.solo_controllo { "0".into() } else { c.lotto_kib.to_string() }),
        ("file", c.file.to_string()),
        ("controllo", js(&c.controllo)),
        ("controllo_modo", js(&c.controllo_modo)),
        ("intervallo_ms", format!("{}", c.intervallo_ms)),
        ("prealloca_mib", c.prealloca_mib.to_string()),
        ("inizio_ms", inizio.to_string()),
        ("fine_ms", fine.to_string()),
        ("secondi", format!("{secondi:.3}")),
        ("lotti", lotti.to_string()),
        ("controlli", controlli.to_string()),
        ("conferme", conferme.to_string()),
        ("conferme_s", format!("{:.1}", conferme as f64 / secondi.max(1e-9))),
        ("byte", byte.to_string()),
        ("errori", errori.to_string()),
        ("p50_ms", format!("{p50:.3}")),
        ("p99_ms", format!("{p99:.3}")),
        ("p999_ms", format!("{p999:.3}")),
        ("max_ms", format!("{max:.3}")),
        ("sync_p50_ms", format!("{s50:.3}")),
        ("sync_p99_ms", format!("{s99:.3}")),
        ("sync_p999_ms", format!("{s999:.3}")),
        ("ctl_p50_ms", format!("{c50:.3}")),
        ("ctl_p99_ms", format!("{c99:.3}")),
        ("ctl_p999_ms", format!("{c999:.3}")),
    ]);
    if !c.fasi.is_empty() {
        aggiungi_riga(Path::new(&c.fasi), &riga).unwrap_or_else(|e| fail(&format!("scrivo {}: {e}", c.fasi)));
    }
    // i libri del banco non servono più: si cancellano (lo spazio su EFS si paga),
    // tranne il libro fisso letto da un'altra istanza
    if c.nome_libro.is_empty() {
        for l in libri {
            let _ = std::fs::remove_file(l);
        }
    }
    Esito { riga }
}

pub fn da_args(a: &Args) -> Config {
    let dir = a.req("dir");
    Config {
        controllo_dir: a.s("controllo-dir", &dir),
        dir,
        modo: a.s("modo", "fsync"),
        lotto_kib: a.u("lotto-kib", 4),
        file: a.u("file", 1),
        durata_s: a.f("durata-s", 10.0),
        lotti_max: a.u("lotti", 0),
        controllo: a.s("controllo", "si"),
        controllo_modo: a.s("controllo-modo", "open"),
        solo_controllo: a.si("solo-controllo", false),
        intervallo_ms: a.f("intervallo-ms", 0.0),
        prealloca_mib: a.u("prealloca-mib", 0),
        nome_libro: a.s("nome-libro", ""),
        etichetta: a.s("fase", "scrivi"),
        fasi: a.s("fasi", ""),
    }
}

/// Tutte le combinazioni, una dopo l'altra, con una pausa tra le fasi così che
/// le metriche CloudWatch (a minuti) si possano attribuire a ogni fase.
/// Con `--anche-solo-controllo si` aggiunge una fase di sole domande per ogni modo di controllo.
pub fn matrice(a: &Args) {
    let base = da_args(a);
    let lotti = a.lista_u("lotti-kib", "1,4,32,64,256");
    let files = a.lista_u("files", "1,8,64");
    let modi = a.lista_s("modi", "fsync,dsync");
    let controlli = a.lista_s("controlli", "si");
    let pausa = a.f("pausa-s", 90.0);
    let prefisso = a.s("fase", "m");
    let mut lista: Vec<Config> = Vec::new();
    if a.si("anche-solo-controllo", false) {
        for cm in a.lista_s("controllo-modi", &base.controllo_modo) {
            let mut c = base.clone();
            c.solo_controllo = true;
            c.controllo = "si".into();
            c.controllo_modo = cm.clone();
            c.etichetta = format!("{prefisso}-solo-controllo-{cm}-{}f", c.file);
            lista.push(c);
        }
    }
    for m in &modi {
        for ctl in &controlli {
            for &l in &lotti {
                for &f in &files {
                    let mut c = base.clone();
                    c.modo = m.clone();
                    c.lotto_kib = l;
                    c.file = f;
                    c.controllo = ctl.clone();
                    c.etichetta = format!("{prefisso}-{m}-{l}k-{f}f-ctl{ctl}");
                    lista.push(c);
                }
            }
        }
    }
    let tot = lista.len();
    for (n, c) in lista.iter().enumerate() {
        eprintln!("[{}/{tot}] {}", n + 1, c.etichetta);
        println!("{}", esegui(c).riga);
        if n + 1 < tot && pausa > 0.0 {
            std::thread::sleep(Duration::from_secs_f64(pausa));
        }
    }
}
