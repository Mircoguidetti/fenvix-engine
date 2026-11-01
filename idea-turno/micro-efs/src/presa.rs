//! Banco 10 e banco zombie: presa del turno.
//!
//! Il titolare (processo figlio, istanza A) scrive lotti nel suo libro: pwrite con
//! offset esplicito, fsync (o O_DSYNC), poi la domanda vera al server "esiste
//! epoche/<io>/2?". Solo se la risposta è no conferma il lotto al padre (il "cliente").
//!
//! Il subentrante (istanza B), per ogni ciclo: crea PRIMA turni/<db>/2 (O_EXCL), POI
//! epoche/<titolare>/2 (O_EXCL), POI rilegge i turni (nessuno più alto), POI legge il libro.
//!
//! Modi del titolare:
//!   uccidi: SIGKILL a metà, poi la presa (banco 10)
//!   ferma:  SIGSTOP, la presa, SIGCONT: il vecchio titolare continua a scrivere (zombie)
//!   vivo:   la presa mentre il titolare scrive senza pause (zombie vivo)
//! Verifica: ogni lotto confermato deve essere nel libro letto dal subentrante.

use crate::comune::*;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn base(dir: &str, run: &str) -> PathBuf {
    pb(dir, &["presa", run])
}

fn rnd(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

// ---------------------------------------------------------------- figlio (il titolare vero)

pub fn figlio(a: &Args) {
    let libro = PathBuf::from(a.req("libro"));
    let segnale = PathBuf::from(a.req("segnale"));
    let lotto = (a.u("lotto-kib", 4) * 1024) as usize;
    let dsync = a.s("modo-sync", "fsync") == "dsync";
    let prealloca = a.u("prealloca-mib", 0) * 1024 * 1024;
    let ritmo = a.u("ritmo-ms", 0);
    let dopo_perso = a.u("dopo-perso", 200);
    let f = apri_libro(&libro, dsync, prealloca).unwrap_or_else(|e| fail(&format!("libro: {e}")));
    let mut out = std::io::stdout().lock();
    let mut off = 0u64;
    let mut seq = 0u64;
    let mut perso = false;
    let mut scritti_dopo = 0u64;
    loop {
        seq += 1;
        let r = record(seq, lotto);
        if f.write_all_at(&r, off).is_err() {
            let _ = writeln!(out, "ERR {seq}");
            std::process::exit(3);
        }
        if !dsync && f.sync_all().is_err() {
            // dopo un errore di fsync non si conferma più niente da questo libro
            let _ = writeln!(out, "ERR {seq}");
            std::process::exit(3);
        }
        off += r.len() as u64;
        // la domanda vera al server, DOPO l'fsync
        let lost = match esiste_dal_server(&segnale) {
            Ok(x) => x,
            Err(_) => {
                let _ = writeln!(out, "ERR {seq}");
                std::process::exit(3);
            }
        };
        let riga = if lost || perso {
            if !perso {
                perso = true;
                format!("PERSO {seq}")
            } else {
                scritti_dopo += 1;
                format!("Z {seq}")
            }
        } else {
            format!("OK {seq} {} {}", off - r.len() as u64, r.len())
        };
        if writeln!(out, "{riga}").and_then(|_| out.flush()).is_err() {
            std::process::exit(4); // il padre non c'è più
        }
        if perso && scritti_dopo >= dopo_perso {
            std::process::exit(0);
        }
        if ritmo > 0 {
            std::thread::sleep(Duration::from_millis(ritmo));
        }
    }
}

// ---------------------------------------------------------------- titolare (padre)

#[derive(Clone)]
struct Ev {
    tipo: char, // 'O' confermato, 'P' perso, 'Z' scritto da zombie, 'E' errore
    seq: u64,
    fase: u8, // 0 prima della presa, 1 dopo SIGCONT (solo modo ferma)
}

fn segnale_path(dir: &str, run: &str, nome: &str) -> PathBuf {
    base(dir, run).join("segnali").join(nome)
}

pub fn titolare(a: &Args) {
    let dir = a.req("dir");
    let cdir = a.s("controllo-dir", &dir);
    let run = a.req("run");
    let cicli = a.u("cicli", 20);
    let lotti = a.u("lotti", 100).max(2);
    let modo = a.s("modo", "uccidi");
    if !["uccidi", "ferma", "vivo"].contains(&modo.as_str()) {
        fail("--modo uccidi|ferma|vivo");
    }
    let attesa = Duration::from_secs(a.u("attesa-s", 300));
    let me = std::env::current_exe().unwrap();
    let mut seed = ora_ms() ^ ((std::process::id() as u64) << 32) | 1;
    let mut tot_conf = 0u64;
    for i in 1..=cicli {
        let t = format!("T{i}");
        let b = base(&dir, &run);
        // turno 1 ed epoca 1 del titolare di questo ciclo
        if !crea_esclusivo(&b.join("turni").join(&t).join("1"), &t).unwrap() {
            fail(&format!("turni/{t}/1 esiste già: usare un --run nuovo"));
        }
        crea_esclusivo(&b.join("epoche").join(&t).join("1"), &t).unwrap();
        let libro = b.join("libri").join(&t).join("1.log");
        let segnale = base(&cdir, &run).join("epoche").join(&t).join("2");
        let mut child = Command::new(&me)
            .arg("presa-figlio")
            .args(["--libro", libro.to_str().unwrap()])
            .args(["--segnale", segnale.to_str().unwrap()])
            .args(["--lotto-kib", &a.s("lotto-kib", "4")])
            .args(["--modo-sync", &a.s("modo-sync", "fsync")])
            .args(["--prealloca-mib", &a.s("prealloca-mib", "0")])
            .args(["--ritmo-ms", &a.s("ritmo-ms", "0")])
            .stdout(Stdio::piped())
            .spawn()
            .expect("avvio del figlio");
        let pid = child.id() as i32;
        let eventi: Arc<Mutex<Vec<Ev>>> = Arc::new(Mutex::new(Vec::new()));
        let fase = Arc::new(AtomicU8::new(0));
        let lettore = {
            let (ev, fase) = (eventi.clone(), fase.clone());
            let so = child.stdout.take().unwrap();
            std::thread::spawn(move || {
                for l in BufReader::new(so).lines() {
                    let Ok(l) = l else { break };
                    let mut it = l.split_whitespace();
                    let tipo = match it.next() {
                        Some("OK") => 'O',
                        Some("PERSO") => 'P',
                        Some("Z") => 'Z',
                        _ => 'E',
                    };
                    let seq = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
                    ev.lock().unwrap().push(Ev { tipo, seq, fase: fase.load(Ordering::SeqCst) });
                }
            })
        };
        let conta = |c: char| eventi.lock().unwrap().iter().filter(|e| e.tipo == c).count() as u64;
        let k = lotti / 2 + rnd(&mut seed) % (lotti / 2 + 1);
        let t0 = Instant::now();
        while conta('O') < k && conta('E') == 0 && t0.elapsed() < attesa {
            std::thread::sleep(Duration::from_millis(1));
        }
        let pronto = segnale_path(&dir, &run, &format!("pronto-{i}"));
        let preso = segnale_path(&cdir, &run, &format!("preso-{i}"));
        match modo.as_str() {
            "uccidi" => {
                unsafe { libc::kill(pid, libc::SIGKILL) };
                let _ = child.wait();
                lettore.join().unwrap();
                crea_esclusivo(&pronto, "ucciso").unwrap();
                if !aspetta_segnale(&preso, attesa) {
                    fail(&format!("ciclo {i}: nessuna presa entro l'attesa"));
                }
            }
            "ferma" => {
                unsafe { libc::kill(pid, libc::SIGSTOP) };
                std::thread::sleep(Duration::from_millis(100)); // il lettore svuota il tubo
                crea_esclusivo(&pronto, "fermo").unwrap();
                if !aspetta_segnale(&preso, attesa) {
                    fail(&format!("ciclo {i}: nessuna presa entro l'attesa"));
                }
                fase.store(1, Ordering::SeqCst);
                unsafe { libc::kill(pid, libc::SIGCONT) };
                let t1 = Instant::now();
                while conta('P') == 0 && conta('E') == 0 && t1.elapsed() < Duration::from_secs(30) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                std::thread::sleep(Duration::from_millis(200)); // lo zombie scrive ancora un po'
                unsafe { libc::kill(pid, libc::SIGKILL) };
                let _ = child.wait();
                lettore.join().unwrap();
            }
            _ => {
                // vivo: la presa avviene mentre il figlio scrive
                crea_esclusivo(&pronto, "vivo").unwrap();
                let t1 = Instant::now();
                while conta('P') == 0 && conta('E') == 0 && t1.elapsed() < attesa {
                    std::thread::sleep(Duration::from_millis(1));
                }
                std::thread::sleep(Duration::from_millis(200));
                unsafe { libc::kill(pid, libc::SIGKILL) };
                let _ = child.wait();
                lettore.join().unwrap();
            }
        }
        let ev = eventi.lock().unwrap().clone();
        let mut testo = String::new();
        for e in ev.iter().filter(|e| e.tipo == 'O') {
            testo.push_str(&format!("{} {}\n", e.seq, e.fase));
        }
        let zombie = ev.iter().filter(|e| e.tipo == 'Z').count();
        let perso = ev.iter().any(|e| e.tipo == 'P');
        let errori = ev.iter().filter(|e| e.tipo == 'E').count();
        scrivi_testo(&b.join("risultati").join(format!("confermati-{i}.txt")), &testo).unwrap();
        scrivi_testo(
            &b.join("risultati").join(format!("titolare-{i}.json")),
            &json(&[
                ("modo", js(&modo)),
                ("confermati", conta_righe(&testo).to_string()),
                ("visto_perso", perso.to_string()),
                ("scritti_da_zombie", zombie.to_string()),
                ("errori", errori.to_string()),
            ]),
        )
        .unwrap();
        tot_conf += conta_righe(&testo);
        crea_esclusivo(&segnale_path(&dir, &run, &format!("fine-{i}")), "fine").unwrap();
        eprintln!("ciclo {i}: {} confermati, perso={perso}, zombie={zombie}, errori={errori}", conta_righe(&testo));
    }
    println!("{}", json(&[("run", js(&run)), ("modo", js(&modo)), ("cicli", cicli.to_string()), ("confermati", tot_conf.to_string())]));
}

fn conta_righe(s: &str) -> u64 {
    s.lines().filter(|l| !l.is_empty()).count() as u64
}

// ---------------------------------------------------------------- subentrante

pub fn subentra(a: &Args) {
    let dir = a.req("dir");
    let cdir = a.s("controllo-dir", &dir);
    let run = a.req("run");
    let cicli = a.u("cicli", 20);
    let me = a.s("id", &host_id());
    let attesa = Duration::from_secs(a.u("attesa-s", 600));
    let b = base(&dir, &run);
    let mut tempi = Campioni::default();
    for i in 1..=cicli {
        let t = format!("T{i}");
        if !aspetta_segnale(&segnale_path(&cdir, &run, &format!("pronto-{i}")), attesa) {
            fail(&format!("ciclo {i}: il titolare non è pronto"));
        }
        let t0 = Instant::now();
        // 1. PRIMA il turno (nome solo n), 2. POI l'epoca nuova, 3. POI la lettura
        if !crea_esclusivo(&b.join("turni").join(&t).join("2"), &me).unwrap() {
            fail(&format!("ciclo {i}: turni/{t}/2 esiste già"));
        }
        if !crea_esclusivo(&b.join("epoche").join(&t).join("2"), &me).unwrap() {
            fail(&format!("ciclo {i}: epoche/{t}/2 esiste già"));
        }
        // 2b. rilegge i turni dal server: se c'è un turno più alto, rinuncia
        if esiste_dal_server(&base(&cdir, &run).join("turni").join(&t).join("3")).unwrap_or(true) {
            fail(&format!("ciclo {i}: c'è un turno più alto, rinuncio"));
        }
        let letti = leggi_libro(&b.join("libri").join(&t).join("1.log")).unwrap_or_default();
        tempi.add(t0.elapsed());
        let mut testo = String::new();
        for l in &letti {
            testo.push_str(&format!("{} {} {}\n", l.seq, l.offset, l.byte));
        }
        scrivi_testo(&b.join("risultati").join(format!("letti-{i}.txt")), &testo).unwrap();
        crea_esclusivo(&segnale_path(&dir, &run, &format!("preso-{i}")), &me).unwrap();
        eprintln!("ciclo {i}: presa in {:.1} ms, {} lotti letti", t0.elapsed().as_secs_f64() * 1000.0, letti.len());
    }
    let (p50, p99, _, max) = tempi.riassunto();
    println!(
        "{}",
        json(&[("run", js(&run)), ("cicli", cicli.to_string()), ("presa_p50_ms", format!("{p50:.2}")), ("presa_p99_ms", format!("{p99:.2}")), ("presa_max_ms", format!("{max:.2}"))])
    );
}

// ---------------------------------------------------------------- verifica

pub fn verifica(a: &Args) -> bool {
    let dir = a.req("dir");
    let run = a.req("run");
    let cicli = a.u("cicli", 20);
    let r = base(&dir, &run).join("risultati");
    let (mut conf, mut mancanti, mut byte_mancanti, mut dopo, mut letti_tot, mut zombie) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let mut modo = String::new();
    let mut cicli_ok = 0u64;
    for i in 1..=cicli {
        let c = leggi(&r.join(format!("confermati-{i}.txt")));
        let l = leggi(&r.join(format!("letti-{i}.txt")));
        let tit = fs::read_to_string(r.join(format!("titolare-{i}.json"))).unwrap_or_default();
        modo = campo(&tit, "modo").unwrap_or_default();
        zombie += campo(&tit, "scritti_da_zombie").and_then(|x| x.parse().ok()).unwrap_or(0);
        let letti: HashMap<u64, u64> = l.iter().map(|v| (v[0], v.get(2).copied().unwrap_or(0))).collect();
        letti_tot += letti.len() as u64;
        let mut ok = !tit.is_empty();
        for v in &c {
            conf += 1;
            if v.get(1).copied().unwrap_or(0) == 1 {
                dopo += 1;
                ok = false;
            }
            if !letti.contains_key(&v[0]) {
                mancanti += 1;
                ok = false;
                // il lotto mancante pesa quanto quelli letti (stessa dimensione)
                byte_mancanti += letti.values().next().copied().unwrap_or(0);
            }
        }
        if ok {
            cicli_ok += 1;
        }
    }
    let pass = mancanti == 0 && dopo == 0 && cicli_ok == cicli;
    println!(
        "{}",
        json(&[
            ("banco", js(if modo == "uccidi" { "10-presa-dopo-sigkill" } else { "zombie" })),
            ("modo", js(&modo)),
            ("run", js(&run)),
            ("cicli", cicli.to_string()),
            ("cicli_completi", cicli_ok.to_string()),
            ("lotti_confermati", conf.to_string()),
            ("lotti_letti_dal_nuovo", letti_tot.to_string()),
            ("lotti_confermati_mancanti", mancanti.to_string()),
            ("byte_confermati_mancanti", byte_mancanti.to_string()),
            ("conferme_dopo_la_presa", dopo.to_string()),
            ("lotti_scritti_dallo_zombie", zombie.to_string()),
            ("esito", js(if pass { "PASSA" } else { "MUORE" })),
        ])
    );
    pass
}

fn leggi(p: &Path) -> Vec<Vec<u64>> {
    fs::read_to_string(p)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split_whitespace().filter_map(|x| x.parse().ok()).collect())
        .collect()
}
