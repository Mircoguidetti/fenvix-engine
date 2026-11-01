//! Banco 9: gara per il turno. Tutti i concorrenti (thread su più istanze) provano
//! a creare `turni/<n>` con O_CREAT|O_EXCL nello stesso istante (orologio di sistema,
//! sincronizzato da chrony/Amazon Time Sync). Deve vincere esattamente uno per round.

use crate::comune::*;
use std::collections::HashMap;
use std::fs;

pub fn gara(a: &Args) {
    let dir = a.req("dir");
    let run = a.req("run");
    let id = a.s("id", &host_id());
    let k = a.u("concorrenti", 4);
    let round = a.u("round", 1000);
    let periodo = a.u("periodo-ms", 50);
    let inizio = a.u("inizio-ms", ora_ms() + 3000);
    if inizio < ora_ms() {
        fail("--inizio-ms è già passato");
    }
    eprintln!("gara {run}: {k} concorrenti da {id}, {round} round ogni {periodo} ms, inizio tra {} ms", inizio - ora_ms());
    let mut hs = Vec::new();
    for j in 0..k {
        let (dir, run, me) = (dir.clone(), run.clone(), format!("{id}-{j}"));
        hs.push(std::thread::spawn(move || {
            let mut righe = String::new();
            let (mut vinti, mut ritentati) = (0u64, 0u64);
            for n in 1..=round {
                dormi_fino_a(inizio + n * periodo);
                let p = pb(&dir, &["gara", &run, "turni", &n.to_string()]);
                let esito = match crea_esclusivo(&p, &me) {
                    Ok(true) => {
                        vinti += 1;
                        "vinto"
                    }
                    Ok(false) => {
                        // chi trova EEXIST rilegge: se dentro c'è il suo nome, aveva vinto
                        // (ritentativo del client NFS dopo una risposta persa)
                        match fs::read_to_string(&p) {
                            Ok(s) if s == me => {
                                vinti += 1;
                                ritentati += 1;
                                "vinto-ritentato"
                            }
                            Ok(_) => "perso",
                            Err(_) => "perso-illeggibile",
                        }
                    }
                    Err(e) => {
                        eprintln!("round {n}: {e}");
                        "errore"
                    }
                };
                righe.push_str(&format!("{n} {esito}\n"));
            }
            let out = pb(&dir, &["gara", &run, "esiti", &format!("{me}.txt")]);
            scrivi_testo(&out, &righe).unwrap_or_else(|e| fail(&format!("scrivo esiti: {e}")));
            (vinti, ritentati)
        }));
    }
    let (mut v, mut r) = (0, 0);
    for h in hs {
        let (a, b) = h.join().unwrap();
        v += a;
        r += b;
    }
    println!("{}", json(&[("run", js(&run)), ("id", js(&id)), ("vinti", v.to_string()), ("ritentati", r.to_string())]));
}

/// Ritorna true se la gara è pulita.
pub fn verifica(a: &Args) -> bool {
    let dir = a.req("dir");
    let run = a.req("run");
    let round = a.u("round", 1000);
    let esiti_dir = pb(&dir, &["gara", &run, "esiti"]);
    let mut vincitori: HashMap<u64, Vec<String>> = HashMap::new();
    let mut concorrenti = 0;
    let mut errori = 0u64;
    let mut ritentati = 0u64;
    let entries = fs::read_dir(&esiti_dir).unwrap_or_else(|e| fail(&format!("{esiti_dir:?}: {e}")));
    for e in entries {
        let e = e.unwrap();
        let nome = e.file_name().to_string_lossy().trim_end_matches(".txt").to_string();
        if nome.ends_with(".tmp") {
            continue;
        }
        concorrenti += 1;
        let s = fs::read_to_string(e.path()).unwrap();
        for riga in s.lines() {
            let mut it = riga.split_whitespace();
            let n: u64 = it.next().unwrap().parse().unwrap();
            match it.next().unwrap() {
                "vinto" => vincitori.entry(n).or_default().push(nome.clone()),
                "vinto-ritentato" => {
                    ritentati += 1;
                    vincitori.entry(n).or_default().push(nome.clone())
                }
                "perso" => {}
                _ => errori += 1,
            }
        }
    }
    let (mut zero, mut doppi, mut diversi) = (0u64, 0u64, 0u64);
    for n in 1..=round {
        let v = vincitori.get(&n).cloned().unwrap_or_default();
        let dentro = fs::read_to_string(pb(&dir, &["gara", &run, "turni", &n.to_string()])).unwrap_or_default();
        match v.len() {
            0 => zero += 1,
            1 => {
                if v[0] != dentro {
                    diversi += 1;
                }
            }
            _ => doppi += 1,
        }
    }
    let ok = zero == 0 && doppi == 0 && diversi == 0 && errori == 0;
    println!(
        "{}",
        json(&[
            ("banco", js("9-gara")),
            ("run", js(&run)),
            ("concorrenti", concorrenti.to_string()),
            ("round", round.to_string()),
            ("round_senza_vincitore", zero.to_string()),
            ("round_con_due_vincitori", doppi.to_string()),
            ("vincitore_diverso_dal_file", diversi.to_string()),
            ("vinti_dopo_ritentativo", ritentati.to_string()),
            ("errori", errori.to_string()),
            ("esito", js(if ok { "PASSA" } else { "MUORE" })),
        ])
    );
    ok
}
