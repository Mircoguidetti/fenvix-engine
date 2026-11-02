//! Il lavaggio a cucitura e, per confronto, il lavaggio che riscrive tutto.
//!
//! Cucitura, in passi:
//! 1. si leggono solo i meta degli ingressi (indice dei blocchi con prima e ultima chiave, filtri per blocco);
//! 2. si trovano le coppie di blocchi di ingressi diversi i cui intervalli di chiavi si toccano (sovrapposti);
//! 3. per ogni coppia almeno uno dei due va scaricato: si sceglie l'insieme da scaricare che pesa meno in byte
//!    (copertura dei vertici pesata, avida: prima i blocchi "radi" che toccano molti blocchi altrui, per esempio i
//!    blocchi pieni di cancellazioni sparse);
//! 4. si scaricano quei blocchi e se ne leggono le chiavi vere; un blocco non scaricato va lavato solo se una di
//!    quelle chiavi cade davvero nel suo intervallo (allora si scarica anche lui);
//! 5. le voci scaricate si fondono come nel lavaggio normale (`lettura::scarico::fondi`) e diventano blocchi nuovi;
//!    i blocchi puliti restano dove sono e si copiano lato server (UploadPartCopy) a tratti di almeno 5 MB;
//! 6. si scrivono indice, filtri e piè di pagina nuovi.
//!
//! Due modi di mettere i pezzi nell'oggetto nuovo:
//! - `Ordine`: i byte seguono l'ordine delle chiavi (come chiede l'idea originale). Ogni tratto copiato e ogni parte
//!   caricata deve essere di almeno 5 MB (regola di S3 per tutte le parti tranne l'ultima): un tratto sporcato da una
//!   sola cancellazione si spezza e i pezzi sotto i 5 MB si scaricano.
//! - `Coda`: i tratti copiati possono contenere blocchi "morti" (quelli lavati, che l'indice nuovo non indica più);
//!   tutti i blocchi nuovi vanno in coda all'oggetto, nell'ultima parte. Si spreca spazio (i blocchi morti) in
//!   cambio di copie lunghe. Un tratto con troppi byte morti (`vivo_min`) si riscrive invece di copiarlo: è il
//!   rimedio alla frammentazione.

use crate::formato::{meta_e_piede, BlockMeta, Blocchi, Meta};
use crate::s3::{Conti, Parte, MIN_PARTE, S3};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use lettura::sst::Entry;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, Serialize)]
pub enum Modo {
    Ordine,
    Coda,
}

#[derive(Clone, Debug, Serialize)]
pub struct Cfg {
    pub modo: Modo,
    pub block_size: usize,
    /// byte morti al più tra due blocchi puliti dello stesso tratto copiato (modo Coda)
    pub buco_max: u64,
    /// frazione minima di byte vivi in un tratto copiato (modo Coda); sotto si riscrive
    pub vivo_min: f64,
    /// lavaggio dell'ultimo livello: le cancellazioni fuse si tolgono
    pub fondo: bool,
    pub par: usize,
    /// due blocchi da scaricare dello stesso ingresso vanno nello stesso GET se tra loro ci sono al più questi byte
    pub buco_get: u64,
    /// se i blocchi da scaricare comunque (copertura) superano questa frazione dei byte, si riscrive tutto
    pub ripiego: f64,
}

impl Default for Cfg {
    fn default() -> Self {
        Cfg { modo: Modo::Coda, block_size: 16 << 10, buco_max: 256 << 10, vivo_min: 0.7, fondo: true, par: 16, buco_get: 64 << 10, ripiego: 0.5 }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Esito {
    /// la cucitura ha rinunciato (troppi blocchi sovrapposti) e ha riscritto tutto
    pub ripiego: bool,
    pub ingressi: usize,
    pub byte_ingressi: u64,
    pub byte_dati_ingressi: u64,
    pub blocchi_ingressi: usize,
    pub coppie_sovrapposte: usize,
    pub blocchi_scaricati_1: usize,
    pub blocchi_intrusi: usize,
    pub blocchi_puliti: usize,
    pub blocchi_puliti_riscritti: usize,
    pub byte_copiati: u64,
    pub byte_copiati_morti: u64,
    pub byte_scaricati_dati: u64,
    pub byte_scaricati_meta: u64,
    pub byte_caricati: u64,
    pub parti_copiate: usize,
    pub parti_caricate: usize,
    pub byte_uscita: u64,
    pub byte_morti_uscita: u64,
    pub blocchi_uscita: usize,
    pub voci_uscita: u64,
    /// frazione dei byte vivi dell'uscita arrivati per copia lato server
    pub frazione_copiata: f64,
    pub ms_meta: u128,
    pub ms_scarico: u128,
    pub ms_fusione: u128,
    pub ms_carico: u128,
    /// CPU del processo per fase: meta+piano, scarico, fusione e blocchi nuovi, carico (meta finale e parti)
    pub cpu_ms_piano: u64,
    pub cpu_ms_scarico: u64,
    pub cpu_ms_fusione: u64,
    pub cpu_ms_carico: u64,
    pub ms_totale: u128,
    pub cpu_ms: u64,
    pub s3: Conti,
}

pub fn cpu_ms() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |tv: libc::timeval| tv.tv_sec as u64 * 1000 + tv.tv_usec as u64 / 1000;
    t(ru.ru_utime) + t(ru.ru_stime)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct G {
    inp: u32,
    j: u32,
}

struct Ingr {
    chiave: String,
    meta: Meta,
}

/// Scarica i blocchi indicati (per ingresso), raggruppando quelli contigui in un GET (al più 16 MB).
async fn scarica_blocchi(s3: &S3, ingr: &[Ingr], quali: &[G], par: usize, buco: u64) -> anyhow::Result<HashMap<(u32, u32), Bytes>> {
    let mut per: HashMap<u32, Vec<u32>> = HashMap::new();
    for g in quali {
        per.entry(g.inp).or_default().push(g.j);
    }
    let mut gruppi: Vec<(u32, Vec<u32>)> = Vec::new();
    for (inp, mut js) in per {
        let bl = &ingr[inp as usize].meta.blocks;
        js.sort_by_key(|j| bl[*j as usize].off);
        js.dedup();
        let mut cur: Vec<u32> = Vec::new();
        for j in js {
            if let Some(&p) = cur.last() {
                let (pb, b) = (&bl[p as usize], &bl[j as usize]);
                let tot = b.end() - bl[cur[0] as usize].off;
                if b.off - pb.end() > buco || tot > 16 << 20 {
                    gruppi.push((inp, std::mem::take(&mut cur)));
                }
            }
            cur.push(j);
        }
        if !cur.is_empty() {
            gruppi.push((inp, cur));
        }
    }
    let res: Vec<Vec<((u32, u32), Bytes)>> = stream::iter(gruppi)
        .map(|(inp, js)| async move {
            let i = &ingr[inp as usize];
            let bl = &i.meta.blocks;
            let da = bl[js[0] as usize].off;
            let a = bl[*js.last().unwrap() as usize].end();
            let b = s3.get_range(&i.chiave, da, a).await?;
            anyhow::Ok(js.iter().map(|j| {
                let m = &bl[*j as usize];
                ((inp, *j), b.slice((m.off - da) as usize..(m.end() - da) as usize))
            }).collect())
        })
        .buffer_unordered(par)
        .try_collect()
        .await?;
    Ok(res.into_iter().flatten().collect())
}

/// Copertura avida pesata: per ogni coppia sovrapposta almeno un blocco in `d`.
fn copertura(pesi: &[u64], adj: &[Vec<u32>]) -> Vec<bool> {
    let n = pesi.len();
    let mut d = vec![false; n];
    let mut deg: Vec<u32> = adj.iter().map(|a| a.len() as u32).collect();
    let ratio = |p: u64, g: u32| p.saturating_mul(1024) / g as u64;
    let mut h: BinaryHeap<Reverse<(u64, u32)>> = (0..n).filter(|&i| deg[i] > 0).map(|i| Reverse((ratio(pesi[i], deg[i]), i as u32))).collect();
    while let Some(Reverse((r, i))) = h.pop() {
        let iu = i as usize;
        if d[iu] || deg[iu] == 0 {
            continue;
        }
        let r2 = ratio(pesi[iu], deg[iu]);
        if r2 != r {
            h.push(Reverse((r2, i)));
            continue;
        }
        d[iu] = true;
        deg[iu] = 0;
        for &nb in &adj[iu] {
            if !d[nb as usize] && deg[nb as usize] > 0 {
                deg[nb as usize] -= 1;
            }
        }
    }
    d
}

enum Logico {
    Pulito(G),
    Nuovo(usize),
}

pub async fn cuci(s3: &S3, chiavi_in: &[String], uscita: &str, cfg: &Cfg) -> anyhow::Result<Esito> {
    let t0 = Instant::now();
    let c0 = cpu_ms();
    let s0 = s3.conti();
    let mut es = Esito { ingressi: chiavi_in.len(), ..Default::default() };

    // 1. meta
    let metas = futures::future::try_join_all(chiavi_in.iter().map(|k| s3.meta(k))).await?;
    let ingr: Vec<Ingr> = chiavi_in.iter().cloned().zip(metas).map(|(chiave, meta)| Ingr { chiave, meta }).collect();
    es.byte_scaricati_meta = s3.conti().meno(s0).get_byte;
    es.ms_meta = t0.elapsed().as_millis();
    for i in &ingr {
        es.byte_ingressi += i.meta.obj_len;
        es.byte_dati_ingressi += i.meta.byte_dati();
        es.blocchi_ingressi += i.meta.blocks.len();
    }

    // 2. coppie sovrapposte
    let mut tutti: Vec<G> = Vec::with_capacity(es.blocchi_ingressi);
    let mut base: Vec<usize> = Vec::new();
    for (i, x) in ingr.iter().enumerate() {
        base.push(tutti.len());
        for j in 0..x.meta.blocks.len() {
            tutti.push(G { inp: i as u32, j: j as u32 });
        }
    }
    let bm = |g: G| -> &BlockMeta { &ingr[g.inp as usize].meta.blocks[g.j as usize] };
    let gid = |g: G| base[g.inp as usize] + g.j as usize;
    let mut ord: Vec<G> = tutti.clone();
    ord.sort_by(|a, b| bm(*a).first.cmp(&bm(*b).first));
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); tutti.len()];
    let mut attivi: Vec<G> = Vec::new();
    for &b in &ord {
        let f = &bm(b).first;
        attivi.retain(|a| &bm(*a).last >= f);
        for &a in &attivi {
            if a.inp != b.inp {
                adj[gid(a)].push(gid(b) as u32);
                adj[gid(b)].push(gid(a) as u32);
                es.coppie_sovrapposte += 1;
            }
        }
        attivi.push(b);
    }

    // 3. cosa scaricare
    let pesi: Vec<u64> = tutti.iter().map(|g| bm(*g).len as u64).collect();
    let d = copertura(&pesi, &adj);
    let primo: Vec<G> = tutti.iter().filter(|g| d[gid(**g)]).cloned().collect();
    es.blocchi_scaricati_1 = primo.len();
    let byte_d: u64 = primo.iter().map(|g| bm(*g).len as u64).sum();
    if byte_d as f64 > cfg.ripiego * es.byte_dati_ingressi as f64 {
        let mut r = riscrivi(s3, chiavi_in, uscita, cfg).await?;
        r.ripiego = true;
        r.blocchi_scaricati_1 = es.blocchi_scaricati_1;
        r.coppie_sovrapposte = es.coppie_sovrapposte;
        r.byte_scaricati_meta = es.byte_scaricati_meta;
        r.ms_meta = es.ms_meta;
        let s = s3.conti().meno(s0);
        r.byte_scaricati_dati = s.get_byte - es.byte_scaricati_meta;
        r.s3 = s;
        r.ms_totale = t0.elapsed().as_millis();
        r.cpu_ms = cpu_ms() - c0;
        return Ok(r);
    }

    es.cpu_ms_piano = cpu_ms() - c0;
    let t1 = Instant::now();
    let c1 = cpu_ms();
    let mut scaricati = scarica_blocchi(s3, &ingr, &primo, cfg.par, cfg.buco_get).await?;
    // 4. chiavi vere: quali blocchi non scaricati sono davvero toccati
    let mut voci: Vec<Entry> = Vec::new();
    for b in scaricati.values() {
        lettura::sst::block_entries(b, &mut voci)?;
    }
    let mut ks: Vec<&Bytes> = voci.iter().map(|e| &e.key).collect();
    ks.sort();
    let intrusi: Vec<G> = tutti
        .iter()
        .filter(|g| !d[gid(**g)])
        .filter(|g| {
            let m = bm(**g);
            let i = ks.partition_point(|k| **k < m.first);
            i < ks.len() && *ks[i] <= m.last
        })
        .cloned()
        .collect();
    drop(ks);
    es.blocchi_intrusi = intrusi.len();
    let secondo = scarica_blocchi(s3, &ingr, &intrusi, cfg.par, cfg.buco_get).await?;
    for (k, b) in secondo {
        lettura::sst::block_entries(&b, &mut voci)?;
        scaricati.insert(k, b);
    }
    es.ms_scarico = t1.elapsed().as_millis();
    es.cpu_ms_scarico = cpu_ms() - c1;

    // 5. fusione delle voci scaricate e blocchi nuovi tra i blocchi puliti
    let t2 = Instant::now();
    let c2 = cpu_ms();
    let fuse = lettura::scarico::fondi(voci, Some(u64::MAX), cfg.fondo);
    let mut puliti: Vec<G> = tutti.iter().filter(|g| !scaricati.contains_key(&(g.inp, g.j))).cloned().collect();
    puliti.sort_by(|a, b| bm(*a).first.cmp(&bm(*b).first));
    es.blocchi_puliti = puliti.len();
    let mut nuovi = Blocchi::new(cfg.block_size);
    let mut logico: Vec<Logico> = Vec::new();
    let mut fi = 0usize;
    let flush_nuovi = |nuovi: &mut Blocchi, logico: &mut Vec<Logico>, da: usize| {
        nuovi.fine();
        for k in da..nuovi.metas.len() {
            logico.push(Logico::Nuovo(k));
        }
    };
    for &p in &puliti {
        let m = bm(p);
        let da = nuovi.metas.len();
        let mut any = false;
        while fi < fuse.len() && fuse[fi].key < m.first {
            nuovi.push(&fuse[fi]);
            fi += 1;
            any = true;
        }
        if any {
            flush_nuovi(&mut nuovi, &mut logico, da);
        }
        debug_assert!(fi >= fuse.len() || fuse[fi].key > m.last);
        logico.push(Logico::Pulito(p));
    }
    let da = nuovi.metas.len();
    while fi < fuse.len() {
        nuovi.push(&fuse[fi]);
        fi += 1;
    }
    flush_nuovi(&mut nuovi, &mut logico, da);
    drop(fuse);

    // 6. disposizione nell'oggetto e parti
    // per ogni blocco pulito: Some(indice del tratto copiato) oppure None (da riscrivere: si scarica a byte)
    let mut tratto_di: HashMap<(u32, u32), usize> = HashMap::new();
    let mut tratti: Vec<(u32, u64, u64)> = Vec::new(); // (ingresso, da, a)
    match cfg.modo {
        Modo::Coda => {
            let mut per: HashMap<u32, Vec<u32>> = HashMap::new();
            for p in &puliti {
                per.entry(p.inp).or_default().push(p.j);
            }
            let mut inps: Vec<u32> = per.keys().cloned().collect();
            inps.sort();
            for inp in inps {
                let bl = &ingr[inp as usize].meta.blocks;
                let mut js = per.remove(&inp).unwrap();
                js.sort_by_key(|j| bl[*j as usize].off);
                let mut cur: Vec<u32> = Vec::new();
                let mut chiudi = |cur: &mut Vec<u32>| {
                    if cur.is_empty() {
                        return;
                    }
                    let da = bl[cur[0] as usize].off;
                    let a = bl[*cur.last().unwrap() as usize].end();
                    let vivi: u64 = cur.iter().map(|j| bl[*j as usize].len as u64).sum();
                    if a - da >= MIN_PARTE && vivi as f64 >= cfg.vivo_min * (a - da) as f64 {
                        let t = tratti.len();
                        tratti.push((inp, da, a));
                        for j in cur.iter() {
                            tratto_di.insert((inp, *j), t);
                        }
                    }
                    cur.clear();
                };
                for j in js {
                    if let Some(&p) = cur.last() {
                        let (pb, b) = (&bl[p as usize], &bl[j as usize]);
                        if b.off - pb.end() > cfg.buco_max || b.end() - bl[cur[0] as usize].off > (4u64 << 30) {
                            chiudi(&mut cur);
                        }
                    }
                    cur.push(j);
                }
                chiudi(&mut cur);
            }
        }
        Modo::Ordine => {
            // elementi in ordine di chiave: Some(g) copiabile, None fisso (nuovo o riscritto)
            let len_of = |l: &Logico| match l {
                Logico::Pulito(g) => bm(*g).len as u64,
                Logico::Nuovo(k) => nuovi.metas[*k].len as u64,
            };
            let mut copiabile: Vec<bool> = logico.iter().map(|l| matches!(l, Logico::Pulito(_))).collect();
            let contiguo = |a: &Logico, b: &Logico| match (a, b) {
                (Logico::Pulito(x), Logico::Pulito(y)) => x.inp == y.inp && bm(*x).end() == bm(*y).off,
                _ => false,
            };
            loop {
                let mut cambiato = false;
                // tratti copiabili sotto i 5 MB diventano fissi
                let mut i = 0;
                while i < logico.len() {
                    if !copiabile[i] {
                        i += 1;
                        continue;
                    }
                    let mut j = i + 1;
                    let mut tot = len_of(&logico[i]);
                    while j < logico.len() && copiabile[j] && contiguo(&logico[j - 1], &logico[j]) {
                        tot += len_of(&logico[j]);
                        j += 1;
                    }
                    if tot < MIN_PARTE {
                        for c in copiabile.iter_mut().take(j).skip(i) {
                            *c = false;
                        }
                        cambiato = true;
                    }
                    i = j;
                }
                // parti caricate sotto i 5 MB seguite da un tratto copiato: prendono blocchi dal tratto
                let mut i = 0;
                while i < logico.len() {
                    if copiabile[i] {
                        i += 1;
                        continue;
                    }
                    let mut j = i;
                    let mut tot = 0;
                    while j < logico.len() && !copiabile[j] {
                        tot += len_of(&logico[j]);
                        j += 1;
                    }
                    while tot < MIN_PARTE && j < logico.len() {
                        copiabile[j] = false;
                        tot += len_of(&logico[j]);
                        j += 1;
                        cambiato = true;
                        while j < logico.len() && !copiabile[j] {
                            tot += len_of(&logico[j]);
                            j += 1;
                        }
                    }
                    i = j;
                }
                if !cambiato {
                    break;
                }
            }
            // tratti finali
            let mut i = 0;
            while i < logico.len() {
                if !copiabile[i] {
                    i += 1;
                    continue;
                }
                let mut j = i + 1;
                while j < logico.len() && copiabile[j] && contiguo(&logico[j - 1], &logico[j]) {
                    j += 1;
                }
                let (Logico::Pulito(a), Logico::Pulito(z)) = (&logico[i], &logico[j - 1]) else { unreachable!() };
                let t = tratti.len();
                tratti.push((a.inp, bm(*a).off, bm(*z).end()));
                for l in &logico[i..j] {
                    if let Logico::Pulito(g) = l {
                        tratto_di.insert((g.inp, g.j), t);
                    }
                }
                i = j;
            }
        }
    }
    // blocchi puliti da riscrivere (non in un tratto): si scaricano a byte, senza rifonderli
    let da_riscrivere: Vec<G> = puliti.iter().filter(|p| !tratto_di.contains_key(&(p.inp, p.j))).cloned().collect();
    es.blocchi_puliti_riscritti = da_riscrivere.len();
    es.ms_fusione = t2.elapsed().as_millis();
    es.cpu_ms_fusione = cpu_ms() - c2;
    let t3 = Instant::now();
    let c3 = cpu_ms();
    let grezzi = scarica_blocchi(s3, &ingr, &da_riscrivere, cfg.par, cfg.buco_get).await?;
    es.ms_scarico += t3.elapsed().as_millis();

    // offset finali e parti
    es.cpu_ms_scarico += cpu_ms() - c3;
    let t4 = Instant::now();
    let c4 = cpu_ms();
    let mut metas_out: Vec<BlockMeta> = Vec::with_capacity(logico.len());
    let mut parti: Vec<Parte> = Vec::new();
    let mut meta_piede = 0u64;
    match cfg.modo {
        Modo::Coda => {
            let mut inizio_tratto = Vec::with_capacity(tratti.len());
            let mut off = 0u64;
            for (inp, da, a) in &tratti {
                inizio_tratto.push(off);
                parti.push(Parte::Copia { chiave: ingr[*inp as usize].chiave.clone(), da: *da, a: *a });
                off += a - da;
            }
            let t_coda = off;
            let mut coda: Vec<u8> = Vec::with_capacity(nuovi.data.len() + grezzi.values().map(|b| b.len()).sum::<usize>() + (1 << 20));
            coda.extend_from_slice(&nuovi.data);
            let mut pos_grezzo: HashMap<(u32, u32), u64> = HashMap::new();
            let mut gz: Vec<&(u32, u32)> = grezzi.keys().collect();
            gz.sort();
            for k in gz {
                pos_grezzo.insert(*k, t_coda + coda.len() as u64);
                coda.extend_from_slice(&grezzi[k]);
            }
            for l in &logico {
                let mut m = match l {
                    Logico::Nuovo(k) => {
                        let mut m = nuovi.metas[*k].clone();
                        m.off += t_coda;
                        m
                    }
                    Logico::Pulito(g) => {
                        let mut m = bm(*g).clone();
                        m.off = match tratto_di.get(&(g.inp, g.j)) {
                            Some(t) => inizio_tratto[*t] + (m.off - tratti[*t].1),
                            None => pos_grezzo[&(g.inp, g.j)],
                        };
                        m
                    }
                };
                metas_out.push(m);
            }
            let meta_off = t_coda + coda.len() as u64;
            let mp = meta_e_piede(&metas_out, meta_off);
            meta_piede = mp.len() as u64;
            coda.extend_from_slice(&mp);
            spezza_coda(Bytes::from(coda), &mut parti);
        }
        Modo::Ordine => {
            let mut off = 0u64;
            let mut buf: Vec<u8> = Vec::new();
            let mut i = 0;
            while i < logico.len() {
                match &logico[i] {
                    Logico::Pulito(g) if tratto_di.contains_key(&(g.inp, g.j)) => {
                        let t = tratto_di[&(g.inp, g.j)];
                        if !buf.is_empty() {
                            spezza_coda(Bytes::from(std::mem::take(&mut buf)), &mut parti);
                        }
                        let (inp, da, a) = tratti[t];
                        let start = off;
                        while i < logico.len() {
                            let Logico::Pulito(h) = &logico[i] else { break };
                            if tratto_di.get(&(h.inp, h.j)) != Some(&t) {
                                break;
                            }
                            let mut m = bm(*h).clone();
                            m.off = start + (m.off - da);
                            metas_out.push(m);
                            i += 1;
                        }
                        parti.push(Parte::Copia { chiave: ingr[inp as usize].chiave.clone(), da, a });
                        off += a - da;
                        continue;
                    }
                    Logico::Pulito(g) => {
                        let b = &grezzi[&(g.inp, g.j)];
                        let mut m = bm(*g).clone();
                        m.off = off;
                        buf.extend_from_slice(b);
                        off += b.len() as u64;
                        metas_out.push(m);
                    }
                    Logico::Nuovo(k) => {
                        let mut m = nuovi.metas[*k].clone();
                        let b = &nuovi.data[m.off as usize..m.end() as usize];
                        m.off = off;
                        buf.extend_from_slice(b);
                        off += b.len() as u64;
                        metas_out.push(m);
                    }
                }
                i += 1;
            }
            let mp = meta_e_piede(&metas_out, off);
            meta_piede = mp.len() as u64;
            buf.extend_from_slice(&mp);
            spezza_coda(Bytes::from(buf), &mut parti);
        }
    }
    drop(nuovi);
    for p in &parti {
        match p {
            Parte::Copia { da, a, .. } => {
                es.parti_copiate += 1;
                es.byte_copiati += a - da;
            }
            Parte::Byte(b) => {
                es.parti_caricate += 1;
                es.byte_caricati += b.len() as u64;
            }
        }
    }
    // le parti caricate sotto i 5 MB che non sono l'ultima non dovrebbero esserci: le unisce `spezza_coda`
    let parti = unisci_piccole(parti);
    s3.multipart(uscita, parti, cfg.par.min(8)).await?;
    es.ms_carico = t4.elapsed().as_millis();
    es.cpu_ms_carico = cpu_ms() - c4;

    es.blocchi_uscita = metas_out.len();
    es.voci_uscita = metas_out.iter().map(|m| m.voci as u64).sum();
    let dati_out: u64 = metas_out.iter().map(|m| m.len as u64).sum();
    let vivi_in_tratti: u64 = puliti.iter().filter(|p| tratto_di.contains_key(&(p.inp, p.j))).map(|p| bm(*p).len as u64).sum();
    es.byte_copiati_morti = es.byte_copiati - vivi_in_tratti;
    es.byte_uscita = es.byte_copiati + es.byte_caricati;
    es.byte_morti_uscita = es.byte_uscita - dati_out - meta_piede;
    es.frazione_copiata = if dati_out > 0 { vivi_in_tratti as f64 / dati_out as f64 } else { 0.0 };
    let s = s3.conti().meno(s0);
    es.byte_scaricati_dati = s.get_byte - es.byte_scaricati_meta;
    es.s3 = s;
    es.ms_totale = t0.elapsed().as_millis();
    es.cpu_ms = cpu_ms() - c0;
    Ok(es)
}

/// Divide un buffer in parti da 16 MB; l'avanzo va nell'ultima parte del buffer (così ogni parte è >= 5 MB salvo,
/// al più, un buffer intero più piccolo).
fn spezza_coda(b: Bytes, parti: &mut Vec<Parte>) {
    let pezzo = 16usize << 20;
    if b.len() < 2 * pezzo {
        parti.push(Parte::Byte(b));
        return;
    }
    let n = b.len() / pezzo;
    for i in 0..n {
        let a = if i + 1 == n { b.len() } else { (i + 1) * pezzo };
        parti.push(Parte::Byte(b.slice(i * pezzo..a)));
    }
}

/// Parti caricate consecutive sotto i 5 MB si uniscono (succede solo per buffer adiacenti).
fn unisci_piccole(parti: Vec<Parte>) -> Vec<Parte> {
    let mut out: Vec<Parte> = Vec::new();
    for p in parti {
        if let (Some(Parte::Byte(prev)), Parte::Byte(b)) = (out.last_mut(), &p) {
            if (prev.len() as u64) < MIN_PARTE {
                let mut v = prev.to_vec();
                v.extend_from_slice(b);
                *prev = Bytes::from(v);
                continue;
            }
        }
        out.push(p);
    }
    out
}

/// Lavaggio normale: scarica tutto, fonde, riscrive tutto.
pub async fn riscrivi(s3: &S3, chiavi_in: &[String], uscita: &str, cfg: &Cfg) -> anyhow::Result<Esito> {
    let t0 = Instant::now();
    let c0 = cpu_ms();
    let s0 = s3.conti();
    let mut es = Esito { ingressi: chiavi_in.len(), ..Default::default() };
    let objs = futures::future::try_join_all(chiavi_in.iter().map(|k| s3.get_all(k))).await?;
    es.ms_scarico = t0.elapsed().as_millis();
    es.cpu_ms_scarico = cpu_ms() - c0;
    let t2 = Instant::now();
    let c2 = cpu_ms();
    let mut voci = Vec::new();
    for o in &objs {
        let m = Meta::from_object(o)?;
        es.byte_ingressi += m.obj_len;
        es.byte_dati_ingressi += m.byte_dati();
        es.blocchi_ingressi += m.blocks.len();
        voci.extend(crate::formato::tutte_le_voci(o, &m)?);
    }
    let fuse = lettura::scarico::fondi(voci, Some(u64::MAX), cfg.fondo);
    let obj = Bytes::from(crate::formato::costruisci(fuse.iter(), cfg.block_size));
    drop(fuse);
    drop(objs);
    es.ms_fusione = t2.elapsed().as_millis();
    es.cpu_ms_fusione = cpu_ms() - c2;
    let t3 = Instant::now();
    let c3 = cpu_ms();
    es.byte_uscita = obj.len() as u64;
    let m = Meta::from_object(&obj)?;
    es.blocchi_uscita = m.blocks.len();
    es.voci_uscita = m.piede.voci;
    s3.put_grande(uscita, obj).await?;
    es.ms_carico = t3.elapsed().as_millis();
    es.cpu_ms_carico = cpu_ms() - c3;
    let s = s3.conti().meno(s0);
    es.byte_scaricati_dati = s.get_byte;
    es.byte_caricati = s.put_byte;
    es.parti_caricate = s.put as usize;
    es.s3 = s;
    es.ms_totale = t0.elapsed().as_millis();
    es.cpu_ms = cpu_ms() - c0;
    Ok(es)
}
