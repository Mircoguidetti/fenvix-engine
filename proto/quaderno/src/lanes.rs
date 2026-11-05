//! Q8 · Corsie per cliente sul quaderno condiviso (Fable 2.5, meccanismo F).
//!
//! In cucina: un quaderno per sala, ma ogni tavolo ha la sua pagina (corsia) e il cameriere di un tavolo non può
//! bloccare la penna. In termini veri:
//!
//! - **una coda per database** (`Lane`), riempita dai thread di rete: i commit di un database rumoroso non stanno mai
//!   davanti a quelli di un altro nella coda unica degli eventi del capo (era il punto debole: un lotto da 50.000
//!   record entrava tutto nel canale del thread principale prima del record del vicino);
//! - **quota per database** (tessera a punti in forma minima): gettoni in record al secondo e in byte al secondo, con
//!   una scorta di `burst_s` secondi; oltre la quota i commit aspettano nella loro corsia;
//! - **corsia piena = freno per quel database solo**: se la corsia supera `max_recs` record, `max_bytes` byte o il
//!   più vecchio aspetta da più di `max_wait_ms`, il commit si rifiuta subito nel thread di rete con `Busy` (motivo
//!   5, quota), errore ritentabile; gli altri database non lo vedono;
//! - **lotto di fdatasync condiviso con un tetto fisso**: il capo fa entrare nel registro al più `cap_bytes` byte non
//!   ancora durevoli sul suo disco; quindi il prossimo giro di fdatasync contiene al più `cap_bytes`, e un record che
//!   arriva aspetta al più il giro in corso più il suo;
//! - **giro equo** (deficit round robin, `quantum` byte per corsia a ogni passata): quando il tetto è stretto, ogni
//!   corsia attiva passa a turno; un database da 100 scritture al secondo ha quasi sempre un record solo in coda ed
//!   entra alla prima passata;
//! - **freno di cella per database**: con il freno di disco o di memoria acceso (livelli 2 e 4) si rifiutano solo i
//!   database "pesanti" (quelli che scrivono di più); tutti solo in emergenza (livello 3, disco quasi pieno, o memoria
//!   oltre il 120% del tetto).

use crate::codec::{CommitReq, Db, Resp};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct LaneCfg {
    /// corsie accese (`false` = comportamento di prima: tutto nel canale unico, nessuna quota)
    pub on: bool,
    /// quota per database: record al secondo e byte al secondo
    pub quota_recs: f64,
    pub quota_bytes: f64,
    /// scorta di gettoni (secondi di quota)
    pub burst_s: f64,
    /// tetto dei byte ammessi nel registro e non ancora durevoli sul disco del capo (il lotto di fdatasync)
    pub cap_bytes: u64,
    /// byte di credito per corsia a ogni passata del giro equo
    pub quantum: u64,
    /// limiti della corsia oltre i quali si rifiuta (solo quel database)
    pub max_recs: usize,
    pub max_bytes: u64,
    pub max_wait_ms: u64,
}

impl LaneCfg {
    pub fn from_env() -> LaneCfg {
        let g = |k: &str, d: f64| std::env::var(k).ok().and_then(|x| x.parse().ok()).unwrap_or(d);
        LaneCfg {
            on: std::env::var("PQ_CORSIE").map(|x| x == "1").unwrap_or(false),
            quota_recs: g("PQ_QUOTA_RECS", 5000.0),
            quota_bytes: g("PQ_QUOTA_MB", 16.0) * 1048576.0,
            burst_s: g("PQ_QUOTA_BURST_S", 1.0),
            cap_bytes: (g("PQ_TETTO_KB", 256.0) * 1024.0) as u64,
            quantum: (g("PQ_QUANTO_KB", 16.0) * 1024.0) as u64,
            max_recs: g("PQ_CORSIA_MAX_REC", 4000.0) as usize,
            max_bytes: (g("PQ_CORSIA_MAX_MB", 8.0) * 1048576.0) as u64,
            max_wait_ms: g("PQ_CORSIA_MAX_MS", 200.0) as u64,
        }
    }
}

/// Byte di un commit per la quota e per il tetto del lotto (chiavi e valori, più un'intestazione fissa).
pub fn commit_bytes(c: &CommitReq) -> u64 {
    64 + c.writes.iter().map(|w| (w.key.len() + w.val.len() + 8) as u64).sum::<u64>()
}

pub struct Item<R> {
    pub t: Instant,
    pub reply: R,
    pub c: CommitReq,
    pub bytes: u64,
}

struct Lane<R> {
    q: VecDeque<Item<R>>,
    bytes: u64,
    tok_r: f64,
    tok_b: f64,
    last: Instant,
    deficit: u64,
    active: bool,
    heavy: bool,
    /// ammessi nel secondo in corso (record, byte)
    sec_r: u64,
    sec_b: u64,
    last_used: Instant,
}

#[derive(Default, Clone, Debug)]
pub struct LaneStats {
    pub pushed: u64,
    pub admitted: u64,
    pub rej_quota: u64,
    pub rej_brake: u64,
    pub rej_all: u64,
    /// giri in cui il tetto ha fermato l'ammissione con record ancora in coda
    pub capped: u64,
    pub queued_recs: u64,
    pub queued_bytes: u64,
    pub active: u64,
    pub lanes: u64,
    pub max_wait_us: u64,
    /// attesa in corsia dei record ammessi, per i database leggeri (sotto 1.000 record/s) e per tutti
    pub wait_light_p99_us: u64,
    pub wait_light_max_us: u64,
    pub wait_all_p99_us: u64,
}

pub struct Lanes<R> {
    pub cfg: LaneCfg,
    map: HashMap<Db, Lane<R>>,
    ring: VecDeque<Db>,
    pub st: LaneStats,
    /// livello del freno di cella (dal thread principale): 2 e 4 rifiutano i pesanti, 9 = emergenza, tutti
    pub brake: u8,
    h_light: hdrhistogram::Histogram<u64>,
    h_all: hdrhistogram::Histogram<u64>,
}

pub enum Push<R> {
    Queued,
    /// rifiutato: si risponde subito con questo esito
    Reject(R, Resp),
}

impl<R> Lanes<R> {
    pub fn new(cfg: LaneCfg) -> Lanes<R> {
        Lanes {
            cfg,
            map: HashMap::new(),
            ring: VecDeque::new(),
            st: LaneStats::default(),
            brake: 0,
            h_light: hdrhistogram::Histogram::new_with_bounds(1, 60_000_000, 2).unwrap(),
            h_all: hdrhistogram::Histogram::new_with_bounds(1, 60_000_000, 2).unwrap(),
        }
    }

    /// Dal thread di rete: mette il commit nella corsia del suo database, o lo rifiuta (solo quel database).
    pub fn push(&mut self, reply: R, c: CommitReq, now: Instant) -> Push<R> {
        self.st.pushed += 1;
        let bytes = commit_bytes(&c);
        let brake = self.brake;
        let cfg = self.cfg.clone();
        if brake >= 9 {
            self.st.rej_all += 1;
            return Push::Reject(reply, Resp::Busy { retry_ms: 200, reason: 4 });
        }
        let db = c.db;
        let st = &mut self.st;
        let ring = &mut self.ring;
        let (qr, qb, bs) = (cfg.quota_recs, cfg.quota_bytes, cfg.burst_s);
        let l = self.map.entry(db).or_insert_with(|| Lane {
            q: VecDeque::new(),
            bytes: 0,
            tok_r: qr * bs,
            tok_b: qb * bs,
            last: now,
            deficit: 0,
            active: false,
            heavy: false,
            sec_r: 0,
            sec_b: 0,
            last_used: now,
        });
        l.last_used = now;
        if l.heavy && (brake == 2 || brake == 4) {
            st.rej_brake += 1;
            return Push::Reject(reply, Resp::Busy { retry_ms: if brake == 4 { 200 } else { 500 }, reason: brake });
        }
        let full = !l.q.is_empty()
            && (l.q.len() >= cfg.max_recs || l.bytes + bytes > cfg.max_bytes || now.duration_since(l.q.front().unwrap().t).as_millis() as u64 > cfg.max_wait_ms);
        if full {
            st.rej_quota += 1;
            // attesa suggerita: quanto serve alla quota per smaltire la corsia (almeno 20 ms)
            let ms = ((l.q.len() as f64 / cfg.quota_recs).max(l.bytes as f64 / cfg.quota_bytes) * 1000.0).clamp(20.0, 1000.0) as u32;
            return Push::Reject(reply, Resp::Busy { retry_ms: ms, reason: 5 });
        }
        l.q.push_back(Item { t: now, reply, c, bytes });
        l.bytes += bytes;
        let activate = !l.active;
        l.active = true;
        st.queued_recs += 1;
        st.queued_bytes += bytes;
        if activate {
            ring.push_back(db);
        }
        Push::Queued
    }

    pub fn queued(&self) -> u64 {
        self.st.queued_recs
    }

    /// Dal thread principale del capo: giro equo fino a `budget` byte (sempre almeno un record se `budget` è pieno e
    /// niente è in volo: un record più grande del tetto passa da solo).
    pub fn pump(&mut self, budget: u64, inflight_zero: bool, out: &mut Vec<Item<R>>) {
        if self.ring.is_empty() {
            return;
        }
        let now = Instant::now();
        let (qr, qb, bs, quantum, cap) = (self.cfg.quota_recs, self.cfg.quota_bytes, self.cfg.burst_s, self.cfg.quantum, self.cfg.cap_bytes);
        let mut taken = 0u64;
        // `alone`: niente in volo e niente preso in questa chiamata: un record più grande del tetto può passare da solo
        let mut alone = inflight_zero;
        'outer: for _pass in 0..64 {
            let n = self.ring.len();
            if n == 0 {
                break;
            }
            let mut progress = false;
            for _ in 0..n {
                let Some(db) = self.ring.pop_front() else { break };
                let l = self.map.get_mut(&db).unwrap();
                let dt = now.duration_since(l.last).as_secs_f64();
                l.last = now;
                l.tok_r = (l.tok_r + qr * dt).min(qr * bs);
                l.tok_b = (l.tok_b + qb * dt).min(qb * bs);
                let fb = l.q.front().map(|f| f.bytes).unwrap_or(0);
                // senza gettoni: la corsia aspetta la ricarica (non prende credito e non perde il posto nel giro)
                if l.tok_r < 1.0 || (l.tok_b < fb as f64 && l.tok_b < qb * bs) {
                    self.ring.push_back(db);
                    continue;
                }
                if taken + fb > budget && !(taken == 0 && alone) {
                    if fb > cap {
                        // record più grande del tetto: aspetta un giro in cui il disco è libero, senza fermare gli altri
                        self.ring.push_back(db);
                        continue;
                    }
                    // tetto pieno: la corsia resta in testa SENZA prendere credito (il suo turno non è passato)
                    self.ring.push_front(db);
                    self.st.capped += 1;
                    break 'outer;
                }
                // il turno della corsia: un quanto di credito (un record grande lo prende tutto, se passa da solo)
                l.deficit += quantum.max(if alone && taken == 0 { fb } else { 0 });
                while let Some(f) = l.q.front() {
                    if f.bytes > l.deficit {
                        break;
                    }
                    if l.tok_r < 1.0 || (l.tok_b < f.bytes as f64 && l.tok_b < qb * bs) {
                        break;
                    }
                    if taken + f.bytes > budget && !(taken == 0 && alone) {
                        self.st.capped += 1;
                        break;
                    }
                    let it = l.q.pop_front().unwrap();
                    l.deficit -= it.bytes;
                    l.bytes -= it.bytes;
                    l.tok_r -= 1.0;
                    l.tok_b -= it.bytes as f64;
                    l.sec_r += 1;
                    l.sec_b += it.bytes;
                    taken += it.bytes;
                    alone = false;
                    progress = true;
                    let w = now.duration_since(it.t).as_micros() as u64;
                    self.h_all.record(w.max(1)).ok();
                    if !l.heavy {
                        self.h_light.record(w.max(1)).ok();
                    }
                    self.st.max_wait_us = self.st.max_wait_us.max(w);
                    self.st.queued_recs -= 1;
                    self.st.queued_bytes -= it.bytes;
                    self.st.admitted += 1;
                    out.push(it);
                }
                if l.q.is_empty() {
                    l.active = false;
                    l.deficit = 0;
                } else {
                    // il credito avanzato resta, ma mai più di un quanto (niente accumulo da giri passati)
                    l.deficit = l.deficit.min(quantum);
                    self.ring.push_back(db);
                }
                if taken >= budget {
                    break 'outer;
                }
            }
            if !progress {
                break;
            }
        }
    }

    /// Tutto quello che è in coda (cambio di guida): il chiamante risponde NotLeader.
    pub fn drain(&mut self) -> Vec<Item<R>> {
        let mut v = vec![];
        for (_, l) in self.map.iter_mut() {
            v.extend(l.q.drain(..));
            l.bytes = 0;
            l.active = false;
            l.deficit = 0;
        }
        self.ring.clear();
        self.st.queued_recs = 0;
        self.st.queued_bytes = 0;
        v
    }

    /// Ogni secondo: chi è pesante (per il freno di cella per database), pulizia delle corsie ferme, statistiche.
    pub fn every_second(&mut self) {
        let tot_r: u64 = self.map.values().map(|l| l.sec_r).sum();
        let tot_b: u64 = self.map.values().map(|l| l.sec_b).sum();
        let heavy_r = (tot_r as f64 * 0.2).max(1000.0);
        let heavy_b = (tot_b as f64 * 0.2).max(2.0 * 1048576.0);
        let now = Instant::now();
        for l in self.map.values_mut() {
            // pesante: tanti record o tanti byte nell'ultimo secondo, oppure una corsia che resta piena
            l.heavy = l.sec_r as f64 >= heavy_r || l.sec_b as f64 >= heavy_b || l.q.len() > 100;
            l.sec_r = 0;
            l.sec_b = 0;
        }
        self.map.retain(|_, l| l.active || now.duration_since(l.last_used).as_secs() < 60);
        self.st.lanes = self.map.len() as u64;
        self.st.active = self.ring.len() as u64;
        self.st.wait_light_p99_us = self.h_light.value_at_quantile(0.99);
        self.st.wait_light_max_us = self.h_light.max();
        self.st.wait_all_p99_us = self.h_all.value_at_quantile(0.99);
    }

    pub fn json(&self) -> String {
        let s = &self.st;
        format!(
            "{{\"accese\":{},\"spinti\":{},\"ammessi\":{},\"rifiuti_quota\":{},\"rifiuti_freno\":{},\"rifiuti_tutti\":{},\"giri_al_tetto\":{},\"in_coda\":{},\"in_coda_byte\":{},\"corsie\":{},\"attive\":{},\"attesa_max_us\":{},\"attesa_leggeri_p99_us\":{},\"attesa_leggeri_max_us\":{},\"attesa_tutti_p99_us\":{},\"freno\":{}}}",
            self.cfg.on, s.pushed, s.admitted, s.rej_quota, s.rej_brake, s.rej_all, s.capped, s.queued_recs, s.queued_bytes, s.lanes, s.active, s.max_wait_us,
            s.wait_light_p99_us, s.wait_light_max_us, s.wait_all_p99_us, self.brake
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Kind, Mode, Write};
    use bytes::Bytes;

    fn c(db: u8, val: usize) -> CommitReq {
        let mut d = [0u8; 16];
        d[0] = db;
        CommitReq { txn: rand::random(), db: d, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key: Bytes::from_static(b"doc/x"), kind: Kind::Put, val: Bytes::from(vec![1u8; val]) }], reads: vec![], session: None }
    }

    fn cfg() -> LaneCfg {
        LaneCfg { on: true, quota_recs: 5000.0, quota_bytes: 16.0 * 1048576.0, burst_s: 1.0, cap_bytes: 256 << 10, quantum: 16 << 10, max_recs: 4000, max_bytes: 8 << 20, max_wait_ms: 200 }
    }

    #[test]
    fn vicino_con_raffica_non_passa_davanti() {
        // B mette 3.000 record in coda, poi A ne mette uno: A entra nel primo giro, B al più fino al tetto
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        for i in 0..3000 {
            assert!(matches!(l.push(i, c(2, 1900), now), Push::Queued));
        }
        assert!(matches!(l.push(99999, c(1, 1900), now), Push::Queued));
        let mut out = vec![];
        l.pump(256 << 10, true, &mut out);
        assert!(out.iter().any(|x| x.reply == 99999), "il record di A deve entrare nel primo giro");
        let bytes: u64 = out.iter().map(|x| x.bytes).sum();
        assert!(bytes <= 256 << 10, "tetto rispettato: {bytes}");
    }

    #[test]
    fn tetto_pieno_non_lascia_il_vicino_in_testa() {
        // B ha tanta coda e riempie il tetto a ogni giro; A (5 database con un record ciascuno) arriva dopo:
        // ogni record di A deve entrare nel primo giro dopo il suo arrivo
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        for i in 0..3000 {
            let _ = l.push(i, c(2, 1900), now);
        }
        let mut out = vec![];
        l.pump(64 << 10, true, &mut out);
        assert!(!out.is_empty());
        for k in 0..5u32 {
            let _ = l.push(100_000 + k, c(10 + k as u8, 1900), now);
        }
        out.clear();
        l.pump(64 << 10, false, &mut out);
        let a = out.iter().filter(|x| x.reply >= 100_000).count();
        assert_eq!(a, 5, "tutti i record di A nel primo giro dopo l'arrivo");
        let bytes: u64 = out.iter().map(|x| x.bytes).sum();
        assert!(bytes <= 64 << 10);
    }

    #[test]
    fn niente_credito_accumulato_a_tetto_pieno() {
        // il tetto resta quasi pieno per 20 giri (bilancio sotto un record); poi arrivano 5 database leggeri e il
        // tetto si libera: i leggeri entrano in questo giro, il vicino non si è accumulato credito
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        for i in 0..3000 {
            let _ = l.push(i, c(2, 1900), now);
        }
        let mut out = vec![];
        for _ in 0..20 {
            l.pump(1000, false, &mut out);
        }
        assert!(out.is_empty());
        for k in 0..5u32 {
            let _ = l.push(100_000 + k, c(10 + k as u8, 1900), now);
        }
        l.pump(64 << 10, false, &mut out);
        let a = out.iter().filter(|x| x.reply >= 100_000).count();
        assert_eq!(a, 5, "i leggeri entrano al primo giro con il tetto libero");
        let b: u64 = out.iter().filter(|x| x.reply < 100_000).map(|x| x.bytes).sum();
        assert!(b <= 64 << 10);
    }

    #[test]
    fn record_grande_non_ferma_gli_altri() {
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        let _ = l.push(1, c(2, 1 << 20), now);
        let _ = l.push(2, c(3, 1900), now);
        let mut out = vec![];
        l.pump(200 << 10, false, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].reply, 2);
    }

    #[test]
    fn corsia_piena_rifiuta_solo_quel_database() {
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        let mut rej = 0;
        for i in 0..5000 {
            if let Push::Reject(_, Resp::Busy { reason: 5, .. }) = l.push(i, c(2, 1900), now) {
                rej += 1;
            }
        }
        assert_eq!(rej, 1000);
        assert!(matches!(l.push(1, c(1, 1900), now), Push::Queued));
    }

    #[test]
    fn quota_in_record_al_secondo() {
        let mut cf = cfg();
        cf.quota_recs = 100.0;
        cf.burst_s = 0.1;
        let mut l: Lanes<u32> = Lanes::new(cf);
        let now = Instant::now();
        for i in 0..50 {
            let _ = l.push(i, c(2, 100), now);
        }
        let mut out = vec![];
        l.pump(1 << 30, true, &mut out);
        assert_eq!(out.len(), 10, "scorta di 0,1 s a 100 record/s");
    }

    #[test]
    fn record_grande_passa_da_solo() {
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        let _ = l.push(1, c(2, 1 << 20), now);
        let mut out = vec![];
        l.pump(256 << 10, false, &mut out);
        assert!(out.is_empty(), "con qualcosa in volo il record da 1 MB aspetta");
        l.pump(256 << 10, true, &mut out);
        assert_eq!(out.len(), 1, "a disco libero passa da solo");
    }

    #[test]
    fn freno_di_cella_solo_ai_pesanti() {
        let mut l: Lanes<u32> = Lanes::new(cfg());
        let now = Instant::now();
        for i in 0..2000 {
            let _ = l.push(i, c(2, 1900), now);
        }
        let _ = l.push(1, c(1, 1900), now);
        let mut out = vec![];
        for _ in 0..20 {
            l.pump(1 << 30, true, &mut out);
        }
        l.every_second();
        l.brake = 4;
        assert!(matches!(l.push(7, c(2, 1900), now), Push::Reject(_, Resp::Busy { reason: 4, .. })));
        assert!(matches!(l.push(8, c(1, 1900), now), Push::Queued));
        l.brake = 9;
        assert!(matches!(l.push(9, c(1, 1900), now), Push::Reject(..)));
    }
}
