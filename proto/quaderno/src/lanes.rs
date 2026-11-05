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
    /// riserva garantita per database (record/s e byte/s): i commit dentro la riserva passano sempre per primi
    pub res_recs: f64,
    pub res_bytes: f64,
    /// tetto di cella per quello che sta SOPRA la riserva (byte/s per tutta la cella; 0 = nessun tetto) e quota del
    /// tetto del lotto che può andare a quel traffico (il resto resta libero per i record dentro la riserva)
    pub cell_bytes: f64,
    pub be_frac: f64,
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
            res_recs: g("PQ_RISERVA_RECS", 0.0),
            res_bytes: g("PQ_RISERVA_MB", 1.0) * 1048576.0,
            cell_bytes: g("PQ_CELLA_MBS", 0.0) * 1048576.0,
            be_frac: g("PQ_SOPRA_FRAZ", 0.5),
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
    res_r: f64,
    res_b: f64,
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
    pub admitted_res: u64,
    pub admitted_over: u64,
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
    cell_tok: f64,
    cell_last: Instant,
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
            cell_tok: 0.0,
            cell_last: Instant::now(),
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
        let (qr, qb, bs, rr, rb) = (cfg.quota_recs, cfg.quota_bytes, cfg.burst_s, cfg.res_recs, cfg.res_bytes);
        let l = self.map.entry(db).or_insert_with(|| Lane {
            q: VecDeque::new(),
            bytes: 0,
            tok_r: qr * bs,
            tok_b: qb * bs,
            res_r: rr,
            res_b: rb,
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
        self.pump2(budget, if inflight_zero { 0 } else { 1 }, out)
    }

    /// Ammissione in due passate. Prima la riserva: ogni corsia attiva, a turno, fa entrare i commit coperti dalla
    /// sua riserva garantita (al più un quanto a turno). Poi quello che sta sopra la riserva, con il giro equo, ma solo
    /// finché i byte in volo restano sotto `be_frac` del tetto e c'è credito nel tetto di cella: così un record dentro
    /// la riserva trova sempre posto nel giro di disco successivo. Con la riserva a 0 (predefinito) si salta la
    /// prima passata e tutto è come prima (giro equo e quota).
    pub fn pump2(&mut self, budget: u64, inflight: u64, out: &mut Vec<Item<R>>) {
        if self.ring.is_empty() {
            return;
        }
        let now = Instant::now();
        let c = self.cfg.clone();
        let (qr, qb, bs, quantum, cap) = (c.quota_recs, c.quota_bytes, c.burst_s, c.quantum, c.cap_bytes);
        let mut taken = 0u64;
        // ricarica dei gettoni di tutte le corsie attive e della cella
        for db in self.ring.iter() {
            let l = self.map.get_mut(db).unwrap();
            let dt = now.duration_since(l.last).as_secs_f64();
            l.last = now;
            l.tok_r = (l.tok_r + qr * dt).min(qr * bs);
            l.tok_b = (l.tok_b + qb * dt).min(qb * bs);
            l.res_r = (l.res_r + c.res_recs * dt).min(c.res_recs * bs);
            l.res_b = (l.res_b + c.res_bytes * dt).min(c.res_bytes * bs);
        }
        if c.cell_bytes > 0.0 {
            let dt = now.duration_since(self.cell_last).as_secs_f64();
            self.cell_last = now;
            // scorta di 100 ms del tetto di cella
            self.cell_tok = (self.cell_tok + c.cell_bytes * dt).min(c.cell_bytes * 0.1);
        }
        // ---- passata 1: dentro la riserva
        if c.res_recs > 0.0 {
            let n = self.ring.len();
            for _ in 0..n {
                let Some(db) = self.ring.pop_front() else { break };
                let l = self.map.get_mut(&db).unwrap();
                let mut lane_taken = 0u64;
                while let Some(f) = l.q.front() {
                    if l.res_r < 1.0 || l.res_b < f.bytes as f64 || l.tok_r < 1.0 || l.tok_b < f.bytes as f64 {
                        break;
                    }
                    if lane_taken + f.bytes > quantum && lane_taken > 0 {
                        break;
                    }
                    if taken + f.bytes > budget && !(taken == 0 && inflight == 0) {
                        break;
                    }
                    let it = l.q.pop_front().unwrap();
                    l.res_r -= 1.0;
                    l.res_b -= it.bytes as f64;
                    l.tok_r -= 1.0;
                    l.tok_b -= it.bytes as f64;
                    lane_taken += it.bytes;
                    taken += it.bytes;
                    self.st.admitted_res += 1;
                    Self::admit(&mut self.st, &mut self.h_all, &mut self.h_light, l, it, now, out);
                }
                if l.q.is_empty() {
                    l.active = false;
                    l.deficit = 0;
                } else {
                    self.ring.push_back(db);
                }
            }
        }
        // ---- passata 2: sopra la riserva, giro equo, con il margine del tetto e il tetto di cella
        let be_cap = if c.res_recs > 0.0 { (cap as f64 * c.be_frac) as u64 } else { u64::MAX };
        let mut alone = inflight == 0 && taken == 0;
        'outer: for _pass in 0..64 {
            let n = self.ring.len();
            if n == 0 {
                break;
            }
            let mut progress = false;
            for _ in 0..n {
                let Some(db) = self.ring.pop_front() else { break };
                let l = self.map.get_mut(&db).unwrap();
                let fb = l.q.front().map(|f| f.bytes).unwrap_or(0);
                if l.tok_r < 1.0 || (l.tok_b < fb as f64 && l.tok_b < qb * bs) {
                    self.ring.push_back(db);
                    continue;
                }
                let over_ok = |taken: u64, b: u64, cell_tok: f64| -> bool {
                    (inflight + taken + b <= be_cap || (inflight + taken == 0)) && (c.cell_bytes <= 0.0 || cell_tok >= b as f64 || (cell_tok >= c.cell_bytes * 0.1 - 1.0))
                };
                if taken + fb > budget && !(taken == 0 && alone) {
                    if fb > cap {
                        self.ring.push_back(db);
                        continue;
                    }
                    self.ring.push_front(db);
                    self.st.capped += 1;
                    break 'outer;
                }
                let covered = c.res_recs > 0.0 && l.res_r >= 1.0 && l.res_b >= fb as f64;
                if !covered && !over_ok(taken, fb, self.cell_tok) {
                    // niente margine per il traffico sopra la riserva: si aspetta il prossimo giro di disco
                    self.ring.push_front(db);
                    self.st.capped += 1;
                    break 'outer;
                }
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
                    let covered = c.res_recs > 0.0 && l.res_r >= 1.0 && l.res_b >= f.bytes as f64;
                    if !covered && !over_ok(taken, f.bytes, self.cell_tok) {
                        self.st.capped += 1;
                        break;
                    }
                    let it = l.q.pop_front().unwrap();
                    l.deficit -= it.bytes;
                    l.tok_r -= 1.0;
                    l.tok_b -= it.bytes as f64;
                    if l.res_r >= 1.0 && l.res_b >= it.bytes as f64 && c.res_recs > 0.0 {
                        l.res_r -= 1.0;
                        l.res_b -= it.bytes as f64;
                        self.st.admitted_res += 1;
                    } else {
                        if c.cell_bytes > 0.0 {
                            self.cell_tok -= it.bytes as f64;
                        }
                        self.st.admitted_over += 1;
                    }
                    taken += it.bytes;
                    alone = false;
                    progress = true;
                    Self::admit(&mut self.st, &mut self.h_all, &mut self.h_light, l, it, now, out);
                }
                if l.q.is_empty() {
                    l.active = false;
                    l.deficit = 0;
                } else {
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

    fn admit(st: &mut LaneStats, h_all: &mut hdrhistogram::Histogram<u64>, h_light: &mut hdrhistogram::Histogram<u64>, l: &mut Lane<R>, it: Item<R>, now: Instant, out: &mut Vec<Item<R>>) {
        l.bytes -= it.bytes;
        l.sec_r += 1;
        l.sec_b += it.bytes;
        let w = now.duration_since(it.t).as_micros() as u64;
        h_all.record(w.max(1)).ok();
        if !l.heavy {
            h_light.record(w.max(1)).ok();
        }
        st.max_wait_us = st.max_wait_us.max(w);
        st.queued_recs -= 1;
        st.queued_bytes -= it.bytes;
        st.admitted += 1;
        out.push(it);
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

    pub fn light_q(&self) -> String {
        let h = &self.h_light;
        format!("[{},{},{},{}]", h.value_at_quantile(0.5), h.value_at_quantile(0.99), h.value_at_quantile(0.999), h.max())
    }

    pub fn reset_stats(&mut self) {
        let (q, b) = (self.st.queued_recs, self.st.queued_bytes);
        self.st = LaneStats { queued_recs: q, queued_bytes: b, ..Default::default() };
        self.h_light.reset();
        self.h_all.reset();
    }

    pub fn json(&self) -> String {
        let s = &self.st;
        format!(
            "{{\"accese\":{},\"spinti\":{},\"ammessi\":{},\"rifiuti_quota\":{},\"rifiuti_freno\":{},\"rifiuti_tutti\":{},\"giri_al_tetto\":{},\"in_coda\":{},\"in_coda_byte\":{},\"corsie\":{},\"attive\":{},\"attesa_max_us\":{},\"attesa_leggeri_p99_us\":{},\"attesa_leggeri_max_us\":{},\"attesa_tutti_p99_us\":{},\"ammessi_riserva\":{},\"ammessi_sopra\":{},\"freno\":{}}}",
            self.cfg.on, s.pushed, s.admitted, s.rej_quota, s.rej_brake, s.rej_all, s.capped, s.queued_recs, s.queued_bytes, s.lanes, s.active, s.max_wait_us,
            s.wait_light_p99_us, s.wait_light_max_us, s.wait_all_p99_us, s.admitted_res, s.admitted_over, self.brake
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
        LaneCfg { on: true, quota_recs: 5000.0, quota_bytes: 16.0 * 1048576.0, burst_s: 1.0, cap_bytes: 256 << 10, quantum: 16 << 10, max_recs: 4000, max_bytes: 8 << 20, max_wait_ms: 200, res_recs: 0.0, res_bytes: 1048576.0, cell_bytes: 0.0, be_frac: 0.5 }
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
    fn riserva_prima_del_traffico_sopra() {
        // con la riserva accesa: B ha 3.000 record in coda (sopra la riserva), A ne ha uno. Con metà tetto già in
        // volo il traffico sopra la riserva aspetta, A entra.
        let mut cf = cfg();
        cf.res_recs = 200.0;
        let mut l: Lanes<u32> = Lanes::new(cf);
        let now = Instant::now();
        for i in 0..3000 {
            let _ = l.push(i, c(2, 1900), now);
        }
        let mut out = vec![];
        // B consuma tutta la sua riserva (200 record) e un po' di traffico sopra
        for _ in 0..10 {
            l.pump2(256 << 10, 0, &mut out);
        }
        assert!(out.len() > 200);
        out.clear();
        let _ = l.push(99_999, c(1, 1900), now);
        l.pump2(128 << 10, 130 << 10, &mut out);
        assert_eq!(out.len(), 1, "solo A (dentro la riserva) con il volo sopra metà tetto");
        assert_eq!(out[0].reply, 99_999);
    }

    #[test]
    fn tetto_di_cella_sopra_la_riserva() {
        let mut cf = cfg();
        cf.res_recs = 100.0;
        cf.burst_s = 1.0;
        cf.cell_bytes = 1048576.0; // 1 MB/s per tutto quello sopra la riserva, scorta 100 ms
        let mut l: Lanes<u32> = Lanes::new(cf);
        let now = Instant::now();
        for i in 0..3000 {
            let _ = l.push(i, c(2, 1900), now);
        }
        let mut out = vec![];
        std::thread::sleep(std::time::Duration::from_millis(120));
        l.pump2(64 << 20, 0, &mut out);
        // riserva 100 record + circa 100 KB sopra (scorta di 100 ms di 1 MB/s) = circa 150 record
        assert!(out.len() >= 100 && out.len() < 200, "{}", out.len());
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
