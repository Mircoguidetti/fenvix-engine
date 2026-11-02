//! M15 · bancone piccolo. Simula su una traccia vera: bancone (cache del cameriere) di taglia fissa, ripiano su
//! S3 Express One Zone, magazzino S3 Standard, richiesta doppia, anticipo e lettura più larga.
//! Uscita: una riga JSON per configurazione.

mod cache;
mod lat;
mod trace;

use cache::{fxmap, FxMap, Policy};
use lat::{Hist, Rng, Scenario, Tier};
use std::fmt::Write as _;
use std::io::Write;
use trace::{Op, Rec};

#[derive(Clone, Copy)]
struct Io {
    t: f64,
    op: Op,
    first: u64,
    n: u32,    // pagine da 4 KB (dischi) o 1 (chiave-valore)
    size: u32, // byte per pagina o per chiave
    client: u32,
}

struct Trace {
    name: String,
    block: bool,
    ios: Vec<Io>,
    foot_units: u64,
    foot_bytes: u64,
    read_foot_bytes: u64,
    info: String,
}

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).and_then(|i| a.get(i + 1).cloned())
}

/// Tempo frazionario: dentro un secondo le richieste si spargono uniformi.
fn spread_times(ios: &mut [Io]) {
    let mut i = 0;
    while i < ios.len() {
        let s = ios[i].t;
        let mut j = i;
        while j < ios.len() && ios[j].t == s {
            j += 1;
        }
        let n = (j - i) as f64;
        for (k, io) in ios[i..j].iter_mut().enumerate() {
            io.t = s + k as f64 / n;
        }
        i = j;
    }
}

fn build_block(recs: &[Rec]) -> Vec<Io> {
    let mut ios: Vec<Io> = Vec::with_capacity(recs.len() / 2);
    for r in recs {
        if let Some(last) = ios.last_mut() {
            if last.t == r.t as f64 && last.op == r.op && last.first + last.n as u64 == r.id && last.client == r.client {
                last.n += 1;
                continue;
            }
        }
        ios.push(Io { t: r.t as f64, op: r.op, first: r.id, n: 1, size: 4096, client: 0 });
    }
    ios
}

/// Chiave-valore: taglia di ogni chiave (la più grande vista), poi la cache dell'app da 60 s (come M9).
fn build_kv(recs: &[Rec], app_ttl: f64) -> (Vec<Io>, u64, u64) {
    let mut sz: FxMap<u64, u32> = fxmap();
    for r in recs {
        let e = sz.entry(r.id).or_insert(0);
        *e = (*e).max(r.size);
    }
    let mut known: Vec<u32> = sz.values().copied().filter(|&s| s > 0).collect();
    known.sort_unstable();
    let median = if known.is_empty() { 1024 } else { known[known.len() / 2] };
    let mut app: FxMap<u64, f64> = fxmap();
    let mut ios = Vec::with_capacity(recs.len() / 2);
    let (mut reads_in, mut reads_absorbed) = (0u64, 0u64);
    for r in recs {
        let s = match sz[&r.id] {
            0 => median,
            x => x,
        };
        let t = r.t as f64;
        if r.op == Op::Read {
            reads_in += 1;
            if let Some(&f) = app.get(&r.id) {
                if t - f < app_ttl {
                    reads_absorbed += 1;
                    continue;
                }
            }
            app.insert(r.id, t);
        }
        ios.push(Io { t, op: r.op, first: r.id, n: 1, size: s, client: r.client });
    }
    (ios, reads_in, reads_absorbed)
}

fn footprint(ios: &[Io], block: bool) -> (u64, u64, u64) {
    let mut seen: FxMap<u64, bool> = fxmap();
    let (mut units, mut bytes, mut rbytes) = (0u64, 0u64, 0u64);
    for io in ios {
        if io.op == Op::Delete {
            continue;
        }
        for k in 0..io.n as u64 {
            let id = io.first + k;
            match seen.get_mut(&id) {
                None => {
                    seen.insert(id, io.op == Op::Read);
                    units += 1;
                    bytes += io.size as u64;
                    if io.op == Op::Read {
                        rbytes += io.size as u64;
                    }
                }
                Some(r) => {
                    if !*r && io.op == Op::Read {
                        *r = true;
                        rbytes += io.size as u64;
                    }
                }
            }
        }
    }
    let _ = block;
    (units, bytes, rbytes)
}

#[derive(Clone, Copy, PartialEq)]
enum Pf {
    None,
    Seq,
    Corr,
    Both,
}

#[derive(Clone)]
struct Cfg {
    policy: &'static str,
    b: f64,
    e: f64,
    full: bool, // ripiano Express con la copia piena di tutti i dati (riempito prima)
    pf: Pf,
    w: u32,
}

/// Una lettura misurata che non è stata servita tutta dal bancone.
#[derive(Clone, Copy, Default)]
struct Miss {
    n_ex: u8,
    n_s3: u8,
    ex_bytes: u32,
    s3_bytes: u32,
    late: f32,
}

#[derive(Default)]
struct Out {
    reads: u64,
    hits: u64,
    misses: Vec<Miss>,
    units_read: u64,
    unit_miss: u64,
    reads_s3: u64,
    reads_ex: u64,
    s3_gets: u64,
    s3_bytes: u64,
    ex_gets: u64,
    ex_bytes: u64,
    ex_puts: u64,
    ex_put_bytes: u64,
    pf_gets_s3: u64,
    pf_gets_ex: u64,
    pf_units: u64,
    pf_bytes: u64,
    pf_used: u64,
    pf_wasted: u64,
    pf_late: u64,
    win: Vec<(f64, u64, u64)>, // (tempo di fine, letture, letture con mancata) dall'inizio, senza riscaldamento
    elapsed_s: f64,
}

fn runs(mut v: Vec<u64>) -> Vec<(u64, u64)> {
    v.sort_unstable();
    v.dedup();
    let mut out = vec![];
    for x in v {
        match out.last_mut() {
            Some((a, n)) if *a + *n == x => *n += 1,
            _ => out.push((x, 1)),
        }
    }
    out
}

fn simulate(tr: &Trace, c: &Cfg, warm_frac: f64, nwin: usize) -> Out {
    let t0 = std::time::Instant::now();
    let med = &lat::scenarios()[1];
    let mut rng = Rng::new(42);
    let cap = (c.b * tr.foot_bytes as f64) as u64;
    let mut banc = cache::make(c.policy, cap);
    let mut ex: Option<Box<dyn Policy>> = if c.e > 0.0 { Some(cache::make(c.policy, (c.e * tr.foot_bytes as f64) as u64)) } else { None };
    let upp = if tr.block { (c.w / 4096).max(1) as u64 } else { 1 };
    let mut pfm: FxMap<u64, (f64, bool)> = fxmap();
    let mut streams: [(u64, f64); 16] = [(u64::MAX, -1e9); 16];
    let mut last_miss: FxMap<u32, u64> = fxmap();
    let mut succ: FxMap<u64, u64> = fxmap();
    let mut ev: Vec<u64> = Vec::new();
    let mut o = Out::default();
    let warm = (tr.ios.len() as f64 * warm_frac) as usize;
    let win_len = (tr.ios.len() / nwin).max(1);
    let (mut wr, mut wm) = (0u64, 0u64);
    let mut mis_units: Vec<u64> = Vec::new();
    let mut ex_units: Vec<u64> = Vec::new();
    let mut s3_units: Vec<u64> = Vec::new();
    let mut pf_list: Vec<u64> = Vec::new();

    for (idx, io) in tr.ios.iter().enumerate() {
        let meas = idx >= warm;
        let (u0, u1, usz) = if tr.block {
            let a = io.first / upp;
            let b = (io.first + io.n as u64 - 1) / upp;
            (a, b, (upp * 4096) as u32)
        } else {
            (io.first, io.first, io.size)
        };
        macro_rules! drain_ev {
            () => {
                for id in ev.drain(..) {
                    if let Some((_, used)) = pfm.remove(&id) {
                        if !used && meas {
                            o.pf_wasted += 1;
                        }
                    }
                }
            };
        }
        match io.op {
            Op::Write => {
                if c.full && meas {
                    // la copia piena va tenuta aggiornata: una PUT per scrittura (STIMATO, il lotto reale è più grosso)
                    o.ex_puts += 1;
                    o.ex_put_bytes += (u1 - u0 + 1) * usz as u64;
                }
                for u in u0..=u1 {
                    banc.get(u, usz, &mut ev);
                    if let Some(m) = pfm.get_mut(&u) {
                        m.1 = true; // non più un anticipo in attesa
                    }
                }
                drain_ev!();
            }
            Op::Delete => {
                for u in u0..=u1 {
                    banc.remove(u);
                    pfm.remove(&u);
                }
            }
            Op::Read => {
                let mut late = 0.0f64;
                mis_units.clear();
                ex_units.clear();
                s3_units.clear();
                for u in u0..=u1 {
                    if banc.contains(u) {
                        banc.get(u, usz, &mut ev);
                        if let Some(m) = pfm.get_mut(&u) {
                            if !m.1 {
                                m.1 = true;
                                if meas {
                                    o.pf_used += 1;
                                }
                            }
                            if m.0 > io.t {
                                late = late.max((m.0 - io.t) * 1e3);
                                if meas {
                                    o.pf_late += 1;
                                }
                            }
                        }
                    } else {
                        banc.get(u, usz, &mut ev);
                        pfm.remove(&u);
                        mis_units.push(u);
                        let from_ex = if c.full { true } else { match ex.as_mut() {
                            Some(x) => {
                                let h = x.get(u, usz, &mut Vec::new());
                                if !h && meas {
                                    o.ex_puts += 1;
                                    o.ex_put_bytes += usz as u64;
                                }
                                h
                            }
                            None => false,
                        } };
                        if from_ex {
                            ex_units.push(u)
                        } else {
                            s3_units.push(u)
                        }
                    }
                }
                drain_ev!();
                let n_units = (u1 - u0 + 1) as u64;
                let missed = !mis_units.is_empty();
                // le GET: una per tratto contiguo mancante, per ripiano
                let rex = runs(ex_units.clone());
                let rs3 = runs(s3_units.clone());
                if meas {
                    o.reads += 1;
                    o.units_read += n_units;
                    o.unit_miss += mis_units.len() as u64;
                    if !rs3.is_empty() {
                        o.reads_s3 += 1;
                    } else if !rex.is_empty() {
                        o.reads_ex += 1;
                    }
                    o.s3_gets += rs3.len() as u64;
                    o.ex_gets += rex.len() as u64;
                    o.s3_bytes += rs3.iter().map(|r| r.1).sum::<u64>() * usz as u64;
                    o.ex_bytes += rex.iter().map(|r| r.1).sum::<u64>() * usz as u64;
                    if !missed && late == 0.0 {
                        o.hits += 1;
                    } else {
                        o.misses.push(Miss {
                            n_ex: rex.len().min(255) as u8,
                            n_s3: rs3.len().min(255) as u8,
                            ex_bytes: (rex.iter().map(|r| r.1).max().unwrap_or(0) * usz as u64) as u32,
                            s3_bytes: (rs3.iter().map(|r| r.1).max().unwrap_or(0) * usz as u64) as u32,
                            late: late as f32,
                        });
                    }
                }
                wr += 1;
                if missed {
                    wm += 1;
                }
                // anticipo
                pf_list.clear();
                let mut seq_run = false;
                if (c.pf == Pf::Seq || c.pf == Pf::Both) && tr.block {
                    let mut hit_stream = None;
                    for (k, s) in streams.iter().enumerate() {
                        if s.0 != u64::MAX && (u0 == s.0 + 1 || u0 == s.0) && io.t - s.1 < 2.0 {
                            hit_stream = Some(k);
                            break;
                        }
                    }
                    match hit_stream {
                        Some(k) => {
                            streams[k] = (u1, io.t);
                            let depth = 8u64;
                            for u in u1 + 1..=u1 + depth {
                                if !banc.contains(u) {
                                    pf_list.push(u);
                                }
                            }
                            seq_run = true;
                        }
                        None => {
                            let k = (0..16).min_by(|&a, &b| streams[a].1.partial_cmp(&streams[b].1).unwrap()).unwrap();
                            streams[k] = (u1, io.t);
                        }
                    }
                }
                if (c.pf == Pf::Corr || c.pf == Pf::Both) && missed {
                    let x = mis_units[0];
                    if let Some(prev) = last_miss.insert(io.client, x) {
                        if prev != x {
                            succ.insert(prev, x);
                        }
                    }
                    if let Some(&y) = succ.get(&x) {
                        if !banc.contains(y) && !pf_list.contains(&y) {
                            pf_list.push(y);
                        }
                    }
                }
                if !pf_list.is_empty() {
                    let n_corr_start = if seq_run { pf_list.iter().filter(|&&u| u > u1 && u <= u1 + 8).count() } else { 0 };
                    let mut lat_seq = 0.0f64;
                    let mut src_seq_ex = false;
                    for (k, &y) in pf_list.clone().iter().enumerate() {
                        let in_ex = c.full || ex.as_ref().map(|x| x.contains(y)).unwrap_or(false);
                        let tier: &Tier = if in_ex { &med.ex } else { &med.s3 };
                        if let Some(x) = ex.as_mut() {
                            let h = x.get(y, usz, &mut Vec::new());
                            if !h && meas {
                                o.ex_puts += 1;
                                o.ex_put_bytes += usz as u64;
                            }
                        }
                        banc.get(y, usz, &mut ev);
                        drain_ev!();
                        let l = if k < n_corr_start {
                            // il tratto in avanti arriva con una sola GET
                            if k == 0 {
                                lat_seq = tier.sample(&mut rng, n_corr_start as u64 * usz as u64);
                                src_seq_ex = in_ex;
                                if meas {
                                    if in_ex {
                                        o.pf_gets_ex += 1
                                    } else {
                                        o.pf_gets_s3 += 1
                                    }
                                }
                            }
                            let _ = src_seq_ex;
                            lat_seq
                        } else {
                            if meas {
                                if in_ex {
                                    o.pf_gets_ex += 1
                                } else {
                                    o.pf_gets_s3 += 1
                                }
                            }
                            tier.sample(&mut rng, usz as u64)
                        };
                        pfm.insert(y, (io.t + l / 1e3, false));
                        if meas {
                            o.pf_units += 1;
                            o.pf_bytes += usz as u64;
                        }
                    }
                }
            }
        }
        if (idx + 1) % win_len == 0 {
            o.win.push((io.t, wr, wm));
            wr = 0;
            wm = 0;
        }
    }
    o.elapsed_s = t0.elapsed().as_secs_f64();
    o
}

struct LatRes {
    p50: f64,
    p99: f64,
    p999: f64,
    frac_over_10: f64,
    extra_gets: u64,
}

fn latency(o: &Out, sc: &Scenario, hedge: bool, gp3: bool, seed: u64) -> LatRes {
    let mut h = Hist::new();
    let mut r = Rng::new(seed);
    let g = lat::gp3();
    if gp3 {
        // i successi su gp3: 100.000 campioni, ognuno pesa successi/100.000
        let k = 100_000u64.min(o.hits.max(1));
        let wgt = o.hits / k;
        let rest = o.hits - wgt * k;
        for j in 0..k {
            h.add(g.sample(&mut r, 0), wgt + if j < rest { 1 } else { 0 });
        }
    } else {
        h.add(0.05, o.hits);
    }
    let mut extra = 0u64;
    let mut over = 0u64;
    let get = |t: &Tier, bytes: u64, r: &mut Rng, extra: &mut u64| -> f64 {
        let l1 = t.sample(r, bytes);
        if hedge {
            let d = t.p95(bytes);
            if l1 > d {
                *extra += 1;
                return l1.min(d + t.sample(r, bytes));
            }
        }
        l1
    };
    for m in &o.misses {
        let mut l = m.late as f64;
        for _ in 0..m.n_s3 {
            l = l.max(get(&sc.s3, m.s3_bytes as u64, &mut r, &mut extra));
        }
        for _ in 0..m.n_ex {
            l = l.max(get(&sc.ex, m.ex_bytes as u64, &mut r, &mut extra));
        }
        let base = if gp3 { g.sample(&mut r, 0) } else { 0.05 };
        let l = l.max(base);
        if l > 10.0 {
            over += 1;
        }
        h.add(l, 1);
    }
    LatRes { p50: h.q(0.50), p99: h.q(0.99), p999: h.q(0.999), frac_over_10: over as f64 / h.n.max(1) as f64, extra_gets: extra }
}

fn pfname(p: Pf) -> &'static str {
    match p {
        Pf::None => "no",
        Pf::Seq => "seq",
        Pf::Corr => "corr",
        Pf::Both => "seq+corr",
    }
}

fn report(tr: &Trace, c: &Cfg, o: &Out, combo: &str) -> String {
    let mut s = String::new();
    let rd = o.reads.max(1) as f64;
    write!(
        s,
        "{{\"trace\":\"{}\",\"combo\":\"{}\",\"policy\":\"{}\",\"b\":{},\"e\":{},\"full\":{},\"pf\":\"{}\",\"w\":{},\"reads\":{},\"read_miss\":{:.6},\"unit_miss\":{:.6},\"to_s3\":{:.6},\"to_ex\":{:.6},\"s3_gets_per_read\":{:.6},\"s3_bytes_per_read\":{:.1},\"ex_gets_per_read\":{:.6},\"ex_bytes_per_read\":{:.1},\"ex_puts_per_read\":{:.6},\"ex_put_bytes_per_read\":{:.1},\"pf_gets_s3_per_read\":{:.6},\"pf_gets_ex_per_read\":{:.6},\"pf_bytes_per_read\":{:.1},\"pf_units\":{},\"pf_used\":{},\"pf_wasted\":{},\"pf_late\":{},\"sim_s\":{:.2}",
        tr.name,
        combo,
        c.policy,
        c.b,
        c.e,
        c.full,
        pfname(c.pf),
        c.w,
        o.reads,
        1.0 - o.hits as f64 / rd,
        o.unit_miss as f64 / o.units_read.max(1) as f64,
        o.reads_s3 as f64 / rd,
        o.reads_ex as f64 / rd,
        o.s3_gets as f64 / rd,
        o.s3_bytes as f64 / rd,
        o.ex_gets as f64 / rd,
        o.ex_bytes as f64 / rd,
        o.ex_puts as f64 / rd,
        o.ex_put_bytes as f64 / rd,
        o.pf_gets_s3 as f64 / rd,
        o.pf_gets_ex as f64 / rd,
        o.pf_bytes as f64 / rd,
        o.pf_units,
        o.pf_used,
        o.pf_wasted,
        o.pf_late,
        o.elapsed_s
    )
    .unwrap();
    for (k, sc) in lat::scenarios().iter().enumerate() {
        for hedge in [false, true] {
            let l = latency(o, sc, hedge, false, 7 + k as u64);
            let tag = format!("{}{}", sc.name, if hedge { "_doppia" } else { "" });
            write!(
                s,
                ",\"{t}\":{{\"p50\":{:.3},\"p99\":{:.3},\"p999\":{:.3},\"over10\":{:.6},\"extra_gets_per_read\":{:.6}}}",
                l.p50,
                l.p99,
                l.p999,
                l.frac_over_10,
                l.extra_gets as f64 / rd,
                t = tag
            )
            .unwrap();
        }
    }
    let l = latency(o, &lat::scenarios()[1], true, true, 99);
    write!(s, ",\"medio_doppia_gp3\":{{\"p50\":{:.3},\"p99\":{:.3},\"p999\":{:.3}}}", l.p50, l.p99, l.p999).unwrap();
    s.push('}');
    s
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let kind = arg(&a, "--kind").unwrap_or("lcs".into());
    let mode = arg(&a, "--mode").unwrap_or("block".into());
    let input = arg(&a, "--input").expect("--input");
    let name = arg(&a, "--name").unwrap_or(input.clone());
    let max: usize = arg(&a, "--max").map(|x| x.parse().unwrap()).unwrap_or(usize::MAX);
    let grid = arg(&a, "--grid").unwrap_or("full".into());
    let out_path = arg(&a, "--out").expect("--out");
    let block = mode == "block";
    let rd: Box<dyn std::io::Read> = if input == "-" { Box::new(std::io::stdin()) } else { Box::new(std::fs::File::open(&input).expect("apro la traccia")) };
    let t0 = std::time::Instant::now();
    let (recs, info) = match kind.as_str() {
        "lcs" => {
            let (r, i) = trace::read_lcs(rd, max);
            (r, format!("lcs formato {} richieste nell'intestazione {}", i.format, i.n_req_header))
        }
        "twitter" => (trace::read_twitter(rd, max), "twitter csv".into()),
        "og" => (trace::read_og(rd, max), "oracleGeneral".into()),
        k => panic!("tipo {k}"),
    };
    let n_recs = recs.len();
    let n_read_recs = recs.iter().filter(|r| r.op == Op::Read).count();
    let (t_first, t_last) = (recs.first().map(|r| r.t).unwrap_or(0), recs.last().map(|r| r.t).unwrap_or(0));
    let app_ttl: f64 = arg(&a, "--app-ttl").map(|x| x.parse().unwrap()).unwrap_or(60.0);
    let (mut ios, app_in, app_abs) = if block { (build_block(&recs), 0, 0) } else { build_kv(&recs, app_ttl) };
    drop(recs);
    spread_times(&mut ios);
    let (fu, fb, rfb) = footprint(&ios, block);
    let tr = Trace { name: name.clone(), block, ios, foot_units: fu, foot_bytes: fb, read_foot_bytes: rfb, info };
    let db_reads = tr.ios.iter().filter(|i| i.op == Op::Read).count();
    let db_writes = tr.ios.iter().filter(|i| i.op == Op::Write).count();
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out_path).unwrap();
    writeln!(
        f,
        "{{\"trace\":\"{}\",\"kind\":\"info\",\"mode\":\"{}\",\"records\":{},\"read_records\":{},\"t_first\":{},\"t_last\":{},\"app_reads_in\":{},\"app_absorbed\":{},\"db_ios\":{},\"db_reads\":{},\"db_writes\":{},\"foot_units\":{},\"foot_bytes\":{},\"read_foot_bytes\":{},\"info\":\"{}\",\"load_s\":{:.1}}}",
        name,
        mode,
        n_recs,
        n_read_recs,
        t_first,
        t_last,
        app_in,
        app_abs,
        tr.ios.len(),
        db_reads,
        db_writes,
        tr.foot_units,
        tr.foot_bytes,
        tr.read_foot_bytes,
        tr.info,
        t0.elapsed().as_secs_f64()
    )
    .unwrap();
    if let Some(p) = arg(&a, "--export") {
        // flusso del database pagina per pagina (o chiave per chiave), formato oracleGeneral, per libCacheSim
        let mut w = std::io::BufWriter::new(std::fs::File::create(p).unwrap());
        for io in &tr.ios {
            if io.op == Op::Delete {
                continue;
            }
            for k in 0..io.n as u64 {
                w.write_all(&(io.t as u32).to_le_bytes()).unwrap();
                w.write_all(&(io.first + k).to_le_bytes()).unwrap();
                w.write_all(&io.size.to_le_bytes()).unwrap();
                w.write_all(&(-1i64).to_le_bytes()).unwrap();
            }
        }
    }
    if grid == "info" {
        return;
    }
    let sizes = [0.02, 0.05, 0.10, 0.20, 0.50];
    let mut cfgs: Vec<(String, Cfg)> = vec![];
    let base = |p: &'static str, b: f64| Cfg { policy: p, b, e: 0.0, full: false, pf: Pf::None, w: 4096 };
    for &b in &sizes {
        cfgs.push(("0-lru".into(), base("lru", b)));
        cfgs.push(("0".into(), base("s3fifo", b)));
        if grid == "verifica" {
            continue;
        }
        if grid == "snello" {
            // giro finale con pochi minuti: le combinazioni che decidono
            cfgs.push(("0+1+2 e1".into(), Cfg { e: 1.0, ..base("s3fifo", b) }));
            cfgs.push(("0+1+2 copia-piena".into(), Cfg { full: true, ..base("s3fifo", b) }));
            let pf = if block { Pf::Seq } else { Pf::Corr };
            cfgs.push((format!("0+3 {}", pfname(pf)), Cfg { pf, ..base("s3fifo", b) }));
            cfgs.push((format!("0+1+2+3 {} e0.5", pfname(pf)), Cfg { pf, e: 0.5, ..base("s3fifo", b) }));
            if block {
                for w in [65536u32, 262144] {
                    cfgs.push((format!("4 w{}", w), Cfg { w, ..base("s3fifo", b) }));
                }
            }
            continue;
        }
        for e in [0.2, 0.5, 1.0] {
            cfgs.push((format!("0+1+2 e{}", e), Cfg { e, ..base("s3fifo", b) }));
        }
        cfgs.push(("0+1+2 copia-piena".into(), Cfg { full: true, ..base("s3fifo", b) }));
        let pfs: &[Pf] = if block { &[Pf::Seq, Pf::Corr, Pf::Both] } else { &[Pf::Corr] };
        for &pf in pfs {
            cfgs.push((format!("0+3 {}", pfname(pf)), Cfg { pf, ..base("s3fifo", b) }));
            cfgs.push((format!("0+1+2+3 {} e0.5", pfname(pf)), Cfg { pf, e: 0.5, ..base("s3fifo", b) }));
        }
        if block {
            for w in [65536u32, 262144, 1048576] {
                cfgs.push((format!("4 w{}", w), Cfg { w, ..base("s3fifo", b) }));
                cfgs.push((format!("0+1+2+3+4 w{} e0.5", w), Cfg { w, e: 0.5, pf: Pf::Both, ..base("s3fifo", b) }));
            }
        }
    }
    for (combo, c) in &cfgs {
        let o = simulate(&tr, c, 0.2, 50);
        let line = report(&tr, c, &o, combo);
        writeln!(f, "{}", line).unwrap();
        if (combo == "0" || (combo == "0-lru" && grid != "snello")) && (c.b == 0.10 || c.b == 0.50 || c.b == 0.02) {
            // curva del riscaldamento: dall'avvio a vuoto, senza scartare niente
            let o2 = simulate(&tr, c, 0.0, 100);
            let mut s = format!("{{\"trace\":\"{}\",\"kind\":\"warm\",\"combo\":\"{}\",\"b\":{},\"win\":[", tr.name, combo, c.b);
            for (k, (t, r, m)) in o2.win.iter().enumerate() {
                if k > 0 {
                    s.push(',');
                }
                write!(s, "[{:.1},{},{}]", t, r, m).unwrap();
            }
            s.push_str("]}");
            writeln!(f, "{}", s).unwrap();
        }
        // verifica: tasso di mancata su tutte le operazioni pagina per pagina, come libCacheSim
        if grid == "verifica" {
            let mut p = cache::make(c.policy, (c.b * tr.foot_bytes as f64) as u64);
            let (mut n, mut m) = (0u64, 0u64);
            let mut ev = vec![];
            for io in &tr.ios {
                if io.op == Op::Delete {
                    continue;
                }
                for k in 0..io.n as u64 {
                    n += 1;
                    if !p.get(io.first + k, io.size, &mut ev) {
                        m += 1;
                    }
                    ev.clear();
                }
            }
            writeln!(f, "{{\"trace\":\"{}\",\"kind\":\"verifica\",\"policy\":\"{}\",\"b\":{},\"cap\":{},\"req\":{},\"miss_ratio\":{:.6}}}", tr.name, c.policy, c.b, (c.b * tr.foot_bytes as f64) as u64, n, m as f64 / n.max(1) as f64).unwrap();
        }
        eprintln!("{} {} b={} {:.1}s", tr.name, combo, c.b, o.elapsed_s);
    }
}
