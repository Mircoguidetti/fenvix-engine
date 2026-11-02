//! M16: la finestra da sola, su un thread (programma del revisore di M12, adattato al giro 6).
//! Chiama le funzioni vere della finestra (`validate`, `apply`, `set_flushed`, `trim_db`) con i record veri, come il
//! thread principale del capo, e misura i µs per record solo a finestra piena. L'indicizzatore (ordine delle chiavi
//! per `scan`) gira su un altro thread, come nel nodo.
//!
//! Uso: taskset -c 0 finestra DBS CHIAVI W N MIX [INDICIZZATORE_CORE]
//!   MIX: 1 = inserimenti (documento + CHIAVI-1 voci d'indice), 2 = miscela B1
//!   W = record in finestra (per esempio 24 s × ritmo), N = record totali
use bytes::Bytes;
use quaderno::bench::{b1_record, db_id, real_record};
use quaderno::codec::*;
use quaderno::window::{Loc, Window};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn thread_cpu_ns() -> u64 {
    let mut t = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) };
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    let a: Vec<u64> = std::env::args().skip(1).map(|x| x.parse().unwrap()).collect();
    let (dbs, keys, w, n, mix) = (a[0], a[1] as usize, a[2], a[3], a[4]);
    let win = Arc::new(Window::new(true, std::path::PathBuf::from("/nonesiste")));
    let stop = Arc::new(AtomicBool::new(false));
    let meas_on = Arc::new(AtomicBool::new(false));
    // un indicizzatore per parte dell'insieme ordinato, come nel nodo
    let idx: Vec<_> = (0..quaderno::window::ORD_P).map(|p| {
        let (win, stop, meas_on) = (win.clone(), stop.clone(), meas_on.clone());
        let core = a.get(5).copied();
        std::thread::spawn(move || {
            if let Some(c) = core {
                unsafe {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    libc::CPU_SET(c as usize, &mut set);
                    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
                }
            }
            let mut ops = 0usize;
            let mut c0: Option<u64> = None;
            while !stop.load(Relaxed) {
                if c0.is_none() && meas_on.load(Relaxed) {
                    c0 = Some(thread_cpu_ns());
                }
                let k = win.index_pending_part(p);
                if c0.is_some() {
                    ops += k;
                }
                if k == 0 {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            // l'ultimo pezzo della coda (dopo lo stop) non si conta: la CPU è quella della finestra di misura
            (ops, thread_cpu_ns() - c0.unwrap_or(0))
        })
    }).collect();
    let dbl: Vec<Db> = (0..dbs).map(db_id).collect();
    let step = 2560u64;
    let (mut tv, mut te, mut ta, mut tf, mut tt) = (0u128, 0u128, 0u128, 0u128, 0u128);
    let (mut measured, mut wkeys) = (0u64, 0u64);
    // la singola aggiunta più lenta e quante oltre 1 ms (una mappa a hash che si riorganizza tutta insieme si vede qui)
    let (mut amax, mut slow) = (0u64, 0u64);
    let far = Instant::now() + Duration::from_secs(3600);
    let mut pending: Vec<(Db, Vec<Write>)> = Vec::with_capacity(step as usize);
    let mut i = 0u64;
    while i < n {
        pending.clear();
        for _ in 0..step {
            let db = dbl[(rand::random::<u64>() % dbs) as usize];
            let ws = if mix == 2 { b1_record(keys) } else { real_record(keys) };
            pending.push((db, ws));
        }
        let meas = i >= w;
        if meas {
            meas_on.store(true, Relaxed);
        }
        for (db, ws) in pending.drain(..) {
            i += 1;
            let pos: Pos = i;
            let t0 = Instant::now();
            // conflitti possibili con la miscela B1 (stesso documento due volte): si contano, il record si aggiunge lo stesso
            let _ = win.validate(&db, pos - 1, &ws, true, &[]);
            let t1 = Instant::now();
            let nk = ws.len() as u64;
            let e = Entry::encode(1, i, pos, Body::Rec { txn: rand::random(), db, session: None, writes: ws });
            let t2 = Instant::now();
            win.apply(&Arc::new(e), Loc::default());
            let t3 = Instant::now();
            if meas {
                let a = (t3 - t2).as_nanos() as u64;
                amax = amax.max(a);
                if a > 1_000_000 {
                    slow += 1;
                }
                tv += (t1 - t0).as_nanos();
                te += (t2 - t1).as_nanos();
                ta += (t3 - t2).as_nanos();
                measured += 1;
                wkeys += nk;
            }
        }
        if i > w {
            let cut = i - w;
            let t0 = Instant::now();
            for db in &dbl {
                win.set_flushed(db, cut, &Bytes::new());
            }
            let t1 = Instant::now();
            for db in &dbl {
                let _ = win.trim_db(db, cut + 1, Pos::MAX, far);
            }
            let t2 = Instant::now();
            if meas {
                tf += (t1 - t0).as_nanos();
                tt += (t2 - t1).as_nanos();
            }
        }
    }
    stop.store(true, Relaxed);
    let r: Vec<(usize, u64)> = idx.into_iter().map(|h| h.join().unwrap()).collect();
    let ord_ops: usize = r.iter().map(|x| x.0).sum();
    // CPU per record: in tutto (somma delle parti) e della parte più carica (il tetto se ogni parte ha il suo core)
    let ix_us = r.iter().map(|x| x.1).sum::<u64>() as f64 / 1000.0 / measured.max(1) as f64;
    let ix_max = r.iter().map(|x| x.1).max().unwrap_or(0) as f64 / 1000.0 / measured.max(1) as f64;
    let m = measured.max(1) as f64 / 1000.0;
    let (v, e, ap, f, t) = (tv as f64 / m / 1e6, te as f64 / m / 1e6, ta as f64 / m / 1e6, tf as f64 / m / 1e6, tt as f64 / m / 1e6);
    let rss = std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap())).unwrap_or(0);
    println!(
        "{{\"dbs\":{dbs},\"keys\":{keys},\"mix\":{mix},\"w\":{w},\"n\":{n},\"chiavi_medie\":{:.2},\"us_valida\":{v:.2},\"us_codifica\":{e:.2},\"us_aggiunta\":{ap:.2},\"us_flushed\":{f:.2},\"us_potatura\":{t:.2},\"us_finestra\":{:.2},\"us_totale\":{:.2},\"ord_ops\":{ord_ops},\"us_indicizzatore_cpu\":{ix_us:.2},\"us_indicizzatore_parte_max\":{ix_max:.2},\"max_us_aggiunta\":{},\"aggiunte_oltre_1ms\":{slow},\"rss_max_mb\":{}}}",
        wkeys as f64 / measured.max(1) as f64,
        v + ap + f + t,
        v + e + ap + f + t,
        amax / 1000,
        rss / 1024
    );
}
