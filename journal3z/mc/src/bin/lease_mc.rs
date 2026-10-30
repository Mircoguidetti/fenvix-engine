//! Exhaustive check of the lease takeover rule (design §5.1), on the real code (`journal3z_core::lease`).
//!
//! Discrete time, 1 tick = 0.1 s: lease 100 ticks (10 s), margin 15 ticks (1.5 s), as in production.
//! One holder H and one observer O; every combination of:
//! - clock rates of H and O (monotonic clocks): each of -6 %, 0, +6 % (the bound the margin covers);
//! - when H stops (frozen, or cut from the store: it still believes it holds the lease until its own deadline);
//! - for every renewal H sends (every 33 ticks while running): lost, or landing in the store after 0..=3 ticks;
//! - every tick at which O reads the lease (read or not, every tick);
//! - one jump of O's wall clock by -50, 0 or +50 ticks (±5 s) at any tick (the rule must not care);
//! - O's conditional takeover write landing after 0..=3 ticks (it fails if the lease changed meanwhile).
//! Checked at every takeover: H's own monotonic clock is past the deadline its last successful write allows (no two
//! servers believe they hold the lease). With `--bug` one of the 3 negative controls; each must be found.
//!
//!   lease_mc [--bug none|lease_no_margin|lease_wall_clock|lease_holder_only] [--horizon 400]
use journal3z_core::lease::{holder_valid_until, LeaseBug, LeaseView, Observer, TakeoverRule};
use std::collections::HashSet;

const TTL: u64 = 100;
const MARGIN: u64 = 15;
const RENEW_EVERY: u64 = 33;
const DMAX: u64 = 3;
const JUMP: i64 = 50;

fn clock(t: u64, pct: i64) -> u64 {
    (t as i64 * (100 + pct) / 100) as u64
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct S {
    t: u64,
    h_running: bool,
    h_next_renew: u64,
    /// a renewal in flight: (lands at, sent on H's clock)
    h_inflight: Option<(u64, u64)>,
    h_valid_until: u64,
    version: u64,
    o: Observer,
    /// O's takeover write in flight: (lands at, expected version)
    o_cas: Option<(u64, u64)>,
    jump: Option<i64>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned().unwrap_or(d.to_string());
    let bug = LeaseBug::parse(&get("--bug", "none")).expect("bug");
    let horizon: u64 = get("--horizon", "400").parse().unwrap();
    let rule = TakeoverRule { ttl: TTL, margin: MARGIN, bug };
    let t0 = std::time::Instant::now();
    let mut total_states = 0usize;
    let mut takeovers = 0u64;
    let mut found: Option<String> = None;
    'rates: for ph in [-6i64, 0, 6] {
        for po in [-6i64, 0, 6] {
            // H acquired at t = 0 (sent at 0, landed at 0): version 1, valid until ttl on its clock
            let init = S { t: 0, h_running: true, h_next_renew: RENEW_EVERY, h_inflight: None, h_valid_until: holder_valid_until(&rule, 0), version: 1, o: Observer::default(), o_cas: None, jump: None };
            let mut frontier = vec![init];
            while let Some(_) = frontier.first() {
                let mut next: HashSet<S> = HashSet::new();
                for s in frontier.drain(..) {
                    total_states += 1;
                    if s.t >= horizon {
                        continue;
                    }
                    // nothing can overlap any more: H has stopped, nothing of it is in flight, its deadline is past
                    if !s.h_running && s.h_inflight.is_none() && clock(s.t, ph) >= s.h_valid_until {
                        continue;
                    }
                    // nondeterministic choices for this tick, applied in a fixed order
                    let mut alts = vec![s.clone()];
                    // 1. O's takeover write lands now
                    alts = alts
                        .into_iter()
                        .flat_map(|mut a| {
                            if let Some((at, exp)) = a.o_cas {
                                if at == a.t {
                                    a.o_cas = None;
                                    if a.version == exp {
                                        takeovers += 1;
                                        let h_mono = clock(a.t, ph);
                                        if h_mono < a.h_valid_until && found.is_none() {
                                            found = Some(format!(
                                                "rates H {ph}% O {po}%: O took over at tick {} while H believes until its clock reaches {} (now {h_mono}); H running {}, wall jump {:?}",
                                                a.t, a.h_valid_until, a.h_running, a.jump
                                            ));
                                        }
                                        return vec![]; // taken: this branch ends
                                    }
                                }
                            }
                            vec![a]
                        })
                        .collect();
                    // 2. H's renewal lands (version + 1; H extends from the send time)
                    alts = alts
                        .into_iter()
                        .map(|mut a| {
                            if let Some((at, sent)) = a.h_inflight {
                                if at == a.t {
                                    a.h_inflight = None;
                                    a.version += 1;
                                    a.h_valid_until = a.h_valid_until.max(holder_valid_until(&rule, sent));
                                }
                            }
                            a
                        })
                        .collect();
                    // 3. H stops now (or not), or sends a renewal (lost, or landing after 0..=DMAX ticks)
                    alts = alts
                        .into_iter()
                        .flat_map(|a| {
                            let mut v = vec![];
                            if a.h_running {
                                let mut st = a.clone();
                                st.h_running = false;
                                st.h_inflight = None;
                                v.push(st);
                                if a.t >= a.h_next_renew && a.h_inflight.is_none() {
                                    let sent = clock(a.t, ph);
                                    let mut lost = a.clone();
                                    lost.h_next_renew = a.t + RENEW_EVERY;
                                    v.push(lost);
                                    for d in 0..=DMAX {
                                        let mut x = a.clone();
                                        x.h_next_renew = a.t + RENEW_EVERY;
                                        x.h_inflight = Some((a.t + d, sent));
                                        v.push(x);
                                    }
                                } else {
                                    v.push(a);
                                }
                            } else {
                                v.push(a);
                            }
                            v
                        })
                        .collect();
                    // 4. O's wall clock jumps now (once)
                    alts = alts
                        .into_iter()
                        .flat_map(|a| {
                            if a.jump.is_some() {
                                vec![a]
                            } else {
                                [None, Some(-JUMP), Some(JUMP)].into_iter().map(|j| S { jump: j, ..a.clone() }).collect()
                            }
                        })
                        .collect();
                    // 5. O reads (or not); on "may take", sends the conditional write
                    alts = alts
                        .into_iter()
                        .flat_map(|a| {
                            let mut v = vec![a.clone()];
                            if a.o_cas.is_none() {
                                let mut r = a.clone();
                                let mono = clock(a.t, po);
                                let wall = (mono as i64 + 1000 + a.jump.unwrap_or(0)) as u64;
                                let view = LeaseView { free: false, content: a.version, holder: 7 };
                                if r.o.observe(&rule, &view, mono, wall) {
                                    for d in 0..=DMAX {
                                        let mut x = r.clone();
                                        x.o_cas = Some((a.t + d, a.version));
                                        v.push(x);
                                    }
                                } else {
                                    v.push(r);
                                }
                            }
                            v
                        })
                        .collect();
                    for mut a in alts {
                        a.t += 1;
                        // an in-flight write landing "now" was handled; landings this tick for d = 0 happen next tick
                        next.insert(a);
                    }
                }
                if found.is_some() {
                    break 'rates;
                }
                frontier = next.into_iter().collect();
                if std::env::var("LEASE_MC_PROGRESS").is_ok() && frontier.first().map_or(false, |x| x.t % 25 == 0) {
                    eprintln!("rates {ph} {po} tick {} frontier {} total {total_states} {:.0}s", frontier[0].t, frontier.len(), t0.elapsed().as_secs_f64());
                }
            }
        }
    }
    println!(
        "{{\"checker\": \"lease_mc\", \"bug\": \"{}\", \"ttl_ticks\": {TTL}, \"margin_ticks\": {MARGIN}, \"rates_pct\": [-6, 0, 6], \"horizon_ticks\": {horizon}, \"states\": {total_states}, \"takeovers_checked\": {takeovers}, \"verdict\": \"{}\", \"secs\": {:.1}}}",
        get("--bug", "none"),
        found.clone().map_or("complete: no overlap".to_string(), |f| format!("OVERLAP: {f}")),
        t0.elapsed().as_secs_f64()
    );
}
