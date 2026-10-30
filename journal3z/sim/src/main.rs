//! journal3z deterministic simulator.
//!
//!   journal3z-sim run --seeds 1-100 [--steps 200000] [--mutation none|<name>] [--only-a] [--double] [--dbs 3]
//!   journal3z-sim controls --seeds 1-200 [--steps 200000]       (every negative control; each must be caught)
//!   journal3z-sim replay --seed 17 [--mutation x] [--trace]
//!   journal3z-sim scenario walbench_cycle1
//!
//! Output: one JSON line per seed (or a summary). A violation prints the seed, step, time and invariant.

mod rng;
mod scenarios;
mod world;

use journal3z_core::Mutations;
use std::time::Instant;
use world::{Params, World};

fn parse_range(s: &str) -> (u64, u64) {
    match s.split_once('-') {
        Some((a, b)) => (a.parse().unwrap(), b.parse().unwrap()),
        None => (s.parse().unwrap(), s.parse().unwrap()),
    }
}

struct Opts {
    seeds: (u64, u64),
    p: Params,
    mutation: String,
    out: String,
}

fn opts(args: &[String]) -> Opts {
    let mut o = Opts { seeds: (1, 10), p: Params::default(), mutation: "none".into(), out: String::new() };
    let mut i = 0;
    while i < args.len() {
        let next = || args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--seeds" | "--seed" => {
                o.seeds = parse_range(&next());
                i += 1
            }
            "--steps" => {
                o.p.max_steps = next().parse().unwrap();
                i += 1
            }
            "--dbs" => {
                o.p.dbs = next().parse().unwrap();
                i += 1
            }
            "--mutation" => {
                o.mutation = next();
                i += 1
            }
            "--out" => {
                o.out = next();
                i += 1
            }
            "--only-a" => o.p.only_a = true,
            "--double" => o.p.double = true,
            "--trace" => o.p.trace = true,
            "--calm" => o.p.calm = true,
            "--fault-every-ms" => {
                let (a, b) = parse_range(&next());
                o.p.fault_every = (a * 1000, b * 1000);
                i += 1
            }
            x => panic!("unknown argument {x}"),
        }
        i += 1;
    }
    o
}

struct SeedResult {
    seed: u64,
    ok: bool,
    w: World,
}

fn run_seed(seed: u64, p: &Params, m: Mutations) -> SeedResult {
    let mut w = World::new(seed, p.clone(), m);
    let ok = w.run();
    SeedResult { seed, ok, w }
}

fn seed_json(r: &SeedResult) -> serde_json::Value {
    let s = &r.w.stats;
    let v = r.w.violation.as_ref().map(|v| serde_json::json!({"inv": v.inv, "step": v.step, "time_us": v.time, "what": v.what}));
    serde_json::json!({"seed": r.seed, "ok": r.ok, "steps": s.steps, "sim_s": r.w.now as f64 / 1e6, "write_attempts": s.write_attempts, "client_writes": s.client_writes, "acks": s.acks,
        "opens": s.opens, "takeovers": s.takeovers, "stale_aborts": s.stale_aborts, "uploads": s.uploads, "reconfigs": s.reconfigs, "sleeps": s.sleeps,
        "moves": s.moves, "late_acks": s.late_acks, "mismatches": s.mismatches_sent, "fenced_replies": s.fenced_replies, "messages": s.messages, "faults": s.faults,
        "healed_at_s": s.healed_at.map(|h| h as f64 / 1e6), "recovered_after_heal_ms": s.recovered_after_heal.iter().map(|(d, t)| (d.to_string(), *t as f64 / 1e3)).collect::<std::collections::BTreeMap<_, _>>(),
        "unprotected_steps": s.unprotected_steps, "lost_in_unprotected": s.lost_in_unprotected, "violation": v})
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).cloned().unwrap_or_else(|| "run".into());
    let rest = &args[2.min(args.len())..];
    match cmd.as_str() {
        "run" | "replay" => {
            let o = opts(rest);
            let m = Mutations::parse(&o.mutation).expect("unknown mutation");
            let t = Instant::now();
            let (mut steps, mut fails, mut acks, mut seeds) = (0u64, 0u64, 0u64, 0u64);
            let mut tot: std::collections::BTreeMap<&str, u64> = Default::default();
            let mut faults: std::collections::BTreeMap<String, u64> = Default::default();
            let (mut not_recovered, mut max_recovery_ms, mut rec_list) = (0u64, 0f64, Vec::<f64>::new());
            let mut lines = vec![];
            for seed in o.seeds.0..=o.seeds.1 {
                let r = run_seed(seed, &o.p, m);
                let j = seed_json(&r);
                println!("{j}");
                if o.p.trace {
                    for l in r.w.trace.iter().rev().take(60).rev() {
                        eprintln!("{l}");
                    }
                }
                steps += r.w.stats.steps;
                acks += r.w.stats.acks;
                seeds += 1;
                fails += (!r.ok) as u64;
                if r.ok {
                    for db in 0..o.p.dbs {
                        match r.w.stats.recovered_after_heal.get(&db) {
                            Some(t) => {
                                let ms = *t as f64 / 1e3;
                                max_recovery_ms = max_recovery_ms.max(ms);
                                rec_list.push(ms);
                            }
                            None => not_recovered += 1,
                        }
                    }
                }
                let st = &r.w.stats;
                for (k, v) in [("write_attempts", st.write_attempts), ("client_writes", st.client_writes), ("opens", st.opens), ("takeovers", st.takeovers), ("stale_aborts", st.stale_aborts),
                    ("late_acks", st.late_acks), ("fenced_replies", st.fenced_replies), ("mismatches", st.mismatches_sent), ("uploads", st.uploads),
                    ("reconfigs", st.reconfigs), ("sleeps", st.sleeps), ("moves", st.moves), ("messages", st.messages),
                    ("unprotected_steps", st.unprotected_steps), ("lost_in_unprotected", st.lost_in_unprotected)] {
                    *tot.entry(k).or_default() += v;
                }
                for (k, v) in &r.w.stats.faults {
                    *faults.entry(k.to_string()).or_default() += v;
                }
                lines.push(j);
            }
            rec_list.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p50 = rec_list.get(rec_list.len() / 2).copied().unwrap_or(f64::NAN);
            let p99 = rec_list.get(rec_list.len() * 99 / 100).copied().unwrap_or(f64::NAN);
            let summary = serde_json::json!({"summary": true, "mutation": o.mutation, "only_a": o.p.only_a, "double": o.p.double, "seeds": seeds, "seeds_failed": fails,
                "steps": steps, "acks": acks, "totals": tot,
                "liveness": {"dbs_not_writing_after_heal": not_recovered, "max_ms_to_first_ack_after_heal": max_recovery_ms,
                    "p50_ms": p50, "p99_ms": p99}, "faults": faults, "wall_s": t.elapsed().as_secs_f64()});
            println!("{summary}");
            if !o.out.is_empty() {
                std::fs::write(&o.out, serde_json::to_string_pretty(&serde_json::json!({"summary": summary, "seeds": lines})).unwrap()).unwrap();
            }
        }
        "controls" => {
            // every negative control: run seeds until the first violation (or the seed range ends)
            let o = opts(rest);
            let mut out = vec![];
            for name in Mutations::ALL {
                let m = Mutations::parse(name).unwrap();
                for only_a in [false, true] {
                    let mut p = o.p.clone();
                    p.only_a = only_a;
                    let t = Instant::now();
                    let (mut caught, mut steps, mut tried) = (None, 0u64, 0u64);
                    for seed in o.seeds.0..=o.seeds.1 {
                        let r = run_seed(seed, &p, m);
                        steps += r.w.stats.steps;
                        tried += 1;
                        if let Some(v) = &r.w.violation {
                            caught = Some(serde_json::json!({"seed": seed, "inv": v.inv, "step": v.step, "what": v.what}));
                            break;
                        }
                    }
                    let j = serde_json::json!({"mutation": name, "checker": if only_a {"(a) durability only"} else {"all invariants"}, "caught": caught.is_some(),
                        "seeds_tried": tried, "steps": steps, "first": caught, "wall_s": t.elapsed().as_secs_f64()});
                    println!("{j}");
                    out.push(j);
                }
            }
            if !o.out.is_empty() {
                std::fs::write(&o.out, serde_json::to_string_pretty(&out).unwrap()).unwrap();
            }
        }
        "scenario" => {
            let name = rest.first().cloned().unwrap_or_default();
            scenarios::run(&name);
        }
        x => panic!("unknown command {x}"),
    }
}
