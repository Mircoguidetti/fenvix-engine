//! Replays the random draws of `walbench crashtest` (tools/walbench/bench/src/crash.rs) for the quorum backend, exactly
//! in the order the harness makes them, with the parameters the agent passed (tools/walbench/instance/agent.py,
//! `crash_args("quorum", 5)`, plan.json `crash`): seed 5, variants "w0,w1,w5", concs "1,8,64" (default), sizes
//! "200,1024" (default), kill 150..=1500 ms, max_writes 1000, kill_min_acks 20 (default).
//! Output: one line per cycle: cycle, variant, conc, size, kill_ms, read pair. Compare with
//! results-walbench/raw/results/crash-quorum.json (the draws do not depend on what the cycle did).
use rand::{seq::SliceRandom, Rng, SeedableRng};

fn main() {
    let n: u32 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let mut rng = rand::rngs::StdRng::seed_from_u64(5);
    let variants = ["w0", "w1", "w5"];
    let concs = [1usize, 8, 64];
    let sizes = [200usize, 1024];
    let (kill_min_ms, kill_max_ms, max_writes, kill_min_acks) = (150u64, 1500u64, 1000u64, 20u64);
    for c in 0..n {
        let variant = variants.choose(&mut rng).unwrap();
        let conc = concs.choose(&mut rng).unwrap();
        let size = sizes.choose(&mut rng).unwrap();
        let kill_ms = rng.gen_range(kill_min_ms..=kill_max_ms);
        let kill_acks = rng.gen_range(kill_min_acks..max_writes.max(kill_min_acks + 1));
        let pair = ["0,1", "0,2", "1,2"].choose(&mut rng).unwrap();
        println!("{c}\t{variant}\t{conc}\t{size}\t{kill_ms}\t{kill_acks}\t{pair}");
    }
}
