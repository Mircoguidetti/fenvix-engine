//! Accuracy of the delay line: ping-pong 0 -> 1 -> 0, sequential, 20,000 times. Prints the measured round trip.
use j3z_harness::{DelayCfg, DelayLine};
use std::time::Instant;
fn main() {
    let spec = std::env::args().nth(1).unwrap_or("0.5-1.0".into());
    let dl = DelayLine::new(DelayCfg::parse(&spec, 1).unwrap());
    let mut v = Vec::new();
    for _ in 0..20000 {
        let (tx, rx) = std::sync::mpsc::channel();
        let t = Instant::now();
        let d2 = dl.clone();
        dl.send(0, 1, 1024, Box::new(move || d2.send(1, 0, 16, Box::new(move || tx.send(()).unwrap()))));
        rx.recv().unwrap();
        v.push(t.elapsed().as_secs_f64() * 1e3);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean: f64 = v.iter().sum::<f64>() / v.len() as f64;
    println!("delay {spec} ms one way: round trip mean {:.3} p50 {:.3} p99 {:.3} max {:.3} ms (ideal mean {:.3})", mean, v[10000], v[19800], v[19999],
             { let c = DelayCfg::parse(&spec, 1).unwrap(); (c.min_us + c.max_us) as f64 / 1000.0 });
}
