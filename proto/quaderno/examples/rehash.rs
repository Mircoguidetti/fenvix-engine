//! M16: quanto ferma il thread una mappa a hash che si riorganizza (crescita o pulizia delle lapidi), con il ricambio
//! della finestra (una chiave entra, una più vecchia esce). Uso: rehash VIVE N
use quaderno::window::{IKey, KeyS};
use std::collections::VecDeque;
use std::time::Instant;
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
fn main() {
    let a: Vec<usize> = std::env::args().skip(1).map(|x| x.parse().unwrap()).collect();
    let (live, n) = (a[0], a[1]);
    let mut m: hashbrown::HashMap<IKey, KeyS> = Default::default();
    let mut q: VecDeque<IKey> = VecDeque::new();
    let (mut worst, mut slow, mut tot) = (0u128, 0u64, 0u128);
    for i in 0..n {
        let k = IKey::new(format!("ix/3/parola/{:024x}{i}", rand::random::<u64>()).as_bytes());
        let t = Instant::now();
        m.insert(k.clone(), KeyS::default());
        if q.len() >= live {
            let o = q.pop_front().unwrap();
            m.remove(o.as_slice());
        }
        let d = t.elapsed().as_nanos();
        q.push_back(k);
        if i > live {
            worst = worst.max(d);
            tot += d;
            if d > 1_000_000 {
                slow += 1;
                eprintln!("ferma {} ms alla chiave {i}, capacità {}", d / 1_000_000, m.capacity());
            }
        }
    }
    println!("{{\"vive\":{live},\"n\":{n},\"ns_medi\":{},\"max_ms\":{:.1},\"oltre_1ms\":{slow},\"capacita\":{}}}", tot / (n - live) as u128, worst as f64 / 1e6, m.capacity());
}
