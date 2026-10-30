//! Child process of the kill -9 recovery test (tests/recovery.rs): writes the generator's records for `seed` into a
//! store in `dir`, `fdatasync` + gc after every batch, and prints `S <records>` to stdout after every sync, until it
//! is killed. Usage: killchild <dir> <seed> <segment_max> [bug]
use j3z_store::testgen::Gen;
use j3z_store::{RealFs, Store, StoreCfg};
use std::io::Write;
use std::sync::Arc;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (dir, seed, segmax) = (&a[1], a[2].parse::<u64>().unwrap(), a[3].parse::<u64>().unwrap());
    let mut cfg = StoreCfg { segment_max: segmax, ..Default::default() };
    match a.get(4).map(|s| s.as_str()) {
        Some("gc_not_prefix") => cfg.bugs.gc_not_prefix = true,
        Some("no_fence_table") => cfg.bugs.no_fence_table = true,
        Some(x) => panic!("unknown bug {x}"),
        None => {}
    }
    let (mut st, mut core, _) = Store::open(dir, Arc::new(RealFs), cfg).unwrap();
    let mut g = Gen::new(seed);
    let mut n = 0u64;
    let out = std::io::stdout();
    loop {
        let b = g.next_batch();
        for r in &b {
            core.apply(r).unwrap();
        }
        st.write(&b).unwrap();
        n += b.len() as u64;
        st.sync().unwrap();
        st.gc(&core).unwrap();
        let mut o = out.lock();
        writeln!(o, "S {n}").unwrap();
        o.flush().unwrap();
    }
}
