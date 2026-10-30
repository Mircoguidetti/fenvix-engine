//! P2: recovery after restart.
//! 1. `random_equivalence`: realistic random record sequences (the generator drives a real NodeCore), tiny segments,
//!    gc after syncs, clean restarts in the middle; at the end either a clean shutdown (recovered state must equal the
//!    live state) or a crash simulated by cutting the current segment at a random byte after the last sync (recovered
//!    state must equal the state after the synced records plus some prefix of the unsynced ones). Twins with each
//!    store bug must be caught.
//! 2. `kill9_real_process`: a child process writes and is SIGKILLed at a random moment; the recovered state must equal
//!    the fold of a prefix of the generated records at least as long as what the child had reported synced.
use j3z_store::testgen::Gen;
use j3z_store::{RealFs, Store, StoreCfg, TestBugs};
use journal3z_core::{DbLog, DbId, Mutations, NodeCore, Rec};
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;

type Dbs = BTreeMap<DbId, DbLog>;

fn fold(recs: &[Rec]) -> Dbs {
    let mut c = NodeCore::new(1, Mutations::default());
    for r in recs {
        c.apply(r).unwrap();
    }
    c.dbs
}

/// Outcome of one seed: Ok(()) if recovery matched, Err(description) otherwise.
fn one(seed: u64, bugs: &TestBugs) -> Result<(), String> {
    let d = tempfile::tempdir().unwrap();
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed ^ 0xabcdef);
    let cfg = StoreCfg { segment_max: rng.gen_range(200..3000), bugs: bugs.clone(), ..Default::default() };
    let open = |cfg: &StoreCfg| Store::open(d.path(), Arc::new(RealFs), cfg.clone()).map_err(|e| format!("open: {e}"));
    let (mut st, mut live, _) = open(&cfg)?;
    let mut g = Gen::new(seed);
    let mut all: Vec<Rec> = vec![];
    let mut synced_len = 0usize; // records known durable
    for _ in 0..rng.gen_range(20..250) {
        let b = g.next_batch();
        for r in &b {
            live.apply(r).unwrap();
        }
        st.write(&b).unwrap();
        all.extend(b);
        if rng.gen_bool(0.7) {
            st.sync().unwrap();
            synced_len = all.len();
            st.gc(&live).unwrap();
        }
        if rng.gen_bool(0.03) {
            // clean restart in the middle
            st.sync().unwrap();
            synced_len = all.len();
            drop(st);
            let (s2, c2, _) = open(&cfg)?;
            if c2.dbs != live.dbs {
                return Err(format!("seed {seed}: clean restart in the middle differs"));
            }
            st = s2;
            live = c2;
        }
    }
    if rng.gen_bool(0.5) {
        st.sync().unwrap();
        drop(st);
        let (_, c, _) = open(&cfg)?;
        if c.dbs != live.dbs {
            return Err(format!("seed {seed}: clean shutdown: recovered state differs from the live state"));
        }
        return Ok(());
    }
    // crash: cut the current segment at a random byte at or after its synced length
    let seg = st.current_segment();
    let unsynced = all.len() - synced_len;
    drop(st);
    let len = std::fs::metadata(&seg).map(|m| m.len()).unwrap_or(0);
    let unsynced_bytes: u64 = all[synced_len..].iter().map(|r| 8 + j3z_codec::encode_rec(r).len() as u64).sum();
    let synced_bytes = len - unsynced_bytes;
    let cut = rng.gen_range(synced_bytes..=len);
    std::fs::OpenOptions::new().write(true).open(&seg).unwrap().set_len(cut).unwrap();
    let (_, c, _) = open(&cfg)?;
    let ok = (0..=unsynced).any(|k| fold(&all[..synced_len + k]) == c.dbs);
    if !ok {
        return Err(format!("seed {seed}: after a crash the recovered state is not the synced state plus a prefix of the unsynced records"));
    }
    // and a second restart must give the same state
    let (_, c2, _) = open(&cfg)?;
    if c2.dbs != c.dbs {
        return Err(format!("seed {seed}: a second restart after the crash differs"));
    }
    Ok(())
}

const SEEDS: u64 = 2000;

#[test]
fn random_equivalence() {
    let mut fails = vec![];
    for seed in 0..SEEDS {
        if let Err(e) = one(seed, &TestBugs::default()) {
            fails.push(e);
        }
    }
    println!("random_equivalence: {SEEDS} seeds, {} failures", fails.len());
    assert!(fails.is_empty(), "{:?}", &fails[..fails.len().min(5)]);
}

#[test]
fn random_equivalence_catches_each_store_bug() {
    for (name, bugs) in [
        ("no_fence_table", TestBugs { no_fence_table: true, ..Default::default() }),
        ("gc_not_prefix", TestBugs { gc_not_prefix: true, ..Default::default() }),
        ("no_tail_truncate", TestBugs { no_tail_truncate: true, ..Default::default() }),
    ] {
        let caught: Vec<u64> = (0..SEEDS).filter(|s| one(*s, &bugs).is_err()).collect();
        println!("twin {name}: caught in {} of {SEEDS} seeds (first {:?})", caught.len(), caught.first());
        assert!(!caught.is_empty(), "the recovery test does not see the bug {name}");
    }
}

fn kill_run(seed: u64, bug: Option<&str>) -> Result<u64, String> {
    use std::io::{BufRead, BufReader};
    let d = tempfile::tempdir().unwrap();
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let segmax: u64 = rng.gen_range(300..4000);
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_j3z-killchild"));
    cmd.arg(d.path()).arg(seed.to_string()).arg(segmax.to_string());
    if let Some(b) = bug {
        cmd.arg(b);
    }
    let mut child = cmd.stdout(std::process::Stdio::piped()).spawn().unwrap();
    let out = child.stdout.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut last = 0u64;
        for l in BufReader::new(out).lines().map_while(Result::ok) {
            if let Some(n) = l.strip_prefix("S ") {
                last = n.parse().unwrap();
            }
        }
        last
    });
    std::thread::sleep(std::time::Duration::from_millis(rng.gen_range(5..150)));
    unsafe {
        libc_kill(child.id() as i32);
    }
    let _ = child.wait();
    let synced = reader.join().unwrap() as usize;
    let cfg = StoreCfg { segment_max: segmax, ..Default::default() };
    let (_, c, _) = Store::open(d.path(), Arc::new(RealFs), cfg).map_err(|e| format!("seed {seed}: open: {e}"))?;
    // regenerate the same records and find the prefix the disk holds (>= what was reported synced)
    let mut g = Gen::new(seed);
    let mut all: Vec<Rec> = vec![];
    while all.len() < synced + 2000 {
        all.extend(g.next_batch());
    }
    let mut c2 = NodeCore::new(1, Mutations::default());
    for (k, r) in all.iter().enumerate() {
        if k >= synced && c2.dbs == c.dbs {
            return Ok(synced as u64);
        }
        c2.apply(r).unwrap();
    }
    Err(format!("seed {seed}: recovered state is not a prefix of the written records at least {synced} long"))
}

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}
unsafe fn libc_kill(pid: i32) {
    kill(pid, 9);
}

#[test]
fn kill9_real_process() {
    let (mut fails, mut synced_total) = (vec![], 0u64);
    for seed in 0..200 {
        match kill_run(seed, None) {
            Ok(n) => synced_total += n,
            Err(e) => fails.push(e),
        }
    }
    println!("kill9_real_process: 200 kills, {synced_total} records reported synced in total, {} failures", fails.len());
    assert!(fails.is_empty(), "{:?}", &fails[..fails.len().min(5)]);
}

#[test]
fn kill9_catches_store_bugs() {
    for bug in ["gc_not_prefix", "no_fence_table"] {
        let caught = (0..200).filter(|s| kill_run(*s, Some(bug)).is_err()).count();
        println!("kill9 twin {bug}: caught in {caught} of 200 kills");
        assert!(caught > 0, "the kill -9 test does not see the bug {bug}");
    }
}

#[test]
#[ignore]
fn debug_first_failures() {
    let mut n = 0;
    for seed in 0..200 {
        if let Err(e) = one(seed, &TestBugs::default()) {
            println!("{e}");
            n += 1;
            if n > 8 { break; }
        }
    }
}

fn copy_dir(a: &std::path::Path, b: &std::path::Path) {
    std::fs::create_dir_all(b).unwrap();
    for e in std::fs::read_dir(a).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), b.join(e.file_name())).unwrap();
    }
}

#[test]
#[ignore]
fn debug_step_by_step() {
    for seed in 0..50u64 {
        let d = tempfile::tempdir().unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed ^ 0xabcdef);
        let cfg = StoreCfg { segment_max: rng.gen_range(200..3000), ..Default::default() };
        let (mut st, mut live, _) = Store::open(d.path(), Arc::new(RealFs), cfg.clone()).unwrap();
        let mut g = Gen::new(seed);
        let mut hist: Vec<String> = vec![];
        for step in 0..250 {
            let b = g.next_batch();
            for r in &b {
                live.apply(r).unwrap();
            }
            st.write(&b).unwrap();
            hist.push(format!("{step}: {:?}", b.iter().map(|r| match r { Rec::Append { db, e } => format!("A{db}:{}e{}", e.idx, e.epoch), Rec::Trim { db, upto } => format!("T{db}:{upto}"), Rec::Truncate { db, from } => format!("X{db}:{from}"), Rec::Promise { db, epoch } => format!("P{db}:{epoch}"), Rec::Erase { db } => format!("E{db}") }).collect::<Vec<_>>()));
            st.sync().unwrap();
            let del = st.gc(&live).unwrap();
            if del > 0 {
                hist.push(format!("   gc deleted {del}, segments now {}", st.segment_count()));
            }
            let d2 = tempfile::tempdir().unwrap();
            copy_dir(d.path(), d2.path());
            match Store::open(d2.path(), Arc::new(RealFs), cfg.clone()) {
                Ok((_, c, _)) if c.dbs == live.dbs => {}
                other => {
                    println!("seed {seed} step {step}: {:?}", other.map(|_| "state differs").err());
                    for h in hist.iter().rev().take(25).rev() {
                        println!("  {h}");
                    }
                    return;
                }
            }
        }
    }
}
