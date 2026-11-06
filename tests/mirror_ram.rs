//! gp3 floor prototype, point 1: the RAM the SST mirror's bookkeeping takes, measured with a counting allocator (every
//! heap byte this test binary holds), not estimated from sizes.
//!
//! The mirror is opened on a directory (indexing one entry per file) and told that one database names all its SSTs
//! (what the maps loop does at each manifest); the heap held after that, minus the heap before, is its bookkeeping.
//!
//! - Default: a synthetic mirror of 20,000 empty SST files (the bookkeeping does not depend on the file sizes).
//! - `MIRROR_RAM_DIR=<dir>`: a real mirror left by `bench/gp3_floor/run.sh` (its `work/mirror`), read only.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};

struct Counting;
static HELD: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            HELD.fetch_add(l.size() as i64, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        HELD.fetch_sub(l.size() as i64, Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, n) };
        if !q.is_null() {
            HELD.fetch_add(n as i64 - l.size() as i64, Relaxed);
        }
        q
    }
}

#[global_allocator]
static A: Counting = Counting;

fn ssts_in(dir: &std::path::Path, out: &mut Vec<(String, [u8; 26], u64)>, base: &std::path::Path) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            ssts_in(&p, out, base);
        } else if let Some((root, s)) = tenant_bench::mirror::parse(p.strip_prefix(base).unwrap().to_str().unwrap()) {
            out.push((root.to_string(), s, e.metadata().unwrap().len()));
        }
    }
}

#[tokio::test]
async fn mirror_bookkeeping_ram() {
    let (dir, synthetic) = match std::env::var("MIRROR_RAM_DIR") {
        Ok(d) if !d.is_empty() => (std::path::PathBuf::from(d), false),
        _ => {
            let d = std::env::temp_dir().join(format!("mirror-ram-{}", std::process::id()));
            let c = d.join("tenants/big/compacted");
            std::fs::create_dir_all(&c).unwrap();
            for _ in 0..20_000 {
                std::fs::File::create(c.join(format!("{}.sst", ulid::Ulid::new()))).unwrap();
            }
            (d, true)
        }
    };
    let mut ssts = Vec::new();
    ssts_in(&dir, &mut ssts, &dir);
    let data: u64 = ssts.iter().map(|x| x.2).sum();
    let refs: Vec<(String, [u8; 26])> = ssts.iter().map(|(r, s, _)| (r.clone(), *s)).collect();
    let n = refs.len() as i64;
    let rt = tokio::runtime::Handle::current();
    let _g = rt.enter();

    let before = HELD.load(Relaxed);
    let m = tenant_bench::mirror::Mirror::open(dir.clone(), true, 1).unwrap();
    m.track("big", 1, refs.clone());
    let after = HELD.load(Relaxed);
    let held = after - before;
    // `refs` (the manifest's list, owned by the caller) was cloned into the mirror's sets, not kept: what is held is
    // the mirror's own tables
    let per = held as f64 / n.max(1) as f64;
    println!(
        "mirror of {n} SSTs, {:.2} GiB: bookkeeping heap {held} bytes MEASURED ({per:.1} bytes per SST); the mirror's own estimate (index_bytes) {}",
        data as f64 / (1u64 << 30) as f64,
        m.index_bytes()
    );
    if data > 0 {
        let per_100gb = held as f64 * (100.0 * (1u64 << 30) as f64) / data as f64;
        println!("  = {:.3} MB per 100 GiB of mirrored data at this data's SST size ({:.1} MiB per SST)", per_100gb / 1e6, data as f64 / n as f64 / (1 << 20) as f64);
    }
    assert!(per < 400.0, "bookkeeping over 400 bytes per SST: {per:.1}");
    assert_eq!(m.usage().0 as i64, n);
    drop(m);
    if synthetic {
        let _ = std::fs::remove_dir_all(&dir);
    }
}
