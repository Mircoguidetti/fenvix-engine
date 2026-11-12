//! The process's memory budget (P-big-speed fix 1): every cache and buffer sized from the memory the process may use,
//! as mongod sizes WiredTiger's cache from the machine or its cgroup.
//!
//! Before, the server's caches had fixed sizes (block 256 MiB, metadata 64 MiB, our runs' decoded blocks 256 MiB, the
//! first course 1 GiB, the counter 50% of `memory.max` of cgroup v2 only): 576 MiB of caches alone. Under a 1 GiB
//! cgroup (the limit mongod and Postgres ran under in P-bench-big) the server was OOM-killed 40% into the load
//! (sessions/2026-11-12-bench-big, MEASURED).
//!
//! The limit is the smallest of: the machine's `MemTotal`, the cgroup v2 `memory.max`, the cgroup v1
//! `memory.limit_in_bytes` (this sandbox and its Docker containers are cgroup v1), and `--memory-limit-mb` when given.
//!
//! The split ([`plan`]), with L the limit:
//! - caches together: max((L - 1 GiB) / 2, L / 4), WiredTiger's rule (50% of RAM minus 1 GiB, at least 256 MB) with the
//!   floor at a quarter so a small machine still caches: 256 MiB at 1 GiB, 512 MiB at 2 GiB, 3.5 GiB at 8 GiB;
//!   - of them, SlateDB's block cache 45%, its metadata cache (SST indexes and filters) 15%, our runs' decoded blocks
//!     40% (the old fixed sizes were 44 / 11 / 44%); with the waiter's counter (`--waiter-tools`) the counter takes the
//!     block and metadata shares (60%);
//!   - the per-customer box cache (fix 3, `CustomerBox`) takes its bytes from the same 60% (it is a counter of blocks);
//! - the memtable (the size at which a database's writes are flushed to an L0 SST): 8 MiB, or L / 64 if smaller;
//!   SlateDB holds at most 4 of them per database before writes wait (`lifecycle::writer_settings`);
//! - the first course (opening.rs): L / 16;
//! - the change-stream event log: L / 32, at most 32 MiB;
//! - L0 SSTs a compactor's start reads at once (each whole in memory): L / 16 divided by 4 memtables, 1 to 4.
//!
//! What is left (at 1 GiB: about 600 MiB) is for what is not a cache: the memtables, the compactor's jobs (a run
//! writer's 16 MiB part buffer per job, its decoded input blocks), the connections' buffers, the allocator's own
//! overhead, and the cgroup's page cache, which the kernel reclaims first.
//!
//! An explicit flag (`--block-cache-mb`, `--sealed-cache-mb`, ...) still wins over the budget.

use std::sync::OnceLock;

/// The sizes of one plan, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Budget {
    /// The memory limit the plan was made for.
    pub limit: u64,
    /// Where the limit came from: "memtotal", "cgroup2", "cgroup1", "flag".
    pub source: &'static str,
    pub caches: u64,
    pub block_cache: u64,
    pub meta_cache: u64,
    pub sealed_cache: u64,
    /// RAM of the waiter's counter when `--waiter-tools` is on (instead of the block and metadata caches).
    pub counter_ram: u64,
    pub memtable: u64,
    pub first_course: u64,
    pub change_log: u64,
    pub l0_read_parallel: usize,
}

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// The plan for a limit of `limit` bytes (pure: tested below).
pub fn plan(limit: u64, source: &'static str) -> Budget {
    let limit = limit.max(64 * MIB);
    let caches = (limit.saturating_sub(GIB) / 2).max(limit / 4);
    let block_cache = caches / 100 * 45;
    let meta_cache = caches / 100 * 15;
    let sealed_cache = caches - block_cache - meta_cache;
    let memtable = (8 * MIB).min(limit / 64).max(MIB);
    let l0_read_parallel = ((limit / 16) / (memtable * 4)).clamp(1, 4) as usize;
    Budget {
        limit,
        source,
        caches,
        block_cache,
        meta_cache,
        sealed_cache,
        counter_ram: block_cache + meta_cache,
        memtable,
        first_course: limit / 16,
        change_log: (limit / 32).min(32 * MIB),
        l0_read_parallel,
    }
}

/// The cgroup memory limit in bytes from the text of cgroup v2 `memory.max` or v1 `memory.limit_in_bytes` (`None`:
/// no limit). cgroup v1 writes "no limit" as a number close to i64::MAX rounded to the page size.
pub fn parse_cgroup_limit(text: &str) -> Option<u64> {
    let t = text.trim();
    if t.is_empty() || t == "max" {
        return None;
    }
    let v: u64 = t.parse().ok()?;
    (v < (1u64 << 62)).then_some(v)
}

/// `MemTotal` of a `/proc/meminfo` text, in bytes.
pub fn parse_mem_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// The smallest of the machine's memory and the cgroup limits, from their file contents (pure: tested below).
pub fn limit_from(meminfo: &str, cgroup2_max: &str, cgroup1_limit: &str) -> (u64, &'static str) {
    let mut best = (parse_mem_total(meminfo).unwrap_or(4 * GIB), "memtotal");
    if let Some(v) = parse_cgroup_limit(cgroup2_max) {
        if v < best.0 {
            best = (v, "cgroup2");
        }
    }
    if let Some(v) = parse_cgroup_limit(cgroup1_limit) {
        if v < best.0 {
            best = (v, "cgroup1");
        }
    }
    best
}

/// The limit of this process, read from /proc and /sys.
pub fn detect() -> (u64, &'static str) {
    let read = |p: &str| std::fs::read_to_string(p).unwrap_or_default();
    limit_from(&read("/proc/meminfo"), &read("/sys/fs/cgroup/memory.max"), &read("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
}

static BUDGET: OnceLock<Budget> = OnceLock::new();

/// Fix the process's budget (`limit_mb` > 0: `--memory-limit-mb`). The first call wins.
pub fn install(limit_mb: u64) -> Budget {
    *BUDGET.get_or_init(|| if limit_mb > 0 { plan(limit_mb * MIB, "flag") } else { let (l, s) = detect(); plan(l, s) })
}

/// The installed budget, or the one detected now (tests, tools).
pub fn get() -> Budget {
    *BUDGET.get_or_init(|| {
        let (l, s) = detect();
        plan(l, s)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_gib_fits_like_mongod() {
        let b = plan(GIB, "cgroup1");
        // WiredTiger's rule gives 256 MB at 1 GiB; ours the same
        assert_eq!(b.caches, 256 * MIB);
        assert_eq!(b.block_cache + b.meta_cache + b.sealed_cache, b.caches);
        // the old fixed sizes were 576 MiB of caches: the plan keeps more than half of the limit for everything else
        let fixed = b.caches + b.first_course + b.change_log + 4 * b.memtable + b.l0_read_parallel as u64 * b.memtable;
        assert!(fixed <= GIB / 2, "fixed reservations {fixed} above half of 1 GiB");
        assert_eq!(b.memtable, 8 * MIB);
        assert_eq!(b.l0_read_parallel, 2);
        assert_eq!(b.counter_ram, b.block_cache + b.meta_cache);
    }

    #[test]
    fn grows_with_the_machine_and_never_passes_half() {
        let mut last = 0;
        for gb in [1u64, 2, 4, 8, 16, 64, 256] {
            let b = plan(gb * GIB, "memtotal");
            assert!(b.caches >= last, "caches shrink at {gb} GiB");
            assert!(b.caches <= b.limit / 2 || b.limit <= 2 * GIB, "caches above half at {gb} GiB");
            assert!(b.caches + b.first_course + b.change_log + 4 * b.memtable < b.limit * 3 / 4, "{gb} GiB");
            last = b.caches;
        }
        assert_eq!(plan(8 * GIB, "memtotal").caches, 3584 * MIB);
        // a tiny limit still gets a memtable and one L0 read
        let t = plan(128 * MIB, "flag");
        assert!(t.memtable >= MIB && t.l0_read_parallel == 1 && t.caches == 32 * MIB);
    }

    #[test]
    fn reads_cgroup_v1_and_v2() {
        let mi = "MemTotal:       15728640 kB\nMemFree: 1 kB\n";
        assert_eq!(limit_from(mi, "", ""), (15 * GIB, "memtotal"));
        // cgroup v1 (this sandbox's Docker): the counter used to read only v2 and sized itself from the 15 GB host
        assert_eq!(limit_from(mi, "", "1073741824\n"), (GIB, "cgroup1"));
        assert_eq!(limit_from(mi, "536870912\n", ""), (512 * MIB, "cgroup2"));
        assert_eq!(limit_from(mi, "max\n", "9223372036854771712\n"), (15 * GIB, "memtotal"));
        assert_eq!(limit_from(mi, "2147483648", "1073741824"), (GIB, "cgroup1"));
        // a limit above the machine does not raise it
        assert_eq!(limit_from(mi, "34359738368", ""), (15 * GIB, "memtotal"));
    }
}
