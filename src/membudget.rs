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
//! - L0 SSTs a compactor's start reads at once (each whole in memory): L / 16 divided by 4 memtables, 1 to 4;
//! - review of big-speed (2026-11-13), the parts that grew with the number of databases or the size of one customer:
//!   - the memtables of every database together (`memgov.rs`): L / 8, at least 16 MiB (128 MiB at 1 GiB); writes wait
//!     past 1.5 times that. Before, only each database was bounded (4 memtables of 8 MiB), and 200 databases written
//!     6 MiB each were OOM-killed under 1 GiB (MEASURED by the reviewer);
//!   - the customer's box (`custbox.rs`): one customer's warm-up keeps at most the runs' cache / 8 (12 MiB at 1 GiB), and
//!     every warm-up of the process together has at most the runs' cache / 4 in flight (25 MiB at 1 GiB). Before, one
//!     warm-up fetched and held a customer's whole range: a 1.5 GiB customer OOM-killed the server under 1 GiB;
//!   - a compaction merge's read-ahead over all its source runs together: L / 32 (32 MiB at 1 GiB), 8 to 512 MiB (before:
//!     4 MiB per source run, and a reseal merges the sealed run with every delta run meeting it);
//!   - L0 SSTs one absorption holds decoded: L / 32 divided by the memtable, 2 to 16 (4 at 1 GiB; before: up to 16);
//!   - every compaction job of every database together (`compaction_permit`): L / 8 (128 MiB at 1 GiB).
//!
//! What is left (at 1 GiB: about 450 MiB under the memtables' hard limit) is for what is not a cache: the compactor's
//! jobs (a run writer's 16 MiB part buffer per job), the connections' buffers, the allocator's own overhead, and the
//! cgroup's page cache, which the kernel reclaims first.
//!
//! An explicit flag (`--block-cache-mb`, `--sealed-cache-mb`, ...) still wins over the budget.

use std::sync::OnceLock;

/// The sizes of one plan, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Budget {
    /// The memory limit the plan was made for.
    pub limit: u64,
    /// Where the limit came from: "memtotal", "cgroup2", "cgroup1" (the mount's root), "cgroup2-own", "cgroup1-own"
    /// (the process's own group or one above it), "flag".
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
    /// Every database's memtables together (`memgov.rs`); writes wait past 1.5 times this.
    pub memtables: u64,
    /// Decoded bytes one customer's box warm-up keeps (`custbox.rs`).
    pub box_customer: u64,
    /// Stored bytes every box warm-up of the process has in flight together.
    pub box_inflight: u64,
    /// Stored bytes a compaction merge (reseal, tier merge) reads ahead over all its source runs together.
    pub merge_read: u64,
    /// L0 SSTs one absorption holds decoded at once.
    pub absorb_l0: usize,
    /// Every compaction job of the process together (absorptions and merges of every database, `compaction_permit`).
    pub compaction: u64,
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
        memtables: (limit / 8).max(16 * MIB),
        box_customer: (sealed_cache / 8).max(MIB),
        box_inflight: (sealed_cache / 4).max(2 * MIB),
        merge_read: (limit / 32).clamp(8 * MIB, 512 * MIB),
        absorb_l0: ((limit / 32) / memtable).clamp(2, 16) as usize,
        compaction: (limit / 8).max(32 * MIB),
    }
}

/// The process-wide allowance of compaction memory (review of big-speed, error 2, "across all databases"): every open
/// database runs its own compactor, and each absorption (its L0 SSTs read whole and their rows held decoded) or merge
/// (its sources' read-ahead and its run writer's part buffer) was bounded per database only: 200 databases could hold
/// 200 absorptions at once. A job takes a permit for its estimated bytes (KiB units) for as long as it runs; a job
/// bigger than the whole allowance takes all of it. A permit not granted within `COMPACTION_WAIT` is skipped (the job
/// goes on, `COMPACTION_PERMIT_TIMEOUTS`): a job never waits forever on another that waits for it (two merges of one
/// reseal, an erasure waiting for an absorption).
pub static COMPACTION_PERMIT_WAITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static COMPACTION_PERMIT_TIMEOUTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub const COMPACTION_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
static COMPACTION_SEM: OnceLock<(tokio::sync::Semaphore, u32)> = OnceLock::new();

pub async fn compaction_permit(bytes: u64) -> Option<tokio::sync::SemaphorePermit<'static>> {
    use std::sync::atomic::Ordering::Relaxed;
    let (sem, total) = COMPACTION_SEM.get_or_init(|| {
        let kib = (get().compaction >> 10).clamp(1, u32::MAX as u64 >> 4) as u32;
        (tokio::sync::Semaphore::new(kib as usize), kib)
    });
    let want = (bytes.div_ceil(1024) as u32).clamp(1, *total);
    if let Ok(p) = sem.try_acquire_many(want) {
        return Some(p);
    }
    COMPACTION_PERMIT_WAITS.fetch_add(1, Relaxed);
    match tokio::time::timeout(COMPACTION_WAIT, sem.acquire_many(want)).await {
        Ok(Ok(p)) => Some(p),
        _ => {
            COMPACTION_PERMIT_TIMEOUTS.fetch_add(1, Relaxed);
            None
        }
    }
}

/// (KiB free, KiB in all) of the compaction allowance.
pub fn compaction_allowance() -> (usize, u32) {
    COMPACTION_SEM.get().map_or((0, 0), |(s, t)| (s.available_permits(), *t))
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
///
/// Review of big-speed (2026-11-13, error 3): the files at the root of the cgroup mount are the process's own group only
/// when the process has its own cgroup namespace (Docker's and Kubernetes' default). A limit set by a supervisor without
/// a namespace (a systemd unit's `MemoryMax=`, `docker --cgroupns=host`, a process placed in a child group) sits in the
/// process's own group, deeper in the tree: before, it was missed and the budget took the whole machine (MEASURED by the
/// reviewer: 16,086 MiB under a 1 GiB child group). Now the process's own group is read from `/proc/self/cgroup`, its
/// directory found through `/proc/self/mountinfo`, and the smallest limit from that group up to the mount's root is
/// taken (cgroup v1 also gives `hierarchical_memory_limit` in `memory.stat`, which covers groups above the mount's root).
pub fn detect() -> (u64, &'static str) {
    let read = |p: &str| std::fs::read_to_string(p).ok();
    detect_with(&read)
}

/// [`detect`] over a file reader (pure: tested below with the layouts of cgroup v1, v2, systemd and namespaces).
pub fn detect_with(read: &dyn Fn(&str) -> Option<String>) -> (u64, &'static str) {
    let r = |p: &str| read(p).unwrap_or_default();
    let mut best = limit_from(&r("/proc/meminfo"), &r("/sys/fs/cgroup/memory.max"), &r("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    for (v, src) in own_group_limits(read) {
        if v < best.0 {
            best = (v, src);
        }
    }
    best
}

/// One mount of a cgroup hierarchy: (the cgroup path at the mount's root, the mount point, v2, its super options).
struct CgMount {
    root: String,
    point: String,
    v2: bool,
    opts: String,
}

fn cgroup_mounts(mountinfo: &str) -> Vec<CgMount> {
    let mut out = Vec::new();
    for line in mountinfo.lines() {
        // "id parent maj:min root mountpoint options [optional fields] - fstype source superoptions"
        let Some((left, right)) = line.split_once(" - ") else { continue };
        let l: Vec<&str> = left.split_whitespace().collect();
        let r: Vec<&str> = right.split_whitespace().collect();
        if l.len() < 5 || r.is_empty() {
            continue;
        }
        let v2 = match r[0] {
            "cgroup2" => true,
            "cgroup" => false,
            _ => continue,
        };
        out.push(CgMount { root: unescape(l[3]), point: unescape(l[4]), v2, opts: r.get(2).copied().unwrap_or("").to_string() });
    }
    out
}

/// mountinfo escapes space, tab, newline and backslash as octal (`\040`).
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The directories from `dir` up to `top` (both included), deepest first.
fn up_to(dir: &str, top: &str) -> Vec<String> {
    let top = top.trim_end_matches('/');
    let mut out = Vec::new();
    let mut d = dir.trim_end_matches('/').to_string();
    loop {
        out.push(if d.is_empty() { "/".to_string() } else { d.clone() });
        if d.len() <= top.len() || !d.starts_with(top) {
            break;
        }
        match d.rfind('/') {
            Some(i) => d.truncate(i),
            None => break,
        }
    }
    out
}

/// Every memory limit of the process's own groups (v2: `memory.max` from the group up to the mount root; v1: the
/// memory controller's `memory.limit_in_bytes` from the group up, and `hierarchical_memory_limit`).
fn own_group_limits(read: &dyn Fn(&str) -> Option<String>) -> Vec<(u64, &'static str)> {
    let mut out = Vec::new();
    let (Some(cg), Some(mi)) = (read("/proc/self/cgroup"), read("/proc/self/mountinfo")) else { return out };
    let mounts = cgroup_mounts(&mi);
    for line in cg.lines() {
        let mut it = line.splitn(3, ':');
        let (Some(_id), Some(ctrls), Some(path)) = (it.next(), it.next(), it.next()) else { continue };
        let v2 = ctrls.is_empty();
        if !v2 && !ctrls.split(',').any(|c| c == "memory") {
            continue;
        }
        let m = mounts.iter().find(|m| if v2 { m.v2 } else { !m.v2 && m.opts.split(',').any(|o| o == "memory") });
        let Some(m) = m else { continue };
        // the group's path relative to the mount's root (a namespaced container sees "/" for both)
        let root = m.root.trim_end_matches('/');
        let rel = if root.is_empty() {
            path
        } else if let Some(r) = path.strip_prefix(root) {
            r
        } else {
            // the group lies outside what this mount shows: only the mount's own root can be read
            ""
        };
        let dir = format!("{}{}", m.point.trim_end_matches('/'), if rel == "/" { "" } else { rel });
        let (file, src) = if v2 { ("memory.max", "cgroup2-own") } else { ("memory.limit_in_bytes", "cgroup1-own") };
        for d in up_to(&dir, &m.point) {
            if let Some(v) = read(&format!("{}/{file}", d.trim_end_matches('/'))).as_deref().and_then(parse_cgroup_limit) {
                out.push((v, src));
            }
        }
        if !v2 {
            if let Some(st) = read(&format!("{dir}/memory.stat")) {
                if let Some(v) = st.lines().find_map(|l| l.strip_prefix("hierarchical_memory_limit ")).and_then(parse_cgroup_limit) {
                    out.push((v, src));
                }
            }
        }
    }
    out
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
        // review of big-speed: the parts that grew with the databases or one customer, bounded
        assert_eq!(b.memtables, 128 * MIB);
        assert_eq!(b.merge_read, 32 * MIB);
        assert_eq!(b.absorb_l0, 4);
        assert!(b.box_customer >= 12 * MIB && b.box_customer < 13 * MIB, "{}", b.box_customer);
        assert!(b.box_inflight >= 25 * MIB && b.box_inflight < 26 * MIB, "{}", b.box_inflight);
        // everything fixed together, with the memtables at their hard limit, stays under 3/4 of the limit
        assert_eq!(b.compaction, 128 * MIB);
        let all = b.caches + b.first_course + b.change_log + b.memtables * 3 / 2 + b.box_inflight + b.compaction;
        assert!(all <= GIB / 4 * 3, "fixed reservations {} MiB", all >> 20);
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

    /// Review of big-speed, error 3: a limit on the process's own group, without its own cgroup namespace.
    #[test]
    fn rvbs_reads_the_own_group_without_a_namespace() {
        let fs = |files: Vec<(&'static str, &'static str)>| move |p: &str| files.iter().find(|(k, _)| *k == p).map(|(_, v)| v.to_string());
        let mi = "MemTotal:       16472064 kB\n";
        // cgroup v1, a child group (this sandbox; the reviewer's set-up): the mount's root has no limit, the group 1 GiB
        let v1 = fs(vec![
            ("/proc/meminfo", mi),
            ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n"),
            ("/proc/self/cgroup", "9:name=systemd:/\n4:memory:/process_api/abc/claude-code-bash/rv1g\n1:cpu:/\n0::/\n"),
            ("/proc/self/mountinfo", "36 23 0:28 / /sys/fs/cgroup rw,relatime - tmpfs tmpfs rw,mode=755\n40 36 0:32 / /sys/fs/cgroup/memory rw,relatime - cgroup cgroup rw,memory\n46 36 0:38 / /sys/fs/cgroup/unified rw,relatime - cgroup2 cgroup2 rw\n"),
            ("/sys/fs/cgroup/memory/process_api/abc/claude-code-bash/rv1g/memory.limit_in_bytes", "1073741824\n"),
            ("/sys/fs/cgroup/memory/process_api/abc/claude-code-bash/memory.limit_in_bytes", "14337294336\n"),
            ("/sys/fs/cgroup/memory/process_api/abc/memory.limit_in_bytes", "9223372036854771712\n"),
            ("/sys/fs/cgroup/memory/process_api/memory.limit_in_bytes", "9223372036854771712\n"),
        ]);
        assert_eq!(detect_with(&v1), (GIB, "cgroup1-own"));
        // the same with the limit on a parent group only: the smallest from the group up wins
        let v1p = fs(vec![
            ("/proc/meminfo", mi),
            ("/proc/self/cgroup", "4:memory:/a/b\n"),
            ("/proc/self/mountinfo", "40 36 0:32 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n"),
            ("/sys/fs/cgroup/memory/a/b/memory.limit_in_bytes", "9223372036854771712\n"),
            ("/sys/fs/cgroup/memory/a/memory.limit_in_bytes", "2147483648\n"),
        ]);
        assert_eq!(detect_with(&v1p), (2 * GIB, "cgroup1-own"));
        // cgroup v2, a systemd unit with MemoryMax= (the README's systemd unit): no memory.max at the root
        let v2 = fs(vec![
            ("/proc/meminfo", mi),
            ("/proc/self/cgroup", "0::/system.slice/fenvix.service\n"),
            ("/proc/self/mountinfo", "30 23 0:26 / /sys/fs/cgroup rw,nosuid - cgroup2 cgroup2 rw,nsdelegate\n"),
            ("/sys/fs/cgroup/system.slice/fenvix.service/memory.max", "1073741824\n"),
            ("/sys/fs/cgroup/system.slice/memory.max", "max\n"),
        ]);
        assert_eq!(detect_with(&v2), (GIB, "cgroup2-own"));
        // cgroup v2 with its own namespace (Docker's default): the group is "/", the root file holds the limit
        let ns = fs(vec![
            ("/proc/meminfo", mi),
            ("/proc/self/cgroup", "0::/\n"),
            ("/proc/self/mountinfo", "30 23 0:26 / /sys/fs/cgroup ro - cgroup2 cgroup2 rw\n"),
            ("/sys/fs/cgroup/memory.max", "536870912\n"),
        ]);
        assert_eq!(detect_with(&ns).0, 512 * MIB);
        // cgroup v1, `docker --cgroupns=host`-like bind mount: the mount's root is the container's group
        let bind = fs(vec![
            ("/proc/meminfo", mi),
            ("/proc/self/cgroup", "4:memory:/docker/c1\n"),
            ("/proc/self/mountinfo", "40 36 0:32 /docker/c1 /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n"),
            ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "805306368\n"),
        ]);
        assert_eq!(detect_with(&bind).0, 768 * MIB);
        // v1 hierarchical limit (a group above the mount's root) from memory.stat
        let hier = fs(vec![
            ("/proc/meminfo", mi),
            ("/proc/self/cgroup", "4:memory:/x\n"),
            ("/proc/self/mountinfo", "40 36 0:32 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n"),
            ("/sys/fs/cgroup/memory/x/memory.stat", "cache 0\nhierarchical_memory_limit 3221225472\n"),
        ]);
        assert_eq!(detect_with(&hier), (3 * GIB, "cgroup1-own"));
        // nothing readable: the machine
        let none = fs(vec![("/proc/meminfo", mi)]);
        assert_eq!(detect_with(&none).1, "memtotal");
        assert_eq!(unescape("/a\\040b"), "/a b");
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
