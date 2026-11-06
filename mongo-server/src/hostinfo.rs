//! `hostInfo` from the machine the server runs on (it was the answer of the recorded mongod: 4 cores and 16 GB on any
//! machine). The shape is the template captured from mongod 7.0 (`diag/hostInfo_sa.json`); every number that describes the
//! machine is replaced with the real one:
//!
//! - `system.numCores` and `system.numCoresAvailableToProcess`: the CPUs this process can use
//!   (`std::thread::available_parallelism`: online CPUs, restricted by the affinity mask and by a cgroup CPU quota). The
//!   benchmark script reads `numCores` as "cores of the primary";
//! - `system.numPhysicalCores` and `system.numCpuSockets`: distinct (physical id, core id) pairs and physical ids in
//!   `/proc/cpuinfo`, at least 1 and at most the CPUs online;
//! - `system.memSizeMB`: `MemTotal` of `/proc/meminfo`; `system.memLimitMB`: the smaller of it and the cgroup memory limit;
//! - `system.cpuArch`, `os.*`, `extra.*`: from the running system (`/proc`, `/etc/os-release`, `getrlimit`, `sysconf`).

use bson::{Bson, Document};

/// `MemTotal` of a `/proc/meminfo` text, in bytes.
pub fn mem_total_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// The cgroup memory limit in bytes, from the text of cgroup v2 `memory.max` or v1 `memory.limit_in_bytes`
/// (`None`: no limit).
pub fn cgroup_limit_bytes(text: &str) -> Option<u64> {
    let t = text.trim();
    if t.is_empty() || t == "max" {
        return None;
    }
    let v: u64 = t.parse().ok()?;
    // cgroup v1 writes "no limit" as a number close to i64::MAX rounded to the page size
    (v < (1u64 << 62)).then_some(v)
}

/// (physical cores, sockets) of a `/proc/cpuinfo` text; `None` when it names no physical ids (some virtual machines).
pub fn physical_cores(cpuinfo: &str) -> Option<(usize, usize)> {
    let mut cores = std::collections::HashSet::new();
    let mut sockets = std::collections::HashSet::new();
    let (mut phys, mut core) = (None::<String>, None::<String>);
    let mut flush = |phys: &mut Option<String>, core: &mut Option<String>| {
        if let Some(p) = phys.take() {
            sockets.insert(p.clone());
            cores.insert((p, core.take().unwrap_or_default()));
        }
        *core = None;
    };
    for line in cpuinfo.lines() {
        if line.trim().is_empty() {
            flush(&mut phys, &mut core);
            continue;
        }
        let Some((k, v)) = line.split_once(':') else { continue };
        match k.trim() {
            "physical id" => phys = Some(v.trim().to_string()),
            "core id" => core = Some(v.trim().to_string()),
            _ => {}
        }
    }
    flush(&mut phys, &mut core);
    (!cores.is_empty()).then(|| (cores.len(), sockets.len()))
}

/// The first value of `key` in a `/proc/cpuinfo` text.
pub fn cpuinfo_field(cpuinfo: &str, key: &str) -> Option<String> {
    cpuinfo.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == key).then(|| v.trim().to_string())
    })
}

/// `NAME` and `VERSION_ID` of an `/etc/os-release` text.
pub fn os_release(text: &str) -> (Option<String>, Option<String>) {
    let get = |key: &str| {
        text.lines().find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix('=')).map(|v| v.trim().trim_matches('"').to_string()))
    };
    (get("NAME"), get("VERSION_ID"))
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// The CPUs this process can use.
pub fn usable_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

fn online_cpus() -> usize {
    let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    if n > 0 {
        n as usize
    } else {
        usable_cpus()
    }
}

/// Machine memory and the limit this process runs under, in bytes.
pub fn memory() -> (Option<u64>, Option<u64>) {
    let total = mem_total_bytes(&read("/proc/meminfo"));
    let cg = cgroup_limit_bytes(&read("/sys/fs/cgroup/memory.max")).or_else(|| cgroup_limit_bytes(&read("/sys/fs/cgroup/memory/memory.limit_in_bytes")));
    let limit = match (total, cg) {
        (Some(t), Some(c)) => Some(t.min(c)),
        (t, c) => t.or(c),
    };
    (total, limit)
}

/// Fill the machine numbers into the mongod `hostInfo` template `d`.
pub fn fill(d: &mut Document) {
    let cpuinfo = read("/proc/cpuinfo");
    let usable = usable_cpus();
    let online = online_cpus();
    let (phys, sockets) = physical_cores(&cpuinfo).map(|(c, s)| (c.clamp(1, online.max(1)), s.max(1))).unwrap_or((online, 1));
    let (total, limit) = memory();
    let mb = |b: Option<u64>| b.map(|b| (b / (1024 * 1024)) as i64).unwrap_or(0);
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as i64;
    if let Some(Bson::Document(s)) = d.get_mut("system") {
        s.insert("memSizeMB", Bson::Int64(mb(total)));
        s.insert("memLimitMB", Bson::Int64(mb(limit)));
        s.insert("numCores", Bson::Int32(usable as i32));
        s.insert("numCoresAvailableToProcess", Bson::Int32(usable as i32));
        s.insert("numPhysicalCores", Bson::Int32(phys as i32));
        s.insert("numCpuSockets", Bson::Int32(sockets as i32));
        s.insert("cpuArch", std::env::consts::ARCH);
        s.insert("cpuAddrSize", Bson::Int32((std::mem::size_of::<usize>() * 8) as i32));
    }
    if let Some(Bson::Document(o)) = d.get_mut("os") {
        let (name, version) = os_release(&read("/etc/os-release"));
        o.insert("type", if cfg!(target_os = "linux") { "Linux" } else { std::env::consts::OS });
        o.insert("name", name.unwrap_or_default());
        o.insert("version", version.unwrap_or_default());
    }
    if let Some(Bson::Document(e)) = d.get_mut("extra") {
        e.insert("versionString", read("/proc/version").trim().to_string());
        e.insert("kernelVersion", read("/proc/sys/kernel/osrelease").trim().to_string());
        e.insert("cpuFrequencyMHz", cpuinfo_field(&cpuinfo, "cpu MHz").unwrap_or_default());
        e.insert("cpuFeatures", cpuinfo_field(&cpuinfo, "flags").or_else(|| cpuinfo_field(&cpuinfo, "Features")).unwrap_or_default());
        e.insert("pageSize", Bson::Int64(page));
        e.insert("numPages", Bson::Int32(total.map(|t| (t as i64 / page) as i32).unwrap_or(0)));
        let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } == 0 {
            e.insert("maxOpenFiles", Bson::Int32(rl.rlim_cur.min(i32::MAX as u64) as i32));
        }
        e.remove("libcVersion");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn parses_meminfo_and_cgroup() {
        assert_eq!(mem_total_bytes("MemTotal:        8040512 kB\nMemFree: 1 kB\n"), Some(8040512 * 1024));
        assert_eq!(mem_total_bytes("MemFree: 1 kB\n"), None);
        assert_eq!(cgroup_limit_bytes("max\n"), None);
        assert_eq!(cgroup_limit_bytes("4294967296\n"), Some(4294967296));
        assert_eq!(cgroup_limit_bytes("9223372036854771712\n"), None);
    }

    #[test]
    fn counts_physical_cores_and_sockets() {
        let two_ht = "processor : 0\nphysical id : 0\ncore id : 0\n\nprocessor : 1\nphysical id : 0\ncore id : 0\n\nprocessor : 2\nphysical id : 0\ncore id : 1\n\nprocessor : 3\nphysical id : 0\ncore id : 1\n";
        assert_eq!(physical_cores(two_ht), Some((2, 1)));
        assert_eq!(physical_cores("processor : 0\nmodel name : x\n"), None);
        assert_eq!(cpuinfo_field("cpu MHz\t\t: 2100.000\nflags : fpu sse\n", "cpu MHz").as_deref(), Some("2100.000"));
        assert_eq!(os_release("NAME=\"Ubuntu\"\nVERSION_ID=\"24.04\"\n"), (Some("Ubuntu".into()), Some("24.04".into())));
    }

    #[test]
    fn host_info_is_this_machine() {
        let mut d = doc! { "system": { "numCores": 4, "memSizeMB": 16091_i64 }, "os": {}, "extra": {} };
        fill(&mut d);
        let s = d.get_document("system").unwrap();
        assert_eq!(s.get_i32("numCores").unwrap() as usize, usable_cpus());
        let total = mem_total_bytes(&std::fs::read_to_string("/proc/meminfo").unwrap()).unwrap();
        assert_eq!(s.get_i64("memSizeMB").unwrap() as u64, total / (1024 * 1024));
        assert!(s.get_i64("memLimitMB").unwrap() <= s.get_i64("memSizeMB").unwrap());
    }
}
