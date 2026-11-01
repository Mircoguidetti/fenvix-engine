//! Misure di processo e istogrammi.

use serde::Serialize;

/// Secondi di CPU (utente + sistema) del processo `pid` (0 = questo).
pub fn cpu_s(pid: u32) -> f64 {
    let p = if pid == 0 { "/proc/self/stat".to_string() } else { format!("/proc/{pid}/stat") };
    let Ok(s) = std::fs::read_to_string(p) else { return 0.0 };
    let Some(i) = s.rfind(')') else { return 0.0 };
    let f: Vec<&str> = s[i + 2..].split_whitespace().collect();
    let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    (f[11].parse::<f64>().unwrap_or(0.0) + f[12].parse::<f64>().unwrap_or(0.0)) / tck
}

/// Memoria residente (byte) del processo `pid` (0 = questo).
pub fn rss(pid: u32) -> u64 {
    let p = if pid == 0 { "/proc/self/statm".to_string() } else { format!("/proc/{pid}/statm") };
    let Ok(s) = std::fs::read_to_string(p) else { return 0 };
    s.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0) * 4096
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct Perc {
    pub n: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub media_us: f64,
}

pub fn perc(h: &hdrhistogram::Histogram<u64>) -> Perc {
    Perc {
        n: h.len(),
        p50_us: h.value_at_quantile(0.5),
        p90_us: h.value_at_quantile(0.9),
        p99_us: h.value_at_quantile(0.99),
        p999_us: h.value_at_quantile(0.999),
        max_us: h.max(),
        media_us: h.mean(),
    }
}

pub fn hist() -> hdrhistogram::Histogram<u64> {
    hdrhistogram::Histogram::new_with_bounds(1, 600_000_000, 3).unwrap()
}
