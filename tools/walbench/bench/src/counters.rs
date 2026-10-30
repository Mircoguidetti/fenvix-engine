//! Process-wide request counters. Every request attempt the benchmark sends is counted here (SDK retries are disabled;
//! the stores retry themselves and count each attempt), so cost is computed from counted requests, not from estimates.
//! A live snapshot is written every few seconds to $WALBENCH_LIVE_DIR/<pid>.json for the spend log.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub struct Counters {
    pub s3_put: AtomicU64,
    pub s3_get: AtomicU64,
    pub s3_list: AtomicU64,
    pub s3_put_bytes: AtomicU64,
    pub xz_put: AtomicU64,
    pub xz_get: AtomicU64,
    pub xz_list: AtomicU64,
    pub xz_put_bytes: AtomicU64,
    pub xz_get_bytes: AtomicU64,
    pub ddb_put: AtomicU64,
    pub ddb_wcu_milli: AtomicU64,
    pub ddb_query: AtomicU64,
    pub ddb_rcu_milli: AtomicU64,
    pub ddb_put_bytes: AtomicU64,
    pub file_syncs: AtomicU64,
    pub file_bytes: AtomicU64,
    /// bytes sent to / received from each quorum node (index = node number), for inter-AZ transfer cost
    pub q_sent: [AtomicU64; 3],
    pub q_recv: [AtomicU64; 3],
    pub ddb_throttled: AtomicU64,
    pub errors: AtomicU64,
    pub retries: AtomicU64,
}

pub static C: std::sync::LazyLock<Counters> = std::sync::LazyLock::new(Counters::default);

#[derive(Serialize, Clone, Default, Debug)]
pub struct Snap {
    pub s3_put: u64,
    pub s3_get: u64,
    pub s3_list: u64,
    pub s3_put_bytes: u64,
    pub xz_put: u64,
    pub xz_get: u64,
    pub xz_list: u64,
    pub xz_put_bytes: u64,
    pub xz_get_bytes: u64,
    pub ddb_put: u64,
    pub ddb_wcu: f64,
    pub ddb_query: u64,
    pub ddb_rcu: f64,
    pub ddb_put_bytes: u64,
    pub file_syncs: u64,
    pub file_bytes: u64,
    pub q_sent: [u64; 3],
    pub q_recv: [u64; 3],
    pub ddb_throttled: u64,
    pub errors: u64,
    pub retries: u64,
}

pub fn snap() -> Snap {
    let c = &*C;
    Snap {
        s3_put: c.s3_put.load(Relaxed),
        s3_get: c.s3_get.load(Relaxed),
        s3_list: c.s3_list.load(Relaxed),
        s3_put_bytes: c.s3_put_bytes.load(Relaxed),
        xz_put: c.xz_put.load(Relaxed),
        xz_get: c.xz_get.load(Relaxed),
        xz_list: c.xz_list.load(Relaxed),
        xz_put_bytes: c.xz_put_bytes.load(Relaxed),
        xz_get_bytes: c.xz_get_bytes.load(Relaxed),
        ddb_put: c.ddb_put.load(Relaxed),
        ddb_wcu: c.ddb_wcu_milli.load(Relaxed) as f64 / 1000.0,
        ddb_query: c.ddb_query.load(Relaxed),
        ddb_rcu: c.ddb_rcu_milli.load(Relaxed) as f64 / 1000.0,
        ddb_put_bytes: c.ddb_put_bytes.load(Relaxed),
        file_syncs: c.file_syncs.load(Relaxed),
        file_bytes: c.file_bytes.load(Relaxed),
        q_sent: std::array::from_fn(|i| c.q_sent[i].load(Relaxed)),
        q_recv: std::array::from_fn(|i| c.q_recv[i].load(Relaxed)),
        ddb_throttled: c.ddb_throttled.load(Relaxed),
        errors: c.errors.load(Relaxed),
        retries: c.retries.load(Relaxed),
    }
}

pub fn add(a: &AtomicU64, v: u64) {
    a.fetch_add(v, Relaxed);
}

/// Background writer of the live snapshot (no-op without WALBENCH_LIVE_DIR).
pub fn start_live(tag: String) {
    start_live_every(tag, 5000)
}

pub fn start_live_every(tag: String, every_ms: u64) {
    let Ok(dir) = std::env::var("WALBENCH_LIVE_DIR") else { return };
    let _ = std::fs::create_dir_all(&dir);
    let path = format!("{dir}/{}.json", std::process::id());
    tokio::spawn(async move {
        loop {
            write_live(&path, &tag);
            tokio::time::sleep(std::time::Duration::from_millis(every_ms)).await;
        }
    });
}

pub fn write_live(path: &str, tag: &str) {
    let v = serde_json::json!({"tag": tag, "pid": std::process::id(), "unix_ms": now_ms(), "counters": snap()});
    let tmp = format!("{path}.tmp");
    if std::fs::write(&tmp, v.to_string()).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

pub fn flush_live(tag: &str) {
    if let Ok(dir) = std::env::var("WALBENCH_LIVE_DIR") {
        write_live(&format!("{dir}/{}.json", std::process::id()), tag);
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}
