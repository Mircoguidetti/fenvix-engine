//! Measurement-only switches of the E1 efficiency study (branch `studio-efficienza`, bench/efficiency/README.md).
//! Both are UNSAFE and off unless their environment variable is set to 1: they exist only to measure, by A/B runs,
//! how much CPU one piece of the insert path costs. They must never be set outside a benchmark.
//! - `E1_UNSAFE_SKIP_ID_CHECK=1`: inserts do not check that the `_id` is new (a duplicate `_id` would overwrite).
//! - `E1_UNSAFE_SKIP_CHANGE_LOG=1`: inserts, updates and deletes publish no change event (change streams miss them).
use std::sync::OnceLock;

fn flag(cell: &'static OnceLock<bool>, name: &str) -> bool {
    *cell.get_or_init(|| {
        let on = std::env::var(name).map(|v| v == "1").unwrap_or(false);
        if on {
            eprintln!("WARNING: {name}=1 (E1 measurement switch, unsafe)");
        }
        on
    })
}

pub fn skip_id_check() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    flag(&C, "E1_UNSAFE_SKIP_ID_CHECK")
}

pub fn skip_change_log() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    flag(&C, "E1_UNSAFE_SKIP_CHANGE_LOG")
}
