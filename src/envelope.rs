//! Envelope values (compaction 2, step 2; plan round 2, fix 4, rule I6).
//!
//! Our compacted tiers (later steps) need deletes they can see: a SlateDB tombstone is invisible to its merged scan
//! (probe 1). So deletes become values: an envelope is one byte, `0` (a live value follows) or `1` (deleted).
//!
//! Which rows are envelopes is decided by sequence number, not by the bytes (a plain value may start with any byte):
//! the first writer that starts envelopes writes the marker key [`MARKER`] alone in its own batch, and the marker row's
//! SlateDB sequence number is E_env. Every row with a sequence number above E_env (except the marker) is an envelope;
//! every row at or below it is a plain value. Rows keep their sequence number through flushes and compactions (SlateDB
//! 0.17 stores it per row; probe 1), so a reader decides each row from the `seq` SlateDB returns with it
//! ([`decode`]).
//!
//! Two releases, so that a rollback never meets a format it cannot read:
//! - E0 (this release by default): reads envelopes, and writes them only in a database that already has the marker (a
//!   database rolled back from E1 keeps writing envelopes above its E_env, so its rows stay decidable). It never writes
//!   the marker itself.
//! - E1 ([`START`] on, `--envelopes write`): also writes the marker in a database that has none, then envelopes.
//!
//! The rollback window goes from E1 back to E0, never further: a release before E0 reads an envelope as a plain value
//! (a deleted key comes back as a 1-byte value, every live value gets a stray 0 byte in front).

use anyhow::{bail, Result};
use bytes::Bytes;

/// The marker key. Its own row is always plain; its sequence number is E_env.
pub const MARKER: &[u8] = b"sys/fx-envelopes";
pub const LIVE: u8 = 0;
pub const DEAD: u8 = 1;

/// Release E1: start envelopes in a database that has no marker yet (`--envelopes write`). Off: release E0.
pub static START: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Envelope rows read (live and deleted), since the start (diagnostics and tests).
pub static READ_LIVE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static READ_DEAD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The value of a row as the layers above see it: `None` when the row is a delete envelope. `env` is the database's
/// E_env (`None`: no marker, every row is plain).
pub fn decode(env: Option<u64>, key: &[u8], seq: u64, v: Bytes) -> Result<Option<Bytes>> {
    let Some(e) = env else { return Ok(Some(v)) };
    if seq <= e || key == MARKER {
        return Ok(Some(v));
    }
    use std::sync::atomic::Ordering::Relaxed;
    match v.first() {
        Some(&LIVE) => {
            READ_LIVE.fetch_add(1, Relaxed);
            Ok(Some(v.slice(1..)))
        }
        Some(&DEAD) if v.len() == 1 => {
            READ_DEAD.fetch_add(1, Relaxed);
            Ok(None)
        }
        _ => bail!("corrupt envelope at seq {seq} (E_env {e}): first byte {:?}, {} bytes", v.first(), v.len()),
    }
}

/// A put as an envelope.
pub fn live(v: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() + 1);
    out.push(LIVE);
    out.extend(v);
    out
}

/// A delete as an envelope.
pub fn dead() -> Vec<u8> {
    vec![DEAD]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_above_the_marker_are_envelopes() {
        let v = Bytes::from_static(b"\x01abc");
        // no marker: plain, whatever the bytes
        assert_eq!(decode(None, b"k", 9, v.clone()).unwrap(), Some(v.clone()));
        // at or below E_env: plain
        assert_eq!(decode(Some(9), b"k", 9, v.clone()).unwrap(), Some(v.clone()));
        // above: an envelope
        assert_eq!(decode(Some(9), b"k", 10, Bytes::from(live(b"abc".to_vec()))).unwrap(), Some(Bytes::from_static(b"abc")));
        assert_eq!(decode(Some(9), b"k", 10, Bytes::from(dead())).unwrap(), None);
        assert_eq!(decode(Some(9), b"k", 10, Bytes::from(live(Vec::new()))).unwrap(), Some(Bytes::new()));
        // a plain value above E_env is an error, never data
        assert!(decode(Some(9), b"k", 10, v).is_err());
        assert!(decode(Some(9), b"k", 10, Bytes::new()).is_err());
        // the marker itself stays plain
        assert_eq!(decode(Some(9), MARKER, 10, Bytes::from_static(b"E1")).unwrap(), Some(Bytes::from_static(b"E1")));
    }
}
