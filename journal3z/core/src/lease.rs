//! The lease takeover rule (design §5.1, phase-2 decision of 30-10-2026).
//!
//! A server that wants a database held by another server **observes** the lease object: it remembers the content it
//! read and the time it first read it, on **its own monotonic clock**. It may try to take the lease (a conditional
//! write against exactly that version) only after it has seen the same content, unchanged, for `ttl + margin` on that
//! clock. The holder acts only until `sent + ttl` on **its own** monotonic clock, where `sent` is the time it sent the
//! write that created or renewed its lease.
//!
//! Why the two cannot overlap: the observer starts counting when it reads a version, which is after that version was
//! written, which is after the holder sent the write. So the observer counts `ttl + margin` from a later point than the
//! holder counts `ttl`. The clocks may run at slightly different speeds; the margin covers that: with `ttl` = 10 s and
//! margin = 1.5 s, each clock may run up to about 6.9 % fast or slow (exactly: (ttl + margin)·(1 − ρ) ≥ ttl·(1 + ρ)). Nothing compares an hour written by
//! one machine with the clock of another: clocks need not be aligned. A clock that jumps (wall clock set back or
//! forward) is not used: only a monotonic clock is.
//!
//! The lease is never needed for safety (the journal and store fences give that); it decides who may *try*, and when.
//! A free lease (no holder, released) may be taken at once; a server may re-take its own.
//!
//! [`LeaseBug`] holds the negative controls for this rule.

/// Deliberate bugs in the takeover rule (negative controls). `None` = the rule.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LeaseBug {
    #[default]
    None,
    /// Take over after `ttl` instead of `ttl + margin`.
    NoMargin,
    /// Count with the wall clock (which jumps) instead of the monotonic clock.
    WallClock,
    /// Compare only the holder's name, not the whole content: renewals look like "unchanged".
    HolderOnly,
}

impl LeaseBug {
    pub const ALL: [&'static str; 3] = ["lease_no_margin", "lease_wall_clock", "lease_holder_only"];
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "none" => LeaseBug::None,
            "lease_no_margin" => LeaseBug::NoMargin,
            "lease_wall_clock" => LeaseBug::WallClock,
            "lease_holder_only" => LeaseBug::HolderOnly,
            _ => return None,
        })
    }
}

/// Durations in the caller's unit (µs in the simulator, ms in the writer).
#[derive(Clone, Copy, Debug)]
pub struct TakeoverRule {
    pub ttl: u64,
    pub margin: u64,
    pub bug: LeaseBug,
}

/// What the observer read: `free` = no holder; `content` = a key of the whole object (holder, sequence, members,
/// version tag); `holder` = a key of the holder alone.
#[derive(Clone, Copy, Debug)]
pub struct LeaseView {
    pub free: bool,
    pub content: u64,
    pub holder: u64,
}

/// The observer side, one per database.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Observer {
    seen: Option<(u64, u64)>,
}

impl Observer {
    /// Record a read of the lease at `mono` (monotonic clock) / `wall` (wall clock, used only by the `WallClock`
    /// bug). Returns true if the reader may now try to take the lease (with a write conditional on the version it
    /// just read).
    pub fn observe(&mut self, r: &TakeoverRule, v: &LeaseView, mono: u64, wall: u64) -> bool {
        if v.free {
            self.seen = None;
            return true;
        }
        let key = if r.bug == LeaseBug::HolderOnly { v.holder } else { v.content };
        let now = if r.bug == LeaseBug::WallClock { wall } else { mono };
        let wait = if r.bug == LeaseBug::NoMargin { r.ttl } else { r.ttl + r.margin };
        match self.seen {
            Some((k, since)) if k == key => now.saturating_sub(since) >= wait,
            _ => {
                self.seen = Some((key, now));
                false
            }
        }
    }

    /// Forget what was seen (after taking the lease, or when no longer interested).
    pub fn reset(&mut self) {
        self.seen = None;
    }
}

/// The holder side: it may act until this time on its monotonic clock (`sent` = when it sent the acquire / renew
/// write that succeeded).
pub fn holder_valid_until(r: &TakeoverRule, sent: u64) -> u64 {
    sent + r.ttl
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: u64 = 10_000;
    const MARGIN: u64 = 1_500;

    fn rule(bug: LeaseBug) -> TakeoverRule {
        TakeoverRule { ttl: TTL, margin: MARGIN, bug }
    }

    /// A clock with a rate (parts per million) and an offset, as a function of real time (ms).
    fn clock(real: u64, ppm: i64, offset: i64) -> u64 {
        ((real as i64) + (real as i64) * ppm / 1_000_000 + offset).max(0) as u64
    }

    /// Holder renews (version v) at real time `renew`, then stops. Observer (clock rate `po`, wall offset jumping by
    /// `jump` at real time `jump_at`) reads every `poll` ms from real time `first`. Returns the overlap in ms between
    /// "holder still acts" and "observer took over" (0 = none).
    fn overlap(bug: LeaseBug, ph: i64, po: i64, renew: u64, first: u64, poll: u64, jump: i64, jump_at: u64, renewing_holder: bool) -> u64 {
        let r = rule(bug);
        // holder: valid until (on its clock) sent + ttl; convert to real time
        let mut sent_h = clock(renew, ph, 0);
        let mut version = 1u64;
        let mut ob = Observer::default();
        let mut real = first;
        loop {
            if renewing_holder && real >= renew + TTL / 3 * version {
                sent_h = clock(renew + TTL / 3 * version, ph, 0);
                version += 1;
            }
            let wall = clock(real, po, if real >= jump_at { jump } else { 0 });
            let v = LeaseView { free: false, content: 100 + version, holder: 7 };
            if ob.observe(&r, &v, clock(real, po, 0), wall) {
                // holder acts while its clock is below sent_h + ttl
                let mut end = real;
                while clock(end, ph, 0) < holder_valid_until(&r, sent_h) {
                    end += 1;
                }
                return end.saturating_sub(real);
            }
            real += poll;
            if real > renew + 100 * TTL {
                return 0;
            }
        }
    }

    #[test]
    fn rule_never_overlaps() {
        // drifts up to ±6 % each, every reading phase, wall jumps of ±5 s (ignored by the rule)
        let mut worst = 0;
        for ph in [-60_000, 0, 60_000] {
            for po in [-60_000, 0, 60_000] {
                for first in [0, 1, 7, 100, 999] {
                    for jump in [-5_000, 0, 5_000] {
                        worst = worst.max(overlap(LeaseBug::None, ph, po, 0, first, 50, jump, 2_000, false));
                        worst = worst.max(overlap(LeaseBug::None, ph, po, 0, first, 50, jump, 2_000, true));
                    }
                }
            }
        }
        assert_eq!(worst, 0);
    }

    #[test]
    fn each_bug_overlaps() {
        // no margin: holder clock slow, observer fast
        assert!(overlap(LeaseBug::NoMargin, -50_000, 50_000, 0, 0, 10, 0, 0, false) > 0);
        assert_eq!(overlap(LeaseBug::None, -50_000, 50_000, 0, 0, 10, 0, 0, false), 0, "twin");
        // wall clock: the observer's wall clock jumps forward 5 s while it watches
        assert!(overlap(LeaseBug::WallClock, 0, 0, 0, 0, 10, 5_000, 3_000, false) > 0);
        assert_eq!(overlap(LeaseBug::None, 0, 0, 0, 0, 10, 5_000, 3_000, false), 0, "twin");
        // holder only: the holder keeps renewing, the observer still takes over
        assert!(overlap(LeaseBug::HolderOnly, 0, 0, 0, 0, 10, 0, 0, true) > 0);
        assert_eq!(overlap(LeaseBug::None, 0, 0, 0, 0, 10, 0, 0, true), 0, "twin");
    }

    #[test]
    fn free_lease_taken_at_once_and_change_restarts_the_wait() {
        let r = rule(LeaseBug::None);
        let mut o = Observer::default();
        assert!(o.observe(&r, &LeaseView { free: true, content: 0, holder: 0 }, 0, 0));
        assert!(!o.observe(&r, &LeaseView { free: false, content: 1, holder: 1 }, 0, 0));
        assert!(!o.observe(&r, &LeaseView { free: false, content: 1, holder: 1 }, TTL + MARGIN - 1, 0));
        assert!(!o.observe(&r, &LeaseView { free: false, content: 2, holder: 1 }, TTL + MARGIN, 0)); // renewed
        assert!(!o.observe(&r, &LeaseView { free: false, content: 2, holder: 1 }, 2 * TTL + 2 * MARGIN - 1, 0));
        assert!(o.observe(&r, &LeaseView { free: false, content: 2, holder: 1 }, 2 * TTL + 2 * MARGIN, 0));
    }
}
