//! journal3z writer client (phase-2 plan P5).
//!
//! - [`journal::ThreeZone`]: the 3-zone `Journal` backend (the engine-facing trait of `journal3z/api`).
//! - [`meta`]: lease and object-store stand-in on any `object_store` with conditional writes (MinIO locally).
//! - [`standin::Server`]: the engine stand-in the bench drives until the real engine is wired (P6, after PR #1).
pub mod journal;
pub mod meta;
pub mod standin;
