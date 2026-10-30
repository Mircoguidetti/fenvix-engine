//! Shared pieces of the slatedb tenant benchmark: the request-counting object store and the
//! tenant wake lifecycle. Used by the `tenant-bench` benchmark binary and the `mongo-server` binary.

pub mod counting;
pub mod lifecycle;
pub mod wake_bundle;
