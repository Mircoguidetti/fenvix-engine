//! Where a per-thread delta encoding of agent checkpoints would plug in (founder's note 2026-11-09, "agent checkpoint
//! delta test"; the measure itself runs on branch checkpoint-delta).
//!
//! An agent framework's checkpointer (LangGraph, Mastra) stores the whole message list of a thread at every step, so a
//! thread's bytes grow with the square of its length. In a sealed run the steps of one thread sit next to each other
//! (layout v2 puts a customer's documents in one key range, and a thread's checkpoints are keyed by thread then step),
//! so a sealed run could store each checkpoint as the difference from the previous one of the same thread.
//!
//! The hook: every row the compactor writes into a sealed run passes through `RowCodec::encode` (today the identity).
//! Delta runs are not encoded: they are young, small and rewritten often, and the first course keeps their recent rows.
//!
//! What a real delta codec needs (none of it is built):
//! 1. A run-format envelope kind for "delta from the previous row" (runfmt `LIVE` 0 and `DELETED` 1 exist; a third
//!    value, for example 2, would carry the base key and an edit script). Run format v1 readers reject an unknown kind
//!    (`check_envelope`), so such a run is an error for an old reader, never wrong data: it needs a format version 2 and
//!    the release rule of envelopes (readers first, writers later; rule 21's E0/E1 pattern).
//! 2. A base within the same block: a block is the unit a reader decodes, so a delta whose base sits in another block
//!    would cost a second block read. The writer starts a fresh full copy at every block start (like a restart point),
//!    so a point read of any step decodes at most one block (time travel stays one GET).
//! 3. The decode in `Block::env` (or a wrapper in `view.rs`) for point and range reads, and in the compactor's `Scan`
//!    (a reseal re-encodes from full values).
//! 4. The thread boundary: a codec option naming which key prefix is a thread (for example the customer component plus
//!    the `thread_id` field), set per collection like `customerKey`.

/// Encodes one row on its way into a sealed run. Must be deterministic and must keep the key order.
pub trait RowCodec: Send + Sync {
    /// The key and envelope to store.
    fn encode(&self, key: &[u8], env: &[u8]) -> (Vec<u8>, Vec<u8>);
    fn name(&self) -> &'static str;
}

pub struct Identity;

impl RowCodec for Identity {
    fn encode(&self, key: &[u8], env: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (key.to_vec(), env.to_vec())
    }
    fn name(&self) -> &'static str {
        "identity"
    }
}

pub fn identity() -> std::sync::Arc<dyn RowCodec> {
    std::sync::Arc::new(Identity)
}
