//! Engine-facing journal interface (design: `docs/journal3z-design.md` §10).
//!
//! The engine writes a database's operations to a [`Journal`] and acknowledges the client when the record is durable;
//! the database state is uploaded to object storage in batches, and the journal is trimmed below the uploaded index.
//! The backend is chosen per database: [`local::LocalJournal`] (one disk, the semantics of round3's
//! `--local-log-dir`) or the 3-zone journal (three nodes in three AZs, acknowledgement on 2 of 3; protocol in
//! `journal3z/core`, not wired to a network here).
//!
//! Contract (both backends):
//! - one writer per database at a time, identified by its **epoch** (from the database lease); an older epoch gets
//!   [`JournalError::Fenced`] and must abandon the database;
//! - indices are per database, contiguous, starting at 1; `append` assigns the next one (call it under the database's
//!   lock so that the journal order is the apply order);
//! - `durable` resolves only when the record **and every earlier record of the database** are durable;
//! - the database's own state carries a marker with the index of the last record it contains (`sys/journal`); `open`
//!   returns the records above the marker index (`applied`) for the engine to apply again;
//! - trim happens only below an index the engine reports as confirmed in object storage (`uploaded`).

use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub mod local;

/// A database as the journal knows it. `incarnation` is a random id created with the database (stored in its lease
/// record): a database dropped and created again with the same name is a different journal key (design D4).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DbKey {
    pub name: String,
    pub incarnation: u64,
}

impl DbKey {
    pub fn new(name: impl Into<String>, incarnation: u64) -> Self {
        DbKey { name: name.into(), incarnation }
    }
}

/// Writer epoch: the lease `seq` written by the conditional PUT that made this server the owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(pub u64);

/// Per-database record index (1, 2, 3, ...). `Index(0)` means "nothing".
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Index(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalError {
    /// Another writer (higher epoch) owns the database. Abandon it; nothing more will be acknowledged for this epoch.
    Fenced { promised: Epoch },
    /// No quorum, timeout, or the backend is stopping. Retryable; the record was not acknowledged.
    Unavailable(String),
    /// The database's unflushed bytes are over its hard cap. Retryable after an upload.
    Quota,
    /// The database was erased.
    Erased,
    /// Local I/O failure (for the local backend: a failed fsync stops the journal, like round3).
    Io(String),
    /// The call does not fit the protocol (for example `close` with records not yet uploaded).
    Invalid(String),
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Fenced { promised } => write!(f, "fenced: another writer holds epoch {}", promised.0),
            JournalError::Unavailable(s) => write!(f, "journal unavailable: {s}"),
            JournalError::Quota => write!(f, "journal quota exceeded for the database"),
            JournalError::Erased => write!(f, "database erased"),
            JournalError::Io(s) => write!(f, "journal I/O error: {s}"),
            JournalError::Invalid(s) => write!(f, "invalid journal call: {s}"),
        }
    }
}

impl std::error::Error for JournalError {}

/// Result of [`Journal::open`].
#[derive(Debug, Default)]
pub struct Opened {
    /// Records above `applied`, in index order, to apply again (the engine's apply is idempotent via its marker).
    pub replay: Vec<(Index, Bytes)>,
    /// Index the next `append` will get.
    pub next: Index,
}

/// Result of [`Journal::append`]: the index (known at once) and a future that resolves when it is acknowledged.
pub struct Appended {
    pub index: Index,
    pub durable: Durable,
}

/// Resolves to `Ok(())` once the record and every earlier record of the database are durable.
pub struct Durable(tokio::sync::oneshot::Receiver<Result<(), JournalError>>);

impl Durable {
    pub fn ready(r: Result<(), JournalError>) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(r);
        Durable(rx)
    }
    pub fn channel() -> (tokio::sync::oneshot::Sender<Result<(), JournalError>>, Self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (tx, Durable(rx))
    }
}

impl Future for Durable {
    type Output = Result<(), JournalError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(JournalError::Unavailable("journal stopped".into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The journal of one engine server process (all its databases).
#[async_trait::async_trait]
pub trait Journal: Send + Sync {
    /// Identity of this journal, written into the database marker (round3: `LOG_ID`). A marker naming another
    /// journal id is the engine's "conflict" case (records not applied), as in round3.
    fn id(&self) -> &str;

    /// Become the writer of `db` at `epoch`. `applied` is the index in the database's marker (0 for a new database).
    /// Fences every older epoch. Returns the records above `applied` to apply again and the next index.
    async fn open(&self, db: &DbKey, epoch: Epoch, applied: Index) -> Result<Opened, JournalError>;

    /// Append one record. Assigns the index now; `durable` resolves when it is acknowledged.
    fn append(&self, db: &DbKey, epoch: Epoch, payload: Bytes) -> Result<Appended, JournalError>;

    /// The database state up to `index` is durable in object storage (confirmed by the store). Allows trim.
    async fn uploaded(&self, db: &DbKey, epoch: Epoch, index: Index) -> Result<(), JournalError>;

    /// The database goes to sleep or moves: every appended record must already be uploaded.
    async fn close(&self, db: &DbKey, epoch: Epoch) -> Result<(), JournalError>;

    /// The database is dropped or erased: none of its records may ever be replayed; later opens get `Erased`.
    async fn erase(&self, db: &DbKey) -> Result<(), JournalError>;

    /// Databases with records not yet uploaded, with their last index (startup: what must be opened and replayed).
    async fn pending(&self) -> Result<Vec<(DbKey, Index)>, JournalError>;
}
