//! Storage adapter: openraft 0.9 `RaftLogStorage` on the harness `LogFile` (one file per node, shared by every group
//! on that node), plus a trivial state machine (last applied log id). Entries are kept in memory for reads; the file is
//! write-only (no restart recovery in the spike).
use crate::TC;
use j3z_harness::{frame, LogFile};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

type NID = u64;

// record kinds in the node's file
const K_ENTRY: u8 = 1;
const K_VOTE: u8 = 2;
const K_TRUNCATE: u8 = 3;
const K_PURGE: u8 = 4;

/// `frame(kind u8 | group u32 | term u64 | node u64 | index u64 | body)`
fn record(buf: &mut Vec<u8>, kind: u8, g: u32, term: u64, node: u64, index: u64, body: &[u8]) {
    let mut r = Vec::with_capacity(29 + body.len());
    r.push(kind);
    r.extend_from_slice(&g.to_le_bytes());
    r.extend_from_slice(&term.to_le_bytes());
    r.extend_from_slice(&node.to_le_bytes());
    r.extend_from_slice(&index.to_le_bytes());
    r.extend_from_slice(body);
    buf.extend_from_slice(&frame(&r));
}

#[derive(Default)]
pub struct LogInner {
    pub log: BTreeMap<u64, Entry<TC>>,
    pub vote: Option<Vote<NID>>,
    pub last_purged: Option<LogId<NID>>,
}

#[derive(Clone)]
pub struct LogStore {
    pub g: u32,
    pub inner: Arc<Mutex<LogInner>>,
    pub file: Arc<LogFile>,
}

impl LogStore {
    pub fn new(g: u32, file: Arc<LogFile>) -> Self {
        LogStore { g, inner: Arc::new(Mutex::new(LogInner::default())), file }
    }
    /// (first index, last index) present in the log.
    pub fn bounds(&self) -> (Option<u64>, Option<u64>) {
        let g = self.inner.lock().unwrap();
        (g.log.keys().next().copied(), g.log.keys().next_back().copied())
    }
    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.inner.lock().unwrap().log.get(&index).map(|e| e.log_id.leader_id.term)
    }
    async fn durable(&self, rec: Vec<u8>) -> Result<(), std::io::Error> {
        self.file.append_wait(rec).await.map_err(|_| std::io::Error::other("log file thread gone"))
    }
}

impl RaftLogReader<TC> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> Result<Vec<Entry<TC>>, StorageError<NID>> {
        let g = self.inner.lock().unwrap();
        Ok(g.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TC> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TC>, StorageError<NID>> {
        let g = self.inner.lock().unwrap();
        let last = g.log.values().next_back().map(|e| e.log_id).or(g.last_purged);
        Ok(LogState { last_purged_log_id: g.last_purged, last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Durable (fdatasync'd) before returning.
    async fn save_vote(&mut self, vote: &Vote<NID>) -> Result<(), StorageError<NID>> {
        let mut rec = Vec::new();
        record(&mut rec, K_VOTE, self.g, vote.leader_id.term, vote.leader_id.node_id, vote.committed as u64, &[]);
        self.durable(rec).await.map_err(|e| StorageIOError::write_vote(&e))?;
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NID>>, StorageError<NID>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    /// Entries become readable at once (the leader may replicate them while its own fsync runs, as Raft allows);
    /// openraft is told they are flushed only from the LogFile callback, i.e. after write + fdatasync.
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TC>) -> Result<(), StorageError<NID>>
    where
        I: IntoIterator<Item = Entry<TC>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut buf = Vec::new();
        {
            let mut g = self.inner.lock().unwrap();
            for e in entries {
                let body: Vec<u8> = match &e.payload {
                    EntryPayload::Blank => Vec::new(),
                    EntryPayload::Normal(b) => b.to_vec(),
                    // spike: membership kept in a debug form; production needs a real encoding
                    EntryPayload::Membership(m) => format!("{m:?}").into_bytes(),
                };
                record(&mut buf, K_ENTRY, self.g, e.log_id.leader_id.term, e.log_id.leader_id.node_id, e.log_id.index, &body);
                g.log.insert(e.log_id.index, e);
            }
        }
        self.file.append(buf, Box::new(move || callback.log_io_completed(Ok(()))));
        Ok(())
    }

    /// Delete `[log_id.index, ..)`; the truncation record is durable before returning.
    async fn truncate(&mut self, log_id: LogId<NID>) -> Result<(), StorageError<NID>> {
        self.inner.lock().unwrap().log.split_off(&log_id.index);
        let mut rec = Vec::new();
        record(&mut rec, K_TRUNCATE, self.g, log_id.leader_id.term, log_id.leader_id.node_id, log_id.index, &[]);
        self.durable(rec).await.map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    /// Delete `(.., log_id.index]`.
    async fn purge(&mut self, log_id: LogId<NID>) -> Result<(), StorageError<NID>> {
        {
            let mut g = self.inner.lock().unwrap();
            let keep = g.log.split_off(&(log_id.index + 1));
            g.log = keep;
            g.last_purged = Some(log_id);
        }
        let mut rec = Vec::new();
        record(&mut rec, K_PURGE, self.g, log_id.leader_id.term, log_id.leader_id.node_id, log_id.index, &[]);
        self.durable(rec).await.map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }
}

// --------------------------------------------------------------------------------------------------- state machine

#[derive(Default)]
pub struct SmInner {
    pub last_applied: Option<LogId<NID>>,
    pub membership: StoredMembership<NID, BasicNode>,
    /// log ids of applied entries not yet covered by a snapshot (to build a snapshot at an external index)
    pub ids: BTreeMap<u64, LogId<NID>>,
    /// The "S3 marker": when set, the next snapshot is built at this index (a pointer, no state copied).
    pub marker: Option<u64>,
    pub snapshot: Option<(SnapshotMeta<NID, BasicNode>, Vec<u8>)>,
    pub snap_seq: u64,
    pub built: u64,
    pub installed: u64,
}

#[derive(Clone, Default)]
pub struct Sm {
    pub inner: Arc<Mutex<SmInner>>,
}

impl RaftSnapshotBuilder<TC> for Sm {
    /// The "snapshot" is only a pointer to the externally stored state: `s3-marker:<index>`.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TC>, StorageError<NID>> {
        let mut g = self.inner.lock().unwrap();
        let at = match (g.marker, g.last_applied) {
            (Some(k), Some(la)) if k < la.index => g.ids.get(&k).copied().or(Some(la)),
            (_, la) => la,
        };
        g.snap_seq += 1;
        g.built += 1;
        let data = format!("s3-marker:{}", at.map(|x| x.index).unwrap_or(0)).into_bytes();
        let meta = SnapshotMeta { last_log_id: at, last_membership: g.membership.clone(), snapshot_id: format!("snap-{}", g.snap_seq) };
        if let Some(a) = at {
            let keep = g.ids.split_off(&(a.index + 1));
            g.ids = keep;
        }
        g.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}

impl RaftStateMachine<TC> for Sm {
    type SnapshotBuilder = Sm;

    async fn applied_state(&mut self) -> Result<(Option<LogId<NID>>, StoredMembership<NID, BasicNode>), StorageError<NID>> {
        let g = self.inner.lock().unwrap();
        Ok((g.last_applied, g.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NID>>
    where
        I: IntoIterator<Item = Entry<TC>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for e in entries {
            g.last_applied = Some(e.log_id);
            g.ids.insert(e.log_id.index, e.log_id);
            if let EntryPayload::Membership(m) = &e.payload {
                g.membership = StoredMembership::new(Some(e.log_id), m.clone());
            }
            out.push(());
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NID>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<NID, BasicNode>, snapshot: Box<Cursor<Vec<u8>>>) -> Result<(), StorageError<NID>> {
        let mut g = self.inner.lock().unwrap();
        g.last_applied = meta.last_log_id;
        g.membership = meta.last_membership.clone();
        if let Some(a) = meta.last_log_id {
            let keep = g.ids.split_off(&(a.index + 1));
            g.ids = keep;
        }
        g.installed += 1;
        g.snapshot = Some((meta.clone(), snapshot.into_inner()));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TC>>, StorageError<NID>> {
        let g = self.inner.lock().unwrap();
        Ok(g.snapshot.as_ref().map(|(m, d)| Snapshot { meta: m.clone(), snapshot: Box::new(Cursor::new(d.clone())) }))
    }
}
