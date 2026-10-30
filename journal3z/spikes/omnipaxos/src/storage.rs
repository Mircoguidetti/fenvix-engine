//! Storage adapter: OmniPaxos's synchronous `Storage` trait on top of `MemoryStorage` (reads) plus a record of every
//! write, which the driver (`server.rs`) hands to the node's `LogFile` (write + fdatasync) after each step and before
//! any message that depends on it leaves the server. The trait itself cannot express "durable later": that ordering is
//! enforced by the driver, not here.
use bytes::Bytes;
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::{Entry, NoSnapshot, StopSign, Storage, StorageOp, StorageResult};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::sync::{Arc, Mutex};

/// One journal record (opaque payload).
#[derive(Clone, Debug)]
pub struct E(pub Bytes);

impl Entry for E {
    type Snapshot = NoSnapshot;
}

/// Writes made by OmniPaxos since the driver last took them.
#[derive(Default)]
pub struct Pending {
    bytes: Vec<u8>,
    /// Latest decided index, written lazily with the next real write (it is not needed for safety: a node relearns it
    /// from the leader), so that a decide alone never costs an fdatasync.
    decided: Option<usize>,
    /// Shadow of the accepted round (OmniPaxos does not expose it; the fence workaround needs it).
    pub accepted_round: Ballot,
}

impl Pending {
    /// Bytes to write + fdatasync now, if any real write happened.
    pub fn take_batch(&mut self, group: u32) -> Option<Vec<u8>> {
        if self.bytes.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(self.bytes.len() + 24);
        if let Some(d) = self.decided.take() {
            let mut r = rec_head(group, K_DECIDED);
            r.extend_from_slice(&(d as u64).to_le_bytes());
            out.extend_from_slice(&j3z_harness::frame(&r));
        }
        out.append(&mut self.bytes);
        Some(out)
    }
}

pub type PendingRef = Arc<Mutex<Pending>>;

const K_TXN: u8 = 0;
const K_APPEND: u8 = 1;
const K_PREFIX: u8 = 2;
const K_PROMISE: u8 = 3;
const K_DECIDED: u8 = 4;
const K_ACC_ROUND: u8 = 5;
const K_COMPACTED: u8 = 6;
const K_TRIM: u8 = 7;
const K_STOPSIGN: u8 = 8;
const K_SNAPSHOT: u8 = 9;

fn rec_head(group: u32, kind: u8) -> Vec<u8> {
    let mut r = Vec::with_capacity(64);
    r.extend_from_slice(&group.to_le_bytes());
    r.push(kind);
    r
}

fn put_ballot(r: &mut Vec<u8>, b: &Ballot) {
    r.extend_from_slice(&b.config_id.to_le_bytes());
    r.extend_from_slice(&b.n.to_le_bytes());
    r.extend_from_slice(&b.priority.to_le_bytes());
    r.extend_from_slice(&b.pid.to_le_bytes());
}

fn put_entries(r: &mut Vec<u8>, es: &[E]) {
    r.extend_from_slice(&(es.len() as u32).to_le_bytes());
    for e in es {
        r.extend_from_slice(&j3z_harness::frame(&e.0));
    }
}

/// Body of one op (kind byte + fields). The stop sign is recorded as present/absent only (enough for the spike).
fn enc_op(r: &mut Vec<u8>, op: &StorageOp<E>) {
    match op {
        StorageOp::AppendEntry(e) => {
            r.push(K_APPEND);
            put_entries(r, std::slice::from_ref(e))
        }
        StorageOp::AppendEntries(es) => {
            r.push(K_APPEND);
            put_entries(r, es)
        }
        StorageOp::AppendOnPrefix(i, es) => {
            r.push(K_PREFIX);
            r.extend_from_slice(&(*i as u64).to_le_bytes());
            put_entries(r, es)
        }
        StorageOp::SetPromise(b) => {
            r.push(K_PROMISE);
            put_ballot(r, b)
        }
        StorageOp::SetDecidedIndex(i) => {
            r.push(K_DECIDED);
            r.extend_from_slice(&(*i as u64).to_le_bytes())
        }
        StorageOp::SetAcceptedRound(b) => {
            r.push(K_ACC_ROUND);
            put_ballot(r, b)
        }
        StorageOp::SetCompactedIdx(i) => {
            r.push(K_COMPACTED);
            r.extend_from_slice(&(*i as u64).to_le_bytes())
        }
        StorageOp::Trim(i) => {
            r.push(K_TRIM);
            r.extend_from_slice(&(*i as u64).to_le_bytes())
        }
        StorageOp::SetStopsign(s) => {
            r.push(K_STOPSIGN);
            r.push(s.is_some() as u8)
        }
        StorageOp::SetSnapshot(_) => r.push(K_SNAPSHOT),
    }
}

pub struct DurableStorage {
    mem: MemoryStorage<E>,
    group: u32,
    out: PendingRef,
}

impl DurableStorage {
    pub fn new(group: u32, out: PendingRef) -> Self {
        DurableStorage { mem: MemoryStorage::default(), group, out }
    }

    /// Record one op as its own framed record.
    fn log(&self, op: &StorageOp<E>) {
        let mut r = self.group.to_le_bytes().to_vec();
        enc_op(&mut r, op);
        self.out.lock().unwrap().bytes.extend_from_slice(&j3z_harness::frame(&r));
    }
}

impl Storage<E> for DurableStorage {
    fn write_atomically(&mut self, ops: Vec<StorageOp<E>>) -> StorageResult<()> {
        // One record for the whole transaction: it reaches the file in one write and one fdatasync.
        let mut r = rec_head(self.group, K_TXN);
        r.extend_from_slice(&(ops.len() as u32).to_le_bytes());
        for op in &ops {
            let mut b = Vec::new();
            enc_op(&mut b, op);
            r.extend_from_slice(&j3z_harness::frame(&b));
            if let StorageOp::SetAcceptedRound(b) = op {
                self.out.lock().unwrap().accepted_round = *b;
            }
        }
        self.out.lock().unwrap().bytes.extend_from_slice(&j3z_harness::frame(&r));
        self.mem.write_atomically(ops)
    }
    fn append_entry(&mut self, entry: E) -> StorageResult<()> {
        let op = StorageOp::AppendEntry(entry);
        self.log(&op);
        let StorageOp::AppendEntry(entry) = op else { unreachable!() };
        self.mem.append_entry(entry)
    }
    fn append_entries(&mut self, entries: Vec<E>) -> StorageResult<()> {
        let op = StorageOp::AppendEntries(entries);
        self.log(&op);
        let StorageOp::AppendEntries(entries) = op else { unreachable!() };
        self.mem.append_entries(entries)
    }
    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<E>) -> StorageResult<()> {
        let op = StorageOp::AppendOnPrefix(from_idx, entries);
        self.log(&op);
        let StorageOp::AppendOnPrefix(from_idx, entries) = op else { unreachable!() };
        self.mem.append_on_prefix(from_idx, entries)
    }
    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.log(&StorageOp::SetPromise(n_prom));
        self.mem.set_promise(n_prom)
    }
    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.out.lock().unwrap().decided = Some(ld);
        self.mem.set_decided_idx(ld)
    }
    fn get_decided_idx(&self) -> StorageResult<usize> {
        self.mem.get_decided_idx()
    }
    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.log(&StorageOp::SetAcceptedRound(na));
        self.out.lock().unwrap().accepted_round = na;
        self.mem.set_accepted_round(na)
    }
    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        self.mem.get_accepted_round()
    }
    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<E>> {
        self.mem.get_entries(from, to)
    }
    fn get_log_len(&self) -> StorageResult<usize> {
        self.mem.get_log_len()
    }
    fn get_suffix(&self, from: usize) -> StorageResult<Vec<E>> {
        self.mem.get_suffix(from)
    }
    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        self.mem.get_promise()
    }
    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.log(&StorageOp::SetStopsign(s.clone()));
        self.mem.set_stopsign(s)
    }
    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        self.mem.get_stopsign()
    }
    fn trim(&mut self, idx: usize) -> StorageResult<()> {
        self.log(&StorageOp::Trim(idx));
        self.mem.trim(idx)
    }
    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.log(&StorageOp::SetCompactedIdx(idx));
        self.mem.set_compacted_idx(idx)
    }
    fn get_compacted_idx(&self) -> StorageResult<usize> {
        self.mem.get_compacted_idx()
    }
    fn set_snapshot(&mut self, snapshot: Option<NoSnapshot>) -> StorageResult<()> {
        self.log(&StorageOp::SetSnapshot(snapshot));
        self.mem.set_snapshot(snapshot)
    }
    fn get_snapshot(&self) -> StorageResult<Option<NoSnapshot>> {
        self.mem.get_snapshot()
    }
}
