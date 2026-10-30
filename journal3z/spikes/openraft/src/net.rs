//! Network adapter: openraft 0.9 `RaftNetworkFactory`/`RaftNetwork` over the harness `DelayLine`. A request is
//! delivered to the target Raft after the link delay; the response travels back through the delay line too.
//! For the fence test: per-node "cut" switches (partition) and an optional epoch guard (the lease epoch attached to
//! every message, refused below the receiver's `promised`; this is application code, not openraft).
use crate::TC;
use j3z_harness::DelayLine;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Timeout, Unreachable};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use openraft::{BasicNode, Raft};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, OnceLock};

pub struct Router {
    pub dl: Arc<DelayLine>,
    pub rt: tokio::runtime::Handle,
    /// index g*3 + node
    pub rafts: Vec<OnceLock<Raft<TC>>>,
    pub cut: [AtomicBool; 3],
    pub guard: AtomicBool,
    /// epoch (lease seq) each replica believes it holds; attached to what it sends
    pub lease_epoch: Vec<AtomicU64>,
    /// highest epoch each replica has seen (spike: in memory; the design keeps it durable)
    pub promised: Vec<AtomicU64>,
    pub fenced: AtomicU64,
    pub snap_msgs: AtomicU64,
    pub snap_bytes: AtomicU64,
}

impl Router {
    pub fn new(dl: Arc<DelayLine>, groups: usize) -> Arc<Self> {
        let n = groups * 3;
        Arc::new(Router {
            dl,
            rt: tokio::runtime::Handle::current(),
            rafts: (0..n).map(|_| OnceLock::new()).collect(),
            cut: Default::default(),
            guard: AtomicBool::new(false),
            lease_epoch: (0..n).map(|_| AtomicU64::new(0)).collect(),
            promised: (0..n).map(|_| AtomicU64::new(0)).collect(),
            fenced: AtomicU64::new(0),
            snap_msgs: AtomicU64::new(0),
            snap_bytes: AtomicU64::new(0),
        })
    }
    fn is_cut(&self, a: usize, b: usize) -> bool {
        a != b && (self.cut[a].load(SeqCst) || self.cut[b].load(SeqCst))
    }
}

pub struct NetFactory {
    pub r: Arc<Router>,
    pub g: usize,
    pub from: usize,
}

impl RaftNetworkFactory<TC> for NetFactory {
    type Network = Conn;
    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Conn {
        Conn { r: self.r.clone(), g: self.g, from: self.from, to: target as usize }
    }
}

pub struct Conn {
    r: Arc<Router>,
    g: usize,
    from: usize,
    to: usize,
}

enum NetErr {
    Unreachable,
    Timeout,
}

impl Conn {
    /// request -> delay line -> `f(target raft)` on the runtime -> response -> delay line -> caller.
    async fn call<T, F, Fut>(&self, size: usize, ttl: std::time::Duration, f: F) -> Result<T, NetErr>
    where
        F: FnOnce(Raft<TC>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (g, from, to) = (self.g, self.from, self.to);
        if self.r.is_cut(from, to) {
            return Err(NetErr::Unreachable);
        }
        let epoch = self.r.lease_epoch[g * 3 + from].load(SeqCst);
        let (tx, rx) = tokio::sync::oneshot::channel::<T>();
        let r = self.r.clone();
        self.r.dl.send(from, to, size, Box::new(move || {
            if r.is_cut(from, to) {
                return; // dropped: caller sees Unreachable
            }
            if r.guard.load(SeqCst) {
                let p = &r.promised[g * 3 + to];
                if epoch < p.load(SeqCst) {
                    r.fenced.fetch_add(1, SeqCst);
                    return; // fenced: the stale sender gets no answer
                }
                p.fetch_max(epoch, SeqCst);
            }
            let raft = r.rafts[g * 3 + to].get().expect("raft registered").clone();
            let r2 = r.clone();
            r.rt.spawn(async move {
                let resp = f(raft).await;
                if r2.is_cut(to, from) {
                    return;
                }
                r2.dl.send(to, from, 32, Box::new(move || {
                    let _ = tx.send(resp);
                }));
            });
        }));
        match tokio::time::timeout(ttl, rx).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(_)) => Err(NetErr::Unreachable),
            Err(_) => Err(NetErr::Timeout),
        }
    }

    fn err<E: std::error::Error>(&self, e: NetErr, action: RPCTypes, ttl: std::time::Duration) -> RPCError<u64, BasicNode, E> {
        match e {
            NetErr::Unreachable => RPCError::Unreachable(Unreachable::new(&std::io::Error::other("unreachable"))),
            NetErr::Timeout => RPCError::Timeout(Timeout { action, id: self.from as u64, target: self.to as u64, timeout: ttl }),
        }
    }
}

fn entries_size(rpc: &AppendEntriesRequest<TC>) -> usize {
    64 + rpc
        .entries
        .iter()
        .map(|e| {
            40 + match &e.payload {
                openraft::EntryPayload::Normal(b) => b.len(),
                _ => 64,
            }
        })
        .sum::<usize>()
}

impl RaftNetwork<TC> for Conn {
    async fn append_entries(&mut self, rpc: AppendEntriesRequest<TC>, option: RPCOption) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let ttl = option.hard_ttl();
        let size = entries_size(&rpc);
        match self.call(size, ttl, move |raft| async move { raft.append_entries(rpc).await }).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.to as u64, e))),
            Err(e) => Err(self.err(e, RPCTypes::AppendEntries, ttl)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TC>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>> {
        let ttl = option.hard_ttl();
        let size = 64 + rpc.data.len();
        self.r.snap_msgs.fetch_add(1, SeqCst);
        self.r.snap_bytes.fetch_add(size as u64, SeqCst);
        match self.call(size, ttl, move |raft| async move { raft.install_snapshot(rpc).await }).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.to as u64, e))),
            Err(e) => Err(self.err(e, RPCTypes::InstallSnapshot, ttl)),
        }
    }

    async fn vote(&mut self, rpc: VoteRequest<u64>, option: RPCOption) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let ttl = option.hard_ttl();
        match self.call(64, ttl, move |raft| async move { raft.vote(rpc).await }).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.to as u64, e))),
            Err(e) => Err(self.err(e, RPCTypes::Vote, ttl)),
        }
    }
}
