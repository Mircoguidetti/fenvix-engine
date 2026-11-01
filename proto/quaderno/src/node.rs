//! Una replica del registro.
//!
//! Protocollo: Raft (Ongaro & Ousterhout), con
//! - pre-voto e "fedeltà al leader" (una replica che ha sentito il leader da meno di `elect_min` non vota per altri):
//!   una replica isolata che rientra non disturba il leader;
//! - controllo del quorum: un leader che non sente nessuna delle altre due repliche da `elect_min` si dimette;
//! - conferma quando 2 repliche su 3 hanno fatto fdatasync (il leader conta se stesso) e la voce è del mandato corrente;
//! - letture lineari: posizione_confermata() = ReadIndex (giro di heartbeat con numero di sequenza);
//! - il leader valida le scritture (modalità validata) sulla finestra, che comprende anche le voci accettate ma non
//!   ancora confermate (vengono prima nell'ordine), e assegna la posizione (orologio ibrido 48+16 bit).
//!
//! Tutta la logica gira su un thread solo (`Node::run`), alimentato da un canale di eventi: messaggi delle altre
//! repliche, richieste dei client, fine dei fdatasync. Ogni giro del ciclo raccoglie tutti gli eventi pronti e poi
//! spedisce: così le scritture si raggruppano da sole (un fdatasync e un messaggio per gruppo).

use crate::codec::*;
use crate::net::{spawn_out_link, FrameReader};
use crate::store::{self, Ckpt, DiskOp, RaftState};
use crate::window::{Val, Window};
use bytes::{Bytes, BytesMut};
use crossbeam_channel::{Receiver, Sender};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone, Debug)]
pub struct Config {
    pub id: u8,
    pub dir: PathBuf,
    pub listen_peer: String,
    pub listen_client: String,
    /// (id, indirizzo della porta repliche di quel nodo, di solito attraverso il proxy)
    pub peers: Vec<(u8, String)>,
    pub window_ms: u64,
    /// tetto della RAM per i corpi delle voci (byte); oltre, i valori più vecchi vanno letti dal disco
    pub mem_cap: u64,
    pub elect_min_ms: u64,
    pub elect_max_ms: u64,
    pub hb_ms: u64,
    pub seg_bytes: u64,
    pub max_inflight: u64,
}

pub enum Ev {
    Peer(Msg),
    Client(UnboundedSender<Vec<u8>>, u64, Req),
    Disk(u64, u64),
}

struct Reply {
    conn: UnboundedSender<Vec<u8>>,
    id: u64,
}
impl Reply {
    fn send(&self, r: &Resp) {
        let _ = self.conn.send(enc_resp(self.id, r));
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct Slot {
    term: u64,
    pos: Pos,
    ent: Option<Arc<Entry>>,
    seg: u32,
    off: u64,
    len: u32,
    /// uscita dalla finestra (o voce di servizio applicata): può lasciare la RAM
    done: bool,
    /// istante dell'aggiunta (diagnostica delle latenze)
    t: Instant,
}

struct Peer {
    id: u8,
    tx: UnboundedSender<Vec<u8>>,
    next: u64,
    matched: u64,
    acked_seq: u64,
    last_heard: Instant,
    last_sent: Instant,
    sent_commit: u64,
    probing: bool,
}

enum WaitKind {
    Get { db: Db, key: Bytes },
    Scan { db: Db, from: Bytes, to: Bytes, limit: u32 },
    Wait,
}
struct Waiter {
    kind: WaitKind,
    at: Pos,
    deadline: Instant,
    reply: Reply,
}

pub struct Node {
    cfg: Config,
    rx: Receiver<Ev>,
    disk_tx: Sender<DiskOp>,
    reader: store::Reader,
    term: u64,
    voted: i32,
    role: Role,
    leader: i32,
    leader_since: Instant,
    log: VecDeque<Slot>,
    front: u64,
    prev_term: u64,
    prev_pos: Pos,
    cur_seg: u32,
    cur_seg_len: u64,
    segs: BTreeMap<u32, (u64, u64)>,
    gen: u64,
    submitted: u64,
    durable: u64,
    verified: u64,
    commit: u64,
    applied: u64,
    commit_pos: Pos,
    all_match: u64,
    win: Window,
    txns: HashMap<TxnId, (u64, Pos)>,
    sessions: HashMap<(Bytes, u64), (u64, Pos)>,
    peers: Vec<Peer>,
    pending: BTreeMap<u64, Vec<Reply>>,
    reads: Vec<(u64, u64, Reply)>,
    need_hb: bool,
    hb_seq: u64,
    waiters: BTreeMap<(Pos, u64), Waiter>,
    wseq: u64,
    election_deadline: Instant,
    last_heard_leader: Instant,
    prevotes: HashSet<u8>,
    prevote_term: u64,
    votes: HashSet<u8>,
    noop_index: u64,
    resident: u64,
    evict_cursor: u64,
    evicted: u64,
    last_seen_seq: u64,
    last_ack_sent: u64,
    last_trim: Instant,
    disk_bytes: u64,
    /// follower: le voci ricevute vanno prima al disco, poi nella finestra
    defer_index: bool,
    to_index: Vec<u64>,
    /// diagnostica (µs): aggiunta -> fdatasync locale, aggiunta -> ack di un follower, aggiunta -> conferma; durata dei giri del ciclo
    h_durable: hdrhistogram::Histogram<u64>,
    h_ack: hdrhistogram::Histogram<u64>,
    h_commit: hdrhistogram::Histogram<u64>,
    h_loop: hdrhistogram::Histogram<u64>,
}

fn rand_ms(a: u64, b: u64) -> Duration {
    Duration::from_millis(a + rand::random::<u64>() % (b - a + 1))
}

pub fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)?.parse().ok()))
        .unwrap_or(0)
}

/// Avvia la replica: rete su un runtime tokio, logica sul thread chiamante (non ritorna).
pub fn start(cfg: Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.dir)?;
    let (ev_tx, ev_rx) = crossbeam_channel::unbounded::<Ev>();
    let (disk_tx, disk_rx) = crossbeam_channel::unbounded::<DiskOp>();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(std::env::var("PQ_IO_THREADS").ok().and_then(|x| x.parse().ok()).unwrap_or(1)).enable_all().build()?;
    let mut peers = vec![];
    {
        let _g = rt.enter();
        for (id, addr) in &cfg.peers {
            peers.push((*id, spawn_out_link(addr.clone())));
        }
        let lp = rt.block_on(TcpListener::bind(&cfg.listen_peer))?;
        let lc = rt.block_on(TcpListener::bind(&cfg.listen_client))?;
        let t = ev_tx.clone();
        rt.spawn(async move {
            loop {
                let Ok((s, _)) = lp.accept().await else { continue };
                let _ = s.set_nodelay(true);
                let t = t.clone();
                tokio::spawn(async move {
                    let mut r = FrameReader::new(s);
                    loop {
                        // un collegamento muto per 3 s viene chiuso (partizione silenziosa del proxy)
                        match tokio::time::timeout(Duration::from_secs(3), r.next()).await {
                            Ok(Ok(body)) => match dec_msg(body) {
                                Ok(m) => {
                                    if t.send(Ev::Peer(m)).is_err() {
                                        return;
                                    }
                                }
                                Err(_) => return,
                            },
                            _ => return,
                        }
                    }
                });
            }
        });
        let t = ev_tx.clone();
        rt.spawn(async move {
            loop {
                let Ok((s, _)) = lc.accept().await else { continue };
                let _ = s.set_nodelay(true);
                let t = t.clone();
                tokio::spawn(async move {
                    let (rd, mut wr) = s.into_split();
                    let (rtx, mut rrx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
                    tokio::spawn(async move {
                        let mut buf = Vec::with_capacity(1 << 16);
                        while let Some(f) = rrx.recv().await {
                            buf.clear();
                            buf.extend_from_slice(&f);
                            while buf.len() < (4 << 20) {
                                match rrx.try_recv() {
                                    Ok(m) => buf.extend_from_slice(&m),
                                    Err(_) => break,
                                }
                            }
                            if wr.write_all(&buf).await.is_err() {
                                return;
                            }
                        }
                    });
                    let mut r = FrameReader::new(rd);
                    while let Ok(body) = r.next().await {
                        match dec_req(body) {
                            Ok((id, q)) => {
                                if t.send(Ev::Client(rtx.clone(), id, q)).is_err() {
                                    return;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                });
            }
        });
    }
    let dt = ev_tx.clone();
    store::spawn_disk(cfg.dir.clone(), disk_rx, cfg.seg_bytes, move |gen, last| {
        let _ = dt.send(Ev::Disk(gen, last));
    });
    let mut node = Node::recover(cfg, ev_rx, disk_tx, peers)?;
    eprintln!("replica {} pronta: indice {}..{} mandato {}", node.cfg.id, node.front, node.last_index(), node.term);
    println!("READY");
    node.run();
    drop(rt);
    Ok(())
}

impl Node {
    fn recover(cfg: Config, rx: Receiver<Ev>, disk_tx: Sender<DiskOp>, peers: Vec<(u8, UnboundedSender<Vec<u8>>)>) -> anyhow::Result<Node> {
        let st: RaftState = std::fs::read(cfg.dir.join("state")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or(RaftState { term: 0, voted: -1 });
        let ck: Option<Ckpt> = std::fs::read(cfg.dir.join("ckpt")).ok().and_then(|b| serde_json::from_slice(&b).ok());
        let (recs, last_seg, last_len) = store::recover(&cfg.dir)?;
        let now = Instant::now();
        let ckv = ck.as_ref();
        let prev_index = ckv.map(|c| c.prev_index).unwrap_or(0);
        let genesis = ck.is_none() && recs.first().map(|r| r.entry.index == 1).unwrap_or(true);
        let mut n = Node {
            rx,
            disk_tx,
            reader: store::Reader::new(cfg.dir.clone()),
            term: st.term,
            voted: st.voted,
            role: Role::Follower,
            leader: -1,
            leader_since: now,
            log: VecDeque::with_capacity(1 << 21),
            front: prev_index + 1,
            prev_term: ckv.map(|c| c.prev_term).unwrap_or(0),
            prev_pos: ckv.map(|c| c.prev_pos).unwrap_or(0),
            cur_seg: if recs.is_empty() && last_seg == 0 { 1 } else { last_seg.max(1) },
            cur_seg_len: last_len,
            segs: BTreeMap::new(),
            gen: 0,
            submitted: 0,
            durable: 0,
            verified: 0,
            commit: prev_index,
            applied: prev_index,
            commit_pos: ckv.map(|c| c.prev_pos).unwrap_or(0),
            all_match: prev_index,
            win: Window::new(genesis),
            txns: HashMap::with_capacity(1 << 21),
            sessions: HashMap::new(),
            peers: peers
                .into_iter()
                .map(|(id, tx)| Peer { id, tx, next: 1, matched: 0, acked_seq: 0, last_heard: now, last_sent: now, sent_commit: 0, probing: false })
                .collect(),
            pending: BTreeMap::new(),
            reads: vec![],
            need_hb: false,
            hb_seq: 0,
            waiters: BTreeMap::new(),
            wseq: 0,
            election_deadline: now + rand_ms(cfg.elect_min_ms, cfg.elect_max_ms),
            last_heard_leader: now - Duration::from_secs(3600),
            prevotes: HashSet::new(),
            prevote_term: 0,
            votes: HashSet::new(),
            noop_index: u64::MAX,
            resident: 0,
            evict_cursor: 0,
            evicted: 0,
            last_seen_seq: 0,
            last_ack_sent: 0,
            last_trim: now,
            disk_bytes: 0,
            defer_index: false,
            to_index: vec![],
            h_durable: hdrhistogram::Histogram::new(3).unwrap(),
            h_ack: hdrhistogram::Histogram::new(3).unwrap(),
            h_commit: hdrhistogram::Histogram::new(3).unwrap(),
            h_loop: hdrhistogram::Histogram::new(3).unwrap(),
            cfg,
        };
        if let Some(c) = ckv {
            for (h, p) in &c.flushed {
                if let Some(db) = store::unhex16(h) {
                    n.win.dbs.entry(db).or_default().flushed = *p;
                }
            }
        }
        for r in recs {
            let e = r.entry;
            if e.index < n.front {
                continue;
            }
            assert_eq!(e.index, n.last_index() + 1, "registro non contiguo sul disco");
            let len = e.raw.len() as u64;
            n.segs.entry(r.seg).or_insert((e.index, e.index)).1 = e.index;
            n.disk_bytes += len;
            n.push_slot(Arc::new(e), r.seg, r.off);
        }
        n.submitted = n.last_index();
        n.durable = n.last_index();
        if n.cur_seg_len >= n.cfg.seg_bytes {
            n.cur_seg += 1;
            n.cur_seg_len = 0;
        }
        Ok(n)
    }

    fn me(&self) -> u8 {
        self.cfg.id
    }
    fn last_index(&self) -> u64 {
        self.front + self.log.len() as u64 - 1
    }
    fn last_term(&self) -> u64 {
        self.log.back().map(|s| s.term).unwrap_or(self.prev_term)
    }
    fn last_pos(&self) -> Pos {
        self.log.back().map(|s| s.pos).unwrap_or(self.prev_pos)
    }
    fn slot(&self, i: u64) -> Option<&Slot> {
        if i < self.front {
            return None;
        }
        self.log.get((i - self.front) as usize)
    }
    fn term_at(&self, i: u64) -> Option<u64> {
        if i == 0 {
            return Some(0);
        }
        if i + 1 == self.front {
            return Some(self.prev_term);
        }
        self.slot(i).map(|s| s.term)
    }
    fn pos_at(&self, i: u64) -> Pos {
        if i + 1 == self.front || i == 0 {
            return self.prev_pos;
        }
        self.slot(i).map(|s| s.pos).unwrap_or(0)
    }
    fn entry(&mut self, i: u64) -> Arc<Entry> {
        let s = &self.log[(i - self.front) as usize];
        if let Some(e) = &s.ent {
            return e.clone();
        }
        let (seg, off, len) = (s.seg, s.off, s.len);
        Arc::new(self.reader.read(seg, off, len).unwrap_or_else(|e| panic!("lettura voce {i} da disco: {e}")))
    }

    fn persist_state(&self) {
        let s = serde_json::to_vec(&RaftState { term: self.term, voted: self.voted }).unwrap();
        store::write_atomic(&self.cfg.dir, "state", &s).expect("scrittura stato");
    }

    // -------------------------------------------------------------------------------------------- registro locale

    /// Aggiunge una voce in coda (in RAM e nella finestra); il disco la riceve al prossimo `submit_disk`.
    fn push_slot(&mut self, e: Arc<Entry>, seg: u32, off: u64) {
        if self.defer_index {
            self.to_index.push(e.index);
        } else {
            self.index_entry(&e);
        }
        self.resident += e.raw.len() as u64;
        self.log.push_back(Slot { term: e.term, pos: e.pos, len: e.raw.len() as u32, ent: Some(e), seg, off, done: false, t: Instant::now() });
    }

    fn flush_index(&mut self) {
        for i in std::mem::take(&mut self.to_index) {
            let e = self.log[(i - self.front) as usize].ent.clone().expect("voce appena aggiunta");
            self.index_entry(&e);
        }
    }

    fn index_entry(&mut self, e: &Arc<Entry>) {
        let index = e.index;
        if let Body::Rec { txn, db, session, writes } = &e.body {
            self.win.apply(index, e.pos, *txn, db, writes, e.raw.len() as u32);
            self.txns.insert(*txn, (index, e.pos));
            if let Some(s) = session {
                self.sessions.insert((s.lsid.clone(), s.txn_number), (index, e.pos));
            }
        }
    }

    fn append_new(&mut self, e: Entry) {
        let len = e.raw.len() as u64;
        if self.cur_seg_len > 0 && self.cur_seg_len + len > self.cfg.seg_bytes {
            self.cur_seg += 1;
            self.cur_seg_len = 0;
        }
        let (seg, off) = (self.cur_seg, self.cur_seg_len);
        self.cur_seg_len += len;
        self.disk_bytes += len;
        self.segs.entry(seg).or_insert((e.index, e.index)).1 = e.index;
        self.push_slot(Arc::new(e), seg, off);
    }

    fn truncate_from(&mut self, i: u64) {
        assert!(i > self.commit, "troncamento di una voce confermata ({i} ≤ {})", self.commit);
        let (seg, off) = {
            let s = self.slot(i).unwrap();
            (s.seg, s.off)
        };
        while self.last_index() >= i {
            let idx = self.last_index();
            let e = self.entry(idx);
            let s = self.log.pop_back().unwrap();
            if s.ent.is_some() {
                self.resident -= s.len as u64;
            }
            self.disk_bytes -= s.len as u64;
            if let Body::Rec { db, session, .. } = &e.body {
                if let Some(txn) = self.win.unapply_last(db, idx) {
                    if self.txns.get(&txn).map(|x| x.0) == Some(idx) {
                        self.txns.remove(&txn);
                    }
                }
                if let Some(s) = session {
                    self.sessions.remove(&(s.lsid.clone(), s.txn_number));
                }
            }
        }
        let later: Vec<u32> = self.segs.range(seg + 1..).map(|(k, _)| *k).collect();
        for s in later {
            self.segs.remove(&s);
            self.reader.forget(s);
        }
        if off == 0 {
            self.segs.remove(&seg);
        } else if let Some(x) = self.segs.get_mut(&seg) {
            x.1 = i - 1;
        }
        self.reader.forget(seg);
        self.cur_seg = seg;
        self.cur_seg_len = off;
        self.gen += 1;
        self.submitted = self.submitted.min(i - 1);
        self.durable = self.durable.min(i - 1);
        self.verified = self.verified.min(i - 1);
        self.evict_cursor = self.evict_cursor.min(i);
        let _ = self.disk_tx.send(DiskOp::Truncate { seg, off });
    }

    fn submit_disk(&mut self) {
        let last = self.last_index();
        if self.submitted >= last {
            return;
        }
        let mut cur: Option<(u32, u64, Vec<Bytes>, u64)> = None;
        for i in self.submitted + 1..=last {
            let s = &self.log[(i - self.front) as usize];
            let raw = s.ent.as_ref().expect("voce non ancora su disco senza corpo").raw.clone();
            match &mut cur {
                Some((seg, _, v, l)) if *seg == s.seg => {
                    v.push(raw);
                    *l = i;
                }
                _ => {
                    if let Some((seg, off, v, l)) = cur.take() {
                        let _ = self.disk_tx.send(DiskOp::Append { seg, off, data: v, last: l, gen: self.gen });
                    }
                    cur = Some((s.seg, s.off, vec![raw], i));
                }
            }
        }
        if let Some((seg, off, v, l)) = cur {
            let _ = self.disk_tx.send(DiskOp::Append { seg, off, data: v, last: l, gen: self.gen });
        }
        self.submitted = last;
    }

    fn next_pos(&self) -> Pos {
        (self.last_pos() + 1).max(pos_make(now_ms(), 0))
    }

    // -------------------------------------------------------------------------------------------- ciclo

    pub fn run(&mut self) {
        loop {
            match self.rx.recv_timeout(Duration::from_millis(1)) {
                Ok(ev) => {
                    // giri brevi: disco e rete partono presto (al più 1024 eventi o 0,5 ms per giro)
                    let t0 = Instant::now();
                    self.handle(ev);
                    for k in 0..1024 {
                        if k % 32 == 31 && t0.elapsed() > Duration::from_micros(500) {
                            break;
                        }
                        match self.rx.try_recv() {
                            Ok(ev) => self.handle(ev),
                            Err(_) => break,
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(_) => return,
            }
            let t1 = Instant::now();
            self.tick();
            self.submit_disk();
            self.send_out();
            self.h_loop.record(t1.elapsed().as_micros() as u64).ok();
        }
    }

    fn handle(&mut self, ev: Ev) {
        match ev {
            Ev::Peer(m) => self.on_msg(m),
            Ev::Client(conn, id, q) => self.on_req(Reply { conn, id }, q),
            Ev::Disk(gen, last) => {
                if gen == self.gen {
                    let nd = last.min(self.last_index());
                    if nd > self.durable {
                        if let Some(sl) = self.slot(nd) {
                            self.h_durable.record(sl.t.elapsed().as_micros() as u64).ok();
                        }
                    }
                    self.durable = self.durable.max(nd);
                }
                if self.role == Role::Leader {
                    self.maybe_commit();
                }
            }
        }
    }

    fn send_peer(&self, to: u8, m: &Msg) {
        let mut b = BytesMut::with_capacity(64);
        enc_msg(&mut b, m);
        if let Some(p) = self.peers.iter().find(|p| p.id == to) {
            let _ = p.tx.send(frame(&b));
        }
    }

    fn become_follower(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.voted = -1;
            self.persist_state();
        }
        if self.role == Role::Leader {
            eprintln!("replica {}: lascio la guida (mandato {})", self.me(), self.term);
            for (_, v) in std::mem::take(&mut self.pending) {
                for r in v {
                    r.send(&Resp::Ambiguous);
                }
            }
            for (_, _, r) in std::mem::take(&mut self.reads) {
                r.send(&Resp::NotLeader(-1));
            }
        }
        self.role = Role::Follower;
        self.leader = -1;
        self.verified = 0;
        self.prevotes.clear();
        self.votes.clear();
        self.noop_index = u64::MAX;
    }

    fn become_leader(&mut self) {
        eprintln!("replica {}: guida, mandato {} (indice {})", self.me(), self.term, self.last_index());
        self.role = Role::Leader;
        self.leader = self.me() as i32;
        self.leader_since = Instant::now();
        let last = self.last_index();
        let now = Instant::now();
        for p in &mut self.peers {
            p.next = last + 1;
            p.matched = 0;
            p.acked_seq = 0;
            p.last_heard = now;
            p.probing = false;
            p.sent_commit = 0;
        }
        let pos = self.next_pos();
        let e = Entry::encode(self.term, last + 1, pos, Body::Noop);
        self.noop_index = last + 1;
        self.append_new(e);
        self.need_hb = true;
    }

    fn majority_has_seq(&self, s: u64) -> bool {
        1 + self.peers.iter().filter(|p| p.acked_seq >= s).count() >= 2
    }

    // -------------------------------------------------------------------------------------------- messaggi Raft

    fn on_msg(&mut self, m: Msg) {
        match m {
            Msg::Append { term, leader, prev_index, prev_term, commit, all_match, seq, entries } => {
                self.on_append(term, leader, prev_index, prev_term, commit, all_match, seq, entries)
            }
            Msg::AppendResp { term, from, ok, ack, hint, seq } => self.on_append_resp(term, from, ok, ack, hint, seq),
            Msg::Vote { term, from, last_index, last_term, pre } => self.on_vote(term, from, last_index, last_term, pre),
            Msg::VoteResp { term, from, granted, pre } => self.on_vote_resp(term, from, granted, pre),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append(&mut self, term: u64, leader: u8, prev_index: u64, prev_term: u64, commit: u64, all_match: u64, seq: u64, entries: Vec<Bytes>) {
        let me = self.me();
        if term < self.term {
            self.send_peer(leader, &Msg::AppendResp { term: self.term, from: me, ok: false, ack: 0, hint: 0, seq });
            return;
        }
        if term > self.term || self.role != Role::Follower {
            self.become_follower(term);
        }
        let now = Instant::now();
        self.leader = leader as i32;
        self.last_heard_leader = now;
        self.election_deadline = now + rand_ms(self.cfg.elect_min_ms, self.cfg.elect_max_ms);
        self.last_seen_seq = self.last_seen_seq.max(seq);
        let fail = |n: &Node, hint: u64| {
            n.send_peer(leader, &Msg::AppendResp { term: n.term, from: me, ok: false, ack: 0, hint, seq });
        };
        if prev_index > self.last_index() {
            fail(self, self.last_index());
            return;
        }
        if prev_index + 1 >= self.front {
            let t = self.term_at(prev_index).unwrap();
            if t != prev_term {
                let mut i = prev_index;
                while i > self.front && self.term_at(i - 1) == Some(t) {
                    i -= 1;
                }
                fail(self, i.saturating_sub(1).max(self.commit));
                return;
            }
        }
        let mut idx = prev_index;
        self.defer_index = true;
        for raw in entries {
            idx += 1;
            if idx < self.front {
                continue;
            }
            let e = match Entry::decode(raw) {
                Ok(e) => e,
                Err(_) => {
                    self.defer_index = false;
                    self.submit_disk();
                    self.flush_index();
                    fail(self, idx - 1);
                    return;
                }
            };
            debug_assert_eq!(e.index, idx);
            if idx <= self.last_index() {
                if self.term_at(idx) == Some(e.term) {
                    continue;
                }
                self.flush_index();
                self.truncate_from(idx);
            }
            self.append_new(e);
        }
        self.defer_index = false;
        self.submit_disk();
        self.flush_index();
        self.verified = self.verified.max(idx);
        let c = commit.min(self.verified);
        if c > self.commit {
            self.advance_commit(c);
        }
        self.all_match = self.all_match.max(all_match.min(self.commit));
        let ack = self.durable.min(self.verified);
        self.last_ack_sent = ack;
        self.send_peer(leader, &Msg::AppendResp { term: self.term, from: me, ok: true, ack, hint: self.last_index(), seq });
    }

    fn on_append_resp(&mut self, term: u64, from: u8, ok: bool, ack: u64, hint: u64, seq: u64) {
        if term > self.term {
            self.become_follower(term);
            return;
        }
        if self.role != Role::Leader || term < self.term {
            return;
        }
        let Some(p) = self.peers.iter_mut().find(|p| p.id == from) else { return };
        p.last_heard = Instant::now();
        p.acked_seq = p.acked_seq.max(seq);
        if ok {
            if ack > p.matched {
                p.matched = ack;
                let t = self.log.get((ack.saturating_sub(self.front)) as usize).map(|s| s.t.elapsed().as_micros() as u64);
                if let Some(t) = t {
                    self.h_ack.record(t).ok();
                }
            }
            if p.probing {
                p.probing = false;
                p.next = p.matched + 1;
            }
            if p.next <= p.matched {
                p.next = p.matched + 1;
            }
        } else {
            p.probing = false;
            p.next = (p.matched + 1).max(p.next.min(hint + 1));
        }
        self.maybe_commit();
        self.check_reads();
    }

    fn maybe_commit(&mut self) {
        let mut m = vec![self.durable.min(self.last_index())];
        m.extend(self.peers.iter().map(|p| p.matched));
        m.sort_unstable_by(|a, b| b.cmp(a));
        let n = m[1];
        if n > self.commit && self.term_at(n) == Some(self.term) {
            self.advance_commit(n);
        }
        let am = *m.iter().min().unwrap();
        if am > self.all_match {
            self.all_match = am.min(self.commit);
        }
    }

    fn advance_commit(&mut self, n: u64) {
        let n = n.min(self.last_index());
        if n <= self.commit {
            return;
        }
        self.commit = n;
        for i in self.applied + 1..=n {
            let k = (i - self.front) as usize;
            let is_rec = matches!(self.log[k].ent.as_ref().map(|e| &e.body), Some(Body::Rec { .. }));
            if !is_rec {
                let e = self.entry(i);
                if let Body::Flushed(items) = &e.body {
                    for (db, p, etag) in items {
                        let d = self.win.dbs.entry(*db).or_default();
                        if *p > d.flushed {
                            d.flushed = *p;
                            d.etag = etag.clone();
                        }
                    }
                }
                if !matches!(e.body, Body::Rec { .. }) {
                    let sub = self.submitted;
                    let s = &mut self.log[k];
                    s.done = true;
                    // il corpo resta finché non è stato passato al disco
                    if i <= sub && s.ent.take().is_some() {
                        self.resident -= s.len as u64;
                    }
                }
            }
        }
        self.applied = n;
        self.commit_pos = self.pos_at(n);
        if self.role == Role::Leader {
            let rest = self.pending.split_off(&(n + 1));
            let done = std::mem::replace(&mut self.pending, rest);
            for (i, v) in done {
                let p = self.pos_at(i);
                if let Some(sl) = self.slot(i) {
                    self.h_commit.record(sl.t.elapsed().as_micros() as u64).ok();
                }
                for r in v {
                    r.send(&Resp::Committed(p));
                }
            }
            self.check_reads();
        }
        self.wake_waiters(false);
    }

    fn check_reads(&mut self) {
        if self.reads.is_empty() || self.commit < self.noop_index {
            return;
        }
        let mut keep = vec![];
        for (ri, s, r) in std::mem::take(&mut self.reads) {
            if self.commit >= ri && self.majority_has_seq(s) {
                r.send(&Resp::Pos(self.commit_pos));
            } else {
                keep.push((ri, s, r));
            }
        }
        self.reads = keep;
    }

    fn on_vote(&mut self, term: u64, from: u8, li: u64, lt: u64, pre: bool) {
        let me = self.me();
        let up_to_date = (lt, li) >= (self.last_term(), self.last_index());
        let now = Instant::now();
        let heard_recently = self.role == Role::Leader || (self.leader >= 0 && now.duration_since(self.last_heard_leader) < Duration::from_millis(self.cfg.elect_min_ms));
        if pre {
            let granted = term > self.term && up_to_date && !heard_recently;
            self.send_peer(from, &Msg::VoteResp { term, from: me, granted, pre: true });
            return;
        }
        if term < self.term || heard_recently {
            self.send_peer(from, &Msg::VoteResp { term: self.term, from: me, granted: false, pre: false });
            return;
        }
        if term > self.term {
            self.become_follower(term);
        }
        let granted = (self.voted == -1 || self.voted == from as i32) && up_to_date;
        if granted {
            self.voted = from as i32;
            self.persist_state();
            self.election_deadline = now + rand_ms(self.cfg.elect_min_ms, self.cfg.elect_max_ms);
        }
        self.send_peer(from, &Msg::VoteResp { term: self.term, from: me, granted, pre: false });
    }

    fn on_vote_resp(&mut self, term: u64, from: u8, granted: bool, pre: bool) {
        if pre {
            if self.role != Role::Leader && granted && term == self.prevote_term && term == self.term + 1 {
                self.prevotes.insert(from);
                if self.prevotes.len() + 1 >= 2 {
                    self.start_election();
                }
            }
            return;
        }
        if term > self.term {
            self.become_follower(term);
            return;
        }
        if self.role == Role::Candidate && term == self.term && granted {
            self.votes.insert(from);
            if self.votes.len() + 1 >= 2 {
                self.become_leader();
            }
        }
    }

    fn start_prevote(&mut self) {
        self.prevotes.clear();
        self.prevote_term = self.term + 1;
        let m = Msg::Vote { term: self.term + 1, from: self.me(), last_index: self.last_index(), last_term: self.last_term(), pre: true };
        for p in &self.peers {
            self.send_peer(p.id, &m);
        }
        self.election_deadline = Instant::now() + rand_ms(self.cfg.elect_min_ms, self.cfg.elect_max_ms);
    }

    fn start_election(&mut self) {
        self.term += 1;
        self.voted = self.me() as i32;
        self.persist_state();
        self.role = Role::Candidate;
        self.leader = -1;
        self.votes.clear();
        self.prevotes.clear();
        eprintln!("replica {}: candidata, mandato {}", self.me(), self.term);
        let m = Msg::Vote { term: self.term, from: self.me(), last_index: self.last_index(), last_term: self.last_term(), pre: false };
        for p in &self.peers {
            self.send_peer(p.id, &m);
        }
        self.election_deadline = Instant::now() + rand_ms(self.cfg.elect_min_ms, self.cfg.elect_max_ms);
    }

    // -------------------------------------------------------------------------------------------- spedizioni

    fn raw_at(&mut self, i: u64) -> Bytes {
        let s = &self.log[(i - self.front) as usize];
        if let Some(e) = &s.ent {
            return e.raw.clone();
        }
        self.entry(i).raw.clone()
    }

    fn send_out(&mut self) {
        let me = self.me();
        if self.role != Role::Leader {
            // follower: conferma i nuovi fdatasync al leader
            let ack = self.durable.min(self.verified);
            if self.leader >= 0 && ack > self.last_ack_sent {
                self.last_ack_sent = ack;
                self.send_peer(self.leader as u8, &Msg::AppendResp { term: self.term, from: me, ok: true, ack, hint: self.last_index(), seq: self.last_seen_seq });
            }
            return;
        }
        if self.need_hb {
            self.hb_seq += 1;
        }
        let now = Instant::now();
        let last = self.last_index();
        let hb = Duration::from_millis(self.cfg.hb_ms);
        for k in 0..self.peers.len() {
            let (next, matched, probing, last_sent, sent_commit) = {
                let p = &self.peers[k];
                (p.next, p.matched, p.probing, p.last_sent, p.sent_commit)
            };
            let mut next = next.max(self.front);
            let mut entries = vec![];
            let first = next;
            if !probing {
                let mut bytes = 0usize;
                while next <= last && next - matched <= self.cfg.max_inflight && bytes < (2 << 20) {
                    let r = self.raw_at(next);
                    bytes += r.len();
                    entries.push(r);
                    next += 1;
                }
            }
            let send = !entries.is_empty() || self.need_hb || now.duration_since(last_sent) >= hb || sent_commit < self.commit;
            if !send {
                continue;
            }
            let prev_index = if probing { matched.max(self.front - 1) } else { first - 1 };
            let prev_term = self.term_at(prev_index).unwrap_or(0);
            let m = Msg::Append {
                term: self.term,
                leader: me,
                prev_index,
                prev_term,
                commit: self.commit,
                all_match: self.all_match,
                seq: self.hb_seq,
                entries,
            };
            let mut b = BytesMut::with_capacity(256);
            enc_msg(&mut b, &m);
            let p = &mut self.peers[k];
            let _ = p.tx.send(frame(&b));
            if !probing {
                p.next = next;
            }
            p.last_sent = now;
            p.sent_commit = self.commit;
        }
        self.need_hb = false;
    }

    // -------------------------------------------------------------------------------------------- tempo

    fn tick(&mut self) {
        let now = Instant::now();
        let emin = Duration::from_millis(self.cfg.elect_min_ms);
        match self.role {
            Role::Leader => {
                // controllo del quorum
                let recent = self.peers.iter().filter(|p| now.duration_since(p.last_heard) < emin).count();
                if recent == 0 && now.duration_since(self.leader_since) > emin {
                    eprintln!("replica {}: nessun contatto con le altre repliche da {:?}, mi dimetto", self.me(), emin);
                    let t = self.term;
                    self.become_follower(t);
                } else {
                    for p in &mut self.peers {
                        if !p.probing && p.next > p.matched + 1 && now.duration_since(p.last_heard) > Duration::from_millis(1000) {
                            p.probing = true;
                            p.next = p.matched + 1;
                        }
                    }
                }
            }
            _ => {
                if now >= self.election_deadline {
                    self.start_prevote();
                }
            }
        }
        // attese scadute
        if !self.waiters.is_empty() {
            self.wake_waiters(true);
        }
        if now.duration_since(self.last_trim) >= Duration::from_millis(200) {
            self.last_trim = now;
            self.trim();
        }
    }

    fn wake_waiters(&mut self, check_deadline: bool) {
        let cp = self.commit_pos;
        let mut ready = vec![];
        while let Some((&(p, s), _)) = self.waiters.first_key_value() {
            if p > cp {
                break;
            }
            ready.push(self.waiters.remove(&(p, s)).unwrap());
        }
        if check_deadline {
            let now = Instant::now();
            let late: Vec<(Pos, u64)> = self.waiters.iter().filter(|(_, w)| w.deadline <= now).map(|(k, _)| *k).collect();
            for k in late {
                let w = self.waiters.remove(&k).unwrap();
                w.reply.send(&Resp::Behind(cp));
            }
        }
        for w in ready {
            self.answer_read(w.kind, w.at, w.reply);
        }
    }

    fn answer_read(&mut self, kind: WaitKind, at: Pos, reply: Reply) {
        let (log, front, reader) = (&self.log, self.front, &mut self.reader);
        let mut load = |i: u64, wi: u16| load_val(log, front, reader, i, wi);
        match kind {
            WaitKind::Get { db, key } => {
                let g = self.win.get(&db, &key, at, &mut load);
                reply.send(&Resp::Get(g));
            }
            WaitKind::Scan { db, from, to, limit } => {
                let v = self.win.scan(&db, &from, &to, at, limit as usize, &mut load);
                reply.send(&Resp::Scan(v));
            }
            WaitKind::Wait => reply.send(&Resp::Pos(self.commit_pos)),
        }
    }

    fn trim(&mut self) {
        let cutoff = pos_make(now_ms().saturating_sub(self.cfg.window_ms), 0);
        let cp = self.commit_pos;
        let mut gone = vec![];
        {
            let (log, front, reader) = (&self.log, self.front, &mut self.reader);
            let mut load = |i: u64, wi: u16| load_val(log, front, reader, i, wi);
            let dbs: Vec<Db> = self.win.dbs.iter().filter(|(_, d)| d.recs.front().map(|r| r.pos <= d.flushed && r.pos < cutoff).unwrap_or(false)).map(|(k, _)| *k).collect();
            for db in dbs {
                while let Some(r) = self.win.trim_one(&db, cutoff, cp, &mut load) {
                    gone.push((r.index, r.txn));
                }
            }
        }
        for (i, txn) in gone {
            if self.txns.get(&txn).map(|x| x.0) == Some(i) {
                self.txns.remove(&txn);
            }
            if i >= self.front {
                let sub = self.submitted;
                let s = &mut self.log[(i - self.front) as usize];
                s.done = true;
                if i <= sub && s.ent.take().is_some() {
                    self.resident -= s.len as u64;
                }
            }
        }
        // sessioni: si tengono quanto i txn (approssimazione: si potano con lo stesso limite di indice)
        if self.sessions.len() > 2 * self.txns.len() + 1024 {
            let f = self.front;
            self.sessions.retain(|_, v| v.0 >= f);
        }
        // fronte del registro in RAM: voci uscite dalla finestra, confermate e presenti su tutte e tre le repliche
        let am = if self.role == Role::Leader { self.all_match } else { self.all_match.min(self.commit) };
        while let Some(s) = self.log.front() {
            if !(s.done && self.front <= self.commit && self.front <= am && self.front <= self.durable) {
                break;
            }
            let s = self.log.pop_front().unwrap();
            if s.ent.is_some() {
                self.resident -= s.len as u64;
            }
            self.prev_term = s.term;
            self.prev_pos = s.pos;
            self.front += 1;
        }
        // segmenti interamente prima del fronte: si cancellano dopo aver scritto il checkpoint
        let dead: Vec<u32> = self.segs.iter().filter(|(s, (_, l))| **s != self.cur_seg && *l < self.front).map(|(s, _)| *s).collect();
        if !dead.is_empty() {
            let ck = Ckpt {
                prev_index: self.front - 1,
                prev_term: self.prev_term,
                prev_pos: self.prev_pos,
                flushed: self.win.dbs.iter().filter(|(_, d)| d.flushed > 0).map(|(k, d)| (store::hex(k), d.flushed)).collect(),
            };
            store::write_atomic(&self.cfg.dir, "ckpt", &serde_json::to_vec(&ck).unwrap()).expect("checkpoint");
            self.win.genesis = false;
            for s in dead {
                self.segs.remove(&s);
                self.reader.forget(s);
                let _ = self.disk_tx.send(DiskOp::Delete { seg: s });
            }
            self.disk_bytes = 0; // ricalcolato sotto
            let segs: Vec<u32> = self.segs.keys().copied().collect();
            for s in segs {
                if let Ok(m) = std::fs::metadata(store::seg_path(&self.cfg.dir, s)) {
                    self.disk_bytes += m.len();
                }
            }
        }
        // tetto di RAM: i valori più vecchi ancora nella finestra vanno su disco
        if self.resident > self.cfg.mem_cap {
            let target = self.cfg.mem_cap / 10 * 9;
            let mut i = self.evict_cursor.max(self.front);
            let lim = self.durable.min(self.last_index());
            while self.resident > target && i <= lim {
                let k = (i - self.front) as usize;
                if let Some(e) = self.log[k].ent.clone() {
                    if let Body::Rec { db, writes, .. } = &e.body {
                        self.win.evict(db, e.pos, i, writes);
                    }
                    self.log[k].ent = None;
                    self.resident -= self.log[k].len as u64;
                    self.evicted += 1;
                }
                i += 1;
            }
            self.evict_cursor = i;
        }
    }

    // -------------------------------------------------------------------------------------------- client

    fn park(&mut self, kind: WaitKind, at: Pos, wait_ms: u32, reply: Reply) {
        if wait_ms == 0 {
            reply.send(&Resp::Behind(self.commit_pos));
            return;
        }
        self.wseq += 1;
        self.waiters.insert((at, self.wseq), Waiter { kind, at, deadline: Instant::now() + Duration::from_millis(wait_ms as u64), reply });
    }

    fn on_req(&mut self, reply: Reply, q: Req) {
        match q {
            Req::Commit(c) => self.on_commit(reply, c),
            Req::Get { db, key, at, wait_ms, spec } => {
                if spec {
                    if self.role != Role::Leader {
                        reply.send(&Resp::NotLeader(self.leader));
                        return;
                    }
                    self.answer_read(WaitKind::Get { db, key }, Pos::MAX, reply);
                    return;
                }
                if at <= self.commit_pos {
                    self.answer_read(WaitKind::Get { db, key }, at, reply);
                } else {
                    self.park(WaitKind::Get { db, key }, at, wait_ms, reply);
                }
            }
            Req::Scan { db, from, to, at, limit, wait_ms } => {
                if at <= self.commit_pos {
                    self.answer_read(WaitKind::Scan { db, from, to, limit }, at, reply);
                } else {
                    self.park(WaitKind::Scan { db, from, to, limit }, at, wait_ms, reply);
                }
            }
            Req::Wait { pos, timeout_ms } => {
                if pos <= self.commit_pos {
                    reply.send(&Resp::Pos(self.commit_pos));
                } else {
                    self.park(WaitKind::Wait, pos, timeout_ms, reply);
                }
            }
            Req::Confirmed => {
                if self.role != Role::Leader {
                    reply.send(&Resp::NotLeader(self.leader));
                    return;
                }
                self.reads.push((self.commit, self.hb_seq + 1, reply));
                self.need_hb = true;
            }
            Req::Flushable { dbs, upto } => {
                let upto = upto.min(self.commit_pos);
                let mut out = vec![];
                let mut bytes = 0usize;
                for db in dbs {
                    let Some(d) = self.win.dbs.get(&db) else { continue };
                    let fl = d.flushed;
                    let idx: Vec<(u64, Pos)> = d.recs.iter().filter(|r| r.pos > fl && r.pos <= upto).map(|r| (r.index, r.pos)).collect();
                    let mut recs = vec![];
                    for (i, p) in idx {
                        if bytes > (4 << 20) {
                            break;
                        }
                        let e = self.entry(i);
                        if let Body::Rec { writes, .. } = &e.body {
                            bytes += e.raw.len();
                            recs.push(FlushRec { pos: p, writes: writes.clone() });
                        }
                    }
                    if !recs.is_empty() {
                        out.push((db, recs));
                    }
                }
                reply.send(&Resp::Flushable(out));
            }
            Req::FlushDone { items } => {
                if self.role != Role::Leader {
                    reply.send(&Resp::NotLeader(self.leader));
                    return;
                }
                let idx = self.last_index() + 1;
                let e = Entry::encode(self.term, idx, self.next_pos(), Body::Flushed(items));
                self.append_new(e);
                self.pending.entry(idx).or_default().push(reply);
            }
            Req::Pending { limit } => {
                let mut v: Vec<(Db, Pos)> = self
                    .win
                    .dbs
                    .iter()
                    .filter_map(|(k, d)| {
                        let i = d.recs.partition_point(|r| r.pos <= d.flushed);
                        d.recs.get(i).filter(|r| r.pos <= self.commit_pos).map(|r| (*k, r.pos))
                    })
                    .collect();
                v.sort_by_key(|x| x.1);
                v.truncate(limit as usize);
                reply.send(&Resp::Pending(v));
            }
            Req::Status => {
                let unflushed: u64 = self.win.dbs.values().map(|d| (d.recs.len() - d.recs.partition_point(|r| r.pos <= d.flushed)) as u64).sum();
                let minp = self.win.dbs.values().filter_map(|d| d.recs.get(d.recs.partition_point(|r| r.pos <= d.flushed)).map(|r| r.pos)).min().unwrap_or(0);
                let st = Status {
                    id: self.me(),
                    role: format!("{:?}", self.role),
                    term: self.term,
                    leader: self.leader,
                    last_index: self.last_index(),
                    commit_index: self.commit,
                    commit_pos: self.commit_pos,
                    durable: self.durable,
                    log_front: self.front,
                    dbs: self.win.dbs.len() as u64,
                    keys: self.win.keys_count(),
                    versions: self.win.versions,
                    resident_bytes: self.resident,
                    evicted: self.evicted,
                    unflushed_recs: unflushed,
                    min_unflushed_pos: minp,
                    segments: self.segs.len() as u64,
                    disk_bytes: self.disk_bytes,
                    rss_kb: rss_kb(),
                    diag: format!(
                        "durable p50/p99 {}/{} ack {}/{} commit {}/{} giro-uscite {}/{} max {}",
                        self.h_durable.value_at_quantile(0.5), self.h_durable.value_at_quantile(0.99),
                        self.h_ack.value_at_quantile(0.5), self.h_ack.value_at_quantile(0.99),
                        self.h_commit.value_at_quantile(0.5), self.h_commit.value_at_quantile(0.99),
                        self.h_loop.value_at_quantile(0.5), self.h_loop.value_at_quantile(0.99), self.h_loop.max()
                    ),
                };
                self.h_durable.reset();
                self.h_ack.reset();
                self.h_commit.reset();
                self.h_loop.reset();
                reply.send(&Resp::Status(st));
            }
            Req::Dump { from_index, max } => {
                let mut v = vec![];
                let mut i = from_index.max(self.front);
                while i <= self.commit && v.len() < max as usize {
                    let e = self.entry(i);
                    if let Body::Rec { txn, .. } = &e.body {
                        v.push((i, *txn, e.pos));
                    }
                    i += 1;
                }
                reply.send(&Resp::Dump(v));
            }
        }
    }

    fn on_commit(&mut self, reply: Reply, c: CommitReq) {
        if self.role != Role::Leader {
            reply.send(&Resp::NotLeader(self.leader));
            return;
        }
        // stesso id (o stessa sessione) già nel registro: stessa posizione
        let dup = self.txns.get(&c.txn).copied().or_else(|| c.session.as_ref().and_then(|s| self.sessions.get(&(s.lsid.clone(), s.txn_number)).copied()));
        if let Some((i, p)) = dup {
            if i <= self.commit {
                reply.send(&Resp::Committed(p));
            } else {
                self.pending.entry(i).or_default().push(reply);
            }
            return;
        }
        // S più nuova del registro di questo leader: ha letto da un leader più nuovo, questo è deposto senza saperlo
        // (S_SCONOSCIUTA in docs/protocollo.md): il client cerca il leader e rinvia
        if c.read_pos > self.last_pos() {
            reply.send(&Resp::NotLeader(-1));
            return;
        }
        if let Err(k) = self.win.validate(&c.db, c.read_pos, &c.writes, c.mode == Mode::Validated, &c.reads) {
            reply.send(&Resp::Conflict(k));
            return;
        }
        let idx = self.last_index() + 1;
        let pos = self.next_pos();
        let e = Entry::encode(self.term, idx, pos, Body::Rec { txn: c.txn, db: c.db, session: c.session, writes: c.writes });
        self.append_new(e);
        self.pending.entry(idx).or_default().push(reply);
    }
}

fn load_val(log: &VecDeque<Slot>, front: u64, reader: &mut store::Reader, i: u64, wi: u16) -> Bytes {
    let s = &log[(i - front) as usize];
    let e = match &s.ent {
        Some(e) => e.clone(),
        None => Arc::new(reader.read(s.seg, s.off, s.len).unwrap_or_else(|e| panic!("lettura valore {i}: {e}"))),
    };
    match &e.body {
        Body::Rec { writes, .. } => writes[wi as usize].val.clone(),
        _ => Bytes::new(),
    }
}

#[allow(dead_code)]
fn _unused(_: Val) {}
