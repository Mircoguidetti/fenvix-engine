//! Client delle repliche: connessioni multiplexate (id di richiesta), inseguimento del leader, rinvio dei commit
//! ambigui con lo stesso id di transazione (fa la parte del nodo d'ingresso senza stato).

use crate::codec::*;
use crate::net::FrameReader;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallErr {
    Timeout,
    Closed,
}

pub struct Conn {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Resp>>>>,
    next: AtomicU64,
    alive: Arc<AtomicBool>,
}

impl Conn {
    pub async fn connect(addr: &str) -> std::io::Result<Arc<Conn>> {
        let s = tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect"))??;
        s.set_nodelay(true)?;
        let (rd, mut wr) = s.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Resp>>>> = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let a2 = alive.clone();
        tokio::spawn(async move {
            let mut buf = Vec::with_capacity(1 << 16);
            while let Some(f) = rx.recv().await {
                buf.clear();
                buf.extend_from_slice(&f);
                while buf.len() < (1 << 20) {
                    match rx.try_recv() {
                        Ok(m) => buf.extend_from_slice(&m),
                        Err(_) => break,
                    }
                }
                if wr.write_all(&buf).await.is_err() {
                    a2.store(false, SeqCst);
                    return;
                }
            }
        });
        let p2 = pending.clone();
        let a3 = alive.clone();
        tokio::spawn(async move {
            let mut r = FrameReader::new(rd);
            while let Ok(body) = r.next().await {
                if let Ok((id, resp)) = dec_resp(body) {
                    if let Some(s) = p2.lock().remove(&id) {
                        let _ = s.send(resp);
                    }
                }
            }
            a3.store(false, SeqCst);
            p2.lock().clear(); // chi aspetta riceve Closed
        });
        Ok(Arc::new(Conn { tx, pending, next: AtomicU64::new(1), alive }))
    }

    pub fn alive(&self) -> bool {
        self.alive.load(Relaxed)
    }

    pub async fn call(&self, q: &Req, timeout: Duration) -> Result<Resp, CallErr> {
        if !self.alive() {
            return Err(CallErr::Closed);
        }
        let id = self.next.fetch_add(1, Relaxed);
        let (s, r) = oneshot::channel();
        self.pending.lock().insert(id, s);
        if self.tx.send(enc_req(id, q)).is_err() {
            self.pending.lock().remove(&id);
            return Err(CallErr::Closed);
        }
        match tokio::time::timeout(timeout, r).await {
            Ok(Ok(x)) => Ok(x),
            Ok(Err(_)) => Err(CallErr::Closed),
            Err(_) => {
                self.pending.lock().remove(&id);
                Err(CallErr::Timeout)
            }
        }
    }
}

/// Le tre repliche viste da un nodo d'ingresso.
pub struct Cluster {
    pub addrs: Vec<String>,
    conns: Vec<Vec<Mutex<Option<Arc<Conn>>>>>,
    rr: AtomicUsize,
    pub leader: AtomicI32,
    /// la posizione confermata più alta vista in una risposta (per letture non lineari "recenti")
    pub seen: AtomicU64,
}

#[derive(Debug, Clone)]
pub enum Outcome {
    /// confermato a questa posizione; `ambiguous` = almeno un tentativo è finito senza risposta certa; `busy` = quante
    /// volte il freno ha chiesto di ritentare
    Committed { pos: Pos, attempts: u32, ambiguous: bool, busy: u32 },
    Conflict(Bytes),
    /// tempo scaduto senza esito certo
    Unknown,
    /// tempo scaduto mentre il freno rifiutava (errore ritentabile pulito: il commit non è entrato nel registro)
    Rejected(u8),
}

impl Cluster {
    pub fn new(addrs: Vec<String>, conns_per_replica: usize) -> Arc<Cluster> {
        let conns = addrs.iter().map(|_| (0..conns_per_replica).map(|_| Mutex::new(None)).collect()).collect();
        Arc::new(Cluster { addrs, conns, rr: AtomicUsize::new(0), leader: AtomicI32::new(-1), seen: AtomicU64::new(0) })
    }

    pub fn n(&self) -> usize {
        self.addrs.len()
    }

    pub async fn conn(&self, r: usize) -> Result<Arc<Conn>, CallErr> {
        let k = self.rr.fetch_add(1, Relaxed) % self.conns[r].len();
        if let Some(c) = self.conns[r][k].lock().as_ref() {
            if c.alive() {
                return Ok(c.clone());
            }
        }
        let c = Conn::connect(&self.addrs[r]).await.map_err(|_| CallErr::Closed)?;
        *self.conns[r][k].lock() = Some(c.clone());
        Ok(c)
    }

    pub async fn call(&self, r: usize, q: &Req, timeout: Duration) -> Result<Resp, CallErr> {
        let c = self.conn(r).await?;
        c.call(q, timeout).await
    }

    fn leader_guess(&self) -> usize {
        let l = self.leader.load(Relaxed);
        if l >= 0 {
            l as usize
        } else {
            rand::random::<usize>() % self.n()
        }
    }

    fn note_pos(&self, p: Pos) {
        self.seen.fetch_max(p, Relaxed);
    }

    /// Commit con rinvii: NotLeader -> si va dal leader indicato; ambiguo/tempo scaduto -> stesso id, di nuovo.
    pub async fn commit(&self, c: &CommitReq, deadline: Duration) -> Outcome {
        let start = Instant::now();
        let q = Req::Commit(c.clone());
        let mut attempts = 0;
        let mut ambiguous = false;
        let mut busy = 0u32;
        let mut last_busy: Option<u8> = None;
        let mut target = self.leader_guess();
        loop {
            attempts += 1;
            let left = deadline.saturating_sub(start.elapsed());
            if left.is_zero() {
                // un rifiuto del freno vale solo se nessun tentativo precedente è rimasto senza risposta certa
                return match last_busy {
                    Some(r) if !ambiguous => Outcome::Rejected(r),
                    _ => Outcome::Unknown,
                };
            }
            match self.call(target, &q, left.min(Duration::from_millis(1500))).await {
                Ok(Resp::Committed(p)) => {
                    self.leader.store(target as i32, Relaxed);
                    self.note_pos(p);
                    return Outcome::Committed { pos: p, attempts, ambiguous, busy };
                }
                Ok(Resp::Busy { retry_ms, reason }) => {
                    busy += 1;
                    last_busy = Some(reason);
                    // attesa che raddoppia a ogni rifiuto (fino a 2 s), con un po' di caso: senza, 15.000 commit/s
                    // rifiutati e ritentati ogni ~150 ms sovraccaricavano il capo proprio mentre frenava
                    let base = (retry_ms as u64) << (busy - 1).min(5);
                    let base = base.min(2000);
                    let ms = base / 2 + rand::random::<u64>() % (base / 2 + 1);
                    tokio::time::sleep(Duration::from_millis(ms).min(deadline.saturating_sub(start.elapsed()))).await;
                }
                Ok(Resp::Conflict(k)) => {
                    self.leader.store(target as i32, Relaxed);
                    return Outcome::Conflict(k);
                }
                Ok(Resp::NotLeader(h)) => {
                    if h >= 0 && h as usize != target {
                        target = h as usize;
                        self.leader.store(h, Relaxed);
                        continue;
                    }
                    self.leader.store(-1, Relaxed);
                    target = (target + 1) % self.n();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(Resp::Ambiguous) | Err(CallErr::Timeout) => {
                    ambiguous = true;
                    self.leader.store(-1, Relaxed);
                    target = (target + 1) % self.n();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(CallErr::Closed) => {
                    // la richiesta può essere arrivata: ambiguo
                    ambiguous = true;
                    self.leader.store(-1, Relaxed);
                    target = (target + 1) % self.n();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(other) => panic!("risposta inattesa al commit: {other:?}"),
            }
        }
    }

    /// posizione_confermata() lineare (dal leader).
    pub async fn confirmed(&self, deadline: Duration) -> Option<Pos> {
        let start = Instant::now();
        let mut target = self.leader_guess();
        while start.elapsed() < deadline {
            match self.call(target, &Req::Confirmed, Duration::from_millis(500)).await {
                Ok(Resp::Pos(p)) => {
                    self.leader.store(target as i32, Relaxed);
                    self.note_pos(p);
                    return Some(p);
                }
                Ok(Resp::NotLeader(h)) if h >= 0 && h as usize != target => {
                    target = h as usize;
                    self.leader.store(h, Relaxed);
                }
                _ => {
                    self.leader.store(-1, Relaxed);
                    target = (target + 1) % self.n();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        None
    }

    /// Chiama il leader (per conferma_scarico, letture speculative): segue NotLeader.
    pub async fn call_leader(&self, q: &Req, deadline: Duration) -> Option<Resp> {
        let start = Instant::now();
        let mut target = self.leader_guess();
        while start.elapsed() < deadline {
            match self.call(target, q, Duration::from_millis(2000)).await {
                Ok(Resp::NotLeader(h)) => {
                    if h >= 0 && h as usize != target {
                        target = h as usize;
                        self.leader.store(h, Relaxed);
                    } else {
                        self.leader.store(-1, Relaxed);
                        target = (target + 1) % self.n();
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                Ok(r) => {
                    self.leader.store(target as i32, Relaxed);
                    return Some(r);
                }
                Err(_) => {
                    self.leader.store(-1, Relaxed);
                    target = (target + 1) % self.n();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        None
    }

    pub async fn status(&self, r: usize) -> Option<Status> {
        match self.call(r, &Req::Status, Duration::from_millis(2000)).await {
            Ok(Resp::Status(s)) => Some(s),
            _ => None,
        }
    }
}
