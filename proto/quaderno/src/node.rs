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
//! La logica del registro gira su un thread solo (`Node::run`), alimentato da un canale di eventi: messaggi delle altre
//! repliche, commit, fine dei fdatasync. Ogni giro del ciclo raccoglie gli eventi pronti e poi spedisce: così le
//! scritture si raggruppano da sole (un fdatasync e un messaggio per gruppo).
//!
//! Le letture (get, scan, attendi), `scaricabili`, `pendenti`, `stato_scarico` e `status` NON passano dal thread
//! principale: le servono i thread di rete leggendo la finestra (un lock per database) e la "posizione leggibile"
//! pubblicata dal thread principale su un canale `watch`. I lavori periodici del thread principale (potatura,
//! applicazione degli scarichi, indicizzazione sui follower, sfratto) procedono a passi con un limite di tempo, e un
//! cronometro per tipo di lavoro registra il blocco più lungo di ciascuno.

use crate::codec::*;
use crate::net::{spawn_out_link, FrameReader};
use crate::store::{self, Ckpt, DiskOp, RaftState};
use crate::window::{Loc, Window};
use parking_lot::Mutex;
use tokio::sync::watch;
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
    /// compressione zstd dei messaggi di replica (livello; 0 = spenta). Giro 4
    pub zstd: i32,
    /// soglie del freno sul disco (registro + dispensa), byte: rallenta, rifiuta, sola lettura (0 = disattivo)
    pub disk_soft: u64,
    pub disk_hard: u64,
    pub disk_ro: u64,
    /// allarme se il record non su S3 più vecchio supera questi secondi
    pub alarm_s: u64,
}

/// Costi stimati in RAM per il conto unico della memoria (byte). Tarati confrontando il conto con l'RSS misurato.
pub const C_REC: u64 = 150;
pub const C_VER: u64 = 64;
pub const C_KEY: u64 = 120;
pub const C_SLOT: u64 = 96;
pub const C_TXN: u64 = 48;
pub const C_BODY: u64 = 200;

pub enum Ev {
    Peer(Msg),
    Client(UnboundedSender<Vec<u8>>, u64, Req),
    Disk(u64, u64),
    /// run della dispensa scritto: per ogni database, fino a che posizione i record possono uscire dalla RAM
    Spilled(u64, Vec<(Db, Pos)>),
}

/// Richiesta al thread della dispensa.
pub struct SpillReq {
    pub cutoff: Pos,
    pub commit_pos: Pos,
    pub max_recs: usize,
}

/// Stato condiviso tra il thread principale e i thread di rete.
pub struct Shared {
    pub win: Window,
    /// posizione fino alla quale la replica sa rispondere: confermata E già indicizzata nella finestra
    pub readable: watch::Sender<Pos>,
    /// fotografia dello stato aggiornata dal thread principale ogni 100 ms
    pub status: Mutex<Status>,
    /// livello del freno pubblicato dal thread principale (solo sul leader; 0 altrimenti). Da 2 in su i commit si
    /// rifiutano già nel thread di rete, senza mettersi in coda dietro al thread principale (giro 4: era la coda a
    /// far scadere i tentativi e a lasciare esiti ignoti)
    pub brake: std::sync::atomic::AtomicU8,
    /// commit rifiutati dal freno nel thread di rete
    pub fast_busy: std::sync::atomic::AtomicU64,
}

/// Cronometro per tipo di lavoro del thread principale (criterio: nessun blocco sopra 1 ms).
pub const SW_NAMES: [&str; 16] = [
    "commit", "lettura_spec", "append_follower", "risposte_append", "voti", "disco_fatto", "indice_follower", "potatura",
    "scarichi_applicati", "sfratto", "fronte_e_segmenti", "invio_repliche", "invio_disco", "conferma", "fotografia", "altre_richieste",
];
#[derive(Clone, Copy, Default)]
pub struct Sw {
    pub n: u64,
    pub tot_us: u64,
    pub max_us: u64,
    pub over_1ms: u64,
}

/// Destinatario di una risposta: una richiesta sola, oppure un posto in un lotto (`CommitBatch`). Il lotto parte con
/// un solo messaggio quando tutti i suoi commit hanno un esito.
enum Reply {
    One { conn: UnboundedSender<Vec<u8>>, id: u64 },
    Part { agg: std::rc::Rc<std::cell::RefCell<Agg>>, k: usize },
}
struct Agg {
    conn: UnboundedSender<Vec<u8>>,
    id: u64,
    res: Vec<Option<Resp>>,
    left: usize,
}
impl Reply {
    fn send(&self, r: &Resp) {
        match self {
            Reply::One { conn, id } => {
                let _ = conn.send(enc_resp(*id, r));
            }
            Reply::Part { agg, k } => {
                let mut a = agg.borrow_mut();
                if a.res[*k].is_some() {
                    return;
                }
                a.res[*k] = Some(r.clone());
                a.left -= 1;
                if a.left == 0 {
                    let v: Vec<Resp> = a.res.drain(..).map(|x| x.unwrap()).collect();
                    let _ = a.conn.send(enc_resp(a.id, &Resp::Batch(v)));
                }
            }
        }
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
    sh: Arc<Shared>,
    /// indice fino al quale le voci sono nella finestra (sui follower l'indicizzazione segue il disco)
    indexed: u64,
    published: Pos,
    flush_q: VecDeque<(Db, Pos, Bytes)>,
    trim_list: Vec<Db>,
    trim_cursor: usize,
    trim_next: Instant,
    sw: [Sw; 16],
    last_snap: Instant,
    rounds: u64,
    rounds_over: u64,
    rounds_over_cpu: u64,
    // ---- tetto di memoria, dispensa, freno, allarme
    mem_used: u64,
    /// RSS del processo (letto ogni secondo): il controllo usa il maggiore tra conto stimato e RSS
    rss: u64,
    resident_n: u64,
    spill_tx: Sender<SpillReq>,
    spill_busy: bool,
    last_spill: Instant,
    spill_q: VecDeque<(Db, u64, Pos, bool)>,
    spilled_recs: u64,
    seg_pos: BTreeMap<u32, Pos>,
    oldest_unfl: Pos,
    last_sec: Instant,
    last_alarm: Instant,
    alarm: bool,
    rate_cnt: HashMap<Db, u32>,
    rate: HashMap<Db, u32>,
    rate_total: u32,
    tokens: HashMap<Db, f64>,
    tok_rate: HashMap<Db, f64>,
    last_refill: Instant,
    delayq: VecDeque<(Instant, Reply, CommitReq)>,
    brake: u8,
    busy_n: u64,
    delayed_n: u64,
    /// byte dei messaggi di replica prima e dopo la compressione (giro 4)
    repl_raw: u64,
    repl_sent: u64,
    txns: HashMap<TxnId, (u64, Pos)>,
    sessions: HashMap<(Bytes, u64), (u64, Pos)>,
    peers: Vec<Peer>,
    pending: BTreeMap<u64, Vec<Reply>>,
    reads: Vec<(u64, u64, Reply)>,
    need_hb: bool,
    hb_seq: u64,
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

/// Tempo CPU del thread chiamante (µs).
pub fn thread_cpu_us() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1000
}

/// Priorità del thread chiamante (nice; negativo = più priorità). Sulla macchina condivisa dai banchi il thread
/// principale e quello del disco delle repliche passano davanti al generatore di carico e a MinIO.
pub fn set_thread_nice(n: i32) {
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, n);
    }
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
    let io_threads = std::env::var("PQ_IO_THREADS").ok().and_then(|x| x.parse().ok()).unwrap_or(2);
    let io_nice: i32 = std::env::var("PQ_IO_NICE").ok().and_then(|x| x.parse().ok()).unwrap_or(-10);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(io_threads).on_thread_start(move || set_thread_nice(io_nice)).enable_all().build()?;
    let sh = Arc::new(Shared { win: Window::new(false, cfg.dir.clone()), readable: watch::channel(0).0, status: Mutex::new(Status::default()), brake: Default::default(), fast_busy: Default::default() });
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
        let sh2 = sh.clone();
        rt.spawn(async move {
            loop {
                let Ok((s, _)) = lc.accept().await else { continue };
                let _ = s.set_nodelay(true);
                let t = t.clone();
                let sh = sh2.clone();
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
                                // letture e richieste di servizio: qui, senza passare dal thread principale
                                if let Some(q) = serve_local(&sh, &rtx, id, q) {
                                    if t.send(Ev::Client(rtx.clone(), id, q)).is_err() {
                                        return;
                                    }
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
    let sh3 = sh.clone();
    store::spawn_disk(
        cfg.dir.clone(),
        disk_rx,
        cfg.seg_bytes,
        move |gen, last| {
            let _ = dt.send(Ev::Disk(gen, last));
        },
        move || sh3.win.flushed_all(),
    );
    // thread della dispensa: raccoglie i record vecchi non su S3 e scrive i run (fuori dal thread principale)
    let (spill_tx, spill_rx) = crossbeam_channel::unbounded::<SpillReq>();
    {
        let (sh, t) = (sh.clone(), ev_tx.clone());
        std::thread::Builder::new()
            .name("dispensa".into())
            .spawn(move || {
                while let Ok(q) = spill_rx.recv() {
                    let mut ents = vec![];
                    let mut dbs = vec![];
                    let mut total = 0;
                    // prima i database con più byte non su S3 (il caldo), così un run copre molto con poche schede
                    let mut cand = sh.win.pending_list(usize::MAX);
                    // per byte non su S3 ancora in RAM (quelli già nella dispensa non contano)
                    cand.retain(|p| p.ram_bytes > 0);
                    cand.sort_by_key(|p| std::cmp::Reverse(p.ram_bytes));
                    // al più 256 database per run: i piccoli si accumulano e finiscono nella dispensa in pochi pezzi
                    // grandi (una scheda per database per run tiene RAM: con tutti i 10.000 in ogni run erano ~0,8 MB a run)
                    cand.truncate(256);
                    for p in cand {
                        if p.oldest >= q.cutoff || total >= q.max_recs {
                            continue;
                        }
                        // a pezzi: il lock in lettura del database si tiene poco. Al più 50.000 record per database per run:
                        // `scaricabili` restituisce il pezzo di un run intero, e deve stare in un messaggio (< 64 MB)
                        let mut after = 0;
                        let mut per_db = 0;
                        loop {
                            let (last, n) = sh.win.collect_spill(&p.db, after, q.cutoff, q.commit_pos, 2048, &mut ents);
                            total += n;
                            per_db += n;
                            if n == 0 {
                                break;
                            }
                            after = last;
                            if n < 2048 || total >= q.max_recs || per_db >= 50_000 {
                                break;
                            }
                        }
                        if after > 0 {
                            dbs.push((p.db, after));
                        }
                    }
                    let run = if ents.is_empty() {
                        0
                    } else {
                        match sh.win.spill.write_run(ents) {
                            Ok(r) => r.id,
                            Err(e) => {
                                eprintln!("dispensa: scrittura run fallita: {e}");
                                dbs.clear();
                                0
                            }
                        }
                    };
                    if t.send(Ev::Spilled(run, dbs)).is_err() {
                        return;
                    }
                }
            })
            .unwrap();
    }
    // restituzione periodica al sistema della memoria liberata (l'allocatore di glibc la tratterrebbe)
    std::thread::Builder::new()
        .name("trim-mem".into())
        .spawn(|| loop {
            std::thread::sleep(Duration::from_secs(5));
            // con mimalloc non serve; resta per chi compila con l'allocatore di sistema
            unsafe {
                libc::malloc_trim(0);
            }
        })
        .unwrap();
    let mut node = Node::recover(cfg, ev_rx, disk_tx, peers, sh, spill_tx)?;
    eprintln!("replica {} pronta: indice {}..{} mandato {}", node.cfg.id, node.front, node.last_index(), node.term);
    println!("READY");
    node.run();
    drop(rt);
    Ok(())
}

impl Node {
    fn recover(cfg: Config, rx: Receiver<Ev>, disk_tx: Sender<DiskOp>, peers: Vec<(u8, UnboundedSender<Vec<u8>>)>, sh: Arc<Shared>, spill_tx: Sender<SpillReq>) -> anyhow::Result<Node> {
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
            sh,
            indexed: prev_index,
            published: 0,
            flush_q: VecDeque::new(),
            trim_list: vec![],
            trim_cursor: 0,
            trim_next: now,
            sw: [Sw::default(); 16],
            last_snap: now,
            rounds: 0,
            rounds_over: 0,
            rounds_over_cpu: 0,
            mem_used: 0,
            rss: 0,
            resident_n: 0,
            spill_tx,
            spill_busy: false,
            last_spill: now,
            spill_q: VecDeque::new(),
            spilled_recs: 0,
            seg_pos: BTreeMap::new(),
            oldest_unfl: Pos::MAX,
            last_sec: now,
            last_alarm: now - Duration::from_secs(3600),
            alarm: false,
            rate_cnt: HashMap::new(),
            rate: HashMap::new(),
            rate_total: 0,
            tokens: HashMap::new(),
            tok_rate: HashMap::new(),
            last_refill: now,
            delayq: VecDeque::new(),
            brake: 0,
            busy_n: 0,
            repl_raw: 0,
            repl_sent: 0,
            delayed_n: 0,
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
            disk_bytes: 0,
            defer_index: false,
            to_index: vec![],
            h_durable: hdrhistogram::Histogram::new(3).unwrap(),
            h_ack: hdrhistogram::Histogram::new(3).unwrap(),
            h_commit: hdrhistogram::Histogram::new(3).unwrap(),
            h_loop: hdrhistogram::Histogram::new(3).unwrap(),
            cfg,
        };
        n.sh.win.genesis.store(genesis, std::sync::atomic::Ordering::Relaxed);
        if let Some(c) = ckv {
            for (h, p) in &c.flushed {
                if let Some(db) = store::unhex16(h) {
                    n.sh.win.set_flushed(&db, *p, &Bytes::new());
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
            n.seg_pos.insert(r.seg, e.pos);
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
        let loc = Loc { seg, off, len: e.raw.len() as u32 };
        self.resident += e.raw.len() as u64;
        self.resident_n += 1;
        self.log.push_back(Slot { term: e.term, pos: e.pos, len: e.raw.len() as u32, ent: Some(e.clone()), seg, off, done: false, t: Instant::now() });
        if self.defer_index {
            self.to_index.push(e.index);
        } else {
            self.index_entry(&e, loc);
            self.indexed = e.index;
        }
    }

    /// Indicizza le voci in attesa (sui follower), al più fino a `deadline`.
    fn flush_index_until(&mut self, deadline: Option<Instant>) {
        let mut k = 0;
        while k < self.to_index.len() {
            if let Some(d) = deadline {
                if k % 8 == 7 && Instant::now() >= d {
                    break;
                }
            }
            let i = self.to_index[k];
            let s = &self.log[(i - self.front) as usize];
            let loc = Loc { seg: s.seg, off: s.off, len: s.len };
            let e = s.ent.clone().expect("voce appena aggiunta");
            self.index_entry(&e, loc);
            self.indexed = i;
            k += 1;
        }
        self.to_index.drain(..k);
    }

    fn flush_index(&mut self) {
        self.flush_index_until(None);
    }

    fn index_entry(&mut self, e: &Arc<Entry>, loc: Loc) {
        let index = e.index;
        if let Body::Rec { txn, session, .. } = &e.body {
            self.sh.win.apply(e, loc);
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
        self.seg_pos.insert(seg, e.pos);
        self.push_slot(Arc::new(e), seg, off);
    }

    fn truncate_from(&mut self, i: u64) {
        assert!(i > self.commit, "troncamento di una voce confermata ({i} ≤ {})", self.commit);
        self.flush_index();
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
                self.resident_n = self.resident_n.saturating_sub(1);
            }
            self.disk_bytes -= s.len as u64;
            if let Body::Rec { db, session, .. } = &e.body {
                if let Some(txn) = self.sh.win.unapply_last(db, idx) {
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
        self.indexed = self.indexed.min(i - 1);
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
        set_thread_nice(-10);
        loop {
            let (mut w0, mut c0) = (Instant::now(), 0);
            match self.rx.recv_timeout(Duration::from_millis(1)) {
                Ok(ev) => {
                    // il giro si misura dal primo evento ricevuto (non dall'attesa a canale vuoto)
                    (w0, c0) = (Instant::now(), thread_cpu_us());
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
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    (w0, c0) = (Instant::now(), thread_cpu_us());
                }
                Err(_) => return,
            }
            let t1 = Instant::now();
            self.tick();
            let t = Instant::now();
            self.submit_disk();
            self.sw(12, t);
            let t = Instant::now();
            self.send_out();
            self.sw(11, t);
            self.publish();
            self.h_loop.record(t1.elapsed().as_micros() as u64).ok();
            // giro lungo: lavoro vero (CPU del thread) o sospensione da parte del sistema operativo?
            let wall = w0.elapsed().as_micros() as u64;
            if wall > 1000 {
                self.rounds_over += 1;
                if thread_cpu_us().saturating_sub(c0) > 1000 {
                    self.rounds_over_cpu += 1;
                }
            }
            self.rounds += 1;
        }
    }

    fn sw(&mut self, k: usize, t: Instant) {
        let us = t.elapsed().as_micros() as u64;
        let s = &mut self.sw[k];
        s.n += 1;
        s.tot_us += us;
        s.max_us = s.max_us.max(us);
        if us > 1000 {
            s.over_1ms += 1;
        }
    }

    /// Pubblica la posizione leggibile: confermata e già nella finestra.
    fn publish(&mut self) {
        let ri = self.commit.min(self.indexed);
        let p = self.pos_at(ri).min(self.commit_pos);
        if p > self.published {
            self.published = p;
            self.sh.readable.send_replace(p);
        }
    }

    fn handle(&mut self, ev: Ev) {
        let t = Instant::now();
        let k = match &ev {
            Ev::Peer(Msg::Append { .. }) => 2,
            Ev::Peer(Msg::AppendResp { .. }) => 3,
            Ev::Peer(_) => 4,
            Ev::Client(_, _, Req::Commit(_) | Req::CommitBatch(_)) => 0,
            Ev::Client(_, _, Req::Get { .. }) => 1,
            Ev::Client(..) => 15,
            Ev::Disk(..) => 5,
            Ev::Spilled(..) => 15,
        };
        self.handle_ev(ev);
        self.sw(k, t);
    }

    fn handle_ev(&mut self, ev: Ev) {
        match ev {
            Ev::Spilled(run, dbs) => {
                self.spill_busy = false;
                for (db, upto) in dbs {
                    self.spill_q.push_back((db, run, upto, true));
                }
            }
            Ev::Peer(m) => self.on_msg(m),
            Ev::Client(conn, id, Req::CommitBatch(v)) => {
                if self.role != Role::Leader {
                    Reply::One { conn, id }.send(&Resp::NotLeader(self.leader));
                    return;
                }
                let agg = std::rc::Rc::new(std::cell::RefCell::new(Agg { conn, id, res: vec![None; v.len()], left: v.len() }));
                for (k, c) in v.into_iter().enumerate() {
                    self.on_commit(Reply::Part { agg: agg.clone(), k }, c);
                }
            }
            Ev::Client(conn, id, q) => self.on_req(Reply::One { conn, id }, q),
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
        self.publish_brake();
        self.leader = -1;
        self.verified = 0;
        self.prevotes.clear();
        self.votes.clear();
        self.noop_index = u64::MAX;
    }

    fn become_leader(&mut self) {
        self.flush_index();
        eprintln!("replica {}: guida, mandato {} (indice {})", self.me(), self.term, self.last_index());
        self.role = Role::Leader;
        self.publish_brake();
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
                    // applicati a passi dal tick (possono essere migliaia di database)
                    self.flush_q.extend(items.iter().cloned());
                }
                if !matches!(e.body, Body::Rec { .. }) {
                    let sub = self.submitted;
                    let s = &mut self.log[k];
                    s.done = true;
                    // il corpo resta finché non è stato passato al disco
                    if i <= sub && s.ent.take().is_some() {
                        self.resident -= s.len as u64;
                        self.resident_n = self.resident_n.saturating_sub(1);
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
        // stesso messaggio per le due repliche (caso normale): si comprime una volta sola
        let mut zcache: Option<((u64, u64, bool), Vec<u8>)> = None;
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
                while next <= last && next - matched <= self.cfg.max_inflight && bytes < (128 << 10) {
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
            let f = self.maybe_compress(&b, &mut zcache, (prev_index, next, probing));
            let p = &mut self.peers[k];
            let _ = p.tx.send(f);
            if !probing {
                p.next = next;
            }
            p.last_sent = now;
            p.sent_commit = self.commit;
        }
        self.need_hb = false;
    }

    /// Compressione dei messaggi di replica (giro 4, `PQ_ZSTD=livello`, 0 = spenta). Solo i messaggi con voci sopra
    /// 512 byte; se compresso non conviene, si manda in chiaro. Conta i byte prima e dopo.
    fn maybe_compress(&mut self, b: &[u8], cache: &mut Option<((u64, u64, bool), Vec<u8>)>, key: (u64, u64, bool)) -> Vec<u8> {
        self.repl_raw += b.len() as u64 + 8;
        let lvl = self.cfg.zstd;
        let out = if lvl == 0 || b.len() < 512 {
            frame(b)
        } else if let Some((k, f)) = cache.as_ref().filter(|(k, _)| *k == key) {
            let _ = k;
            f.clone()
        } else {
            let c = zstd::bulk::compress(b, lvl).unwrap_or_default();
            let f = if !c.is_empty() && c.len() + 5 < b.len() {
                let mut z = Vec::with_capacity(c.len() + 5);
                z.push(MSG_ZSTD);
                z.extend_from_slice(&(b.len() as u32).to_le_bytes());
                z.extend_from_slice(&c);
                frame(&z)
            } else {
                frame(b)
            };
            *cache = Some((key, f.clone()));
            f
        };
        self.repl_sent += out.len() as u64;
        out
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
        // lavori periodici, ciascuno a passi brevi e cronometrato
        if !self.to_index.is_empty() {
            let t = Instant::now();
            self.flush_index_until(Some(t + Duration::from_micros(500)));
            self.sw(6, t);
        }
        if !self.flush_q.is_empty() {
            let t = Instant::now();
            let dl = t + Duration::from_micros(200);
            let mut k = 0;
            while let Some((db, p, etag)) = self.flush_q.pop_front() {
                self.sh.win.set_flushed(&db, p, &etag);
                k += 1;
                if k % 16 == 0 && Instant::now() >= dl {
                    break;
                }
            }
            self.sw(8, t);
        }
        if now >= self.trim_next {
            let t = Instant::now();
            self.trim_step(t + Duration::from_micros(200));
            self.sw(7, t);
        }
        // conto unico della memoria; sopra il 65% del tetto i valori vanno su disco, sopra il 70% i record vecchi non
        // su S3 vanno nella dispensa (margine per la memoria di lavoro della dispensa stessa)
        self.mem_used = self.mem_count().max(self.rss);
        if self.mem_used > self.cfg.mem_cap / 20 * 13 && self.resident > 0 {
            let t = Instant::now();
            self.evict_step(t + Duration::from_micros(200));
            self.sw(9, t);
        }
        if self.mem_used > self.cfg.mem_cap / 10 * 7 && !self.spill_busy && self.spill_q.is_empty() && self.oldest_unfl != Pos::MAX && self.last_spill.elapsed() >= Duration::from_secs(2) {
            let cutoff = pos_make(now_ms().saturating_sub(self.cfg.window_ms), 0);
            if self.oldest_unfl < cutoff {
                self.spill_busy = true;
                self.last_spill = Instant::now();
                let _ = self.spill_tx.send(SpillReq { cutoff, commit_pos: self.commit_pos.min(self.published), max_recs: 300_000 });
            }
        }
        if !self.spill_q.is_empty() {
            let t = Instant::now();
            self.spill_step(t + Duration::from_micros(200));
            self.sw(9, t);
        }
        if now.duration_since(self.last_sec) >= Duration::from_secs(1) {
            self.last_sec = now;
            self.every_second();
        }
        if self.brake == 1 || !self.delayq.is_empty() {
            self.release_delayed();
        }
        {
            let t = Instant::now();
            self.front_and_segments();
            self.sw(10, t);
        }
        if now.duration_since(self.last_snap) >= Duration::from_millis(100) {
            self.last_snap = now;
            let t = Instant::now();
            self.snapshot();
            self.sw(14, t);
        }
    }

    /// Potatura a passi: gira sui database con un cursore, al più fino a `deadline`.
    fn trim_step(&mut self, deadline: Instant) {
        if self.trim_cursor >= self.trim_list.len() {
            self.trim_list = self.sh.win.all_dbs();
            self.trim_cursor = 0;
        }
        let cutoff = pos_make(now_ms().saturating_sub(self.cfg.window_ms), 0);
        let cp = self.commit_pos;
        let mut gone = vec![];
        if self.trim_list.is_empty() {
            self.trim_next = Instant::now() + Duration::from_millis(200);
        }
        while self.trim_cursor < self.trim_list.len() {
            let db = self.trim_list[self.trim_cursor];
            let (out, more) = self.sh.win.trim_db(&db, cutoff, cp, deadline);
            gone.extend(out);
            if more {
                break; // stesso database al prossimo giro
            }
            self.trim_cursor += 1;
            if self.trim_cursor >= self.trim_list.len() {
                // giro completo: il prossimo tra 200 ms
                self.trim_next = Instant::now() + Duration::from_millis(200);
                break;
            }
            if self.trim_cursor % 16 == 0 && Instant::now() >= deadline {
                break;
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
                    self.resident_n = self.resident_n.saturating_sub(1);
                }
            }
        }
        // sessioni: si tengono quanto i txn (approssimazione: si potano con lo stesso limite di indice)
        if self.sessions.len() > 2 * self.txns.len() + 1024 {
            let f = self.front;
            self.sessions.retain(|_, v| v.0 >= f);
        }
    }

    /// Fronte del registro in RAM (voci uscite dalla finestra, confermate e su tutte e tre le repliche) e segmenti
    /// interamente prima del fronte: il checkpoint e la cancellazione li fa il thread del disco.
    fn front_and_segments(&mut self) {
        let am = if self.role == Role::Leader { self.all_match } else { self.all_match.min(self.commit) };
        let mut k = 0;
        while let Some(s) = self.log.front() {
            if !(s.done && self.front <= self.commit && self.front <= am && self.front <= self.durable && self.front <= self.indexed) {
                break;
            }
            let s = self.log.pop_front().unwrap();
            if s.ent.is_some() {
                self.resident -= s.len as u64;
                self.resident_n = self.resident_n.saturating_sub(1);
            }
            self.prev_term = s.term;
            self.prev_pos = s.pos;
            self.front += 1;
            k += 1;
            if k >= 20_000 {
                break;
            }
        }
        // un segmento si cancella solo se è prima del fronte E tutti i suoi record sono su S3 (la dispensa punta al
        // registro su disco per i valori)
        let ou = self.oldest_unfl;
        let dead: Vec<u32> = self
            .segs
            .iter()
            .filter(|(s, (_, l))| **s != self.cur_seg && *l < self.front && self.seg_pos.get(s).map(|p| *p < ou).unwrap_or(true))
            .map(|(s, _)| *s)
            .collect();
        if !dead.is_empty() {
            self.sh.win.genesis.store(false, std::sync::atomic::Ordering::Relaxed);
            for s in &dead {
                self.segs.remove(s);
                self.seg_pos.remove(s);
                self.reader.forget(*s);
            }
            self.disk_bytes = self.disk_bytes.saturating_sub(dead.len() as u64 * self.cfg.seg_bytes);
            let _ = self.disk_tx.send(DiskOp::CkptDelete { prev_index: self.front - 1, prev_term: self.prev_term, prev_pos: self.prev_pos, segs: dead });
        }
    }

    /// Tetto di RAM: i valori più vecchi ancora nella finestra vanno su disco (a passi).
    fn evict_step(&mut self, deadline: Instant) {
        let target = self.cfg.mem_cap / 10 * 9;
        let mut i = self.evict_cursor.max(self.front);
        let lim = self.durable.min(self.indexed).min(self.last_index());
        let mut n = 0;
        while self.resident > target && i <= lim {
            let k = (i - self.front) as usize;
            if let Some(e) = self.log[k].ent.clone() {
                if let Body::Rec { db, writes, .. } = &e.body {
                    let s = &self.log[k];
                    self.sh.win.evict(db, e.pos, Loc { seg: s.seg, off: s.off, len: s.len }, writes);
                }
                self.log[k].ent = None;
                self.resident -= self.log[k].len as u64;
                self.resident_n = self.resident_n.saturating_sub(1);
                self.evicted += 1;
            }
            i += 1;
            n += 1;
            if n % 16 == 0 && Instant::now() >= deadline {
                break;
            }
        }
        self.evict_cursor = i;
    }

    /// Fotografia dello stato per `status` (servito dai thread di rete).
    fn snapshot(&mut self) {
        let sw: serde_json::Map<String, serde_json::Value> = SW_NAMES
            .iter()
            .zip(self.sw.iter())
            .filter(|(_, s)| s.n > 0)
            .map(|(n, s)| (n.to_string(), serde_json::json!({"n": s.n, "tot_ms": s.tot_us / 1000, "max_us": s.max_us, "sopra_1ms": s.over_1ms})))
            .collect();
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
            resident_bytes: self.resident,
            evicted: self.evicted,
            segments: self.segs.len() as u64,
            disk_bytes: self.disk_bytes,
            mem_used: self.mem_used,
            mem_est: self.mem_count(),
            mem_cap: self.cfg.mem_cap,
            disk_used: self.disk_used(),
            disk_soft: self.cfg.disk_soft,
            disk_hard: self.cfg.disk_hard,
            disk_ro: self.cfg.disk_ro,
            spill_runs: self.sh.win.spill.runs.read().len() as u64,
            spill_disk: self.sh.win.spill.disk_bytes.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64,
            spilled_recs: self.spilled_recs,
            oldest_unflushed_s: if self.oldest_unfl == Pos::MAX { 0.0 } else { now_ms().saturating_sub(pos_ms(self.oldest_unfl)) as f64 / 1000.0 },
            alarm: self.alarm,
            brake: self.brake,
            busy: self.busy_n + self.sh.fast_busy.load(std::sync::atomic::Ordering::Relaxed),
            repl_raw: self.repl_raw,
            repl_sent: self.repl_sent,
            delayed: self.delayed_n,
            diag: format!(
                "{{\"durable_p50_p99\":[{},{}],\"ack_p50_p99\":[{},{}],\"commit_p50_p99\":[{},{}],\"giro_max_us\":{},\"giri\":{},\"giri_sopra_1ms\":{},\"giri_sopra_1ms_di_cpu\":{},\"cronometro\":{}}}",
                self.h_durable.value_at_quantile(0.5),
                self.h_durable.value_at_quantile(0.99),
                self.h_ack.value_at_quantile(0.5),
                self.h_ack.value_at_quantile(0.99),
                self.h_commit.value_at_quantile(0.5),
                self.h_commit.value_at_quantile(0.99),
                self.h_loop.max(),
                self.rounds,
                self.rounds_over,
                self.rounds_over_cpu,
                serde_json::Value::Object(sw)
            ),
            ..Default::default()
        };
        *self.sh.status.lock() = st;
    }

    // -------------------------------------------------------------------------------------------- client

    fn on_req(&mut self, reply: Reply, q: Req) {
        match q {
            Req::Commit(c) => self.on_commit(reply, c),
            Req::Get { db, key, dal, spec: true, .. } => {
                // lettura speculativa (solo leader): ultima versione accettata, anche non confermata
                if self.role != Role::Leader {
                    reply.send(&Resp::NotLeader(self.leader));
                    return;
                }
                reply.send(&Resp::Get(self.sh.win.get(&db, &key, Pos::MAX, dal)));
            }
            Req::Confirmed => {
                if self.role != Role::Leader {
                    reply.send(&Resp::NotLeader(self.leader));
                    return;
                }
                self.reads.push((self.commit, self.hb_seq + 1, reply));
                self.need_hb = true;
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
            // servite dai thread di rete (serve_local): qui non arrivano
            Req::CommitBatch(_) | Req::Get { .. } | Req::Scan { .. } | Req::Wait { .. } | Req::Flushable { .. } | Req::Pending { .. } | Req::FlushState { .. } | Req::Status => {
                reply.send(&Resp::Err("richiesta servita dai thread di rete".into()));
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

    /// Conto unico della memoria del quaderno (byte, STIMATO dai contatori con i costi C_*).
    fn mem_count(&self) -> u64 {
        let w = &self.sh.win;
        let r = |a: &std::sync::atomic::AtomicI64| a.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64;
        self.resident
            + self.resident_n * C_BODY
            + r(&w.nrecs) * C_REC
            + r(&w.versions) * C_VER
            + r(&w.nkeys) * C_KEY
            + self.log.len() as u64 * C_SLOT
            + self.txns.len() as u64 * C_TXN
            + r(&w.spill.ram_bytes)
    }

    fn disk_used(&self) -> u64 {
        self.segs.len() as u64 * self.cfg.seg_bytes + self.sh.win.spill.disk_bytes.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64
    }

    /// Record passati alla dispensa: escono dalla RAM a passi.
    fn spill_step(&mut self, deadline: Instant) {
        while let Some((db, run, upto, first)) = self.spill_q.pop_front() {
            let (gone, more) = self.sh.win.apply_spill(&db, run, upto, first, 512);
            self.spilled_recs += gone.len() as u64;
            for (i, txn) in gone {
                if self.txns.get(&txn).map(|x| x.0) == Some(i) {
                    self.txns.remove(&txn);
                }
                if i >= self.front {
                    let sub = self.submitted;
                    let s = &mut self.log[(i - self.front) as usize];
                    s.done = true;
                    if i <= sub {
                        if s.ent.take().is_some() {
                            self.resident -= s.len as u64;
                            self.resident_n = self.resident_n.saturating_sub(1);
                        }
                    }
                }
            }
            if more {
                self.spill_q.push_front((db, run, upto, false));
            }
            if Instant::now() >= deadline {
                break;
            }
        }
    }

    /// Ogni secondo: record non su S3 più vecchio (allarme, cancellazione dei segmenti), ritmo di scrittura per database,
    /// stato del freno.
    fn every_second(&mut self) {
        self.rss = rss_kb() * 1024;
        self.oldest_unfl = self.sh.win.pending.lock().values().map(|p| p.oldest).filter(|p| *p > 0).min().unwrap_or(Pos::MAX);
        let age = if self.oldest_unfl == Pos::MAX { 0 } else { now_ms().saturating_sub(pos_ms(self.oldest_unfl)) / 1000 };
        self.alarm = self.cfg.alarm_s > 0 && age > self.cfg.alarm_s;
        if self.alarm && self.last_alarm.elapsed() >= Duration::from_secs(30) {
            self.last_alarm = Instant::now();
            eprintln!("ALLARME replica {}: il record non su S3 più vecchio ha {age} s (soglia {} s); memoria {} MB su {} MB, disco {} MB", self.me(), self.cfg.alarm_s, self.mem_used >> 20, self.cfg.mem_cap >> 20, self.disk_used() >> 20);
        }
        self.rate = std::mem::take(&mut self.rate_cnt);
        self.rate_total = self.rate.values().sum();
        // freno
        let disk = self.disk_used();
        let c = &self.cfg;
        let old = self.brake;
        // freno di memoria con isteresi: entra sopra il tetto, esce sotto il 95%
        self.brake = if self.mem_used > c.mem_cap || (old == 4 && self.mem_used > c.mem_cap / 20 * 19) {
            4
        } else if c.disk_ro > 0 && disk >= c.disk_ro {
            3
        } else if c.disk_hard > 0 && disk >= c.disk_hard {
            2
        } else if c.disk_soft > 0 && disk >= c.disk_soft {
            1
        } else {
            0
        };
        self.publish_brake();
        if self.brake != old {
            eprintln!("replica {}: freno {} -> {} (disco {} MB, memoria {} MB su {} MB)", self.me(), old, self.brake, disk >> 20, self.mem_used >> 20, c.mem_cap >> 20);
        }
        // rallentamento: i database che scrivono di più ricevono gettoni a un ritmo ridotto, tanto più quanto il disco
        // si avvicina alla soglia di rifiuto
        self.tok_rate.clear();
        if self.brake == 1 {
            let frac = if c.disk_hard > c.disk_soft { (disk - c.disk_soft) as f64 / (c.disk_hard - c.disk_soft) as f64 } else { 1.0 };
            let f = (1.0 - 0.8 * frac).clamp(0.2, 1.0);
            let heavy = (self.rate_total as f64 * 0.2).max(1000.0);
            for (db, r) in &self.rate {
                if *r as f64 >= heavy {
                    // gettoni al ms
                    self.tok_rate.insert(*db, (*r as f64 * f).max(100.0) / 1000.0);
                }
            }
        }
        self.tokens.retain(|db, _| self.tok_rate.contains_key(db));
        for db in self.tok_rate.keys() {
            self.tokens.entry(*db).or_insert(0.0);
        }
    }

    /// Il livello del freno visto dai thread di rete: quello vero sul leader, 0 sulle altre repliche (che rispondono
    /// "non sono il leader" dal thread principale).
    fn publish_brake(&self) {
        let b = if self.role == Role::Leader { self.brake } else { 0 };
        self.sh.brake.store(b, std::sync::atomic::Ordering::Relaxed);
    }

    /// Commit trattenuti dal rallentamento: escono quando il database ha gettoni (che si ricaricano a ogni ms);
    /// dopo 100 ms di attesa si rifiutano con l'errore ritentabile.
    fn release_delayed(&mut self) {
        let dt = self.last_refill.elapsed().as_secs_f64() * 1000.0;
        self.last_refill = Instant::now();
        for (db, tk) in self.tokens.iter_mut() {
            let r = self.tok_rate.get(db).copied().unwrap_or(0.0);
            *tk = (*tk + r * dt).min(r * 50.0 + 1.0);
        }
        let mut keep = VecDeque::new();
        while let Some((t, reply, c)) = self.delayq.pop_front() {
            let ok = match self.tokens.get_mut(&c.db) {
                Some(tk) if *tk >= 1.0 => {
                    *tk -= 1.0;
                    true
                }
                Some(_) => false,
                None => true,
            };
            if ok && self.brake <= 1 {
                self.commit_now(reply, c);
            } else if t.elapsed() > Duration::from_millis(100) || self.brake >= 2 {
                self.busy_n += 1;
                reply.send(&Resp::Busy { retry_ms: 50, reason: 1 });
            } else {
                keep.push_back((t, reply, c));
            }
        }
        self.delayq = keep;
    }

    fn on_commit(&mut self, reply: Reply, c: CommitReq) {
        if self.role != Role::Leader {
            reply.send(&Resp::NotLeader(self.leader));
            return;
        }
        // freno: memoria sopra il tetto, poi disco (sola lettura, rifiuto, rallentamento dei database che scrivono di più)
        if self.brake >= 2 {
            let (ms, why) = match self.brake {
                4 => (200, 4),
                3 => (1000, 3),
                _ => (500, 2),
            };
            self.busy_n += 1;
            reply.send(&Resp::Busy { retry_ms: ms, reason: why });
            return;
        }
        *self.rate_cnt.entry(c.db).or_insert(0) += 1;
        if self.brake == 1 {
            if let Some(tk) = self.tokens.get_mut(&c.db) {
                if *tk < 1.0 {
                    self.delayed_n += 1;
                    self.delayq.push_back((Instant::now(), reply, c));
                    return;
                }
                *tk -= 1.0;
            }
        }
        self.commit_now(reply, c);
    }

    fn commit_now(&mut self, reply: Reply, c: CommitReq) {
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
        if let Err(k) = self.sh.win.validate(&c.db, c.read_pos, &c.writes, c.mode == Mode::Validated, &c.reads) {
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

/// Richieste servite dal thread di rete senza passare dal thread principale. Restituisce la richiesta se va al thread
/// principale (commit, posizione confermata, conferma_scarico, letture speculative, dump).
fn serve_local(sh: &Arc<Shared>, conn: &UnboundedSender<Vec<u8>>, id: u64, q: Req) -> Option<Req> {
    let send = |r: Resp| {
        let _ = conn.send(enc_resp(id, &r));
    };
    let readable = || *sh.readable.borrow();
    match q {
        Req::Get { spec: false, db, key, at, wait_ms, dal } => {
            if at <= readable() {
                send(Resp::Get(sh.win.get(&db, &key, at, dal)));
            } else {
                let (sh, conn) = (sh.clone(), conn.clone());
                tokio::spawn(async move {
                    let r = if wait_readable(&sh, at, wait_ms).await { Resp::Get(sh.win.get(&db, &key, at, dal)) } else { Resp::Behind(*sh.readable.borrow()) };
                    let _ = conn.send(enc_resp(id, &r));
                });
            }
            None
        }
        Req::Scan { db, from, to, at, limit, wait_ms, dal } => {
            let (sh, conn) = (sh.clone(), conn.clone());
            tokio::spawn(async move {
                let r = if wait_readable(&sh, at, wait_ms).await { Resp::Scan(sh.win.scan(&db, &from, &to, at, limit as usize, dal)) } else { Resp::Behind(*sh.readable.borrow()) };
                let _ = conn.send(enc_resp(id, &r));
            });
            None
        }
        Req::Wait { pos, timeout_ms } => {
            if pos <= readable() {
                send(Resp::Pos(readable()));
            } else {
                let (sh, conn) = (sh.clone(), conn.clone());
                tokio::spawn(async move {
                    let r = if wait_readable(&sh, pos, timeout_ms).await { Resp::Pos(*sh.readable.borrow()) } else { Resp::Behind(*sh.readable.borrow()) };
                    let _ = conn.send(enc_resp(id, &r));
                });
            }
            None
        }
        Req::Flushable { dbs, upto } => {
            let (sh, conn) = (sh.clone(), conn.clone());
            // può rileggere dal disco: su un thread a parte
            tokio::task::spawn_blocking(move || {
                let upto = upto.min(*sh.readable.borrow());
                let mut budget = 4usize << 20;
                let mut out = vec![];
                for db in dbs {
                    if budget == 0 {
                        break;
                    }
                    let recs = sh.win.flushable(&db, upto, &mut budget);
                    if !recs.is_empty() {
                        out.push((db, recs));
                    }
                }
                let _ = conn.send(enc_resp(id, &Resp::Flushable(out)));
            });
            None
        }
        Req::Pending { limit } => {
            send(Resp::Pending(sh.win.pending_list(limit as usize)));
            None
        }
        Req::FlushState { dbs } => {
            let v = dbs
                .iter()
                .map(|db| {
                    let (f, e, t) = sh.win.flush_state(db);
                    (*db, f, e, t)
                })
                .collect();
            send(Resp::FlushState(v));
            None
        }
        Req::Commit(_) | Req::CommitBatch(_) if sh.brake.load(std::sync::atomic::Ordering::Relaxed) >= 2 => {
            // freno che rifiuta: risposta subito, dal thread di rete. Il commit non entra nel registro: errore
            // ritentabile pulito, come il rifiuto del thread principale (stessi tempi di attesa suggeriti)
            let b = sh.brake.load(std::sync::atomic::Ordering::Relaxed);
            let ms = match b {
                4 => 200,
                3 => 1000,
                _ => 500,
            };
            sh.fast_busy.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            send(Resp::Busy { retry_ms: ms, reason: b });
            None
        }
        Req::Status => {
            let mut st = sh.status.lock().clone();
            let (unfl, minp) = sh.win.unflushed_summary();
            st.dbs = sh.win.ndbs.load(std::sync::atomic::Ordering::Relaxed) as u64;
            st.keys = sh.win.nkeys.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64;
            st.versions = sh.win.versions.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64;
            st.unflushed_recs = unfl;
            st.min_unflushed_pos = minp;
            st.rss_kb = rss_kb();
            send(Resp::Status(st));
            None
        }
        other => Some(other),
    }
}

async fn wait_readable(sh: &Shared, at: Pos, wait_ms: u32) -> bool {
    if *sh.readable.borrow() >= at {
        return true;
    }
    if wait_ms == 0 {
        return false;
    }
    let mut rx = sh.readable.subscribe();
    let r = tokio::time::timeout(Duration::from_millis(wait_ms as u64), rx.wait_for(|p| *p >= at)).await;
    matches!(r, Ok(Ok(_)))
}
