//! Prova M8 "senza cuoco fisso": il database non ha un proprietario con lease. Le scritture vanno al quaderno
//! (proto/quaderno) come commit validati; le letture leggono la finestra del quaderno a una posizione S.
//!
//! Acceso con `--quaderno host:porta,host:porta,host:porta`. Solo per la prova: insert, find per `_id`,
//! update per `_id`, indici unici. Niente S3 (tutta la storia del database sta nella finestra: nessuno scarico),
//! niente transazioni, change stream, TTL, capped.
//!
//! Regola di un comando (vedi `Dbs::lock`, `Entry::apply_mode`, `Server::route`):
//! 1. al primo accesso il comando prende S = posizione confermata dal capo (lineare, ReadIndex);
//! 2. tutte le letture del comando sono a S; le chiavi lette con `get` sono dichiarate nel commit (dipendenza `valore`);
//! 3. il commit è "validato" con read_pos = S: il capo rifiuta se una chiave scritta o dichiarata ha una versione dopo S;
//! 4. su conflitto il comando intero si rifà da capo (nuova S), fino a `MAX_ATTEMPTS` volte.
//!
//! Indice unico: le voci d'indice hanno l'`_id` nella chiave, quindi due insert con lo stesso valore e `_id` diversi
//! scrivono chiavi diverse e la validazione da sola non li ferma. Per questo ogni voce aggiunta a un indice unico scrive
//! anche una "chiave di guardia" `uqg/` + prefisso dell'indice + valore (senza `_id`): due insert con lo stesso valore
//! scrivono la stessa guardia e il secondo trova il conflitto. Con `FENVIX_Q_NO_GUARD=1` la guardia è spenta (prova
//! negativa: mostra i doppioni).

use bytes::Bytes;
use quaderno::client::{Cluster, Outcome};
use quaderno::codec::{CommitReq, Dep, GetRes, Kind, Mode, Pos, Req, Resp, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tenant_bench::lifecycle::Op;

static CL: OnceLock<Arc<Cluster>> = OnceLock::new();
static ON: AtomicBool = AtomicBool::new(false);
pub static GUARD: AtomicBool = AtomicBool::new(true);
static RR: AtomicUsize = AtomicUsize::new(0);

pub const MAX_ATTEMPTS: u32 = 200;
const COMMIT_DEADLINE: Duration = Duration::from_secs(10);
const READ_DEADLINE: Duration = Duration::from_secs(10);

/// Contatori per `serverStatus`/log della prova.
pub static COMMITS: AtomicU64 = AtomicU64::new(0);
pub static CONFLICTS: AtomicU64 = AtomicU64::new(0);
pub static UNKNOWN: AtomicU64 = AtomicU64::new(0);
pub static REJECTED: AtomicU64 = AtomicU64::new(0);
pub static RETRIES: AtomicU64 = AtomicU64::new(0);
pub static GIVEUPS: AtomicU64 = AtomicU64::new(0);

pub fn init(addrs: &str) {
    let v: Vec<String> = addrs.split(',').filter(|x| !x.is_empty()).map(String::from).collect();
    assert!(!v.is_empty(), "--quaderno: nessun indirizzo");
    let _ = CL.set(Cluster::new(v, 4));
    ON.store(true, Relaxed);
    if std::env::var("FENVIX_Q_NO_GUARD").map(|x| x == "1").unwrap_or(false) {
        GUARD.store(false, Relaxed);
    }
    eprintln!("quaderno: acceso, repliche {addrs}, guardia degli indici unici {}", GUARD.load(Relaxed));
}

#[inline]
pub fn on() -> bool {
    ON.load(Relaxed)
}

fn cl() -> &'static Arc<Cluster> {
    CL.get().expect("quaderno non inizializzato")
}

/// Id del database nel quaderno: md5 del nome.
pub fn db_id(name: &str) -> [u8; 16] {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(name.as_bytes());
    h.finalize().into()
}

/// Esito di un commit che non è "confermato".
#[derive(Debug)]
pub enum QErr {
    /// il capo ha trovato una chiave cambiata dopo S: il comando si rifà
    Conflict(Vec<u8>),
    /// tempo scaduto senza esito certo (il record può essere entrato o no)
    Unknown,
    /// rifiutato dal freno: non è entrato
    Rejected(u8),
    /// la lettura non ha trovato una replica che risponda
    Unavailable(String),
}
impl std::fmt::Display for QErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QErr::Conflict(k) => write!(f, "quaderno: conflitto sulla chiave {}", String::from_utf8_lossy(k)),
            QErr::Unknown => write!(f, "quaderno: esito ignoto del commit"),
            QErr::Rejected(r) => write!(f, "quaderno: commit rifiutato dal freno (motivo {r})"),
            QErr::Unavailable(s) => write!(f, "quaderno: {s}"),
        }
    }
}
impl std::error::Error for QErr {}

/// Posizione di lettura di un comando: posizione confermata dal capo (lineare).
pub async fn read_pos() -> anyhow::Result<Pos> {
    match cl().confirmed(READ_DEADLINE).await {
        Some(p) => Ok(p),
        None => Err(QErr::Unavailable("nessun capo per la posizione confermata".into()).into()),
    }
}

/// Repliche che non hanno risposto: fino a quando (ms dall'avvio) non si provano per prime.
static DOWN_UNTIL: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static EPOCH: OnceLock<Instant> = OnceLock::new();
fn now_ms() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Chiama una replica (a turno, saltando per 1 s quelle che non hanno risposto), passando alla successiva se non
/// risponde o è indietro.
async fn read_call(q: &Req) -> anyhow::Result<Resp> {
    let c = cl();
    let n = c.n();
    let start = Instant::now();
    let mut r = RR.fetch_add(1, Relaxed) % n;
    let mut last = String::new();
    let mut tried = 0usize;
    while start.elapsed() < READ_DEADLINE {
        // nel primo giro si saltano le repliche segnate come giù (se sono tutte giù si provano lo stesso)
        if tried < n && n <= 8 && DOWN_UNTIL[r].load(Relaxed) > now_ms() {
            tried += 1;
            r = (r + 1) % n;
            continue;
        }
        tried += 1;
        match c.call(r, q, Duration::from_millis(1500)).await {
            Ok(Resp::Behind(p)) => last = format!("replica {r} indietro ({p})"),
            Ok(Resp::Err(e)) => last = format!("replica {r}: {e}"),
            Ok(x) => return Ok(x),
            Err(_) => {
                last = format!("replica {r} non risponde");
                if n <= 8 {
                    DOWN_UNTIL[r].store(now_ms() + 1000, Relaxed);
                }
            }
        }
        r = (r + 1) % n;
        if tried >= n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Err(QErr::Unavailable(format!("lettura: {last}")).into())
}

fn value(g: GetRes, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
    match g {
        GetRes::Value(_, v) => Ok(Some(v)),
        GetRes::Absent(_) => Ok(None),
        other => Err(QErr::Unavailable(format!("chiave {} fuori dalla finestra: {other:?}", String::from_utf8_lossy(key))).into()),
    }
}

pub async fn get(db: &[u8; 16], key: &[u8], at: Pos) -> anyhow::Result<Option<Bytes>> {
    let q = Req::Get { db: *db, key: Bytes::copy_from_slice(key), at, wait_ms: 300, spec: false, dal: 0 };
    match read_call(&q).await? {
        Resp::Get(g) => value(g, key),
        other => anyhow::bail!("quaderno: risposta inattesa a get: {other:?}"),
    }
}

/// Le chiavi vive in `[start, end)` alla posizione `at`, in ordine (o al contrario), al massimo `limit`.
pub async fn scan(db: &[u8; 16], start: &[u8], end: Option<&[u8]>, reverse: bool, limit: Option<usize>, at: Pos) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
    const PAGE: u32 = 2000;
    let mut out = Vec::new();
    let mut from = start.to_vec();
    let to = Bytes::copy_from_slice(end.unwrap_or(&[]));
    loop {
        let q = Req::Scan { db: *db, from: Bytes::copy_from_slice(&from), to: to.clone(), at, limit: PAGE, wait_ms: 300, dal: 0 };
        let rows = match read_call(&q).await? {
            Resp::Scan(v) => v,
            other => anyhow::bail!("quaderno: risposta inattesa a scan: {other:?}"),
        };
        let n = rows.len();
        let last = rows.last().map(|(k, _)| k.clone());
        for (k, g) in rows {
            if let Some(v) = value(g, &k)? {
                out.push((k, v));
            }
        }
        if !reverse {
            if let Some(l) = limit {
                if out.len() >= l {
                    out.truncate(l);
                    return Ok(out);
                }
            }
        }
        if (n as u32) < PAGE {
            break;
        }
        let mut k = last.unwrap().to_vec();
        k.push(0);
        from = k;
    }
    if reverse {
        out.reverse();
        if let Some(l) = limit {
            out.truncate(l);
        }
    }
    Ok(out)
}

/// Commit validato delle scritture di un comando, letto a `read_pos`, con le chiavi lette dichiarate.
pub async fn commit(db: &[u8; 16], read_pos: Pos, ops: &[Op], reads: &[Vec<u8>]) -> Result<Pos, QErr> {
    let writes = ops
        .iter()
        .map(|o| match o {
            Op::Put(k, v) => Write { key: Bytes::copy_from_slice(k), kind: Kind::Put, val: Bytes::copy_from_slice(v) },
            Op::Delete(k) => Write { key: Bytes::copy_from_slice(k), kind: Kind::Del, val: Bytes::new() },
        })
        .collect();
    let c = CommitReq {
        txn: rand::random(),
        db: *db,
        read_pos,
        mode: Mode::Validated,
        writes,
        reads: reads.iter().map(|k| (Bytes::copy_from_slice(k), Dep::Valore)).collect(),
        session: None,
    };
    match cl().commit(&c, COMMIT_DEADLINE).await {
        Outcome::Committed { pos, .. } => {
            COMMITS.fetch_add(1, Relaxed);
            Ok(pos)
        }
        Outcome::Conflict(k) => {
            CONFLICTS.fetch_add(1, Relaxed);
            Err(QErr::Conflict(k.to_vec()))
        }
        Outcome::Unknown => {
            UNKNOWN.fetch_add(1, Relaxed);
            Err(QErr::Unknown)
        }
        Outcome::Rejected(r) => {
            REJECTED.fetch_add(1, Relaxed);
            Err(QErr::Rejected(r))
        }
    }
}

/// Chiave di guardia di un valore d'indice unico (prefisso dell'indice + componenti, senza `_id`).
pub fn guard_key(prefix: &[u8], comps: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + prefix.len() + comps.len());
    k.extend(b"uqg/");
    k.extend(prefix);
    k.extend(comps);
    k
}

pub fn stats() -> bson::Document {
    bson::doc! {
        "commits": COMMITS.load(Relaxed) as i64,
        "conflicts": CONFLICTS.load(Relaxed) as i64,
        "unknown": UNKNOWN.load(Relaxed) as i64,
        "rejected": REJECTED.load(Relaxed) as i64,
        "commandRetries": RETRIES.load(Relaxed) as i64,
        "giveups": GIVEUPS.load(Relaxed) as i64,
        "guard": GUARD.load(Relaxed),
    }
}
