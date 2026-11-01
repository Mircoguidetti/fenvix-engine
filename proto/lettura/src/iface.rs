//! Tipi dell'interfaccia comune tra registro (proto-quaderno) e lettura (proto-lettura): vedi proto/INTERFACCIA.md.
//! I nomi e la codifica di posizione, tipo di scrittura e delta seguono `proto/quaderno/src/codec.rs`.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Posizione: orologio logico ibrido assegnato dal leader. 48 bit di millisecondi + 16 bit di contatore.
pub type Pos = u64;
pub type Db = [u8; 16];
pub type TxnId = [u8; 16];

pub fn pos_ms(p: Pos) -> u64 {
    p >> 16
}
pub fn pos_make(ms: u64, ctr: u64) -> Pos {
    (ms << 16) | (ctr & 0xffff)
}
/// Posizione -> Timestamp BSON (secondi, incremento). Monotona e reversibile.
pub fn pos_to_bson_ts(p: Pos) -> (u32, u32) {
    let ms = pos_ms(p);
    ((ms / 1000) as u32, (((ms % 1000) << 16) | (p & 0xffff)) as u32)
}
pub fn bson_ts_to_pos(secs: u32, inc: u32) -> Pos {
    let ms = secs as u64 * 1000 + (inc as u64 >> 16);
    pos_make(ms, inc as u64 & 0xffff)
}
pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    Put = 0,
    Del = 1,
    /// Valore = i64 little-endian da sommare al valore precedente letto come i64 LE (assente = 0), come `$inc`.
    Delta = 2,
}
impl Kind {
    pub fn from_u8(x: u8) -> Option<Kind> {
        match x {
            0 => Some(Kind::Put),
            1 => Some(Kind::Del),
            2 => Some(Kind::Delta),
            _ => None,
        }
    }
}

pub fn delta_val(v: &[u8]) -> i64 {
    if v.len() >= 8 {
        i64::from_le_bytes(v[..8].try_into().unwrap())
    } else {
        0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Write {
    /// chiave dello spazio unico del motore (`doc/...` o `idx/...`), senza l'id del database
    pub key: Bytes,
    pub kind: Kind,
    pub val: Bytes,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Mode {
    Validated,
    Blind,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Session {
    pub lsid: [u8; 16],
    pub txn_number: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitReq {
    pub txn: TxnId,
    pub db: Db,
    pub read_pos: Pos,
    pub mode: Mode,
    pub writes: Vec<Write>,
    /// chiavi lette da validare (vuoto = nessuna)
    pub reads: Vec<Bytes>,
    pub session: Option<Session>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommitResp {
    Committed(Pos),
    Conflict(Bytes),
    /// si rinvia con lo stesso id e si ottiene la stessa posizione
    Ambiguous,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetResp {
    /// valore con la posizione della versione
    Valore { pos: Pos, val: Bytes },
    /// cancellato (o mai scritto, per un database nato nella finestra) alla posizione
    Assente { pos: Pos },
    /// [aggiunta proposta] la finestra ha solo delta per la chiave fino a `pos`, la base è negli SST:
    /// il valore è (valore SST alla posizione S) + `somma`. Senza questo caso il registro dovrebbe rispondere
    /// fuori_finestra e i delta della finestra non ancora su S3 andrebbero persi nella lettura.
    Delta { pos: Pos, somma: i64, potato_fino_a: Pos },
    /// la risposta sta negli SST. [aggiunta] `potato_fino_a`: tutti i record fino a questa posizione sono usciti
    /// dalla finestra, quindi chi legge deve usare un manifest con `scaricato >= potato_fino_a`; `etag` è l'ultimo
    /// manifest confermato al registro (serve solo a sapere se il manifest in cache è cambiato).
    FuoriFinestra { potato_fino_a: Pos, etag: Option<String> },
    /// [aggiunta di proto-quaderno] la replica non ha ancora applicato S
    Indietro(Pos),
}

/// Un record confermato (una transazione) come lo vede lo scarico.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub pos: Pos,
    pub txn: TxnId,
    pub writes: Vec<Write>,
}

/// [aggiunta di proto-quaderno: `pendenti`] stato di scarico di un database con record non ancora su S3.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pendente {
    pub db: Db,
    /// posizione del record più vecchio non ancora su S3
    pub piu_vecchio: Pos,
    pub record: u64,
    pub byte: u64,
    /// posizione già su S3 secondo il registro
    pub scaricato: Pos,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct StatoScarico {
    pub scaricato: Pos,
    pub etag: Option<String>,
    pub potato_fino_a: Pos,
}

/// Il registro visto da nodi d'ingresso e worker. Una replica qualsiasi per le letture, il leader per commit e
/// conferme. Nel prototipo dietro questo tratto c'è il registro simulato (in memoria, con log su file), in processo
/// o via TCP.
#[async_trait]
pub trait Registro: Send + Sync {
    async fn commit(&self, r: CommitReq) -> anyhow::Result<CommitResp>;
    async fn get(&self, db: Db, key: Bytes, s: Pos) -> anyhow::Result<GetResp>;
    /// chiavi della finestra in [da, a) con il loro esito alla posizione S
    async fn scan(&self, db: Db, da: Bytes, a: Bytes, s: Pos, limite: usize) -> anyhow::Result<Vec<(Bytes, GetResp)>>;
    async fn posizione_confermata(&self) -> anyhow::Result<Pos>;
    /// Ok(posizione) appena la posizione confermata è >= c; Err(posizione) allo scadere
    async fn attendi(&self, c: Pos, timeout_ms: u64) -> anyhow::Result<Result<Pos, Pos>>;
    /// record confermati di ciascun database con posizione in (già_su_S3, fino_a], in ordine; al massimo circa
    /// `max_byte` in tutto [trasporto: più database per richiesta, come in proto-quaderno]
    async fn scaricabili(&self, dbs: Vec<Db>, fino_a: Pos, max_byte: u64) -> anyhow::Result<Vec<(Db, Vec<Record>)>>;
    /// [trasporto: più database per richiesta] un solo manifest (etag) può coprire più database
    async fn conferma_scarico(&self, voci: Vec<(Db, Pos)>, etag: String) -> anyhow::Result<()>;
    async fn pendenti(&self, limite: usize) -> anyhow::Result<Vec<Pendente>>;
    /// [aggiunta] stato di scarico di un database
    async fn stato_scarico(&self, db: Db) -> anyhow::Result<StatoScarico>;
}

pub fn db_hex(db: &Db) -> String {
    hex::encode(db)
}
pub fn db_from_hex(s: &str) -> Option<Db> {
    let v = hex::decode(s).ok()?;
    v.try_into().ok()
}
/// Database di prova numero `i`.
pub fn db_n(i: u64) -> Db {
    let mut d = [0u8; 16];
    d[..8].copy_from_slice(&0x4645_4e56_4958_0000u64.to_be_bytes());
    d[8..].copy_from_slice(&i.to_be_bytes());
    d
}
/// Chiave di documento nello spazio unico del motore: `doc/{collezione}/{id}` (mongo-server/src/layout.rs). Qui l'id
/// è un byte di tipo e un intero a 8 byte in ordine; il motore usa `encode_sortable` di `_id`: solo la forma conta.
pub fn doc_key(coll: &str, id: u64) -> Bytes {
    let mut k = Vec::with_capacity(4 + coll.len() + 1 + 9);
    k.extend_from_slice(b"doc/");
    k.extend_from_slice(coll.as_bytes());
    k.push(b'/');
    k.push(0x10);
    k.extend_from_slice(&((id as i64 as u64) ^ (1 << 63)).to_be_bytes());
    Bytes::from(k)
}
