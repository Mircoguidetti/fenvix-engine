//! Tipi dell'interfaccia comune (proto/INTERFACCIA.md) e loro codifica binaria.
//!
//! Frame (disco e rete), preso dal formato di journal3z (`journal3z/p2/codec`):
//! `len u32 LE | crc32 u32 LE | corpo`. Un frame con lunghezza oltre i byte disponibili o CRC sbagliato non è valido:
//! sul disco è la coda strappata di un crash, sulla rete chiude la connessione.
//! Tutti gli interi sono little-endian; la decodifica non va mai in panic su input ostile.

use bytes::{BufMut, Bytes, BytesMut};

/// Posizione: orologio logico ibrido assegnato dal leader. 48 bit di millisecondi + 16 bit di contatore.
pub type Pos = u64;
pub type Db = [u8; 16];
pub type TxnId = [u8; 16];

pub const MAX_FRAME: usize = 64 << 20;

pub fn pos_ms(p: Pos) -> u64 {
    p >> 16
}
pub fn pos_make(ms: u64, ctr: u64) -> Pos {
    (ms << 16) | (ctr & 0xffff)
}
/// Posizione -> Timestamp BSON (secondi u32, incremento u32). Monotona: l'incremento è (ms % 1000) << 16 | contatore,
/// al massimo 65.535.999 < 2^32.
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

// ------------------------------------------------------------------------------------------------ frame

pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(body.len() + 8);
    v.extend_from_slice(&(body.len() as u32).to_le_bytes());
    v.extend_from_slice(&crc32fast::hash(body).to_le_bytes());
    v.extend_from_slice(body);
    v
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrameRead {
    /// frame valido: (inizio corpo, fine corpo)
    Ok(usize, usize),
    Incomplete,
    Corrupt,
}

pub fn read_frame(buf: &[u8]) -> FrameRead {
    if buf.len() < 8 {
        return FrameRead::Incomplete;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if len == 0 || len > MAX_FRAME {
        return FrameRead::Corrupt;
    }
    if buf.len() < 8 + len {
        return FrameRead::Incomplete;
    }
    if crc32fast::hash(&buf[8..8 + len]) != crc {
        return FrameRead::Corrupt;
    }
    FrameRead::Ok(8, 8 + len)
}

// ------------------------------------------------------------------------------------------------ reader

#[derive(Debug)]
pub struct Short;
impl std::fmt::Display for Short {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "messaggio corto o malformato")
    }
}
impl std::error::Error for Short {}
pub type R<T> = Result<T, Short>;

/// Lettore su `Bytes`: le fette restituite condividono il buffer (nessuna copia dei valori).
pub struct Rd {
    b: Bytes,
    p: usize,
}
impl Rd {
    pub fn new(b: Bytes) -> Self {
        Rd { b, p: 0 }
    }
    pub fn left(&self) -> usize {
        self.b.len() - self.p
    }
    fn take(&mut self, n: usize) -> R<Bytes> {
        if self.left() < n {
            return Err(Short);
        }
        let s = self.b.slice(self.p..self.p + n);
        self.p += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> R<u16> {
        Ok(u16::from_le_bytes(self.take(2)?[..].try_into().unwrap()))
    }
    pub fn u32(&mut self) -> R<u32> {
        Ok(u32::from_le_bytes(self.take(4)?[..].try_into().unwrap()))
    }
    pub fn u64(&mut self) -> R<u64> {
        Ok(u64::from_le_bytes(self.take(8)?[..].try_into().unwrap()))
    }
    pub fn a16(&mut self) -> R<[u8; 16]> {
        Ok(self.take(16)?[..].try_into().unwrap())
    }
    pub fn bytes16(&mut self) -> R<Bytes> {
        let n = self.u16()? as usize;
        self.take(n)
    }
    pub fn bytes32(&mut self) -> R<Bytes> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn end(&self) -> R<()> {
        if self.left() == 0 {
            Ok(())
        } else {
            Err(Short)
        }
    }
}

pub fn put16(b: &mut BytesMut, s: &[u8]) {
    b.put_u16_le(s.len() as u16);
    b.put_slice(s);
}
pub fn put32(b: &mut BytesMut, s: &[u8]) {
    b.put_u32_le(s.len() as u32);
    b.put_slice(s);
}

// ------------------------------------------------------------------------------------------------ scritture

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Put = 0,
    Del = 1,
    /// Prototipo: valore = i64 LE da sommare al valore precedente (i64 LE, assente = 0), come `$inc` su un campo.
    Delta = 2,
}
impl Kind {
    pub fn from(x: u8) -> R<Kind> {
        Ok(match x {
            0 => Kind::Put,
            1 => Kind::Del,
            2 => Kind::Delta,
            _ => return Err(Short),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Write {
    /// chiave dello spazio unico del motore (doc/... o idx/...), senza l'id del database
    pub key: Bytes,
    pub kind: Kind,
    pub val: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// ogni scrittura (e ogni chiave letta dichiarata) controllata contro la posizione di lettura
    Validated = 0,
    /// put/delete/delta applicati in ordine di registro senza controllo
    Blind = 1,
}

/// Dipendenza dichiarata (docs/protocollo.md §2, branch tla-registro): `Esiste` conflittua solo con scritture
/// strutturali (put, delete) dopo S; `Valore` con qualsiasi scrittura dopo S (anche delta).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dep {
    Esiste = 0,
    Valore = 1,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub lsid: Bytes,
    pub txn_number: u64,
}

/// Richiesta di commit. Tutte le scritture di una richiesta appartengono allo stesso database (semplificazione).
#[derive(Clone, Debug)]
pub struct CommitReq {
    pub txn: TxnId,
    pub db: Db,
    pub read_pos: Pos,
    pub mode: Mode,
    pub writes: Vec<Write>,
    /// chiavi lette da validare, con il modo della dipendenza
    pub reads: Vec<(Bytes, Dep)>,
    pub session: Option<Session>,
}

pub fn enc_writes(b: &mut BytesMut, w: &[Write]) {
    b.put_u16_le(w.len() as u16);
    for x in w {
        put16(b, &x.key);
        b.put_u8(x.kind as u8);
        put32(b, &x.val);
    }
}
pub fn dec_writes(r: &mut Rd) -> R<Vec<Write>> {
    let n = r.u16()? as usize;
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let key = r.bytes16()?;
        let kind = Kind::from(r.u8()?)?;
        let val = r.bytes32()?;
        v.push(Write { key, kind, val });
    }
    Ok(v)
}
fn enc_session(b: &mut BytesMut, s: &Option<Session>) {
    match s {
        None => b.put_u8(0),
        Some(s) => {
            b.put_u8(1);
            put16(b, &s.lsid);
            b.put_u64_le(s.txn_number);
        }
    }
}
fn dec_session(r: &mut Rd) -> R<Option<Session>> {
    Ok(match r.u8()? {
        0 => None,
        _ => Some(Session { lsid: r.bytes16()?, txn_number: r.u64()? }),
    })
}

// ------------------------------------------------------------------------------------------------ voci del registro

/// Corpo di una voce del registro.
#[derive(Clone, Debug)]
pub enum Body {
    /// voce vuota che ogni nuovo leader aggiunge (regola di Raft: si conferma solo nel proprio mandato)
    Noop,
    /// un record: una transazione confermata
    Rec { txn: TxnId, db: Db, session: Option<Session>, writes: Vec<Write> },
    /// conferma_scarico: questi database sono su S3 fino a questa posizione (applicato alla conferma)
    Flushed(Vec<(Db, Pos, Bytes)>),
}

/// Una voce del registro. `raw` è il frame completo così come sta sul disco e viaggia in rete.
#[derive(Clone, Debug)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub pos: Pos,
    pub body: Body,
    pub raw: Bytes,
}

impl Entry {
    pub fn encode(term: u64, index: u64, pos: Pos, body: Body) -> Entry {
        let mut b = BytesMut::with_capacity(64);
        b.put_u64_le(term);
        b.put_u64_le(index);
        b.put_u64_le(pos);
        match &body {
            Body::Noop => b.put_u8(0),
            Body::Rec { txn, db, session, writes } => {
                b.put_u8(1);
                b.put_slice(txn);
                b.put_slice(db);
                enc_session(&mut b, session);
                enc_writes(&mut b, writes);
            }
            Body::Flushed(v) => {
                b.put_u8(2);
                b.put_u32_le(v.len() as u32);
                for (db, p, etag) in v {
                    b.put_slice(db);
                    b.put_u64_le(*p);
                    put16(&mut b, etag);
                }
            }
        }
        let raw = Bytes::from(frame(&b));
        // ri-decodifica dal frame: così i valori puntano dentro `raw` e la RAM non li tiene due volte
        Entry::decode(raw).expect("codifica appena fatta")
    }

    /// `raw` = frame completo (len | crc | corpo), CRC già verificato da chi chiama.
    pub fn decode(raw: Bytes) -> R<Entry> {
        if raw.len() < 8 {
            return Err(Short);
        }
        let mut r = Rd::new(raw.slice(8..));
        let term = r.u64()?;
        let index = r.u64()?;
        let pos = r.u64()?;
        let body = match r.u8()? {
            0 => Body::Noop,
            1 => {
                let txn = r.a16()?;
                let db = r.a16()?;
                let session = dec_session(&mut r)?;
                let writes = dec_writes(&mut r)?;
                Body::Rec { txn, db, session, writes }
            }
            2 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    v.push((r.a16()?, r.u64()?, r.bytes16()?));
                }
                Body::Flushed(v)
            }
            _ => return Err(Short),
        };
        r.end()?;
        Ok(Entry { term, index, pos, body, raw })
    }
}

// ------------------------------------------------------------------------------------------------ messaggi tra repliche

#[derive(Clone, Debug)]
pub enum Msg {
    /// Raft AppendEntries. `seq` serve alle letture fresche (ReadIndex): la risposta lo riporta.
    Append { term: u64, leader: u8, prev_index: u64, prev_term: u64, commit: u64, all_match: u64, seq: u64, entries: Vec<Bytes> },
    /// `ack` = ultimo indice sul disco (fdatasync) che coincide col registro del leader; `hint` per tornare indietro
    AppendResp { term: u64, from: u8, ok: bool, ack: u64, hint: u64, seq: u64 },
    /// `pre` = pre-voto (non cambia il mandato di nessuno)
    Vote { term: u64, from: u8, last_index: u64, last_term: u64, pre: bool },
    VoteResp { term: u64, from: u8, granted: bool, pre: bool },
}

pub fn enc_msg(b: &mut BytesMut, m: &Msg) {
    match m {
        Msg::Append { term, leader, prev_index, prev_term, commit, all_match, seq, entries } => {
            b.put_u8(1);
            b.put_u64_le(*term);
            b.put_u8(*leader);
            b.put_u64_le(*prev_index);
            b.put_u64_le(*prev_term);
            b.put_u64_le(*commit);
            b.put_u64_le(*all_match);
            b.put_u64_le(*seq);
            b.put_u32_le(entries.len() as u32);
            for e in entries {
                b.put_slice(e); // già frame con lunghezza e CRC
            }
        }
        Msg::AppendResp { term, from, ok, ack, hint, seq } => {
            b.put_u8(2);
            b.put_u64_le(*term);
            b.put_u8(*from);
            b.put_u8(*ok as u8);
            b.put_u64_le(*ack);
            b.put_u64_le(*hint);
            b.put_u64_le(*seq);
        }
        Msg::Vote { term, from, last_index, last_term, pre } => {
            b.put_u8(3);
            b.put_u64_le(*term);
            b.put_u8(*from);
            b.put_u64_le(*last_index);
            b.put_u64_le(*last_term);
            b.put_u8(*pre as u8);
        }
        Msg::VoteResp { term, from, granted, pre } => {
            b.put_u8(4);
            b.put_u64_le(*term);
            b.put_u8(*from);
            b.put_u8(*granted as u8);
            b.put_u8(*pre as u8);
        }
    }
}

pub fn dec_msg(body: Bytes) -> R<Msg> {
    let mut r = Rd::new(body.clone());
    Ok(match r.u8()? {
        1 => {
            let term = r.u64()?;
            let leader = r.u8()?;
            let prev_index = r.u64()?;
            let prev_term = r.u64()?;
            let commit = r.u64()?;
            let all_match = r.u64()?;
            let seq = r.u64()?;
            let n = r.u32()? as usize;
            let mut entries = Vec::with_capacity(n.min(1 << 16));
            let mut p = body.len() - r.left();
            for _ in 0..n {
                match read_frame(&body[p..]) {
                    FrameRead::Ok(_, end) => {
                        entries.push(body.slice(p..p + end));
                        p += end;
                    }
                    _ => return Err(Short),
                }
            }
            if p != body.len() {
                return Err(Short);
            }
            Msg::Append { term, leader, prev_index, prev_term, commit, all_match, seq, entries }
        }
        2 => Msg::AppendResp { term: r.u64()?, from: r.u8()?, ok: r.u8()? != 0, ack: r.u64()?, hint: r.u64()?, seq: r.u64()? },
        3 => Msg::Vote { term: r.u64()?, from: r.u8()?, last_index: r.u64()?, last_term: r.u64()?, pre: r.u8()? != 0 },
        4 => Msg::VoteResp { term: r.u64()?, from: r.u8()?, granted: r.u8()? != 0, pre: r.u8()? != 0 },
        _ => return Err(Short),
    })
}

// ------------------------------------------------------------------------------------------------ client <-> replica

#[derive(Clone, Debug)]
pub enum Req {
    Commit(CommitReq),
    /// get(db, chiave, S). `spec` = lettura speculativa dal leader (ultima versione accettata, anche non ancora
    /// confermata): aggiunta del prototipo per la modalità validata sulla chiave calda, vedi INTERFACCIA.md
    Get { db: Db, key: Bytes, at: Pos, wait_ms: u32, spec: bool },
    Scan { db: Db, from: Bytes, to: Bytes, at: Pos, limit: u32, wait_ms: u32 },
    /// posizione_confermata(): lineare (ReadIndex sul leader); un follower risponde NotLeader
    Confirmed,
    /// attendi(C, timeout)
    Wait { pos: Pos, timeout_ms: u32 },
    /// scaricabili(db, fino_a) per più database in un colpo (raggruppamento di trasporto)
    Flushable { dbs: Vec<Db>, upto: Pos },
    /// conferma_scarico(db, fino_a, etag) per più database in un colpo
    FlushDone { items: Vec<(Db, Pos, Bytes)> },
    /// aiuto per gli scaricatori: database con record non ancora su S3 (e posizione del più vecchio)
    Pending { limit: u32 },
    Status,
    /// solo per i controlli dei banchi: voci da `from_index` (txn, posizione) già confermate
    Dump { from_index: u64, max: u32 },
}

#[derive(Clone, Debug)]
pub enum GetRes {
    /// valore e posizione della versione
    Value(Pos, Bytes),
    /// cancellata o mai scritta (finestra completa); posizione della cancellazione o 0
    Absent(Pos),
    /// la finestra non basta: la risposta sta negli SST
    OutOfWindow,
}

#[derive(Clone, Debug)]
pub struct FlushRec {
    pub pos: Pos,
    pub writes: Vec<Write>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Status {
    pub id: u8,
    pub role: String,
    pub term: u64,
    pub leader: i32,
    pub last_index: u64,
    pub commit_index: u64,
    pub commit_pos: u64,
    pub durable: u64,
    pub log_front: u64,
    pub dbs: u64,
    pub keys: u64,
    pub versions: u64,
    pub resident_bytes: u64,
    pub evicted: u64,
    pub unflushed_recs: u64,
    pub min_unflushed_pos: u64,
    pub segments: u64,
    pub disk_bytes: u64,
    pub rss_kb: u64,
    #[serde(default)]
    pub diag: String,
}

#[derive(Clone, Debug)]
pub enum Resp {
    Committed(Pos),
    Conflict(Bytes),
    Ambiguous,
    NotLeader(i32),
    Get(GetRes),
    Scan(Vec<(Bytes, GetRes)>),
    /// la replica non ha ancora applicato S entro l'attesa: posizione confermata che conosce
    Behind(Pos),
    Pos(Pos),
    Flushable(Vec<(Db, Vec<FlushRec>)>),
    Pending(Vec<(Db, Pos)>),
    Status(Status),
    Dump(Vec<(u64, TxnId, Pos)>),
    Err(String),
}

fn enc_getres(b: &mut BytesMut, g: &GetRes) {
    match g {
        GetRes::Value(p, v) => {
            b.put_u8(0);
            b.put_u64_le(*p);
            put32(b, v);
        }
        GetRes::Absent(p) => {
            b.put_u8(1);
            b.put_u64_le(*p);
        }
        GetRes::OutOfWindow => b.put_u8(2),
    }
}
fn dec_getres(r: &mut Rd) -> R<GetRes> {
    Ok(match r.u8()? {
        0 => GetRes::Value(r.u64()?, r.bytes32()?),
        1 => GetRes::Absent(r.u64()?),
        2 => GetRes::OutOfWindow,
        _ => return Err(Short),
    })
}

/// corpo della richiesta: id u64 | tag | campi
pub fn enc_req(id: u64, q: &Req) -> Vec<u8> {
    let mut b = BytesMut::with_capacity(64);
    b.put_u64_le(id);
    match q {
        Req::Commit(c) => {
            b.put_u8(1);
            b.put_slice(&c.txn);
            b.put_slice(&c.db);
            b.put_u64_le(c.read_pos);
            b.put_u8(c.mode as u8);
            enc_writes(&mut b, &c.writes);
            b.put_u16_le(c.reads.len() as u16);
            for (k, d) in &c.reads {
                put16(&mut b, k);
                b.put_u8(*d as u8);
            }
            enc_session(&mut b, &c.session);
        }
        Req::Get { db, key, at, wait_ms, spec } => {
            b.put_u8(2);
            b.put_slice(db);
            put16(&mut b, key);
            b.put_u64_le(*at);
            b.put_u32_le(*wait_ms);
            b.put_u8(*spec as u8);
        }
        Req::Scan { db, from, to, at, limit, wait_ms } => {
            b.put_u8(3);
            b.put_slice(db);
            put16(&mut b, from);
            put16(&mut b, to);
            b.put_u64_le(*at);
            b.put_u32_le(*limit);
            b.put_u32_le(*wait_ms);
        }
        Req::Confirmed => b.put_u8(4),
        Req::Wait { pos, timeout_ms } => {
            b.put_u8(5);
            b.put_u64_le(*pos);
            b.put_u32_le(*timeout_ms);
        }
        Req::Flushable { dbs, upto } => {
            b.put_u8(6);
            b.put_u64_le(*upto);
            b.put_u32_le(dbs.len() as u32);
            for d in dbs {
                b.put_slice(d);
            }
        }
        Req::FlushDone { items } => {
            b.put_u8(7);
            b.put_u32_le(items.len() as u32);
            for (d, p, e) in items {
                b.put_slice(d);
                b.put_u64_le(*p);
                put16(&mut b, e);
            }
        }
        Req::Pending { limit } => {
            b.put_u8(8);
            b.put_u32_le(*limit);
        }
        Req::Status => b.put_u8(9),
        Req::Dump { from_index, max } => {
            b.put_u8(10);
            b.put_u64_le(*from_index);
            b.put_u32_le(*max);
        }
    }
    frame(&b)
}

pub fn dec_req(body: Bytes) -> R<(u64, Req)> {
    let mut r = Rd::new(body);
    let id = r.u64()?;
    let q = match r.u8()? {
        1 => {
            let txn = r.a16()?;
            let db = r.a16()?;
            let read_pos = r.u64()?;
            let mode = if r.u8()? == 0 { Mode::Validated } else { Mode::Blind };
            let writes = dec_writes(&mut r)?;
            let n = r.u16()? as usize;
            let mut reads = Vec::with_capacity(n);
            for _ in 0..n {
                let k = r.bytes16()?;
                let d = if r.u8()? == 0 { Dep::Esiste } else { Dep::Valore };
                reads.push((k, d));
            }
            let session = dec_session(&mut r)?;
            Req::Commit(CommitReq { txn, db, read_pos, mode, writes, reads, session })
        }
        2 => Req::Get { db: r.a16()?, key: r.bytes16()?, at: r.u64()?, wait_ms: r.u32()?, spec: r.u8()? != 0 },
        3 => Req::Scan { db: r.a16()?, from: r.bytes16()?, to: r.bytes16()?, at: r.u64()?, limit: r.u32()?, wait_ms: r.u32()? },
        4 => Req::Confirmed,
        5 => Req::Wait { pos: r.u64()?, timeout_ms: r.u32()? },
        6 => {
            let upto = r.u64()?;
            let n = r.u32()? as usize;
            let mut dbs = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                dbs.push(r.a16()?);
            }
            Req::Flushable { dbs, upto }
        }
        7 => {
            let n = r.u32()? as usize;
            let mut items = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                items.push((r.a16()?, r.u64()?, r.bytes16()?));
            }
            Req::FlushDone { items }
        }
        8 => Req::Pending { limit: r.u32()? },
        9 => Req::Status,
        10 => Req::Dump { from_index: r.u64()?, max: r.u32()? },
        _ => return Err(Short),
    };
    r.end()?;
    Ok((id, q))
}

pub fn enc_resp(id: u64, s: &Resp) -> Vec<u8> {
    let mut b = BytesMut::with_capacity(32);
    b.put_u64_le(id);
    match s {
        Resp::Committed(p) => {
            b.put_u8(1);
            b.put_u64_le(*p);
        }
        Resp::Conflict(k) => {
            b.put_u8(2);
            put16(&mut b, k);
        }
        Resp::Ambiguous => b.put_u8(3),
        Resp::NotLeader(l) => {
            b.put_u8(4);
            b.put_i32_le(*l);
        }
        Resp::Get(g) => {
            b.put_u8(5);
            enc_getres(&mut b, g);
        }
        Resp::Scan(v) => {
            b.put_u8(6);
            b.put_u32_le(v.len() as u32);
            for (k, g) in v {
                put16(&mut b, k);
                enc_getres(&mut b, g);
            }
        }
        Resp::Behind(p) => {
            b.put_u8(7);
            b.put_u64_le(*p);
        }
        Resp::Pos(p) => {
            b.put_u8(8);
            b.put_u64_le(*p);
        }
        Resp::Flushable(v) => {
            b.put_u8(9);
            b.put_u32_le(v.len() as u32);
            for (db, recs) in v {
                b.put_slice(db);
                b.put_u32_le(recs.len() as u32);
                for r in recs {
                    b.put_u64_le(r.pos);
                    enc_writes(&mut b, &r.writes);
                }
            }
        }
        Resp::Pending(v) => {
            b.put_u8(10);
            b.put_u32_le(v.len() as u32);
            for (db, p) in v {
                b.put_slice(db);
                b.put_u64_le(*p);
            }
        }
        Resp::Status(st) => {
            b.put_u8(11);
            put32(&mut b, serde_json::to_string(st).unwrap().as_bytes());
        }
        Resp::Dump(v) => {
            b.put_u8(12);
            b.put_u32_le(v.len() as u32);
            for (i, t, p) in v {
                b.put_u64_le(*i);
                b.put_slice(t);
                b.put_u64_le(*p);
            }
        }
        Resp::Err(e) => {
            b.put_u8(13);
            put16(&mut b, e.as_bytes());
        }
    }
    frame(&b)
}

pub fn dec_resp(body: Bytes) -> R<(u64, Resp)> {
    let mut r = Rd::new(body);
    let id = r.u64()?;
    let s = match r.u8()? {
        1 => Resp::Committed(r.u64()?),
        2 => Resp::Conflict(r.bytes16()?),
        3 => Resp::Ambiguous,
        4 => Resp::NotLeader(r.u32()? as i32),
        5 => Resp::Get(dec_getres(&mut r)?),
        6 => {
            let n = r.u32()? as usize;
            let mut v = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                v.push((r.bytes16()?, dec_getres(&mut r)?));
            }
            Resp::Scan(v)
        }
        7 => Resp::Behind(r.u64()?),
        8 => Resp::Pos(r.u64()?),
        9 => {
            let n = r.u32()? as usize;
            let mut v = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                let db = r.a16()?;
                let m = r.u32()? as usize;
                let mut recs = Vec::with_capacity(m.min(1 << 16));
                for _ in 0..m {
                    recs.push(FlushRec { pos: r.u64()?, writes: dec_writes(&mut r)? });
                }
                v.push((db, recs));
            }
            Resp::Flushable(v)
        }
        10 => {
            let n = r.u32()? as usize;
            let mut v = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                v.push((r.a16()?, r.u64()?));
            }
            Resp::Pending(v)
        }
        11 => {
            let s = r.bytes32()?;
            Resp::Status(serde_json::from_slice(&s).map_err(|_| Short)?)
        }
        12 => {
            let n = r.u32()? as usize;
            let mut v = Vec::with_capacity(n.min(1 << 20));
            for _ in 0..n {
                v.push((r.u64()?, r.a16()?, r.u64()?));
            }
            Resp::Dump(v)
        }
        13 => Resp::Err(String::from_utf8_lossy(&r.bytes16()?).into_owned()),
        _ => return Err(Short),
    };
    Ok((id, s))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn entry_roundtrip() {
        let w = vec![Write { key: Bytes::from_static(b"doc/a"), kind: Kind::Put, val: Bytes::from(vec![7u8; 200]) }];
        let e = Entry::encode(3, 9, pos_make(1000, 5), Body::Rec { txn: [1; 16], db: [2; 16], session: None, writes: w });
        assert!(matches!(read_frame(&e.raw), FrameRead::Ok(8, _)));
        let d = Entry::decode(e.raw.clone()).unwrap();
        assert_eq!((d.term, d.index, d.pos), (3, 9, pos_make(1000, 5)));
        match d.body {
            Body::Rec { writes, .. } => assert_eq!(writes[0].val.len(), 200),
            _ => panic!(),
        }
    }
    #[test]
    fn bson_ts() {
        let p = pos_make(1_700_000_000_123, 77);
        let (s, i) = pos_to_bson_ts(p);
        assert_eq!(bson_ts_to_pos(s, i), p);
        assert!(pos_to_bson_ts(p + 1) > pos_to_bson_ts(p));
        assert!(pos_to_bson_ts(pos_make(1_700_000_000_124, 0)) > pos_to_bson_ts(pos_make(1_700_000_000_123, 65535)));
    }
}
