//! Registro simulato dietro l'interfaccia comune: un solo processo fa da leader e da replica. Ogni commit è
//! confermato subito (nessuna replica vera: quella è proto-quaderno). Tiene in RAM, per database, i record non ancora
//! su S3 più quelli scaricati da meno di `finestra_ms`, indicizzati per chiave. Facoltativo: log su file di ogni
//! record confermato (db, posizione, chiavi, crc dei valori) per la verifica dello scarico.

use crate::iface::*;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write as _;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct Ver {
    pos: Pos,
    kind: Kind,
    val: Bytes,
}

#[derive(Default)]
struct DbState {
    recs: VecDeque<(Arc<Record>, u64)>,
    keys: BTreeMap<Bytes, VecDeque<Ver>>,
    scaricato: Pos,
    etag: Option<String>,
    potato: Pos,
    pend_byte: u64,
    pend_rec: u64,
    txns: HashMap<TxnId, Pos>,
    sessions: HashMap<Session, Pos>,
}

impl DbState {
    /// primo indice di `recs` con posizione > p
    fn after(&self, p: Pos) -> usize {
        self.recs.partition_point(|(r, _)| r.pos <= p)
    }
}

/// Byte di RAM attribuiti a un record: chiavi + valori + 48 byte per scrittura (versione nell'indice per chiave).
pub fn rec_bytes(w: &[Write]) -> u64 {
    w.iter().map(|w| (w.key.len() + w.val.len() + 48) as u64).sum::<u64>() + 64
}

struct Inner {
    dbs: HashMap<Db, DbState>,
    last: Pos,
    log: Option<std::io::BufWriter<std::fs::File>>,
    win_byte: u64,
    win_rec: u64,
    commits: u64,
}

pub struct RegistroSim {
    inner: Mutex<Inner>,
    pub finestra_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StatReg {
    pub posizione: Pos,
    pub commit: u64,
    /// byte e record tenuti in RAM (finestra + non ancora su S3)
    pub win_byte: u64,
    pub win_rec: u64,
    /// byte e record non ancora su S3
    pub pend_byte: u64,
    pub pend_rec: u64,
    pub db: u64,
    /// età del record più vecchio non ancora su S3
    pub ritardo_ms: u64,
}

/// Riga del log di verifica: (db, posizione, [(chiave, tipo, crc32 del valore)]).
pub type LogRow = (Db, Pos, Vec<(Vec<u8>, u8, u32)>);

impl RegistroSim {
    pub fn new(finestra_ms: u64, log: Option<&std::path::Path>) -> anyhow::Result<Arc<Self>> {
        let log = match log {
            Some(p) => Some(std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(p)?)),
            None => None,
        };
        Ok(Arc::new(RegistroSim {
            inner: Mutex::new(Inner { dbs: HashMap::new(), last: 0, log, win_byte: 0, win_rec: 0, commits: 0 }),
            finestra_ms,
        }))
    }

    /// Fa avanzare l'orologio del leader almeno fino a `p` (per partire dopo i dati precaricati negli SST).
    pub fn avanza(&self, p: Pos) {
        let mut g = self.inner.lock();
        g.last = g.last.max(p);
    }

    /// Stato di scarico iniziale di un database già presente negli SST (precarico): niente nella finestra.
    pub fn imposta_scaricato(&self, db: Db, p: Pos, etag: Option<String>) {
        let mut g = self.inner.lock();
        let st = g.dbs.entry(db).or_default();
        st.scaricato = st.scaricato.max(p);
        st.potato = st.potato.max(p);
        st.etag = etag;
    }

    pub fn posizione_confermata_sync(&self) -> Pos {
        self.inner.lock().last
    }

    pub fn flush_log(&self) {
        if let Some(l) = self.inner.lock().log.as_mut() {
            let _ = l.flush();
        }
    }

    pub fn stat(&self) -> StatReg {
        let g = self.inner.lock();
        let mut s = StatReg { posizione: g.last, commit: g.commits, win_byte: g.win_byte, win_rec: g.win_rec, db: g.dbs.len() as u64, ..Default::default() };
        let mut vecchio = u64::MAX;
        for st in g.dbs.values() {
            s.pend_byte += st.pend_byte;
            s.pend_rec += st.pend_rec;
            if st.pend_rec > 0 {
                vecchio = vecchio.min(st.recs[st.after(st.scaricato)].0.pos);
            }
        }
        if vecchio != u64::MAX {
            s.ritardo_ms = now_ms().saturating_sub(pos_ms(vecchio));
        }
        s
    }

    /// Toglie dalla finestra i record già su S3 e più vecchi di `finestra_ms`.
    pub fn pota(&self) {
        let lim = pos_make(now_ms().saturating_sub(self.finestra_ms), 0xffff);
        let mut g = self.inner.lock();
        let g = &mut *g;
        let (mut tolti_b, mut tolti_r) = (0u64, 0u64);
        for st in g.dbs.values_mut() {
            while let Some((r, b)) = st.recs.front() {
                if r.pos > st.scaricato || r.pos > lim {
                    break;
                }
                let (r, b) = (r.clone(), *b);
                st.recs.pop_front();
                for w in &r.writes {
                    if let Some(vs) = st.keys.get_mut(&w.key) {
                        if vs.front().map(|v| v.pos) == Some(r.pos) {
                            vs.pop_front();
                        }
                        if vs.is_empty() {
                            st.keys.remove(&w.key);
                        }
                    }
                }
                st.txns.remove(&r.txn);
                st.sessions.retain(|_, p| *p != r.pos);
                st.potato = r.pos;
                tolti_b += b;
                tolti_r += 1;
            }
        }
        g.win_byte -= tolti_b;
        g.win_rec -= tolti_r;
    }

    /// Avvia il task che pota la finestra ogni 100 ms.
    pub fn avvia_potatura(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                me.pota();
            }
        });
    }

    pub fn commit_sync(&self, r: CommitReq) -> CommitResp {
        let mut g = self.inner.lock();
        let g = &mut *g;
        let st = g.dbs.entry(r.db).or_default();
        if let Some(p) = st.txns.get(&r.txn) {
            return CommitResp::Committed(*p);
        }
        if let Some(s) = &r.session {
            if let Some(p) = st.sessions.get(s) {
                return CommitResp::Committed(*p);
            }
        }
        if r.mode == Mode::Validated {
            if r.read_pos < st.potato {
                let k = r.writes.first().map(|w| w.key.clone()).unwrap_or_default();
                return CommitResp::Conflict(k);
            }
            for k in r.writes.iter().map(|w| &w.key).chain(r.reads.iter()) {
                if let Some(vs) = st.keys.get(k) {
                    if vs.back().map(|v| v.pos > r.read_pos).unwrap_or(false) {
                        return CommitResp::Conflict(k.clone());
                    }
                }
            }
        }
        let pos = (g.last + 1).max(pos_make(now_ms(), 0));
        g.last = pos;
        g.commits += 1;
        let b = rec_bytes(&r.writes);
        for w in &r.writes {
            st.keys.entry(w.key.clone()).or_default().push_back(Ver { pos, kind: w.kind, val: w.val.clone() });
        }
        if let Some(l) = g.log.as_mut() {
            let row: LogRow = (r.db, pos, r.writes.iter().map(|w| (w.key.to_vec(), w.kind as u8, crc32fast::hash(&w.val))).collect());
            let body = bincode::serialize(&row).unwrap();
            let _ = l.write_all(&(body.len() as u32).to_le_bytes());
            let _ = l.write_all(&body);
        }
        st.txns.insert(r.txn, pos);
        if let Some(s) = r.session {
            st.sessions.insert(s, pos);
        }
        st.recs.push_back((Arc::new(Record { pos, txn: r.txn, writes: r.writes }), b));
        st.pend_byte += b;
        st.pend_rec += 1;
        g.win_byte += b;
        g.win_rec += 1;
        CommitResp::Committed(pos)
    }

    fn esito(st: &DbState, vs: Option<&VecDeque<Ver>>, s: Pos) -> Option<GetResp> {
        let vs = vs?;
        let mut somma: i64 = 0;
        let mut primo: Option<Pos> = None;
        for v in vs.iter().rev() {
            if v.pos > s {
                continue;
            }
            let p = *primo.get_or_insert(v.pos);
            match v.kind {
                Kind::Delta => somma = somma.wrapping_add(delta_val(&v.val)),
                Kind::Put => {
                    let val = if primo == Some(v.pos) && somma == 0 { v.val.clone() } else { Bytes::copy_from_slice(&delta_val(&v.val).wrapping_add(somma).to_le_bytes()) };
                    return Some(GetResp::Valore { pos: p, val });
                }
                Kind::Del => {
                    return Some(if p == v.pos { GetResp::Assente { pos: p } } else { GetResp::Valore { pos: p, val: Bytes::copy_from_slice(&somma.to_le_bytes()) } });
                }
            }
        }
        primo.map(|p| GetResp::Delta { pos: p, somma, potato_fino_a: st.potato })
    }

    pub fn get_sync(&self, db: Db, key: &[u8], s: Pos) -> GetResp {
        let g = self.inner.lock();
        if s > g.last {
            return GetResp::Indietro(g.last);
        }
        let Some(st) = g.dbs.get(&db) else { return GetResp::FuoriFinestra { potato_fino_a: 0, etag: None } };
        match Self::esito(st, st.keys.get(key), s) {
            Some(r) => r,
            None => GetResp::FuoriFinestra { potato_fino_a: st.potato, etag: st.etag.clone() },
        }
    }
}

#[async_trait]
impl Registro for RegistroSim {
    async fn commit(&self, r: CommitReq) -> anyhow::Result<CommitResp> {
        Ok(self.commit_sync(r))
    }

    async fn get(&self, db: Db, key: Bytes, s: Pos) -> anyhow::Result<GetResp> {
        Ok(self.get_sync(db, &key, s))
    }

    async fn scan(&self, db: Db, da: Bytes, a: Bytes, s: Pos, limite: usize) -> anyhow::Result<Vec<(Bytes, GetResp)>> {
        let g = self.inner.lock();
        if s > g.last {
            return Ok(vec![(Bytes::new(), GetResp::Indietro(g.last))]);
        }
        let Some(st) = g.dbs.get(&db) else { return Ok(vec![]) };
        let mut out = Vec::new();
        for (k, vs) in st.keys.range(da..a) {
            if out.len() >= limite {
                break;
            }
            if let Some(r) = Self::esito(st, Some(vs), s) {
                out.push((k.clone(), r));
            }
        }
        Ok(out)
    }

    async fn posizione_confermata(&self) -> anyhow::Result<Pos> {
        Ok(self.inner.lock().last)
    }

    async fn attendi(&self, c: Pos, timeout_ms: u64) -> anyhow::Result<Result<Pos, Pos>> {
        let t0 = std::time::Instant::now();
        loop {
            let p = self.inner.lock().last;
            if p >= c {
                return Ok(Ok(p));
            }
            if t0.elapsed().as_millis() as u64 >= timeout_ms {
                return Ok(Err(p));
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    async fn scaricabili(&self, dbs: Vec<Db>, fino_a: Pos, max_byte: u64) -> anyhow::Result<Vec<(Db, Vec<Record>)>> {
        let g = self.inner.lock();
        let lim = fino_a.min(g.last);
        let mut out = Vec::new();
        let mut tot = 0u64;
        for db in dbs {
            let Some(st) = g.dbs.get(&db) else { continue };
            let mut v = Vec::new();
            let mut i = st.after(st.scaricato);
            while i < st.recs.len() {
                let (r, b) = &st.recs[i];
                if r.pos > lim || (tot > 0 && tot + b > max_byte) {
                    break;
                }
                tot += b;
                v.push((**r).clone());
                i += 1;
            }
            if !v.is_empty() {
                out.push((db, v));
            }
            if tot >= max_byte {
                break;
            }
        }
        Ok(out)
    }

    async fn conferma_scarico(&self, voci: Vec<(Db, Pos)>, etag: String) -> anyhow::Result<()> {
        let mut g = self.inner.lock();
        let last = g.last;
        for (db, p) in voci {
            anyhow::ensure!(p <= last, "conferma_scarico oltre la posizione confermata");
            let st = g.dbs.entry(db).or_default();
            if p > st.scaricato {
                let a = st.after(st.scaricato);
                let b = st.after(p);
                for i in a..b {
                    st.pend_byte -= st.recs[i].1;
                    st.pend_rec -= 1;
                }
                st.scaricato = p;
                st.etag = Some(etag.clone());
            } else if p == st.scaricato {
                st.etag = Some(etag.clone());
            }
        }
        Ok(())
    }

    async fn pendenti(&self, limite: usize) -> anyhow::Result<Vec<Pendente>> {
        let g = self.inner.lock();
        let mut v: Vec<Pendente> = g
            .dbs
            .iter()
            .filter(|(_, st)| st.pend_rec > 0)
            .map(|(db, st)| Pendente { db: *db, piu_vecchio: st.recs[st.after(st.scaricato)].0.pos, record: st.pend_rec, byte: st.pend_byte, scaricato: st.scaricato })
            .collect();
        v.sort_by_key(|p| p.piu_vecchio);
        v.truncate(limite);
        Ok(v)
    }

    async fn stato_scarico(&self, db: Db) -> anyhow::Result<StatoScarico> {
        let g = self.inner.lock();
        Ok(match g.dbs.get(&db) {
            Some(st) => StatoScarico { scaricato: st.scaricato, etag: st.etag.clone(), potato_fino_a: st.potato },
            None => StatoScarico::default(),
        })
    }
}

/// Legge il log di verifica scritto dal registro.
pub fn leggi_log(p: &std::path::Path) -> anyhow::Result<Vec<LogRow>> {
    let b = std::fs::read(p)?;
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= b.len() {
        let n = u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize;
        if i + 4 + n > b.len() {
            break;
        }
        out.push(bincode::deserialize(&b[i + 4..i + 4 + n])?);
        i += 4 + n;
    }
    Ok(out)
}
