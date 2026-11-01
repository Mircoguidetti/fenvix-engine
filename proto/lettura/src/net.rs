//! Il registro simulato via TCP, per i worker in processi separati (che si possono uccidere) e per misurare le
//! letture con un giro di rete. Frame: lunghezza u32 LE + corpo bincode. Una richiesta alla volta per connessione;
//! il client tiene un piccolo gruppo di connessioni.

use crate::iface::*;
use crate::registro::{RegistroSim, StatReg};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Serialize, Deserialize)]
pub enum Req {
    Commit(CommitReq),
    Get(Db, Bytes, Pos),
    Scan(Db, Bytes, Bytes, Pos, usize),
    Confermata,
    Attendi(Pos, u64),
    Scaricabili(Vec<Db>, Pos, u64),
    Conferma(Vec<(Db, Pos)>, String),
    Pendenti(usize),
    Stato(Db),
    /// controllo del banco: "stat", "flush_log", "ferma_carico", ...
    Ctl(String),
}

#[derive(Serialize, Deserialize)]
pub enum Resp {
    Commit(CommitResp),
    Get(GetResp),
    Scan(Vec<(Bytes, GetResp)>),
    Pos(Pos),
    Attendi(Result<Pos, Pos>),
    Scaricabili(Vec<(Db, Vec<Record>)>),
    Ok,
    Pendenti(Vec<Pendente>),
    Stato(StatoScarico),
    Ctl(String),
    Err(String),
}

async fn read_frame(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let n = s.read_u32_le().await? as usize;
    if n > (256 << 20) {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame troppo grande"));
    }
    let mut b = vec![0u8; n];
    s.read_exact(&mut b).await?;
    Ok(b)
}

async fn write_frame(s: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
    let mut b = Vec::with_capacity(body.len() + 4);
    b.extend_from_slice(&(body.len() as u32).to_le_bytes());
    b.extend_from_slice(body);
    s.write_all(&b).await
}

pub type CtlFn = Arc<dyn Fn(&str) -> String + Send + Sync>;

pub async fn serve(addr: &str, reg: Arc<RegistroSim>, ctl: CtlFn) -> anyhow::Result<()> {
    let l = TcpListener::bind(addr).await?;
    loop {
        let (mut s, _) = l.accept().await?;
        s.set_nodelay(true)?;
        let reg = reg.clone();
        let ctl = ctl.clone();
        tokio::spawn(async move {
            while let Ok(b) = read_frame(&mut s).await {
                let resp = match bincode::deserialize::<Req>(&b) {
                    Ok(r) => handle(&reg, &ctl, r).await,
                    Err(e) => Resp::Err(e.to_string()),
                };
                let body = bincode::serialize(&resp).unwrap();
                if write_frame(&mut s, &body).await.is_err() {
                    break;
                }
            }
        });
    }
}

async fn handle(reg: &Arc<RegistroSim>, ctl: &CtlFn, r: Req) -> Resp {
    let res: anyhow::Result<Resp> = async {
        Ok(match r {
            Req::Commit(c) => Resp::Commit(reg.commit(c).await?),
            Req::Get(db, k, s) => Resp::Get(reg.get(db, k, s).await?),
            Req::Scan(db, a, b, s, l) => Resp::Scan(reg.scan(db, a, b, s, l).await?),
            Req::Confermata => Resp::Pos(reg.posizione_confermata().await?),
            Req::Attendi(c, t) => Resp::Attendi(reg.attendi(c, t).await?),
            Req::Scaricabili(d, f, m) => Resp::Scaricabili(reg.scaricabili(d, f, m).await?),
            Req::Conferma(v, e) => {
                reg.conferma_scarico(v, e).await?;
                Resp::Ok
            }
            Req::Pendenti(l) => Resp::Pendenti(reg.pendenti(l).await?),
            Req::Stato(db) => Resp::Stato(reg.stato_scarico(db).await?),
            Req::Ctl(c) => Resp::Ctl(if c == "stat" { serde_json::to_string(&reg.stat()).unwrap() } else { ctl(&c) }),
        })
    }
    .await;
    res.unwrap_or_else(|e| Resp::Err(e.to_string()))
}

pub struct RegistroRemoto {
    addr: String,
    conns: Vec<tokio::sync::Mutex<Option<TcpStream>>>,
    rr: AtomicUsize,
}

impl RegistroRemoto {
    pub fn new(addr: &str, n: usize) -> Arc<Self> {
        Arc::new(RegistroRemoto { addr: addr.to_string(), conns: (0..n.max(1)).map(|_| tokio::sync::Mutex::new(None)).collect(), rr: AtomicUsize::new(0) })
    }

    pub async fn call(&self, r: &Req) -> anyhow::Result<Resp> {
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        let mut g = self.conns[i].lock().await;
        if g.is_none() {
            let s = TcpStream::connect(&self.addr).await?;
            s.set_nodelay(true)?;
            *g = Some(s);
        }
        let s = g.as_mut().unwrap();
        let body = bincode::serialize(r)?;
        let res = async {
            write_frame(s, &body).await?;
            read_frame(s).await
        }
        .await;
        match res {
            Ok(b) => match bincode::deserialize::<Resp>(&b)? {
                Resp::Err(e) => anyhow::bail!("registro: {e}"),
                x => Ok(x),
            },
            Err(e) => {
                *g = None;
                Err(e.into())
            }
        }
    }

    pub async fn ctl(&self, c: &str) -> anyhow::Result<String> {
        match self.call(&Req::Ctl(c.to_string())).await? {
            Resp::Ctl(s) => Ok(s),
            _ => anyhow::bail!("risposta inattesa"),
        }
    }

    pub async fn stat(&self) -> anyhow::Result<StatReg> {
        Ok(serde_json::from_str(&self.ctl("stat").await?)?)
    }
}

macro_rules! expect {
    ($e:expr, $p:pat => $v:expr) => {
        match $e {
            $p => Ok($v),
            _ => Err(anyhow::anyhow!("risposta inattesa dal registro")),
        }
    };
}

#[async_trait]
impl Registro for RegistroRemoto {
    async fn commit(&self, r: CommitReq) -> anyhow::Result<CommitResp> {
        match self.call(&Req::Commit(r)).await {
            Ok(x) => expect!(x, Resp::Commit(c) => c),
            // nessuna risposta: il commit può essere entrato o no
            Err(_) => Ok(CommitResp::Ambiguous),
        }
    }
    async fn get(&self, db: Db, key: Bytes, s: Pos) -> anyhow::Result<GetResp> {
        expect!(self.call(&Req::Get(db, key, s)).await?, Resp::Get(g) => g)
    }
    async fn scan(&self, db: Db, da: Bytes, a: Bytes, s: Pos, limite: usize) -> anyhow::Result<Vec<(Bytes, GetResp)>> {
        expect!(self.call(&Req::Scan(db, da, a, s, limite)).await?, Resp::Scan(v) => v)
    }
    async fn posizione_confermata(&self) -> anyhow::Result<Pos> {
        expect!(self.call(&Req::Confermata).await?, Resp::Pos(p) => p)
    }
    async fn attendi(&self, c: Pos, timeout_ms: u64) -> anyhow::Result<Result<Pos, Pos>> {
        expect!(self.call(&Req::Attendi(c, timeout_ms)).await?, Resp::Attendi(r) => r)
    }
    async fn scaricabili(&self, dbs: Vec<Db>, fino_a: Pos, max_byte: u64) -> anyhow::Result<Vec<(Db, Vec<Record>)>> {
        expect!(self.call(&Req::Scaricabili(dbs, fino_a, max_byte)).await?, Resp::Scaricabili(v) => v)
    }
    async fn conferma_scarico(&self, voci: Vec<(Db, Pos)>, etag: String) -> anyhow::Result<()> {
        expect!(self.call(&Req::Conferma(voci, etag)).await?, Resp::Ok => ())
    }
    async fn pendenti(&self, limite: usize) -> anyhow::Result<Vec<Pendente>> {
        expect!(self.call(&Req::Pendenti(limite)).await?, Resp::Pendenti(v) => v)
    }
    async fn stato_scarico(&self, db: Db) -> anyhow::Result<StatoScarico> {
        expect!(self.call(&Req::Stato(db)).await?, Resp::Stato(s) => s)
    }
}
