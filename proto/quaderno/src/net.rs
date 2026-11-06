//! Rete: frame su TCP (formato `codec::frame`), lettore bufferizzato, collegamenti in uscita che si riconnettono.

use crate::codec::{frame, read_frame, FrameRead, MSG_ZSTD};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub struct FrameReader<R> {
    r: R,
    buf: BytesMut,
}

#[derive(Debug)]
pub enum ReadErr {
    Closed,
    Corrupt,
    Io(std::io::Error),
}

impl<R: AsyncReadExt + Unpin> FrameReader<R> {
    pub fn new(r: R) -> Self {
        FrameReader { r, buf: BytesMut::with_capacity(256 << 10) }
    }
    /// Il prossimo corpo di frame (senza intestazione).
    pub async fn next(&mut self) -> Result<Bytes, ReadErr> {
        loop {
            match read_frame(&self.buf) {
                FrameRead::Ok(s, e) => {
                    let f = self.buf.split_to(e).freeze();
                    return Ok(f.slice(s..e));
                }
                FrameRead::Corrupt => return Err(ReadErr::Corrupt),
                FrameRead::Incomplete => {}
            }
            if self.buf.capacity() - self.buf.len() < 64 << 10 {
                self.buf.reserve(256 << 10);
            }
            let n = self.r.read_buf(&mut self.buf).await.map_err(ReadErr::Io)?;
            if n == 0 {
                return Err(ReadErr::Closed);
            }
        }
    }
}

/// Un frame da mandare a una replica. `Z`: messaggio di replica da incorniciare e comprimere nel thread di rete di
/// uscita (giro 5, `PQ_ZSTD` con la compressione fuori dal thread principale del capo). Lo stesso `ZJob` va alle due
/// repliche: chi lo manda per primo lo comprime, l'altro riusa il risultato.
pub enum Out {
    Raw(Vec<u8>),
    Z(Arc<ZJob>),
}

pub struct ZJob {
    pub body: Vec<u8>,
    pub lvl: i32,
    pub out: OnceLock<Vec<u8>>,
    /// byte sul filo (contatore condiviso del capo)
    pub sent: Arc<AtomicU64>,
}

impl ZJob {
    pub fn get(&self) -> &[u8] {
        let f = self.out.get_or_init(|| compress_frame(&self.body, self.lvl));
        self.sent.fetch_add(f.len() as u64, Relaxed);
        f
    }
}

/// Frame compresso (`MSG_ZSTD`, lunghezza in chiaro, corpo zstd) se conviene e il corpo supera 512 byte, altrimenti in
/// chiaro. Stessa regola del giro 4.
pub fn compress_frame(b: &[u8], lvl: i32) -> Vec<u8> {
    if lvl == 0 || b.len() < 512 {
        return frame(b);
    }
    let c = zstd::bulk::compress(b, lvl).unwrap_or_default();
    if !c.is_empty() && c.len() + 5 < b.len() {
        let mut z = Vec::with_capacity(c.len() + 5);
        z.push(MSG_ZSTD);
        z.extend_from_slice(&(b.len() as u32).to_le_bytes());
        z.extend_from_slice(&c);
        frame(&z)
    } else {
        frame(b)
    }
}

/// Collegamento in uscita verso un indirizzo: tiene la connessione su, scrive i frame che riceve. I frame accodati
/// mentre il collegamento è giù vengono scartati (il protocollo ritrasmette).
pub fn spawn_out_link(addr: String) -> mpsc::UnboundedSender<Out> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Out>();
    tokio::spawn(async move {
        loop {
            let s = match tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(&addr)).await {
                Ok(Ok(s)) => s,
                _ => {
                    while rx.try_recv().is_ok() {}
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if rx.is_closed() {
                        return;
                    }
                    continue;
                }
            };
            let _ = s.set_nodelay(true);
            while rx.try_recv().is_ok() {}
            let (mut rd, mut wr) = s.into_split();
            // il lato di lettura serve solo a vedere la chiusura
            let closed = tokio::spawn(async move {
                let mut b = [0u8; 64];
                loop {
                    match rd.read(&mut b).await {
                        Ok(0) | Err(_) => return,
                        _ => {}
                    }
                }
            });
            let mut buf = Vec::with_capacity(1 << 20);
            loop {
                tokio::select! {
                    m = rx.recv() => {
                        let Some(first) = m else { return };
                        buf.clear();
                        push_out(&mut buf, first);
                        while buf.len() < (8 << 20) {
                            match rx.try_recv() { Ok(more) => push_out(&mut buf, more), Err(_) => break }
                        }
                        if wr.write_all(&buf).await.is_err() { break; }
                    }
                    // log-on-pair: the closure check runs every 200 ms even on an idle link. Before, the guard was
                    // evaluated only when the select started, so a link closed while idle (the peer restarted, or was
                    // not listening yet when this side connected) was noticed only by losing the next frame on it:
                    // the first vote request of a failover was lost and the election waited one more timeout
                    _ = tokio::time::sleep(Duration::from_millis(200)) => { if closed.is_finished() { break; } }
                }
                if closed.is_finished() {
                    break;
                }
            }
            closed.abort();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    tx
}

fn push_out(buf: &mut Vec<u8>, o: Out) {
    match o {
        Out::Raw(v) => buf.extend_from_slice(&v),
        Out::Z(j) => buf.extend_from_slice(j.get()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::*;
    use bytes::{Bytes, BytesMut};

    /// Giro 5: il frame compresso nel thread di rete (ZJob) si legge come quello del giro 4, e le due repliche
    /// ricevono gli stessi byte con una sola compressione.
    #[test]
    fn zjob_roundtrip_e_una_compressione() {
        let e = Entry::encode(3, 9, pos_make(1000, 5), Body::Rec { txn: [1; 16], db: [2; 16], session: None, writes: vec![Write { key: Bytes::from_static(b"doc/a"), kind: Kind::Put, val: Bytes::from(vec![7u8; 2000]) }] });
        let m = Msg::Append { term: 3, leader: 1, prev_index: 8, prev_term: 3, commit: 8, all_match: 8, seq: 4, entries: vec![e.raw.clone()] };
        let mut b = BytesMut::new();
        enc_msg(&mut b, &m);
        let sent = Arc::new(AtomicU64::new(0));
        let j = Arc::new(ZJob { body: b.to_vec(), lvl: 1, out: OnceLock::new(), sent: sent.clone() });
        let (a, c) = (j.get().to_vec(), j.get().to_vec());
        assert_eq!(a, c);
        assert_eq!(sent.load(Relaxed), 2 * a.len() as u64);
        assert!(a.len() < b.len() / 4);
        let FrameRead::Ok(s, en) = read_frame(&a) else { panic!() };
        match dec_msg(Bytes::from(a[s..en].to_vec())).unwrap() {
            Msg::Append { entries, seq, .. } => {
                assert_eq!(seq, 4);
                assert_eq!(entries[0], e.raw);
            }
            _ => panic!(),
        }
        // corpo piccolo: in chiaro, come prima
        let small = compress_frame(b"abc", 1);
        assert_eq!(small, frame(b"abc"));
    }
}
