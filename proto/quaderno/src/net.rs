//! Rete: frame su TCP (formato `codec::frame`), lettore bufferizzato, collegamenti in uscita che si riconnettono.

use crate::codec::{read_frame, FrameRead};
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

/// Collegamento in uscita verso un indirizzo: tiene la connessione su, scrive i frame che riceve. I frame accodati
/// mentre il collegamento è giù vengono scartati (il protocollo ritrasmette).
pub fn spawn_out_link(addr: String) -> mpsc::UnboundedSender<Vec<u8>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
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
                        buf.extend_from_slice(&first);
                        while buf.len() < (8 << 20) {
                            match rx.try_recv() { Ok(more) => buf.extend_from_slice(&more), Err(_) => break }
                        }
                        if wr.write_all(&buf).await.is_err() { break; }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)), if closed.is_finished() => { break; }
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
