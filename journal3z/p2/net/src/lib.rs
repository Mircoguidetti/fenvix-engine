//! journal3z network (phase-2 plan P4).
//!
//! One TCP connection per writer–node pair. Every frame is `j3z_codec::frame` (`len | crc32 | body`). The first frame
//! in each direction is a handshake: `"J3ZH" | version u8 | id u32` (the writer sends its address, the node answers
//! with its identity — a node whose disk was lost answers with a new identity). Every later frame is a batch of
//! protocol messages (`j3z_codec::encode_batch`). An **empty batch** is a keepalive: the writer sends one every
//! `PING`, the node answers each with an empty batch; a link that receives nothing for `DEAD_AFTER` is closed and
//! reconnected (a cut that drops packets silently would otherwise hang the connection forever).
//!
//! Messages queued while a link is down are dropped: the protocol retransmits (writer ticks), it does not rely on the
//! network to deliver anything.

use j3z_codec::{decode_batch, encode_batch, frame, read_frame, FrameRead};
use journal3z_core::{Addr, Msg, NodeId};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub const HELLO: &[u8; 4] = b"J3ZH";
pub const NET_VERSION: u8 = 1;
pub const PING: Duration = Duration::from_millis(500);
pub const DEAD_AFTER: Duration = Duration::from_secs(2);
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub fn hello(id: u32) -> Vec<u8> {
    let mut b = HELLO.to_vec();
    b.push(NET_VERSION);
    b.extend_from_slice(&id.to_le_bytes());
    frame(&b)
}

pub fn parse_hello(body: &[u8]) -> Option<u32> {
    if body.len() != 9 || &body[..4] != HELLO || body[4] != NET_VERSION {
        return None;
    }
    Some(u32::from_le_bytes(body[5..9].try_into().unwrap()))
}

/// Buffered frame reader over a byte stream.
pub struct FrameReader<R> {
    r: R,
    buf: Vec<u8>,
}

#[derive(Debug)]
pub enum ReadErr {
    Closed,
    Corrupt,
    Io(std::io::Error),
}

impl<R: AsyncReadExt + Unpin> FrameReader<R> {
    pub fn new(r: R) -> Self {
        FrameReader { r, buf: Vec::with_capacity(64 << 10) }
    }

    /// The next frame body.
    pub async fn next(&mut self) -> Result<Vec<u8>, ReadErr> {
        loop {
            match read_frame(&self.buf) {
                FrameRead::Ok(body, used) => {
                    let b = body.to_vec();
                    self.buf.drain(..used);
                    return Ok(b);
                }
                FrameRead::Corrupt => return Err(ReadErr::Corrupt),
                FrameRead::Incomplete => {}
            }
            let mut tmp = [0u8; 64 << 10];
            let n = self.r.read(&mut tmp).await.map_err(ReadErr::Io)?;
            if n == 0 {
                return Err(ReadErr::Closed);
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}

/// Events a writer receives from its links.
#[derive(Debug)]
pub enum LinkEvent {
    /// Connected to the node at `slot`, which says it is `node`.
    Up { slot: usize, node: NodeId },
    Down { slot: usize },
    Msgs { slot: usize, node: NodeId, msgs: Vec<Msg> },
}

/// A writer's link to one node address: keeps a connection up (reconnecting with a short backoff), sends what it is
/// given, delivers what arrives. Returns the sender for outbound batches.
pub fn spawn_link(slot: usize, addr: String, me: Addr, events: mpsc::UnboundedSender<LinkEvent>) -> mpsc::UnboundedSender<Vec<Msg>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<Msg>>();
    tokio::spawn(async move {
        loop {
            let r: Result<(), String> = async {
                let s = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr)).await.map_err(|_| "connect timeout".to_string())?.map_err(|e| e.to_string())?;
                s.set_nodelay(true).map_err(|e| e.to_string())?;
                let (rd, mut wr) = s.into_split();
                let mut reader = FrameReader::new(rd);
                wr.write_all(&hello(me)).await.map_err(|e| e.to_string())?;
                let body = tokio::time::timeout(DEAD_AFTER, reader.next()).await.map_err(|_| "no hello".to_string())?.map_err(|e| format!("{e:?}"))?;
                let node = parse_hello(&body).ok_or("bad hello")?;
                let _ = events.send(LinkEvent::Up { slot, node });
                // drop what was queued while down: the protocol retransmits
                while rx.try_recv().is_ok() {}
                let mut ping = tokio::time::interval(PING);
                let mut last_in = tokio::time::Instant::now();
                loop {
                    tokio::select! {
                        m = rx.recv() => {
                            let Some(first) = m else { return Ok(()) };
                            let mut batch = first;
                            while let Ok(more) = rx.try_recv() {
                                batch.extend(more);
                            }
                            wr.write_all(&encode_batch(&batch)).await.map_err(|e| e.to_string())?;
                        }
                        f = reader.next() => {
                            let body = f.map_err(|e| format!("{e:?}"))?;
                            last_in = tokio::time::Instant::now();
                            let msgs = decode_batch(&body).map_err(|e| e.to_string())?;
                            if !msgs.is_empty() {
                                let _ = events.send(LinkEvent::Msgs { slot, node, msgs });
                            }
                        }
                        _ = ping.tick() => {
                            if last_in.elapsed() > DEAD_AFTER {
                                return Err("dead (no frame received)".into());
                            }
                            wr.write_all(&encode_batch(&[])).await.map_err(|e| e.to_string())?;
                        }
                    }
                }
            }
            .await;
            if events.send(LinkEvent::Down { slot }).is_err() {
                return; // the writer is gone
            }
            if r.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100 + rand::random::<u64>() % 200)).await;
        }
    });
    tx
}

// ================================================================================================== fault proxy

/// One proxy route: listen here, forward to there.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Route {
    pub name: String,
    pub listen: String,
    pub target: String,
}

/// What a route does to traffic. `c2s` = client (writer) to server (node).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Pass,
    /// Every byte in both directions is read and dropped; connections stay open (a silent partition).
    Blackhole,
    /// Every connection is closed and new ones are refused.
    Cut,
    /// Only writer→node bytes are dropped.
    DropC2s,
    /// Only node→writer bytes are dropped.
    DropS2c,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct RouteCtl {
    #[serde(default)]
    pub mode: Mode,
    /// Extra one-way delay added to every chunk (both directions), milliseconds.
    #[serde(default)]
    pub delay_ms: u64,
}
