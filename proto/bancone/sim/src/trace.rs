//! Lettura delle tracce: formato lcs di libCacheSim (intestazione da 1024 byte che descrive i campi) e il csv dei
//! campioni Twitter (timestamp,key,key_size,value_size,client_id,op,ttl).

use std::io::{BufRead, BufReader, Read};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    Read,
    Write,
    Delete,
}

#[derive(Clone, Copy, Debug)]
pub struct Rec {
    pub t: u32, // secondi
    pub id: u64,
    pub size: u32,
    pub op: Op,
    pub client: u32,
}

fn op_from_lcs(v: i64) -> Op {
    // valori di ReqOp in libCacheSim: GET 1, GETS 2, DELETE 9, READ 12; il resto sono scritture
    match v {
        1 | 2 | 12 => Op::Read,
        9 => Op::Delete,
        _ => Op::Write,
    }
}

fn fsize(c: u8) -> usize {
    match c {
        b'c' | b'b' | b'B' | b'?' => 1,
        b'h' | b'H' => 2,
        b'i' | b'I' | b'f' => 4,
        b'q' | b'Q' | b'd' => 8,
        _ => panic!("campo lcs sconosciuto {}", c as char),
    }
}

fn read_field(buf: &[u8], c: u8) -> i64 {
    match c {
        b'c' | b'b' => buf[0] as i8 as i64,
        b'B' | b'?' => buf[0] as i64,
        b'h' => i16::from_le_bytes([buf[0], buf[1]]) as i64,
        b'H' => u16::from_le_bytes([buf[0], buf[1]]) as i64,
        b'i' => i32::from_le_bytes(buf[..4].try_into().unwrap()) as i64,
        b'I' => u32::from_le_bytes(buf[..4].try_into().unwrap()) as i64,
        b'q' | b'Q' => i64::from_le_bytes(buf[..8].try_into().unwrap()),
        _ => 0,
    }
}

pub struct LcsInfo {
    pub format: String,
    pub n_req_header: i64,
}

/// Legge un flusso lcs già decompresso. Si ferma dopo `max` richieste.
pub fn read_lcs<R: Read>(r: R, max: usize) -> (Vec<Rec>, LcsInfo) {
    let mut r = BufReader::with_capacity(1 << 22, r);
    let mut h = [0u8; 1024];
    r.read_exact(&mut h).expect("intestazione lcs");
    let magic = u64::from_le_bytes(h[0..8].try_into().unwrap());
    assert_eq!(magic, 0x123456789abcdef0, "non è un file lcs");
    let fmt_end = h[8..248].iter().position(|&b| b == 0).unwrap_or(240);
    let format = String::from_utf8_lossy(&h[8..8 + fmt_end]).to_string();
    let (f_id, f_size, f_time, f_op, f_tenant) = (h[248], h[249], h[250], h[253], h[255]);
    let n_req_header = i64::from_le_bytes(h[488..496].try_into().unwrap());
    let item_size = u32::from_le_bytes(h[1004..1008].try_into().unwrap()) as usize;
    let fb = format.as_bytes();
    let mut offs = vec![0usize; fb.len()];
    let mut o = 0;
    for k in 1..fb.len() {
        offs[k] = o;
        o += fsize(fb[k]);
    }
    assert_eq!(o, item_size, "dimensione del record lcs");
    let mut out = Vec::with_capacity(max.min(1 << 26));
    let mut buf = vec![0u8; item_size];
    let get = |buf: &[u8], f: u8| -> i64 {
        if f == 0 {
            return 0;
        }
        let k = f as usize;
        read_field(&buf[offs[k]..], fb[k])
    };
    while out.len() < max {
        if r.read_exact(&mut buf).is_err() {
            break;
        }
        let op = if f_op != 0 { op_from_lcs(get(&buf, f_op)) } else { Op::Read };
        out.push(Rec {
            t: get(&buf, f_time) as u32,
            id: get(&buf, f_id) as u64,
            size: get(&buf, f_size).max(0) as u32,
            op,
            client: get(&buf, f_tenant) as u32,
        });
    }
    (out, LcsInfo { format, n_req_header })
}

fn fnv(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Campioni Twitter (github.com/twitter/cache-trace, samples/2020Mar).
pub fn read_twitter<R: Read>(r: R, max: usize) -> Vec<Rec> {
    let r = BufReader::with_capacity(1 << 22, r);
    let mut out = Vec::new();
    for line in r.split(b'\n') {
        if out.len() >= max {
            break;
        }
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let f: Vec<&[u8]> = line.split(|&b| b == b',').collect();
        if f.len() < 7 {
            continue;
        }
        let num = |x: &[u8]| std::str::from_utf8(x).ok().and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(0);
        let op = match f[5] {
            b"get" | b"gets" => Op::Read,
            b"delete" => Op::Delete,
            _ => Op::Write,
        };
        out.push(Rec {
            t: num(f[0]) as u32,
            id: fnv(f[1]),
            size: (num(f[2]) + num(f[3])) as u32,
            op,
            client: num(f[4]) as u32,
        });
    }
    out
}

/// oracleGeneral (time u32, id u64, size u32, next i64): solo per la verifica contro libCacheSim; tutto è lettura.
pub fn read_og<R: Read>(r: R, max: usize) -> Vec<Rec> {
    let mut r = BufReader::with_capacity(1 << 22, r);
    let mut out = Vec::new();
    let mut b = [0u8; 24];
    while out.len() < max && r.read_exact(&mut b).is_ok() {
        out.push(Rec {
            t: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            id: u64::from_le_bytes(b[4..12].try_into().unwrap()),
            size: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            op: Op::Read,
            client: 0,
        });
    }
    out
}
