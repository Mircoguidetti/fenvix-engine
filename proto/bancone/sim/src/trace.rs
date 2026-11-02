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



pub struct LcsInfo {
    pub format: String,
    pub n_req_header: i64,
}

/// Legge un flusso lcs già decompresso (formato dei file di cache-datasets: intestazione da 8192 byte con la
/// versione all'offset 8; record v1 24 byte, v2 28, v3 36, v4..v8 = v3 più 1, 2, 4, 8, 16 campi da 4 byte;
/// definizione in libCacheSim, traceReader/customizedReader/lcs.h). Si ferma dopo `max` richieste.
pub fn read_lcs<R: Read>(r: R, max: usize) -> (Vec<Rec>, LcsInfo) {
    let mut r = BufReader::with_capacity(1 << 22, r);
    let mut h = vec![0u8; 8192];
    r.read_exact(&mut h).expect("intestazione lcs");
    let magic = u64::from_le_bytes(h[0..8].try_into().unwrap());
    assert_eq!(magic, 0x123456789abcdef0, "non è un file lcs");
    let end = u64::from_le_bytes(h[8184..8192].try_into().unwrap());
    assert_eq!(end, 0x123456789abcdef0, "intestazione lcs da 8192 byte attesa");
    let ver = u64::from_le_bytes(h[8..16].try_into().unwrap());
    let n_req_header = i64::from_le_bytes(h[24..32].try_into().unwrap());
    let item = match ver {
        1 => 24,
        2 => 28,
        3 => 36,
        4 => 40,
        5 => 44,
        6 => 52,
        7 => 68,
        8 => 100,
        v => panic!("versione lcs {v}"),
    };
    let mut out = Vec::with_capacity(max.min(1 << 26));
    let mut b = vec![0u8; item];
    while out.len() < max {
        if r.read_exact(&mut b).is_err() {
            break;
        }
        let t = u32::from_le_bytes(b[0..4].try_into().unwrap());
        let id = u64::from_le_bytes(b[4..12].try_into().unwrap());
        let (size, opten) = match ver {
            1 => (u32::from_le_bytes(b[12..16].try_into().unwrap()) as u64, None),
            2 => (u32::from_le_bytes(b[12..16].try_into().unwrap()) as u64, Some(u32::from_le_bytes(b[16..20].try_into().unwrap()))),
            _ => (i64::from_le_bytes(b[12..20].try_into().unwrap()).max(0) as u64, Some(u32::from_le_bytes(b[20..24].try_into().unwrap()))),
        };
        let (op, client) = match opten {
            Some(x) => (op_from_lcs((x & 0xff) as i64), x >> 8),
            None => (Op::Read, 0),
        };
        out.push(Rec { t, id, size: size.min(u32::MAX as u64) as u32, op, client });
    }
    (out, LcsInfo { format: format!("v{ver}"), n_req_header })
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
