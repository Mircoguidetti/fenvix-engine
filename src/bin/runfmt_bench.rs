//! Measures the sealed-run format v1 (compaction 2, step 1): writer CPU per GB and the maps kept in RAM per stored GB.
//!
//! Usage: `cargo run --release --bin runfmt_bench -- [raw GiB, default 1] [block KiB, default 256]`
//!
//! Data: customer-keyed chat messages in key layout v2 (`u/{customer}/messages/d/{_id}`), each about 470 bytes of
//! BSON-like bytes (field names, an ObjectId, a time, words: half from 28 common ones, half from 20,000 random ones), 2% delete envelopes, plus one index entry per message
//! (`u/{customer}/messages/x/1/{ts}{_id}`), customers with Pareto-distributed message counts (about 2,100 customers
//! per GiB, the 1 TB medium profile's 2.1 million users per TB). Rows are generated first, into one arena, so the CPU
//! measured is the writer's only: lz4, CRC32C, the customer filter, the fence, and the multipart upload into an
//! in-memory store (one thread, `getrusage(RUSAGE_THREAD)`). Then the run is opened and its maps measured, and read
//! back whole with the streaming reader (CPU measured too).

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tenant_bench::runfmt::*;

fn thread_cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn cust(u: i64) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.push(17);
    k.push(3);
    let f = (u as f64).to_bits() | (1 << 63);
    k.extend(f.to_be_bytes());
    k.extend(((u as u64) ^ (1 << 63)).to_be_bytes());
    k
}

const WORDS: &[&str] = &[
    "the", "agent", "replied", "with", "a", "summary", "of", "order", "status", "please", "check", "invoice", "shipping", "tomorrow", "thanks",
    "customer", "support", "ticket", "update", "refund", "account", "password", "reset", "delivery", "address", "confirmed", "pending", "error",
];

struct Arena {
    bytes: Vec<u8>,
    /// (key start, key len, env len, seq)
    rows: Vec<(usize, u32, u32, u64)>,
}

impl Arena {
    fn push(&mut self, k: &[u8], env: &[u8], seq: u64) {
        self.rows.push((self.bytes.len(), k.len() as u32, env.len() as u32, seq));
        self.bytes.extend_from_slice(k);
        self.bytes.extend_from_slice(env);
    }
}

fn generate(raw_target: u64, rng: &mut StdRng) -> (Arena, u64) {
    // a vocabulary of 20,000 random words next to the common ones: chat text that lz4 compresses about like BSON
    let mut vocab: Vec<Vec<u8>> = WORDS.iter().map(|w| w.as_bytes().to_vec()).collect();
    for _ in 0..20_000 {
        let n = rng.gen_range(2..11);
        vocab.push((0..n).map(|_| b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJ0123456789"[rng.gen_range(0..46)]).collect());
    }
    let mut a = Arena { bytes: Vec::with_capacity(raw_target as usize + (raw_target as usize / 8)), rows: Vec::new() };
    let (mut u, mut seq, mut ts) = (0i64, 1u64, 1_700_000_000_000u64);
    let mut customers = 0;
    while (a.bytes.len() as u64) < raw_target {
        // Pareto counts, mean about 500 messages (with the index entries about 1,000 rows and 0.48 MB per customer)
        let x: f64 = rng.gen_range(0.0001f64..1.0);
        let n = ((50.0 / x.powf(1.0 / 1.11)) as u64).clamp(1, 200_000);
        let p = cust(u);
        let mut docs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            ts += rng.gen_range(1..5000);
            let mut oid = [0u8; 12];
            oid[0..4].copy_from_slice(&((ts / 1000) as u32).to_be_bytes());
            // machine and process bytes, then a counter: ObjectIds grow within a customer, as the driver makes them
            oid[4..9].copy_from_slice(&[0x5a, 0x11, 0x3c, 0x07, 0x42]);
            oid[9..12].copy_from_slice(&(seq as u32).to_be_bytes()[1..]);
            let mut k = p.clone();
            k.extend(b"/messages/d/");
            k.push(7);
            k.extend(oid);
            let env = if rng.gen_bool(0.02) {
                deleted()
            } else {
                let mut d = Vec::with_capacity(480);
                d.extend(b"\x00\x00\x00\x00\x07_id\x00");
                d.extend(oid);
                d.extend(b"\x12userId\x00");
                d.extend(u.to_le_bytes());
                d.extend(b"\x09ts\x00");
                d.extend(ts.to_le_bytes());
                d.extend(b"\x02role\x00\x05\x00\x00\x00user\x00\x02text\x00");
                let target = rng.gen_range(300..560);
                while d.len() < target {
                    let w = if rng.gen_bool(0.5) { &vocab[rng.gen_range(0..WORDS.len())] } else { &vocab[rng.gen_range(0..vocab.len())] };
                    d.extend(w);
                    d.push(b' ');
                }
                d.push(0);
                d.push(0);
                let l = d.len() as u32;
                d[0..4].copy_from_slice(&l.to_le_bytes());
                live(&d)
            };
            docs.push((k, env, seq, ts, oid));
            seq += 1;
        }
        for (k, env, s, _, _) in &docs {
            a.push(k, env, *s);
        }
        for (_, env, s, t, oid) in &docs {
            let mut k = p.clone();
            k.extend(b"/messages/x/1/");
            k.push(9);
            k.extend(t.to_be_bytes());
            k.push(7);
            k.extend(oid);
            a.push(&k, if env[0] == DELETED { &[DELETED][..] } else { &[LIVE][..] }, *s);
        }
        u += 1;
        customers += 1;
    }
    (a, customers)
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let gib: f64 = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(1.0);
    let block_kib: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(256);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("model name")).map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string()))
        .unwrap_or_else(|| "UNKNOWN".into());
    let mut rng = StdRng::seed_from_u64(20261107);
    let raw_target = (gib * (1u64 << 30) as f64) as u64;
    let (arena, customers) = generate(raw_target, &mut rng);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let opts = RunOptions::new(block_kib << 10, DEFAULT_PART, CustomerKind::V2)?;
    let path = Path::from("bench/run-0");
    let (meta, cpu_w, wall_w) = rt.block_on(async {
        let t0 = std::time::Instant::now();
        let c0 = thread_cpu();
        let mut w = RunWriter::new(&store, path.clone(), opts).await?;
        for &(s, kl, el, seq) in &arena.rows {
            let k = &arena.bytes[s..s + kl as usize];
            let e = &arena.bytes[s + kl as usize..s + kl as usize + el as usize];
            w.add(k, seq, e).await?;
        }
        let meta = w.finish().await?;
        anyhow::Ok((meta, thread_cpu() - c0, t0.elapsed().as_secs_f64()))
    })?;
    let raw_gib = meta.raw_bytes as f64 / (1u64 << 30) as f64;
    let stored_gib = meta.size as f64 / (1u64 << 30) as f64;
    let (map_bytes, fence_bytes, filter_bytes, open_gets) = rt.block_on(async {
        let (r, gets) = Run::open(&store, path.clone(), meta.size).await?;
        let fence = r.fence.mem_bytes();
        let filter = r.filter.mem_bytes();
        let rd = RunReader::new(store.clone(), vec![r], 0, 1)?;
        anyhow::Ok((rd.map_bytes(), fence, filter, gets))
    })?;
    let (cpu_r, rows_read) = rt.block_on(async {
        let c0 = thread_cpu();
        let mut s = RunStream::open(store.clone(), path.clone(), meta.size, 8 << 20).await?;
        let mut n = 0u64;
        while s.next().await?.is_some() {
            n += 1;
        }
        anyhow::Ok((thread_cpu() - c0, n))
    })?;
    anyhow::ensure!(rows_read == meta.entries, "streamed {rows_read} of {} rows", meta.entries);
    let mb = |b: usize| b as f64 / 1e6;
    println!("RUNFMT bench: cpu {cpu_model}, {} logical CPUs, one thread", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));
    println!(
        "RUNFMT bench: {} rows ({} deleted), {customers} customers, {:.3} GiB raw rows, stored {:.3} GiB (lz4 ratio {:.2}), {} blocks of {block_kib} KiB, {} parts, {} PUT-class requests",
        meta.entries,
        meta.deleted,
        raw_gib,
        stored_gib,
        meta.raw_bytes as f64 / meta.size as f64,
        meta.blocks,
        meta.parts,
        meta.puts
    );
    println!(
        "RUNFMT bench: write CPU {cpu_w:.2} s ({:.2} s per raw GiB, {:.2} s per stored GiB), wall {wall_w:.2} s",
        cpu_w / raw_gib,
        cpu_w / stored_gib
    );
    println!(
        "RUNFMT bench: maps in RAM {} bytes (fence {} bytes, customer filter {} bytes): {:.4} MB per stored GiB, {:.4} MB per raw GiB; opening took {open_gets} GET(s)",
        map_bytes,
        fence_bytes,
        filter_bytes,
        mb(map_bytes) / stored_gib,
        mb(map_bytes) / raw_gib
    );
    println!("RUNFMT bench: streaming read-back CPU {cpu_r:.2} s ({:.2} s per raw GiB), {rows_read} rows, every block checked", cpu_r / raw_gib);
    Ok(())
}
