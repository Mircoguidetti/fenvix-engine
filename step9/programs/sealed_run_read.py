"""Step 9 · program "sealed-run": the one-trip read of a sealed run, on real S3, from the client machine.
What it measures: a sealed run (compaction 2, run format v1, src/runfmt.rs on the compaction branches) is one S3 object
of about 1 GB made of 256 KiB blocks, with its fence (the first key of every block) kept in RAM by the server. A point
read of one customer's key is then ONE ranged GET of one block: the fence says which block, the block holds the row.
This program builds an object with the same PHYSICAL layout as format v1 (blocks of BLOCK_KIB, footer with the fence
at the end, a 40-byte trailer; open = one GET of the last 256 KiB, as runfmt.rs OPEN_TAIL), then measures:
  open     one GET of the tail (trailer + footer + fence): what a server pays once per run when it first meets it;
  one-trip a random key: fence lookup in RAM, ONE ranged GET of its block, the row found and checked (CRC32 of block);
  two-trip the same without the fence in RAM (GET of the tail, then GET of the block): what a server pays when the
           fence was evicted, for comparison.
It does NOT run the engine's reader (src/runfmt.rs is not in `integration` yet; compaction 2 step 3). The bytes on the
wire and the number of trips are the same as the format's, so the S3 latency measured here is the latency of the real
read; CPU of lz4 and of the engine is not included. HOOK for step 3: when `runfmt` is in the engine ref of the round,
add a binary that writes a run with runfmt::RunWriter to this bucket and reads keys with the random reader, and give its
path with --engine-reader; this program then runs it and stores its output next to its own numbers.
Raw lines in OUT/sealed-run.jsonl, summary in OUT/sealed-run-summary.json.
Cost (ESTIMATED, DECLARED S3 prices): one 1 GiB object (multipart, about 70 PUT-class requests), READS x 3 GETs, about
$0.001; storage 1 GiB for an hour, about $0.00003.
Usage: sealed_run_read.py BUCKET OUT [--run-mib 1024] [--block-kib 256] [--reads 300] [--seed 11] [--engine-reader CMD]"""
import argparse
import bisect
import json
import os
import random
import struct
import subprocess
import time
import zlib

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("bucket")
ap.add_argument("out")
ap.add_argument("--prefix", default="step9-sealed/")
ap.add_argument("--run-mib", type=int, default=1024)
ap.add_argument("--block-kib", type=int, default=256)
ap.add_argument("--reads", type=int, default=300)
ap.add_argument("--seed", type=int, default=11)
ap.add_argument("--engine-reader", default="", help="step 3 hook: a command that writes and reads a real runfmt v1 run")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
rng = random.Random(a.seed)
BLOCK = a.block_kib * 1024
OPEN_TAIL = 256 * 1024
TRAILER = 40
ROW = 470                      # bytes of one chat message (runfmt_bench.rs)
key_of = lambda c, m: f"u/{c:08d}/messages/d/{m:012d}".encode()

# 1. build the run: blocks of rows in key order; each block = rows, padded, CRC32 at the end
path = os.path.join(a.out, ".run.bin")
fence, offsets, keys_sample = [], [], []
n_blocks = (a.run_mib << 20) // BLOCK
cust, msg = 0, 0
with open(path, "wb") as f:
    for b in range(n_blocks):
        rows, size, first = [], 4, None
        while size + 2 + 34 + 8 + ROW <= BLOCK - 4:
            k = key_of(cust, msg)
            first = first or k
            rows.append(struct.pack("<H", len(k)) + k + struct.pack("<Q", msg) + rng.randbytes(ROW))
            size += 2 + len(k) + 8 + ROW
            if rng.random() < 0.3 and len(keys_sample) < 200000:
                keys_sample.append((b, k))
            msg += 1
            if rng.random() < 0.002:
                cust += 1
        body = struct.pack("<I", len(rows)) + b"".join(rows)
        body += b"\0" * (BLOCK - 4 - len(body))
        offsets.append(f.tell())
        f.write(body + struct.pack("<I", zlib.crc32(body)))
        fence.append(first)
    footer_off = f.tell()
    footer = struct.pack("<II", 1, BLOCK) + struct.pack("<I", len(fence)) + b"".join(struct.pack("<H", len(k)) + k for k in fence) \
        + b"".join(struct.pack("<Q", o) for o in offsets + [footer_off])
    f.write(footer)
    f.write(struct.pack("<QI", footer_off, len(footer)) + b"\0" * 20 + struct.pack("<I", zlib.crc32(footer)) + b"FXR1")
size = os.path.getsize(path)
key = f"{a.prefix}run-{a.run_mib}m.fxr"
t0 = time.perf_counter()
s3.upload_file(path, a.bucket, key)
upload_s = time.perf_counter() - t0
os.remove(path)
print(f"run written: {size / 2**20:.0f} MiB, {n_blocks} blocks of {a.block_kib} KiB, footer {len(footer) / 1024:.0f} KiB, "
      f"upload {upload_s:.1f} s", flush=True)


def get_range(start, end):
    t0 = time.perf_counter()
    body = s3.get_object(Bucket=a.bucket, Key=key, Range=f"bytes={start}-{end - 1}")["Body"]
    first = body.read(1)
    t1 = time.perf_counter()
    data = first + body.read()
    return data, (t1 - t0) * 1000, (time.perf_counter() - t0) * 1000


def find_in_block(data, k):
    body, crc = data[:-4], struct.unpack("<I", data[-4:])[0]
    if zlib.crc32(body) != crc:
        raise ValueError("block CRC")
    n = struct.unpack_from("<I", body, 0)[0]
    p = 4
    for _ in range(n):
        kl = struct.unpack_from("<H", body, p)[0]
        if body[p + 2:p + 2 + kl] == k:
            return True
        p += 2 + kl + 8 + ROW
    return False


raw = open(os.path.join(a.out, "sealed-run.jsonl"), "a")
res = {"open": [], "one_trip": [], "two_trip": []}
wrong = 0
for i in range(a.reads):
    b, k = keys_sample[rng.randrange(len(keys_sample))]
    # one trip: fence in RAM
    j = bisect.bisect_right(fence, k) - 1
    data, ttfb, total = get_range(offsets[j], offsets[j] + BLOCK)
    ok = find_in_block(data, k) and j == b
    wrong += 0 if ok else 1
    res["one_trip"].append(total)
    raw.write(json.dumps({"t": time.time(), "kind": "one_trip", "block": j, "ttfb_ms": round(ttfb, 3), "total_ms": round(total, 3), "ok": ok}) + "\n")
    # two trips: tail first (open), then the block
    tail, ttfb_o, total_o = get_range(size - OPEN_TAIL, size)
    data, ttfb2, total2 = get_range(offsets[j], offsets[j] + BLOCK)
    res["open"].append(total_o)
    res["two_trip"].append(total_o + total2)
    raw.write(json.dumps({"t": time.time(), "kind": "two_trip", "block": j, "open_ms": round(total_o, 3), "block_ms": round(total2, 3)}) + "\n")


def pct(v, p):
    v = sorted(v)
    return round(v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))], 3)


summary = {"run_mib": a.run_mib, "block_kib": a.block_kib, "blocks": n_blocks, "reads": a.reads, "wrong": wrong,
           "upload_s": round(upload_s, 2)}
for name, v in res.items():
    summary[name] = {"p50": pct(v, 50), "p95": pct(v, 95), "p99": pct(v, 99), "max": round(max(v), 3), "n": len(v)}
if a.engine_reader:
    r = subprocess.run(a.engine_reader, shell=True, capture_output=True, text=True, timeout=3600)
    open(os.path.join(a.out, "sealed-run-engine.txt"), "w").write(r.stdout + r.stderr)
    summary["engine_reader_rc"] = r.returncode
json.dump(summary, open(os.path.join(a.out, "sealed-run-summary.json"), "w"), indent=1)
print(json.dumps(summary))
s3.delete_object(Bucket=a.bucket, Key=key)
print(f"SEALED-RUN: {'DONE' if wrong == 0 else 'WRONG ROWS'} ({wrong} wrong of {a.reads})")
