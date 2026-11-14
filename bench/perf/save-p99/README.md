# save-p99: where the tail of an acknowledged write comes from (2026-11-14)

Engine `night-head` 36c6a3e; branch `save-p99`. Three `pq replica` copies of the log on this machine (as ctl-tests
test_91: one dir per copy, all on the same root disk, `--mem-cap-mb 256` unless said), MinIO on the same disk, engine
with `--quaderno` and `FX_WAITS=0` (every command prints its stage breakdown). `run.py` = paced insert_one (open loop,
4 connections) + background insert_many into another database, 600 s; then every acknowledged `_id` is read back.
`report.py` = per-stage p50/p99/max of the paced inserts (first 10 s dropped), owner of the slowest 1%, the log
leader's own timers. 4 vCPU cloud VM, one virtio disk (ext4, `discard`).

Loads: **std** = 100 insert_one/s + 400 docs/s (20 x 1 KB every 50 ms); **heavy** = 200 insert_one/s + 2000 docs/s.

## Stages (std, baseline, traced binary)

| stage (ms) | p50 | p99 | max |
|---|---|---|---|
| client insert_one (service) | 1.7 | 4.8 | 150.2 |
| server total | 1.4 | 4.6 | 149.6 |
| admit (fair queue) | 0.0 | 0.0 | 18.9 |
| lock_w (db exclusive lock) | 0.0 | 0.0 | 12.0 |
| stage (memtable insert, SlateDB backpressure) | 0.0 | 0.1 | 10.5 |
| **log_ack (append + 2-of-3 fdatasync)** | **1.0** | **3.6** | **149.2** |
|   of which: queue in our log client | 0.0 | 0.1 | 2.1 |
|   of which: log round trip | 1.0 | 3.5 | 149.1 |
| visible (earlier batches confirmed) | 0.0 | 0.0 | 0.3 |
| read_get (reads inside the insert) | 0.1 | 0.8 | 30.4 |
| rest (parse + execute CPU) | 0.2 | 0.7 | 13.8 |
| write_reply | 0.0 | 0.1 | 1.6 |

Lease/fence check (`owner_valid`) is a synchronous lookup, never visible. Slowest 1% (716 writes): log_ack owns 581,
mean 11.0 of which 10.9 is the log round trip; the log leader's append->fdatasync max 148.9 ms = the run's max.

## Suspects, one at a time (600 s each, 0 lost in every valid run)

| run | client p99 | max | writes >= 50 ms | writes >= 15 ms |
|---|---|---|---|---|
| std base (two runs) | 5.0 / 4.8 | 155 / 150 | - / 22 | - / 132 |
| std, log copies on tmpfs (diagnostic only) | 3.1 | 68 | 2 | 12 |
| std, MinIO on tmpfs | 4.6 | 160 | 21 | 115 |
| std, `PQ_RICICLA=1` (segment recycle) | 5.0 | 161 | 28 | 164 |
| std, `PQ_PREALLOC_MBS=16` (paced spare prealloc) | 4.7 | 94 | 5 | 121 |
| std, MinIO tmpfs + paced 16 | 4.9 | 93 | 6 | 126 |
| **std, fix (paced 32 by default)** | **4.6** | **81** | **7** | 130 |
| heavy, cap 256 MB, old | 9.3 | 8457 | 248 | 744 |
| heavy, cap 256 MB, fix | 9.0 | 8498 | 160 | 655 |
| heavy, cap 1 GB, old (two runs) | 11.9 / 11.7 | 208 / 178 | 142 / 142 | 979 / 941 |
| **heavy, cap 1 GB, fix (two runs)** | **11.0 / 10.2** | **80 / 88** | **15 / 8** | 760 / 684 |

Findings:
1. **The save tail is the log's fdatasync on the disk the three copies share** (tmpfs copies: >=15 ms writes 132 -> 12,
   leader fdatasync max 154 -> 30 ms). Our side adds nothing: queue in the log client p99 0.1 ms, batches of 1-2
   records, no lock is held across the ack (lock_w / visible ~0), memtable insert p99 0.1 ms, no runtime stall in the tail.
2. **The max (120-210 ms) is the spare-segment preallocation**: with `PQ_PREALLOC_MBS` unset (default 0 on night-head)
   each copy writes the next 64 MB segment of zeros at once with one sync at the end, on the same disk, at every segment
   switch (every ~123 s std, ~27 s heavy). Paced (1 MB writes + sync, 32 MB/s) it no longer stalls the log.
   Recycling (`PQ_RICICLA=1`) does not help in a 10-minute run (no dropped segment is free in time; it falls back to zeros).
3. **With test_91's `--mem-cap-mb 256` a heavy load hits the log's memory brake**: Busy replies, client retries, 208
   records fall back to the S3 WAL flush, writes of 1-8.5 s. Records stay in the log until the engine's upload
   (every `--local-log-upload-secs`, default 30 s) confirms them. A harness setting (production default = 60% of RAM),
   but any bench that copies test_91's cap will show seconds-long saves under load.
4. What remains of p99 at heavy load (10-11 ms) is ordinary fdatasync latency with three copies + MinIO + the engine on
   one disk (leader commit p99 7.4-8.3 ms). In the 3-zone product each copy has its own disk and the commit takes the
   2nd fastest of 3 (`node.rs maybe_commit`): one slow disk is masked. Not measurable on this one-disk box.

## Fix (branch `save-p99`)

`proto/quaderno/src/store.rs`: the spare-segment preallocation is paced by default (32 MB/s; `PQ_PREALLOC_MBS=0`
restores the one-shot fill). Durability unchanged: it only touches the spare file, never a record; an ack still means
2 of 3 on disk. Gain: max 150 -> 81 ms (std), 178-208 -> 80-88 ms (heavy), writes >= 50 ms -90% (heavy 142 -> 8-15),
p99 -0.2 ms (std) / -0.9 to -1.5 ms (heavy).

Temporary tracing (only with `FX_WAITS`): `sv_lq` / `sv_rtt` / `sv_bn` in the WAITS line (`src/waits.rs`,
`mongo-server/src/qlog.rs`). Raw runs: scratchpad of the session (server.log of each run is large).
