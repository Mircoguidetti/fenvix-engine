# save-p99: where the tail of an acknowledged write comes from (2026-11-14)

Engine `night-head` 36c6a3e; branch `save-p99`. Three `pq replica` copies of the log on this machine (as ctl-tests
test_91: one dir per copy, all on the same root disk, `--mem-cap-mb 256` unless said), MinIO on the same disk, engine
with `--quaderno` and `FX_WAITS=0` (every command prints its stage breakdown). `run.py` = paced insert_one (open loop,
4 connections) + background insert_many into another database, 600 s; then every acknowledged `_id` is read back.
`report.py` = per-stage p50/p99/max of the paced inserts (first 10 s dropped), owner of the slowest 1%, the log
leader's own timers. 4 vCPU cloud VM, one virtio disk (ext4, `discard`).

Loads: **std** = 100 insert_one/s + 400 docs/s (20 x 1 KB every 50 ms); **heavy** = 200 insert_one/s + 2000 docs/s.

## Stages (clean runs: background writer in its own collection; 600 s; 0 lost)

`c-std-old` = std load, night-head behaviour (`PQ_PREALLOC_MBS=0`); `c-heavy-old` = heavy load, log cap 1 GB.

| stage (ms), p50/p99/max | std | heavy |
|---|---|---|
| client insert_one (service) | 1.5 / 4.2 / 145.7 | 1.6 / 11.0 / 152.2 |
| server total | 1.2 / 3.6 / 145.1 | 1.2 / 9.4 / 151.5 |
| admit (fair queue) | 0.0 / 0.0 / 11.1 | 0.0 / 0.0 / 1.9 |
| lock_w (db exclusive lock) | 0.0 / 0.0 / 0.4 | 0.0 / 0.0 / 3.0 |
| stage (memtable insert, SlateDB backpressure) | 0.0 / 0.1 / 2.1 | 0.0 / 0.1 / 3.5 |
| **log_ack (append + 2-of-3 fdatasync)** | **0.9 / 3.2 / 144.7** | **0.9 / 9.0 / 151.1** |
| &nbsp;&nbsp;queue in our log client | 0.0 / 0.1 / 2.5 | 0.0 / 0.1 / 12.6 |
| &nbsp;&nbsp;log round trip | 0.9 / 3.1 / 144.8 | 0.9 / 8.9 / 151.0 |
| visible (earlier batches confirmed) | 0.0 / 0.0 / 1.1 | 0.0 / 0.0 / 2.8 |
| read_get + get (reads inside the insert) | 0.1 / 0.2 / 10.2 | 0.1 / 0.2 / 18.9 |
| rest (parse + execute CPU) | 0.2 / 0.3 / 7.2 | 0.2 / 0.4 / 25.3 |
| write_reply | 0.0 / 0.1 / 2.1 | 0.0 / 0.1 / 5.2 |
| log leader append->commit (us, p50/p99/p999/max) | 711 / 2795 / 18287 / 145407 | 753 / 8111 / 43935 / 150271 |

Owner of the slowest 1% (largest stage per write): log_ack 557 of 597 (std), 1183 of 1194 (heavy). Batches of 1 record
(max 4). The lease/fence check (`owner_valid`) is a synchronous lookup, never visible. The client is above the server at
heavy load (driver + 5 Python threads on 4 vCPU).

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
| clean std, old / **fix** | 4.2 / **4.3** | 146 / **46** | 17 / **0** | 96 / 113 |
| heavy, cap 256 MB, old | 9.3 | 8457 | 248 | 744 |
| heavy, cap 256 MB, fix | 9.0 | 8498 | 160 | 655 |
| heavy, cap 1 GB, old (two runs) | 11.9 / 11.7 | 208 / 178 | 142 / 142 | 979 / 941 |
| **heavy, cap 1 GB, fix (two runs)** | **11.0 / 10.2** | **80 / 88** | **15 / 8** | 760 / 684 |
| clean heavy 1 GB, old / **fix** | 11.0 / **11.1** | 152 / **62** | 101 / **6** | 622 / 521 |
| heavy 1 GB, fix, MinIO on tmpfs | 8.2 | 67 | 8 | 539 |

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
   one disk (leader commit p99 7.4-8.3 ms); the fix does not move it. The local MinIO on the same disk is part of it:
   with MinIO on tmpfs the heavy p99 is 8.2 ms (leader commit p99 4.2 ms), a local-bench artifact (S3 is remote). In the 3-zone product each copy has its own disk and the commit takes the
   2nd fastest of 3 (`node.rs maybe_commit`): one slow disk is masked. Not measurable on this one-disk box.

## Fix (branch `save-p99`)

`proto/quaderno/src/store.rs`: the spare-segment preallocation is paced by default (32 MB/s; `PQ_PREALLOC_MBS=0`
restores the one-shot fill). Durability unchanged: it only touches the spare file, never a record; an ack still means
2 of 3 on disk. Gain (clean pairs): max 146 -> 46 ms (std), 152 -> 62 ms (heavy); writes >= 50 ms 17 -> 0 (std), 101 -> 6 (heavy);
mean of the slowest 1% 10.0 -> 8.8 ms (std), 25.3 -> 16.0 ms (heavy). p99 unchanged within noise (4.2 / 4.3, 11.0 / 11.1):
the fix removes the spikes, not the ordinary fdatasync latency of a shared disk.

Not done: separate disks per copy (one disk on this box), and a fix for the memory brake (finding 3: e.g. the engine
confirming uploads sooner when the log is near its cap) - both need their own measurement.

Temporary tracing (only with `FX_WAITS`): `sv_lq` / `sv_rtt` / `sv_bn` in the WAITS line (`src/waits.rs`,
`mongo-server/src/qlog.rs`). Raw runs: scratchpad of the session (server.log of each run is large).
