# walbench: method and pass criteria (fixed before the run)

Question: where to put the durable write log (the "notebook") of the write-ack path. A write is acknowledged only
when the log says it is durable. Code: `tools/walbench/` (Rust benchmark `bench/`, orchestrator, instance agent,
infra script, report). Permissions: `docs/walbench-permissions.md`.

## Backends (what "durable" means for each)

| # | backend | one commit is | durable when |
|---|---|---|---|
| 1 | S3 Standard | one PutObject `<log>/<seq>` | PUT returned 200 |
| 2 | S3 Express One Zone | one PutObject in a directory bucket (single AZ, same AZ as the client) | PUT returned 200 |
| 3 | DynamoDB on-demand | one PutItem (pk = log, sk = seq), table used only as a log | PutItem returned |
| 4 | EBS gp3 (20 GB, 3000 IOPS, 125 MB/s, ext4) | pwrite of a frame into a pre-zeroed file + fdatasync | fdatasync returned |
| 5 | 3 local logs, 3 AZs | the frame sent to 3 nodes, each pwrite + fdatasync | 2 of 3 nodes answered |

Every commit carries a batch with a CRC; each write's bytes are a function of its id, so recovery checks content.
SDK retries are off; the benchmark retries itself (up to 6 attempts) and counts every attempt.

## Variants

- `perwrite`: one commit per write (1–3 only).
- `w1`, `w5`, `w20`: group commit, the batch is cut 1/5/20 ms after its first write, up to 64 commits in flight.
- `w0` (4, 5): classic group commit, one commit in flight, the next takes everything queued meanwhile.
- Acks are released in log order (a write is acked only when it and every earlier write are durable).
- One shared batch across many databases (fleet, 1–3): 200 databases × 1 write/s each (Poisson, open loop, 60 s,
  1 KB): `perdb perwrite` (one log per database) vs `shared w1/w5/w20` (one batch for all databases).

## Measurements

- Client: c6i.large (non-burstable), Ubuntu 24.04, eu-west-1, same AZ as the Express bucket and the EBS volume.
  Quorum nodes: c6i.large, one in the client's AZ and one in each of two other AZs (log on the gp3 root volume).
- Closed loop: 200 B and 1 KB writes × concurrency 1, 8, 64 on one log; 100 warm-up writes, then 1500 measured
  writes (max 90 s); p50/p95/p99/max from every ack.
- Cost per million writes: requests / WCU / bytes counted in each cell × prices fetched at run time from the AWS
  public offer files and Price List API for eu-west-1 (saved in `results-walbench/prices.json`), + 24 h of storage.
- Crash test: 200 cycles per backend; each cycle a writer process (random variant, concurrency, size) is killed with
  SIGKILL after 150–1500 ms, a new process reads the log, every acknowledged id must be there with correct bytes.
  A negative control (an "ack before write" mode) was run locally and the check reported the loss.
- Quorum: node 1 killed, returns stale (negative control: node 1 alone misses acked writes), caught up, then node 0
  killed; recovery from nodes 1+2 only must have every acked write. Latency per phase.
- EBS: force-detach the volume while 8 writers write (machine-loss case), attach to the second instance, mount,
  recover, first ack; 6 cycles alternating instances; acked ⊆ recovered checked every cycle.

## Pass criteria (fixed now, applied mechanically by `tools/walbench/report.py`)

1. **Loss**: zero acknowledged writes lost in every crash cycle of the backend (plus the quorum scenario for 5 and
   the reattach cycles for 4).
2. **Latency**: p99 ack < 15 ms. A closed-loop configuration (backend × variant) must meet it in all 6 cells; a
   fleet configuration in its cell.
3. **Cost**: variable cost < 1.5 USD per million acknowledged writes, in the same cells. EBS and the quorum have
   no per-request charge: their variable cost is ~0 (quorum: inter-AZ transfer); their fixed cost per hour and the
   write rate at which it equals 1.5 USD/M are reported next to it.

A configuration passes when it meets all three; a backend passes when one of its configurations passes. The report
lists every configuration; it does not rank beyond this.

## Spend control

Estimate of the whole plan before anything is created (stop if > 3.5 USD); live spend every 20 s in
`results-walbench/cost_live.csv`; at 4 USD the run stops and everything is deleted; cap 5 USD. Teardown and a
two-way verification (per-service listings + tag API) always run; instances also terminate themselves after
200 minutes.
