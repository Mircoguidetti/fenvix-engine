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
- Crash test: 200 cycles per backend, run BEFORE the latency sweeps (so an automatic stop cannot cut the loss
  evidence). Each cycle a writer process (random variant, concurrency, size) issues at most 1000 writes and is killed
  with SIGKILL after a random 150–1500 ms or a random number of acks (whichever comes first, so the kill lands
  mid-run even on fast backends); a new process reads the log, every acknowledged id must be there with correct
  bytes. Writers die with their harness (PR_SET_PDEATHSIG) and exit by themselves after 30 s.
  A negative control (an "ack before write" mode) was run locally and the check reported the loss.
- Quorum nodes: one per AZ. If the vCPU quota allows only 3 instances, node 0 (client's AZ) runs as a process on the
  client instance; this is recorded in the results.
- Quorum: node 1 killed, returns stale (negative control: node 1 alone misses acked writes), caught up, then node 0
  killed; recovery from nodes 1+2 only must have every acked write. Latency per phase.
- EBS: force-detach the volume while 8 writers write (machine-loss case), attach to the second instance, mount,
  recover, first ack; 6 cycles alternating instances; acked ⊆ recovered checked every cycle.

## Pass criteria (fixed now, applied mechanically by `tools/walbench/report.py`)

1. **Loss**: zero acknowledged writes lost in every crash cycle of the backend (plus the quorum scenario for 5 and
   the reattach cycles for 4).
2. **Latency**: p99 ack < 15 ms. A closed-loop configuration (backend × variant) must meet it in all 6 cells; a
   fleet configuration in its cell.
3. **Cost**: variable cost < 1.5 USD per million acknowledged writes at concurrency 8 and 64 (closed loop, both
   sizes) and in the shared-batch case (200 databases, `shared w1/w5/w20`). The cost at concurrency 1 is reported
   but does not fail a configuration; the 200-database `perdb perwrite` cell is shown as a reference and is not a
   candidate configuration. (Rule set on 2026-10-30, before any AWS run.) EBS and the quorum have
   no per-request charge: their variable cost is ~0 (quorum: inter-AZ transfer); their fixed cost per hour and the
   write rate at which it equals 1.5 USD/M are reported next to it.

A pass needs complete evidence: all planned crash cycles run, no harness failure, acked writes > 0, no corrupt batch
or bad payload; closed-loop cells with no write error and all target writes acked; the quorum scenario run with its
negative control confirmed; all EBS reattach cycles run with the previous session's ack list present. Anything
missing is NOT RUN / fail, never a pass.

A configuration passes when it meets all three; a backend passes when one of its configurations passes. The report
lists every configuration; it does not rank beyond this.

## Spend control and visibility

Estimate of the whole plan before anything is created (stop if > 3.5 USD); live spend every 20 s in
`results-walbench/cost_live.csv`, one `SPEND |` line in the Actions log every 60 s (elapsed time, running step,
USD, % of the 4 USD stop), and `cost_live.csv` pushed to branch `walbench` every 5 minutes (only that file; never a
trigger path); the final spend is printed at the end and in the job summary. At 4 USD the run stops: instances are
terminated first, then results are collected and everything is deleted; cap 5 USD. A stop marker also ends the
step running on the client within ~10 s. Teardown and a
two-way verification (per-service listings + tag API) always run; instances also terminate themselves after
200 minutes.
