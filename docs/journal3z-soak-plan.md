# journal3z: plan for the next phase — 72-hour soak test on AWS (NOT executed, NOT authorized)

Nothing in this document has been run. No AWS action, no spend. The founder authorizes any AWS spend separately; this
plan asks for **one authorization with a hard cap of 25 USD and an automatic stop at 20 USD** (§6).

## 1. Goal and pass criteria (fixed before the run)

Run the 3-zone journal for **72 hours** on 3 machines in 3 AZs (eu-west-1) under continuous random faults, with a
zero-loss checker that is outside every fault domain.

Pass, all of:
1. **0 acknowledged writes lost** at every checkpoint (hourly) and at the end.
2. 0 violations of the on-disk invariants checked at each checkpoint: log matching across nodes (same index and epoch
   → same bytes, CRC-checked), no node trimmed above the S3 marker, every node's fence record ≥ every epoch that node
   acknowledged.
3. Every harness error triaged to a cause (the walbench lesson: an unexplained "connection refused" is a finding, not
   noise). Unexplained harness errors = fail.
4. Liveness: after each fault ends, every database acknowledges writes again within (lease TTL + 5 s).
Reported, not judged: p50/p99 ack latency per hour and per fault type; takeover times; catch-up times; CPU, memory,
disk; inter-AZ bytes; spend.

## 2. What must exist before the soak (next-phase build, local, no AWS)

The soak tests an implementation, not the simulator. Before any AWS spend:
- `journal3z-node`: journal node daemon around `journal3z/core::NodeCore` — segment files with CRC frames (design §6.3),
  fence checkpoint, restart scan, one TCP connection per writer, batching, exit on fsync error.
- `journal3z-writer` test client: `WriterDb` per database, lease in S3 (conditional PUT, as round3 `lease.rs`), the
  object-store side modelled by an S3 object per database (marker + content digest), uploads every 30 s, trim.
- Local acceptance: the same binaries pass a local 3-process soak (1 hour, faults by the same injector, loopback with
  `tc netem` delay) and the simulator's scenarios replayed against them where possible.

## 3. Infrastructure

| item | choice | why |
|---|---|---|
| region / AZs | eu-west-1, AZ ids euw1-az1, -az2, -az3 | as walbench |
| 3 test machines | one per AZ; each runs a journal node **and** a writer process (10 databases each, 30 in total) | the production layout (writer co-located with one node) |
| instance type | option A: c6i.large (as walbench, 0.0912 USD/h fetched 2026-10-30); option B (recommended for cost): a smaller non-burstable Graviton type, price to be fetched at run time | correctness soak, not a performance benchmark |
| disk | gp3 20 GB root per machine (journal on root, as walbench quorum) | |
| orchestrator + checker | t4g.nano in az1, **not** subject to faults; runs the fault injector and the checker; holds no journal | outside every fault domain |
| S3 | one bucket `fenvix-journal3z-soak-<run>`: lease objects, database objects, ack ledger | |
| load | 30 databases, open loop, 1 KB, 50 writes/s in total (≈13 M writes in 72 h), one database at 20 writes/s burst for 5 min every hour | enough for many takeovers without inter-AZ cost |

Ack ledger (the zero-loss oracle): each writer appends every acknowledged (database, write id, index) to a local file
and ships it to S3 every 10 s; the checker reads the ledgers. A writer that dies loses at most its last 10 s of ledger
lines: those writes are then simply *not counted* as acknowledged (conservative: never a false "lost"). The orchestrator
also issues its own writes through the writers' client port and logs their acknowledgements directly (not via the
writers' ledgers), so a ledger bug cannot hide a loss.

Checkpoint (hourly, 2 minutes): pause the injector, heal everything, stop client writes, wait for commit, run recovery
for every database from **each pair** of nodes (not only from the writer), compare with S3 + ledgers, check on-disk
invariants, resume.

## 4. Faults (random, one every 2–10 minutes, at most one "single failure" active at a time, plus a 4-hour window of double failures that is reported separately and expected to show loss only for two lost disks)

| fault | how | expected |
|---|---|---|
| kill -9 of a node / of a writer | SSM `kill -9` | no loss; takeover after lease TTL |
| freeze | `kill -STOP`, 5–40 s, then `-CONT` | stale writer fenced |
| stop/start instance | `ec2:StopInstances` / `StartInstances` (disk kept) | node returns stale, caught up |
| zone cut | replace the instance's security group with a deny-all SG for 30 s–5 min (also cuts S3 via the VPC endpoint policy, or via an egress rule) | majority side continues; partitioned writer gives up |
| asymmetric cut | remove one direction only (ingress rule on one SG) | as above |
| network delay / loss | `tc netem` 5–50 ms, 1–10 % loss on one machine | latency only |
| disk pressure | `fallocate` to 99 % of the root disk, 2–10 min | that node stops acking; others continue |
| fsync error | not injectable on EBS without a fault-injection device; use `dm-flakey` on a loop device for the journal of one node during one 6-hour window | node exits, restarts, is caught up |
| clock skew | stop chrony, shift the clock ±0.5–5 s (never > 5 min: S3 request signing) | possible early takeover; fence |
| disk loss | terminate the node's journal directory (`rm -rf` + new node id) and bring a replacement node up | membership change at the next upload; no loss |

## 5. Permissions (least privilege, as walbench: `docs/walbench-permissions.md`)

- Orchestrator role (GitHub Actions OIDC for setup/teardown; instance profile for the run): `ec2:RunInstances`,
  `TerminateInstances`, `StopInstances`, `StartInstances`, `Describe*`, `CreateSecurityGroup`,
  `AuthorizeSecurityGroupIngress/Egress`, `RevokeSecurityGroupIngress/Egress`, `ModifyInstanceAttribute` (SG swap),
  `DeleteSecurityGroup`, `CreateTags` — all conditioned on tag `project=fenvix-journal3z-soak`; `ssm:SendCommand`,
  `ssm:GetCommandInvocation`; `s3:*` on the soak bucket only; `pricing:GetProducts`; `tag:GetResources`;
  `iam:PassRole` for the instance profile only.
- Test-machine instance profile: `s3:GetObject/PutObject/DeleteObject/ListBucket` on the soak bucket only; SSM agent
  policy.
- No secrets in the repository; credentials only through OIDC and instance profiles.

## 6. Cost estimate (estimated, not measured; prices from the walbench fetch of 2026-10-30 unless noted)

| item | quantity | option A (c6i.large) | option B (smaller type) |
|---|---|---|---|
| 3 test instances | 216 instance-hours | 19.70 USD (0.0912/h) | ≈ 8–11 USD (≈0.04–0.05/h, **price not fetched**) |
| orchestrator t4g.nano | 72 h | ≈ 0.30 USD (price not fetched) | same |
| gp3 root volumes | 4 × 20 GB × 72 h | 0.70 USD (0.088/GB-month) | same |
| public IPv4 | 4 × 72 h | 1.44 USD (0.005/h) | same (or 0 with a NAT-less private design + VPC endpoints; not planned) |
| inter-AZ transfer | 13 M writes × 2 remote copies × ≈1.1 KB, both directions | ≈ 0.55 USD (0.01/GB each way) | same |
| S3 requests | lease renewals (per-process lease mode: 3 objects every ≈3 s), uploads (30 databases every 30 s), ack ledgers, checkpoints | ≈ 1.0–1.5 USD (0.005 per 1,000 PUT) | same |
| total | | **≈ 24 USD** | **≈ 12–15 USD** |

Recommendation: option B, cap 25 USD, automatic stop at 20 USD (live spend estimate every 5 minutes from counted
requests and instance-hours at fetched prices, as walbench did). AWS billing lags about a day: the cap is enforced on
the estimate.

## 7. Teardown plan

1. Stop the injector and the load; final checkpoint; upload all logs and results to the bucket, then to the repository
   (`results-journal3z-soak/`), as walbench did.
2. Terminate instances; delete security groups (after the ENIs are gone); delete the instance profile and role; empty
   and delete the bucket.
3. Verify: describe instances, volumes, ENIs, SGs, buckets by tag **and** by the run's resource ids, plus the Resource
   Groups Tagging API. **Repeat the verification after 10 minutes**: the walbench teardown reported ENIs and volumes
   that were still being deleted at the moment of the first check (`results-walbench.md`, "RESOURCES LEFT"). A
   resource still present after the second check is deleted explicitly and reported.
4. The teardown also runs automatically on: spend cap, 80 hours of wall time, orchestrator failure (a scheduled
   GitHub Actions job checks every hour and tears down if the orchestrator's heartbeat object in S3 is older than
   30 minutes).

## 8. Duration and schedule (estimated)

Build of §2: 2–3 weeks. Local 3-process soak: 1 day. AWS soak: 72 h + 1 h setup + 1 h teardown. Report: 1 day.
