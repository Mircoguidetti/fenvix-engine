# Aurora PostgreSQL (Standard and I/O-Optimized)

Read `README.md` first (names, secrets, client, run length).

## What the founder creates (console, eu-west-1 Ireland)

1. RDS > Create database > Aurora (PostgreSQL Compatible), latest 16.x minor. Cluster name `app-demo`, initial database
   `app_demo`, master user `app_user`, password from a password manager.
2. Cluster storage configuration: **Aurora Standard** for the first cluster. A second cluster `app-demo-2` with
   **Aurora I/O-Optimized**. (A cluster can be switched between the two, but AWS limits how often; two clusters avoid
   waiting. To verify on the page before choosing.)
3. Instance: **db.r7g.large** (2 vCPU, 16 GB), one writer, no replica, Single-AZ for the round.
   Why not db.t4g.large (2 vCPU, 8 GB, the same memory as our machines): T classes are burstable; under a sustained load
   they run on CPU credits and then throttle or bill extra credits, so the result would depend on the credit balance,
   not on the database. db.r7g.large is the smallest non-burstable Aurora class; it has twice our memory, which favours
   Aurora (state it in the report). If the founder prefers equal memory, run db.t4g.large as well and report both.
4. Networking: in the step9 VPC (same as the client EC2), not publicly accessible, security group allowing 5432 from
   the client only. Encryption on (default). Backups: 1 day retention. Performance Insights off.
5. Copy the writer endpoint into `BENCH_PG_URI_AURORA_STD` (and `BENCH_PG_URI_AURORA_IOOPT`).

## Spend estimate (ESTIMATED, 3 hours per cluster, standard profile)

- Instance: UNKNOWN $/h (prices.json; aws.amazon.com/rds/aurora/pricing not opened). Lead to verify: a search snippet of
  bytebase.com says db.r7g.large eu-west-1 $222/month = $0.304/h; at that rate 3 h = $0.91 per cluster.
- Storage: about 1.8 GB stored (4.7 GB logical x 0.386 stored/logical MEASURED locally on Postgres 16) x $0.10/GB-month
  (DECLARED) x 3/730 month = under $0.01. I/O-Optimized at $0.225: also under $0.01.
- I/O (Standard only): 96,754 checkpoints x about 6 write I/Os (ceil(21,644 B / 4 KiB), ESTIMATED) = 0.58 M I/Os x
  $0.20/M (DECLARED) = $0.12, plus reads, under $0.10 more.
- Total: about $1-2 per cluster, cap $5 for both.

## Teardown and check

1. Delete each instance, then the cluster; "Create final snapshot": No; "Retain automated backups": No.
2. Check: RDS > Databases shows no `app-demo*`; RDS > Snapshots (manual and system) shows none; the security group is
   deleted; Cost Explorer filtered on RDS the next day shows no running charge.
