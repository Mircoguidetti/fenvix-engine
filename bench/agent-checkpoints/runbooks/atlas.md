# MongoDB Atlas (Dedicated M30 and Infinite M30)

Read `README.md` first. Same tiers as the Part A comparison, so the numbers line up.

## What the founder creates (cloud.mongodb.com)

1. Project `app-demo` (neutral name).
2. Cluster 1: **Dedicated M30**, AWS, **eu-west-1 (Ireland)**, MongoDB 7.0 or 8.0 (say which; our server answers as 7.0),
   3-node replica set (the M30 default), no backups, cluster name `app-demo`.
3. Cluster 2: **Infinite M30 (Gen2)**, AWS Ireland, the same settings as in Part A, cluster name `app-demo-2`.
4. Database user `app_user` (readWrite on `app_demo`), network access: the client EC2's public IP only (or VPC peering if
   Part A used it).
5. SRV connection strings into `BENCH_MONGO_URI_ATLAS_M30` and `BENCH_MONGO_URI_ATLAS_INF`.
   Note: `fsync` is refused on Atlas; bench.py ignores the refusal, so `storageSize` may lag up to 60 s (WiredTiger
   checkpoint); the size is measured after the read phase, at least 60 s after the last write.

## Spend estimate (ESTIMATED, 3 hours each)

- M30: $0.54/h (DECLARED, mongodb.com/pricing) x 3 h = $1.62.
- Infinite M30: $0.677/h (DECLARED) x 3 h = $2.03; storage about 2 GB x $0.30/GB-month (DECLARED, preview) x 3/730 =
  under $0.01.
- Data transfer out of Atlas to the client in the same region: small (reads of about 10 GB at most), price UNKNOWN.
- Cap: $6 for both.

## Teardown and check

1. Terminate both clusters ("Terminate", no final backup), then delete the project.
2. Check: the organization's project list has no `app-demo`; Billing > Pending invoice the next day shows no running cluster.
