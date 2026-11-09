# Fenvix (our server on the step9 kitchen)

Read `README.md` first.

## What runs

- `mongo-server` (the commit of the round, `cargo build --release -p mongo-server`) on the step9 kitchen machine in
  eu-west-1: 2 vCPU 8 GB, the same size as the M30 nodes, started the way the step9 runbook starts it
  (`--store s3 --bucket <step9 bucket> --region eu-west-1`, instance role credentials, no keys).
- The benchmark database is `app_demo` (S3 prefix `tenants/app_demo/`), so it never mixes with other step9 data.
- The client EC2 is the shared one; `BENCH_MONGO_URI_FENVIX` holds the kitchen's private address, `BENCH_S3_BUCKET` the
  bucket (the client's instance role needs `s3:ListBucket` on the `tenants/app_demo/` prefix only).

## What the founder creates

Nothing new if the step9 kitchen exists: approve the run window. Otherwise the step9 runbook creates it.

## Commands (on the client)

    BENCH_S3_BUCKET=... BENCH_S3_REGION=eu-west-1 python bench.py --target fenvix --uri "$BENCH_MONGO_URI_FENVIX" \
      --label "Fenvix <commit> step9 kitchen eu-west-1" --profile standard --out results/aws-<date>/fenvix
    # after the tenant's idle close (default 60 s), list the bucket again: the steady-state stored bytes
    sleep 90; python -c "import json,targets; print(json.dumps(targets.s3_prefix_bytes({'bucket':'$BENCH_S3_BUCKET','region':'eu-west-1'},'tenants/app_demo/')))" \
      > results/aws-<date>/fenvix/s3_after_idle_close.json

## Spend estimate (ESTIMATED, 3 hours)

- Machine: the step9 kitchen's own hourly price x 3 h (UNKNOWN here; in the step9 budget).
- S3 PUT: 1.8 PUT per checkpoint (MEASURED locally on MinIO, 4,600 PUTs / 2,551 checkpoints; on S3 the rate depends on
  the flush interval) x 96,754 = 175,000 PUTs x $0.005/1000 (DECLARED) = $0.87.
- S3 GET: under 1 M GETs x $0.0004/1000 = under $0.40.
- S3 storage: about 5 GB (stored/logical 1.02 after idle close, MEASURED locally) x $0.023 x 3/730 = under $0.01.
- Cap: $3 plus the machine.

## Teardown and check

1. Drop the database: `db.getSiblingDB("app_demo").dropDatabase()`, then confirm `tenants/app_demo/` is empty after GC
   (list the prefix; if objects remain after 1 hour, delete the prefix with `aws s3 rm --recursive` and note it in the
   report: it is a finding about erasure).
2. Stop the kitchen per the step9 runbook if no other run follows.
