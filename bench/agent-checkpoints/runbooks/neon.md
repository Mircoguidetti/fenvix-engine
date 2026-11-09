# Neon (serverless Postgres)

Read `README.md` first.

## What the founder creates (console.neon.tech)

1. Plan: the paid plan that allows a fixed 2 CU compute (the Free plan caps compute size and suspends on idle; to be
   checked on neon.com/pricing, not opened on 2026-11-09). 1 CU is 1 vCPU / 4 GB as far as we know (to verify), so
   **2 CU fixed (min = max = 2, autoscaling off, scale to zero off)** matches our 2 vCPU 8 GB.
2. Project name `app-demo`, Postgres 16, database `app_demo`, role `app_user`.
3. Region: Neon's regions are its own list; we believe there is **no Ireland (eu-west-1)**: choose AWS Europe
   (Frankfurt, eu-central-1) or London (eu-west-2) if offered (UNKNOWN until checked in the console). The client must then
   run in the same AWS region (a second small EC2 there), or the latencies include about 10-25 ms Ireland-Frankfurt
   round trips (ESTIMATED, not measured); the report states which.
4. Connection string: the **direct** (non-pooled) endpoint, `sslmode=require`, into `BENCH_PG_URI_NEON`.
   (bench.py sets prepare_threshold=0, so the pooled endpoint also works; use direct for the comparison.)

## Spend estimate (ESTIMATED, 3 hours)

- Compute: 2 CU x 3 h = 6 CU-hours x UNKNOWN $/CU-hour (neon.com/pricing not opened), plus the plan's monthly minimum
  (UNKNOWN), prorated or not depending on the plan (to check).
- Storage: about 1.8 GB x $0.35/GB-month (DECLARED, neon.com/pricing, updated 2026-11-04) x 3/730 = under $0.01;
  history (PITR) $0.20/GB-month (DECLARED) on the written bytes: about 4.7 GB x 0.20 x 3/730 = under $0.01. Set history
  retention to the minimum the plan allows.
- Cap: $10 including the plan minimum, to confirm against the page.

## Teardown and check

1. Project settings > Delete project.
2. Check: the project list is empty; Billing shows no running compute the next day. Downgrade the plan if it was raised
   for the round.
