# AWS round of the agent checkpoint benchmark: common rules

One file per target: `aurora-postgresql.md`, `neon.md`, `supabase.md`, `atlas.md`, `fenvix.md`. This page holds what they share.

## Rules

- Nothing here runs by itself. The founder creates each competitor resource by hand in its console, puts the connection
  string in a GitHub secret, approves the spend, and deletes the resource after the run.
- Neutral names on every competitor service: project, cluster, database and user names such as `app_demo`, `app-demo`,
  `app_user`. Never "fenvix", "test", "bench" or "competitor".
- Credentials only in GitHub secrets (repository fenvix-engine, environment `bench-aws`), never in a file of any repo.
- Same workload everywhere: `generator.py --profile standard --seed 42` (2,453 threads, 25,398 turns, 96,754 checkpoints,
  about 4.4 GB of full-copy message payload: ESTIMATED by `generator.py --describe`, serializer overhead not included).
- Same client everywhere: one EC2 instance in eu-west-1 running `bench.py` with one worker, then `--workers 8`. A target
  outside eu-west-1 is noted in its runbook, and its latencies are reported with the measured client-to-database
  round trip next to them (`ping`-style: 20 `hello`/`SELECT 1` round trips before the run, MEASURED).
- Run length budget: 3 hours of provisioned time per target (create, load ~30-60 min, reads, erase, sizes, delete).
  Spend estimates below use 3 hours; they exclude the client EC2, which is shared.

## Secrets (names)

    BENCH_PG_URI_AURORA_STD      postgresql://app_user:...@<aurora standard writer endpoint>:5432/app_demo?sslmode=require
    BENCH_PG_URI_AURORA_IOOPT    the same for the I/O-Optimized cluster
    BENCH_PG_URI_NEON            postgresql://app_user:...@<neon endpoint>/app_demo?sslmode=require  (direct, not pooled)
    BENCH_PG_URI_SUPABASE        postgresql://postgres.<ref>:...@<host>:5432/postgres?sslmode=require  (session mode)
    BENCH_MONGO_URI_ATLAS_M30    mongodb+srv://app_user:...@<cluster>/?retryWrites=true&w=majority
    BENCH_MONGO_URI_ATLAS_INF    the same for Infinite M30
    BENCH_MONGO_URI_FENVIX       mongodb://<step9 kitchen address>:27017/?directConnection=true
    BENCH_S3_BUCKET              the step9 kitchen bucket (the client EC2 reads its size with its instance role; no keys)

## Command on the client (per target)

    python bench.py --target postgres --uri "$BENCH_PG_URI_AURORA_STD" --label "Aurora Standard db.r7g.large eu-west-1" \
      --profile standard --out results/aws-<date>/aurora-standard
    python cost.py results/aws-<date>/aurora-standard/summary.json --option aurora_standard

## Before the round

Fill every `UNKNOWN` price in `prices.json` from the vendor page (the 2026-11-09 session could not open them) and keep the URL.
