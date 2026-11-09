# AWS round of the agent checkpoint benchmark: common rules

One file per target: `aurora-postgresql.md`, `postgres-ec2.md` (the Supabase stand-in), `fenvix.md`. This page holds what they share.

Founder, 2026-11-09: no Atlas and no Supabase account in any paid run. The MongoDB-saver side uses the local mongod
measurements already taken (sessions/2026-11-09-checkpoint-delta-local and this benchmark's local run) priced with
Atlas's declared storage price; Supabase is PostgreSQL on our own EC2 (`postgres-ec2.md`); Aurora stays real (its shared
storage layer is a different architecture). Our server and the Postgres machine run inside the step 9 workflow (program
`agent-bench`); Aurora needs one RDS permission (FOUNDER-SETUP.md).

## Rules

- Nothing here runs by itself. Our server and the Postgres machine are created and deleted by the step 9 workflow; the
  Aurora cluster is the only resource outside it (one RDS permission, FOUNDER-SETUP.md); every run waits for the
  founder's yes on the amount.
- Neutral names on every competitor service: project, cluster, database and user names such as `app_demo`, `app-demo`,
  `app_user`. Never "fenvix", "test", "bench" or "competitor".
- Credentials only in GitHub secrets (repository fenvix-engine, environment `bench-aws`), never in a file of any repo.
- Same workload everywhere: `generator.py --profile standard --seed 42` (2,453 threads, 25,398 turns, 96,754 checkpoints,
  about 4.4 GB of full-copy message payload: ESTIMATED by `generator.py --describe`, serializer overhead not included).
- Same client everywhere: one EC2 c7i.large (2 vCPU) in eu-west-1 running `bench.py` with one worker, then `--workers 8`.
  The AWS on-demand quota stays 8 vCPU (founder, 2026-11-09): against our server the kitchen is the cook with log-0
  (m7i.large, 2) + log-1 and log-2 (m7g.medium, 1 + 1) + this client (2) + the Postgres machine (2) = 8 vCPU;
  against Aurora only the client runs on our account (2 vCPU). Never two targets at once. A target
  outside eu-west-1 is noted in its runbook, and its latencies are reported with the measured client-to-database
  round trip next to them (`ping`-style: 20 `hello`/`SELECT 1` round trips before the run, MEASURED).
- Run length budget: 3 hours of provisioned time per target (create, load ~30-60 min, reads, erase, sizes, delete).
  Spend estimates below use 3 hours; they exclude the client EC2, which is shared.

## Secrets (names)

    BENCH_PG_URI_AURORA         postgresql://app_user:...@<aurora writer endpoint>:5432/app_demo?sslmode=require
                                (only if Aurora runs outside the workflow; the workflow passes it to the agent client
                                through the run bucket, key secret/aurora-uri, never in a log)
Our server and the Postgres machine need no secret: the round makes their password and never writes it in a file.

## Command on the client (per target)

    python bench.py --target postgres --uri "$BENCH_PG_URI_AURORA_STD" --label "Aurora Standard db.r7g.large eu-west-1" \
      --profile standard --out results/aws-<date>/aurora-standard
    python cost.py results/aws-<date>/aurora-standard/summary.json --option aurora_standard

## Before the round

Fill every `UNKNOWN` price in `prices.json` from the vendor page (the 2026-11-09 session could not open them) and keep the URL.
