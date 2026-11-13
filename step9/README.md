# Step 9: the AWS round of compaction 2 (prepared at $0, 2026-11-09)

Our full kitchen in eu-west-1, measured like for like against the Atlas Infinite M30 round from Ireland, plus the
separate programs the founder asked for. Session report: fenvix-brain `sessions/2026-11-09-step9-prep/REPORT.md`.
Nothing here has run on AWS. Every script is meant to be read by the reviewer before any cloud run (RULES.md).

## aws-fixes (2026-11-11): what changed after the total review (fenvix-brain sessions/2026-11-11-review-total/)

- The gate (`ci/gate.py`, B1) closes when any side's summary has `ok` false, when any listed operation is missing on
  any side or has fewer samples than its written minimum (`MIN_SAMPLES`), when stored bytes are unknown, or when the
  sides ran different plans; it also checks `save_step`, `time_travel`, `resume[cold]` and the `--workers 8` line when
  the report names one. Test: `python3 local/tests/test_gate.py` (the reviewer's script must say CLOSED).
- The job plan (B2): the measure is THREE jobs on the kept pantry: M1 the m30 program alone; M2 the extra programs then
  write-peak; M3 the agent benchmark. `ci/decide.py` estimates every job's duration (`JOB_HOURS`, ESTIMATED) and
  refuses one above 90% of its ceiling, and refuses a command that does nothing (`"phases": ["extras"]` alone). The
  exact commands of every job are in `commands/`, with their dry run through decide.py (`commands/DRY-RUN.txt`).
- write-peak (B4, `programs/write_peak.py`): peak writes with the real indexes through the MEASURED cook (log on): the
  founder's script's own load, 6 databases x 16 threads as in the Atlas load that gave 25,800 documents per second.
- Settle (B5, `kitchen/settle.py`): waits on the server's compaction counters (serverStatus `fenvix.compactor`,
  `fenvix.compaction`, `fenvix.sealedRuns`) and on the `fx/` objects, not only `.sst`; never gives up silently (the job
  stops: `LOADED_NOT_SETTLED`, or the size is `NOT_SETTLED` and not measured); `"prepare": true` by default; the record
  is `client/settle.json` and `client/size-1/settle.json` in the results. Test: `local/tests/test_settle.py`.
- Durability (B10): Aurora is the equal-durability line; the Postgres machine is "single node, no copy" in every output;
  its TLS is required and `shared_buffers` is 25% of memory; `pg_settings` of both is saved, and Aurora's description
  (`aurora-describe.json`). Aurora runs first in the agent benchmark and is deleted as soon as its target is done.
- Zones (question 1 point 4): every client in eu-west-1b, the cook, the Postgres machine and Aurora in eu-west-1a, as
  on Atlas (client and primary in different zones); `"client_zone": "same"` puts them together. Every machine records
  its zone name and zone ID (`results/<name>/zone.json`, listed in `run.json`).
- Takeovers (question 2 point 2): every acknowledged `_id` with its time and server (`takeovers-acked.jsonl.gz`) and
  every missing `_id` at each read (`takeovers-missing.json`).
- `{"mode": "drop-pantry"}` was refused by decide.py (the default phases include load on an existing pantry); fixed.

## Like for like with Atlas Infinite M30 (Ireland)

- Same program: code-irlanda's `m30-corto` (cold reads one at a time Q2/Q3/Q1/Q1d/Q4 with 300 active and 300 dormant
  customers, parallel 10/50/100, maximum load = hot reads per second with CPU and peak batch inserts with the real
  indexes, w:1 and majority), run by `kitchen/client.sh` UNCHANGED, which runs code-irlanda's `programma.py` and the
  founder's `prova_infinite.py` unchanged (md5 8396ca455557baaa4e3a9b1f881451f9, checked on every machine).
- Same data: the five databases m30-corto reads, loaded with the same seeds and stopped at the fraction Atlas reached
  (`kitchen/load.py`), the same customers and windows (`merge_states.py`, then `compare_plans.py` must say
  `PLANS: SAME` before anything is measured), checked afterwards (`check_reads.py`).
- Same region: eu-west-1. Not the same client: Atlas's was a c7i.2xlarge (8 vCPU); within the 8-vCPU quota ours is a
  c7i.xlarge (4 vCPU). The maximum-load step runs 5 copies of the script whatever the machine (the program line
  `massimo m30s 5`), so hot reads per second and peak batch writes are likely NOT VALID for us (client CPU) and Atlas's
  hot reads were already a floor (its client at 81.6%): total review B3, a founder decision.
- Machines matching an M30 node (2 vCPU, 8 GB): cook and warm spare `m7i.large` (x86, $0.1124/h) or `m7g.large`
  (Graviton, $0.0910/h) / `m8g.large` ($0.1000/h), prices DECLARED (AWS Price List eu-west-1 2026-11-04). The 3-zone
  log on three copies, one per zone: log-0 on the cook's own machine, log-1 and log-2 on `c7g.medium` ($0.0388/h).
- Data size is a parameter: `atlas` (about 27 GB of BSON, the size measured on Atlas), `100gb`, `1tb`. The bigger
  sizes keep the five Atlas databases exactly (loaded through the founder's script, unchanged) and add ONE ballast
  database of the same shape, `app_data_b01`, of `ballast_gib` GiB of BSON (generator-1tb, 2026-11-12; defaults 93.2
  for `100gb`, 931.4 for `1tb`, any size from 1 to 1100), GENERATED straight into the pantry in our stored format by
  the ballast generator on several writers (below; `"ballast_mode": "script"` keeps the old path through the script on
  a filler machine); the ballast rounds read that database with the same phases.

## The ballast generator (aws-ready, 2026-11-10)

`mongo-server <the round's server options> --ballast PLAN.json` (`mongo-server/src/ballast.rs`): the server process
itself, started with the cook's options, writes each ballast database as sealed runs plus one published version of our
manifest (compaction 2's format, keys of layout v2 with `--customer-key`), with no client, no wire, no WAL and no
compaction rewrite. Every row comes from the server's own code: the catalog through its command handler in the same
process (`createIndexes` as the script sends it), every document through `Writes::put` (the `insert` command's path:
document, index entries, locator, insertion order, counters), the storage counters through `collstat::ops_for`, the
layout through `layout2::phys_ops`. The rows of the customer space (`u/`) stream into sealed runs as they come (customer
after customer); the shared keyspace (locators, the `stato_ts` index, insertion order, counters) is sorted on local disk
(zstd chunks) and merged with SlateDB's catalog rows at the end (`fx/build.rs`, `publish_generated`). The documents
have the script's shapes, vocabulary, mix, timeline and customers (`machine/ballast_plan.py` imports the frozen script
and calls its own `prepara_clienti`); `machine/ballast_states.py` writes a state file in the script's format per
database, so the ballast rounds run the script's read phases unchanged.
On AWS: `machine/generator.sh` on one machine (`generator`, c7i.2xlarge for 1 TB, c7i.xlarge up to 4 databases,
60 GB gp3 for the spill), alone in its job, one database per vCPU. Needs `--sealed-runs` and `--customer-key` in
`server_options` (decide.py refuses otherwise). Local proof at 10 GB and the measured rate: the session report.

## Aurora PostgreSQL in the agent benchmark

`"aurora": true` (with the agent benchmark): `ci/aurora.py` creates an Aurora PostgreSQL cluster (Aurora Standard, one
writer db.r7g.large, Single-AZ eu-west-1a, default VPC and its default security group, tag project=fenvix-coldread,
names `fenvix-coldread-s9-<run>-aurora*`) while the Postgres machine boots, waits for it under the SPESA line, gives its
address to the agent client (the same benchmark as the Postgres machine, target "aurora"), and deletes it right after
(no final snapshot, no automated backups kept). BLOCCO starts its deletion; the teardown step deletes it; the final check
lists any cluster, instance, subnet group or snapshot left (RIMASTO / ALLARME). The RDS policy the founder adds:
`python3 step9/ci/aurora.py policy`; `{"mode": "permissions", "aurora": true}` checks it at $0 (no cluster is created).

## The engine commit and the gate

`"engine"` has no default: a run names the FULL 40-character commit id of the final merged head, never a branch or tag
(total review R5, 2026-11-12: a branch moves between the jobs of one round; `ci/decide.py` refuses anything else). The
build checks out exactly that commit and builds it with `ci/build-engine.sh` (one fixed folder + path remapping, so the
same commit gives the same bytes as the local build: `local/builds/`). `ci/gate.py` stays CLOSED unless
`step9/local/benchmark-report-<commit>.json` exists for that commit (written by `local/benchmark_report.py` from the local
takeover tests, log on and off, and the local agent benchmark with its `--workers 8` line, all made on it) and passes:
0 acknowledged writes lost in at least 50 takeovers per log mode, no write batch on the S3-WAL fallback with the log on,
and our p50 and p99 within the speed tolerance of `ci/speed_tolerance.json` (DECISIONS 2026-11-12: 20 ms above the best of
PostgreSQL and mongod) on every operation, stored bytes per logical byte not worse.

## How the round is started (total review R8, 2026-11-12)

The workflow listens only to `step9-prep` and `aws-*`, and the AWS role trusts `aws-*` only: `step9-final` (the harness
branch) can never start it. The round therefore runs from a branch made for it:

1. The final engine head is chosen (coordinator, founder) and its full commit id written as `"engine"` in every run
   command of `commands/` (and `commands/dry_run.sh`'s default); `bash commands/dry_run.sh` must accept them all.
2. On that head, the local round and the local benchmark are made and the report written:
   `local/benchmark_report.py ... > local/benchmark-report-<commit>.json` with every result file it names; the gate run
   locally (`ENGINE_REF=<commit> ENGINE_COMMIT=<commit> python3 ci/gate.py`) must print `GATE: OPEN`.
3. A branch `aws-<name>` (e.g. `aws-round-1`) is made from `step9-final` with the report and its result files committed.
   Nothing on `aws-*` is merged anywhere; `main` is never touched.
4. Each job starts with one commit on `aws-<name>` that changes `step9/command.json` to that job's command (from
   `commands/`, the pantry of job L filled in after L). Only the founder's go starts the first paid job (RULES).
5. Every job pushes its results back to that same `aws-<name>` branch; the next job's command is committed after reading
   them. A red job (a program failed, the gate closed, BLOCCO) stops the round there.

## The kitchen of the round: never more than 8 vCPU at once (founder, 2026-11-09)

```
load job, Atlas databases: loader c7i.xlarge (4) + m30 client c7i.xlarge (4)                         = 8
load job, ballast:         loader c7i.xlarge (4) + filler c7i.xlarge (4)                             = 8
measure job, m30:          cook m7i.large + log-0 (2, zone a) + log-1, log-2 c7g.medium (1+1, b, c)
                           + m30 client c7i.xlarge (4, zone a)                                        = 8
measure job, extras:       cook + log-0 (2) + log-1, log-2 (2) + spare m7i.large (2, zone b)
                           + extras client c7i.large (2)                                              = 8
```
```
write-peak (in M2):        cook + log-0 (2) + log-1, log-2 (2) + a client of the m30 client's type c7i.xlarge (4) = 8
agent benchmark (M3):      cook + log-0 (2) + log-1, log-2 (2) + Postgres m7i.large (2) + agent client c7i.large (2) = 8
```
`ci/decide.py` refuses any command above 8 (`S9_VCPU_LIMIT` raises it only in local mode). The cook and the spare run
`mongo-server --quaderno log-0:7200,log-1:7200,log-2:7200`: a write is confirmed when 2 of the 3 copies have it (like
w: majority on three zones); log-0 shares the cook's machine as Atlas's oplog shares its nodes. Ports open only between
the round's own groups. Not like for like within 8 vCPU: the maximum-load test's client (4 vCPU running the program's 5 copies of the script,
against Atlas's c7i.2xlarge with 5); `somma_massimo.py` says when the client was the limit.

## Files

- `.github/workflows/step9-kitchen.yml`: the workflow. Trigger: a push on `step9-prep` or an `aws-*` branch that changes `step9/command.json`.
- `command.json`: the command (`{"mode": "none"}` = nothing). Fields and defaults: the docstring of `ci/decide.py`.
- `ci/decide.py` (checks and resolves the command, vCPU peak against the quota), `ci/permissions.sh` ($0 check),
  `ci/packages.sh` (machine packages: the kitchen's client package + step 9 scripts; mongo-server and pq per CPU),
  `ci/run.py` (the round: SPESA every minute, BLOCCO at the cap), `ci/infra.py` (EC2+S3 or docker+MinIO),
  `ci/teardown.py`, `ci/final_check.py` ("RIMASTO: niente" or ALLARME), `ci/machine_types.py` (types, DECLARED
  prices), `ci/estimate.py` (cost and time per size, arithmetic printed).
- `machine/`: what runs on the machines. `node.sh` (dispatch by role), `log.sh` (a log node), `cook.sh` (loader,
  cook, spare, compactor), `takeover_agent.py` (starts/kills the takeover pair on 27018), `filler.sh` + `ballast.py`,
  `extras.sh`, `common.sh`, `cfg.py`.
- `programs/`: `s3_first_byte.py`, `sealed_run_read.py`, `log_ack.py`, `takeovers.py`, `agent_bench.py`.
- aws-ready (2026-11-10): `ci/aurora.py` (the Aurora cluster, its policy and $0 check), `machine/generator.sh`,
  `machine/ballast_plan.py`, `machine/ballast_states.py` (the ballast generator's machine side), `local/ballast_check.py`
  (the generated data against mongod 7.0), `local/test_aurora.py` (aurora.py against moto, offline),
  `local/benchmark_report.py` (the report the gate needs).
- `local/`: the $0 validation: `local-run.sh WORK` runs the three jobs (Atlas databases into a kept pantry, ballast,
  measure) with docker containers as machines, MinIO as S3, a fake Atlas made like the kitchen's local test, the same
  scripts; then drops the pantry; `BLOCK_TEST=1` adds a round with a $0.05 cap. `check_local.py`, `fake_atlas.sh`,
  `Dockerfile`.

## Commands (examples)

- `{"mode": "permissions"}`: $0, every permission and the vCPU quota.
- The exact commands of the round are the files in `commands/` (aws-fixes, B6): 00 permissions, 01 L load the Atlas
  databases into a kept pantry, 02 M1 m30, 03 M2 extra programs and write-peak, 04 M3 agent benchmark with Aurora,
  05 G the 100 GB ballast generated into the same pantry, 06 B the ballast rounds, 07 drop the pantry; pilot-P a small
  pilot (5 + 5 takeovers, agent benchmark at the small profile). Every run command names the engine commit and the same
  `loader_options` and `server_options` (`--sealed-runs 2 --envelopes write --customer-key ...`), so the Atlas databases
  and the ballast share one layout. Before a command is committed to `command.json`: the engine is the full commit
  id of the final head and the pantry the real one of job L, then `bash commands/dry_run.sh` (see "How the round is
  started").
- "measure" and "extras" with every program in one job (about 7 h) is REFUSED by decide.py (B2).
- 100 GB and 1 TB (every run command also names `"engine"` and, for the generator, `"server_options"` with
  `--sealed-runs` and `--customer-key`): `{"mode": "run", "data": "1tb", "phases": ["load"], "pantry": "keep",
  "ballast_slice": [0, 0]}` (the Atlas databases), then `{"mode": "run", "data": "1tb", "phases": ["load"], "pantry":
  "<kept>"}` (the generator: the one ballast database; at 1 TB about 6-7 h on c7i.2xlarge ESTIMATED, so 2 jobs: the
  generator commits slice by slice in the pantry, stops 20 minutes before the job's ceiling with BALLAST_PARTIAL, and
  the same command again resumes it; generator-1tb), then the measure jobs (not in `commands/`);
  `{"mode": "drop-pantry", "pantry": "..."}` at the end. The first job stores the round password in the kept pantry;
  later jobs read it (the root user lives there).
- `"spend_cap"` above 5 needs `"cap_approved"`: the date and words of the founder's yes.

## What depends on compaction 2 steps 4-8

The workflow builds the servers from `"engine"` (default `integration`) and passes `"server_options"`: the waiter's
counter, the first course, hedged GETs and the new compaction are options of the engine ref chosen when step 8 is
approved. The compactor machine runs only when `"compactor"` and its command line are given (step 4). The sealed-run
program measures the physical one-trip read; its hook `--engine-reader` runs the engine's own reader once
`src/runfmt.rs` is in the engine ref (step 3). The warm spare is today a second server on the same pantry and log that
takes the databases over when the cook's lease lapses. The takeover program reads the acknowledged writes back three
times (at once, after 90 s, after a clean restart that replays the log) to tell a loss from late visibility.

## bench-local fix round, part 2 (total review intermediate 2, 2026-11-11)

- N3: `local/benchmark_report.py` takes `--takeovers-off` and `--engine-sha256`; the gate checks the takeovers with the
  log on AND off, and refuses a report without either.
- The binary: every result names the mongo-server it was made by (`engine_binary_sha256` in bench.py's summary; a
  `<summary>.binary-sha256` sidecar for local takeovers; on AWS the cook writes its hash in `ready.json`, run.py passes
  it to every program, and `programs/fenvix_status.py stamp` writes it into each result). The gate refuses a result made
  by another binary than the report's.
- N4 (DECISIONS 2026-11-11, the S3-WAL fallback): `machine/extras.sh` saves our server's status before and after
  log-ack, write-peak and the agent benchmark (`programs/fenvix_status.py`) and labels each result with the batches that
  took the fallback; `programs/takeovers.py` reads each process's `quaderno.failed` before killing it and the survivor's
  before the clean restart (`log_fallback` in its summary).
- B9 on AWS: the agent benchmark passes `--settle` to our target, uses `--read-sample 300 --erase-users 100`, and runs the
  cold phase: `programs/cold_restart.py` asks the cook or the Postgres machine, through the run bucket, to stop its
  server, drop its page cache and start again (`machine/common.sh serve_cold_restart`, in `cook.sh` and `pgbox.sh`).
  Aurora has no cold phase (a reboot needs an IAM permission the workflow does not have).
- Commands: job L runs with `client_zone` "same" and `settle_max_min` 60; the pilot adds 5 minutes of write-peak (the
  write-peak estimate now follows its `--minutes`); job L or any job turns red when `client/end.json` says the plans
  differ (`ci/outcomes.py`).
- N5: the 100 GB plan counts job B; the SPESA line adds each job's cross-zone allowance (`ci/outcomes.py`).
Tests: `local/tests/test_gate.py`, `test_cold_restart.py`, `test_fenvix_status.py`, `test_outcomes.py`,
`test_takeover_ids.py`.

## The 1 TB part of the round (2026-11-13, coordinator's request to the total review; nothing run on AWS)

The founder's goal is a database of 1 TB and up where a customer's first request may pay S3 and every next request is
as fast as on MongoDB. The round now goes from the Atlas size straight to ONE database of 1 TB on the same kept pantry
(the generator of fenvix-engine `generator-1tb` 7b4eb31, approved: `ballast_gib`, one database `app_data_b01`,
resumable across jobs). The 100 GB commands (05, 06) stay as they are but are not part of this plan: a pantry holds
one ballast database.

- `commands/08-G1-generate-1tb.json`: the generator alone on a c7i.2xlarge. 3.8 to 6.0 hours of generation (ESTIMATED:
  the rate per writer is MEASURED locally, what 8 hyperthreads give is not), so 1 or 2 jobs: a job that ends
  `BALLAST_PARTIAL_RESUME_NEXT_JOB` is red on purpose; commit the same command again and it resumes from its last
  committed slice. The first minutes of the job print the real rate per slice.
- `commands/09-B1-read-rounds-1tb.json`: reads only. first-next, then the founder's script's cold read rounds on the
  1 TB database. No spare (no takeover runs). The pantry stays as generated, so this job can be repeated.
- `commands/10-W1-first-next-ops-reads-after-writes-1tb.json`: the LAST job on the pantry, because it writes:
  - `programs/first_next_ops.py`: FIRST and NEXT of each request of the agent benchmark (resume, last 10, whole history,
    time travel, save), the customer as the thread and its `messaggi` as its checkpoints; each operation has its own
    cold customers; the cook's GETs, bytes and rows from sealed runs per request; the time of the opening read apart.
    `--no-save` sends the four reads only (the pilot uses it: its pantry is read as loaded by job M1 afterwards).
  - `programs/reads_after_writes.py`: the `stato_ts` index read (the generator's own probe) before about 13,000 order
    updates, right after them, and after the compactor went quiet: seconds, documents examined, rows read from sealed
    runs per document, bytes fetched. The slowdown found by the generator's second session (15 to 40 times after
    updates) is being fixed; this measures it at 1 TB either way. A read is cut at 180 s and its stage stops there.
  - then first-next-ops again on other cold customers (label after-writes) and
    `programs/first_next_compare.py` after-writes against before-writes.
- Against MongoDB at the same size: `first_next_ops.py` and `first_next_compare.py` run unchanged against any MongoDB
  URI (on a mongod the counters are WiredTiger's pages and bytes read into its cache). What is NOT built: the machine
  role that starts a mongod replica set, loads the Atlas-shaped databases through the founder's script and restarts it
  cold. Until it is, the round has no MongoDB line for these five requests; the comparison with Atlas is the founder's
  script's own cold and warm reads at the Atlas size (job M1). A mongod at 1 TB is not planned: its load through the wire
  takes more than a day at the rate measured on Atlas.
- Rule for the two programs that write (decide.py `WRITING_PROGRAMS`): never in the default program list; inside a job
  they run after the script's read rounds (extras.sh's order); across jobs, only after every job that must read the
  pantry as loaded (M1 for the Atlas databases, B1 for the 1 TB one).
- `ci/estimate.py` prints the 1 TB addition and the round's total (`plan_1tb`); decide.py's estimate of the agent
  benchmark is now 0.9 hours per target at the standard profile and 0.2 at the small one (the local write phases alone
  took 19 to 33 minutes, MEASURED 2026-11-11).
- Tests: `local/tests/test_first_next_ops.py` (12, with a fake server: the real runs are the local docker round and the
  pilot), `local/tests/test_plan_1tb.py` (7). `bench/agent-checkpoints/tests/review_miss_wrong_answers.py` plants its
  stale-resume fault with a fixed hash (it failed about one run in four with Python's per-process `hash()`).
