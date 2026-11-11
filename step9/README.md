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
  sizes keep the five Atlas databases exactly (loaded through the founder's script, unchanged) and add ballast
  databases of the same shape (3 or 40 of 24.3 GiB), GENERATED straight into the pantry in our stored format by the
  ballast generator (below; `"ballast_mode": "script"` keeps the old path through the script on a filler machine); the
  ballast rounds read two of them with the same phases.

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

## The engine ref and the gate

`"engine"` has no default: a run names the final merged ref (branch or tag). The decide job resolves it to a commit, the
build checks out exactly that commit, and `ci/gate.py` stays CLOSED unless `step9/local/benchmark-report-<commit>.json`
exists for that commit (written by `local/benchmark_report.py` from the local takeover test and the local agent
benchmark made on it) and passes (0 acknowledged writes lost; our server not worse than PostgreSQL and mongod on any
operation or on stored bytes).

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
  pilot (5 + 5 takeovers, agent benchmark at the small profile). Every run command names the engine ref and the same
  `loader_options` and `server_options` (`--sealed-runs 2 --envelopes write --customer-key ...`), so the Atlas databases
  and the ballast share one layout. Before a command is committed to `command.json`: replace the engine ref with the
  final merged ref and the pantry with the real one of job L, then `bash commands/dry_run.sh`.
- "measure" and "extras" with every program in one job (about 7 h) is REFUSED by decide.py (B2).
- 100 GB and 1 TB (every run command also names `"engine"` and, for the generator, `"server_options"` with
  `--sealed-runs` and `--customer-key`): `{"mode": "run", "data": "1tb", "phases": ["load"], "pantry": "keep",
  "ballast_slice": [0, 0]}` (the Atlas databases), then `{"mode": "run", "data": "1tb", "phases": ["load"], "pantry":
  "<kept>"}` (the generator: all 40 ballast databases in one job, about 3.7 h ESTIMATED), then the measure jobs (1 TB as
  built is 40 databases of 24.3 GiB, not one 1 TB database: total review question 1; not in `commands/`);
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
