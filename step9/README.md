# Step 9: the AWS round of compaction 2 (prepared at $0, 2026-11-09)

Our full kitchen in eu-west-1, measured like for like against the Atlas Infinite M30 round from Ireland, plus the
separate programs the founder asked for. Session report: fenvix-brain `sessions/2026-11-09-step9-prep/REPORT.md`.
Nothing here has run on AWS. Every script is meant to be read by the reviewer before any cloud run (RULES.md).

## Like for like with Atlas Infinite M30 (Ireland)

- Same program: code-irlanda's `m30-corto` (cold reads one at a time Q2/Q3/Q1/Q1d/Q4 with 300 active and 300 dormant
  customers, parallel 10/50/100, maximum load = hot reads per second with CPU and peak batch inserts with the real
  indexes, w:1 and majority), run by `kitchen/client.sh` UNCHANGED, which runs code-irlanda's `programma.py` and the
  founder's `prova_infinite.py` unchanged (md5 8396ca455557baaa4e3a9b1f881451f9, checked on every machine).
- Same data: the five databases m30-corto reads, loaded with the same seeds and stopped at the fraction Atlas reached
  (`kitchen/load.py`), the same customers and windows (`merge_states.py`, then `compare_plans.py` must say
  `PLANS: SAME` before anything is measured), checked afterwards (`check_reads.py`).
- Same region and client: eu-west-1, the program's client machine c7i.2xlarge with 5 copies for the maximum load.
- Machines matching an M30 node (2 vCPU, 8 GB): cook and warm spare `m7i.large` (x86, $0.1124/h) or `m7g.large`
  (Graviton, $0.0910/h) / `m8g.large` ($0.1000/h), prices DECLARED (AWS Price List eu-west-1 2026-11-04). The 3-zone
  log on three `m7g.medium` ($0.0455/h), one per zone.
- Data size is a parameter: `atlas` (about 27 GB of BSON, the size measured on Atlas), `100gb`, `1tb`. The bigger
  sizes keep the five Atlas databases exactly and add ballast databases of the same shape, loaded by the founder's
  script (`carica`, own seeds) on a filler machine; the ballast rounds read two of them with the same phases.

## The kitchen of the round

```
zone a: log-0, server-0 (loader, off after the load), server-1 (the measured cook), client, filler, extras
zone b: log-1, spare (warm spare, same type as the cook)
zone c: log-2, compactor (only if the command gives one: compaction 2 step 4)
```
The cook and the spare run `mongo-server --quaderno log-0:7200,log-1:7200,log-2:7200`: a write is confirmed when 2 of
the 3 log copies have it (like w: majority on three zones). Ports open only between the round's own groups.

## Files

- `.github/workflows/step9-kitchen.yml`: the workflow. Trigger: a push on `step9-prep` that changes `step9/command.json`.
- `command.json`: the command (`{"mode": "none"}` = nothing). Fields and defaults: the docstring of `ci/decide.py`.
- `ci/decide.py` (checks and resolves the command, vCPU peak against the quota), `ci/permissions.sh` ($0 check),
  `ci/packages.sh` (machine packages: the kitchen's client package + step 9 scripts; mongo-server and pq per CPU),
  `ci/run.py` (the round: SPESA every minute, BLOCCO at the cap), `ci/infra.py` (EC2+S3 or docker+MinIO),
  `ci/teardown.py`, `ci/final_check.py` ("RIMASTO: niente" or ALLARME), `ci/machine_types.py` (types, DECLARED
  prices), `ci/estimate.py` (cost and time per size, arithmetic printed).
- `machine/`: what runs on the machines. `node.sh` (dispatch by role), `log.sh` (a log node), `cook.sh` (loader,
  cook, spare, compactor), `takeover_agent.py` (starts/kills the takeover pair on 27018), `filler.sh` + `ballast.py`,
  `extras.sh`, `common.sh`, `cfg.py`.
- `programs/`: `s3_first_byte.py`, `sealed_run_read.py`, `log_ack.py`, `takeovers.py`.
- `local/`: the $0 validation: `local-run.sh WORK` (docker containers as machines, MinIO as S3, a fake Atlas made
  like the kitchen's local test, the same scripts), `check_local.py`, `fake_atlas.sh`, `Dockerfile`.

## Commands (examples)

- `{"mode": "permissions"}`: $0, every permission and the vCPU quota.
- Atlas size, two jobs (a GitHub job lasts at most 6 hours; load about 2.8 h + measure about 4.4 h, ESTIMATED):
  `{"mode": "run", "data": "atlas", "phases": ["load"], "pantry": "keep"}` then
  `{"mode": "run", "data": "atlas", "phases": ["measure", "extras"], "pantry": "fenvix-kitchen-pantry-<run>"}`.
- 1 TB: one `keep` load round, then load rounds with `"ballast_slice": [a, b]` on the kept pantry, then the measure
  round; `{"mode": "drop-pantry", "pantry": "..."}` at the end.
- `"spend_cap"` above 5 needs `"cap_approved"`: the date and words of the founder's yes.

## What depends on compaction 2 steps 4-8

The workflow builds the servers from `"engine"` (default `integration`) and passes `"server_options"`: the waiter's
counter, the first course, hedged GETs and the new compaction are options of the engine ref chosen when step 8 is
approved. The compactor machine runs only when `"compactor"` and its command line are given (step 4). The sealed-run
program measures the physical one-trip read; its hook `--engine-reader` runs the engine's own reader once
`src/runfmt.rs` is in the engine ref (step 3). The warm spare is today a second server on the same pantry and log that
takes the databases over when the cook's lease lapses; known gap (Q8d, agent-lane report): a live takeover does not
replay the log's records not yet in S3, so the takeovers program may count lost writes with `--log on` until the
warm-spare work fixes it.
