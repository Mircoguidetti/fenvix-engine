# C2 delta log: data preparation (competitor bytes and raw Mongo saver dumps)

Data side of the thread message log work. It has two jobs. First, rerun the LangGraph savers on every cell the
two earlier sessions measured, and check that their published per-thread bytes reproduce. Second, dump every
document the MongoDBSaver wrote in default mode as the raw BSON mongod stores, so the compactor can be tested
on real documents. Cost: $0. Everything ran on one sandbox (4 vCPU, 15 GB), local only, with no paid API.

Labels: **MEASURED** means measured in this rerun (2026-11-10). **Published** means a value taken from an
earlier session's file, which is named each time.

## What ran

- Earlier code, used unchanged, extracted with `git archive` (never checked out):
  - Owner: `origin/checkpoint-delta` (673efc12), `research/checkpoint-delta/`, mainly `run_savers.py` and `workload.py`.
  - Reviewer: `origin/claude/quirky-lamport-p2v27w` (7658727c), `research/checkpoint-delta/review/`, mainly
    `convert_real.py`, `real_run.py` and `edge_cases.py`.
  - The published copies stay untouched in `/home/user/c2data/src/{owner,reviewer}`. The runs used a working copy
    in `/home/user/c2data/run` so that they could not overwrite the published results.
- Versions:
  - Python 3.13.18 venv in `/home/user/c2data/.venv`: langgraph 1.2.20, langgraph-checkpoint 4.3.1,
    langgraph-checkpoint-postgres 3.2.0, langgraph-checkpoint-mongodb 0.5.1, langgraph-checkpoint-sqlite 3.1.3,
    langchain 1.4.9, langchain-core 1.7.7, ormsgpack 1.13.1, pymongo 4.19.0, psycopg 3.3.8, zstandard 0.25.0, lz4 4.4.5.
  - PostgreSQL 16.15 (Ubuntu package) on 127.0.0.1:5432, user `postgres`, password `pg`,
    `default_toast_compression = pglz`, data dir `/home/user/c2data/pg/data`.
  - mongod 7.0.34 from `judge/setup.sh` (`~/.fenvix-judge/env/bin/mongod`), standalone on 127.0.0.1:27017,
    `--wiredTigerCacheSizeGB 1`, dbpath `/home/user/c2data/mongo`.
- Seed: 20261109 throughout. The owner's thread scripts use `random.Random(f"20261109-{n}-{i}")`. The reviewer's
  tau-bench retail sample uses `random.Random(20261109).sample(scripts, 300)`. The SWE-agent samples are described below.

### Commands

```bash
./setup_and_fetch.sh                 # sources, venv, Postgres, mongod, real data, convert_real.py
./run_competitors.sh scripted        # owner run_savers.py: default + delta, 10x400 40x150 100x40 300x8 (also SQLite, as the owner did)
./run_competitors.sh real            # reviewer real_run.py --no-sqlite on the 4 real sets (tau retail --sample 300)
./run_competitors.sh edge            # reviewer edge_cases.py 40 3 and 100 8 (thread counts as the reviewer ran them)
P=/home/user/c2data/.venv/bin/python
$P dump_fxd.py                       # -> /home/user/c2data/dumps/<cell>.fxd + <cell>.json
$P verify_fxd.py                     # -> results/verify.json, exits 1 on any mismatch
$P collect.py                        # -> results/competitors.json
$P field_orders.py                   # -> results/field_orders.json
```

`scripted` and `real` ran in parallel (2 processes). `edge` and the dumps ran afterwards, one process at a time.
Run times: about 6 minutes for scripted, 8 for real and 5 for edge.

### Real data (reviewer's sets)

- tau-bench is a git clone of `sierra-research/tau-bench` (59a200c6). `historical_trajectories` gives the
  `tau_gpt_4o_airline` set (200 threads) and the `tau_sonnet_35_new_retail` set (920 threads, 300 sampled).
- SWE-agent trajectories come from the public bucket `s3://swe-bench-submissions`, fetched over HTTPS with the
  S3 REST listing because there is no aws CLI.
  - `lite/20240620_sweagent_claude3.5sonnet/trajs/` (289 `.traj` files). The reviewer's sample is reproduced
    exactly by `shuf -n 80 --random-source=<(yes 20261109) FILE` run on the sorted key list read **from a file**.
    Fed from a pipe, GNU `shuf -n` uses reservoir sampling and picks another set (my first attempt did this).
    List: `swe35_sample.txt`.
  - `lite/20250526_sweagent_claude-4-sonnet-20250514/trajs/` (300 `.traj` files). No `shuf` variant I tried
    reproduced the reviewer's sample: file or pipe input, populations of 300 to 1,199 lines, and the full
    1,042-key listing. So I identified the 80 files from the reviewer's `results/real/swe4.json`. That file gives,
    in file order, each thread's checkpoint count and serialized size (`per_thread_list[].logical`). I aligned
    them against all 300 files and fixed one ambiguous pick by rerunning. List: `swe4_sample.txt`.
- Check that these are the reviewer's threads (MEASURED, in `competitors.json` `same_threads_as_reviewer`):
  - On all 4 sets, the per-thread checkpoint counts equal the reviewer's, in order.
  - Per-thread logical bytes are within 0.15% (tau airline), 0.09% (tau retail), 0.005% (swe4) and 0.012% (swe35).
    They cannot be identical, because uuid6 checkpoint ids and timestamps vary between runs.
- Hugging Face was not needed and was not tried.

## Measurement (as the earlier sessions did it)

- Postgres: `pg_total_relation_size` of `checkpoints` + `checkpoint_blobs` + `checkpoint_writes`, taken after
  `CHECKPOINT` and `VACUUM ANALYZE`, divided by the number of threads. Default mode and DeltaChannel mode each
  have their own database.
- Mongo, after `fsync`, over both collections:
  - disk = `storageSize + totalIndexSize`;
  - uncompressed = `size + totalIndexSize`, the quantity Atlas bills.
- Sources of the measured numbers:
  - scripted cells: `run_savers.py` `disk.json`;
  - real cells: `real_run.py` `results/real/DS.json`;
  - edge cells: `edge_cases.py` `results/edge/N.json` for Postgres. Mongo is read with collStats in `collect.py`,
    because `edge_cases.py` does not record it.

## Results (`results/competitors.json`)

Every value below is bytes per thread. Each line gives the MEASURED value first and the published value in
brackets, with the difference. Published sources:
- scripted cells: owner `results/summary.json` `per_thread`;
- real cells: reviewer `review/results/real/<ds>.json` `per_thread`;
- edge cells: reviewer `review/results/edge/<n>.json`, which has Postgres only.

Scripted (owner's cells; mean checkpoints per thread = N exactly):
- 10 x 400:
  - Postgres default 68,444 (68,485, -0.06%); Postgres DeltaChannel 29,368 (29,327, +0.14%).
  - Mongo default disk 35,830 (35,871, -0.11%); Mongo default uncompressed 111,703 (111,393, +0.28%).
- 40 x 150:
  - Postgres default 739,683 (739,574, +0.01%); DeltaChannel 122,498 (122,498, 0.00%).
  - Mongo disk 740,475 (739,028, +0.20%); uncompressed 1,622,512 (1,622,647, -0.01%).
- 100 x 40:
  - Postgres default 4,148,838 (4,148,224, +0.01%); DeltaChannel 315,392 (315,187, +0.06%).
  - Mongo disk 4,736,205 (4,807,475, -1.48%); uncompressed 10,048,201 (10,048,410, 0.00%).
- 300 x 8:
  - Postgres default 35,276,800 (35,275,776, 0.00%); DeltaChannel 1,309,696 (1,309,696, 0.00%).
  - Mongo disk 41,001,984 (40,886,272, +0.28%); uncompressed 88,309,524 (88,298,260, +0.01%).

Real (reviewer's sets):
- tau-bench GPT-4o airline (200 threads, mean 31.76 checkpoints):
  - Postgres default 167,158 (167,281, -0.07%); DeltaChannel 72,253 (72,212, +0.06%).
  - Mongo disk 76,431 (67,789, +12.8%); uncompressed 357,692 (356,750, +0.26%).
- tau-bench Claude 3.5 Sonnet retail (300 sampled, mean 32.29):
  - Postgres default 193,604 (193,604, 0.00%); DeltaChannel 81,183 (81,210, -0.03%).
  - Mongo disk 96,611 (93,157, +3.7%); uncompressed 472,667 (472,541, +0.03%).
- SWE-agent Claude 4 Sonnet (80 threads, mean 127.72):
  - Postgres default 5,310,874 (5,311,693, -0.02%); DeltaChannel 423,117 (423,014, +0.02%).
  - Mongo disk 4,872,653 (4,897,741, -0.51%); uncompressed 14,739,272 (14,739,718, 0.00%).
- SWE-agent Claude 3.5 Sonnet (80 threads, mean 67.50):
  - Postgres default 2,610,995 (2,611,405, -0.02%); DeltaChannel 239,514 (239,514, 0.00%).
  - Mongo disk 1,982,310 (1,992,858, -0.53%); uncompressed 6,940,781 (6,940,892, 0.00%).

Edge cases (reviewer's policies on the owner's scripted threads). The Mongo values are MEASURED, with nothing
published to compare:
- 40 steps x 3 threads:
  - base: Postgres default 729,088 (729,088); DeltaChannel 204,800 (207,531, -1.3%); Mongo disk 514,731, uncompressed 1,293,843.
  - summarize: Postgres default 729,088 (729,088); DeltaChannel 204,800 (204,800); Mongo disk 513,365, uncompressed 1,293,895.
  - edit: Postgres default 589,824 (589,824); DeltaChannel 207,531 (207,531); Mongo disk 333,141, uncompressed 982,714.
  - remove: Postgres default 510,635 (510,635); DeltaChannel 207,531 (210,261, -1.3%); Mongo disk 232,107, uncompressed 743,994.
- 100 steps x 8 threads:
  - base: Postgres default 3,718,144 (3,718,144); DeltaChannel 327,680 (327,680); Mongo disk 4,234,752, uncompressed 9,139,672.
  - summarize (mean 102 checkpoints): Postgres default 2,031,616 (2,030,592, +0.05%); DeltaChannel 348,160 (348,160); Mongo disk 1,964,032, uncompressed 4,441,574.
  - edit: Postgres default 1,988,608 (1,986,560, +0.10%); DeltaChannel 330,752 (330,752); Mongo disk 1,979,904, uncompressed 4,805,522.
  - remove: Postgres default 1,681,408 (1,679,360, +0.12%); DeltaChannel 335,872 (335,872); Mongo disk 1,535,488, uncompressed 3,688,771.

Reproduction verdict (MEASURED):
- Postgres default and DeltaChannel reproduce within 0.2% on every cell, and within 1.3% on the two small
  3-thread edge cells.
- Mongo uncompressed reproduces within 0.3% in default mode and within 1.3% in DeltaChannel mode.
- Mongo storageSize does not reproduce as closely. Default mode lands within 1.5%, except tau airline at +12.8%.
  DeltaChannel mode ranges from -8.5% to +11%, and swe4 is +7.0%. This is WiredTiger block allocation on small or
  compressed collections, the same noise the reviewer reported. Use uncompressed or Postgres numbers for
  comparisons; take Mongo disk numbers as ±10%.
- The Mongo DeltaChannel values for all cells are also in `competitors.json`
  (`mongo_delta_disk_per_thread`, `mongo_delta_uncompressed_per_thread`).

## Dumps (`/home/user/c2data/dumps/<cell>.fxd`, outside git)

Format (little endian): the magic `b"FXD1"`, then one record per document until EOF. Each record is:
`coll u8` (0 = checkpoints, 1 = checkpoint_writes), `thread_id len u16`, the `thread_id` bytes (UTF-8),
`doc len u32`, then the doc bytes, which are the raw BSON from pymongo `RawBSONDocument.raw`.

The dumps cover default mode only: 16 cells, 3.3 GB in total. For every cell there is also a `<cell>.json` with
the doc counts and bytes per collection, the thread count, the total BSON bytes, the sha256 per collection in
`_id` order, the order checks and the field layout.

Order:
- MongoDBSaver 0.5.1 never sets `_id`. `put()` is `update_one(filter, {"$set": doc}, upsert=True)` and
  `put_writes()` is `bulk_write([UpdateOne(filter, {"$set"|"$setOnInsert": doc}, upsert=True)])`, so mongod
  generates the ObjectId at insert from one process-wide counter.
- The dump merges both collections sorted by `_id`.
- Checks on every cell (MEASURED): 0 problems. Threads are contiguous, and within each thread the checkpoint
  ids (uuid6) increase.
- Expected, not an error: many writes reach mongod before the checkpoint document they reference (3,741 of
  5,600 at 10x400). LangGraph's default durability is `async`, so `put` and `put_writes` run on a background
  executor. The `_id` order is the real arrival order, but it is not deterministic between reruns.

BSON layout (MEASURED, see `<cell>.json` `layout`):
- `checkpoints`:
  - `_id` objectId;
  - three filter fields in varying order: `checkpoint_ns` string (empty), `checkpoint_id` string (36),
    `thread_id` string;
  - `checkpoint`: binary subtype 0, the msgpack of the whole checkpoint;
  - `metadata`: an embedded document. Each value is an array `[type string "msgpack", binary subtype 0]`
    (`source`, `step`). `parents` is an empty embedded document;
  - `parent_checkpoint_id` string (36), or null on the first checkpoint;
  - `type` string `"msgpack"`.
- `checkpoint_writes`:
  - `_id` objectId;
  - six filter fields in varying order: `thread_id`, `checkpoint_ns`, `checkpoint_id`, `task_id` (36),
    `task_path` (for example `"~__pregel_pull, agent"`) and `idx` int32;
  - then `channel` string (`messages`, `branch:to:agent` or `branch:to:tools`);
  - `type` string `"msgpack"`;
  - `value`: binary subtype 0, the msgpack payload.
- Field order: the upsert's filter (equality) fields come first, and their order is NOT fixed. All dumps
  together show 4 orders in checkpoints and 8 in checkpoint_writes, and one collection can hold up to 8
  (`results/field_orders.json`). A byte-exact rebuild of the stored documents therefore has to keep each
  document's field order. The fields written by `$set` always follow in a fixed order.

Verification (`verify_fxd.py`, MEASURED): 16 of 16 cells pass. Per collection:
- the doc count in the file = collStats `count` = `$group` count;
- the BSON bytes in the file = collStats `size` = the `$bsonSize` sum;
- the sha256 of the docs in `_id` order matches a fresh read;
- every record's thread_id equals the document's `thread_id`;
- each file parses exactly to EOF.

## What failed, was skipped, or differs

- SQLite: skipped for the real sets (`--no-sqlite`) and not run by `edge_cases.py`. It did run for the
  scripted cells, because `run_savers.py` always includes it.
- The swe4 sample needed reconstruction (see above). My first two swe4 samples, made with `shuf`, were not the
  reviewer's threads and were thrown away.
- The edge-case Postgres DeltaChannel value is -1.3% on two 3-thread cells: one 8 KB page, divided by 3.
- During the parallel runs the kernel OOM-killed a process named `tenant_bench` (11.7 GB RSS). That was the
  main session's Rust benchmark, not one of these scripts. Every run here finished with exit 0.
- `research/c2-delta-log/results/` (not this folder) was already there from the main session and was not touched.

## Files

- `setup_and_fetch.sh`: sources, venv, Postgres, mongod, real data and the conversion.
- `run_competitors.sh`: the saver runs.
- `cells.py`: the cell list and the database names.
- `dump_fxd.py`, `verify_fxd.py`, `collect.py`, `field_orders.py`.
- `swe35_sample.txt`, `swe4_sample.txt`: the SWE-agent files used.
- `results/competitors.json`, `results/verify.json` (and `verify.log`), `results/field_orders.json`.

Postgres and mongod are left running. Postgres keeps the databases `ck_*`, `rv_*` and `edge_*` in both modes;
mongod keeps the same names (default and delta).
