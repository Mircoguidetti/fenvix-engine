# bench/mongod: operations per second per core, engine vs mongod

What it measures: CPU efficiency of the engine (`mongo-server`, release build) against mongod 8.0 on the same machine,
same load, same driver, same documents. Not network latency, not S3: the engine runs on `--store local` (a directory on
the local disk). Results and conclusions: `docs/bench-mongod.md`.

## Files

- `install.sh`: pulls the `mongo:8.0` Docker image (the official apt repository and mongodb-memory-server were blocked
  on the machine of the first run), builds the engine in release and the load generator; binaries go to `.bin/`.
- `server.sh start|stop mongod|fenvix CPUS`: starts a server pinned to `CPUS` (taskset for the engine, `--cpuset-cpus`
  for the mongod container, host network) on an empty data directory under `$BENCH_DATA` (default `~/benchdata`) and
  prints its process id. Port 27017 for both.
- `loadgen/`: the load generator (Go, official driver `go.mongodb.org/mongo-driver/v2`). `-mode load` fills `bench0..N-1`
  (collection `usertable`, `_id` 0..D-1, one 1000-character string field: about 1 KB per document); `-mode run` runs
  `read` (find by `_id`), `insert` (insertOne, new `_id`) or `rw` (50% find by `_id`, 50% `$set` of the field by `_id`)
  with C clients, uniform random keys and databases, then prints one JSON line: ops/s, p50/p99, and the CPU of the server
  process (`/proc/PID/stat`, all threads) and of the generator, over the measured window only. Write concern
  `{w: 1, j: true}` for both servers.
- `run_matrix.py`: the whole matrix (server x 1/2 cores x 1 db of 200 000 docs / 100 dbs of 2 000 docs x 3 workloads).
  Per cell: a client sweep (1, 2, 4 ... 256 clients, 10 s warm-up + 20 s) to find the saturation point (smallest client
  count within 3% of the best throughput), then 3 runs of 60 s warm-up + 180 s measured there. Appends to
  `$BENCH_RESULTS/raw.jsonl` (default `results/`) and skips finished cells, so it can be restarted. Each workload gets a
  fresh, freshly loaded server; a load or run that stalls (the engine's write stall, see `docs/bench-mongod.md`) is recorded
  and repeated on a new server (5 load attempts, 4 run attempts).
- `summarize.py`: medians of the 3 runs per cell, ratios engine/mongod, sweep curves.
- `profile.sh WORKLOAD LAYOUT CLIENTS [SECONDS]`: perf profile of the engine on one core under one workload, sampled after
  90 s of load; top functions by self time to `results/perf-*-top.txt`.
- `results/`: raw results, summary, matrix log, profiles, and the evidence of the stall (`results/stall/`).

## Run it

```
bench/mongod/install.sh
BENCH_RESULTS=$HOME/benchdata/results python3 bench/mongod/run_matrix.py     # about 6 hours on 4 vCPU
BENCH_RESULTS=$HOME/benchdata/results python3 bench/mongod/summarize.py
bench/mongod/profile.sh read 1 8 60
```

Core layout on a 4-CPU machine: the database on CPU 0 (or 0-1), the generator on the remaining CPUs. Check the
`gen_cores_used` column: if the generator gets close to the CPUs it has, the measurement is limited by the generator,
not by the database.

## Settings that matter

- mongod 8.0 (Docker `mongo:8.0`): defaults. WiredTiger journal always on; cache 50% of (RAM - 1 GB).
- Engine: defaults of main, except `--store local --local-dir ...` (object store on the local file system). Its local
  store writes files without fsync (object_store `LocalFileSystem` without `with_fsync`); mongod's `j: true` waits for a
  journal fsync. Write durability is therefore not the same; see `docs/bench-mongod.md`.
