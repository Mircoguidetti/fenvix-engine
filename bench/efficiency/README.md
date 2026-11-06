# bench/efficiency: E1 study, where the engine loses CPU against mongod

Report: fenvix-brain, `sessions/2026-11-06-efficiency-study/REPORT.md`.
Branch `studio-efficienza` (from `motore-correzioni`). Measurement code only: nothing here is meant for main.

## What is measured

The write load of the M30 peak of the Ireland round (code-irlanda: `irlanda/programmi/carica`, `prova_infinite.py carica`):
6 databases written together, 16 workers each, unordered `insertMany` of 1,000 documents split by collection
(messaggi 50%, eventi 35%, ordini 15%), documents built like `prova_infinite.py` (same fields, same zipf text from the
same vocabulary, about 337 bytes of BSON on average), indexes `cliente_ts` on the three collections and `stato_ts` on
ordini, no explicit write concern (server default). Then the hot single-document read of the same program
(Q2: an order by `_id`), 64 workers over a hot set of 20,000 orders per database.

Both servers run on the same machine, CPUs 0-1 (2 cores, like an M30); the generator runs on CPUs 2-3.
The CPU of the server is read from `/proc/PID/stat` (all threads, user + system) every 5 seconds, so CPU per
operation is exact for any window, whatever the throughput.

- engine: `mongo-server --store local --replica-set rs0` with its defaults (local files instead of S3);
- mongod: Docker image `mongo:9.0.4` (closest public release to Atlas 9.1.0), single-node replica set,
  `--wiredTigerCacheSizeGB 2` (the cache size of M30).

## Files

- `loadgen/`: the generator (Go, official driver v2). `-mode write` and `-mode read`; one JSON line per 5 s.
- `run.sh SERVER LABEL [DOCS] [MAX_SECS] [READ_SECS]`: one measurement, fresh data directory. Waits until the
  server is quiet between writes and reads. `PERF_WRITE_AT` / `PERF_READ_AT`: perf with DWARF stacks (profile runs).
  `ENGINE_ARGS`, `LG_WRITE_ARGS`, `ENGINE` select variants.
- `summarize.py FILE`: per-minute table and totals (ops/s, server cores, server µs per operation).
- `pieces.py PERF_SCRIPT`: splits a profile into the pieces of the study (sum 100%), with the callers of each piece.
- `results/LABEL/`: raw windows (`write.jsonl`, `read.jsonl`), server status, sizes, and for `engine-prof` the
  demangled perf scripts (`*.script.gz`), folded stacks and flamegraphs (`*.flamegraph.svg`), piece splits (`*.pieces.txt`).
- `mongo-server/src/e1.rs`: two measurement-only switches, UNSAFE, off by default (`E1_UNSAFE_SKIP_ID_CHECK=1`,
  `E1_UNSAFE_SKIP_CHANGE_LOG=1`), used only for A/B runs that measure what a piece costs.

## Reproduce

    bash bench/efficiency/run.sh engine engine-2 10000000 2700 150
    bash bench/efficiency/run.sh mongod mongod-1 10000000 2700 150
    PERF_WRITE_AT=200 PERF_READ_AT=20 bash bench/efficiency/run.sh engine engine-prof 6500000 2700 60
    perf script --no-inline -i results/engine-prof/write.perf.data -F comm,tid,time,event,ip,sym,dso | c++filt | gzip > write.script.gz
    python3 bench/efficiency/pieces.py write.script.gz

Prerequisites: Docker (image from `mirror.gcr.io/library/mongo:9.0.4`), Go, perf (`linux-tools-generic`),
`.bin/mongo-server` (release build with `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`), `.bin/loadgen`.
