# Checkpoint history: cold `get_tuple` with a long history (local, $0)

Task `prompts/P-checkpoint-history-local.md` (fenvix-brain); report `sessions/2026-11-07-checkpoint-history-local/REPORT.md`.

- `ckpt.py load`: the published `langgraph-checkpoint-mongodb` 0.5.1, unchanged, writes a deterministic history
  (threads of 1 + Poisson(16.7) checkpoints, bodies 20-300 kB, then LONG threads grown to 10,000) through its own
  `put` / `put_writes`. Same bytes on every target. `--rate-mb` caps the write rate (small sandbox disks).
- `ckpt.py measure`: cold `get_tuple` per thread (trips and GETs from our server's `serverStatus fenvix.waiter` with
  `--meter`), the same call hot, `list` limit 20 and full; every answer reduced to a digest. `ckpt.py compare A B`.
- `decompose.py`: the two queries of `get_tuple` measured one by one.
- `run_ours.sh BIN OUT THREADS LONG [load|measure|both]`: our server with the customer key on `thread_id`
  (lz4, 64 KiB), settle, cleanup by idle close, bucket count, measure with `FENVIX_SIM_S3=5,0,1000` and
  `--block-cache-mb 1` (`BLOCK_MB=256` for the normal cache).
- `run_mongod.sh OUT THREADS LONG`: the judge's mongod 7.0, WiredTiger cache 1 GB, cold after a restart and
  `drop_caches`.

Run of 2026-11-07: `RATE_MB=8 run_ours.sh ... 800 3`, `run_mongod.sh ... 800 3`, measure args
`--warmup 100 --short-n 500 --long-rounds 10 --list-threads 3 --list-full-threads 3`. Results in `results-2026-11-07/`
(`trace-one-cold-read.log`: the last 12 lines are one traced cold checkpoint query, depth = trip).
Needs: `judge/setup.sh` (mongod, MinIO), a venv with `langgraph-checkpoint-mongodb==0.5.1 langgraph boto3`
(`PY=` to point at it).
