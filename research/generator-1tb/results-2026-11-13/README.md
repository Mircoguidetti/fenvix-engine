# generator-1tb, second session (2026-11-13): independent reproduction

Fresh container (4 vCPU Xeon @ 2.1 GHz, 15 GiB RAM, MinIO and mongod 7.0.34 local), release build of d7af0a5 (engine
code identical to 148bee9). Report: fenvix-brain `sessions/2026-11-12-generator-1tb/REPORT-2.md`.

- `det/`: `determinism.sh` five ways on the 3 GiB plan (ref, writers 1, stop 2, kill random = 82 s, kill 140): all
  `SAME as the reference` (rows c5f11743d8b0e8ca123e57ab0c19d645, groups dbf9d6f26b11e2a93fbe471f36e327fc).
- `big20/`: `big.sh` on 20 GiB: gen (SIGKILL after 8 of 15 slices, resumed; digest equal to the first session's
  `results/big20`), check1 (12,733 reads, WRONG 0), mutate, takeover, mutate2 (writes on B so its compactor starts),
  check3 (B after its compaction, every customer counted, all 175 stato_ts reads: 12,736 reads, WRONG 1 by the checker's rule: a $sum of doubles where ours equals the exact sum and mongod's does not; the 694 documents of that customer are byte-identical, `sum_case-cli-008760.txt`).
- `probes/`: `probe_fetch.py` (3 GiB generated: a hinted stato_ts read 0.3-0.8 s before writes, 11-13.6 s and 1.6 GB
  fetched after about 13,000 updates, takeover or not) and `probe_client.py` (the client-written control).
