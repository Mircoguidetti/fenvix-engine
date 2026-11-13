# fix-read-amplification: proof scripts and results (2026-11-13)

Report: fenvix-brain `sessions/2026-11-13-fix-read-amplification/REPORT.md`. Local only, MinIO and mongod 7.0.34 on
this session's machine (4 vCPU, 15 GiB RAM). Binaries: `base` = integration-4 4f745747ee97, `fix1` = 7fdfe66 without
the area bound, `fix3` = 2f71006, `fix4` = the first warm-up (serial fetches), `fix5` = ed53914 (the measured head code).

- `probe_fetch.py`: generator-1tb's probe (7b4eb31) plus BIN, rows per document, the `fenvix.fetch` counters, a stage
  2a (right after the writes) and ONLY_2A / ONLY_1 for short runs. Data: the 3 GiB plan of `determinism.sh` (seed 2002,
  4 writers), generated once with generator-1tb's binary, restored from a copy before every run (`restore.sh`).
- `probe_client2.py`: the client-written control, loaded once (460,800 orders of 1,536 customers, through the wire,
  published and absorbed), restored before every binary (`restore2.sh`), read with a 32 MB run cache.
- `ab_answers.py` + `compare.py`: rule 8. The same writes (updates, deletes, whole-customer erasures), absorption,
  SIGKILL and takeover, more writes, on base, fix and mongod 7.0; every answer hashed per stage.
- `results-2026-11-13/`: `generated-3gib/out-*` (one probe per binary; `out-noscan` = base with MONGO_NO_FETCH_SCAN=1,
  the counter proof of the cause), `generated-3gib/ab-cold-warm` (interleaved A/B of the cold and warm reads),
  `client-460k/` (load and one measure per binary), `rule8-ab/` (the three runs), unit test logs.
- `results-2026-11-13/on-i6/`: the same generated probe (stages 1 to 2a) and client probe on `fix-read-amplification-on-i6`
  (`out-i6`, `measure-i6`) and on its base, integration-5-stack 0ed53558 (`out-i6base`, `measure-i6base`).
- `results-2026-11-13/bench-big-line/`: command-cpu's `lineprobe.py` A/B on bench-big's big line (integration-4 base
  against the fix binaries, `recook_cap.py` for the 3 GiB cap), every round's answer hashes; `lineab_summary.py` pools them.
