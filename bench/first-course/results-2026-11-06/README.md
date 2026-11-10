# First course, two-visit test of 2026-11-06 (fenvix-brain sessions/2026-11-06-first-course-engine/REPORT.md)

Dataset: bench/compaction cold_customer.py load, 2 GB, 4,569,112 messages, 20,000 zipf customers, customer key on,
lz4 and 64 KiB blocks (load.txt). Every run starts from a copy of the same loaded bucket. Server flags of every run:
`--block-cache-mb 1`, `FENVIX_SIM_S3=5,0,100000` (5 ms per GET), plus the run's flags.

Runs (directory: binary, flags):
- on-short: first build of the feature (single-query logic identical to c6032e3), `--first-course --first-course-idle-secs 20`
- on-short-final, on-long, on-busy: c6032e3, same flags
- q4-short, q4-long: c6032e3, same flags plus `--first-course-queries 4`
- on-long-commit: cf25457; on-long-commit2: 2748e34 (same flags as on-long)
- base-short, base-long, base-busy: `compaction` a7890b3, no first-course flag

summary.txt: `tally.py` over the result files (result.json.gz holds every user's GETs, times and answers).
stress/: `stress.py` outputs on 2748e34 (q1 = single query, q3 = `--first-course-queries 3`; `-negative`: the
negative control FENVIX_FIRST_COURSE_NEGATIVE_CONTROL=1, which must find wrong answers).
