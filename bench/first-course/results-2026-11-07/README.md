# First course fix round, 2026-11-07 (fenvix-brain sessions/2026-11-07-first-course-fix/REPORT.md)

Code under test: ea07c4b (pending openings obey the cap and the budget). Control: the delivered commit 847d188.
Dataset: loaded fresh on this machine (load.txt: 4,569,112 messages, 20,000 customers, the same generator and
counts as 2026-11-06, but its own file layout after compaction, so the GET counts of visit 1 differ: 10.9 here,
9.9 on 2026-11-06). Every run starts from a copy of the same loaded bucket. Server flags of every run:
`--block-cache-mb 1`, `FENVIX_SIM_S3=5,0,100000` (5 ms per GET),
`--first-course --first-course-idle-secs 20 --first-course-queries 4`.

Runs (directory: binary, gap):
- q4-short, q4-long: ea07c4b
- delivered-q4-short, delivered-q4-long: 847d188 (control on the same data and machine)

summary.txt: `tally.py` over the result files.
stress/: `stress.sh` (new: one stress.py run on a fresh bucket) outputs. Binary ea07c4b unless the name says
`delivered-847d188`. `-negative`: the negative control (writes ignored by openings), which must find wrong answers.
