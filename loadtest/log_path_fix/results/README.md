# Results of the log-path fix (P-log-path-fix), all MEASURED on the session sandbox (4 cores), local only

Engine code: commit 56f9152. Pre-fix control binary: 8fbdd8d (agent-lane head, the review's base).

- `review_*.out`: the review's tests R1-R5, unchanged, and the controls `r2c` (R2 --control) and `r3c` (R3 --control).
- `takeover_rounds.log`: 50 live takeovers, no sleep before the kill. `takeover_rounds_control_8fbdd8d.log`: 3 rounds on 8fbdd8d.
- `kill_random_100.log`: 100 kill -9 at random points. `kill_random_30_one_copy_frozen.log`: 30 more with one log copy frozen
  per round. The three ran at the same time on the machine (separate ports). `kill_random_control_8fbdd8d.log`: 10 rounds on
  8fbdd8d (its RESULT line summed the cumulative counts of every round; the last round line has the distinct counts; fixed
  in the script after).
- `latency_fixed.log`, `latency_8fbdd8d.log`: `latency_mix.py`, 15 s per line, PUT modelled at 30 ms.
