# Review of big-speed (2026-11-13): the reviewer's tests

Report: fenvix-brain `sessions/2026-11-13-review-big-speed/REPORT.md`.

- `src/fx/big_speed_tests.rs`, last two tests (`rvbs_*`):
  - `rvbs_retired_run_comes_back_into_the_floor_after_its_wipe`: FAILS on 13d531b (finding 4): a hold on the older
    version writes a retired run's blocks back into the disk floor after `forget_runs` wiped them.
  - `rvbs_single_flight_survives_a_cancelled_leader`: passes.
  - `cargo test --release -p tenant-bench --lib -- rvbs_ --nocapture`
- `test_r1_id_unique_in_box.py` (ctl style, needs an S3 at 127.0.0.1:9000): fix 4 keeps `_id` unique per customer
  (memtable, one batch, after a restart with the rows only in our runs), box check on and off. Passes.
- `test_r4_box_takeover.py`: P1 warms boxes, P2 takes over, updates, inserts, deletes and republishes, P1 takes back:
  0 wrong reads, box on and off. Passes. Note: with `--no-sealed-compactor` and the publish-drop test hook, keep
  `--sealed-runs 1000000 --sealed-reseal-every 1000000` (as test_50 does): otherwise the builder reseals from SlateDB
  rows the hook dropped and the documents vanish (a test set-up error, not an engine defect).
- `manydb.py PORT N MIB PID`: N databases written with MIB MiB each (finding 2: OOM at 1 GiB with 200 x 6 MiB).
- `bigbox.py load|read BUCKET MIB on|off`: one customer of MIB MiB in our runs, then one find_one on a cold server in
  a 1 GiB cgroup (finding 1: 1.5 GiB customer, box on, OOM-killed; box off, 199 MiB).
