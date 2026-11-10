# Compaction 2 step 6: results

Report: fenvix-brain `sessions/2026-11-09-c2-step6-counter/REPORT.md`. All local, $0.

In process (`cargo test --release -p tenant-bench --lib fx::counter_bench::<name> -- --ignored --nocapture`):
- `ram-1g-2000u-256k.json`, `ram-1g-2000u-1m.json`, `ram-1g-20000u-256k.json`: `counter_ram`, RAM per hot customer by slice mode (Off = whole blocks, Customer, Area), before and after trimming, cold and hot GETs. 1 GB, 400 hot customers.
- `reseal-1g-2000u-256k.json`: `counter_reseal`, hot misses in the window after a reseal: none, staged, unstaged (the control). Final code.
- `model-30k-trim2ms.txt`, `model-100k-trim2ms.txt`: step 3's model test with slices trimmed 2 ms after they are made.

Through the server (`bench/first-course/run.sh`, `SEALED=1`, 2 GB chat dataset, 5 ms simulated GETs, 1 MiB SlateDB block cache, images of 4 queries):
- `fc-load`: the load and the first seal (the base bucket was sealed again in `fc-reseal-base` after the customer-kind fix; its first seal's runs were deleted).
- `fc-short-area-final`, `fc-long-area`: short and long gap, area slices, final code (`--meta-cache-mb 2048`).
- `fc-short-off`: short gap with whole blocks (`--sealed-slices off`), the "before" of RAM per hot customer.
- `fc-short-area-before-area-bound`: an earlier short run, before the layout v2 area bound fix (index reads cut as customer slices, so more GETs and more RAM). Kept for the record.
- `fc-long-area-stalled`: the first long-gap attempt, stopped at gap day 13 when inserts fell to 1,000 a minute (64 MiB metadata cache, a loaded machine).

Each server run folder: `visits.txt` (summary), `result.json.gz` (every visit), `server.log`, `copy.txt`.
