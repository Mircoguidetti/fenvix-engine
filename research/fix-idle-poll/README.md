# fix-idle-poll: measurements (2026-11-13)

- `idle_measure.py <tag> <binary> <dbs> <secs> <mode>`: GETs and LISTs of idle open databases (serverStatus deltas, and
  the `WAITS_GET`/`WAITS_LIST` lines by object kind). Results in `out/<tag>.json`.
- Binaries: base = integration-4 c41559f; gate = d86232e9 (the gate of fix-box-first-trip alone); new3 = 6e572cb2
  (60 s holds); new4 = the head (120 s holds for SlateDB's compactor, worker and the floor observation, 60 s for the
  writer).
- `out/def600-*`: server defaults, 600 s idle, 1 and 20 databases.
- `t79/`: test_79's first-request tests on fix-box-first-trip's head 6f705ba with each change off in turn
  (`bisect-6f705ba.txt`), and on this head (`head-new4.txt`).
