# Equal durability: the configuration of the gate's comparison (written 2026-11-09, NOT RUN)

Coordinator, 2026-11-09: compare at equal durability, and run it once, after compaction 2 is wired and merged (not on
today's engine). Until then this is configuration only, untested. `step9/ci/gate.py` reads the newest local results
(`results/local-*/`), so the run that produces them must use this setup.

Why: the first local run compared our server, which acknowledges a write after 2 of 3 log copies have it, with
PostgreSQL and mongod writing to one disk with no copy. Equal durability means every target acknowledges a write only
when two copies on two machines have it.

Lines (same seed 42, same profile, more threads than `small` so p99 is stable: profile `standard`, 2,453 threads,
96,754 checkpoints, ESTIMATED by `generator.py --describe`):

1. PostgreSQL 16, primary + two synchronous standbys, each in its own container:
   - primary `postgresql.conf`: `wal_level = replica`, `synchronous_commit = on`,
     `synchronous_standby_names = 'ANY 1 (standby1, standby2)'`, `max_wal_senders = 10`;
   - standbys from `pg_basebackup -R`, `primary_conninfo` with `application_name=standby1` / `standby2`;
   - why ANY 1 of 2: a commit returns when the primary and one standby have flushed it, 2 copies of 3, the same rule as
     our log (2 of 3) and as a MongoDB majority of 3. Aurora writes 6 copies over 3 zones and acknowledges at 4 of 6
     (DECLARED by AWS, Aurora storage documentation): a quorum of copies too, but storage-level; `FIRST 2` (all three
     copies) would be stricter than both, so it is not the equal line.
2. mongod 7.0 replica set of 3 members, each in its own container; the saver's client with `w=majority&journal=true`.
3. Our server: the 3-zone log on (`--quaderno`, three `pq replica` containers), the cook and the spare, acknowledgement
   at 2 of 3 copies.
Second lines (single node, no copy): PostgreSQL alone, mongod alone, our server with the log off (S3 WAL only: one
PUT per group commit; durable in S3 Standard across zones, so it is NOT a weaker line than the others, it is slower).

Report per operation (save a step, resume, time travel, list, erase one end user, stored bytes per logical byte): the
winner and the ratio, first at equal durability, then single node. The gate (RULES.md 9) passes only if ours is not
worse than both others on every operation, p50 and p99, in the equal-durability lines.

Config: `equal-durability.json` (container names, ports, connection strings with the password from the environment).
