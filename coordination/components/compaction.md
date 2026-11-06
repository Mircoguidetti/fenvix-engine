# Component: per-customer compaction (lavaggio)

Owner session: "Owner · Compaction". Updated by the owner at every delivery.

## Why it matters
It is the piece that makes Fenvix win on our market (data read one customer at a time, most customers idle): a cold read of one customer must take few S3 trips (target about 4 GETs per cold find; today's engine does 21-32, MEASURED M21). It is also our moat in the founder's sense: Atlas keeps B-tree pages ordered by key; to match it, it must change how it lays out data (PLAN-LAVAGGIO.md).

## What exists today (2026-11-06)
- Prototype, outside the server: `proto/lavaggio-per-cliente/` in fenvix-engine (branches lavaggio-per-cliente, clienti-grandi, freddo-s3; now also in `integration`). Tiered-levels scheduler plugged into SlateDB 0.17 as an external compactor (`src/livelli.rs`, `CompactorBuilder::with_scheduler_supplier`), customer-first keys ({customer}/{collection}/{key}), counting fake S3, the cache of M21. Measured in M26, M29, M31 (real S3 day, run 41999425773).
- Key numbers: grouping by customer does not add compaction work (amplification 3.91 vs 3.95, MEASURED M26). With customers of about 2 MB and the tuned reader: 0.92 GET per run at 8 GB, average 7.2 runs (MEASURED M29). Compaction CPU 22.5-24.5 s per GB written (MEASURED). lz4 2.41x, zstd-1 3.40x on synthetic documents (MEASURED M29). 20 TB price about 37% of Infinite (ESTIMATED M29).
- Paper check: TLA+ of the manifest swap, reads, forks and deletions, 23.9 million states, REGGE (MEASURED M28). 24 rules for the code: fenvix-engine `docs/tla-lavaggio/REGOLE-PER-IL-CODICE.md` (branch tla-lavaggio).
- In the server: the compaction that never finished is fixed on branch motore-correzioni (L1; now in `integration`): 0 failed compactions against 24 on main, no write stuck over 60 s against 32 (MEASURED, local disk, 2 vCPU). Not reviewed yet.
- In main: compaction warms the cache before publishing (PR #47).
- Known SlateDB 0.17 limit: the separate compactor (`CompactorBuilder::build`) rewrites SSTs with 4 KB blocks and no compression whatever the settings (M29). Use the distributed workers (`CompactionWorkerBuilder`) or a small change.

## Open, from L1's report
- A long merge interrupted (shutdown, drop, fork) restarts from zero: SlateDB saves progress only per 256 MB output file.
- No CPU cap on the compactions of all databases together.
- Writes over 10 s remain on 2 vCPU (276 in the run, longest 40.7 s).
- Nothing measured on real S3 in the server.

## Tasks done
(none yet by this owner)
