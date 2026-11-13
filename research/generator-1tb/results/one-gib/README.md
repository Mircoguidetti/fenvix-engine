# 1 GiB measurements (seed 2001, 512 customers, 3,209,620 documents), 2026-11-12, 4-vCPU sandbox, MinIO local

Phase timers of the one-core generator (integration-4's ballast.rs with timers, one write set per customer):
generation 115.4 s: documents 16.8, insert code 70.2, key mapping and spill 17.3, sort 0.9, sealed runs 10.2;
wall 151 s with E1's wait (about 30 s) and the publish (5.2 s). 10,111,124 shared rows, of which 3,209,620 are the
insertion counter rewritten with every document.
Write sets of 1,000 documents instead of one per customer: insert code 58.7 s, generation 99.6 s.

Same rows as the one-core generator (fx_rows_digest, rows_digest_without_collection_uuids):
- one-core generator (bucket gen2):            9d5c0d25dc0bf5e995bf1aac878817bb, 13,320,758 rows, 3 groups
- new generator, one slice, 1 writer (gen9):   9d5c0d25dc0bf5e995bf1aac878817bb, 13,320,758 rows, 4 groups
- `fx_rows_digest ... gen2 app_data_b01 gen9`: 3 rows differ, all meta/cuuid/{eventi,messaggi,ordini} (random UUIDs)
- new generator, 2 slices: 4 writers (gen8) and 1 writer (gen10): 43562b7a64b97866c309375c0753b22d (both)

The writers' private catalog (A/B, one writer, 2 slices, insert code seconds):
- scratch copy without an envelope marker: 74.2, 74.6, 76.4 (also with the createIndexes commands on top), 75.3
  (closed and reopened), 76.0 (on the real store under a prefix); real catalog: 60.5, 61.8
- point read of an absent key: real 3.46-4.24 us, scratch without a marker 7.23-7.73 us (also as a writer)
- the scratch copy with its own marker (E1's wait): 3.89 us; insert code 58.8 s; 10.16 MB/s per writer
