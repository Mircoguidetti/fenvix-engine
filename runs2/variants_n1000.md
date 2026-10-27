## Step 1 — Request trace of one cold wake (V0: writer open + get)

One tenant created by the existing load phase, woken once with tracing on. `start ms` is relative to the start of that phase (the get phase is shifted to follow the open phase). Bodies are fully read inside the timed window. *overlapped* = another call was in flight at some point during this call.

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.28 | 1.22 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.53 | 0.99 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 2.54 | 0.69 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 3.27 | 1.21 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 3.31 | 0.63 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 3.35 | 1.16 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 3.38 | 1.14 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 3.43 | 1.10 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 3.45 | 1.09 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 3.48 | 1.08 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 3.50 | 1.07 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 442 | 4.61 | 2.49 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 7.12 | 0.88 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 8.02 | 2.72 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 10.77 | 0.87 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 11.67 | 1.05 | no |
| 17 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 12.81 | 0.81 | no |
| 18 | open | other | GET | `gc/manifest.boundary` | 0 | 13.65 | 0.99 | no |
| 19 | open | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 14.67 | 2.61 | no |
| 20 | open | other | GET | `gc/manifest.boundary` | 0 | 17.30 | 1.17 | no |
| 21 | open | other (compactions/) | LIST | `compactions` | 0 | 18.49 | 1.14 | no |
| 22 | open | other (compactions/) | GET | `compactions/00000000000000000003.compactions` | 42 | 19.65 | 1.03 | no |
| 23 | open | other | GET | `gc/compactions.boundary` | 0 | 20.70 | 0.86 | no |
| 24 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000004.compactions` | 42 | 21.59 | 3.71 | no |
| 25 | open | other | GET | `gc/compactions.boundary` | 0 | 25.32 | 0.97 | no |
| 26 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000005.compactions` | 42 | 26.32 | 2.65 | no |
| 27 | open | other | GET | `gc/compactions.boundary` | 0 | 28.98 | 1.06 | no |
| 28 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 30.21 | 1.00 | yes |
| 29 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 31.01 | 0.30 | yes |
| 30 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 31.02 | 0.30 | yes |
| 31 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 31.12 | 0.19 | yes |
| 32 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 31.19 | 0.12 | yes |
| 33 | get | SST | GET_RANGE | `compacted/01M5SPQHQGH2E0RJD09X4SE6NM.sst` | 2444 | 31.41 | 1.66 | yes |
| 34 | get | other | GET | `gc/manifest.boundary` | 0 | 31.84 | 0.99 | yes |
| 35 | get | other | GET | `gc/compactions.boundary` | 0 | 32.45 | 0.49 | yes |
| 36 | get | other | GET | `gc/compactions.boundary` | 0 | 32.58 | 0.59 | yes |
| 37 | get | other | GET | `gc/manifest.boundary` | 0 | 32.71 | 0.63 | yes |
| 38 | get | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 32.96 | 0.60 | yes |
| 39 | get | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 33.18 | 0.86 | yes |
| 40 | get | SST | GET_RANGE | `compacted/01M5SPQHQGH2E0RJD09X4SE6NM.sst` | 3152 | 33.26 | 1.01 | yes |
| 41 | get | manifest | LIST | `manifest` | 0 | 33.35 | 0.98 | yes |
| 42 | get | other | GET | `gc/compactions.boundary` | 0 | 33.57 | 0.76 | yes |
| 43 | get | other | GET | `gc/manifest.boundary` | 0 | 34.06 | 0.27 | yes |

**Grouped by purpose (this wake):**

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| SST | GET_RANGE ×2 (2.7 ms) | 2 |
| WAL | HEAD ×9 (9.5 ms), PUT(create) ×1 (2.7 ms) | 10 |
| manifest | GET ×6 (4.0 ms), LIST ×2 (2.2 ms), PUT(create) ×2 (5.1 ms) | 10 |
| other | GET ×14 (11.4 ms) | 14 |
| other (compactions/) | GET ×4 (2.1 ms), LIST ×1 (1.1 ms), PUT(create) ×2 (6.4 ms) | 7 |

**This wake:** 43 requests; **19 strictly sequential** (never overlapped with another call), **24 parallel** (overlapped with at least one other call); 22 sequential steps after merging overlapping calls.

**Mean over 10 traced wakes of different tenants:** 43.1 requests, 19.5 strictly sequential, 23.6 parallel, 22.3 sequential steps.

**The matching close (same tenant)** — 9 requests:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | GET | `manifest/00000000000000000007.manifest` | 442 | 0.17 | 1.08 | yes |
| 2 | close | manifest | PUT(create) | `manifest/00000000000000000008.manifest` | 442 | 1.24 | 2.70 | yes |
| 3 | close | manifest | GET | `manifest/00000000000000000008.manifest` | 442 | 1.28 | 5.79 | yes |
| 4 | close | other | GET | `gc/manifest.boundary` | 0 | 3.95 | 0.71 | yes |
| 5 | close | manifest | GET | `manifest/00000000000000000009.manifest` | 0 | 7.09 | 0.76 | no |
| 6 | close | other | GET | `gc/manifest.boundary` | 0 | 7.87 | 0.66 | no |
| 7 | close | manifest | GET | `manifest/00000000000000000009.manifest` | 0 | 8.55 | 0.76 | no |
| 8 | close | other | GET | `gc/manifest.boundary` | 0 | 9.34 | 0.69 | no |
| 9 | close | WAL | LIST | `wal` | 0 | 10.04 | 1.69 | no |

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| WAL | LIST ×1 (1.7 ms) | 1 |
| manifest | GET ×4 (8.4 ms), PUT(create) ×1 (2.7 ms) | 5 |
| other | GET ×3 (2.1 ms) | 3 |


Close, mean over 10: 9.5 requests, 4.8 strictly sequential, 4.7 parallel, 5.9 sequential steps.

### Where an open-but-idle Db spends its requests (60 s window, V0)

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| manifest | GET ×71 (135.3 ms) | 71 |
| other | GET ×96 (186.6 ms) | 96 |
| other (compactions/) | GET ×25 (44.2 ms) | 25 |

<details><summary>Traces of one wake and its close for every other variant</summary>

#### V1a — `DbReader` ManagedCheckpoint (API default) + get

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.22 | 1.44 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.68 | 1.18 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 3.02 | 1.17 | no |
| 4 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 4.23 | 0.94 | no |
| 5 | open | other | GET | `gc/manifest.boundary` | 0 | 5.20 | 0.97 | no |
| 6 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 506 | 6.21 | 2.71 | no |
| 7 | open | other | GET | `gc/manifest.boundary` | 0 | 8.94 | 0.92 | no |
| 8 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 506 | 9.87 | 1.23 | no |
| 9 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 11.15 | 0.88 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 11.24 | 0.73 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 11.29 | 1.33 | yes |
| 12 | open | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 11.34 | 1.30 | yes |
| 13 | open | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 11.49 | 1.16 | yes |
| 14 | open | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 11.54 | 1.12 | yes |
| 15 | open | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 11.56 | 1.10 | yes |
| 16 | open | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 11.59 | 1.09 | yes |
| 17 | get | SST | GET_RANGE | `compacted/01M5SPTG7EN3QZ5AWHAG0JRGWJ.sst` | 2444 | 12.81 | 1.38 | yes |
| 18 | get | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 12.84 | 0.94 | yes |
| 19 | get | other | GET | `gc/manifest.boundary` | 0 | 13.81 | 0.98 | yes |
| 20 | get | SST | GET_RANGE | `compacted/01M5SPTG7EN3QZ5AWHAG0JRGWJ.sst` | 3151 | 14.35 | 1.36 | yes |
| 21 | get | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 14.81 | 0.83 | yes |
| 22 | get | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 14.87 | 1.32 | yes |
| 23 | get | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 14.92 | 1.27 | yes |
| 24 | get | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 14.96 | 1.24 | yes |
| 25 | get | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 14.98 | 1.21 | yes |
| 26 | get | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 15.03 | 1.16 | yes |
| 27 | get | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 15.06 | 1.13 | yes |
| 28 | get | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 15.10 | 1.09 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 0.53 | 0.88 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 1.43 | 0.86 | no |
| 3 | close | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 2.34 | 4.38 | no |
| 4 | close | other | GET | `gc/manifest.boundary` | 0 | 6.74 | 0.89 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| WAL | HEAD ×48 (80.6 ms) | 48 |
| manifest | GET ×6 (10.1 ms) | 6 |
| other | GET ×6 (6.6 ms) | 6 |

#### V1b — `DbReader` FollowLatest (no object-store writes) + get

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.29 | 1.51 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.82 | 1.13 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 2.99 | 0.97 | no |
| 4 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 4.00 | 0.97 | no |
| 5 | open | other | GET | `gc/manifest.boundary` | 0 | 5.00 | 0.81 | no |
| 6 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 5.83 | 1.45 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 5.90 | 1.47 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 5.95 | 1.37 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 5.98 | 1.35 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 6.01 | 1.33 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 6.13 | 1.22 | yes |
| 12 | open | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 6.16 | 1.19 | yes |
| 13 | open | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 6.23 | 1.14 | yes |
| 14 | get | SST | GET_RANGE | `compacted/01M5SPX77QRSC0TRWZ2XTD2RRY.sst` | 2444 | 7.45 | 1.42 | yes |
| 15 | get | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 8.59 | 1.01 | yes |
| 16 | get | SST | GET_RANGE | `compacted/01M5SPX77QRSC0TRWZ2XTD2RRY.sst` | 3152 | 8.99 | 2.22 | yes |
| 17 | get | other | GET | `gc/manifest.boundary` | 0 | 9.63 | 0.70 | yes |
| 18 | get | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 10.35 | 1.09 | yes |
| 19 | get | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 10.44 | 0.99 | yes |
| 20 | get | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 10.48 | 0.96 | yes |
| 21 | get | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 10.50 | 0.90 | yes |
| 22 | get | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 10.52 | 0.92 | yes |
| 23 | get | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 10.58 | 0.87 | yes |
| 24 | get | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 10.61 | 0.84 | yes |
| 25 | get | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 10.63 | 0.82 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| WAL | HEAD ×48 (75.1 ms) | 48 |
| manifest | GET ×6 (12.6 ms) | 6 |
| other | GET ×6 (6.9 ms) | 6 |

#### V1c — `DbReader` FollowLatest + skip_wal_replay + 1 h poll + get

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.33 | 2.12 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 2.47 | 1.31 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 3.82 | 0.82 | no |
| 4 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 4.67 | 0.74 | no |
| 5 | open | other | GET | `gc/manifest.boundary` | 0 | 5.44 | 0.96 | no |
| 6 | get | SST | GET_RANGE | `compacted/01M5SPZNC400NTMC41JACF3B1P.sst` | 2444 | 6.48 | 1.31 | yes |
| 7 | get | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 7.54 | 0.87 | yes |
| 8 | get | SST | GET_RANGE | `compacted/01M5SPZNC400NTMC41JACF3B1P.sst` | 3151 | 7.96 | 1.29 | yes |
| 9 | get | other | GET | `gc/manifest.boundary` | 0 | 8.43 | 0.91 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|

#### V2 — memtable-flush load + offline GC (min_age 0); writer open with in-Db GC off

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.30 | 1.69 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 2.01 | 1.78 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 1 | 3.83 | 1.09 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 4.97 | 1.82 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 5.08 | 1.57 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000005.sst` | 0 | 5.13 | 1.69 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000009.sst` | 0 | 5.18 | 1.76 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000017.sst` | 0 | 5.21 | 3.12 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000033.sst` | 0 | 5.24 | 1.59 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000065.sst` | 0 | 5.28 | 1.62 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000129.sst` | 0 | 5.34 | 1.58 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 442 | 8.52 | 3.62 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 12.16 | 1.04 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 13.22 | 2.99 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 16.24 | 1.03 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 17.30 | 1.00 | no |
| 17 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 18.37 | 1.25 | no |
| 18 | open | other | GET | `gc/manifest.boundary` | 0 | 19.66 | 0.97 | no |
| 19 | open | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 20.68 | 3.33 | no |
| 20 | open | other | GET | `gc/manifest.boundary` | 0 | 24.03 | 0.87 | no |
| 21 | open | other (compactions/) | LIST | `compactions` | 0 | 24.92 | 1.19 | no |
| 22 | open | other (compactions/) | GET | `compactions/00000000000000000003.compactions` | 42 | 26.13 | 1.12 | no |
| 23 | open | other | GET | `gc/compactions.boundary` | 1 | 27.26 | 1.19 | no |
| 24 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000004.compactions` | 42 | 28.47 | 5.27 | no |
| 25 | open | other | GET | `gc/compactions.boundary` | 0 | 33.76 | 1.29 | no |
| 26 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000005.compactions` | 42 | 35.08 | 2.65 | no |
| 27 | open | other | GET | `gc/compactions.boundary` | 0 | 37.75 | 0.99 | no |
| 28 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 38.95 | 1.02 | yes |
| 29 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 38.96 | 0.86 | yes |
| 30 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 39.87 | 1.19 | yes |
| 31 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 39.92 | 1.77 | yes |
| 32 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 39.93 | 1.44 | yes |
| 33 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 131072 | 40.00 | 1.66 | yes |
| 34 | open | other | GET | `gc/compactions.boundary` | 0 | 41.08 | 0.85 | yes |
| 35 | open | other | GET | `gc/manifest.boundary` | 0 | 41.40 | 1.58 | yes |
| 36 | open | other | GET | `gc/compactions.boundary` | 0 | 41.71 | 1.44 | yes |
| 37 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 209338 | 41.79 | 1.19 | yes |
| 38 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 41.95 | 1.06 | yes |
| 39 | open | other | GET | `gc/compactions.boundary` | 0 | 43.03 | 0.20 | yes |
| 40 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 43.16 | 0.07 | yes |
| 41 | get | SST | GET_RANGE | `compacted/01M5SQ2F9EG398586EDBM75Y6K.sst` | 2444 | 43.34 | 1.25 | yes |
| 42 | get | other | GET | `gc/manifest.boundary` | 0 | 44.35 | 0.87 | yes |
| 43 | get | SST | GET_RANGE | `compacted/01M5SQ2F9EG398586EDBM75Y6K.sst` | 3152 | 44.72 | 1.12 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | PUT(create) | `manifest/00000000000000000008.manifest` | 442 | 0.27 | 2.77 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 3.06 | 0.88 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| manifest | GET ×71 (110.9 ms) | 71 |
| other | GET ×96 (120.4 ms) | 96 |
| other (compactions/) | GET ×25 (46.5 ms) | 25 |

#### V3 — V2 + 1 h manifest/compactor poll intervals

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.27 | 1.26 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.55 | 1.13 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 1 | 2.73 | 0.89 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 3.66 | 1.32 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 3.77 | 1.22 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000005.sst` | 0 | 3.82 | 1.50 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000009.sst` | 0 | 3.87 | 1.14 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000017.sst` | 0 | 3.92 | 1.02 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000033.sst` | 0 | 3.97 | 1.05 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000065.sst` | 0 | 4.00 | 0.96 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000129.sst` | 0 | 4.03 | 1.57 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 442 | 5.64 | 3.25 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 8.90 | 1.21 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 10.14 | 3.05 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 13.22 | 0.92 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 14.16 | 1.18 | no |
| 17 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 15.42 | 0.90 | no |
| 18 | open | other | GET | `gc/manifest.boundary` | 0 | 16.36 | 0.99 | no |
| 19 | open | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 17.39 | 3.23 | no |
| 20 | open | other | GET | `gc/manifest.boundary` | 0 | 20.64 | 1.00 | no |
| 21 | open | other (compactions/) | LIST | `compactions` | 0 | 21.67 | 1.21 | no |
| 22 | open | other (compactions/) | GET | `compactions/00000000000000000003.compactions` | 42 | 22.90 | 1.01 | no |
| 23 | open | other | GET | `gc/compactions.boundary` | 1 | 23.93 | 1.14 | no |
| 24 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000004.compactions` | 42 | 25.10 | 2.70 | no |
| 25 | open | other | GET | `gc/compactions.boundary` | 0 | 27.82 | 0.91 | no |
| 26 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000005.compactions` | 42 | 28.75 | 3.19 | no |
| 27 | open | other | GET | `gc/compactions.boundary` | 0 | 31.95 | 1.16 | no |
| 28 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 33.27 | 1.09 | yes |
| 29 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 33.28 | 1.25 | yes |
| 30 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 34.27 | 0.95 | yes |
| 31 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 131072 | 34.37 | 2.49 | yes |
| 32 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 34.44 | 1.35 | yes |
| 33 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 34.44 | 1.11 | yes |
| 34 | open | other | GET | `gc/manifest.boundary` | 0 | 35.26 | 0.73 | yes |
| 35 | open | other | GET | `gc/compactions.boundary` | 0 | 35.57 | 1.29 | yes |
| 36 | open | other | GET | `gc/compactions.boundary` | 0 | 35.80 | 0.76 | yes |
| 37 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 36.58 | 0.77 | yes |
| 38 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 36.88 | 0.71 | yes |
| 39 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 209338 | 37.00 | 1.50 | yes |
| 40 | open | other | GET | `gc/compactions.boundary` | 0 | 37.36 | 1.17 | yes |
| 41 | open | other | GET | `gc/manifest.boundary` | 0 | 37.62 | 1.19 | yes |
| 42 | get | SST | GET_RANGE | `compacted/01M5SQ591132ARN8Z43FS77ZRF.sst` | 2444 | 38.92 | 1.28 | no |
| 43 | get | SST | GET_RANGE | `compacted/01M5SQ591132ARN8Z43FS77ZRF.sst` | 3150 | 40.34 | 1.26 | no |
| 44 | get | SST | GET_RANGE | `compacted/01M5SQ591132ARN8Z43FS77ZRF.sst` | 3151 | 41.80 | 1.24 | no |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | PUT(create) | `manifest/00000000000000000008.manifest` | 442 | 0.28 | 2.84 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 3.14 | 1.02 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| other | GET ×11 (14.7 ms) | 11 |
| other (compactions/) | GET ×11 (22.8 ms) | 11 |

#### V4 — V3 + in-Db compactor off (compaction would have to run offline)

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.24 | 1.09 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000004.manifest` | 434 | 1.35 | 0.90 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 1 | 2.27 | 0.91 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 3.21 | 2.08 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 3.27 | 1.98 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000005.sst` | 0 | 3.31 | 1.78 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000009.sst` | 0 | 3.36 | 1.90 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000017.sst` | 0 | 3.38 | 0.68 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000033.sst` | 0 | 3.40 | 1.70 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000065.sst` | 0 | 3.42 | 1.90 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000129.sst` | 0 | 3.44 | 1.68 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000005.manifest` | 434 | 5.36 | 3.66 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 9.03 | 0.98 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 10.04 | 2.93 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 13.01 | 0.98 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 14.02 | 0.86 | no |
| 17 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 15.02 | 0.78 | yes |
| 18 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 15.03 | 1.00 | yes |
| 19 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 15.09 | 0.84 | yes |
| 20 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 131072 | 15.81 | 2.41 | yes |
| 21 | open | other | GET | `gc/manifest.boundary` | 0 | 15.95 | 2.23 | yes |
| 22 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 209338 | 18.33 | 0.97 | no |
| 23 | get | SST | GET_RANGE | `compacted/01M5SQ82QWKRAG8REYE7V8522D.sst` | 2444 | 19.37 | 1.12 | no |
| 24 | get | SST | GET_RANGE | `compacted/01M5SQ82QWKRAG8REYE7V8522D.sst` | 3150 | 20.68 | 1.24 | no |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 434 | 0.65 | 2.51 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 3.17 | 1.23 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|

</details>

## Step 2 — Variant comparison (1000 fresh tenants per variant, 100 cold wakes each, MinIO on localhost)

| | Variant | Sequential steps / wake | Strictly seq. / parallel / total req. per wake | Wake: PUT/LIST-tier · GET-tier | Close: PUT/LIST-tier · GET-tier | Wake p50 / p99 (ms) | Idle-open req. / 60s (PUT/LIST · GET) | Storage B/tenant (raw → after cleanup) |
|---|---|---|---|---|---|---|---|---|
| **V0** | writer `Db::open` + get, default settings (current behaviour) | **22.3** | 19.5 / 23.6 / 43.1 | 7.97 · 35.67 | 2.21 · 7.39 | 35.9 / 46.2 | 192 (0 · 192) | 424616 → no cleanup run |
| **V1a** | `DbReader` ManagedCheckpoint (API default) + get | **10.1** | 8.0 / 15.4 / 23.4 | 2.00 · 23.54 | 1.00 · 5.86 | 16.9 / 26.6 | 60 (0 · 60) | 424614 → no cleanup run |
| **V1b** | `DbReader` FollowLatest (no object-store writes) + get | **7.4** | 5.1 / 16.1 / 21.2 | 1.00 · 21.12 | 0.00 · 3.18 | 10.5 / 17.4 | 60 (0 · 60) | 424620 → no cleanup run |
| **V1c** | `DbReader` FollowLatest + skip_wal_replay + 1 h poll + get | **6.1** | 5.1 / 4.0 / 9.1 | 1.00 · 8.28 | 0.00 · 0.01 | 9.2 / 14.4 | 0 (0 · 0) | 424611 → no cleanup run |
| **V2** | memtable-flush load + offline GC (min_age 0); writer open with in-Db GC off | **22.9** | 20.3 / 22.7 / 43.0 | 7.00 · 36.39 | 1.00 · 1.00 | 40.6 / 69.1 | 192 (0 · 192) | 424590 → **423610** |
| **V3** | V2 + 1 h manifest/compactor poll intervals | **23.0** | 20.5 / 22.6 / 43.1 | 7.00 · 36.44 | 1.00 · 1.01 | 37.0 / 60.7 | 22 (0 · 22) | 424590 → **423668** |
| **V4** | V3 + in-Db compactor off (compaction would have to run offline) | **12.9** | 10.9 / 13.5 / 24.4 | 3.00 · 21.51 | 1.00 · 1.00 | 22.7 / 34.3 | 0 (0 · 0) | 424342 → **423639** |

PUT/LIST-tier = PUT + COPY + LIST (multipart included); GET-tier = GET + range GET + HEAD. Per-wake figures are exact means over 100 untraced wakes of distinct tenants; sequential-step figures are means over 10 separate traced wakes (tracing buffers response bodies, so latency is taken only from the untraced wakes).

Offline maintenance (not part of any wake):

| Variant | Load time | Offline GC requests (all 1000 tenants) | per tenant PUT/LIST · GET · DELETE | Final objects/tenant |
|---|---|---|---|---|
| V0 | 19.7 s | 0 | 0.00 · 0.00 · 0.00 | 11.95 |
| V1a | 22.5 s | 0 | 0.00 · 0.00 · 0.00 | 11.49 |
| V1b | 20.6 s | 0 | 0.00 · 0.00 · 0.00 | 11.31 |
| V1c | 22.3 s | 0 | 0.00 · 0.00 · 0.00 | 11.25 |
| V2 | 17.3 s | 43001 | 10.00 · 27.00 · 6.00 | 7.77 |
| V3 | 18.2 s | 43000 | 10.00 · 27.00 · 6.00 | 7.77 |
| V4 | 13.2 s | 36002 | 9.00 · 24.00 · 3.00 | 5.60 |

## Step 3 — Projections (NOT measurements)

Projected wake latency = sequential steps × per-request latency. Assumptions: **5 ms** per request (S3 Express One Zone) and **30 ms** (S3 Standard). This ignores transfer time, request-size effects and tail latency, so it is a rough lower bound; parallel requests are assumed to cost one step.

Break-even idle time = (cost of wake + cost of close) ÷ (cost per second of an open-but-idle Db), at S3 Standard prices (PUT/COPY/POST/LIST $0.005 per 1,000; GET $0.0004 per 1,000). If a tenant will stay idle longer than this, closing it and waking it later is cheaper than leaving it open.

| Variant | Sequential steps | Projected wake @ 5 ms | Projected wake @ 30 ms | Wake + close cost | Idle-open cost | Break-even idle time |
|---|---|---|---|---|---|---|
| V0 | 22.3 | 112 ms | 669 ms | $0.00006812 | $3.32/month | 53.2 s |
| V1a | 10.1 | 50 ms | 303 ms | $0.00002676 | $1.04/month | 1.1 min |
| V1b | 7.4 | 37 ms | 222 ms | $0.00001472 | $1.04/month | 36.8 s |
| V1c | 6.1 | 30 ms | 183 ms | $0.00000832 | $0.00000000/month | never (no idle requests) |
| V2 | 22.9 | 114 ms | 687 ms | $0.00005496 | $3.32/month | 42.9 s |
| V3 | 23.0 | 115 ms | 690 ms | $0.00005498 | $0.38016000/month | 6.2 min |
| V4 | 12.9 | 64 ms | 387 ms | $0.00002900 | $0.00000000/month | never (no idle requests) |
