# Component: the waiter's read path (cameriere)

Owner session: "Owner · Waiter". Updated by the owner at every delivery.

## Why it matters
The first cold read of a customer must take one S3 trip (about 25 ms for one document, MEASURED M31 on real S3) and hot reads must match Atlas (targets in DECISIONS 2026-11-03: hot p50 under 2 ms, p99 under 10 ms; cold p50 about 30-60 ms, p99 under 150 ms).

## What exists today (2026-11-06)
- T1 "waiter tools" in the server, behind `--waiter-tools`: `mongo-server/src/prefetch.rs`, `src/maps.rs`, `src/counter.rs` (two-floor cache: RAM and local disk), `src/simstore.rs`, `src/meter.rs`. Branch attrezzi-cameriere, with the reviewer's tests on rev-attrezzi-cameriere; both now in `integration`.
- Measured (local, simulated S3): a cold read is one trip in a row (p50 1, p95 1-2 trips). One document: p50 25.6-29.9 ms. 11-60 scattered documents: one trip with 11-60 parallel requests, p50 63-103 ms (waits for the slowest).
- Maps (SST index blocks and filters plus collection-index blocks) weigh about one third of the data: 1 TB about 320-330 GB (ESTIMATED T1).
- Founder decision 2026-11-06, choice (b): only the stack maps (SST index blocks and filters, about 44 MB per GB) live on the waiter's NVMe; collection-index blocks live on S3 like data and are cached on demand. Cost: one extra S3 trip (about 25 ms) on the first read of a customer never seen.

## The three corrections of the reviewer (R-T1, APPROVED WITH CORRECTIONS), not done yet
1. Maps of closed or dropped databases never leave the cache (`src/maps.rs` only looks at open databases; `forget_ssts` only on manifest change). Forget a database's runs at dropDatabase; maps of long-closed databases must be able to leave RAM and then disk.
2. When maps fill the RAM, active data gets zero (`src/counter.rs` around line 436: data space is "RAM minus maps"). Test `rev_maps_leave_room_for_active_data` fails today in the judge: it must pass. Fix: a minimum RAM share for data, extra maps on disk.
3. The 1 TB sizing forgets the RAM of the on-disk map index (about 120-200 bytes per entry, 72,522 entries per GB: 8-14 GB per TB, ESTIMATED), and map loading has no limit (`src/maps.rs` `join_all`, `src/simstore.rs` `buffered(n)`: 651 requests in flight at 1 GB, MEASURED). Cap runs and requests in flight while loading maps.

## Tasks done
(none yet by this owner)
