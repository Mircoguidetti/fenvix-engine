# first-course-short (2026-11-14): a returning customer's first read after a pause, under one S3 trip

All local, $0. Branch `first-course-short` from night-head 36c6a3e.

## 1. What night-head 36c6a3e has

- The first course is `mongo-server/src/opening.rs`. It is behind `--first-course`, which is **off by default**. Related
  flags: `--first-course-queries 4`, `--first-course-idle-secs 1800`, `--first-course-days 14`, `--first-course-max-kb 256`
  and `--first-course-mb` (default 1/16 of `--memory-limit-mb`, so 32 MiB at 512 MiB). It is hooked in through `dbs.rs`
  (`write_hook`/`after_stage` on every staged batch; `first_course_begin`/`first_course_end` around `find_part`) and
  `cmd_data.rs` (the sorted/filtered find path).
- What it keeps per customer: the key-value pairs that the first N queries of a visit read. This is exact: writes are
  applied through `write_hook`, and the customer's regions are prefix-free. Openings are kept in RAM only.
- **Limit 1, RAM only.** `forget_db` drops a database's openings at every open and close of its tenant: idle close,
  wake, restart, lost lease, drop (dbs.rs 1123/1145/1265/1340/1404/1837). A customer who returns after a restart, deploy
  or idle close therefore finds no opening.
- **Limit 2, customer box first.** `customer_box_warm` (custbox.rs, default **on**) runs *before* `first_course_begin`
  (cmd_data.rs:144). So even a query that an opening answers first pays the box's whole-range read: one trip.
- **Limit 3, partial after writes.** After the customer writes between visits, the next first query is only partial.
  `coll.rs` `docs_by_span` fetches the documents with one range scan over `[min id, max id]`. New messages extend that
  span past the interval the opening recorded, so that scan misses (1 missed read per query). Measured on night-head in one
  process with no restart: 50/50 written customers partial, 0 served.
- Measured before:
  - `bench/first-course/results-2026-11-06` and `-11-07`: 2 GB chat data, 5 ms GETs, 1000 users, the same process between
    two visits, before custbox existed. Visit 2 went from 10.9 GETs to 0.00; first query p50 0.60 ms, p99 2.2 ms;
    0 wrong reads. Openings were 45 KB median (p95 49 KB), and 46.7 KB after a 14-day busy gap.
  - `research/c2-step6-counter` (sealed slices).
  - `research/integration-2/results/first-course-log-on` (stress with the log on, negative control found wrong reads).

## 2. The existing flag on the bench's access pattern

bench-strong-point `history.py` reads each measured customer for the first time after a restart of every container.
That is a first-ever read, so no opening can exist, with or without the flag. To measure the market's case (a customer
who comes back within days), `bench/strong-point-local/fc_local.py` adds visit 1: before the restart, every measured
customer is read once with the same query. The driver differs from the bench as follows. There is no Docker image or
MinIO in this sandbox, so the driver runs one local server (`--store local` wrapped by the engine's simulated S3,
`FENVIX_SIM_S3=20,0,100000`, which is round B at 20 ms per GET) at `--memory-limit-mb 512`, without the log copies. The
restart is SIGTERM followed by a fresh process, plus a drop of the page cache. Everything else is imported from
history.py: the plan generator, documents, index `{userId:1, ts:-1}`, the reader's timed region and the sha256 check.

Results on 2 GB (946,570 messages, 40,000 filler customers, `plan-2gb.json`), which is above the 512 MiB cap.
Each run starts from a copy of the same cleanly stopped bucket. Every customer had visit 1 before the restart. n=200
per row. "GETs" and "trips" are serverStatus deltas around each read. Binaries: `nh` = night-head 36c6a3e, `fc` = this
branch (`fc1` before the box-on-miss change, `fc2` final).

| arm (flags) | whole50 cold p50 / p99 ms | GETs | trips | newest10 cold p50 / p99 ms | GETs | trips | served | wrong |
|---|---|---|---|---|---|---|---|---|
| nh, first course off | 23.57 / 24.50 | 0.99 | 0.99 | 23.30 / 24.46 | 0.97 | 0.97 | 0 | 0 |
| nh, `--first-course --first-course-mb 64` | 23.71 / 27.00 | 0.99 | 0.99 | 23.35 / 24.69 | 0.97 | 0.97 | 0 | 0 |
| fc, no flags (regression check) | 23.75 / 26.84 | 0.99 | 0.99 | 23.42 / 26.15 | 0.97 | 0.97 | 0 | 0 |
| fc1, `... --first-course-dir` | 0.95 / 2.09 | 0 | 0 | 0.67 / 1.08 | 0 | 0 | 400/400 | 0 |
| **fc2, `--first-course --first-course-mb 64 --first-course-dir`** | **0.91 / 1.86** | **0** | **0** | **0.65 / 1.13** | **0** | **0** | 400/400 | 0 |
| fc1, `--first-course-dir`, default budget (32 MiB) | 23.24 / 25.93 | 0.59 | 0.59 | 1.14 / 24.40 | 0.47 | 0.47 | 187/400 | 0 |

A warm read in the same visit (the read repeated) is 0.7-1.1 ms in every arm.

On night-head with the flag on, visit 1 left 400 pending openings (27.9 MB, 70 KB on average). The restart forgot every
one of them, so the flag changes nothing on this access pattern. With 32 MiB, the budget holds about 190 of them.

Partial opening (`--visit1-newest10`: visit 1 reads the newest 10 of each h50 customer, visit 2 reads the whole history):

| arm | whole50 cold p50 / p99 ms | GETs | trips | partial |
|---|---|---|---|---|
| nh off | 23.78 / 25.91 | 0.99 | 0.99 | 0 |
| fc1 | 23.87 / 25.48 | 1.00 | 1.00 | 200 |
| fc2 (box due at the first miss) | 23.95 / 25.82 | 1.00 | 1.00 | 200 |

So a partly answered query still costs one trip, never more.

0.4 GB run, earlier, same driver (`small-*`): off 23.99 / 27.44 ms (3.53 GETs, 1 trip), night-head on 24.01 / 26.55,
prototype before the span fix 0.90 / 1.94 with 0 GETs.

Not measured here: the Docker line (fenvix-log with quaderno, MinIO behind s3delay), mongod and Postgres. Tonight's
numbers for those were Postgres 3.9 ms (whole50) and 1.3-1.5 ms (newest10), mongod 10 ms; ours 27-40 ms cold.
The prototype's 0.9 / 0.65 ms is from a different harness (no container network hop) and needs a rerun on the Docker line.

Load note: at both a 512 MiB and a 4 GiB cap, 2 and 4 parallel loaders stalled the local-store server at about 250k
documents (L0 full, writes not acknowledged within 30 s). One sequential loader went through with 0 retries.
This is outside this task, and it is recorded here only.

## 3. Prototype on `first-course-short`

Three changes, each small:

1. **Openings outlive a clean close** (`--first-course-dir DIR`, empty by default).
   - At a clean close, `close_by` parks the database's openings (pending ones installed, as the next visit would install
     them). It writes them to `DIR/<hex db>.fc` (temp file, fsync, rename, FNV checksum) only after the tenant stopped
     cleanly and the lease release landed. The file is stamped with the lease sequence that release wrote.
   - At the next open as owner, `restore_db` loads the file only if the acquire was not a takeover and the acquired lease
     has exactly the next sequence. That means nobody else can have owned the database, and so written to it, in
     between: it is `Leases::continuous` made to survive a restart. The file is deleted either way. A wrong sequence, a
     takeover, a failed release or a corrupt file means the openings are not used, and the customer reads storage as before.
   - While this process held the lease, as reader or writer, every write went through `write_hook`. So the restored
     openings are exact.
2. **The customer box does not run in front of a served query.** `first_course_begin` now runs first. When its session
   answers from an opening, `customer_box_warm` is skipped. It becomes due at the first read the opening cannot answer
   (`ReadView::box_on_miss`), so a partly answered query still makes one trip.
3. **Span reads defer to the opening.** When the opening knows every wanted document exactly, `docs_by_span` is skipped,
   and the point reads that follow are answered from the opening. This fixes limit 3.

Safety (`fc_local.py safety`, 200 h50 customers):
- A wrote to 50 customers (2 inserts each, plus deleting the newest message of 25), then restarted. The openings were
  restored and served 200/200, with 0 wrong.
- Then holder B started on the same store while A was stopped. B inserted into 50 other customers and deleted the
  newest message of 50 more. When A restarted, the restore was refused, with 0 wrong.
- **Negative control** (`FENVIX_FIRST_COURSE_SHORT_NEGATIVE_CONTROL=1`, which skips the stamp check): 100 wrong reads,
  exactly the 50 + 50 customers that B wrote. So the test does reach the case it guards.
- New unit test `opening::tests::kept_openings_survive_a_close_only_for_the_next_lease` covers a round trip with a write
  after the visit, refusal for another owner, a takeover or a missing lease, single use, and a corrupt file. All 12
  `opening::` tests pass.

## Design at scale: cost per customer, and at 1 TB

- What a returning customer's first read costs: one local read of its opening, from RAM in the prototype or NVMe in the
  design (0.1-0.3 ms). That replaces one S3 trip (20 ms in round B, 25-40 ms in tonight's Docker numbers).
- Size per customer: the opening is what the first N queries of a visit read, capped at 256 KB. Measured sizes: 45 KB
  median (p95 49 KB) for 4 queries on the 2026-11-06/07 chat set; here 70 KB on average over 200 whole-history (50 messages) and 200 newest-10 openings. By content, that is about 115 KB for a 50-message history and about 23 KB for a newest-10 page.
- RAM: the prototype keeps every opening in RAM inside `--first-course-mb` (32 MiB by default at a 512 MiB cap, so
  about 700 openings). That does not scale to 1 TB. **Design:** keep the bodies on local NVMe, keyed by (db, customer),
  in one append-only file per database with an index. RAM then holds only the index, about 40 B per customer, or 0 if
  the index is on disk too.
- The restore should become lazy. The prototype loads a database's whole file at wake: 47-57 ms for 25-28 MB, paid once
  per wake. The design reads one customer's opening on that customer's first query.
- Disk at 1 TB of messages: about 475 M messages of 2.1 KB.
  - The bound is returning customers × min(opening, customer's history). Only customers seen within `--first-course-days`
    (14) are kept.
  - Example 1: 1 M customers with about 1 MB of history each gives 45 GB, about 4.5% of the data.
  - Example 2: 10 M customers with about 100 KB each gives at most 450 GB if every one of them returns within 14 days.
    A budget with least-recently-visited eviction bounds it, for example 100 GB of NVMe for the 2.2 M most recent visitors.
  - A local NVMe GB costs about 1/10 of the S3 GETs it saves per month at one visit per customer per day. Rough: 1 GET
    = $0.0000004 and 1 GB-month of instance NVMe ≈ $0.05-0.08, so 45 GB ≈ $3/month against 30 M first reads/month. The
    money is small on both sides. The point is latency.
- Write cost: `write_hook` costs one map lookup per written key, 1.5 µs per message as measured on 2026-11-07. Persist
  costs one sequential write of the parked openings at a clean close.
- What it does not cover:
  - A first-ever read (history.py's case). The opening needs a previous visit. A write-time "tail" would cover customers
    who wrote recently but never read; that is not built.
  - A takeover or crash between visits. The openings are then refused and the customer pays the trip, as today.
  - Readers (non-owner servers). They do not keep openings.
