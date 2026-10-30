# Fenvix compatibility with MongoDB 7.0

This page is for developers who want to move a MongoDB application to Fenvix. It lists what was compared with a real MongoDB server, what works with a difference, and what does not work. It is generated from an audit of the engine; it is not marketing material, and it says "not measured" where the audit has no data (performance and price are not covered at all).

> **Audit report date:** 2026-10-27. **Reference server:** MongoDB (mongod) 7.0.45. **Drivers tested:** pymongo 4.19 and Node.js mongodb 7.7. **Engine build:** the report header names commit `ff2ad26` (branch `compat-indexes`), but the statuses below were updated after that header was written and the data does not record the build they were measured on. See [Audit date and engine build](#audit-date-and-engine-build).

## How to read this page

Every feature in the audit has one of four statuses:

| Status | Meaning |
|---|---|
| **Supported** | A comparison test against real MongoDB 7.0 passes, and none fails: the same request gives the same result on MongoDB and on Fenvix. |
| **Partial** | It works, with a difference from MongoDB that is stated next to the feature. |
| **Not tested** | The code exists, but no comparison test against MongoDB exists. This is not a claim that it works. |
| **Not supported** | The engine rejects the feature with an error, or accepts it and ignores it. |

A feature counts here when it is something an application can reach through a driver. Features that were left out on purpose are listed [below](#left-out-on-purpose) and are not in any count. The complete list, one row per feature with its status and the id of its evidence, is in [`compatibility.json`](compatibility.json).

## Overall summary

Of **1268** features counted, **1237** are Supported (97.5%), 15 are Partial, 16 are Not tested and 0 are Not supported.

| Status | Features | Share |
|---|---:|---:|
| Supported | 1237 | 97.5% |
| Partial | 15 | 1.1% |
| Not tested | 16 | 1.2% |
| Not supported | 0 | 0.0% |
| **All counted features** | **1268** | |

Shares are truncated to one decimal place, never rounded up, so they can add up to slightly less than 100%.

The audit's own tool (`compat/coverage.py`) counts a feature as "covered" if it passes tests, and reports **1253 of 1268 = 98.8%**. That figure includes 16 features that have no comparison against MongoDB at all (MongoDB 7.0 Community has no such feature, for example the Atlas Search commands). This page does not call those Supported; they are Not tested.

### By area

| Area | Features | Supported | Partial | Not tested | Not supported |
|---|---:|---:|---:|---:|---:|
| [Queries and operators](#queries-and-operators) | 135 | 135 | 0 | 0 | 0 |
| [Aggregation](#aggregation) | 269 | 266 | 0 | 3 | 0 |
| [Indexes](#indexes) | 54 | 51 | 0 | 3 | 0 |
| [Data types](#data-types) | 52 | 51 | 1 | 0 | 0 |
| [Writes and bulk operations](#writes-and-bulk-operations) | 70 | 70 | 0 | 0 | 0 |
| [Transactions and concurrency](#transactions-and-concurrency) | 37 | 37 | 0 | 0 | 0 |
| [Change streams](#change-streams) | 27 | 27 | 0 | 0 | 0 |
| [GridFS](#gridfs) | 17 | 17 | 0 | 0 | 0 |
| [Collation and sorting](#collation-and-sorting) | 160 | 146 | 14 | 0 | 0 |
| [Authentication and security](#authentication-and-security) | 24 | 24 | 0 | 0 | 0 |
| [Drivers and protocol](#drivers-and-protocol) | 305 | 295 | 0 | 10 | 0 |
| [Admin commands](#admin-commands) | 118 | 118 | 0 | 0 | 0 |
| **All areas** | **1268** | **1237** | **15** | **16** | **0** |

## Areas

### Queries and operators

Query operators, projection, update operators, sort and hint, cursors, the find and count family of commands, and server-side JavaScript ($where, $function, $accumulator).

**All 135 features are Supported.**

### Aggregation

Pipeline stages, expressions, accumulators, window functions, the aggregate command and mapReduce.

**266 of 269 features are Supported** (98.8%); 0 Partial, 3 Not tested, 0 Not supported.

Notable gaps:

- **Not tested (3):** `$listSearchIndexes`, `$search`, `$searchMeta`

### Indexes

Index types and properties, and the commands that create, list and drop indexes, including search-index commands.

**51 of 54 features are Supported** (94.4%); 0 Partial, 3 Not tested, 0 Not supported.

Notable gaps:

- **Not tested (3):** `createSearchIndexes`, `dropSearchIndex`, `updateSearchIndex`

### Data types

BSON types, binary subtypes and the order in which values of different types compare.

**51 of 52 features are Supported** (98.0%); 1 Partial, 0 Not tested, 0 Not supported.

Notable gaps:

- **Partial (1):** `binData subtype 7 (Compressed time series data)`

### Writes and bulk operations

insert, update, delete, findAndModify, bulk writes and write concern.

**All 70 features are Supported.**

### Transactions and concurrency

Multi-document transactions, sessions, read concern and read preference.

**All 37 features are Supported.**

### Change streams

Change streams, their options and the events they deliver.

**All 27 features are Supported.**

### GridFS

The GridFS file-storage API of the drivers.

**All 17 features are Supported.**

### Collation and sorting

Collation options and the collation locales (language-specific sort order).

**146 of 160 features are Supported** (91.2%); 14 Partial, 0 Not tested, 0 Not supported.

Notable gaps:

- **Partial (14):** `backwards`, `normalization`, `cs@collation=search`, `de@collation=search`, `en_US_POSIX`, `haw`, `kl`, `kl@collation=search`, `lv`, `se`, and 4 more

### Authentication and security

Authentication mechanisms, user and role management commands.

**All 24 features are Supported.**

### Drivers and protocol

The wire protocol, TLS, compression, Stable API, retryable writes and the members of the pymongo and Node.js driver APIs.

**295 of 305 features are Supported** (96.7%); 0 Partial, 10 Not tested, 0 Not supported.

Notable gaps:

- **Not tested (10):** `Collection.createSearchIndex`, `Collection.createSearchIndexes`, `Collection.dropSearchIndex`, `Collection.listSearchIndexes`, `Collection.updateSearchIndex`, `pymongo.collection.Collection.create_search_index`, `pymongo.collection.Collection.create_search_indexes`, `pymongo.collection.Collection.drop_search_index`, `pymongo.collection.Collection.list_search_indexes`, `pymongo.collection.Collection.update_search_index`

### Admin commands

Collection and database management, diagnostics, server administration, plan cache and replica-set status commands.

**All 118 features are Supported.**

## Not supported yet, and used by real applications

**No counted feature currently has the status Not supported.** The gaps that remain are the Partial features below, ranked by how many of the 10 scanned applications use the feature. Features with status Not tested are not ranked here; they are listed in their areas above.

| # | Gap | Features | Status | Repositories using it (of 10) | Mentions in application code |
|---:|---|---:|---|---|---|
| 1 | Collation (options and locales) | 14 | 14 partial | 5 of 10 | 57 |
| 2 | Bson binary subtypes | 1 | 1 partial | not measured | not measured |

- Collation (options and locales): usage counted from the scan entry `collation` (compat/scan_extra.json). The count is the highest among the 14 features of this group.

### What the usage scan can and cannot tell

The scan looked at 10 public repositories (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform), scanned on 2026-10-26, and counted how many of them use each MongoDB feature in application code (tests excluded). It is text matching, not a run of the applications. Limits, as written in the scan report:

- **Scope.** Files with extensions `.js .jsx .mjs .cjs .ts .tsx .py .java .kt .scala .groovy .go .rb .php .cs .vue .svelte .coffee`. Excluded: `node_modules`, `vendor`, `third_party`, `dist`, `build`, `out`, `target`, `.next`, `coverage`, virtualenvs, `.git`, generated folders, lockfiles, `*.min.*`, bundles, source maps, files over 1 MB and files whose first lines exceed 3,000 characters. **Test files** (paths under `test(s)`, `__tests__`, `spec`, `e2e`, `cypress`, `fixtures`, `mocks`, `src/test`, `*.test.*`, `*.spec.*`, `test_*.py`, `*Test.java`, ...) are counted separately and never contribute to the main metric.
- **Over-counting:** string matching cannot tell a MongoDB query from a similarly shaped literal (for example the same operator names in a query-builder that merely emits them, a JSON fixture inside a source file, or API documentation strings). Repositories that *translate* queries (Parse Server, Payload's database adapters) show many operators that applications built on them would not use directly.
- **Under-counting:** Java/Kotlin code that builds queries with Spring Data's `Criteria`/`Query` API or Python code that uses ODM query builders (Appsmith and edx-platform show few `$` strings for this reason); operators built from variables or concatenated strings, queries in `.json`/`.yml`/`.graphql` files, indexes defined through helper functions, and any language not in the extension list.
- **Same name, two roles.** `$min`, `$max`, `$push`, `$addToSet`, `$set` and `$unset` exist both as update operators and as aggregation accumulators/stages, and their status differs. The role is guessed from the surrounding ~800 characters (an aggregation context with `$group`/`aggregate`/`pipeline` versus an `update`/`$inc`/`upsert` context). This is a heuristic and can misclassify.
- **Several repositories are libraries or platforms** (Parse Server, Payload, Rocket.Chat, Novu), so their usage reflects what a framework needs, which is a fair proxy for what a MongoDB-compatible server must support to host them.

In short: ten repositories is a small sample, several of them are frameworks or platforms rather than applications, and a count of "3 of 10" means three repositories mention the feature, not that three applications depend on it. Features the scans do not look for are marked *not measured*, not zero.

## Left out on purpose

525 items of the MongoDB 7.0 surface were left out of every count above, by a product decision recorded in the audit. They are things an application does not call, or things that belong to running MongoDB itself. Fenvix is a managed service, so replica-set administration, sharding and server-to-server traffic are the operator's side, not something your application code issues.

| Left out | Items | What it is |
|---|---:|---|
| A1 Client-only driver members | 269 | Properties, option holders and helpers that live inside the driver and never send a request (for example a cursor's `closed` flag). There is nothing for a server to implement. |
| A1 Client-side encryption helpers | 47 | Field-level encryption helpers (libmongocrypt, key management) run inside the driver, on the client. |
| A1 Internal server-to-server commands | 133 | Commands that MongoDB's own servers send to each other (config servers, shards, donors). Applications cannot call them. |
| A2 Replication | 15 | Replica-set administration and internal replication commands, not issued by application code. |
| A2 Sharding | 40 | Sharding administration (adding shards, balancer, zones, resharding). |
| A3 Audit (Enterprise/Atlas) | 3 | Auditing is an Enterprise/Atlas feature of MongoDB. |
| A3 Enterprise/Atlas-only authentication mechanisms | 4 | Kerberos, LDAP, OIDC and AWS IAM sign-in exist only in MongoDB Enterprise or Atlas. |
| A3 Removed in 5.1 / 8.0-only | 3 | The client-level `bulkWrite` needs the MongoDB 8.0 `bulkWrite` command, which MongoDB 7.0 does not have; `OP_INSERT` was removed in MongoDB 5.1. |
| A3 Undocumented commands | 7 | Commands that mongod 7.0.45 lists but the 7.0 manual does not document, and that no driver API uses. |
| A3 security.javascriptEnabled (startup option) | 1 | A startup option of mongod, not reachable through a driver. |
| A4 Not producible / not in 7.0 Community | 2 | `$vectorSearch` (Atlas Vector Search only) and an error label that cannot be induced on either server. |
| Purely local driver member (founder-allowed, `apply_local_exclusions.py`) | 1 | A second `connect()` on an already connected Node.js client, which sends no request. |
| **Total** | **525** | |

Every other row is counted, including admin commands such as `shutdown`, `setParameter` and `fsync`.

## Known differences from MongoDB

The audit evidence records no workaround for any of these.

Every Partial feature (15):

| Feature | Area | Difference from MongoDB | Evidence id |
|---|---|---|---|
| `binData subtype 7 (Compressed time series data)` | Data types | A comparison probe (compat.bson.binsubtype_7) shows a difference from MongoDB; the audit data does not say in words what differs. | `binsub:7` |
| `backwards` | Collation and sorting | Backwards is available through the locale only (fr, fr_CA): any other locale with backwards different from its default is refused with BadValue, because the ICU4X collator has no such switch | `coll:backwards` |
| `normalization` | Collation and sorting | Accepted, validated and listed like mongod; the ICU4X collator always normalizes, so strings that are not in FCD form compare as canonically equivalent also with normalization:false (g07.options matrix passes: its data are FCD) | `coll:normalization` |
| `cs@collation=search` | Collation and sorting | Results differ from MongoDB 7.0.45: the 'search' collation type of cs differs between CLDR 29 (ICU 57) and the CLDR of ICU4X | `locale:cs@collation=search` |
| `de@collation=search` | Collation and sorting | Results differ from MongoDB 7.0.45: the 'search' collation type of de differs between CLDR 29 (ICU 57) and the CLDR of ICU4X | `locale:de@collation=search` |
| `en_US_POSIX` | Collation and sorting | Results differ from MongoDB 7.0.45: ICU 57 tailors en_US_POSIX to ASCII code-point order; the ICU4X collation data has no such tailoring (root order is used) | `locale:en_US_POSIX` |
| `haw` | Collation and sorting | Results differ from MongoDB 7.0.45: Hawaiian tailoring is missing from the ICU4X data used by this build | `locale:haw` |
| `kl` | Collation and sorting | Results differ from MongoDB 7.0.45: Kalaallisut tailoring is missing from the ICU4X data used by this build | `locale:kl` |
| `kl@collation=search` | Collation and sorting | Results differ from MongoDB 7.0.45: same cause as locale kl | `locale:kl@collation=search` |
| `lv` | Collation and sorting | Results differ from MongoDB 7.0.45: ICU 57's Latvian tailoring (y sorting with i) is not in the newer CLDR data of ICU4X, which orders lv like root | `locale:lv` |
| `se` | Collation and sorting | Results differ from MongoDB 7.0.45: Northern Sami tailoring (a-ring, a-umlaut, o-umlaut after z) is missing from the ICU4X data used by this build | `locale:se` |
| `se@collation=search` | Collation and sorting | Results differ from MongoDB 7.0.45: same cause as locale se | `locale:se@collation=search` |
| `sk@collation=search` | Collation and sorting | Results differ from MongoDB 7.0.45: the 'search' collation type of sk differs between CLDR 29 (ICU 57) and the CLDR of ICU4X | `locale:sk@collation=search` |
| `smn` | Collation and sorting | Results differ from MongoDB 7.0.45: Inari Sami tailoring is missing from the ICU4X data used by this build | `locale:smn` |
| `smn@collation=search` | Collation and sorting | Results differ from MongoDB 7.0.45: same cause as locale smn | `locale:smn@collation=search` |

## What was tested and what was not

- **Reference server:** MongoDB mongod 7.0.45. A feature is Supported only if the same request gives the same result on that server and on Fenvix.
- **Performance and price:** not measured. This page says nothing about either.
- Replica-set behaviour was tested on single-node replica sets only (no secondaries, no election, no majority across nodes).
- **Atlas-only features** (`$search`, `$searchMeta`, `$vectorSearch`, Atlas Search index commands) cannot be differential-tested: mongod 7.0 itself rejects them. The engine's `$search` subset has no test against mongo:7.0.
- **Drivers.** Only pymongo 4.19 and Node.js `mongodb` 7.7 were run. The inventory has no rows for the Java, Go, C#, Rust or other drivers; other language drivers that use the same server commands are expected to behave alike but that was not tested.
- **"Covered" is not "exhaustively correct".** It means at least one passing differential test exercises the row and none fails; it does not mean every argument combination was compared.
- **Docs v7.0 pages for later versions.** The v7.0 site also serves pages for `$rankFusion`, `$score`, `$scoreFusion`, `$querySettings`, `$listClusterCatalog`, `$toUUID`, `$serializeEJSON`, `$deserializeEJSON`, `$minMaxScaler`; mongod 7.0.45 rejects all of them ("Unrecognized pipeline stage" / "Unknown expression"), so they are not in the inventory. The query operator `$comment` is accepted by mongod 7.0.45 but has no 7.0 manual page, so it is not in the inventory either.
- **Client-only driver members** (properties, option holders, SDAM views, enum constants, event-emitter plumbing) are proposed as exclusions by rule (`compat/tools/driver_map.py`); each one was not individually checked for a hidden server dependency beyond that mapping.

## Audit date and engine build

- Audit report dated **2026-10-27**.
- The inventory file the numbers come from was last changed on **2026-10-28** (repository commit `5df2720`).
- Engine commit named in the audit report header: `ff2ad26` (branch `compat-indexes`). The header names this build, but the inventory has been updated since; the build the current statuses were measured on is not recorded in the data.
- This page and `compatibility.json` are generated by `tools/compat-page/build.py` from `compat/inventory.csv`. Nothing on this page is edited by hand. To regenerate it after a new audit run, see [`compatibility-regenerate.md`](compatibility-regenerate.md).

### Notes on the source data

Where the audit files disagree with each other, this page reports it instead of choosing silently:

- compat/COVERAGE.md (dated 2026-10-27) reports 809 of 1245 covered = 65.0%, but compat/inventory.csv now computes 1253 of 1268 covered = 98.8% with compat/coverage.py. The CSV was updated after that report (later gap-closing work), so this page uses the CSV and COVERAGE.md is out of date. The engine commit in that header (ff2ad26, branch compat-indexes) belongs to the older state; the build the current inventory was run on is not recorded in the data. The report also says 548 proposed exclusions; the founder-approved exclusions in the CSV number 525.
- compat/coverage.py counts 16 rows as covered although no comparison test against MongoDB exists for them (specification tests only). By this page's definition they are Not tested, so Supported is 1237 and not 1253.
- compat/UNVERIFIED.md says TLS and X.509 are marked missing without a comparison test; in compat/inventory.csv they are now covered by comparison tests on the same certificates. The CSV is used; that bullet is not repeated on this page.
- The two usage scans were taken at different commits of 8 of the 10 repositories (compat-scan.md and compat/scan_extra.json record different commits).
- The 'Our status' column of compat-scan.md comes from an older compatibility table and is not used here; status always comes from compat/inventory.csv.
