# results9 — zero unsupported calls in the compat scan

Branch `compat-indexes`. Work log and results of the round that implements text search, geo, hashed/wildcard indexes,
`$search`/`$searchMeta` and the remaining operators found by the scan. **Section 0 was written before any code.**

## 0. The scan before this round: every remaining unsupported call

Scan of the 10 repositories cloned on 2026-10-27 (`tools/compat-scan/scan.py run`, raw data `results9/compat-scan-before.json`; the scanner now also
records `file:line` of every application-code hit). A call is "unsupported" when the feature has no passing differential test or, for index options, is only
recorded (`tools/compat-scan/unsupported_list.py`, profile `after` of results8). Totals: **134 unsupported calls in 6 apps** — Rocket.Chat 6, parse-server 50,
LibreChat 3, countly-server 65, payload 9, edx-platform 1. NodeBB, wekan, novu and appsmith are at 0.

The list, app by app and feature by feature, with `file:line` (also in `results9/unsupported-before.md`):


### Rocket.Chat — 6 unsupported calls in 3 features

- **$meta** ×4: `packages/mongo-adapter/src/sort.ts:30`, `packages/model-typings/src/types/DocumentWithProjection.ts:9`, `packages/model-typings/src/types/DocumentWithProjection.typetest.ts:70`, `apps/meteor/server/lib/parseMessageSearchQuery.ts:274`

- **$search** ×1: `apps/meteor/server/lib/parseMessageSearchQuery.ts:270`

- **index: text (recorded only)** ×1: `apps/meteor/server/startup/ensureMessagesTextIndex.ts:82`

### parse-server — 50 unsupported calls in 13 features

- **$text** ×15: `src/Adapters/Storage/Mongo/MongoTransform.js:333`, `src/Adapters/Storage/Mongo/MongoTransform.js:778`, `src/Adapters/Storage/Mongo/MongoTransform.js:781`, `src/Adapters/Storage/Mongo/MongoTransform.js:784`, `src/Adapters/Storage/Mongo/MongoTransform.js:791`, `src/Adapters/Storage/Mongo/MongoTransform.js:798`, `src/Adapters/Storage/Mongo/MongoTransform.js:806`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:628`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:631`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:634`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:641`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:646`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:652`, `src/Adapters/Storage/Postgres/PostgresStorageAdapter.js:657`, `src/GraphQL/transformers/query.js:27`

- **$maxDistance** ×6: `src/Adapters/Storage/Mongo/MongoTransform.js:824`, `src/Adapters/Storage/Mongo/MongoTransform.js:834`, `src/Adapters/Storage/Mongo/MongoTransform.js:837`, `src/Adapters/Storage/Mongo/MongoTransform.js:840`, `src/LiveQuery/QueryTools.js:407`, `src/GraphQL/transformers/query.js:34`

- **$centerSphere** ×4: `src/Adapters/Storage/Mongo/MongoTransform.js:865`, `src/Adapters/Storage/Mongo/MongoTransform.js:817`, `src/Adapters/Storage/Mongo/MongoTransform.js:932`, `src/GraphQL/transformers/query.js:42`

- **$geoIntersects** ×4: `src/Adapters/Storage/Mongo/MongoTransform.js:937`, `src/Adapters/Storage/Mongo/MongoCollection.js:5`, `src/LiveQuery/QueryTools.js:398`, `src/GraphQL/transformers/query.js:43`

- **$geoWithin** ×4: `src/Adapters/Storage/Mongo/MongoTransform.js:863`, `src/Adapters/Storage/Mongo/MongoCollection.js:5`, `src/LiveQuery/QueryTools.js:377`, `src/GraphQL/transformers/query.js:40`

- **$nearSphere** ×4: `src/Adapters/Storage/Mongo/MongoTransform.js:813`, `src/Adapters/Storage/Mongo/MongoCollection.js:7`, `src/LiveQuery/QueryTools.js:346`, `src/GraphQL/transformers/query.js:33`

- **$box** ×3: `src/Adapters/Storage/Mongo/MongoTransform.js:851`, `src/Adapters/Storage/Mongo/MongoTransform.js:856`, `src/GraphQL/transformers/query.js:39`

- **$polygon** ×3: `src/Adapters/Storage/Mongo/MongoTransform.js:864`, `src/Adapters/Storage/Mongo/MongoTransform.js:903`, `src/GraphQL/transformers/query.js:41`

- **$meta** ×2: `src/RestQuery.js:243`, `src/Adapters/Storage/Mongo/MongoCollection.js:77`

- **$search** ×2: `src/Adapters/Storage/Mongo/MongoTransform.js:787`, `src/GraphQL/transformers/query.js:28`

- **$geoNear** ×1: `src/Adapters/Storage/Mongo/MongoCollection.js:7`

- **$geometry** ×1: `src/Adapters/Storage/Mongo/MongoTransform.js:948`

- **$near** ×1: `src/Adapters/Storage/Mongo/MongoCollection.js:7`

### NodeBB — 0 unsupported calls in 0 features

none

### wekan — 0 unsupported calls in 0 features

none

### LibreChat — 3 unsupported calls in 1 features

- **$text** ×3: `packages/data-schemas/src/methods/insights.ts:523`, `packages/data-schemas/src/methods/message.ts:3515`, `packages/data-schemas/src/methods/message.ts:3518`

### novu — 0 unsupported calls in 0 features

none

### appsmith — 0 unsupported calls in 0 features

none

### countly-server — 65 unsupported calls in 34 features

- **index: text (recorded only)** ×9: `plugins/star-rating/install.js:29`, `plugins/star-rating/api/api.js:1805`, `plugins/star-rating/api/api.js:1850`, `plugins/star-rating/api/api.js:1872`, `plugins/crashes/install.js:37`, `plugins/crashes/api/api.js:1822`, `plugins/crashes/api/api.js:1852`, `plugins/crashes/api/api.js:1884`, `bin/upgrade/23.03/scripts/update_user_text_index.js:10`

- **$geometry** ×5: `plugins/dbviewer/api/parts/aggregation_guard.js:166`, `plugins/dbviewer/api/parts/aggregation_guard.js:167`, `plugins/dbviewer/api/parts/aggregation_guard.js:168`, `plugins/dbviewer/api/parts/aggregation_guard.js:169`, `plugins/dbviewer/api/parts/aggregation_guard.js:161`

- **$search** ×5: `plugins/star-rating/api/api.js:1398`, `plugins/crashes/api/api.js:1045`, `plugins/systemlogs/api/api.js:118`, `plugins/dbviewer/api/parts/aggregation_guard.js:80`, `plugins/dbviewer/api/parts/aggregation_guard.js:170`

- **$geoWithin** ×4: `plugins/dbviewer/api/parts/aggregation_guard.js:149`, `plugins/dbviewer/api/parts/aggregation_guard.js:166`, `plugins/dbviewer/api/parts/aggregation_guard.js:24`, `plugins/dbviewer/api/parts/aggregation_guard.js:307`

- **$text** ×4: `plugins/crashes/api/api.js:1045`, `plugins/systemlogs/api/api.js:118`, `plugins/dbviewer/api/parts/aggregation_guard.js:149`, `plugins/dbviewer/api/parts/aggregation_guard.js:170`

- **$centerSphere** ×3: `plugins/dbviewer/api/parts/aggregation_guard.js:166`, `plugins/dbviewer/api/parts/aggregation_guard.js:24`, `plugins/dbviewer/api/parts/aggregation_guard.js:307`

- **index: hashed / wildcard (recorded only)** ×3: `bin/scripts/sharding/sharding_with_auth_old.js:81`, `bin/scripts/sharding/sharding_old.js:76`, `bin/scripts/sharding/sharding.js:74`

- **$geoIntersects** ×2: `plugins/dbviewer/api/parts/aggregation_guard.js:149`, `plugins/dbviewer/api/parts/aggregation_guard.js:167`

- **$maxDistance** ×2: `plugins/dbviewer/api/parts/aggregation_guard.js:168`, `plugins/dbviewer/api/parts/aggregation_guard.js:169`

- **$minDistance** ×2: `plugins/dbviewer/api/parts/aggregation_guard.js:168`, `plugins/dbviewer/api/parts/aggregation_guard.js:169`

- **$near** ×2: `plugins/dbviewer/api/parts/aggregation_guard.js:149`, `plugins/dbviewer/api/parts/aggregation_guard.js:168`

- **$nearSphere** ×2: `plugins/dbviewer/api/parts/aggregation_guard.js:149`, `plugins/dbviewer/api/parts/aggregation_guard.js:169`

- **$box** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:166`

- **$bucketAuto** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:77`

- **$center** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:166`

- **$densify** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:77`

- **$documents** ×1: `plugins/dbviewer/api/parts/verify_operators.js:163`

- **$fill** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:78`

- **$geoNear** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:78`

- **$graphLookup** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:86`

- **$jsonSchema** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:148`

- **$median** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:138`

- **$meta** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:127`

- **$percentile** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:139`

- **$planCacheStats** ×1: `plugins/dbviewer/api/parts/verify_operators.js:160`

- **$polygon** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:166`

- **$rand** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:116`

- **$redact** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:79`

- **$sampleRate** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:152`

- **$searchMeta** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:80`

- **$setWindowFields** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:80`

- **$toDecimal** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:133`

- **$tsIncrement** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:114`

- **$tsSecond** ×1: `plugins/dbviewer/api/parts/aggregation_guard.js:114`

### payload — 9 unsupported calls in 6 features

- **$geometry** ×2: `packages/db-mongodb/src/queries/sanitizeQueryValue.ts:424`, `packages/db-mongodb/src/queries/sanitizeQueryValue.ts:439`

- **$near** ×2: `packages/db-mongodb/src/queries/buildSearchParams.ts:540`, `packages/db-mongodb/src/queries/operatorMap.ts:13`

- **index: 2dsphere / 2d (geo) (recorded only)** ×2: `packages/db-mongodb/src/models/buildSchema.ts:591`, `packages/db-mongodb/src/models/buildSchema.ts:594`

- **$geoIntersects** ×1: `packages/db-mongodb/src/queries/operatorMap.ts:10`

- **$geoWithin** ×1: `packages/db-mongodb/src/queries/operatorMap.ts:16`

- **$text** ×1: `packages/db-mongodb/src/queries/buildSearchParams.ts:540`

### edx-platform — 1 unsupported calls in 1 features

- **$text** ×1: `lms/static/js/Markdown.Converter.js:35`

TOTAL unsupported calls: 134


### What these calls are (read from the sources: shallow sparse clones of the files)

| App | Real use | Mention only / false positive |
|---|---|---|
| Rocket.Chat | `createIndex({rid: 1, msg: 'text'})` / `{msg: 'text'}` (compound text index with an equality prefix); `query.$text = {$search: text}` with projection `{score: {$meta: 'textScore'}}` (`parseMessageSearchQuery.ts:270-276`, `ensureMessagesTextIndex.ts:82`) | `$meta` in `mongo-adapter/src/sort.ts` and two TypeScript type files; the `$search` hit is the `$search` option of `$text` |
| parse-server | `$text` with `$search`/`$language`/`$caseSensitive`/`$diacriticSensitive` (`MongoTransform.js:778-806`); legacy `$nearSphere: [lng, lat]` + `$maxDistance` (radians); `$geoWithin` with `$box`, `$polygon`, `$centerSphere`; `$geoIntersects` with `$geometry`; `$meta: 'textScore'` (`RestQuery.js:243`, `MongoCollection.js:77`); 2dsphere indexes on legacy coordinate pairs | the Postgres adapter and GraphQL transformer lines (same operators as strings), `QueryTools.js` (client-side evaluation of the operators), comments in `MongoCollection.js:5-7` |
| countly-server | text indexes `{name: 'text'}` on crash and rating collections, `query.$text = {$search: ...}` (`star-rating/api/api.js:1398`, `crashes/api/api.js:1045`), `createIndex({_id: 'hashed'})` + `shardCollection` in the sharding scripts | `plugins/dbviewer/api/parts/aggregation_guard.js` and `verify_operators.js` are **allow/deny lists of operator names** (`$bucketAuto`, `$densify`, `$fill`, `$geoNear`, `$graphLookup`, `$jsonSchema`, `$median`, `$percentile`, `$rand`, `$redact`, `$sampleRate`, `$searchMeta`, `$setWindowFields`, `$toDecimal`, `$tsIncrement`, `$tsSecond`, `$documents`, `$planCacheStats`, the geo operators): the Countly db viewer lets its users run these through the application, so a server has to support them; `$text` in `systemlogs/api/api.js:118` is commented out |
| payload | geo query operators `$near` (+`$maxDistance`/`$minDistance`), `$geoWithin`, `$geoIntersects` with `$geometry` (`operatorMap.ts`, `sanitizeQueryValue.ts`), 2dsphere index on point fields (`buildSchema.ts:591-594`) | `$text` in `buildSearchParams.ts:540` is in a comment |
| LibreChat | none | the three `$text` hits are field references (`'$text'` inside `$ifNull` and `$first`), not the operator |
| edx-platform | none | `$text` in `Markdown.Converter.js:35` is a Perl variable in a comment |

Real Atlas Search (`$search` as a pipeline stage) is not used by any of the ten apps as far as the sources show: every `$search` hit is the option of `$text`, a
commented line, or an operator name in Countly's allow-list. The scan cannot tell the two meanings of `$search` apart, so both are implemented. The false positives
(`$text` as a field reference or in a comment) count as covered once the operator `$text` is covered; the scanner is **not** changed to hide them.

## Plan and order

1. Text search (indexes with weights, compound, `$text`, textScore, stemming and stop words), 2. geo, 3. hashed and wildcard indexes, 4. `$search`/`$searchMeta`,
5. the remaining operators of the list (`$bucketAuto`, `$densify`, `$documents`, `$fill`, `$graphLookup`, `$jsonSchema`, `$median`/`$percentile`, `$planCacheStats`, `$rand`,
`$redact`, `$sampleRate`, `$setWindowFields`, `$toDecimal`, `$tsIncrement`/`$tsSecond`). The table shows that text and geo are the real uses, so the requested order stands.

---

# Results (written after the code)

## 1. What was implemented

Each feature is its own commit on `compat-indexes` (pushed; no PR, nothing merged to main).

| Commit | Feature |
|---|---|
| Text | text indexes (weights, compound with equality prefix, wildcard `$**`, default language, language override), `$text` with `$search` / `$language` / `$caseSensitive` / `$diacriticSensitive`, phrases, negation, `{$meta:"textScore"}` in projection, sort and aggregation, Snowball stemming and stop words for 15 languages (scores and tokenization match MongoDB bit for bit on the tests) |
| Geo | `2dsphere` and `2d` indexes (cell tokens / geohash keys, used by the planner), `$geoWithin` (`$geometry`, `$box`, `$polygon`, `$center`, `$centerSphere`), `$geoIntersects`, `$near` / `$nearSphere` with `$maxDistance` / `$minDistance`, `$geoNear` stage (also inside `$lookup`), GeoJSON Point, LineString, Polygon, Multi\*, GeometryCollection, big polygons (strictwinding CRS), S2 half-open boundary semantics for polygons/lines, `$meta` `geoNearDistance` / `geoNearPoint`, all the error codes the reference gives |
| Hashed / wildcard | hashed indexes (single and compound, array values rejected, unique rejected) and wildcard indexes (`$**`, `a.$**`, `wildcardProjection`) with entries, planner use (`$eq`, `$in`, ranges, `$exists`, `$type`) and MongoDB's rule that a hinted wildcard scan is refused (code 291) when the query gives no usable bound; also Decimal128 ordering/equality, `$mod` on decimals, stored-regex matching |
| `$search` / `$searchMeta` | `text` (fuzzy, matchCriteria), `phrase` (slop), `autocomplete` (tokenOrder), `compound` (must / mustNot / should / filter / minimumShouldMatch), `equals`, `exists`, `range`, `in`, `wildcard`, `regex`, `count`, `sort`, `score` boost/constant, `{$meta:"searchScore"}` (BM25 scores) |
| Aggregation tail | `$bucketAuto` (incl. `POWERSOF2` and `1-2-5`), `$densify`, `$fill`, `$setWindowFields` (accumulators, `$rank`, `$denseRank`, `$documentNumber`, `$shift`, `$expMovingAvg`, `$derivative`, `$integral`, `$locf`, `$linearFill`; `documents` and `range` windows), `$graphLookup`, `$redact`, `$documents` (`aggregate: 1`), `$planCacheStats` (empty), `$median` / `$percentile` (accumulator, window and expression), `$rand`, `$sampleRate`, `$jsonSchema`, `$toDecimal` (15-digit conversion as MongoDB does), `$tsSecond`, `$tsIncrement` |

Bug found by the cost benchmark and fixed in the same round: a document fetch by range scan (`docs_by_scan`) could build an empty range when a 20,000-row chunk ended on the last wanted key, which panics the store (found with 100k documents). The scan now stops there.

## 2. The scan, before and after (10 apps re-cloned on 2026-10-27)

Raw data: `results9/compat-scan-before.json`, `results9/compat-scan-after.json`; lists with `file:line`: `results9/unsupported-before.md`, `results9/unsupported-after.md`.

| App | Unsupported calls before | after |
|---|---|---|
| Rocket.Chat | 6 | **0** |
| parse-server | 50 | **0** |
| NodeBB | 0 | **0** |
| wekan | 0 | **0** |
| LibreChat | 3 | **0** |
| novu | 0 | **0** |
| appsmith | 0 | **0** |
| countly-server | 65 | **0** |
| payload | 9 | **0** |
| edx-platform | 1 | **0** |
| **Total** | **134** | **0** |

All ten apps are at 0. "Supported" means what it meant before: the feature has differential tests and every test that uses it passes on pymongo and Node (`tools/compat-scan/coverage.py`, test-name prefixes `text`, `geo`, `hashed`, `wild`, `tail` added; index options text, geo and hashed/wildcard count only when their tests pass). Two caveats, stated plainly:

* `$searchMeta` has no differential evidence (no reference server). It counts as supported because `mongo-tests/search_test.py` (47 checks of the documented semantics, `results9/search_test.txt`) passes. The `$search` that the apps actually contain is the `$text` option, which has differential tests.
* The scanner cannot tell real uses from mentions (Countly's allow-list names `$search`, `$geoNear`, ... as strings, LibreChat and edx-platform hit `$text` as a field name or in a comment). Those hits are covered because the operators are, not because the scanner was changed.

## 3. Tests

Final binary, full suites (`mongo-tests/run_full.sh`, reference `mongod --replSet`), 3,058 tests:

| Suite | pymongo | Node |
|---|---|---|
| core | 2920 / 2920 | 2920 / 2920 |
| replset | 89 / 89 | 89 / 89 |
| extra (beyond-scope probes) | 46 / 49 | 47 / 49 |

Nothing that passed before regressed. The extra failures are the three probes that already failed in results8: `aggregate.extra.group_add_to_set` (`$addToSet` order is arbitrary on MongoDB; passes or fails with the reference's order of the run), `update.extra.current_date` (compares the stored `$currentDate`), `lookup.extra_let_pipeline_form` (natural order of the joined array). Results: `results9/results_py.json`, `results9/results_node.json`.

New differential tests of this round (all pass on both drivers): text 170, geo 419, hashed 185, wildcard 185, aggregation tail 243 (including error codes and edge cases: empty results, missing fields, mixed types, large and antimeridian polygons, boundary vertices and edges, several languages, hinted-scan refusals). `$search` / `$searchMeta`: 47 checks against the documented semantics (mongo:7.0 has no Atlas Search), run on pymongo only.

## 4. Known differences (not hidden)

* **Equal distances.** Documents at exactly the same distance from a `$near` point come out in index-key order here; MongoDB's order for ties comes from an internal heap and cannot be reproduced. The tests avoid ties.
* **Geometry boundaries.** Point-in-polygon, polygon/polygon and polygon/line relations use S2's half-open edge rules (ported with the `s2` crate predicates) and match MongoDB in every probed case except: a polygon that shares part of an edge with another one (`$geoWithin` of a polygon touching the query's edge), a line lying partly along a polygon edge, and a stored LineString containing a query point on faces 3-5 of the S2 cube (MongoDB does not match a point exactly on such a line; this server does). These are knife-edge cases (exact coincidence of boundaries).
* **`$near` without `$maxDistance`** reads the whole index and sorts (no incremental ring expansion): see the cost table.
* **Text-index `$text` and `$search` scoring.** `$text` scores match MongoDB exactly. `$search` scores are BM25 without Lucene's lossy length normalization, so they rank like Atlas but are not equal to it.
* **`$bucketAuto` granularity** supports `POWERSOF2` and `1-2-5` only (R-series and E-series return an error).
* **Compound wildcard indexes** (`{a:1, "$**":1}` with `wildcardProjection`) are accepted and listed but only recorded: their entries are not maintained and the planner does not use them. `$toHashedIndexKey` is not implemented (the stored hash is this server's own function, not MongoDB's MD5-based one).
* **`$planCacheStats`** returns no entries (there is no plan cache).
* **`$densify`** with `month` / `quarter` / `year` steps adds calendar months iteratively (as MongoDB does for the probed cases); other unit combinations were not probed.
* **Not implemented at all:** `$search` operators `near`, `geoShape`, `geoWithin`, `moreLikeThis`, `queryString`, `embeddedDocument`, `facet`, `highlight`, `scoreDetails`, `returnStoredSource`, synonyms; the search index commands (`createSearchIndex`, `listSearchIndexes`, ...). No app uses them; `$search` here needs no index definition.
* **Hidden indexes** (`collMod` `hidden`) are not supported (found while probing; unrelated to this round).

## 5. Cost of text and geo queries on MinIO

Script `mongo-tests/bench_geo_text.py`, raw data `results9/cost_geo_text.json`. Documents: 8-word text field (Zipf-like vocabulary of 3,000 words), a GeoJSON point (uniform over the globe), a 100-byte payload. "Cold" = the tenant is closed before every sample (7 samples, median); "warm" = the same query repeated on the open tenant. GETs include LISTs. "No index" collections have no text/geo index at all; queries that need an index (`$text`, `$near`, geo with a polygon on the index) cannot run there (the server returns the same error MongoDB does), so the no-index column uses the equivalent scan: a regex on the field, `$search` (which scans), and `$geoWithin $centerSphere` (which a 2dsphere index can also answer).

10,000 documents (about 3 MB of data):

| Query | Index | Results | Cold GETs | Cold bytes | Cold ms | Warm GETs | Warm ms |
|---|---|---|---|---|---|---|---|
| `$text` rare word | text | 14 | 13 | 2.56 MB | 41 | 0 | 19 |
| `$text` common word (29% of docs) | text | 2,888 | 14 | 3.22 MB | 112 | 0 | 88 |
| regex on the word | none | 14 | 10 | 2.95 MB | 64 | 0 | 43 |
| `$search` text stage | none | 14 | 10 | 2.95 MB | 154 | 0 | 117 |
| `$geoWithin` 4°×4° polygon | 2dsphere | 1 | 14 | 1.84 MB | 35 | 0 | 14 |
| `$near` ≤ 100 km | 2dsphere | 0 | 13 | 0.19 MB | 22 | 0 | 2 |
| `$nearSphere` limit 10 (no max distance) | 2dsphere | 10 | 21 | 3.67 MB | 198 | 0 | 182 |
| `$geoWithin $centerSphere` | none | 0 | 10 | 2.95 MB | 76 | 0 | 59 |
| `$geoWithin $centerSphere` | 2dsphere | 0 | 15 | 0.20 MB | 23 | 0 | 2 |

100,000 documents (about 29 MB of data):

| Query | Index | Results | Cold GETs | Cold bytes | Cold ms | Warm GETs | Warm ms |
|---|---|---|---|---|---|---|---|
| `$text` rare word | text | 136 | 22 | 30.3 MB | 284 | 0 | 259 |
| `$text` common word (29% of docs) | text | 29,169 | 37 | 31.8 MB | 1,013 | 0 | 1,232 |
| regex on the word | none | 136 | 16 | 29.4 MB | 429 | 0 | 417 |
| `$search` text stage | none | 136 | 16 | 29.4 MB | 1,227 | 0 | 1,234 |
| `$geoWithin` 4°×4° polygon | 2dsphere | 25 | 73 | 2.04 MB | 69 | 0 | 6 |
| `$near` ≤ 100 km | 2dsphere | 4 | 24 | 1.84 MB | 34 | 0 | 3 |
| `$nearSphere` limit 10 (no max distance) | 2dsphere | 10 | 101 | 36.2 MB | 2,612 | 0 | 2,692 |
| `$geoWithin $centerSphere` | none | 6 | 16 | 29.4 MB | 585 | 0 | 514 |
| `$geoWithin $centerSphere` | 2dsphere | 6 | 35 | 1.89 MB | 45 | 0 | 3.6 |

Reading it honestly:

* **Geo with a bounded region wins clearly**: the 2dsphere index cuts the bytes read from 29.4 MB to about 1.9-2.0 MB and the latency 8-13× at 100k documents (warm 3-6 ms against 514 ms). Cold GET counts are higher with the index (35-73 against 16) because the index range reads and the point reads of the few matching documents are many small requests, while the scan is a few large ones; bytes and latency are what improve.
* **Text does not win in this benchmark**: with 29 MB in the tenant, fetching 136 scattered documents costs the same bytes as scanning (the cost-based fetch from results8 picks a single range scan over the documents when point reads would touch most blocks), so a rare-word `$text` query reads 30.3 MB cold, as much as the regex scan, and a common-word query reads 31.8 MB. The index still finds the matching documents without evaluating the regex, so latency is better (284 vs 429 ms cold) but not the I/O.
* **`$near` / `$nearSphere` without a `$maxDistance` is the weak spot**: the whole index and all documents are read and sorted (36 MB, 2.6 s at 100k documents). With `$maxDistance` it behaves like the bounded geo queries.
* Warm queries cost no GETs in every case (the tenant is cached); warm latency is CPU only, and scans cost CPU in proportion to the documents (1.2 s for 100k documents through `$search`).
* Not measured: queries with several concurrent clients, documents larger than ~200 bytes, hashed and wildcard index queries (their cost follows the regular index path measured in results8), a text index with a prefix field.
