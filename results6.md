# Query, update and aggregation features on the MongoDB wire server — results6

`mongo-server` (results5) now implements the most-used features that `compat-scan.md` ranked as missing: regular expressions, `findAndModify`, the array and numeric update operators, `$size` / `$elemMatch` / `$all` / `$type` / `$mod` / `$expr`, an aggregation expression evaluator, and the `$unwind` / `$lookup` / `$addFields` / `$set` stages. Every item is proven by differential tests: the same operation scripts run through the official Python driver (pymongo 4.19.0) and the official Node.js driver (mongodb 7.7.0), against a real `mongo:7.0` (7.0.45) and against this server, and the results are compared as canonical Extended JSON (types, field order and error codes included).

**Result:** 1,081 in-scope operation scripts (the 308 of results5 plus 773 new) give identical results on both drivers: **pymongo 1,081/1,081, Node.js 1,081/1,081**. All 308 results5 tests still pass. The coverage gain on the ten scanned repositories is large but not complete: unsupported feature usage in application code falls from 3,261 call sites (483 repo/feature pairs) to 619 (249 pairs), and repositories with zero unsupported features stay at **0 of 10**: transactions, change streams, `$text`, geo queries, `$where`, `$merge`/`$out`/`$facet` and a long tail of expression operators are still missing (lists below). Everything ran locally (Docker `mongo:7.0` and MinIO); no cloud account or paid API was used.

## Step 0 — Coverage baseline (before any code)

Method: `tools/compat-scan/coverage.py` takes the raw per-feature application-code counts of `tools/compat-scan/results.json` (the scan behind `compat-scan.md`, unchanged) and, per repository, lists every feature with application-code usage whose status in the results5 compatibility table is "not supported" (`bulkWrite` counted as supported, as asked; index kinds are recorded by the server and never counted). A repository is "fully covered" when that list is empty.

| Repository | Unsupported features used in application code | Count |
|---|---|---|
| Rocket.Chat | `findOneAndUpdate` ×74, `$regex` ×48, `$pull` ×44, `$cond` ×41, `$options` ×40, `$lookup` ×39, `$unwind` ×39, `$addToSet (update)` ×35, `$ifNull` ×32, `$size` ×31, `$each` ×26, `$elemMatch` ×23, `$expr` ×16, `$setOnInsert` ×14, `$all` ×13, `$divide` ×13, `findOneAndDelete` ×13, `$addToSet (aggregation accumulator)` ×12, `$push (update)` ×10, `$addFields` ×8, `$facet` ×8, `$range` ×8, `$filter` ×7, `$slice` ×7, `$type` ×7, `$ceil` ×6, `$pullAll` ×6, `withTransaction / startTransaction` ×6, `$trunc` ×5, `watch (change streams)` ×5, `$hour` ×4, `$meta` ×4, `$subtract` ×4, `$arrayElemAt` ×3, `$concat` ×3, `$rename` ×3, `$substr` ×3, `$currentDate` ×2, `$isArray` ×2, `$literal` ×2, `$map` ×2, `$max (update)` ×2, `$merge` ×2, `$setUnion` ×2, `$add` ×1, `$bitsAllClear` ×1, `$bitsAllSet` ×1, `$bitsAnyClear` ×1, `$bitsAnySet` ×1, `$convert` ×1, `$dateToString` ×1, `$floor` ×1, `$month` ×1, `$multiply` ×1, `$pop` ×1, `$position` ×1, `$search` ×1, `$set (aggregation stage)` ×1, `$toLower` ×1, `$toString` ×1, `$unset (aggregation stage)` ×1, `$year` ×1 | 62 |
| parse-server | `$text` ×15, `$regex` ×14, `findOneAndUpdate` ×7, `$all` ×6, `$maxDistance` ×6, `$centerSphere` ×4, `$geoIntersects` ×4, `$geoWithin` ×4, `$merge` ×4, `$nearSphere` ×4, `$out` ×4, `$box` ×3, `$options` ×3, `$polygon` ×3, `$meta` ×2, `$search` ×2, `findOneAndDelete` ×2, `watch (change streams)` ×2, `withTransaction / startTransaction` ×2, `$addToSet (update)` ×1, `$dayOfMonth` ×1, `$dayOfWeek` ×1, `$dayOfYear` ×1, `$each` ×1, `$geoNear` ×1, `$geometry` ×1, `$hour` ×1, `$isoDayOfWeek` ×1, `$isoWeekYear` ×1, `$millisecond` ×1, `$minute` ×1, `$month` ×1, `$near` ×1, `$pullAll` ×1, `$push (update)` ×1, `$second` ×1, `$setOnInsert` ×1, `$week` ×1, `$year` ×1, `findOneAndReplace` ×1 | 40 |
| NodeBB | `findOneAndUpdate` ×5, `$addToSet (update)` ×3, `$each` ×3, `$size` ×3, `$pull` ×2, `$regex` ×2, `$collStats` ×1, `$cond` ×1, `$map` ×1, `$multiply` ×1, `$pop` ×1, `$position` ×1, `$pullAll` ×1, `$push (update)` ×1, `$rename` ×1 | 15 |
| wekan | `$pull` ×75, `$elemMatch` ×60, `$setOnInsert` ×59, `$push (update)` ×47, `$addToSet (update)` ×45, `$type` ×24, `$regex` ×23, `$where` ×13, `$each` ×12, `$options` ×11, `$rename` ×5, `$pullAll` ×4, `findOneAndUpdate` ×4, `$accumulator` ×3, `$cond` ×3, `$function` ×3, `$ifNull` ×3, `$addFields` ×2, `$expr` ×2, `$map` ×2, `$merge` ×2, `$objectToArray` ×2, `$out` ×2, `$slice` ×2, `findOneAndDelete` ×2, `$all` ×1, `$anyElementTrue` ×1, `$bit` ×1, `$collStats` ×1, `$literal` ×1, `$lookup` ×1, `$max (update)` ×1, `$pop` ×1, `$range` ×1, `$size` ×1, `$toString` ×1, `watch (change streams)` ×1 | 37 |
| LibreChat | `findOneAndUpdate` ×201, `$setOnInsert` ×61, `$cond` ×41, `$elemMatch` ×37, `$ifNull` ×35, `$pull` ×33, `$addToSet (update)` ×26, `$type` ×25, `$push (update)` ×24, `findOneAndDelete` ×23, `$size` ×18, `$max (update)` ×17, `$each` ×16, `$addFields` ×15, `$lookup` ×15, `$regex` ×15, `$slice` ×14, `$filter` ×13, `$strLenBytes` ×11, `withTransaction / startTransaction` ×11, `$isArray` ×9, `$pullAll` ×8, `$add` ×7, `$let` ×7, `$arrayElemAt` ×6, `$expr` ×6, `$switch` ×5, `$trim` ×5, `$unwind` ×5, `$bitsAllSet` ×4, `$concat` ×4, `$currentDate` ×4, `$map` ×4, `$options` ×4, `$reduce` ×4, `$toLower` ×4, `$where` ×4, `$concatArrays` ×3, `$convert` ×3, `$dateToString` ×3, `$indexOfCP` ×3, `$min (update)` ×3, `$regexMatch` ×3, `$subtract` ×3, `$text` ×3, `findOneAndReplace` ×3, `$facet` ×2, `$rename` ×2, `$reverseArray` ×2, `$setUnion` ×2, `$sortArray` ×2, `$strLenCP` ×2, `$substrCP` ×2, `$anyElementTrue` ×1, `$bitsAnySet` ×1, `$indexOfBytes` ×1, `$literal` ×1, `$merge` ×1, `$multiply` ×1, `$out` ×1, `$replaceRoot` ×1, `$set (aggregation stage)` ×1, `$substrBytes` ×1, `watch (change streams)` ×1 | 64 |
| novu | `findOneAndUpdate` ×29, `withTransaction / startTransaction` ×22, `$regex` ×20, `$options` ×17, `$elemMatch` ×12, `$type` ×11, `$pull` ×9, `$lookup` ×6, `$setOnInsert` ×6, `findOneAndDelete` ×6, `$size` ×5, `$cond` ×4, `$push (update)` ×4, `$unwind` ×4, `$expr` ×3, `$max (update)` ×3, `$addToSet (aggregation accumulator)` ×2, `$all` ×2, `$ifNull` ×2, `$isArray` ×2, `$min (update)` ×2, `$rename` ×2, `$concat` ×1, `$concatArrays` ×1, `$dateToString` ×1, `$mergeObjects` ×1, `$regexMatch` ×1, `$trim` ×1, `watch (change streams)` ×1 | 29 |
| appsmith | `withTransaction / startTransaction` ×16, `findOneAndUpdate` ×9, `$regex` ×6, `watch (change streams)` ×5, `$options` ×2, `$position` ×2, `$arrayElemAt` ×1, `$concatArrays` ×1, `$filter` ×1, `$let` ×1, `$size` ×1, `$unwind` ×1, `$where` ×1 | 13 |
| countly-server | `$regex` ×127, `findOneAndUpdate` ×80, `$addFields` ×33, `$pull` ×27, `$objectToArray` ×22, `$elemMatch` ×21, `$size` ×19, `$ifNull` ×18, `$max (update)` ×18, `$unwind` ×18, `$cond` ×17, `$addToSet (update)` ×16, `$each` ×16, `$lookup` ×13, `$addToSet (aggregation accumulator)` ×12, `$switch` ×11, `$filter` ×10, `$map` ×10, `$merge` ×10, `$options` ×10, `$arrayElemAt` ×8, `$toString` ×8, `$concat` ×7, `$facet` ×7, `$split` ×7, `$toLower` ×7, `$type` ×7, `$bitsAllClear` ×6, `$push (update)` ×6, `$toDate` ×6, `$unionWith` ×6, `$arrayToObject` ×5, `$geometry` ×5, `$min (update)` ×5, `$search` ×5, `findOneAndDelete` ×5, `watch (change streams)` ×5, `$add` ×4, `$bitsAnySet` ×4, `$divide` ×4, `$function` ×4, `$geoWithin` ×4, `$literal` ×4, `$multiply` ×4, `$slice` ×4, `$text` ×4, `$toInt` ×4, `$where` ×4, `$all` ×3, `$bit` ×3, `$bitsAllSet` ×3, `$centerSphere` ×3, `$dateFromParts` ×3, `$indexOfBytes` ×3, `$month` ×3, `$out` ×3, `$year` ×3, `$accumulator` ×2, `$collStats` ×2, `$convert` ×2, `$dateToString` ×2, `$dayOfMonth` ×2, `$geoIntersects` ×2, `$isNumber` ×2, `$maxDistance` ×2, `$mergeObjects` ×2, `$minDistance` ×2, `$mod` ×2, `$mul` ×2, `$near` ×2, `$nearSphere` ×2, `$regexMatch` ×2, `$replaceRoot` ×2, `$set (aggregation stage)` ×2, `$sortArray` ×2, `$strLenBytes` ×2, `$strLenCP` ×2, `$substrBytes` ×2, `$substrCP` ×2, `$subtract` ×2, `$toDouble` ×2, `$toLong` ×2, `$toObjectId` ×2, `findOneAndReplace` ×2, `$abs` ×1, `$allElementsTrue` ×1, `$anyElementTrue` ×1, `$binarySize` ×1, `$bitsAnyClear` ×1, `$bottom` ×1, `$bottomN` ×1, `$box` ×1, `$bsonSize` ×1, `$bucket` ×1, `$bucketAuto` ×1, `$ceil` ×1, `$center` ×1, `$cmp` ×1, `$concatArrays` ×1, `$dateAdd` ×1, `$dateDiff` ×1, `$dateFromString` ×1, `$dateSubtract` ×1, `$dateToParts` ×1, `$dateTrunc` ×1, `$dayOfWeek` ×1, `$dayOfYear` ×1, `$densify` ×1, `$documents` ×1, `$exp` ×1, `$expr` ×1, `$fill` ×1, `$firstN` ×1, `$floor` ×1, `$geoNear` ×1, `$getField` ×1, `$graphLookup` ×1, `$hour` ×1, `$indexOfArray` ×1, `$indexOfCP` ×1, `$indexStats` ×1, `$isArray` ×1, `$isoDayOfWeek` ×1, `$isoWeek` ×1, `$isoWeekYear` ×1, `$jsonSchema` ×1, `$lastN` ×1, `$let` ×1, `$ln` ×1, `$log` ×1, `$log10` ×1, `$ltrim` ×1, `$maxN` ×1, `$median` ×1, `$meta` ×1, `$millisecond` ×1, `$minN` ×1, `$minute` ×1, `$percentile` ×1, `$planCacheStats` ×1, `$polygon` ×1, `$pow` ×1, `$rand` ×1, `$range` ×1, `$redact` ×1, `$reduce` ×1, `$regexFind` ×1, `$regexFindAll` ×1, `$replaceAll` ×1, `$replaceOne` ×1, `$replaceWith` ×1, `$reverseArray` ×1, `$round` ×1, `$rtrim` ×1, `$sample` ×1, `$sampleRate` ×1, `$searchMeta` ×1, `$second` ×1, `$setDifference` ×1, `$setEquals` ×1, `$setField` ×1, `$setIntersection` ×1, `$setIsSubset` ×1, `$setOnInsert` ×1, `$setUnion` ×1, `$setWindowFields` ×1, `$sortByCount` ×1, `$sqrt` ×1, `$stdDevPop` ×1, `$stdDevSamp` ×1, `$strcasecmp` ×1, `$substr` ×1, `$toBool` ×1, `$toDecimal` ×1, `$toUpper` ×1, `$top` ×1, `$topN` ×1, `$trim` ×1, `$trunc` ×1, `$tsIncrement` ×1, `$tsSecond` ×1, `$unset (aggregation stage)` ×1, `$unsetField` ×1, `$week` ×1, `$zip` ×1, `withTransaction / startTransaction` ×1 | 186 |
| payload | `$push (update)` ×28, `withTransaction / startTransaction` ×23, `$each` ×12, `$lookup` ×10, `$addFields` ×9, `findOneAndUpdate` ×7, `$options` ×5, `$regex` ×5, `$addToSet (update)` ×3, `$elemMatch` ×3, `$pull` ×3, `$size` ×3, `$slice` ×3, `$expr` ×2, `$geometry` ×2, `$ifNull` ×2, `$map` ×2, `$near` ×2, `$rename` ×2, `$set (aggregation stage)` ×2, `$unwind` ×2, `$add` ×1, `$all` ×1, `$concatArrays` ×1, `$cond` ×1, `$geoIntersects` ×1, `$geoWithin` ×1, `$isArray` ×1, `$literal` ×1, `$sortArray` ×1, `$text` ×1, `findOneAndDelete` ×1 | 32 |
| edx-platform | `$elemMatch` ×2, `$regex` ×2, `$options` ×1, `$setUnion` ×1, `$text` ×1 | 5 |

Repositories fully covered (zero unsupported features): **0/10**

Totals: 483 (repository, feature) pairs and 3,261 of 22,116 feature call sites in application code were unsupported before this work.

## What was built

- **Regex** (`regex.rs`, new crate `fancy-regex = "=0.20.0"`, latest stable on crates.io, checked 2026-10-26): `$regex` with `$options` (`i m s x`, `u` accepted), a BSON regular expression as the `$regex` operand, as a plain equality operand and inside `$in` / `$nin` / `$not`, matched against strings and against array elements. `fancy-regex` is a PCRE-like engine with lookahead, lookbehind and backreferences. Compiled patterns are cached. Errors carry MongoDB's codes: 51091 invalid pattern, 51108 invalid flag, 51075 options in both places, code 2 for `$options` without `$regex` / non-string `$regex` / embedded NUL, 51156 on engine failure.
- **findAndModify** (`cmd.rs`): `query`, `sort`, `remove`, `update` (operators or replacement), `new`, `fields`, `upsert`, reply `{lastErrorObject: {n, updatedExisting, upserted}, value, ok}`, unique-index and immutable-`_id` checks, the lower-case command name, creation of the collection on upsert. `arrayFilters` and pipeline updates return a MongoDB-shaped error (`NotImplemented`, code 238) instead of misbehaving.
- **Update operators** (`update.rs`): `$push` (`$each`, `$slice`, `$sort`, `$position`), `$pull` (value, operator condition, sub-document query incl. `$or`), `$pullAll`, `$addToSet` (`$each`, BSON equality: `1`, `1.0` and `NumberLong(1)` are one value), `$pop`, `$setOnInsert`, `$rename`, `$min`, `$max`, `$mul`, `$currentDate`; conflicting-path detection (code 40), non-array targets, 64-bit overflow, modifiers applied in MongoDB's path order. Positional (`$`, `$[]`, `$[id]`) paths return code 238.
- **Query operators** (`query.rs`): `$size`, `$all` (values, regexes, `$elemMatch` elements), `$elemMatch` (document form and operator form), `$type` (numeric codes, string aliases incl. `number`, lists), `$mod`, top-level `$expr`, `$comment`, and projection `$slice` (`n`, `-n`, `[skip, limit]`).
- **Expression evaluator** (`expr.rs`) used by `$expr`, `$project`, `$addFields` / `$set` and `$group`: `$cond` (array and object form), `$ifNull`, `$literal`, `$eq $ne $gt $gte $lt $lte`, `$and $or $not`, `$in`, `$add $subtract $multiply $divide $mod`, `$concat $toLower $toUpper`, `$size $isArray $arrayElemAt $concatArrays $setUnion $slice`, `$filter`, `$map`, `$dateToString` (all documented format characters but the week ones; time zones `UTC`, `GMT`, `Etc/UTC` and fixed offsets `+HH`, `+HHMM`, `+HH:MM`; other zone names return code 40485 because there is no time-zone database here), variables `$$ROOT`, `$$CURRENT`, `$$REMOVE`, `$$NOW` and user variables, aggregation field paths through arrays of documents, MongoDB's missing-versus-null semantics, and parse-time constant folding (so `{$and: [false, {$divide: [1, 0]}]}` fails as it does on MongoDB).
- **Stages** (`agg.rs`): `$project` with computed, nested and dotted fields; `$addFields` / `$set`; `$unwind` (string and object form, `includeArrayIndex` as int64, `preserveNullAndEmptyArrays`); `$lookup` with `localField` / `foreignField` in the same database (the foreign collections are read before the pipeline runs; `pipeline` / `let` forms return code 238); `$group` keys and accumulators through the evaluator, plus the `$addToSet` accumulator.
- **Tests:** `gen_tests.py` now generates 1,130 scripts (1,081 in scope, 49 beyond-scope probes); `run_py.py` and `run_node.js` gained `find_one_and_update` / `find_one_and_replace` / `find_one_and_delete` with `returnDocument`, `sort`, `projection`, `upsert`; both harnesses restore the `$regex` / `$options` keys that Extended JSON parsers would otherwise turn into regex values. 28 Rust unit tests (10 new).

## Compatibility table

Cells are `passed/total` differential tests per driver. "Supported, passing" means every in-scope test of the row gave identical results on real MongoDB and on this server, including error codes (error messages are compared only where a test sets `cmp_msg`). "probes" are beyond-scope scripts that are expected to differ. Rows under "Not supported" are those probes.

| Area | Feature | pymongo | Node.js | Status |
|---|---|---|---|---|
| Query operators | `$regex` / `$options` (i m s x u), BSON regex as `$regex` operand, in `$in`/`$nin`/`$not`/plain equality, vs array elements; lookahead, lookbehind, backreferences, POSIX classes, `\p{L}`; invalid pattern/flag/options errors | 63/63 | 63/63 | supported, passing |
| Query operators | `$size` | 21/21 | 21/21 | supported, passing |
| Query operators | `$all` (values, regexes, `$elemMatch` elements) | 23/23 | 23/23 | supported, passing |
| Query operators | `$elemMatch` (document form, operator form, nested, `$and/$or/$nor`) | 29/29 | 29/29 | supported, passing |
| Query operators | `$type` (numeric codes, string aliases incl. `number`, lists) | 56/56 | 56/56 | supported, passing |
| Query operators | `$mod` | 14/14 | 14/14 | supported, passing |
| Query operators | `$expr` (find, count, delete, update, aggregate `$match`, inside `$or`/`$and`) | 44/44 | 44/44 | supported, passing |
| Query operators | projection `$slice` (`n`, `-n`, `[skip, limit]`, nested) | 19/19 | 19/19 | supported, passing |
| Commands | findAndModify: `findOneAndUpdate` / `findOneAndReplace` / `findOneAndDelete`, raw command shape (`lastErrorObject`, `value`), sort, fields, upsert, new, errors | 55/55 + 0/2 probes | 55/55 + 0/2 probes | supported, passing |
| Update operators | `$push` (`$each`, `$slice`, `$sort`, `$position`) | 44/44 | 44/44 | supported, passing |
| Update operators | `$pull` (value, condition, sub-document query) | 20/20 | 20/20 | supported, passing |
| Update operators | `$pullAll` | 8/8 | 8/8 | supported, passing |
| Update operators | `$addToSet` (`$each`, BSON equality) | 18/18 | 18/18 | supported, passing |
| Update operators | `$pop` | 12/12 | 12/12 | supported, passing |
| Update operators | `$setOnInsert` | 8/8 | 8/8 | supported, passing |
| Update operators | `$rename` | 17/17 | 17/17 | supported, passing |
| Update operators | `$min`, `$max` | 26/26 | 26/26 | supported, passing |
| Update operators | `$mul` | 16/16 | 16/16 | supported, passing |
| Update operators | `$currentDate` (date, timestamp) | 9/9 | 9/9 | supported, passing |
| Update operators | conflicting paths, unknown operators, non-array targets, overflow | 19/19 | 19/19 | supported, passing |
| Aggregation stages | `$project` with expressions, nested paths, `$$REMOVE`, `$$ROOT` | 47/47 | 47/47 | supported, passing |
| Aggregation stages | `$addFields` / `$set` | 30/30 | 30/30 | supported, passing |
| Aggregation stages | `$unwind` (string/object form, `includeArrayIndex`, `preserveNullAndEmptyArrays`) | 34/34 | 34/34 | supported, passing |
| Aggregation stages | `$lookup` (`localField`/`foreignField`, same database) | 32/32 | 32/32 | supported, passing |
| Aggregation stages | `$group` with expression keys and accumulators, `$addToSet` accumulator | 40/40 | 40/40 | supported, passing |
| Aggregation expressions | `$cond` (array and object form), `$ifNull`, `$literal` | 8/8 | 8/8 | supported, passing |
| Aggregation expressions | `$eq $ne $gt $gte $lt $lte`, `$and $or $not`, `$in` | 11/11 | 11/11 | supported, passing |
| Aggregation expressions | `$add $subtract $multiply $divide $mod` | 15/15 | 15/15 | supported, passing |
| Aggregation expressions | `$concat $toLower $toUpper` | 7/7 | 7/7 | supported, passing |
| Aggregation expressions | `$size $isArray $arrayElemAt $concatArrays $setUnion $slice` | 11/11 | 11/11 | supported, passing |
| Aggregation expressions | `$filter`, `$map`, `$$variables`, field paths | 12/12 | 12/12 | supported, passing |
| Aggregation expressions | `$dateToString` (formats, UTC and fixed offsets) | 7/7 | 7/7 | supported, passing |
| Not supported | `$dateToString` with named time zones (no tz database), `%U %V %G`, ObjectId/Timestamp input via `$toDate` | 1/4 probes | 1/4 probes | partly supported |
| Not supported | other expression operators (`$switch`, `$reduce`, `$let`, `$substr`, `$toString`, `$objectToArray`, `$round`, `$abs`, `$regexMatch`, `$max`, `$year`) | 0/1 probes | 0/1 probes | not supported |
| Not supported | `$lookup` with `pipeline` / `let` | 0/3 probes | 0/3 probes | not supported |
| Not supported | pipeline-style updates, `arrayFilters`, positional `$` / `$[]` / `$[id]`, `$bit` (also via findAndModify) | 0/10 probes | 0/10 probes | not supported |
| Not supported | `$replaceRoot`, `$facet` | 0/2 probes | 0/2 probes | not supported |
| Not supported | dropIndexes, renameCollection, collStats, dbStats | 0/4 probes | 0/4 probes | not supported |

| Driver | Suite | In-scope tests (core) | Beyond-scope probes (extra) |
|---|---|---|---|
| pymongo 4.19.0 | results5 suite (existing) | 308/308 (100.0%) | 24/34 |
| pymongo 4.19.0 | new in results6 | 773/773 (100.0%) | 1/15 |
| pymongo 4.19.0 | all | 1081/1081 (100.0%) | 25/49 |
| Node.js mongodb 7.7.0 | results5 suite (existing) | 308/308 (100.0%) | 23/34 |
| Node.js mongodb 7.7.0 | new in results6 | 773/773 (100.0%) | 1/15 |
| Node.js mongodb 7.7.0 | all | 1081/1081 (100.0%) | 24/49 |

In-scope tests failing on either driver: none

By group (in-scope tests):

| Group | Tests | pymongo | Node.js |
|---|---|---|---|
| handshake and admin | 14 | 14/14 | 14/14 |
| insert | 38 | 38/38 | 38/38 |
| find: filters and operators | 77 | 77/77 | 77/77 |
| find: projection | 33 | 33/33 | 33/33 |
| find: sort, skip, limit | 16 | 16/16 | 16/16 |
| cursors: batchSize, getMore, killCursors | 7 | 7/7 | 7/7 |
| count and distinct | 4 | 4/4 | 4/4 |
| aggregate | 36 | 36/36 | 36/36 |
| update | 52 | 52/52 | 52/52 |
| delete | 10 | 10/10 | 10/10 |
| collections and indexes | 22 | 22/22 | 22/22 |
| persistence: idle close and wake | 14 | 14/14 | 14/14 |
| concurrency | 4 | 4/4 | 4/4 |
| regex | 63 | 63/63 | 63/63 |
| findAndModify | 55 | 55/55 | 55/55 |
| update operators | 197 | 197/197 | 197/197 |
| query operators | 143 | 143/143 | 143/143 |
| $expr | 44 | 44/44 | 44/44 |
| aggregation expressions | 69 | 69/69 | 69/69 |
| aggregate: stages | 111 | 111/111 | 111/111 |
| aggregate: $lookup | 32 | 32/32 | 32/32 |
| aggregate: $group with expressions | 40 | 40/40 | 40/40 |

## Coverage after

Method: same as Step 0, but a feature counts as supported only if it has differential tests and **every** in-scope test for it passes on both drivers (`FEATURE_TESTS` in `coverage.py` maps each scanner feature to its tests; 47 features qualify, none without evidence). Per-repository counts of unsupported features: Rocket.Chat 62 → 21, parse-server 40 → 29, NodeBB 15 → 1, wekan 37 → 12, LibreChat 64 → 25, novu 29 → 5, appsmith 13 → 4, countly-server 186 → 144, payload 32 → 7, edx-platform 5 → 1. Repositories fully covered: **0 of 10 before, 0 of 10 after** (NodeBB and edx-platform are one feature away: `$collStats` and `$text`).

| Repository | Unsupported features used in application code | Count |
|---|---|---|
| Rocket.Chat | `$facet` ×8, `$range` ×8, `$ceil` ×6, `withTransaction / startTransaction` ×6, `$trunc` ×5, `watch (change streams)` ×5, `$hour` ×4, `$meta` ×4, `$substr` ×3, `$merge` ×2, `$bitsAllClear` ×1, `$bitsAllSet` ×1, `$bitsAnyClear` ×1, `$bitsAnySet` ×1, `$convert` ×1, `$floor` ×1, `$month` ×1, `$search` ×1, `$toString` ×1, `$unset (aggregation stage)` ×1, `$year` ×1 | 21 |
| parse-server | `$text` ×15, `$maxDistance` ×6, `$centerSphere` ×4, `$geoIntersects` ×4, `$geoWithin` ×4, `$merge` ×4, `$nearSphere` ×4, `$out` ×4, `$box` ×3, `$polygon` ×3, `$meta` ×2, `$search` ×2, `watch (change streams)` ×2, `withTransaction / startTransaction` ×2, `$dayOfMonth` ×1, `$dayOfWeek` ×1, `$dayOfYear` ×1, `$geoNear` ×1, `$geometry` ×1, `$hour` ×1, `$isoDayOfWeek` ×1, `$isoWeekYear` ×1, `$millisecond` ×1, `$minute` ×1, `$month` ×1, `$near` ×1, `$second` ×1, `$week` ×1, `$year` ×1 | 29 |
| NodeBB | `$collStats` ×1 | 1 |
| wekan | `$where` ×13, `$accumulator` ×3, `$function` ×3, `$merge` ×2, `$objectToArray` ×2, `$out` ×2, `$anyElementTrue` ×1, `$bit` ×1, `$collStats` ×1, `$range` ×1, `$toString` ×1, `watch (change streams)` ×1 | 12 |
| LibreChat | `$strLenBytes` ×11, `withTransaction / startTransaction` ×11, `$let` ×7, `$switch` ×5, `$trim` ×5, `$bitsAllSet` ×4, `$reduce` ×4, `$where` ×4, `$convert` ×3, `$indexOfCP` ×3, `$regexMatch` ×3, `$text` ×3, `$facet` ×2, `$reverseArray` ×2, `$sortArray` ×2, `$strLenCP` ×2, `$substrCP` ×2, `$anyElementTrue` ×1, `$bitsAnySet` ×1, `$indexOfBytes` ×1, `$merge` ×1, `$out` ×1, `$replaceRoot` ×1, `$substrBytes` ×1, `watch (change streams)` ×1 | 25 |
| novu | `withTransaction / startTransaction` ×22, `$mergeObjects` ×1, `$regexMatch` ×1, `$trim` ×1, `watch (change streams)` ×1 | 5 |
| appsmith | `withTransaction / startTransaction` ×16, `watch (change streams)` ×5, `$let` ×1, `$where` ×1 | 4 |
| countly-server | `$objectToArray` ×22, `$switch` ×11, `$merge` ×10, `$toString` ×8, `$facet` ×7, `$split` ×7, `$bitsAllClear` ×6, `$toDate` ×6, `$unionWith` ×6, `$arrayToObject` ×5, `$geometry` ×5, `$search` ×5, `watch (change streams)` ×5, `$bitsAnySet` ×4, `$function` ×4, `$geoWithin` ×4, `$text` ×4, `$toInt` ×4, `$where` ×4, `$bit` ×3, `$bitsAllSet` ×3, `$centerSphere` ×3, `$dateFromParts` ×3, `$indexOfBytes` ×3, `$month` ×3, `$out` ×3, `$year` ×3, `$accumulator` ×2, `$collStats` ×2, `$convert` ×2, `$dayOfMonth` ×2, `$geoIntersects` ×2, `$isNumber` ×2, `$maxDistance` ×2, `$mergeObjects` ×2, `$minDistance` ×2, `$near` ×2, `$nearSphere` ×2, `$regexMatch` ×2, `$replaceRoot` ×2, `$sortArray` ×2, `$strLenBytes` ×2, `$strLenCP` ×2, `$substrBytes` ×2, `$substrCP` ×2, `$toDouble` ×2, `$toLong` ×2, `$toObjectId` ×2, `$abs` ×1, `$allElementsTrue` ×1, `$anyElementTrue` ×1, `$binarySize` ×1, `$bitsAnyClear` ×1, `$bottom` ×1, `$bottomN` ×1, `$box` ×1, `$bsonSize` ×1, `$bucket` ×1, `$bucketAuto` ×1, `$ceil` ×1, `$center` ×1, `$cmp` ×1, `$dateAdd` ×1, `$dateDiff` ×1, `$dateFromString` ×1, `$dateSubtract` ×1, `$dateToParts` ×1, `$dateTrunc` ×1, `$dayOfWeek` ×1, `$dayOfYear` ×1, `$densify` ×1, `$documents` ×1, `$exp` ×1, `$fill` ×1, `$firstN` ×1, `$floor` ×1, `$geoNear` ×1, `$getField` ×1, `$graphLookup` ×1, `$hour` ×1, `$indexOfArray` ×1, `$indexOfCP` ×1, `$indexStats` ×1, `$isoDayOfWeek` ×1, `$isoWeek` ×1, `$isoWeekYear` ×1, `$jsonSchema` ×1, `$lastN` ×1, `$let` ×1, `$ln` ×1, `$log` ×1, `$log10` ×1, `$ltrim` ×1, `$maxN` ×1, `$median` ×1, `$meta` ×1, `$millisecond` ×1, `$minN` ×1, `$minute` ×1, `$percentile` ×1, `$planCacheStats` ×1, `$polygon` ×1, `$pow` ×1, `$rand` ×1, `$range` ×1, `$redact` ×1, `$reduce` ×1, `$regexFind` ×1, `$regexFindAll` ×1, `$replaceAll` ×1, `$replaceOne` ×1, `$replaceWith` ×1, `$reverseArray` ×1, `$round` ×1, `$rtrim` ×1, `$sample` ×1, `$sampleRate` ×1, `$searchMeta` ×1, `$second` ×1, `$setDifference` ×1, `$setEquals` ×1, `$setField` ×1, `$setIntersection` ×1, `$setIsSubset` ×1, `$setWindowFields` ×1, `$sortByCount` ×1, `$sqrt` ×1, `$stdDevPop` ×1, `$stdDevSamp` ×1, `$strcasecmp` ×1, `$substr` ×1, `$toBool` ×1, `$toDecimal` ×1, `$top` ×1, `$topN` ×1, `$trim` ×1, `$trunc` ×1, `$tsIncrement` ×1, `$tsSecond` ×1, `$unset (aggregation stage)` ×1, `$unsetField` ×1, `$week` ×1, `$zip` ×1, `withTransaction / startTransaction` ×1 | 144 |
| payload | `withTransaction / startTransaction` ×23, `$geometry` ×2, `$near` ×2, `$geoIntersects` ×1, `$geoWithin` ×1, `$sortArray` ×1, `$text` ×1 | 7 |
| edx-platform | `$text` ×1 | 1 |

Repositories fully covered (zero unsupported features): **0/10**

What is still missing is outside this task's scope: multi-document transactions (`withTransaction`, in 5 of the repositories) and change streams, geo and text queries (`$text`, `$near`, `$geoWithin`, ...), `$where` and `$function` / `$accumulator` (server-side JavaScript), `$merge` / `$out` / `$facet` / `$unionWith` / `$replaceRoot` / `$bucket` and other stages, `$lookup` with `pipeline` / `let`, bit-test query operators, `$bit`, and about eighty expression operators (`$switch`, `$let`, `$reduce`, `$toString`, `$objectToArray`, the date and string families, `$round` and friends). `$dateToString` is counted as supported, but only with `UTC` or fixed-offset time zones.

## Defects found by the differential tests, and fixed

The first full run of the new scripts failed 85 of 1,108 tests (24 of them beyond-scope probes that are expected to differ), so 61 in-scope failures; the rest passed on the first attempt. Each failure was a real difference from MongoDB 7.0.45 and was fixed in the server, not the test, except where noted.

1. **`$inc` and `$mul` widened 64-bit overflow to a double** (a results5 behaviour never tested before). MongoDB rejects it with code 2 ("Failed to apply $inc operations to current value ..."); only int32 overflow widens, to int64. Fixed for both operators.
2. **Modifiers were applied in the written order.** MongoDB applies all modifiers of an update in path order (one tree over all operators), so `{$set: {b: 1, a: 2}}` produces `a` before `b`, and a `$rename` is ordered by its target path. This shows in the field order of the stored document, which the tests compare.
3. **`$project` computed fields are appended after the included fields** (in specification order), even if the field exists earlier in the document, and a computed `_id` is an ordinary computed field (it comes last). The first implementation replaced fields in place.
4. **`$lookup` value semantics:** the local value is the aggregation field path (a missing field and an empty array count as null, an array contributes its elements); a foreign document matches on its path value and, for arrays, its elements. Foreign values reached through nested arrays differ from the query-language path, which the first version used.
5. **Aggregation field paths do not descend into nested arrays** (`$a.k` over `[{k: 1}, [{k: 2}], 5]` is `[1]`).
6. **`$expr` ran before the other predicates** of a filter, so `{_id: {$in: [1,2,3]}, $expr: {$gt: [{$size: "$arr"}, 1]}}` failed on a document without `arr` that MongoDB never evaluates it on; it is now evaluated last.
7. **`$elemMatch` operator form searched inside nested arrays** (`{$elemMatch: {$gt: 1, $lt: 3}}` matched `[[1, 2]]`); an element is now the value, not searched again.
8. **Error codes** that differed and were aligned: `$options` in both places (51075), embedded NUL (2), `$size` negative (2), `$type` empty list (9) and non-string / non-number argument (14), projection `$slice` (28667, 28724), `$addToSet` `$each` not an array (14), `$pop` on a non-array (14), `$rename` argument checks before the conflict check (2), `$divide` by zero (2, also at parse time for constants), `$mod` on non-numbers (16611), `$arrayElemAt` outside int32 (28691), `$filter` `limit` (327391 / 327392), `$slice` expression (28667, 28725-28729), variable names (9), empty and trailing-dot field paths (15998, 40353), empty `$project` (51272) and empty sub-projection (51270), `$project` path collisions (31249, 31250), unknown operator at the root of a `$project` field (31325), several operators in one expression (16410 in `$project`, 40181 in `$addFields`, 15983 elsewhere), `$unwind` paths (40352, 40353, 15998), `$group` accumulators (40235, 40238), upsert with a replacement whose `_id` differs from the filter's (66).
9. **Smaller semantics:** `$push` with a document `$sort` on non-document elements keeps their order instead of failing; `$pull` with `$and` / `$or` / `$nor` is a query on document elements; `$in` with a missing needle is false; `$toLower` / `$toUpper` convert dates to their ISO string; `%Z` in `$dateToString` is the offset in minutes; `$addFields` with an empty specification is a no-op and applies nested specifications element by element through arrays; `$$NOW` exists; `findandmodify` is accepted in lower case.
10. **Harness:** the Extended JSON parsers of both drivers turned `{"$regex": ..., "$options": ...}` into regex values, which silently changed what the tests asked. The generator now escapes those two keys and the harnesses restore them after parsing (the Node harness after its per-test clone).

Two order-related differences are properties of the design, not defects, and the tests were written around them: this server's natural order is `_id` order (MongoDB's is insertion order), so `$lookup` joined arrays and unsorted results can differ in order, and `$group` emits groups in first-seen order where MongoDB's order is arbitrary (`$addToSet` accumulator order is arbitrary on MongoDB too; the one older beyond-scope probe that compares it, `aggregate.extra.group_add_to_set`, passed on pymongo and failed on Node because the real server returned a different order in that run).

## Known differences and limits

- `arrayFilters`, positional `$` / `$[]` / `$[id]`, pipeline-style updates, `$lookup` `pipeline` / `let`, `$bit`: rejected with `NotImplemented` (238); MongoDB runs them. Beyond-scope probes.
- `$dateToString` with named time zones (`Europe/Rome`) and `%U %V %G`: errors here, values on MongoDB.
- Expression operators outside the list above return code 168 ("Unrecognized expression").
- `$sort` on an array-valued `_id` produced by `$group` orders `[]` against `null` differently from MongoDB (seen while writing the group tests; ordinary `find` and aggregate sorts on array fields match).
- Everything is evaluated in memory over documents scanned from the tenant (index definitions are recorded, not used); `$lookup` reads the whole foreign collection.
- `update.extra.current_date`, an older probe, compares the stored `$currentDate` value, which differs by design; the new `$currentDate` tests compare types and recency instead.

## Performance sanity

The query-engine changes do not touch the storage path. `bench_latency.py` (1,000 documents of about 1 KB, warm, p50): find by `_id` 0.14 ms, filtered find (10 of 1,000) 3.5 ms (results5: 3.1 ms; a first run of this session gave 4.1 ms, so the spread is run-to-run noise on this machine), insert 3.4 ms (results5: 2.9 ms). Object-store requests per operation (`bench_requests.py`) are unchanged for reads: a cold read is 1 PUT-tier and 10 GET-tier requests, a warm read none (a cold durable insert measured 5.05 PUT-tier and 27.55 GET-tier requests against 5.00 and 27.10 in results5, which is background-compaction timing).

## Reproduce

```
mongo-tests/run_all.sh                          # builds, starts mongo:7.0 and the servers, runs both suites, benchmarks, report.py
mongo-tests/.venv/bin/python mongo-tests/report6.py   # the tables above, from mongo-tests/out/results_py.json and results_node.json
python3 tools/compat-scan/coverage.py --profile baseline --markdown   # Step 0
python3 tools/compat-scan/coverage.py --profile after --markdown      # coverage after (reads the test results)
```

Single tests: `mongo-tests/.venv/bin/python mongo-tests/run_py.py --only regex.` and `node mongo-tests/node/run_node.js --only=regex.`. The scan data (`tools/compat-scan/results.json`), `compat-scan.md` and `results.md` to `results5.md` are unchanged.
