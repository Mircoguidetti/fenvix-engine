
**index options recorded but not honoured** — 7 features, 630 call sites, up to 9/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `index: sparse` | 9/10 | 144 | LibreChat, NodeBB, Rocket.Chat, countly-server, edx-platform, novu, parse-server, payload, wekan |
| `index: partialFilterExpression` | 8/10 | 107 | LibreChat, NodeBB, Rocket.Chat, countly-server, novu, parse-server, payload, wekan |
| `index: TTL (expireAfterSeconds)` | 8/10 | 70 | LibreChat, NodeBB, Rocket.Chat, appsmith, countly-server, parse-server, payload, wekan |
| `index: compound` | 6/10 | 294 | LibreChat, NodeBB, appsmith, countly-server, novu, wekan |
| `index: text` | 2/10 | 10 | Rocket.Chat, countly-server |
| `index: hashed / wildcard` | 1/10 | 3 | countly-server |
| `index: 2dsphere / 2d (geo)` | 1/10 | 2 | payload |

**transactions** — 1 features, 81 call sites, up to 7/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `withTransaction / startTransaction` | 7/10 | 81 | LibreChat, Rocket.Chat, appsmith, countly-server, novu, parse-server, payload |

**change streams** — 1 features, 20 call sites, up to 7/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `watch (change streams)` | 7/10 | 20 | LibreChat, Rocket.Chat, appsmith, countly-server, novu, parse-server, wekan |

**query operators** — 19 features, 124 call sites, up to 5/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `$text` | 5/10 | 24 | LibreChat, countly-server, edx-platform, parse-server, payload |
| `$geoWithin` | 3/10 | 9 | countly-server, parse-server, payload |
| `$bitsAllSet` | 3/10 | 8 | LibreChat, Rocket.Chat, countly-server |
| `$geometry` | 3/10 | 8 | countly-server, parse-server, payload |
| `$search` | 3/10 | 8 | Rocket.Chat, countly-server, parse-server |
| `$geoIntersects` | 3/10 | 7 | countly-server, parse-server, payload |
| `$meta` | 3/10 | 7 | Rocket.Chat, countly-server, parse-server |
| `$bitsAnySet` | 3/10 | 6 | LibreChat, Rocket.Chat, countly-server |
| `$near` | 3/10 | 5 | countly-server, parse-server, payload |
| `$maxDistance` | 2/10 | 8 | countly-server, parse-server |
| `$bitsAllClear` | 2/10 | 7 | Rocket.Chat, countly-server |
| `$centerSphere` | 2/10 | 7 | countly-server, parse-server |
| `$nearSphere` | 2/10 | 6 | countly-server, parse-server |
| `$box` | 2/10 | 4 | countly-server, parse-server |
| `$polygon` | 2/10 | 4 | countly-server, parse-server |
| `$bitsAnyClear` | 2/10 | 2 | Rocket.Chat, countly-server |
| `$minDistance` | 1/10 | 2 | countly-server |
| `$center` | 1/10 | 1 | countly-server |
| `$jsonSchema` | 1/10 | 1 | countly-server |

**aggregation stages** — 22 features, 77 call sites, up to 5/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `$merge` | 5/10 | 19 | LibreChat, Rocket.Chat, countly-server, parse-server, wekan |
| `$out` | 4/10 | 10 | LibreChat, countly-server, parse-server, wekan |
| `$facet` | 3/10 | 17 | LibreChat, Rocket.Chat, countly-server |
| `$collStats` | 3/10 | 4 | NodeBB, countly-server, wekan |
| `$replaceRoot` | 2/10 | 3 | LibreChat, countly-server |
| `$geoNear` | 2/10 | 2 | countly-server, parse-server |
| `$unset (aggregation stage)` | 2/10 | 2 | Rocket.Chat, countly-server |
| `$unionWith` | 1/10 | 6 | countly-server |
| `$bucket` | 1/10 | 1 | countly-server |
| `$bucketAuto` | 1/10 | 1 | countly-server |
| `$densify` | 1/10 | 1 | countly-server |
| `$documents` | 1/10 | 1 | countly-server |
| `$fill` | 1/10 | 1 | countly-server |
| `$graphLookup` | 1/10 | 1 | countly-server |
| `$indexStats` | 1/10 | 1 | countly-server |
| `$planCacheStats` | 1/10 | 1 | countly-server |
| `$redact` | 1/10 | 1 | countly-server |
| `$replaceWith` | 1/10 | 1 | countly-server |
| `$sample` | 1/10 | 1 | countly-server |
| `$searchMeta` | 1/10 | 1 | countly-server |
| `$setWindowFields` | 1/10 | 1 | countly-server |
| `$sortByCount` | 1/10 | 1 | countly-server |

**server-side JavaScript** — 3 features, 34 call sites, up to 4/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `$where` | 4/10 | 22 | LibreChat, appsmith, countly-server, wekan |
| `$function` | 2/10 | 7 | countly-server, wekan |
| `$accumulator` | 2/10 | 5 | countly-server, wekan |

**aggregation expressions** — 84 features, 264 call sites, up to 3/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `$range` | 3/10 | 10 | Rocket.Chat, countly-server, wekan |
| `$toString` | 3/10 | 10 | Rocket.Chat, countly-server, wekan |
| `$let` | 3/10 | 9 | LibreChat, appsmith, countly-server |
| `$trim` | 3/10 | 7 | LibreChat, countly-server, novu |
| `$convert` | 3/10 | 6 | LibreChat, Rocket.Chat, countly-server |
| `$hour` | 3/10 | 6 | Rocket.Chat, countly-server, parse-server |
| `$regexMatch` | 3/10 | 6 | LibreChat, countly-server, novu |
| `$month` | 3/10 | 5 | Rocket.Chat, countly-server, parse-server |
| `$sortArray` | 3/10 | 5 | LibreChat, countly-server, payload |
| `$year` | 3/10 | 5 | Rocket.Chat, countly-server, parse-server |
| `$anyElementTrue` | 3/10 | 3 | LibreChat, countly-server, wekan |
| `$objectToArray` | 2/10 | 24 | countly-server, wekan |
| `$switch` | 2/10 | 16 | LibreChat, countly-server |
| `$strLenBytes` | 2/10 | 13 | LibreChat, countly-server |
| `$ceil` | 2/10 | 7 | Rocket.Chat, countly-server |
| `$trunc` | 2/10 | 6 | Rocket.Chat, countly-server |
| `$reduce` | 2/10 | 5 | LibreChat, countly-server |
| `$indexOfBytes` | 2/10 | 4 | LibreChat, countly-server |
| `$indexOfCP` | 2/10 | 4 | LibreChat, countly-server |
| `$strLenCP` | 2/10 | 4 | LibreChat, countly-server |
| `$substr` | 2/10 | 4 | Rocket.Chat, countly-server |
| `$substrCP` | 2/10 | 4 | LibreChat, countly-server |
| `$dayOfMonth` | 2/10 | 3 | countly-server, parse-server |
| `$reverseArray` | 2/10 | 3 | LibreChat, countly-server |
| `$substrBytes` | 2/10 | 3 | LibreChat, countly-server |
| `$dayOfWeek` | 2/10 | 2 | countly-server, parse-server |
| `$dayOfYear` | 2/10 | 2 | countly-server, parse-server |
| `$floor` | 2/10 | 2 | Rocket.Chat, countly-server |
| `$isoDayOfWeek` | 2/10 | 2 | countly-server, parse-server |
| `$isoWeekYear` | 2/10 | 2 | countly-server, parse-server |
| `$millisecond` | 2/10 | 2 | countly-server, parse-server |
| `$minute` | 2/10 | 2 | countly-server, parse-server |
| `$second` | 2/10 | 2 | countly-server, parse-server |
| `$week` | 2/10 | 2 | countly-server, parse-server |
| `$split` | 1/10 | 7 | countly-server |
| `$toDate` | 1/10 | 6 | countly-server |
| `$arrayToObject` | 1/10 | 5 | countly-server |
| `$toInt` | 1/10 | 4 | countly-server |
| `$dateFromParts` | 1/10 | 3 | countly-server |
| `$isNumber` | 1/10 | 2 | countly-server |
| `$toDouble` | 1/10 | 2 | countly-server |
| `$toLong` | 1/10 | 2 | countly-server |
| `$toObjectId` | 1/10 | 2 | countly-server |
| `$abs` | 1/10 | 1 | countly-server |
| `$allElementsTrue` | 1/10 | 1 | countly-server |
| `$binarySize` | 1/10 | 1 | countly-server |
| `$bsonSize` | 1/10 | 1 | countly-server |
| `$cmp` | 1/10 | 1 | countly-server |
| `$dateAdd` | 1/10 | 1 | countly-server |
| `$dateDiff` | 1/10 | 1 | countly-server |
| `$dateFromString` | 1/10 | 1 | countly-server |
| `$dateSubtract` | 1/10 | 1 | countly-server |
| `$dateToParts` | 1/10 | 1 | countly-server |
| `$dateTrunc` | 1/10 | 1 | countly-server |
| `$exp` | 1/10 | 1 | countly-server |
| `$getField` | 1/10 | 1 | countly-server |
| `$indexOfArray` | 1/10 | 1 | countly-server |
| `$isoWeek` | 1/10 | 1 | countly-server |
| `$ln` | 1/10 | 1 | countly-server |
| `$log` | 1/10 | 1 | countly-server |
| `$log10` | 1/10 | 1 | countly-server |
| `$ltrim` | 1/10 | 1 | countly-server |
| `$pow` | 1/10 | 1 | countly-server |
| `$rand` | 1/10 | 1 | countly-server |
| `$regexFind` | 1/10 | 1 | countly-server |
| `$regexFindAll` | 1/10 | 1 | countly-server |
| `$replaceAll` | 1/10 | 1 | countly-server |
| `$replaceOne` | 1/10 | 1 | countly-server |
| `$round` | 1/10 | 1 | countly-server |
| `$rtrim` | 1/10 | 1 | countly-server |
| `$sampleRate` | 1/10 | 1 | countly-server |
| `$setDifference` | 1/10 | 1 | countly-server |
| `$setEquals` | 1/10 | 1 | countly-server |
| `$setField` | 1/10 | 1 | countly-server |
| `$setIntersection` | 1/10 | 1 | countly-server |
| `$setIsSubset` | 1/10 | 1 | countly-server |
| `$sqrt` | 1/10 | 1 | countly-server |
| `$strcasecmp` | 1/10 | 1 | countly-server |
| `$toBool` | 1/10 | 1 | countly-server |
| `$toDecimal` | 1/10 | 1 | countly-server |
| `$tsIncrement` | 1/10 | 1 | countly-server |
| `$tsSecond` | 1/10 | 1 | countly-server |
| `$unsetField` | 1/10 | 1 | countly-server |
| `$zip` | 1/10 | 1 | countly-server |

**aggregation accumulators** — 13 features, 15 call sites, up to 2/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `$mergeObjects` | 2/10 | 3 | countly-server, novu |
| `$bottom` | 1/10 | 1 | countly-server |
| `$bottomN` | 1/10 | 1 | countly-server |
| `$firstN` | 1/10 | 1 | countly-server |
| `$lastN` | 1/10 | 1 | countly-server |
| `$maxN` | 1/10 | 1 | countly-server |
| `$median` | 1/10 | 1 | countly-server |
| `$minN` | 1/10 | 1 | countly-server |
| `$percentile` | 1/10 | 1 | countly-server |
| `$stdDevPop` | 1/10 | 1 | countly-server |
| `$stdDevSamp` | 1/10 | 1 | countly-server |
| `$top` | 1/10 | 1 | countly-server |
| `$topN` | 1/10 | 1 | countly-server |

**update operators** — 1 features, 4 call sites, up to 2/10 apps

| Feature | Apps | Call sites | Apps (names) |
|---|---|---|---|
| `$bit` | 2/10 | 4 | countly-server, wekan |
