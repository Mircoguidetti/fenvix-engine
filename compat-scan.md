# MongoDB feature usage in real applications

Purpose: rank MongoDB features by how many real applications use them, so the most-used missing ones are implemented first. Scanned 2026-10-26 with `tools/compat-scan/scan.py` over shallow clones (`git clone --depth 1`) of public repositories, one at a time, each deleted after its scan. No GitHub API and no paid service was used. "Our status" is taken from the compatibility table in `results5.md`; where that table has no row (`bulkWrite`, transactions) I probed the feature directly against `mongo:7.0` and the server.

## 1. Repositories

| Repository | Commit | Result | MongoDB dependency found | Application files | Test files | Size of clone (KiB) |
|---|---|---|---|---|---|---|
| Rocket.Chat | `1826a86` | scanned | npm: mongodb (package.json) | 8514 | 2412 | 303280 |
| parse-server | `9805d3e` | scanned | npm: mongodb (package.json) | 216 | 161 | 11976 |
| NodeBB | `ce62dcd` | scanned | npm: mongodb (install/package.json) | 853 | 99 | 45516 |
| wekan | `43c8606` | scanned | npm: mongodb (package.json); meteor: built-in mongo package (npm-packages/meteor-globals/meteor-tests/.meteor/packages) | 2030 | 2835 | 339128 |
| LibreChat | `a378657` | scanned | npm: mongodb (packages/api/package.json); npm: mongoose (packages/api/package.json) | 3681 | 2916 | 124208 |
| novu | `d7ff70c` | scanned | npm: mongoose (libs/dal/package.json); npm: mongoose plugin/typegoose (libs/dal/package.json) | 8305 | 1242 | 466968 |
| appsmith | `b0b5020` | scanned | jvm: spring-data-mongodb / mongodb-driver (app/server/appsmith-server/pom.xml); npm: mongodb (app/client/packages/rts/package.json) | 6868 | 2184 | 700260 |
| countly-server | `70a0e28` | scanned | npm: mongodb (package.json) | 850 | 268 | 164536 |
| payload | `aebbf75` | scanned | npm: mongoose (package.json); npm: mongodb (packages/db-mongodb/package.json); npm: mongoose plugin/typegoose (packages/db-mongodb/package.json) | 5283 | 2631 | 155632 |
| edx-platform | `87cd381` | scanned | python: pymongo/motor/mongoengine (pyproject/setup) (pyproject.toml) | 3693 | 1738 | 212036 |

No repository was skipped: every clone succeeded and every repository has a MongoDB dependency.

Totals over the 10 scanned repositories: 40293 application files and 16486 test files scanned; 72 files skipped as larger than 1 MB, minified or generated.

## 2. What was counted, and how reliable it is

- **Scope.** Files with extensions `.js .jsx .mjs .cjs .ts .tsx .py .java .kt .scala .groovy .go .rb .php .cs .vue .svelte .coffee`. Excluded: `node_modules`, `vendor`, `third_party`, `dist`, `build`, `out`, `target`, `.next`, `coverage`, virtualenvs, `.git`, generated folders, lockfiles, `*.min.*`, bundles, source maps, files over 1 MB and files whose first lines exceed 3,000 characters. **Test files** (paths under `test(s)`, `__tests__`, `spec`, `e2e`, `cypress`, `fixtures`, `mocks`, `src/test`, `*.test.*`, `*.spec.*`, `test_*.py`, `*Test.java`, ...) are counted separately and never contribute to the main metric.
- **Main metric:** the number of repositories in which the feature appears at least once in *application* code. Secondary: total occurrences in application code, and (separate column) occurrences that appear only in tests.
- **(a) Operators.** A `$name` counts when it is a quoted string (`"$in"`, `'$lookup'`) or an unquoted object key (`$in: [...]`, also inside Spring `@Query("{ $or: [...] }")`) and `name` is a known MongoDB operator. GraphQL variable declarations (`$limit: Int`) are filtered out; `$comment` is ignored (JSON-schema keyword).
- **Same name, two roles.** `$min`, `$max`, `$push`, `$addToSet`, `$set` and `$unset` exist both as update operators and as aggregation accumulators/stages, and their status differs. The role is guessed from the surrounding ~800 characters (an aggregation context with `$group`/`aggregate`/`pipeline` versus an `update`/`$inc`/`upsert` context). This is a heuristic and can misclassify.
- **(b) Methods** are counted only in files that look MongoDB-related (mention `mongo`, `mongoose`, `.collection(`, `Model`, `Schema(`, `@Document`, ...), because names like `aggregate`, `distinct` and `watch` are common elsewhere. Mongoose shortcuts are included (`findByIdAndUpdate` counts as `findOneAndUpdate`, `findByIdAndDelete` as `findOneAndDelete`).
- **(c) Indexes** are detected from option names and call shapes in MongoDB-related files (`expireAfterSeconds`, `partialFilterExpression`, `sparse: true`, `unique: true`, `'2dsphere'`, `'text'`, `.index({a:1, b:-1})`, `createIndex`, `create_index([...])`, `@CompoundIndex`, `@Indexed`, mongoengine `meta = {'indexes': [...]}`). Compound-index detection needs the key list to be written inline at the call, so indexes built from variables are missed.
- **Over-counting:** string matching cannot tell a MongoDB query from a similarly shaped literal (for example the same operator names in a query-builder that merely emits them, a JSON fixture inside a source file, or API documentation strings). Repositories that *translate* queries (Parse Server, Payload's database adapters) show many operators that applications built on them would not use directly.
- **Under-counting:** Java/Kotlin code that builds queries with Spring Data's `Criteria`/`Query` API or Python code that uses ODM query builders (Appsmith and edx-platform show few `$` strings for this reason); operators built from variables or concatenated strings, queries in `.json`/`.yml`/`.graphql` files, indexes defined through helper functions, and any language not in the extension list.
- **Several repositories are libraries or platforms** (Parse Server, Payload, Rocket.Chat, Novu), so their usage reflects what a framework needs, which is a fair proxy for what a MongoDB-compatible server must support to host them.

## 3. Ranking of all features used in application code

238 features appear in at least one repository's application code. Sorted by number of repositories (of 10), then by occurrences. Features that appear only in tests are not listed.

| # | Feature | Kind | Repos using it (application code) | Occurrences (app) | Occurrences (tests only) | Our status |
|---|---|---|---|---|---|---|
| 1 | `find (reference)` | driver/ODM method | 10/10 | 4298 | 5680 | supported |
| 2 | `$set (update)` | update operator | 10/10 | 2693 | 1959 | supported |
| 3 | `$in` | query operator | 10/10 | 1949 | 850 | supported |
| 4 | `$exists` | query operator | 10/10 | 1349 | 230 | supported |
| 5 | `index: any definition` | index definition | 10/10 | 818 | 133 | supported with differences |
| 6 | `$eq` | query operator | 10/10 | 282 | 85 | supported |
| 7 | `$regex` | query operator | 10/10 | 262 | 198 | not supported |
| 8 | `$group` | aggregation stage | 10/10 | 230 | 69 | supported |
| 9 | `aggregate` | driver/ODM method | 10/10 | 201 | 91 | supported with differences |
| 10 | `$or` | query operator | 9/10 | 811 | 265 | supported |
| 11 | `$ne` | query operator | 9/10 | 731 | 255 | supported |
| 12 | `findOneAndUpdate` | driver/ODM method | 9/10 | 416 | 350 | not supported |
| 13 | `updateMany / update_many` | driver/ODM method | 9/10 | 325 | 80 | supported |
| 14 | `countDocuments / count_documents` | driver/ODM method | 9/10 | 288 | 926 | supported |
| 15 | `$gt` | query operator | 9/10 | 274 | 79 | supported |
| 16 | `$lt` | query operator | 9/10 | 269 | 50 | supported |
| 17 | `$gte` | query operator | 9/10 | 253 | 66 | supported |
| 18 | `bulkWrite` | driver/ODM method | 9/10 | 239 | 173 | supported |
| 19 | `$nin` | query operator | 9/10 | 230 | 71 | supported |
| 20 | `index: unique` | index definition | 9/10 | 221 | 62 | supported |
| 21 | `$inc` | update operator | 9/10 | 220 | 47 | supported |
| 22 | `$project` | aggregation stage | 9/10 | 200 | 46 | supported |
| 23 | `index: sparse` | index definition | 9/10 | 144 | 16 | supported with differences |
| 24 | `$options` | query operator | 9/10 | 93 | 59 | not supported |
| 25 | `$unset (update)` | update operator | 8/10 | 576 | 265 | supported |
| 26 | `$and` | query operator | 8/10 | 336 | 124 | supported |
| 27 | `$lte` | query operator | 8/10 | 266 | 64 | supported |
| 28 | `$sort` | aggregation stage | 8/10 | 123 | 27 | supported |
| 29 | `$push (update)` | update operator | 8/10 | 121 | 134 | not supported |
| 30 | `index: partialFilterExpression` | index definition | 8/10 | 107 | 23 | supported with differences |
| 31 | `$size` | query operator | 8/10 | 81 | 9 | not supported |
| 32 | `index: TTL (expireAfterSeconds)` | index definition | 8/10 | 70 | 7 | supported with differences |
| 33 | `distinct` | driver/ODM method | 8/10 | 50 | 50 | supported |
| 34 | `$match` | aggregation stage | 7/10 | 326 | 97 | supported |
| 35 | `$sum` | aggregation accumulator | 7/10 | 231 | 21 | supported |
| 36 | `$pull` | update operator | 7/10 | 193 | 55 | not supported |
| 37 | `$elemMatch` | query operator | 7/10 | 158 | 89 | not supported |
| 38 | `$addToSet (update)` | update operator | 7/10 | 129 | 59 | not supported |
| 39 | `$cond` | aggregation expression | 7/10 | 108 | 3 | not supported |
| 40 | `$each` | update operator | 7/10 | 86 | 44 | not supported |
| 41 | `$limit` | aggregation stage | 7/10 | 84 | 15 | supported |
| 42 | `withTransaction / startTransaction` | driver/ODM method | 7/10 | 81 | 83 | not supported |
| 43 | `findOneAndDelete` | driver/ODM method | 7/10 | 52 | 12 | not supported |
| 44 | `$nor` | query operator | 7/10 | 39 | 44 | supported |
| 45 | `watch (change streams)` | driver/ODM method | 7/10 | 20 | 10 | not supported |
| 46 | `index: compound` | index definition | 6/10 | 294 | 13 | supported with differences |
| 47 | `$setOnInsert` | update operator | 6/10 | 142 | 35 | not supported |
| 48 | `$ifNull` | aggregation expression | 6/10 | 92 | 1 | not supported |
| 49 | `$lookup` | aggregation stage | 6/10 | 84 | 49 | not supported |
| 50 | `$unwind` | aggregation stage | 6/10 | 69 | 3 | not supported |
| 51 | `insertMany / insert_many` | driver/ODM method | 6/10 | 47 | 357 | supported |
| 52 | `$skip` | aggregation stage | 6/10 | 40 | 4 | supported |
| 53 | `$expr` | query operator | 6/10 | 30 | 17 | not supported |
| 54 | `$all` | query operator | 6/10 | 26 | 27 | not supported |
| 55 | `$map` | aggregation expression | 6/10 | 21 | 1 | not supported |
| 56 | `startSession` | driver/ODM method | 6/10 | 19 | 13 | supported |
| 57 | `$rename` | update operator | 6/10 | 15 | 26 | not supported |
| 58 | `$first` | aggregation accumulator | 5/10 | 76 | 4 | supported |
| 59 | `$type` | query operator | 5/10 | 74 | 26 | not supported |
| 60 | `$addFields` | aggregation stage | 5/10 | 67 | 20 | not supported |
| 61 | `$max (update)` | update operator | 5/10 | 41 | 13 | not supported |
| 62 | `$count` | aggregation stage | 5/10 | 37 | 24 | supported |
| 63 | `$push (aggregation accumulator)` | aggregation accumulator | 5/10 | 36 | 5 | supported |
| 64 | `$not` | query operator | 5/10 | 34 | 17 | supported |
| 65 | `$slice` | query operator | 5/10 | 30 | 12 | not supported |
| 66 | `$text` | query operator | 5/10 | 24 | 25 | not supported |
| 67 | `$pullAll` | update operator | 5/10 | 20 | 24 | not supported |
| 68 | `$merge` | aggregation stage | 5/10 | 19 | 7 | not supported |
| 69 | `$isArray` | aggregation expression | 5/10 | 15 | 0 | not supported |
| 70 | `$min (aggregation accumulator)` | aggregation accumulator | 5/10 | 12 | 3 | supported |
| 71 | `$literal` | aggregation expression | 5/10 | 9 | 4 | not supported |
| 72 | `$max (aggregation accumulator)` | aggregation accumulator | 5/10 | 9 | 4 | supported |
| 73 | `$concatArrays` | aggregation expression | 5/10 | 7 | 0 | not supported |
| 74 | `$filter` | aggregation expression | 4/10 | 31 | 2 | not supported |
| 75 | `$where` | query operator | 4/10 | 22 | 85 | not supported |
| 76 | `$arrayElemAt` | aggregation expression | 4/10 | 18 | 0 | not supported |
| 77 | `$concat` | aggregation expression | 4/10 | 15 | 2 | not supported |
| 78 | `$add` | aggregation expression | 4/10 | 13 | 3 | not supported |
| 79 | `$out` | aggregation stage | 4/10 | 10 | 12 | not supported |
| 80 | `$dateToString` | aggregation expression | 4/10 | 7 | 0 | not supported |
| 81 | `$multiply` | aggregation expression | 4/10 | 7 | 4 | not supported |
| 82 | `$set (aggregation stage)` | aggregation stage | 4/10 | 6 | 9 | not supported |
| 83 | `$setUnion` | aggregation expression | 4/10 | 6 | 0 | not supported |
| 84 | `$addToSet (aggregation accumulator)` | aggregation accumulator | 3/10 | 26 | 3 | not supported |
| 85 | `$facet` | aggregation stage | 3/10 | 17 | 18 | not supported |
| 86 | `$toLower` | aggregation expression | 3/10 | 12 | 1 | not supported |
| 87 | `$min (update)` | update operator | 3/10 | 10 | 9 | not supported |
| 88 | `$range` | aggregation expression | 3/10 | 10 | 0 | not supported |
| 89 | `$toString` | aggregation expression | 3/10 | 10 | 0 | not supported |
| 90 | `$geoWithin` | query operator | 3/10 | 9 | 23 | not supported |
| 91 | `$let` | aggregation expression | 3/10 | 9 | 3 | not supported |
| 92 | `$subtract` | aggregation expression | 3/10 | 9 | 0 | not supported |
| 93 | `$bitsAllSet` | query operator | 3/10 | 8 | 27 | not supported |
| 94 | `$geometry` | query operator | 3/10 | 8 | 7 | not supported |
| 95 | `$search` | query operator | 3/10 | 8 | 21 | not supported |
| 96 | `$geoIntersects` | query operator | 3/10 | 7 | 7 | not supported |
| 97 | `$meta` | query operator | 3/10 | 7 | 15 | not supported |
| 98 | `$trim` | aggregation expression | 3/10 | 7 | 0 | not supported |
| 99 | `$bitsAnySet` | query operator | 3/10 | 6 | 3 | not supported |
| 100 | `$convert` | aggregation expression | 3/10 | 6 | 4 | not supported |
| 101 | `$hour` | aggregation expression | 3/10 | 6 | 0 | not supported |
| 102 | `$regexMatch` | aggregation expression | 3/10 | 6 | 3 | not supported |
| 103 | `findOneAndReplace` | driver/ODM method | 3/10 | 6 | 6 | not supported |
| 104 | `$month` | aggregation expression | 3/10 | 5 | 4 | not supported |
| 105 | `$near` | query operator | 3/10 | 5 | 4 | not supported |
| 106 | `$sortArray` | aggregation expression | 3/10 | 5 | 1 | not supported |
| 107 | `$year` | aggregation expression | 3/10 | 5 | 4 | not supported |
| 108 | `$collStats` | aggregation stage | 3/10 | 4 | 3 | not supported |
| 109 | `$position` | update operator | 3/10 | 4 | 3 | not supported |
| 110 | `$anyElementTrue` | aggregation expression | 3/10 | 3 | 0 | not supported |
| 111 | `$pop` | update operator | 3/10 | 3 | 10 | not supported |
| 112 | `$objectToArray` | aggregation expression | 2/10 | 24 | 0 | not supported |
| 113 | `$divide` | aggregation expression | 2/10 | 17 | 0 | not supported |
| 114 | `$switch` | aggregation expression | 2/10 | 16 | 3 | not supported |
| 115 | `$strLenBytes` | aggregation expression | 2/10 | 13 | 1 | not supported |
| 116 | `index: text` | index definition | 2/10 | 10 | 3 | supported with differences |
| 117 | `$maxDistance` | query operator | 2/10 | 8 | 6 | not supported |
| 118 | `$bitsAllClear` | query operator | 2/10 | 7 | 3 | not supported |
| 119 | `$ceil` | aggregation expression | 2/10 | 7 | 0 | not supported |
| 120 | `$centerSphere` | query operator | 2/10 | 7 | 11 | not supported |
| 121 | `$function` | aggregation accumulator | 2/10 | 7 | 19 | not supported |
| 122 | `$currentDate` | update operator | 2/10 | 6 | 12 | not supported |
| 123 | `$nearSphere` | query operator | 2/10 | 6 | 8 | not supported |
| 124 | `$trunc` | aggregation expression | 2/10 | 6 | 1 | not supported |
| 125 | `$accumulator` | aggregation accumulator | 2/10 | 5 | 10 | not supported |
| 126 | `$reduce` | aggregation expression | 2/10 | 5 | 0 | not supported |
| 127 | `$bit` | update operator | 2/10 | 4 | 27 | not supported |
| 128 | `$box` | query operator | 2/10 | 4 | 1 | not supported |
| 129 | `$indexOfBytes` | aggregation expression | 2/10 | 4 | 0 | not supported |
| 130 | `$indexOfCP` | aggregation expression | 2/10 | 4 | 0 | not supported |
| 131 | `$polygon` | query operator | 2/10 | 4 | 11 | not supported |
| 132 | `$strLenCP` | aggregation expression | 2/10 | 4 | 0 | not supported |
| 133 | `$substr` | aggregation expression | 2/10 | 4 | 1 | not supported |
| 134 | `$substrCP` | aggregation expression | 2/10 | 4 | 1 | not supported |
| 135 | `$dayOfMonth` | aggregation expression | 2/10 | 3 | 6 | not supported |
| 136 | `$mergeObjects` | aggregation accumulator | 2/10 | 3 | 3 | not supported |
| 137 | `$replaceRoot` | aggregation stage | 2/10 | 3 | 6 | not supported |
| 138 | `$reverseArray` | aggregation expression | 2/10 | 3 | 0 | not supported |
| 139 | `$substrBytes` | aggregation expression | 2/10 | 3 | 0 | not supported |
| 140 | `$avg` | aggregation accumulator | 2/10 | 2 | 4 | supported |
| 141 | `$bitsAnyClear` | query operator | 2/10 | 2 | 3 | not supported |
| 142 | `$dayOfWeek` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 143 | `$dayOfYear` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 144 | `$floor` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 145 | `$geoNear` | aggregation stage | 2/10 | 2 | 3 | not supported |
| 146 | `$isoDayOfWeek` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 147 | `$isoWeekYear` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 148 | `$millisecond` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 149 | `$minute` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 150 | `$second` | aggregation expression | 2/10 | 2 | 1 | not supported |
| 151 | `$unset (aggregation stage)` | aggregation stage | 2/10 | 2 | 4 | not supported |
| 152 | `$week` | aggregation expression | 2/10 | 2 | 0 | not supported |
| 153 | `$split` | aggregation expression | 1/10 | 7 | 0 | not supported |
| 154 | `$toDate` | aggregation expression | 1/10 | 6 | 0 | not supported |
| 155 | `$unionWith` | aggregation stage | 1/10 | 6 | 6 | not supported |
| 156 | `$arrayToObject` | aggregation expression | 1/10 | 5 | 0 | not supported |
| 157 | `$toInt` | aggregation expression | 1/10 | 4 | 0 | not supported |
| 158 | `$dateFromParts` | aggregation expression | 1/10 | 3 | 2 | not supported |
| 159 | `index: hashed / wildcard` | index definition | 1/10 | 3 | 0 | supported with differences |
| 160 | `$isNumber` | aggregation expression | 1/10 | 2 | 1 | not supported |
| 161 | `$minDistance` | query operator | 1/10 | 2 | 1 | not supported |
| 162 | `$mod` | query operator | 1/10 | 2 | 4 | not supported |
| 163 | `$mul` | update operator | 1/10 | 2 | 8 | not supported |
| 164 | `$toDouble` | aggregation expression | 1/10 | 2 | 0 | not supported |
| 165 | `$toLong` | aggregation expression | 1/10 | 2 | 0 | not supported |
| 166 | `$toObjectId` | aggregation expression | 1/10 | 2 | 0 | not supported |
| 167 | `index: 2dsphere / 2d (geo)` | index definition | 1/10 | 2 | 5 | supported with differences |
| 168 | `$abs` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 169 | `$allElementsTrue` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 170 | `$binarySize` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 171 | `$bottom` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 172 | `$bottomN` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 173 | `$bsonSize` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 174 | `$bucket` | aggregation stage | 1/10 | 1 | 4 | not supported |
| 175 | `$bucketAuto` | aggregation stage | 1/10 | 1 | 1 | not supported |
| 176 | `$center` | query operator | 1/10 | 1 | 1 | not supported |
| 177 | `$cmp` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 178 | `$dateAdd` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 179 | `$dateDiff` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 180 | `$dateFromString` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 181 | `$dateSubtract` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 182 | `$dateToParts` | aggregation expression | 1/10 | 1 | 2 | not supported |
| 183 | `$dateTrunc` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 184 | `$densify` | aggregation stage | 1/10 | 1 | 0 | not supported |
| 185 | `$documents` | aggregation stage | 1/10 | 1 | 1 | not supported |
| 186 | `$exp` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 187 | `$fill` | aggregation stage | 1/10 | 1 | 0 | not supported |
| 188 | `$firstN` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 189 | `$getField` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 190 | `$graphLookup` | aggregation stage | 1/10 | 1 | 4 | not supported |
| 191 | `$indexOfArray` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 192 | `$indexStats` | aggregation stage | 1/10 | 1 | 5 | not supported |
| 193 | `$isoWeek` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 194 | `$jsonSchema` | query operator | 1/10 | 1 | 0 | not supported |
| 195 | `$last` | aggregation accumulator | 1/10 | 1 | 2 | supported |
| 196 | `$lastN` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 197 | `$ln` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 198 | `$log` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 199 | `$log10` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 200 | `$ltrim` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 201 | `$maxN` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 202 | `$median` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 203 | `$minN` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 204 | `$percentile` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 205 | `$planCacheStats` | aggregation stage | 1/10 | 1 | 2 | not supported |
| 206 | `$pow` | aggregation expression | 1/10 | 1 | 2 | not supported |
| 207 | `$rand` | aggregation expression | 1/10 | 1 | 2 | not supported |
| 208 | `$redact` | aggregation stage | 1/10 | 1 | 0 | not supported |
| 209 | `$regexFind` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 210 | `$regexFindAll` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 211 | `$replaceAll` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 212 | `$replaceOne` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 213 | `$replaceWith` | aggregation stage | 1/10 | 1 | 1 | not supported |
| 214 | `$round` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 215 | `$rtrim` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 216 | `$sample` | aggregation stage | 1/10 | 1 | 0 | not supported |
| 217 | `$sampleRate` | aggregation expression | 1/10 | 1 | 2 | not supported |
| 218 | `$searchMeta` | aggregation stage | 1/10 | 1 | 0 | not supported |
| 219 | `$setDifference` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 220 | `$setEquals` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 221 | `$setField` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 222 | `$setIntersection` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 223 | `$setIsSubset` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 224 | `$setWindowFields` | aggregation stage | 1/10 | 1 | 3 | not supported |
| 225 | `$sortByCount` | aggregation stage | 1/10 | 1 | 5 | not supported |
| 226 | `$sqrt` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 227 | `$stdDevPop` | aggregation accumulator | 1/10 | 1 | 3 | not supported |
| 228 | `$stdDevSamp` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 229 | `$strcasecmp` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 230 | `$toBool` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 231 | `$toDecimal` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 232 | `$toUpper` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 233 | `$top` | aggregation accumulator | 1/10 | 1 | 8 | not supported |
| 234 | `$topN` | aggregation accumulator | 1/10 | 1 | 1 | not supported |
| 235 | `$tsIncrement` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 236 | `$tsSecond` | aggregation expression | 1/10 | 1 | 1 | not supported |
| 237 | `$unsetField` | aggregation expression | 1/10 | 1 | 0 | not supported |
| 238 | `$zip` | aggregation expression | 1/10 | 1 | 2 | not supported |

Notes on status:

- `index: any definition`: context row: any index definition (only `unique` has an effect)
- `aggregate`: subset of stages: $match, $group, $sort, $limit, $skip, $project, $count
- `findOneAndUpdate`: `findAndModify` command; includes Mongoose `findByIdAndUpdate`
- `bulkWrite`: driver lowers it to insert/update/delete; probed directly, results identical to MongoDB 7.0 (ordered and unordered)
- `index: unique`: enforced on insert, update, upsert and index build; a partial/sparse filter on a unique index is ignored
- `index: sparse`: recorded only; not applied
- `index: partialFilterExpression`: recorded only; the filter is not applied
- `index: TTL (expireAfterSeconds)`: definition recorded only; **documents never expire**
- `distinct`: outside the requested scope, passes the differential tests
- `$sum`: $group accumulator
- `$cond`: aggregation expression: no expression operators except field paths and literals
- `withTransaction / startTransaction`: no transactions; a standalone `mongod` has none either, production deployments use replica sets
- `findOneAndDelete`: `findAndModify` command; includes `findByIdAndDelete`/`findOneAndRemove`
- `watch (change streams)`: no change streams (also not available on a standalone mongod)
- `index: compound`: definition recorded and listed; queries never use it (they scan); a unique compound index is enforced
- `$ifNull`: aggregation expression: no expression operators except field paths and literals
- `$map`: aggregation expression: no expression operators except field paths and literals
- `startSession`: sessions are accepted and ignored; passes `admin.session_insert`
- `$first`: $group accumulator ($first/$last/$push work but are outside the requested scope)
- `$push (aggregation accumulator)`: $group accumulator ($first/$last/$push work but are outside the requested scope)
- `$isArray`: aggregation expression: no expression operators except field paths and literals
- `$min (aggregation accumulator)`: $group accumulator
- `$literal`: aggregation expression: no expression operators except field paths and literals
- `$max (aggregation accumulator)`: $group accumulator
- `$concatArrays`: aggregation expression: no expression operators except field paths and literals
- `$filter`: aggregation expression: no expression operators except field paths and literals
- `$arrayElemAt`: aggregation expression: no expression operators except field paths and literals
- `$concat`: aggregation expression: no expression operators except field paths and literals
- `$add`: aggregation expression: no expression operators except field paths and literals
- `$dateToString`: aggregation expression: no expression operators except field paths and literals
- `$multiply`: aggregation expression: no expression operators except field paths and literals
- `$setUnion`: aggregation expression: no expression operators except field paths and literals
- `$toLower`: aggregation expression: no expression operators except field paths and literals
- `$range`: aggregation expression: no expression operators except field paths and literals
- `$toString`: aggregation expression: no expression operators except field paths and literals
- `$let`: aggregation expression: no expression operators except field paths and literals
- `$subtract`: aggregation expression: no expression operators except field paths and literals
- `$trim`: aggregation expression: no expression operators except field paths and literals
- `$convert`: aggregation expression: no expression operators except field paths and literals
- `$hour`: aggregation expression: no expression operators except field paths and literals
- `$regexMatch`: aggregation expression: no expression operators except field paths and literals
- `findOneAndReplace`: `findAndModify` command
- `$month`: aggregation expression: no expression operators except field paths and literals
- `$sortArray`: aggregation expression: no expression operators except field paths and literals
- `$year`: aggregation expression: no expression operators except field paths and literals
- `$anyElementTrue`: aggregation expression: no expression operators except field paths and literals
- `$objectToArray`: aggregation expression: no expression operators except field paths and literals
- `$divide`: aggregation expression: no expression operators except field paths and literals
- `$switch`: aggregation expression: no expression operators except field paths and literals
- `$strLenBytes`: aggregation expression: no expression operators except field paths and literals
- `index: text`: definition recorded only; `$text` queries are not supported, so the index has no effect
- `$ceil`: aggregation expression: no expression operators except field paths and literals
- `$trunc`: aggregation expression: no expression operators except field paths and literals
- `$reduce`: aggregation expression: no expression operators except field paths and literals
- `$indexOfBytes`: aggregation expression: no expression operators except field paths and literals
- `$indexOfCP`: aggregation expression: no expression operators except field paths and literals
- `$strLenCP`: aggregation expression: no expression operators except field paths and literals
- `$substr`: aggregation expression: no expression operators except field paths and literals
- `$substrCP`: aggregation expression: no expression operators except field paths and literals
- `$dayOfMonth`: aggregation expression: no expression operators except field paths and literals
- `$reverseArray`: aggregation expression: no expression operators except field paths and literals
- `$substrBytes`: aggregation expression: no expression operators except field paths and literals
- `$avg`: $group accumulator
- `$dayOfWeek`: aggregation expression: no expression operators except field paths and literals
- `$dayOfYear`: aggregation expression: no expression operators except field paths and literals
- `$floor`: aggregation expression: no expression operators except field paths and literals
- `$isoDayOfWeek`: aggregation expression: no expression operators except field paths and literals
- `$isoWeekYear`: aggregation expression: no expression operators except field paths and literals
- `$millisecond`: aggregation expression: no expression operators except field paths and literals
- `$minute`: aggregation expression: no expression operators except field paths and literals
- `$second`: aggregation expression: no expression operators except field paths and literals
- `$week`: aggregation expression: no expression operators except field paths and literals
- `$split`: aggregation expression: no expression operators except field paths and literals
- `$toDate`: aggregation expression: no expression operators except field paths and literals
- `$arrayToObject`: aggregation expression: no expression operators except field paths and literals
- `$toInt`: aggregation expression: no expression operators except field paths and literals
- `$dateFromParts`: aggregation expression: no expression operators except field paths and literals
- `index: hashed / wildcard`: recorded only
- `$isNumber`: aggregation expression: no expression operators except field paths and literals
- `$toDouble`: aggregation expression: no expression operators except field paths and literals
- `$toLong`: aggregation expression: no expression operators except field paths and literals
- `$toObjectId`: aggregation expression: no expression operators except field paths and literals
- `index: 2dsphere / 2d (geo)`: recorded only; geo queries ($near, $geoWithin, ...) are not supported
- `$abs`: aggregation expression: no expression operators except field paths and literals
- `$allElementsTrue`: aggregation expression: no expression operators except field paths and literals
- `$binarySize`: aggregation expression: no expression operators except field paths and literals
- `$bsonSize`: aggregation expression: no expression operators except field paths and literals
- `$cmp`: aggregation expression: no expression operators except field paths and literals
- `$dateAdd`: aggregation expression: no expression operators except field paths and literals
- `$dateDiff`: aggregation expression: no expression operators except field paths and literals
- `$dateFromString`: aggregation expression: no expression operators except field paths and literals
- `$dateSubtract`: aggregation expression: no expression operators except field paths and literals
- `$dateToParts`: aggregation expression: no expression operators except field paths and literals
- `$dateTrunc`: aggregation expression: no expression operators except field paths and literals
- `$exp`: aggregation expression: no expression operators except field paths and literals
- `$getField`: aggregation expression: no expression operators except field paths and literals
- `$indexOfArray`: aggregation expression: no expression operators except field paths and literals
- `$isoWeek`: aggregation expression: no expression operators except field paths and literals
- `$last`: $group accumulator ($first/$last/$push work but are outside the requested scope)
- `$ln`: aggregation expression: no expression operators except field paths and literals
- `$log`: aggregation expression: no expression operators except field paths and literals
- `$log10`: aggregation expression: no expression operators except field paths and literals
- `$ltrim`: aggregation expression: no expression operators except field paths and literals
- `$pow`: aggregation expression: no expression operators except field paths and literals
- `$rand`: aggregation expression: no expression operators except field paths and literals
- `$regexFind`: aggregation expression: no expression operators except field paths and literals
- `$regexFindAll`: aggregation expression: no expression operators except field paths and literals
- `$replaceAll`: aggregation expression: no expression operators except field paths and literals
- `$replaceOne`: aggregation expression: no expression operators except field paths and literals
- `$round`: aggregation expression: no expression operators except field paths and literals
- `$rtrim`: aggregation expression: no expression operators except field paths and literals
- `$sampleRate`: aggregation expression: no expression operators except field paths and literals
- `$setDifference`: aggregation expression: no expression operators except field paths and literals
- `$setEquals`: aggregation expression: no expression operators except field paths and literals
- `$setField`: aggregation expression: no expression operators except field paths and literals
- `$setIntersection`: aggregation expression: no expression operators except field paths and literals
- `$setIsSubset`: aggregation expression: no expression operators except field paths and literals
- `$sqrt`: aggregation expression: no expression operators except field paths and literals
- `$strcasecmp`: aggregation expression: no expression operators except field paths and literals
- `$toBool`: aggregation expression: no expression operators except field paths and literals
- `$toDecimal`: aggregation expression: no expression operators except field paths and literals
- `$toUpper`: aggregation expression: no expression operators except field paths and literals
- `$tsIncrement`: aggregation expression: no expression operators except field paths and literals
- `$tsSecond`: aggregation expression: no expression operators except field paths and literals
- `$unsetField`: aggregation expression: no expression operators except field paths and literals
- `$zip`: aggregation expression: no expression operators except field paths and literals

## 4. Not supported features, ranked

191 of the 238 features used by at least one repository are not supported.

| # | Feature | Kind | Repos using it (application code) | Occurrences (app) | Occurrences (tests only) | Our status | Repos |
|---|---|---|---|---|---|---|---|
| 1 | `$regex` | query operator | 10/10 | 262 | 198 | not supported | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform |
| 2 | `findOneAndUpdate` | driver/ODM method | 9/10 | 416 | 350 | not supported | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload |
| 3 | `$options` | query operator | 9/10 | 93 | 59 | not supported | Rocket.Chat, parse-server, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform |
| 4 | `$push (update)` | update operator | 8/10 | 121 | 134 | not supported | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, countly-server, payload |
| 5 | `$size` | query operator | 8/10 | 81 | 9 | not supported | Rocket.Chat, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload |
| 6 | `$pull` | update operator | 7/10 | 193 | 55 | not supported | Rocket.Chat, NodeBB, wekan, LibreChat, novu, countly-server, payload |
| 7 | `$elemMatch` | query operator | 7/10 | 158 | 89 | not supported | Rocket.Chat, wekan, LibreChat, novu, countly-server, payload, edx-platform |
| 8 | `$addToSet (update)` | update operator | 7/10 | 129 | 59 | not supported | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, countly-server, payload |
| 9 | `$cond` | aggregation expression | 7/10 | 108 | 3 | not supported | Rocket.Chat, NodeBB, wekan, LibreChat, novu, countly-server, payload |
| 10 | `$each` | update operator | 7/10 | 86 | 44 | not supported | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, countly-server, payload |
| 11 | `withTransaction / startTransaction` | driver/ODM method | 7/10 | 81 | 83 | not supported | Rocket.Chat, parse-server, LibreChat, novu, appsmith, countly-server, payload |
| 12 | `findOneAndDelete` | driver/ODM method | 7/10 | 52 | 12 | not supported | Rocket.Chat, parse-server, wekan, LibreChat, novu, countly-server, payload |
| 13 | `watch (change streams)` | driver/ODM method | 7/10 | 20 | 10 | not supported | Rocket.Chat, parse-server, wekan, LibreChat, novu, appsmith, countly-server |
| 14 | `$setOnInsert` | update operator | 6/10 | 142 | 35 | not supported | Rocket.Chat, parse-server, wekan, LibreChat, novu, countly-server |
| 15 | `$ifNull` | aggregation expression | 6/10 | 92 | 1 | not supported | Rocket.Chat, wekan, LibreChat, novu, countly-server, payload |
| 16 | `$lookup` | aggregation stage | 6/10 | 84 | 49 | not supported | Rocket.Chat, wekan, LibreChat, novu, countly-server, payload |
| 17 | `$unwind` | aggregation stage | 6/10 | 69 | 3 | not supported | Rocket.Chat, LibreChat, novu, appsmith, countly-server, payload |
| 18 | `$expr` | query operator | 6/10 | 30 | 17 | not supported | Rocket.Chat, wekan, LibreChat, novu, countly-server, payload |
| 19 | `$all` | query operator | 6/10 | 26 | 27 | not supported | Rocket.Chat, parse-server, wekan, novu, countly-server, payload |
| 20 | `$map` | aggregation expression | 6/10 | 21 | 1 | not supported | Rocket.Chat, NodeBB, wekan, LibreChat, countly-server, payload |
| 21 | `$rename` | update operator | 6/10 | 15 | 26 | not supported | Rocket.Chat, NodeBB, wekan, LibreChat, novu, payload |
| 22 | `$type` | query operator | 5/10 | 74 | 26 | not supported | Rocket.Chat, wekan, LibreChat, novu, countly-server |
| 23 | `$addFields` | aggregation stage | 5/10 | 67 | 20 | not supported | Rocket.Chat, wekan, LibreChat, countly-server, payload |
| 24 | `$max (update)` | update operator | 5/10 | 41 | 13 | not supported | Rocket.Chat, wekan, LibreChat, novu, countly-server |
| 25 | `$slice` | query operator | 5/10 | 30 | 12 | not supported | Rocket.Chat, wekan, LibreChat, countly-server, payload |
| 26 | `$text` | query operator | 5/10 | 24 | 25 | not supported | parse-server, LibreChat, countly-server, payload, edx-platform |
| 27 | `$pullAll` | update operator | 5/10 | 20 | 24 | not supported | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat |
| 28 | `$merge` | aggregation stage | 5/10 | 19 | 7 | not supported | Rocket.Chat, parse-server, wekan, LibreChat, countly-server |
| 29 | `$isArray` | aggregation expression | 5/10 | 15 | 0 | not supported | Rocket.Chat, LibreChat, novu, countly-server, payload |
| 30 | `$literal` | aggregation expression | 5/10 | 9 | 4 | not supported | Rocket.Chat, wekan, LibreChat, countly-server, payload |
| 31 | `$concatArrays` | aggregation expression | 5/10 | 7 | 0 | not supported | LibreChat, novu, appsmith, countly-server, payload |
| 32 | `$filter` | aggregation expression | 4/10 | 31 | 2 | not supported | Rocket.Chat, LibreChat, appsmith, countly-server |
| 33 | `$where` | query operator | 4/10 | 22 | 85 | not supported | wekan, LibreChat, appsmith, countly-server |
| 34 | `$arrayElemAt` | aggregation expression | 4/10 | 18 | 0 | not supported | Rocket.Chat, LibreChat, appsmith, countly-server |
| 35 | `$concat` | aggregation expression | 4/10 | 15 | 2 | not supported | Rocket.Chat, LibreChat, novu, countly-server |
| 36 | `$add` | aggregation expression | 4/10 | 13 | 3 | not supported | Rocket.Chat, LibreChat, countly-server, payload |
| 37 | `$out` | aggregation stage | 4/10 | 10 | 12 | not supported | parse-server, wekan, LibreChat, countly-server |
| 38 | `$dateToString` | aggregation expression | 4/10 | 7 | 0 | not supported | Rocket.Chat, LibreChat, novu, countly-server |
| 39 | `$multiply` | aggregation expression | 4/10 | 7 | 4 | not supported | Rocket.Chat, NodeBB, LibreChat, countly-server |
| 40 | `$set (aggregation stage)` | aggregation stage | 4/10 | 6 | 9 | not supported | Rocket.Chat, LibreChat, countly-server, payload |
| 41 | `$setUnion` | aggregation expression | 4/10 | 6 | 0 | not supported | Rocket.Chat, LibreChat, countly-server, edx-platform |
| 42 | `$addToSet (aggregation accumulator)` | aggregation accumulator | 3/10 | 26 | 3 | not supported | Rocket.Chat, novu, countly-server |
| 43 | `$facet` | aggregation stage | 3/10 | 17 | 18 | not supported | Rocket.Chat, LibreChat, countly-server |
| 44 | `$toLower` | aggregation expression | 3/10 | 12 | 1 | not supported | Rocket.Chat, LibreChat, countly-server |
| 45 | `$min (update)` | update operator | 3/10 | 10 | 9 | not supported | LibreChat, novu, countly-server |
| 46 | `$range` | aggregation expression | 3/10 | 10 | 0 | not supported | Rocket.Chat, wekan, countly-server |
| 47 | `$toString` | aggregation expression | 3/10 | 10 | 0 | not supported | Rocket.Chat, wekan, countly-server |
| 48 | `$geoWithin` | query operator | 3/10 | 9 | 23 | not supported | parse-server, countly-server, payload |
| 49 | `$let` | aggregation expression | 3/10 | 9 | 3 | not supported | LibreChat, appsmith, countly-server |
| 50 | `$subtract` | aggregation expression | 3/10 | 9 | 0 | not supported | Rocket.Chat, LibreChat, countly-server |
| 51 | `$bitsAllSet` | query operator | 3/10 | 8 | 27 | not supported | Rocket.Chat, LibreChat, countly-server |
| 52 | `$geometry` | query operator | 3/10 | 8 | 7 | not supported | parse-server, countly-server, payload |
| 53 | `$search` | query operator | 3/10 | 8 | 21 | not supported | Rocket.Chat, parse-server, countly-server |
| 54 | `$geoIntersects` | query operator | 3/10 | 7 | 7 | not supported | parse-server, countly-server, payload |
| 55 | `$meta` | query operator | 3/10 | 7 | 15 | not supported | Rocket.Chat, parse-server, countly-server |
| 56 | `$trim` | aggregation expression | 3/10 | 7 | 0 | not supported | LibreChat, novu, countly-server |
| 57 | `$bitsAnySet` | query operator | 3/10 | 6 | 3 | not supported | Rocket.Chat, LibreChat, countly-server |
| 58 | `$convert` | aggregation expression | 3/10 | 6 | 4 | not supported | Rocket.Chat, LibreChat, countly-server |
| 59 | `$hour` | aggregation expression | 3/10 | 6 | 0 | not supported | Rocket.Chat, parse-server, countly-server |
| 60 | `$regexMatch` | aggregation expression | 3/10 | 6 | 3 | not supported | LibreChat, novu, countly-server |
| 61 | `findOneAndReplace` | driver/ODM method | 3/10 | 6 | 6 | not supported | parse-server, LibreChat, countly-server |
| 62 | `$month` | aggregation expression | 3/10 | 5 | 4 | not supported | Rocket.Chat, parse-server, countly-server |
| 63 | `$near` | query operator | 3/10 | 5 | 4 | not supported | parse-server, countly-server, payload |
| 64 | `$sortArray` | aggregation expression | 3/10 | 5 | 1 | not supported | LibreChat, countly-server, payload |
| 65 | `$year` | aggregation expression | 3/10 | 5 | 4 | not supported | Rocket.Chat, parse-server, countly-server |
| 66 | `$collStats` | aggregation stage | 3/10 | 4 | 3 | not supported | NodeBB, wekan, countly-server |
| 67 | `$position` | update operator | 3/10 | 4 | 3 | not supported | Rocket.Chat, NodeBB, appsmith |
| 68 | `$anyElementTrue` | aggregation expression | 3/10 | 3 | 0 | not supported | wekan, LibreChat, countly-server |
| 69 | `$pop` | update operator | 3/10 | 3 | 10 | not supported | Rocket.Chat, NodeBB, wekan |
| 70 | `$objectToArray` | aggregation expression | 2/10 | 24 | 0 | not supported | wekan, countly-server |
| 71 | `$divide` | aggregation expression | 2/10 | 17 | 0 | not supported | Rocket.Chat, countly-server |
| 72 | `$switch` | aggregation expression | 2/10 | 16 | 3 | not supported | LibreChat, countly-server |
| 73 | `$strLenBytes` | aggregation expression | 2/10 | 13 | 1 | not supported | LibreChat, countly-server |
| 74 | `$maxDistance` | query operator | 2/10 | 8 | 6 | not supported | parse-server, countly-server |
| 75 | `$bitsAllClear` | query operator | 2/10 | 7 | 3 | not supported | Rocket.Chat, countly-server |
| 76 | `$ceil` | aggregation expression | 2/10 | 7 | 0 | not supported | Rocket.Chat, countly-server |
| 77 | `$centerSphere` | query operator | 2/10 | 7 | 11 | not supported | parse-server, countly-server |
| 78 | `$function` | aggregation accumulator | 2/10 | 7 | 19 | not supported | wekan, countly-server |
| 79 | `$currentDate` | update operator | 2/10 | 6 | 12 | not supported | Rocket.Chat, LibreChat |
| 80 | `$nearSphere` | query operator | 2/10 | 6 | 8 | not supported | parse-server, countly-server |
| 81 | `$trunc` | aggregation expression | 2/10 | 6 | 1 | not supported | Rocket.Chat, countly-server |
| 82 | `$accumulator` | aggregation accumulator | 2/10 | 5 | 10 | not supported | wekan, countly-server |
| 83 | `$reduce` | aggregation expression | 2/10 | 5 | 0 | not supported | LibreChat, countly-server |
| 84 | `$bit` | update operator | 2/10 | 4 | 27 | not supported | wekan, countly-server |
| 85 | `$box` | query operator | 2/10 | 4 | 1 | not supported | parse-server, countly-server |
| 86 | `$indexOfBytes` | aggregation expression | 2/10 | 4 | 0 | not supported | LibreChat, countly-server |
| 87 | `$indexOfCP` | aggregation expression | 2/10 | 4 | 0 | not supported | LibreChat, countly-server |
| 88 | `$polygon` | query operator | 2/10 | 4 | 11 | not supported | parse-server, countly-server |
| 89 | `$strLenCP` | aggregation expression | 2/10 | 4 | 0 | not supported | LibreChat, countly-server |
| 90 | `$substr` | aggregation expression | 2/10 | 4 | 1 | not supported | Rocket.Chat, countly-server |
| 91 | `$substrCP` | aggregation expression | 2/10 | 4 | 1 | not supported | LibreChat, countly-server |
| 92 | `$dayOfMonth` | aggregation expression | 2/10 | 3 | 6 | not supported | parse-server, countly-server |
| 93 | `$mergeObjects` | aggregation accumulator | 2/10 | 3 | 3 | not supported | novu, countly-server |
| 94 | `$replaceRoot` | aggregation stage | 2/10 | 3 | 6 | not supported | LibreChat, countly-server |
| 95 | `$reverseArray` | aggregation expression | 2/10 | 3 | 0 | not supported | LibreChat, countly-server |
| 96 | `$substrBytes` | aggregation expression | 2/10 | 3 | 0 | not supported | LibreChat, countly-server |
| 97 | `$bitsAnyClear` | query operator | 2/10 | 2 | 3 | not supported | Rocket.Chat, countly-server |
| 98 | `$dayOfWeek` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 99 | `$dayOfYear` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 100 | `$floor` | aggregation expression | 2/10 | 2 | 0 | not supported | Rocket.Chat, countly-server |
| 101 | `$geoNear` | aggregation stage | 2/10 | 2 | 3 | not supported | parse-server, countly-server |
| 102 | `$isoDayOfWeek` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 103 | `$isoWeekYear` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 104 | `$millisecond` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 105 | `$minute` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 106 | `$second` | aggregation expression | 2/10 | 2 | 1 | not supported | parse-server, countly-server |
| 107 | `$unset (aggregation stage)` | aggregation stage | 2/10 | 2 | 4 | not supported | Rocket.Chat, countly-server |
| 108 | `$week` | aggregation expression | 2/10 | 2 | 0 | not supported | parse-server, countly-server |
| 109 | `$split` | aggregation expression | 1/10 | 7 | 0 | not supported | countly-server |
| 110 | `$toDate` | aggregation expression | 1/10 | 6 | 0 | not supported | countly-server |
| 111 | `$unionWith` | aggregation stage | 1/10 | 6 | 6 | not supported | countly-server |
| 112 | `$arrayToObject` | aggregation expression | 1/10 | 5 | 0 | not supported | countly-server |
| 113 | `$toInt` | aggregation expression | 1/10 | 4 | 0 | not supported | countly-server |
| 114 | `$dateFromParts` | aggregation expression | 1/10 | 3 | 2 | not supported | countly-server |
| 115 | `$isNumber` | aggregation expression | 1/10 | 2 | 1 | not supported | countly-server |
| 116 | `$minDistance` | query operator | 1/10 | 2 | 1 | not supported | countly-server |
| 117 | `$mod` | query operator | 1/10 | 2 | 4 | not supported | countly-server |
| 118 | `$mul` | update operator | 1/10 | 2 | 8 | not supported | countly-server |
| 119 | `$toDouble` | aggregation expression | 1/10 | 2 | 0 | not supported | countly-server |
| 120 | `$toLong` | aggregation expression | 1/10 | 2 | 0 | not supported | countly-server |
| 121 | `$toObjectId` | aggregation expression | 1/10 | 2 | 0 | not supported | countly-server |
| 122 | `$abs` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 123 | `$allElementsTrue` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 124 | `$binarySize` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 125 | `$bottom` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 126 | `$bottomN` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 127 | `$bsonSize` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 128 | `$bucket` | aggregation stage | 1/10 | 1 | 4 | not supported | countly-server |
| 129 | `$bucketAuto` | aggregation stage | 1/10 | 1 | 1 | not supported | countly-server |
| 130 | `$center` | query operator | 1/10 | 1 | 1 | not supported | countly-server |
| 131 | `$cmp` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 132 | `$dateAdd` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 133 | `$dateDiff` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 134 | `$dateFromString` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 135 | `$dateSubtract` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 136 | `$dateToParts` | aggregation expression | 1/10 | 1 | 2 | not supported | countly-server |
| 137 | `$dateTrunc` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 138 | `$densify` | aggregation stage | 1/10 | 1 | 0 | not supported | countly-server |
| 139 | `$documents` | aggregation stage | 1/10 | 1 | 1 | not supported | countly-server |
| 140 | `$exp` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 141 | `$fill` | aggregation stage | 1/10 | 1 | 0 | not supported | countly-server |
| 142 | `$firstN` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 143 | `$getField` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 144 | `$graphLookup` | aggregation stage | 1/10 | 1 | 4 | not supported | countly-server |
| 145 | `$indexOfArray` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 146 | `$indexStats` | aggregation stage | 1/10 | 1 | 5 | not supported | countly-server |
| 147 | `$isoWeek` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 148 | `$jsonSchema` | query operator | 1/10 | 1 | 0 | not supported | countly-server |
| 149 | `$lastN` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 150 | `$ln` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 151 | `$log` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 152 | `$log10` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 153 | `$ltrim` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 154 | `$maxN` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 155 | `$median` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 156 | `$minN` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 157 | `$percentile` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 158 | `$planCacheStats` | aggregation stage | 1/10 | 1 | 2 | not supported | countly-server |
| 159 | `$pow` | aggregation expression | 1/10 | 1 | 2 | not supported | countly-server |
| 160 | `$rand` | aggregation expression | 1/10 | 1 | 2 | not supported | countly-server |
| 161 | `$redact` | aggregation stage | 1/10 | 1 | 0 | not supported | countly-server |
| 162 | `$regexFind` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 163 | `$regexFindAll` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 164 | `$replaceAll` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 165 | `$replaceOne` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 166 | `$replaceWith` | aggregation stage | 1/10 | 1 | 1 | not supported | countly-server |
| 167 | `$round` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 168 | `$rtrim` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 169 | `$sample` | aggregation stage | 1/10 | 1 | 0 | not supported | countly-server |
| 170 | `$sampleRate` | aggregation expression | 1/10 | 1 | 2 | not supported | countly-server |
| 171 | `$searchMeta` | aggregation stage | 1/10 | 1 | 0 | not supported | countly-server |
| 172 | `$setDifference` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 173 | `$setEquals` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 174 | `$setField` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 175 | `$setIntersection` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 176 | `$setIsSubset` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 177 | `$setWindowFields` | aggregation stage | 1/10 | 1 | 3 | not supported | countly-server |
| 178 | `$sortByCount` | aggregation stage | 1/10 | 1 | 5 | not supported | countly-server |
| 179 | `$sqrt` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 180 | `$stdDevPop` | aggregation accumulator | 1/10 | 1 | 3 | not supported | countly-server |
| 181 | `$stdDevSamp` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 182 | `$strcasecmp` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 183 | `$toBool` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 184 | `$toDecimal` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 185 | `$toUpper` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 186 | `$top` | aggregation accumulator | 1/10 | 1 | 8 | not supported | countly-server |
| 187 | `$topN` | aggregation accumulator | 1/10 | 1 | 1 | not supported | countly-server |
| 188 | `$tsIncrement` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 189 | `$tsSecond` | aggregation expression | 1/10 | 1 | 1 | not supported | countly-server |
| 190 | `$unsetField` | aggregation expression | 1/10 | 1 | 0 | not supported | countly-server |
| 191 | `$zip` | aggregation expression | 1/10 | 1 | 2 | not supported | countly-server |

### Recommended implementation order (top 10 not-yet-supported features by number of repositories)

| Priority | Feature | Kind | Repos (app) | Occurrences (app) | Repositories | Example file in the heaviest user |
|---|---|---|---|---|---|---|
| 1 | `$regex` | query operator | 10/10 | 262 | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform | countly-server: `api/parts/data/fetch.js` |
| 2 | `findOneAndUpdate` | driver/ODM method | 9/10 | 416 | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload | LibreChat: `api/server/services/Schedules/index.js` |
| 3 | `$options` | query operator | 9/10 | 93 | Rocket.Chat, parse-server, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform | Rocket.Chat: `apps/meteor/ee/server/api/sessions.ts` |
| 4 | `$push (update)` | update operator | 8/10 | 121 | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, countly-server, payload | wekan: `client/components/lists/listBody.js` |
| 5 | `$size` | query operator | 8/10 | 81 | Rocket.Chat, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload | Rocket.Chat: `apps/meteor/ee/server/api/v1/omnichannel/lib/tags.ts` |
| 6 | `$pull` | update operator | 7/10 | 193 | Rocket.Chat, NodeBB, wekan, LibreChat, novu, countly-server, payload | wekan: `client/components/sidebar/sidebarCustomFields.js` |
| 7 | `$elemMatch` | query operator | 7/10 | 158 | Rocket.Chat, wekan, LibreChat, novu, countly-server, payload, edx-platform | wekan: `client/components/cards/subtasks.js` |
| 8 | `$addToSet (update)` | update operator | 7/10 | 129 | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, countly-server, payload | wekan: `client/components/main/header.js` |
| 9 | `$cond` | aggregation expression | 7/10 | 108 | Rocket.Chat, NodeBB, wekan, LibreChat, novu, countly-server, payload | Rocket.Chat: `apps/meteor/ee/server/models/raw/LivechatRooms.ts` |
| 10 | `$each` | update operator | 7/10 | 86 | Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, countly-server, payload | Rocket.Chat: `apps/meteor/server/features/EmailInbox/EmailInbox_Incoming.ts` |

Next five: `withTransaction / startTransaction` (7/10), `findOneAndDelete` (7/10), `watch (change streams)` (7/10), `$setOnInsert` (6/10), `$ifNull` (6/10).

## 5. Secondary indexes

| Index kind | Repos defining it (application code) | Occurrences (app) | Our status |
|---|---|---|---|
| compound | 6/10 (NodeBB, wekan, LibreChat, novu, appsmith, countly-server) | 294 | supported with differences |
| text | 2/10 (Rocket.Chat, countly-server) | 10 | supported with differences |
| TTL (expireAfterSeconds) | 8/10 (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, appsmith, countly-server, payload) | 70 | supported with differences |
| unique | 9/10 (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, countly-server, payload, edx-platform) | 221 | supported |
| partialFilterExpression | 8/10 (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, countly-server, payload) | 107 | supported with differences |
| sparse | 9/10 (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, countly-server, payload, edx-platform) | 144 | supported with differences |
| 2dsphere / 2d (geo) | 1/10 (payload) | 2 | supported with differences |
| hashed / wildcard | 1/10 (countly-server) | 3 | supported with differences |
| any definition | 10/10 (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform) | 818 | supported with differences |

**Do compound, TTL and text indexes appear in most repositories?** Compound indexes: 6/10 (in most repositories); TTL indexes: 8/10 (in most repositories); text indexes: 2/10 (in a minority of repositories); unique indexes: 9/10. Caveat: text and geo indexes are detected only when the key list is written inline at the call, and `$text` queries appear in 5/10 repositories (against 2/10 inline text-index definitions), so text indexes are under-counted; treat the real figure as at least `$text`'s. Our server records every index definition but only enforces `unique`; compound and text definitions have no effect on queries, and TTL indexes silently never expire documents, which is a functional gap, not just a performance one.

## 6. Per-repository detail

- **Rocket.Chat** (`1826a86`): 8514 app / 2412 test files; most-used unsupported features: `findOneAndUpdate` ×74, `$regex` ×48, `$pull` ×44, `$cond` ×41, `$options` ×40, `$lookup` ×39.
- **parse-server** (`9805d3e`): 216 app / 161 test files; most-used unsupported features: `$text` ×15, `$regex` ×14, `findOneAndUpdate` ×7, `$all` ×6, `$maxDistance` ×6, `$nearSphere` ×4.
- **NodeBB** (`ce62dcd`): 853 app / 99 test files; most-used unsupported features: `findOneAndUpdate` ×5, `$addToSet (update)` ×3, `$each` ×3, `$size` ×3, `$regex` ×2, `$pull` ×2.
- **wekan** (`43c8606`): 2030 app / 2835 test files; most-used unsupported features: `$pull` ×75, `$elemMatch` ×60, `$setOnInsert` ×59, `$push (update)` ×47, `$addToSet (update)` ×45, `$type` ×24.
- **LibreChat** (`a378657`): 3681 app / 2916 test files; most-used unsupported features: `findOneAndUpdate` ×201, `$setOnInsert` ×61, `$cond` ×41, `$elemMatch` ×37, `$ifNull` ×35, `$pull` ×33.
- **novu** (`d7ff70c`): 8305 app / 1242 test files; most-used unsupported features: `findOneAndUpdate` ×29, `withTransaction / startTransaction` ×22, `$regex` ×20, `$options` ×17, `$elemMatch` ×12, `$type` ×11.
- **appsmith** (`b0b5020`): 6868 app / 2184 test files; most-used unsupported features: `withTransaction / startTransaction` ×16, `findOneAndUpdate` ×9, `$regex` ×6, `watch (change streams)` ×5, `$options` ×2, `$position` ×2.
- **countly-server** (`70a0e28`): 850 app / 268 test files; most-used unsupported features: `$regex` ×127, `findOneAndUpdate` ×80, `$addFields` ×33, `$pull` ×27, `$objectToArray` ×22, `$elemMatch` ×21.
- **payload** (`aebbf75`): 5283 app / 2631 test files; most-used unsupported features: `$push (update)` ×28, `withTransaction / startTransaction` ×23, `$each` ×12, `$lookup` ×10, `$addFields` ×9, `findOneAndUpdate` ×7.
- **edx-platform** (`87cd381`): 3693 app / 1738 test files; most-used unsupported features: `$regex` ×2, `$elemMatch` ×2, `$options` ×1, `$setUnion` ×1, `$text` ×1.

## Reproduce

```bash
python3 tools/compat-scan/scan.py run --tmp /some/scratch/dir      # clones (depth 1) and scans each repo in turn, deleting each clone; writes tools/compat-scan/results.json
python3 tools/compat-scan/scan.py report                          # writes compat-scan.md
```
