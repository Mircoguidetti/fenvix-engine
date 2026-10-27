
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
