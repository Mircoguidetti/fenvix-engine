| check | docs count | inventory rows | result | note |
|---|---:|---:|---|---|
| commands: docs index tables (command.md + crud-commands.md) all present | 148 | 148 | OK | names in the docs tables that have an inventory row |
| commands: server listCommands (mongo 7.0.45) all present | 264 | 264 | OK | every command the real server lists has a row |
| commands: rows not in either source | 0 | 0 | OK | should be 0:  |
| query operators (7 sub-tables) + $text | 38 | 38 | OK | $text is absent from the docs tables but its page exists (added) |
| projection operators (+1 row: expressions in projection) | 5 | 5 | OK |  |
| update operators (landing page tables; $[] and $[<identifier>] included) | 22 | 22 | OK |  |
| aggregation stages (landing page, $-names; the two non-$ rows of that page are the commands findAndModify/update and are not stages) | 44 | 44 | OK | the Atlas stages $search/$searchMeta/$listSearchIndexes/$vectorSearch are in the docs table |
| aggregation expressions (unique names; +$unsetField) | 151 | 151 | OK | 157 table rows, 7 names listed under two headings |
| accumulators | 23 | 23 | OK |  |
| window functions | 21 | 21 | OK |  |
| bson types (table rows with a type number and alias) | 21 | 21 | OK |  |
| bson binData subtypes (table 'Number | Subtype') | 9 | 9 | OK |  |
| bson comparison order (numbered list of types) | 15 | 15 | OK |  |
| collation locales + variants (every backticked code in the table is a locale or a variant) | 151 | 150 | OK (explained) | locale `bn` appears twice in the docs table (Bengali; Bengali with variant traditional): counted once |
| change stream event types (table rows) | 15 | 15 | OK |  |
| read concern levels | 5 | 5 | OK |  |
| read preference modes | 5 | 5 | OK |  |
| index types (H2 headings on core/indexes/index-types.md; geospatial = 2 rows, + compound wildcard) | 8 | 8 | OK | 8 headings; geospatial split into 2d/2dsphere (+1), compound wildcard added (+1): 10 rows |
| index properties (H2 headings on core/indexes/index-properties.md) | 6 | 6 | OK |  |
| pymongo MongoClient: documented API entries (html anchors) | 26 | 26 | OK |  |
| pymongo Database: documented API entries (html anchors) | 22 | 22 | OK |  |
| pymongo Collection: documented API entries (html anchors) | 54 | 54 | OK |  |
| pymongo Cursor: documented API entries (html anchors) | 37 | 37 | OK |  |
| pymongo CommandCursor: documented API entries (html anchors) | 20 | 20 | OK |  |
| pymongo ChangeStream: documented API entries (html anchors) | 24 | 24 | OK |  |
| pymongo ClientSession: documented API entries (html anchors) | 25 | 25 | OK |  |
| pymongo operations: documented API entries (html anchors) | 10 | 10 | OK |  |
| pymongo GridFS: documented API entries (html anchors) | 25 | 25 | OK |  |
| pymongo grid_file: documented API entries (html anchors) | 44 | 44 | OK |  |
| pymongo encryption: documented API entries (html anchors) | 33 | 33 | OK |  |
| node MongoClient: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 17) | 17 | 17 | OK |  |
| node Db: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 35) | 35 | 35 | OK |  |
| node Collection: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 48) | 48 | 48 | OK |  |
| node AbstractCursor: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 26) | 26 | 26 | OK |  |
| node FindCursor: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 20) | 20 | 20 | OK |  |
| node AggregationCursor: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 16) | 16 | 16 | OK |  |
| node ListIndexesCursor: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 4) | 4 | 4 | OK |  |
| node ListCollectionsCursor: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 5) | 5 | 5 | OK |  |
| node ChangeStream: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 21) | 21 | 21 | OK |  |
| node ClientSession: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 24) | 24 | 24 | OK |  |
| node Admin: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 9) | 9 | 9 | OK |  |
| node GridFSBucket: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 10) | 10 | 10 | OK |  |
| node GridFSBucketReadStream: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 4) | 4 | 4 | OK |  |
| node GridFSBucketWriteStream: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 16) | 16 | 16 | OK |  |
| node BulkOperationBase: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 11) | 11 | 11 | OK |  |
| node OrderedBulkOperation: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 1) | 1 | 1 | OK |  |
| node UnorderedBulkOperation: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 1) | 1 | 1 | OK |  |
| node ClientEncryption: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: 14) | 14 | 14 | OK |  |
