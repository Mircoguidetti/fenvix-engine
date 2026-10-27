# Spot check: 30 random rows against their docs page (twice, two seeds)

Method: `python3 compat/tools/spot_check.py --seed N` picks 30 random rows of `inventory_base.csv`, fetches the page behind each `docs_url` and prints the page title and the passage that mentions the row. A person (the auditor) read every passage and checked that the row's name, its category and the page agree.

| seed | rows checked | mismatches found | fixed |
|---|---:|---:|---:|
| 7  | 30 | 0 | 0 |
| 11 | 30 | 0 | 0 |

Raw output: `spot_check_raw7.txt`, `spot_check_raw.txt` (seed 11). Observations from reading them:

- Rows of internal/undocumented commands (`_transferMods`, `_shardsvrDropIndexCatalogEntryParticipant`, `getDatabaseVersion`, `replSetStepUp`, `_shardsvrNotifyShardingEvent`) point at the command index page, which by construction cannot name them; their existence rests on the real server's `listCommands`. This is flagged in `exclusion_reason` and in `UNVERIFIED.md`.
- Driver rows were checked on the HTML anchor of the API reference (pymongo `dt id=...`, typedoc `a id=...`), e.g. `node:ChangeStream.RESPONSE` is a static event-name constant (client-only).

Problems that the *quality gate as a whole* (count recounts, URL verification, category review) found and that were fixed before this check (they would not show up in a random 30-row sample):

1. The stages landing table has a second section "Stages Available for Updates" whose two rows are the commands `findAndModify` and `update`; the builder had turned them into stage rows. Removed.
2. `isMaster`, `saslStart`, `saslContinue` were excluded as "undocumented"; they are sent by every driver (the task names `isMaster` explicitly), so they were put back in scope.
3. The sub-fields of `update.updates[]`, `delete.deletes[]` (`upsert`, `multi`, `arrayFilters`, `hint`, `collation`) were lost by an over-eager heading filter in the options parser (found by the count recount); restored.
4. The 7.0 docs landing tables omit `$text` (query operators) and `$unsetField` (expressions); both pages exist and mongod 7.0.45 accepts them, so they were added. The query operator `$comment` is accepted by mongod but has no 7.0 page: not added (see `UNVERIFIED.md`).
5. Node driver pages list inherited `EventEmitter`/stream members (`on`, `pipe`, ...); these are Node.js, not driver API, and were filtered through the class declarations in `mongodb.d.ts`.
6. `OP_INSERT` is a removed opcode and `security.javascriptEnabled` a startup option; both proposed as exclusions rather than counted.
