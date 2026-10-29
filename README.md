# test5

## Engine (`mongo-server`): access control and network limits

### Who may run the administration commands (with `--auth`)

| command | who may run it |
|---|---|
| `rotateCertificates` | the cluster action `rotateCertificates`, as in mongod: `root`, `hostManager`, `clusterAdmin`, or a custom role that carries the action. With TLS on, the certificate, key and CA files are read again and used for every new connection. |
| `tenantStats` (S3 request counters of the whole process) | `root` only (or a role that inherits `root`). Never the roles of one database, nor the other cluster roles. |
| `tenantAdmin` on the data port | nobody (root included). The control layer is the admin port, with its token. |

### Roles a provisioner may hand out: `--grantable-roles`

By default (flag empty) the engine follows mongod: whoever has `grantRole` on a database may hand out any role of that
database, so a provisioner with user administration on every database can grant `dbOwner` to a customer, `userAdmin` on
`admin`, or create a copy of itself.

Set the flag to the roles the control plane really needs:

```
mongo-server --auth --grantable-roles readWrite,read ...
```

With the flag set, a caller that holds neither `root` nor `clusterAdmin` (directly or through a role that inherits them):

* may create users and hand out roles (`createUser`, `updateUser` with `roles`, `grantRolesToUser`) **only** with the roles
  of the list, as built-in roles of the database the user is defined on (the customer database);
* may never manage users on `admin`, `local`, `config` or `$external` (no second provisioner, no change to the password,
  roles or existence of the users of `admin`);
* may never hand out one of its own roles, a role that can manage users or roles, a role of another database, or a custom
  role (even one that includes only `read`);
* may not create, change or drop roles at all;
* may still change passwords and custom data, revoke roles, list and drop the users of customer databases.

The refusal is mongod's `Unauthorized` (code 13). `root` and `clusterAdmin` keep mongod's rules. The rule applies to every
caller without those roles, including a customer's own user administrator.

Accepted values: comma-separated names of built-in roles of one database without user administration: `read`,
`readWrite`, `dbAdmin`. Anything else (`dbOwner`, `userAdmin`, admin or cluster roles, unknown names, empty items, spaces,
`role@db`) stops the engine at startup with a message that names the flag.

The control plane's flows keep working with `--grantable-roles readWrite,read`: customer users with `readWrite` or `read`
on their database, and the internal read-only user of each database `cp_<id>` (role `read`), **created on the customer
database itself** (a user defined on `admin` with `read` on the customer database would be refused).

### Authentication blocks and connection limits

All counters and blocks are in memory, per process. Trusted networks (`--trusted-networks`) are never counted, blocked or
limited. The values in force are shown by `GET /v1/network` on the admin port (`config`), with the blocked addresses;
`DELETE /v1/network/blocks/{ip}` lifts every block of an address and forgets its counts (the address-wide block and the
blocks of every database: the operator's tool). `DELETE /v1/network/blocks/{ip}?database=<db>` lifts **only** the block
of the pair (address, database) and forgets that pair's counts: the address-wide block and the other databases' blocks
stay. The reply says `{"ip", "database", "unblocked"}`, where `unblocked` is false when the pair had nothing tracked; an
empty `database` is a 400.

| flag | default | effect |
|---|---|---|
| `--auth-fail-max` | 100 | failed authentications from one address within the window that block the address on every database (the 100th failure starts the block) |
| `--auth-fail-max-per-db` | 5 | failed authentications from one address on one database within the window that block the address on that database only |
| `--auth-fail-window-secs` | 60 | sliding window of both thresholds |
| `--auth-block-secs` | 300 | first block; each new block of the same key doubles it |
| `--auth-block-max-secs` | 3600 | longest block |
| `--auth-block-forget-secs` | 86400 | without a block for this long, the doubling starts again |
| `--max-conns-per-ip` | 200 | connections at once per address (0: no limit) |
| `--handshake-timeout-secs` | 10 | TLS handshake and first message; before authentication, each message once started (0: off) |
| `--pre-auth-max-message-bytes` | 65536 | largest message before authentication |
| `--ip-allowlist-refresh-secs` | 5 | re-reading of a database's allow list |
| `--ipv6-group-prefix` | 64 | IPv6 addresses are counted per prefix |
| `--netguard-max-tracked` | 100000 | addresses tracked at once (memory bound) |
| `--netguard-max-dbs-per-ip` | 1000 | databases tracked at once per address (memory bound) |

Why 100 per address: a platform with one database per customer connects from few addresses; after a wrong password
rotation, 10 failures would block every database of the platform for up to an hour. The per-database threshold (5) still
stops guessing on one database.

## Engine: request cost, memory and durability modes (round3)

Details and measurements: `results-round3.md`.

### Defaults that changed

| flag | before | now | why |
|---|---|---|---|
| `--process-lease` | off (one lease per open database, renewed every ttl/3: 6 PUT/min each) | **on** (one lease per process; `--process-lease false` goes back) | an open database costs no lease request; safety with two processes: `ctl-tests/test_20_process_lease.py` |
| `--max-concurrent-wakes` | 0 (unlimited) | **32** | a restart wakes every database at once |
| `--writer-poll-secs` | 30 | **60** | one LIST + one GET per open writer per poll |
| `--usage-flush-secs` | 60 | **300** | one PUT per active database per flush |
| `--memtable-max-mb` (new) | slatedb's 64 MiB per database | **8** | memory held by unflushed writes |
| `--block-cache-mb`, `--meta-cache-mb` (new) | slatedb's 512 MiB + 128 MiB **per open database** | **256 + 64 MiB shared by all databases** | memory grew with the data read and written |
| `--change-log-max-mb` (new) | 100 000 events with whole documents | **32 MiB** (and still at most 100 000 events) | ~2.5 KB of memory per write |

The WAL cleanup of open writers (`--wal-gc-mode bulk`, the default) no longer falls back to slatedb's pass when a database has a
retention window: it deletes the WAL objects below the oldest checkpoint (restore points are checkpoints) and below the
writer's replay boundary, using the writer's own view of the manifest. A pass with nothing to delete makes no request.

### Open files: `LimitNOFILE`

Each client connection is one file descriptor; the engine itself holds a few dozen (listeners, the S3 connection pool, the
local log if on). Measured in `results-round3.md`: about 2 descriptors per database with 2 client connections per database.
Rule: `LimitNOFILE` >= 2 x (connections per database) x (databases open at once) + 1024. For a server with up to 2000
databases and drivers that keep 2 to 4 connections each: `LimitNOFILE=65536` (the systemd unit's `[Service]` section).

### Write modes

| mode | how to switch it on | acknowledged when | lost if the process dies (kill -9) | lost if the disk is lost |
|---|---|---|---|---|
| immediate (default) | - | the write's WAL object is in S3 | nothing | nothing |
| adaptive group commit | `--adaptive-group-commit true` | same as immediate; the first write of a group may wait up to half a flush time (max 5 ms) when more writes of the same database are coming, so they share one PUT | nothing | nothing |
| relaxed (per database) | `PUT /v1/databases/{db}/durability {"relaxed_flush_ms": 1000..10000}` (0 = off), or `--relaxed-flush-ms` for every database | a write with write concern `{w: 1, j: false}` (or `w: 0`): once applied in memory. Every other write: as immediate | the relaxed writes of the last `relaxed_flush_ms` (+ one PUT) | the same |
| local log (PROTOTYPE) | `--local-log-dir DIR --local-log-upload-secs 30..60` | the record is fsynced on the local disk | nothing (the restart applies the log) | up to `--local-log-upload-secs` of writes of every database of the process |

**Relaxed durability, exact semantics.** Only a write command (insert, update, delete, findAndModify, bulk writes) whose write
concern is `{w: 1, j: false}`, `{j: false}` or `{w: 0}` (without `j: true`), outside a transaction, to a database whose
`relaxed_flush_ms` is not 0, is acknowledged before it is durable. It is visible at once to every reader of that server (reads of
such a database also see writes that are staged and waiting for their flush, as mongod's `j: false`). The database's writer
flushes its WAL to S3 on its own every `relaxed_flush_ms`, so the write is durable within that time plus one PUT. `j: true`,
`w: "majority"`, a write without any write concern (the drivers' default) and a transaction commit stay durable before the
acknowledgement, and they make durable every relaxed write staged before them. An idle close or a clean shutdown flushes. If
the process dies before the flush, the relaxed writes of the interval are lost, and a reader may have seen them. Setting the
value closes the database if it is open (its writer reopens with the new flush interval).

**Local log (prototype, not a default).** Writes are acknowledged after a group fsync of a log file in `--local-log-dir`;
every `--local-log-upload-secs` each written database flushes its WAL to S3 in one PUT. At start, before accepting
connections, the engine applies again the records not yet in S3 (exactly once: every batch carries a marker with the
record's number). The databases of a process in this mode are tied to its disk: until the restart has run on the same
disk, another process that takes a database over (after the lease expired) does not see the writes that were only in
the log, and losing the disk loses them. All processes of a bucket should use the same mode. See `mongo-server/src/locallog.rs`.
