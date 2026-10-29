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
