# results-engine-round2: permissions with --auth, block thresholds, grantable roles, per-database unblock (branch `engine-round2`)

Base: `origin/network-limits`, tip checked before starting: **`628273b`** (engine commit `1c9fee7`), as expected. Only `engine-round2` is pushed; no merge, no PR; `final-engine`, `network-limits`, `aws-test3`, `main`, `compat-gaps`, `fusion-ops` not touched.
No AWS activity, no real secrets, no load tests.

* **Final engine commit:** `3bcd7e5` (later commits touch only tests, tools, results and this report: `git diff 3bcd7e5..HEAD -- mongo-server Cargo.toml Cargo.lock` is empty). Engine commits of this work: `4d34b90` (points 1–3) and `3bcd7e5` (point 4).
* **Binary:** `cargo build --release -p mongo-server`, **SHA-256 `615c631aea767267e646cacc665995eb77f06f52eb719a94ca8d1b5ba398fcc6`**. Every "after" check of the regression ran on this file (a copy in `/tmp`, so the mutation rebuilds couldn't touch it).
* **Starting binary rebuilt here:** compiled from a clean worktree at `628273b`, it has **exactly** the SHA-256 from the network-limits report (`8b3d7b71…77632f4`): the build is reproducible. I used it to run the new tests "before" the code.
* **Environment rebuilt from scratch** (as last time): reference mongod 7.0.45 taken from the official Docker Hub image `mongo:7.0.45` (fastdl.mongodb.org is blocked by the proxy here), mongodump/mongorestore 100.14.1 **compiled from source** (the image ships 100.20.0), MinIO compiled from source, pymongo 4.19.0, Node driver 7.7.0.

## 1. What I built

Method for each point: first the test (new files `ctl-tests/test_15…18`), run on the binary without the change (section 3.1), then the code, then the same tests, then the mutations (section 3.2).

### Point 1: permissions with `--auth` (`authz.rs`, `auth.rs`)

| command | before | now |
|---|---|---|
| `rotateCertificates` | nobody (needed `anyAction`, error 13 for root too) | the cluster action `rotateCertificates`, as in mongod: **root, hostManager, clusterAdmin** (and a custom role that carries the action). With TLS active it really reloads the certificate (tested for root, hostManager and clusterAdmin: new connections see the new certificate). |
| `tenantStats` | nobody (13 for root too) | **root only** (also through a custom role that inherits root). Refused (13) for read, readWrite, dbOwner, userAdmin of a database, and also for hostManager, clusterAdmin, clusterManager, clusterMonitor, userAdminAnyDatabase, readWriteAnyDatabase, dbAdminAnyDatabase, on `admin` and on the customer database. |
| `tenantAdmin` on the data port | nobody | **unchanged**: nobody (tested with root, a role inheriting root, readWrite, dbOwner, hostManager, clusterAdmin, on admin and on a customer database). |

Negative cases requested: a readWrite user of one database gets **13 on both commands** (also through TLS, and the certificate doesn't change). Without authentication: 13 on all three.

### Point 2: block thresholds (`netguard.rs`)

* **`--auth-fail-max`: default from 10 to 100** errors in 60 s per IP (the 100th failure starts the block). `--auth-fail-max-per-db` stays **5**.
* Every threshold and duration has a flag with a description in `--help` and the default shown (checked by a test that reads `--help`). Added: the description of `--auth-fail-window-secs` (it had none), the units (seconds) on the durations, and a new flag **`--netguard-max-dbs-per-ip`** (default 1000) in place of the 1000 that was hard-coded (maximum number of databases tracked per IP).
* **`GET /v1/network`** shows all the values in force, now also `netguard_max_tracked` and `netguard_max_dbs_per_ip` (tested with defaults and with every flag changed).
* Full table of flags in the README.

Tests (server started with **no** limit flags): 99 errors from one IP spread over 25 databases (4 per database, so no pair reaches 5) don't block: the correct password works; the **100th blocks** the IP on every database (another IP is not affected). 5 errors on the same pair block **only that database**: the other databases stay open for that IP, and the database stays open for other IPs.

### Point 3: `--grantable-roles` (`auth.rs`, `authz.rs`, `main.rs`)

* **Flag:** `--grantable-roles`, **default empty = no restriction** (unchanged behavior, compatible with mongod; tested: without the flag the provisioner can still grant dbOwner, userAdmin@admin and copy itself).
* **With the flag set** (e.g. `readWrite,read`), anyone without root or clusterAdmin (also counted when inherited):
  * can create users and grant roles (`createUser`, `updateUser` with `roles`, `grantRolesToUser`) **only** with roles from the list, as built-in roles **of the database the user is defined on**;
  * **never on `admin`** (and not on `local`, `config`, `$external`): for user commands on these databases (create, modify, drop, password, revoke) the answer is always 13. So no second provisioner, and no takeover of root by changing its password;
  * **never their own role**, never a role with user or role management powers, never a role of another database, never a custom role;
  * **cannot create or modify roles** (`createRole`, `updateRole`, `grantRolesToRole`, `grantPrivilegesToRole`, and the corresponding drop/revoke);
  * can still change passwords and customData, revoke roles, list and drop the users of customer databases.
* **Value validated at startup:** only built-in database roles without user management (`read`, `readWrite`, `dbAdmin`). `dbOwner`, `userAdmin`, admin/cluster roles, unknown names, empty items, spaces, `role@db` → the engine doesn't start, with a message that names the flag.
* Refusal: `Unauthorized` (13), same text as mongod.
* **Control plane flows tested with `readWrite,read`** (with a custom provisioner role, and also with `userAdminAnyDatabase`): users with readWrite and read on the customer database, role given as a string, internal user `cp_<id>` with `read` (it reads, and can't write), password rotation, customData, updateUser/grant/revoke between read and readWrite, usersInfo, dropUser. Everything works.
* **Hostile cases refused: 39** (each tried by both provisioners = 78 tests). After each one, I check that users and roles on admin, c1 and c2 are unchanged and that root still logs in. Cases: dbOwner (object and string), userAdmin, dbAdmin and enableSharding not in the list, userAdmin@admin, read@admin, user with no roles on admin, readWrite@admin from a customer, root (on admin and to a customer user), clusterAdmin, hostManager, clusterMonitor, userAdminAnyDatabase, readWriteAnyDatabase, **own role** (provisioner@admin to a customer user), **second provisioner** (on admin), **custom role that includes dbOwner**, custom role with only read, role from another database, list mixing a good role and a bad one, grant/update of dbOwner/userAdmin to an existing user, grant root or userAdminAnyDatabase to itself, **changing root's password**, dropping root, revoking root from root, users on local/config/$external, createRole with createUser, createRole inheriting the provisioner, grantRolesToRole root to its own role, grantPrivilegesToRole, updateRole and grantRolesToRole on a custom role.
* root and clusterAdmin+userAdminAnyDatabase keep mongod's rules with the flag set (tested).
* Documented in the README ("Roles a provisioner may hand out").

### Point 4: per-database unblock (`netguard.rs`, `ctl/allowlist.rs`, `ctl/api.rs`)

* **`DELETE /v1/network/blocks/{ip}?database=<db>`**: removes **only** the block of the pair (IP, database) and forgets that pair's counters. It does **not** touch the global block of the IP or the blocks of other databases, and keeps the IP's global counter. Reply `{"ip", "database", "unblocked"}` (`false` if the pair had nothing). Empty `database` → 400; without the token → 401.
* **Without the parameter: unchanged** (removes everything for the IP; reply `{"ip", "unblocked"}` as before).
* Tests: unblock of A with blocks on A and B → A open, B still blocked; with a global block + A + B → after unblocking A the global and B remain, and all databases stay closed; unblocking a pair with no entry changes nothing; then without the parameter everything goes, as before; pair counters forgotten, global ones and other databases' kept; error requests.
* Documented in the README (section on blocks).

### Files touched

| file | |
|---|---|
| `mongo-server/src/authz.rs` | privilege of `rotateCertificates`, `tenantStats` without action privileges (checked on roles), validation of `--grantable-roles`, unit test |
| `mongo-server/src/auth.rs` | "root only" check for `tenantStats`, `--grantable-roles` policy (`grant_policy`), expanded roles of the connection |
| `mongo-server/src/main.rs` | flag `--grantable-roles` (validated before anything else starts) |
| `mongo-server/src/netguard.rs` | default 100, flag descriptions, `--netguard-max-dbs-per-ip`, values in the status, `unblock_db`, unit test of the defaults |
| `mongo-server/src/ctl/allowlist.rs`, `ctl/api.rs` | `database` parameter of the unblock |
| `README.md` | who may run the admin commands, `--grantable-roles`, table of the block flags, unblock |
| `ctl-tests/test_15_admin_cmd_privileges.py`, `test_16_block_defaults.py`, `test_17_grantable_roles.py`, `test_18_unblock_per_database.py` | new tests |
| `tools/round2/mutations.py`, `tools/round2/secret_scan.py` | mutations, secret scan |
| `results-engine-round2/`, `results-engine-round2.md` | results |

**No existing test was modified.** (The only fix was in one of my new tests: the regex that reads `--help` didn't accept digits in `<IPV6_GROUP_PREFIX>`. It was the test's fault, not the engine's; corrected before the commit.)

## 2. New tests

| file | tests | starting binary | final binary |
|---|---|---|---|
| `test_15_admin_cmd_privileges.py` | 54 | 7 failed (all "allowed" cases: root, hostManager, clusterAdmin on rotate; root on tenantStats; TLS) | **54 passed** |
| `test_16_block_defaults.py` | 5 | 4 failed | **5 passed** |
| `test_17_grantable_roles.py` | 94 | 82 errors + 1 failed (the flag doesn't exist: the server doesn't start) | **94 passed** |
| `test_18_unblock_per_database.py` | 4 | 4 failed (run on the binary from points 1–3, before point 4) | **4 passed** |
| Rust unit tests (whole crate) | 60 | – | **60 passed** (this time the other 51 too) |

On the starting binary some tests "pass" without proving anything (`results-engine-round2/before_new_tests_by_test.txt`): the refusals (13) of `rotateCertificates`/`tenantStats`/`tenantAdmin`, because the old engine refused everyone; `test_5_failures_on_one_pair…`, because the per-pair threshold was already 5; `test_bad_values_refuse_to_start`, because the flag didn't exist (the error message contains its name anyway). The real proof is the mutations.

## 3. Negative checks

### 3.1 Before/after
Outputs: `results-engine-round2/before_new_tests.txt`, `before_point4_tests.txt`, `after_new_tests.txt`.

### 3.2 Mutations (`tools/round2/mutations.py`, diff and output in `results-engine-round2/mutations/`)
Each check removed, engine rebuilt, test file run, source restored with `git checkout` (at the end `git status` clean and binary rebuilt; its SHA equals the final one).

| mutation | what it removes | failed tests |
|---|---|---|
| `rotate_privilege` | privilege of rotateCertificates (back to nobody) | **5** of 54 |
| `tenant_stats_root_only` | root-only check of tenantStats | **26** of 54 |
| `fail_max_100` | default back to 10 | **2** of 5 |
| `fail_max_per_db` | per-pair threshold off | **1** of 5 |
| `grantable_list` | list check (any role goes through) | **2** of 94 |
| `protected_dbs` | ban on admin/local/config/$external | **30** of 94 |
| `own_role` | ban on own role | **1** of 94 |
| `same_database` | ban on roles of another database | **4** of 94 |
| `role_commands` | ban on creating/modifying roles | **6** of 94 |
| `startup_check` | startup validation of the flag | **2** of 94 |
| `runtime_power_check` | runtime re-check of user management powers | **0**: survives, by construction. A role with powers can't get into the list (startup validation, caught by `startup_check`), so the re-check is a redundant second layer that no test can reach. |
| `unblock_one_database` | the `database` parameter ignored (unblock everything) | **3** of 4 |

The mutations for points 1–3 ran on the source of `4d34b90`; point 4 (`3bcd7e5`) only adds a separate function and a parameter, without touching the mutated lines.

## 4. Regression on the final binary (`615c631a…`)

"Before" = numbers from `results-network-limits.md` (binary `8b3d7b71…`, which I reproduced identically here, but I did **not** rerun its regression). "After" = this environment. Raw outputs in `results-engine-round2/regression/`.

| check | before (network-limits) | after |
|---|---|---|
| pymongo core / extra / replset | 3989/3989, 49/49, 89/89 | **3989/3989, 49/49, 89/89** |
| Node core / extra / replset | 3989/3989, 49/49, 89/89 | **3989/3989, 49/49, 89/89** |
| ctl (all files) | 267 passed, 0 failed | **424 passed, 0 failed** (267 existing + 157 new) |
| of which test_09_security | 30/30 | **30/30** |
| of which network-limits tests (test_13 / test_14) | 84/84, 10/10 | **84/84, 10/10** |
| ctl with `--auth` (test_01…08b) | 71 / 45 | **71 / 45**, same files and same numbers |
| SIGKILL wal_1..4 | 0 losses | **0 inserts and 0 updates lost** (54307, 55539, 58191, 60354 confirmed writes) |
| SIGKILL sst_1..4 | 0 lost, 0 missing, 0 stale | **0 lost, 0 missing, 0 stale** (10714, 11133, 10898, 11018 documents) |
| controls walctl_1..2 (must lose) | lose | **656/18 and 308/22 lost** (the control works) |
| controls sstctl_1..2 (must lose) | lose | **8122/8122 and 8341/8341 lost** (the control works) |

**On the 45 `--auth` failures:** they are all refusals of `tenantAdmin` on the data port (90 `not authorized … tenantAdmin` messages in the outputs, 2 per failure, none for `tenantStats`). This is the known limit, unchanged on purpose. Unlocking `tenantStats` for root doesn't change the count: the tests that use it (fork, export, restore_gc) fail anyway on `tenantAdmin`.

The JSON files of the SIGKILL tests (which the script rewrites into `loadtest/results/`) are copied into `results-engine-round2/regression/kill_json/`; the tracked files were restored as they were.

## 5. Secret scan (`tools/round2/secret_scan.py`, `results-engine-round2/secret_scan.json`)
* **Logs and outputs:** 822 files, 18 values searched (the passwords of all tests, the new ones too, nonce, admin token, MinIO pair): **none found**, not even in the engine logs.
* **Repository files:** the 158 files new or modified compared to `origin/network-limits`, searched for cloud keys, private keys, provider tokens, credentials in URLs: **none found**. The fake test passwords (`Round2-SECRET-…`, `Grant-SECRET-…`) are test inputs, listed separately in the JSON.

## 6. Decisions I need from you

* **D1: `cp_<id>` must be defined on the customer database.** With the flag set, I accept only the database's roles **on the database where the user is created**. If the control plane creates `cp_<id>` on `admin` with `read@<id>`, it would be refused. Easy to allow if needed, but then you'd have to pick a rule (e.g. "only read, only on customer databases") for users defined on admin.
* **D2: no cross-database roles** (a user of c1 with readWrite on c2 is refused). This is what you described ("on the customer's database").
* **D3: custom roles never grantable, and role creation/modification refused** to non-root users when the flag is set (even a role that contains only `read`). It's the simplest way to rule out "a custom role that includes them".
* **D4: the rule applies to everyone without root/clusterAdmin, so customers too.** A customer with `userAdmin`/`dbOwner` on its own database, when the flag is set, can only grant the roles in the list.
* **D5: "never its own role" applied literally:** a caller can't grant a role (name, database) it holds itself. Example: a customer admin that has `read` on its database can't grant `read` (it can grant `readWrite`). For the provisioner this changes nothing (its role is on admin). Tell me if you'd rather limit it to the provisioner role only.
* **D6: `tenantStats`:** root also counts through a custom role that inherits root. clusterAdmin is refused, as you asked.
* **D7: other admin commands left as they were:** `dropConnections`, `setUserWriteBlockMode`, `setFeatureCompatibilityVersion` still need `anyAction` with `--auth` (nobody can run them, like rotate before). In mongod they belong to hostManager/clusterManager. I didn't touch them because you didn't ask; it's a small change if you want it.
* **D8: `dbAdmin` is accepted in the flag** (it has no user management, but it does have `dropDatabase`). If you want only read/readWrite to be allowed, I'll restrict the validation.
* **D9: the flag's value isn't visible over the network** (it's not network state): it's printed in the startup log ("grantable roles …"). If you want it in `GET /v1/network` or in another route, it's quick.
* **D10: per-database unblock** also forgets that pair's counters, but **not** the IP's global counter (failures on that database still count toward the 100).
* **D11: with 100 per IP**, an attacker from one IP can try up to 99 passwords a minute spread across ≥20 databases (4 per database) without being blocked globally. It's the price of the choice; the pair threshold (5) still limits each single database.
* **D12: tenantStats and aws-test3.** The tenantStats authorization is in `authz.rs` (`"tenantStats" => vec![]`) and in `auth.rs` (`Server::authorize`, "root only" check). I didn't read the one-line change in aws-test3: when you reconcile, look at those two places.
* **Branch:** as you asked, I pushed to `engine-round2` (the session had suggested another name, `claude/engine-round2-auth-limits-pbxa6o`, which I didn't use).

## 7. What I could NOT verify

* **"Before" regression numbers in the same environment:** not rerun; they come from `results-network-limits.md`. However the starting binary I rebuilt has the same SHA-256 as that report.
* **Reference mongod:** from the Docker Hub image, not from the fastdl tgz (blocked); mongodump/mongorestore 100.14.1 compiled from source without `gssapi` (Kerberos, not used by the tests).
* **Real control plane:** I verified its flows as you described them (readWrite/read on the customer database, `cp_<id>` read), not with the control plane's own code or its e2e suite.
* **IPv6, non-loopback addresses, multiple processes on the same bucket, real S3, load:** not tested (as in network-limits; no load test as requested). Per-database unblocks are per process, like the blocks.
* **TLS rotation** only with self-signed certificates generated by the test; no client-certificate/CA rotation in the new tests (existing `test_09c` and `test_11` pass in the regression).
* **gaps g01–g12** tests of compat-gaps: not run.

## 8. Branch and commits

Branch `engine-round2` (from `origin/network-limits` 628273b). Final engine commit: `3bcd7e5`. Binary SHA-256: `615c631aea767267e646cacc665995eb77f06f52eb719a94ca8d1b5ba398fcc6`. Only `origin/engine-round2` pushed.
