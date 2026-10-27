#!/usr/bin/env python3
"""Step 3: generate compat/COVERAGE.md from inventory.csv, the stored run results and the two usage scans.

Everything numeric in the report is computed here from files in compat/ (nothing is typed by hand except the one-line explanations of each gap group,
which restate what the cited probes/tests showed)."""
import collections, csv, json, os, re, subprocess, sys, datetime
HERE = os.path.dirname(os.path.abspath(__file__)); C = os.path.join(HERE, "..")
sys.path.insert(0, C)
import coverage

rows = coverage.load()
by = {r["id"]: r for r in rows}
inscope = [r for r in rows if r["exclusion_proposed"] != "yes"]
excl = [r for r in rows if r["exclusion_proposed"] == "yes"]
cov = [r for r in inscope if r["engine_status"] == "covered"]
ST = collections.Counter(r["engine_status"] for r in inscope)

# ---------------------------------------------------------------- usage data
orig = json.load(open(os.path.join(C, "..", "results9", "compat-scan-before.json")))
extra = json.load(open(os.path.join(C, "scan_extra.json")))
ORIG_REPOS = {r["name"]: r for r in orig["repos"]}


def orig_usage(kind, key):
    repos = occ = 0
    for r in orig["repos"]:
        v = r.get(kind, {}).get(key)
        if v and v["app"] > 0:
            repos += 1; occ += v["app"]
    return repos, occ


def extra_usage(feature):
    repos = occ = 0
    for repo, v in extra.items():
        x = v.get("features", {}).get(feature)
        if x and x["app"] > 0:
            repos += 1; occ += x["app"]
    return repos, occ


ROLE = {"qop": "query", "upd": "update", "stage": "stage", "expr": "expression", "acc": "accumulator"}


def X(name):   # exact extra-scan feature key by prefix
    return next(k for k in next(iter(extra.values()))["features"] if k.startswith(name))


def ids_in(*prefixes):
    return lambda r: any(r["id"].startswith(p) for p in prefixes)


def cat_in(*cats):
    return lambda r: r["category"].split(" / ")[0] in cats


def any_of(*fs):
    return lambda r: any(f(r) for f in fs)


def idset(*ids):
    s = set(ids)
    return lambda r: r["id"] in s


def opt_field(*fields):
    return lambda r: r["id"].startswith("opt:") and r["id"].split(".")[-1] in fields


# (name, predicate, usage resolver or None, what is wrong -- restating verified probe/test results)
GROUPS = [
    ("Authentication: SCRAM-SHA-1/256 (saslStart, saslContinue, authenticate, logout)",
     any_of(ids_in("authmech:SCRAM", "cmd:saslStart", "cmd:saslContinue", "cmd:authenticate", "cmd:logout")),
     lambda: extra_usage(X("auth: credentials")),
     "saslStart returns CommandNotFound, so any connection string that carries credentials fails at connect (pymongo: OperationFailure 'no such command: saslStart')."),
    ("TLS (tls=true, mongodb+srv:// URIs)", ids_in("tls:"), lambda: extra_usage(X("TLS options")),
     "the engine's listener is plain TCP (no TLS code, buildInfo reports openssl disabled); a client with tls=true cannot connect."),
    ("Collation: fields, 150 locales/variants, per-command collation options, case-insensitive indexes",
     any_of(cat_in("collation", "collation locales"), opt_field("collation"), idset("idxprop:case-insensitive index"), lambda r: r["id"].startswith(("py:", "node:")) and "collation" in r["name"].lower()),
     lambda: extra_usage(X("collation")),
     "every collation document is accepted and ignored: results are binary-ordered and case-sensitive (probes compat.collation.*, compat.opt.*.collation)."),
    ("Server diagnostics commands (serverStatus, currentOp, killOp, top, hostInfo, getCmdLineOpts, connPoolStats, dbHash, dataSize, lockInfo, getLog, listCommands, ...)",
     any_of(lambda r: r["id"].startswith("cmd:") and r["category"].endswith("diagnostics"), ids_in("stage:$currentOp", "stage:$queryStats", "stage:$listSessions", "stage:$listLocalSessions", "node:Admin.serverStatus", "py:pymongo.mongo_client.MongoClient.server_info")),
     lambda: extra_usage(X("diagnostics commands")),
     "these commands return CommandNotFound (probe_results.json); health checks and monitoring agents that call serverStatus/currentOp fail."),
    ("User and role management (createUser, usersInfo, grantRolesToUser, createRole, ...)", lambda r: r["id"].startswith("cmd:") and r["category"].endswith("user and role management") or r["id"] in ("node:Db.removeUser",),
     lambda: extra_usage(X("user/role management")), "all return CommandNotFound; there is no user store."),
    ("createIndexes with several indexes in one call", any_of(idset("opt:createIndexes.indexes", "cmd:createIndexes"), lambda r: r["id"].startswith(("py:", "node:")) and ("create_indexes" in r["name"] or "createIndexes" in r["name"] or "index_information" in r["name"] or ".indexes" in r["name"] or "indexExists" in r["name"] or "indexInformation" in r["name"]) and r["engine_status"] != "covered"),
     lambda: extra_usage(X("createIndexes with an array")),
     "the reply claims numIndexesAfter = 4 but only the FIRST index of the batch is created (probe compat.opt.createIndexes.several_indexes_in_one_call)."),
    ("Collection validation at create/collMod ($jsonSchema validator, validationLevel/Action)", opt_field("validator", "validationLevel", "validationAction"),
     lambda: extra_usage(X("collection validation")), "create/collMod accept the options and ignore them (collection created with no options)."),
    ("Capped collections (capped/size/max, cappedSize) and tailable/awaitData cursors", any_of(opt_field("capped", "size", "max", "cappedSize", "cappedMax", "tailable", "awaitData"), ids_in("cursor:tailable", "cursor:awaitData"), lambda r: r["id"].startswith("py:") and "add_option" in r["name"] or "remove_option" in r["name"]),
     lambda: extra_usage(X("tailable cursor")), "capped options are ignored and a tailable cursor dies at the end of the first batch instead of staying open."),
    ("Time series collections", opt_field("granularity", "metaField", "timeField", "bucketMaxSpanSeconds", "bucketRoundingSeconds", "timeseries.timeField", "timeseries.metaField", "timeseries.granularity") if False else (lambda r: r["id"].startswith("opt:create.timeseries") or r["id"] in ("opt:create.expireAfterSeconds", "opt:collMod.expireAfterSeconds")),
     lambda: extra_usage(X("time series")), "create with timeseries creates an ordinary collection (type 'collection', _id index present)."),
    ("Views (viewOn / pipeline)", idset("opt:create.viewOn", "opt:create.pipeline", "opt:collMod.viewOn", "opt:collMod.pipeline"), lambda: extra_usage(X("view (viewOn")),
     "view creation is accepted but the system.views behaviour differs (probes compat.opt.create.viewOn_pipeline, compat.opt.collMod.viewOn)."),
    ("Clustered collections / clustered index", idset("idxtype:clustered index", "opt:create.clusteredIndex"), None, "clusteredIndex is ignored (no clustered index created)."),
    ("Retryable writes (txnNumber replay) and retryWrites", idset("sess:txnNumber (retryable writes)", "sess:retryWrites"), lambda: extra_usage(X("retryable writes")),
     "a replayed write with the same lsid+txnNumber is executed again (update applied twice; insert replay returns n=0 instead of the original result)."),
    ("Causal consistency and cluster time (operationTime, $clusterTime, afterClusterTime, startAtOperationTime)", any_of(idset("sess:causal consistency", "sess:$clusterTime", "rc:afterClusterTime", "cso:$changeStream.startAtOperationTime"), lambda r: r["id"].startswith(("py:", "node:")) and ("cluster_time" in r["name"] or "operation_time" in r["name"] or "clusterTime" in r["name"] or "operationTime" in r["name"])), None,
     "replies carry no operationTime/$clusterTime, so session.operation_time/cluster_time stay None."),
    ("Projection: positional $, $elemMatch and aggregation expressions in find projections", ids_in("proj:"), lambda: extra_usage(X("$elemMatch")),
     "'arr.$' returns an empty array, $elemMatch in a projection and expression values are rejected (BadValue)."),
    ("$lookup with let/pipeline when the let variable is missing", idset("stage:$lookup"), lambda: orig_usage("operators", "$lookup|stage"),
     "$$var bound to a missing field matches documents whose field is null (real: only documents where it is missing) in lookup.extra_let_pipeline_form."),
    ("$out / $merge into another database; renameCollection across databases", idset("stage:$out", "stage:$merge", "cmd:renameCollection"), lambda: tuple(map(max, zip(orig_usage("operators", "$merge|stage"), orig_usage("operators", "$out|stage")))),
     "NotImplemented (238) for any target in a different database (probes compat.stage.out_other_database, compat.stage.merge_other_database, compat.cmd.renameCollection_across_databases)."),
    ("let option on find/aggregate/update/delete/findAndModify", opt_field("let"), None, "'Use of undefined variable' (17276): let variables are not bound."),
    ("$where details ($where `obj` alias)", idset("qop:$where"), lambda: orig_usage("operators", "$where|query"), "`obj` is undefined inside $where (JSInterpreterFailure); `this` works."),
    ("$natural sort/hint and default (insertion) order", ids_in("sorthint:"), lambda: extra_usage(X("$natural")), "sort/hint on $natural is ignored and documents come back in _id order, not insertion order."),
    ("Change streams: expanded events, rename event, pre/post images, whenAvailable/required", any_of(cat_in("change stream events", "change streams"), lambda r: r["id"].startswith("py:") and False), lambda: extra_usage(X("change stream pre/post")),
     "create/createIndexes/dropIndexes/modify/rename events are not emitted; fullDocument whenAvailable/required and pre-images are not honoured; TTL delete event missing in the pymongo run of cs.ttl_delete_event."),
    ("mapReduce", any_of(idset("cmd:mapReduce"), lambda r: r["id"].startswith("opt:mapReduce")), lambda: extra_usage(X("mapReduce")), "CommandNotFound."),
    ("Window functions $top/$bottom/$topN/$bottomN/$firstN/$lastN/$maxN/$minN in $setWindowFields", cat_in("window functions"), lambda: orig_usage("operators", "$setWindowFields|stage"), "FailedToParse (9) for these window operators."),
    ("Trigonometric, bitwise and misc. expressions ($sin..$atanh, $bitAnd/Or/Xor/Not, $toHashedIndexKey, $stdDevPop as an expression)", cat_in("aggregation expressions"), None, "'Unknown expression' for each."),
    ("Profiler, plan cache, reIndex, compact, convertToCapped, validateDBMetadata, filemd5, getDefaultRWConcern, getClusterParameter, setIndexCommitQuorum",
     any_of(idset("cmd:profile", "cmd:reIndex", "cmd:compact", "cmd:convertToCapped", "cmd:cloneCollectionAsCapped", "cmd:validateDBMetadata", "cmd:filemd5", "cmd:getDefaultRWConcern", "cmd:getClusterParameter", "cmd:setIndexCommitQuorum", "cmd:compactStructuredEncryptionData", "node:Db.profilingLevel", "node:Db.setProfilingLevel"), lambda r: r["id"].startswith("cmd:planCache")),
     lambda: extra_usage(X("profiler")), "CommandNotFound."),
    ("Sessions: startSession, killAllSessions, killAllSessionsByPattern", idset("cmd:startSession", "cmd:killAllSessions", "cmd:killAllSessionsByPattern"), None, "CommandNotFound."),
    ("Compression (OP_COMPRESSED / compressors=)", idset("wire:OP_COMPRESSED"), lambda: extra_usage(X("compression option")), "not advertised in hello: clients silently fall back to uncompressed (works, but is not compression)."),
    ("Stable API strict mode (apiStrict)", idset("stableapi:apiStrict"), None, "commands outside API v1 (distinct, collStats, dbStats) are not rejected with APIStrictError."),
    ("Read/write concern fidelity (unsatisfiable w, unknown tag names, afterClusterTime)", idset("wc:w: <number>", "wc:w: <tag set name>", "rc:afterClusterTime"), None, "w greater than the member count and unknown custom write-concern tag names are accepted instead of failing (UnsatisfiableWriteConcern / UnknownReplWriteConcern)."),
    ("collMod on indexes/views/capped, $bucketAuto granularity, listCollections info.uuid, Collection.options()", any_of(idset("stage:$bucketAuto", "cmd:listCollections", "cmd:collMod"), lambda r: r["id"].startswith("opt:collMod.")), None, "collMod index/view changes are not applied; listCollections omits info.uuid; $bucketAuto supports only POWERSOF2 and 1-2-5."),
    ("Numeric/array/binData comparison edge cases (int64 vs double, NaN, empty array in descending sort, binData subtype 7)", any_of(cat_in("bson comparison order", "bson binary subtypes")), None, "int64 2^53+1 vs double 2^53 order, NaN under $lt, [] vs MinKey in descending sorts, subtype 7 validation (probes compat.bson.*)."),
    ("find options min/max/returnKey/showRecordId, singleBatch-related cursor options", any_of(opt_field("min", "max", "returnKey", "showRecordId"), lambda r: r["id"].startswith(("py:", "node:")) and any(w in r["name"] for w in (".min", ".max", "returnKey", "showRecordId"))), lambda: extra_usage(X("returnKey")), "options are accepted and ignored."),
]


def usage_of(group_rows, fn):
    if fn is None:
        return None
    return fn()


# rows already assigned stay with the first matching group
gaps = [r for r in inscope if r["engine_status"] != "covered"]
assigned, groups_out = set(), []
for name, pred, ufn, why in GROUPS:
    members = [r for r in gaps if r["id"] not in assigned and pred(r)]
    if not members:
        continue
    for r in members:
        assigned.add(r["id"])
    # expression rows: usage per row (max over members of the original scan key)
    usage = None
    if ufn is not None:
        usage = ufn()
    elif all(r["id"].split(":")[0] in ROLE for r in members):
        best = (0, 0)
        for r in members:
            u = orig_usage("operators", f"{r['name']}|{ROLE[r['id'].split(':')[0]]}")
            best = max(best, u)
        usage = best
    groups_out.append(dict(name=name, rows=members, usage=usage, why=why))
rest = [r for r in gaps if r["id"] not in assigned]
rest_by_cat = collections.defaultdict(list)
for r in rest:
    rest_by_cat[r["category"].split(" / ")[0]].append(r)


def sortkey(g):
    u = g["usage"]
    return (0, -u[0], -u[1], -len(g["rows"])) if u is not None else (1, 0, 0, -len(g["rows"]))


groups_out.sort(key=sortkey)

# ---------------------------------------------------------------- facts for the header
def git(*a):
    try:
        return subprocess.run(["git", "-C", os.path.join(C, ".."), *a], capture_output=True, text=True).stdout.strip()
    except Exception:
        return "?"


def suite(f):
    d = json.load(open(os.path.join(C, "results", f)))
    return d


def tally(*fs):
    names = {}
    for f in fs:
        for x in suite(f):
            names[x["name"]] = x
    n = len(names); p = sum(x["status"] == "pass" for x in names.values())
    return p, n, [k for k, x in names.items() if x["status"] != "pass"]


py_p, py_n, py_fail = tally("py_main.json", "py_retry.json")
nd_p, nd_n, nd_fail = tally("node_core.json", "node_replset.json", "node_retry.json")
probes_d = json.load(open(os.path.join(C, "probe_diff_results.json")))["cases"]
probes_n = json.load(open(os.path.join(C, "probe_node_results.json")))["cases"]
pd_p = sum(v["status"] == "pass" for v in probes_d.values()); nd2_p = sum(v["status"] == "pass" for v in probes_n.values())
lc = json.load(open(os.path.join(C, "listCommands-mongo-7.0.json")))

# ---------------------------------------------------------------- write
out = []
w = out.append
tot, cv = len(inscope), len(cov)
w("# MongoDB 7.0 driver-reachable surface: what FENVIX covers\n")
w(f"Generated by `compat/tools/report.py` from `compat/inventory.csv` on {datetime.date.today().isoformat()}. Reference server: `mongo:7.0` = mongod {lc['server_version']} (`{lc['git'][:10]}`). "
  f"Engine: `mongo-server` built from `{git('rev-parse', '--short', 'origin/compat-indexes')}` (branch `compat-indexes`; engine source untouched by this audit). "
  "Docs: the MongoDB 7.0 manual (www.mongodb.com/docs/v7.0), pymongo 4.19 and mongodb (Node.js) 7.7 API references; every URL in the CSV was fetched (`compat/fetch_log.csv`).\n")
w("## 1. Overall\n")
w(f"**{cv} / {tot} covered = {100*cv/tot:.1f}%** of the driver-reachable surface (rows proposed for exclusion are not counted).\n")
w(f"Of the {tot} in-scope rows: {ST['covered']} covered, {ST['partial']} partial (supported with a stated difference), {ST['untested']} untested (code exists, no differential test), {ST['missing']} missing (engine rejects or ignores it). "
  f"The inventory has {len(rows)} rows; {len(excl)} are proposed exclusions (section 4).\n")
w("`covered` means at least one passing differential test against mongo:7.0 and no failing one; the evidence column of the CSV names the tests. Two sources of tests were used: "
  f"(a) the repository's own differential suite (`mongo-tests`: {py_n} tests) run with pymongo ({py_p}/{py_n} pass) and with the Node.js driver ({nd_p}/{nd_n} pass); "
  f"(b) {len(probes_d)} + {len(probes_n)} extra probes written for this audit (`compat/probes`, same method: identical request on real mongod and on the engine, canonical comparison): {pd_p}/{len(probes_d)} (pymongo) and {nd2_p}/{len(probes_n)} (Node.js) identical.\n")
w("Run notes: persistence tests and TTL tests were first run against a `--store local` engine and a stock mongod (TTL monitor sleeps 60 s) and failed for environmental reasons; "
  "they were re-run against an S3-compatible store (moto) and a mongod with `ttlMonitorSleepSecs=1`, and the re-run result replaces the first one (both stored in `compat/results/`). "
  "Replica-set tests ran against a single-node replica set. Failing after the re-runs: " + ", ".join(sorted(set(py_fail) | set(nd_fail))) + ".\n")
w("## 2. Coverage per category\n")
w("| category | covered / total | % | partial | untested | missing |\n|---|---:|---:|---:|---:|---:|")
agg = collections.OrderedDict()
for r in sorted(inscope, key=lambda r: r["category"].split(" / ")[0]):
    a = agg.setdefault(r["category"].split(" / ")[0], collections.Counter())
    a["total"] += 1; a[r["engine_status"]] += 1
for k, a in agg.items():
    w(f"| {k} | {a['covered']} / {a['total']} | {100*a['covered']/a['total']:.1f}% | {a['partial']} | {a['untested']} | {a['missing']} |")
w(f"| **overall** | **{cv} / {tot}** | **{100*cv/tot:.1f}%** | {ST['partial']} | {ST['untested']} | {ST['missing']} |\n")
nloc = sum(r["id"].startswith("locale:") for r in inscope); ndrv = sum(r["id"].startswith(("py:", "node:")) for r in inscope)
w(f"Sub-category numbers (`python3 compat/coverage.py --detail`) are reproducible from the CSV alone. Caveat on weighting: {nloc} of the {tot} denominator rows are individual collation locales and {ndrv} are pymongo/Node.js class members; "
  "a flat row count weighs them like any other row, which is not how often applications use them (section 3 ranks by usage, not by row count).\n")
miss = [r for r in inscope if r["engine_status"] == "missing"]
def how(r):
    e = r["evidence"]
    if "engine rejects it" in e or "[reject]" in e: return "rejected: the engine returns an error (CommandNotFound / unknown operator / parse error)"
    if "[ignored]" in e or "[differ]" in e: return "accepted without error but with no effect or a different result than mongod (silent: wrong results, no crash)"
    if r["id"].startswith(("py:", "node:")) and "=missing" in e: return "driver member that calls a command/stage the engine rejects"
    return "other (see the evidence column)"
hc = collections.Counter(how(r) for r in miss)
w("Of the missing rows, how the engine fails (this matters for the promise \"no query that crashes because we do not support it\": rows rejected with an error, and the driver members that call them, are the ones that crash a query; the silent kind returns wrong results with no error at all, which is the more dangerous failure for a drop-in replacement):\n")
for k, v in hc.most_common():
    w(f"- {v} rows — {k}")
w("")
w("## 3. The 30 biggest gaps, ranked by real-application usage\n")
w("**Usage data.** Two text scans of the same 10 public repositories as `compat-scan.md` (Rocket.Chat, parse-server, NodeBB, wekan, LibreChat, novu, appsmith, countly-server, payload, edx-platform): "
  "`results9/compat-scan-before.json` for operators/methods/index options (taken as is) and `compat/scan_extra.json` (new, `compat/tools/scan_extra.py`, same file selection) for the features the first scan did not measure. "
  "\"Repos\" = number of the 10 repositories whose application code (tests excluded) mentions the feature; \"hits\" = application-code occurrences. Both scans are plain text matching and over/under-count "
  "(for example `$elemMatch` cannot be told apart between query and projection, `retryWrites` is default-on in drivers whether or not it is written down, and generic names collide with application code). "
  "10 repositories are a small, framework-heavy sample. Gaps the scans cannot measure are listed after the measured ones and marked *not measured*; within each block they are ordered by number of in-scope rows affected.\n")
w("| # | gap (rows affected) | repos / hits | status mix | what is wrong (verified) |\n|---:|---|---|---|---|")
for i, g in enumerate(groups_out[:30], 1):
    c = collections.Counter(r["engine_status"] for r in g["rows"])
    mix = ", ".join(f"{v} {k}" for k, v in c.most_common())
    u = f"{g['usage'][0]} / {g['usage'][1]}" if g["usage"] is not None else "*not measured*"
    w(f"| {i} | {g['name']} ({len(g['rows'])}) | {u} | {mix} | {g['why']} |")
if len(groups_out) > 30:
    w(f"\n{len(groups_out) - 30} further gap groups below the top 30:\n")
    for g in groups_out[30:]:
        w(f"- {g['name']} ({len(g['rows'])} rows; " + (f"{g['usage'][0]} repos / {g['usage'][1]} hits" if g["usage"] is not None else "not measured") + ")")
if rest:
    w(f"\nGap rows not assigned to any group above ({len(rest)}), by category: " + ", ".join(f"{k} {len(v)}" for k, v in sorted(rest_by_cat.items())) + ". "
      "They are in the CSV with their evidence; most are driver-class members whose status follows from the server features above.\n")

# notable engine defects found
w("\n## 3b. Defects found while auditing (all reproducible with the probe named)\n")
DEF = [
    ("createIndexes with several indexes creates only the first one while reporting success", "compat.opt.createIndexes.several_indexes_in_one_call"),
    ("replayed retryable writes execute twice", "compat.sessions.retryable_write_replay"),
    ("positional projection `arr.$` returns empty arrays", "compat.proj.positional"),
    ("documents come back in _id order instead of insertion order; $natural ignored", "compat.natural.sort"),
    ("a tailable cursor dies after the first batch", "compat.cursor.tailable_capped"),
    ("change stream `rename` event not emitted", "compat.cs.rename_and_drop_events"),
    ("fullDocument 'required' does not fail when no post-image exists", "compat.cs.fullDocument_required"),
    ("int64/double ordering near 2^53 and NaN under $lt differ", "compat.bson.compare_numeric"),
    ("`$where` has no `obj` variable", "compat.js.where_obj_alias"),
    ("operationTime/$clusterTime never returned to sessions", "compat.sessions.causal_consistency"),
]
for t, p in DEF:
    st = probes_d.get(p, {}).get("status", "?")
    w(f"- {t} — probe `{p}`: {'fails on the engine' if st == 'fail' else st}")
w("")

# exclusions
w("## 4. Proposed exclusions (not in the denominator)\n")
w(f"{len(excl)} rows. They stay in the CSV with `exclusion_proposed=yes`; their `engine_status` is still computed for information. Grouped by reason:\n")
grp = collections.defaultdict(list)
for r in excl:
    grp[re.sub(r"; documented but not listed.*|; no page in the 7\.0 manual.*", "", r["exclusion_reason"])].append(r)
for reason, rs in sorted(grp.items(), key=lambda kv: -len(kv[1])):
    names = ", ".join(r["name"] for r in rs[:12]) + (f", ... (+{len(rs)-12} more)" if len(rs) > 12 else "")
    w(f"- **{reason}** — {len(rs)} rows: {names}")
w("")
# quality gate
w("## 5. Quality gate\n")
vc = open(os.path.join(C, "verify_counts.md")).read().splitlines()[2:]
nok = sum("| OK" in l for l in vc); ndiff = sum("| DIFF" in l for l in vc)
w(f"- **Row counts vs the docs index pages:** `compat/verify_counts.md` — {len(vc)} independent recounts (different parsing code than the builder), {nok} agree ({sum('explained' in l for l in vc)} with an explained difference), {ndiff} unexplained differences.")
w(f"- **Every row's URL:** `compat/verify_report.txt` — every `docs_url` returned HTTP 200 and the page contains the row's name (or the HTML anchor, for driver rows). Rows whose command exists only in `listCommands` point at the command index and are marked in `exclusion_reason`.")
w("- **30 random rows against their docs page:** `compat/spot_check.md` (seeded, reproducible with `compat/tools/spot_check.py`); result and fixes are recorded there.")
w("- **Evidence integrity:** `compat/tools/verify_evidence.py` re-checks that every test/probe cited in the CSV passed (or failed, for cited failures) in the stored results.")
w("")
w("### What could not be verified\n")
for l in open(os.path.join(C, "UNVERIFIED.md")).read().strip().splitlines():
    w(l)
w("")
w("## 6. Reproducing\n")
w("`compat/README.md` lists the commands (build inventory → verify → run harnesses → probes → map → coverage → report).")
open(os.path.join(C, "COVERAGE.md"), "w").write("\n".join(out) + "\n")
print(f"wrote COVERAGE.md: {cv}/{tot} = {100*cv/tot:.1f}%; {len(groups_out)} gap groups; {len(rest)} unassigned gap rows")
