#!/usr/bin/env python3
"""Step 1: build compat/inventory_base.csv (the denominator) from fetched MongoDB 7.0 docs pages,
the real mongo:7.0 server's listCommands output, and the pymongo / Node.js driver API reference pages.

Every row's docs_url is a page this script fetched (see compat/fetch_log.csv); `verify` re-checks that the
page returned 200 and contains the row's name.  Engine status columns are filled by map_status.py.
"""
import csv, json, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch import fetch
from mdparse import entries

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "inventory_base.csv")
D = "https://www.mongodb.com/docs/v7.0/"
R = D + "reference/"
ROWS, SEEN = [], {}
NOTES = []  # things worth reporting (anomalies in the docs, skipped items)


def strip(url):
    return url.split("#")[0]


def add(cat, name, url, rid, reach="yes", excl="no", reason="", kind="", check=None):
    """kind: how verify() checks the page: 'md' (name must occur in the page text) or 'anchor:<id>' (HTML id)."""
    if rid in SEEN:
        raise SystemExit("duplicate id " + rid)
    SEEN[rid] = 1
    if excl == "no" and reach == "no":
        raise SystemExit("reach=no needs excl=yes: " + rid)
    ROWS.append(dict(id=rid, category=cat, name=name, docs_url=strip(url), driver_reachable=reach,
                     exclusion_proposed=excl, exclusion_reason=reason, _kind=kind or "md", _check=check if check is not None else name))


# --------------------------------------------------------------------------------------------- 1. commands
LC = json.load(open(os.path.join(HERE, "..", "listCommands-mongo-7.0.json")))
SRV = LC["commands"]

CRUD = "find insert update delete findAndModify getMore killCursors bulkWrite".split()
CMDCAT = {}
for n in CRUD: CMDCAT[n] = "crud"
for n in "aggregate count distinct mapReduce".split(): CMDCAT[n] = "aggregation"
for n in "planCacheClear planCacheClearFilters planCacheListFilters planCacheSetFilter".split(): CMDCAT[n] = "query plan cache"
for n in "startSession endSessions refreshSessions killSessions killAllSessions killAllSessionsByPattern".split(): CMDCAT[n] = "sessions"
for n in "commitTransaction abortTransaction".split(): CMDCAT[n] = "transactions"
for n in "createIndexes dropIndexes listIndexes reIndex setIndexCommitQuorum".split(): CMDCAT[n] = "index management"
for n in ("create drop dropDatabase renameCollection listCollections listDatabases collMod convertToCapped "
          "cloneCollectionAsCapped compact compactStructuredEncryptionData filemd5").split(): CMDCAT[n] = "collection and database management"
for n in "authenticate logout saslStart saslContinue".split(): CMDCAT[n] = "authentication"
for n in ("createUser dropAllUsersFromDatabase dropUser grantRolesToUser revokeRolesFromUser updateUser usersInfo createRole "
          "dropAllRolesFromDatabase dropRole grantPrivilegesToRole grantRolesToRole invalidateUserCache "
          "revokePrivilegesFromRole revokeRolesFromRole rolesInfo updateRole").split(): CMDCAT[n] = "user and role management"
for n in "hello isMaster ping buildInfo getParameter".split(): CMDCAT[n] = "handshake and driver utility"
for n in ("collStats connectionStatus connPoolStats dataSize dbHash dbStats explain getCmdLineOpts getLog hostInfo "
          "listCommands lockInfo profile serverStatus top validate validateDBMetadata whatsmyuri currentOp killOp "
          "getDefaultRWConcern getClusterParameter").split(): CMDCAT[n] = "diagnostics"
for n in ("fsync fsyncUnlock logRotate rotateCertificates setParameter setFeatureCompatibilityVersion setClusterParameter "
          "setDefaultRWConcern setUserWriteBlockMode dropConnections shutdown").split(): CMDCAT[n] = "server administration"
for n in "logApplicationMessage getAuditConfig setAuditConfig".split(): CMDCAT[n] = "auditing"
for n in "createSearchIndexes dropSearchIndex updateSearchIndex".split(): CMDCAT[n] = "atlas search"

# exclusion reasons by command category (driver_reachable=no for all of these)
EXCL = {
    "server administration": "privileged process/cluster administration (not issued by application code)",
    "replication": "replica-set administration/internal replication command, not issued by application code",
    "sharding": "sharding administration; a FENVIX database is not a sharded cluster",
    "internal": "internal server-to-server command, not callable by applications",
    "auditing": "auditing is an Enterprise/Atlas feature",
    "atlas search": "Atlas-only service (Atlas Search index management)",
}
SHARD_DOC = set("""addShard addShardToZone analyzeShardKey balancerCollectionStatus balancerStart balancerStatus balancerStop
checkMetadataConsistency cleanupOrphaned cleanupReshardCollection clearJumboFlag commitReshardCollection
configureCollectionBalancing configureQueryAnalyzer enableSharding flushRouterConfig getShardMap isdbgrid listShards
mergeAllChunksOnShard mergeChunks moveChunk movePrimary moveRange refineCollectionShardKey removeShard removeShardFromZone
reshardCollection setAllowMigrations shardCollection shardConnPoolStats shardingState split unsetSharding
updateZoneKeyRange""".split())
REPL_DOC = set("""appendOplogNote applyOps replSetAbortPrimaryCatchUp replSetFreeze replSetGetConfig replSetGetStatus
replSetInitiate replSetMaintenance replSetReconfig replSetResizeOplog replSetStepDown replSetSyncFrom""".split())

doc_cmds = {}  # name -> (docs heading, url)
for page in ("command", "mql/crud-commands"):
    for h2, h3, name, url, desc in entries(fetch(R + page + ".md")):
        doc_cmds.setdefault(name, (h3 or h2, strip(url)))
CMD_INDEX = R + "command.md"


def internal_name(n):
    h = SRV.get(n, {}).get("help", "")
    return (n.startswith("_") or re.match(r"(cluster[A-Z]|donor|recipient|forgetShardSplit|commitShardSplit|abortShardSplit|"
                                          r"replSet(Heartbeat|RequestVotes|UpdatePosition|GetRBID|StepUp)|coordinateCommit|prepareTransaction|"
                                          r"voteCommitIndexBuild|splitVector|splitChunk|autoSplitVector|checkShardingIndex|connPoolSync|"
                                          r"getShardVersion|getDatabaseVersion|internalRename|startRecordingTraffic|stopRecordingTraffic|"
                                          r"waitForFailPoint|recipientVote|getQueryableEncryptionCountInfo|listDatabasesForAllTenants)", n)
            is not None or "INTERNAL ONLY" in h or h.strip().lower().startswith("internal"))


for name in sorted(set(doc_cmds) | set(SRV), key=str.lower):
    docpage = doc_cmds.get(name)
    if docpage is None:  # not in the command index tables: look for its own page
        cand = R + "command/" + name.lower() + ".md"
        t = fetch(cand)
        docpage = ("", cand) if (t and name in t) else None
    cat = CMDCAT.get(name)
    reach, excl, reason = "yes", "no", ""
    if cat is None:
        if name in SHARD_DOC or re.match(r"_(configsvr|shardsvr|recvChunk|migrateClone|transferMods|flush|getNextSessionMods)", name):
            cat = "sharding" if name in SHARD_DOC else "internal"
        elif name in REPL_DOC or name.startswith("replSet"):
            cat = "replication"
        elif internal_name(name):
            cat = "internal"
        elif docpage is None:
            cat = "undocumented"
        else:
            cat = {"Administration Commands": "server administration", "Diagnostic Commands": "diagnostics",
                   "Sharding Commands": "sharding", "Replication Commands": "replication"}.get(docpage[0], "UNMAPPED:" + docpage[0])
    if cat in EXCL:
        reach, excl, reason = "no", "yes", EXCL[cat]
    elif cat == "undocumented":
        reach, excl, reason = "no", "yes", "listed by listCommands on mongo 7.0.45 but absent from the 7.0 manual (undocumented, no driver API uses it)"
    if docpage is None:
        url = CMD_INDEX
        reason = (reason + "; " if reason else "") + "no page in the 7.0 manual (docs_url is the command index, which does not list it)"
        if excl == "no":
            excl, reach = "yes", "no"
    else:
        url = docpage[1]
    if name not in SRV:
        reason = (reason + "; " if reason else "") + "documented but not listed by mongod 7.0.45 listCommands (mongos/Enterprise/Atlas only)"
        if excl == "no":
            reach, excl = "no", "yes"
    # kind=md: name must appear on the page; for index-only rows we cannot verify, so use 'index'
    add("commands / " + cat, name, url, "cmd:" + name, reach, excl, reason, kind="index" if docpage is None else "md")

# commands every driver sends although the 7.0 manual has no command page for them: kept in scope; docs_url is the manual page about the same mechanism
for _r in ROWS:
    if _r["id"] in ("cmd:isMaster", "cmd:saslStart", "cmd:saslContinue"):
        _r["driver_reachable"], _r["exclusion_proposed"], _r["exclusion_reason"] = "yes", "no", ""
        _r["docs_url"] = R + "mongodb-wire-protocol.md" if _r["id"] == "cmd:isMaster" else D + "core/security-scram.md"
        _r["_kind"] = "index"       # the page does not name the command; mechanism page only
        NOTES.append(f"{_r['id']}: no command page in the 7.0 manual; kept in scope (sent by every driver); docs_url is the page on the same mechanism")

# --------------------------------------------------------------------------------------------- 2. operators
def table_rows(page, cat_fmt, rid_prefix, skip=(), reach="yes", excl="no", reason="", by_h=True):
    t = fetch(page)
    for h2, h3, name, url, desc in entries(t):
        if name in skip or h2 == "Stages Available for Updates":     # that section lists the commands findAndModify/update, not stages
            continue
        cat = cat_fmt.format(h=h3 or h2)
        rid = rid_prefix + name
        if rid in SEEN:
            NOTES.append(f"{rid} listed twice in {page.split('/docs/')[1]} (also under another heading); counted once")
            continue
        add(cat, name, url, rid, reach, excl, reason)

QP = R + "mql/query-predicates/"
for sub, label in [("arrays", "array"), ("bitwise", "bitwise"), ("comparison", "comparison"), ("data-type", "element"),
                   ("misc", "evaluation"), ("logical", "logical"), ("geospatial", "geospatial")]:
    table_rows(QP + sub + ".md", "query operators / " + label, "qop:")
# operators the docs landing tables omit but whose pages exist in 7.0
for name, page, label in [("$text", "operator/query/text.md", "evaluation"), ("$comment", "command/find.md", "evaluation")]:
    u = R + page
    t = fetch(u)
    if t and name in t:
        add("query operators / " + label, name, u, "qop:" + name)
    else:
        NOTES.append(f"{name}: no 7.0 page at {u} (not added from docs; checked against server separately)")

table_rows(R + "mql/projection.md", "projection operators", "proj:")
add("projection operators", "aggregation expressions in find projection", R + "mql/projection.md", "proj:aggregation expressions in find projection", check="expressions")
table_rows(R + "mql/update.md", "update operators / {h}", "upd:")
table_rows(R + "mql/aggregation-stages.md", "aggregation stages", "stage:")
for name, page in [("$search", "search"), ("$searchMeta", "searchmeta"), ("$listSearchIndexes", "listsearchindexes")]:
    u = R + "operator/aggregation/" + page + ".md"
    if fetch(u) and ("stage:" + name) not in SEEN:
        add("aggregation stages", name, u, "stage:" + name, "yes", "yes", "Atlas-only service (Atlas Search / search index management)")
for r in ROWS:
    if r["id"] in ("stage:$search", "stage:$searchMeta", "stage:$listSearchIndexes", "stage:$vectorSearch") or False:
        r["category"] = "aggregation stages"
# stages that are sharding/cluster administration
for r in ROWS:
    if r["id"] in ("stage:$shardedDataDistribution", "stage:$listSampledQueries"):
        r["driver_reachable"], r["exclusion_proposed"] = "no", "yes"
        r["exclusion_reason"] = "sharding/query-analyzer administration; not a sharded cluster"
table_rows(R + "mql/expressions.md", "aggregation expressions / {h}", "expr:")
# the expressions page lists accumulator-style forms of $avg etc. under their own heading; they were counted once above
add("aggregation expressions / Object Operators", "$unsetField", R + "operator/aggregation/unsetfield.md", "expr:$unsetField")
NOTES.append("expr:$unsetField: page exists and the server accepts it, but the 7.0 expressions landing table omits it; added")
table_rows(R + "mql/accumulators.md", "accumulators", "acc:")
table_rows(R + "mql/window-functions.md", "window functions", "win:")

# --------------------------------------------------------------------------------------------- 3. command options
TYPE_WORDS = re.compile(r"^(?:non-negative |positive )?(?:string|document|array|boolean|bool|integer|int|long|number|any|object|timestamp|date|binary|decimal|double|"
                        r"objectid|numeric|[a-z]+ or [a-z ]+)(?:[ ,/]+(?:or )?[a-z-]+){0,4}$", re.I)


def fields_of(url):
    """Names in the `| Field | Type | Description |` (or `| Field | Description |`) tables of a command page, excluding output/example sections."""
    t = fetch(url) or ""
    out, active, head = [], False, ""
    for line in t.splitlines():
        if line.startswith("#"):
            active, head = False, line
            continue
        hm = re.match(r"^\|\s*(Fields?|Parameters?|Options?)\s*\|\s*(Type|Description)\s*\|", line)
        if hm:
            active = not re.search(r"output|response|return|example|result", head, re.I)
            typed = hm.group(2) == "Type"
            continue
        if active and line.startswith("|") and not line.startswith("| ---"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 2: continue
            m = re.match(r"^(?:\[)?`?([A-Za-z0-9_][A-Za-z0-9_.]*)`?(?:\]\([^)]*\))?$", cells[0])
            if not m: continue
            if typed and not TYPE_WORDS.match(re.sub(r"[`*]", "", cells[1])): continue
            if m.group(1) not in out: out.append(m.group(1))
    return out


SUB = {"update": ("updates[].", {"q", "u", "c", "upsert", "multi", "collation", "arrayFilters", "hint"}),
       "delete": ("deletes[].", {"q", "limit", "collation", "hint"}),
       "createindexes": ("indexes[].", None)}   # None: everything except the top-level set below
TOP_CREATEINDEXES = {"createIndexes", "indexes", "writeConcern", "commitQuorum", "comment"}
OPT_CMDS = ("find aggregate insert update delete findandmodify count distinct getmore killcursors create createindexes dropindexes listindexes "
            "listcollections listdatabases drop dropdatabase renamecollection collmod mapreduce explain hello dbstats collstats validate "
            "committransaction aborttransaction endsessions startsession").split()
CMDNAME = {n.lower(): n for n in SRV}
for page in OPT_CMDS:
    u = R + "command/" + page + ".md"
    t = fetch(u)
    cname = CMDNAME.get(page, page)
    if not t:
        NOTES.append(f"command options: no page {u}"); continue
    fl = fields_of(u)
    if not fl:
        NOTES.append(f"command options: {cname}: page has no parsable Field/Type table; options not inventoried (review)")
    for fname in fl:
        pre = ""
        if page in SUB:
            pfx, names = SUB[page]
            if (names is not None and fname in names) or (names is None and fname not in TOP_CREATEINDEXES):
                pre = pfx
        add("command options / " + cname, f"{cname}.{pre}{fname}", u, f"opt:{cname}.{pre}{fname}", check=fname)

FAM_DENY = {"findAndModify": {"value", "lastErrorObject", "ok", "updatedExisting", "upserted"}}
ROWS[:] = [r for r in ROWS if not (r["id"].startswith("opt:findAndModify.") and r["name"].split(".", 1)[1] in FAM_DENY["findAndModify"])]
for r in list(SEEN):
    pass
for _k in [k for k in SEEN if k.startswith("opt:findAndModify.") and k.split(".", 1)[1] in FAM_DENY["findAndModify"]]:
    del SEEN[_k]

# --------------------------------------------------------------------------------------------- 4. curated feature rows
def curated(cat, rows, rid_prefix, page_for_all=None, reach="yes", excl="no", reason=""):
    for r in rows:
        name, page, chk = ((r, page_for_all, None) if page_for_all else (tuple(r) + (None,))[:3])
        add(cat, name, page if page.startswith("http") else D + page, f"{rid_prefix}:{name}", reach, excl, reason, check=chk)

def copts(cmd, page, fields, check=None):
    for f in fields:
        add("command options / " + cmd, f"{cmd}.{f}", R + "command/" + page + ".md", f"opt:{cmd}.{f}", check=f.split(".")[-1])


copts("collMod", "collmod", "validator validationLevel validationAction index index.keyPattern index.name index.hidden index.expireAfterSeconds index.unique "
                           "index.prepareUnique viewOn pipeline cappedSize cappedMax changeStreamPreAndPostImages expireAfterSeconds".split())
copts("commitTransaction", "committransaction", "txnNumber writeConcern autocommit comment".split())
copts("abortTransaction", "aborttransaction", "txnNumber writeConcern autocommit comment".split())
copts("hello", "hello", "saslSupportedMechs comment compression client".split())

IDX = D + "core/indexes/index-types/"
curated("index types", [("single field index", IDX + "index-single.md"), ("compound index", IDX + "index-compound.md"),
                        ("multikey index", IDX + "index-multikey.md"), ("wildcard index", IDX + "index-wildcard.md"),
                        ("2dsphere", IDX + "index-geospatial.md"), ("2d", IDX + "index-geospatial.md"),
                        ("hashed index", IDX + "index-hashed.md"), ("text index", IDX + "index-text.md"),
                        ("clustered index", D + "core/clustered-collections.md")], "idxtype")
curated("index types", [("compound wildcard index", IDX + "index-wildcard.md")], "idxtype")
curated("index properties", [("case-insensitive index", D + "core/index-case-insensitive.md"), ("hidden index", D + "core/index-hidden.md"),
                             ("partial index", D + "core/index-partial.md"), ("sparse index", D + "core/index-sparse.md"),
                             ("TTL index", D + "core/index-ttl.md"), ("unique index", D + "core/index-unique.md")], "idxprop")
# index specification options (createIndexes `indexes[]`) are inventoried under "command options / createIndexes"

curated("collation", [(f, R + "collation.md") for f in "locale strength caseLevel caseFirst numericOrdering alternate maxVariable backwards normalization".split()], "coll")
curated("collation", [("simple (binary) collation", R + "collation.md", "simple")], "coll")
# supported locales: every locale in the locales table
loc = fetch(R + "collation-locales-defaults.md") or ""
tbl = next(l for l in loc.splitlines() if l.startswith("| LanguageLocaleVariants"))
cur = None
for m in re.finditer(r"`([^`]+)`|([^`|]+)", tbl[len("| LanguageLocaleVariants"):]):
    code, text = m.group(1), m.group(2)
    if text and text.strip():
        cur = {"lang": text.strip(), "loc": None}
    elif code and cur is not None:
        if cur["loc"] is None:
            cur["loc"] = code
            lid = code
        else:
            lid = f"{cur['loc']}@collation={code}"
        if ("locale:" + lid) not in SEEN:
            add("collation locales", lid, R + "collation-locales-defaults.md", "locale:" + lid, check="`" + code + "`")
curated("read concern", [(f'level "{l}"', R + "read-concern.md", f'"{l}"') for l in "local available majority linearizable snapshot".split()], "rc")
curated("read concern", [("afterClusterTime", R + "read-concern.md")], "rc")
curated("write concern", [("w: <number>", R + "write-concern.md", "<number>"), ('w: "majority"', R + "write-concern.md", '"majority"'),
                          ("w: <tag set name>", R + "write-concern.md", "tag"),
                          ("j", R + "write-concern.md", "j: true"), ("wtimeout", R + "write-concern.md"), ("w: 0 (unacknowledged)", R + "write-concern.md", "w: 0")], "wc")
curated("read preference", [(m, D + "core/read-preference.md") for m in "primary primaryPreferred secondary secondaryPreferred nearest".split()], "rp")
curated("read preference", [("maxStalenessSeconds", D + "core/read-preference-staleness.md"), ("tag sets", D + "core/read-preference-tags.md", "tag set"),
                            ("hedge", D + "core/read-preference-hedge-option.md")], "rp")

bt = fetch(R + "bson-types.md")
for m in re.finditer(r"^\|\s*([^|]+?)\s*\|\s*(-?\d+)\s*\|\s*\"(\w+)\"\s*\|", bt, re.M):
    add("bson types", f"{m.group(1)} (type {m.group(2)}, \"{m.group(3)}\")", R + "bson-types.md", f"bson:{m.group(3)}", check=f'"{m.group(3)}"')
for m in re.finditer(r"^\|\s*(\d+)\s*\|\s*([A-Za-z][^|]*?)\s*\|\s*$", bt, re.M):
    add("bson binary subtypes", f"binData subtype {m.group(1)} ({re.sub(r'[*]+','',m.group(2)).split('New in')[0].strip()})", R + "bson-types.md", f"binsub:{m.group(1)}", check=re.sub(r"[*]+","",m.group(2)).split("New in")[0].strip())
co = fetch(R + "bson-type-comparison-order.md")
for m in re.finditer(r"^\s*(\d+)\. (.+?)\s*$", co.split("## Numeric Types")[0], re.M):
    add("bson comparison order", f"order {m.group(1)}: {m.group(2)}", R + "bson-type-comparison-order.md", f"bsonord:{m.group(1)}", check=re.split(r"[(,]", m.group(2))[0].strip())
for n in ["Numeric Types", "Strings", "Arrays", "Objects", "Dates and Timestamps", "Non-existent Fields", "BinData"]:
    add("bson comparison order", f"comparison rules: {n}", R + "bson-type-comparison-order.md", f"bsoncmp:{n}", check=n)

curated("bulk write", [(m, D + "reference/method/db.collection.bulkwrite.md") for m in
                       "insertOne updateOne updateMany replaceOne deleteOne deleteMany ordered".split()], "bulk")
G = D + "core/gridfs.md"
curated("gridfs", [("fs.files collection", G, "fs.files"), ("fs.chunks collection", G, "fs.chunks"),
                   ("files index (filename, uploadDate)", G, "uploadDate"), ("chunks unique index (files_id, n)", G, "unique")], "gridfs")
curated("gridfs", [(f"chunks.{f}", G, f) for f in "_id files_id n data".split()], "gridfs")
curated("gridfs", [(f"files.{f}", G, f) for f in "_id length chunkSize uploadDate md5 filename contentType aliases metadata".split()], "gridfs")

cs = D + "changeStreams.md"
CSS = R + "operator/aggregation/changestream.md"
curated("change streams", [("$changeStream." + o, CSS, o) for o in "allChangesForCluster fullDocument fullDocumentBeforeChange resumeAfter showExpandedEvents startAfter startAtOperationTime".split()], "cso")
curated("change streams", [(o, cs) for o in ["updateLookup", "whenAvailable", "required", "resume token"]], "cs")
curated("change streams", [("watch a collection", cs, "collection"), ("watch a database", cs, "database"), ("watch a deployment", cs, "deployment")], "cs")
ev = R + "change-events.md"
for _h2, _h3, name, url, _d in entries(fetch(ev)):
    add("change stream events", name, url, "cse:" + name,
        *(("yes", "yes", "sharding event (only emitted on sharded clusters)") if name in ("refineCollectionShardKey", "reshardCollection", "shardCollection") else ("yes", "no", "")))
curated("change streams", [("pre- and post-images (changeStreamPreAndPostImages)", D + "changeStreams.md", "changeStreamPreAndPostImages")], "cs")

# server-side javascript: fields of the JS features (rows for $where/$function/$accumulator/mapReduce themselves are above)
SJ = R + "operator/aggregation/"
curated("server-side javascript", [(f"$function.{f}", SJ + "function.md", f) for f in "body args lang".split()], "js")
curated("server-side javascript", [(f"$accumulator.{f}", SJ + "accumulator.md", f) for f in
                                   "init initArgs accumulate accumulateArgs merge finalize lang".split()], "js")
curated("server-side javascript", [("security.javascriptEnabled", D + "core/server-side-javascript.md", "javascriptEnabled")], "js")

# driver protocol features every driver relies on
curated("driver protocol", [("OP_MSG", R + "mongodb-wire-protocol.md"), ("OP_COMPRESSED", R + "mongodb-wire-protocol.md"),
                            ("OP_QUERY", R + "mongodb-wire-protocol.md"), ("OP_REPLY", R + "mongodb-wire-protocol.md"),
                            ("OP_INSERT", R + "mongodb-wire-protocol.md")], "wire")
curated("driver protocol", [("TLS/SSL transport encryption (tls=true; the default for mongodb+srv:// URIs)", D + "core/security-transport-encryption.md", "TLS")], "tls")
curated("driver protocol", [("apiVersion", R + "stable-api.md"), ("apiStrict", R + "stable-api.md"), ("apiDeprecationErrors", R + "stable-api.md")], "stableapi")
curated("driver protocol", [("lsid (logical session id)", R + "server-sessions.md", "lsid"), ("txnNumber (retryable writes)", R + "server-sessions.md", "txnNumber"), ("retryWrites", D + "core/retryable-writes.md"),
                            ("causal consistency", D + "core/read-isolation-consistency-recency.md", "causal"),
                            ("$clusterTime", D + "core/read-isolation-consistency-recency.md", "clusterTime")], "sess")

curated("cursors", [("tailable cursor", D + "core/tailable-cursors.md"), ("awaitData", R + "command/find.md"),
                     ("noCursorTimeout", R + "command/find.md"), ("batchSize", R + "command/find.md"), ("allowPartialResults", R + "command/find.md"),
                     ("firstBatch", R + "command/find.md"), ("nextBatch", R + "command/getmore.md"), ("noCursorTimeout (method)", D + "reference/method/cursor.noCursorTimeout.md", "noCursorTimeout")], "cursor")
curated("transactions", [("startTransaction", D + "core/transactions.md"), ("commitTransaction", D + "core/transactions.md"),
                         ("abortTransaction", D + "core/transactions.md"), ("autocommit", R + "command/committransaction.md"), ("TransientTransactionError", D + "core/transactions-in-applications.md"),
                         ("UnknownTransactionCommitResult", D + "core/transactions-in-applications.md"), ("maxTimeMS (commitTransaction)", R + "command/committransaction.md", "txnNumber"),
                         ("transactionLifetimeLimitSeconds", D + "core/transactions-production-consideration.md")], "txn")
curated("authentication mechanisms", [("SCRAM-SHA-1", D + "core/security-scram.md"), ("SCRAM-SHA-256", D + "core/security-scram.md"),
                                      ("x.509 (MONGODB-X509)", D + "core/authentication.md", "x.509"), ("AWS IAM (MONGODB-AWS)", D + "core/authentication.md", "AWS"),
                                      ("OIDC (MONGODB-OIDC)", D + "core/authentication.md", "OIDC"), ("LDAP (PLAIN)", D + "core/authentication.md", "LDAP"),
                                      ("Kerberos (GSSAPI)", D + "core/authentication.md", "Kerberos")], "authmech")

curated("sort and hint specifications", [("$natural (sort)", D + "reference/method/cursor.sort.md", "$natural"), ("$natural (hint)", D + "reference/method/cursor.hint.md", "$natural"),
                                         ("$meta textScore (sort)", D + "reference/method/cursor.sort.md", "$meta")], "sorthint")

# --------------------------------------------------------------------------------------------- 5. driver APIs
PY = "https://pymongo.readthedocs.io/en/stable/api/"
PYPAGES = [("pymongo/mongo_client.html", "MongoClient"), ("pymongo/database.html", "Database"), ("pymongo/collection.html", "Collection"),
           ("pymongo/cursor.html", "Cursor"), ("pymongo/command_cursor.html", "CommandCursor"), ("pymongo/change_stream.html", "ChangeStream"),
           ("pymongo/client_session.html", "ClientSession"), ("pymongo/operations.html", "operations"),
           ("gridfs/index.html", "GridFS"), ("gridfs/grid_file.html", "grid_file"), ("pymongo/encryption.html", "encryption")]
for page, label in PYPAGES:
    t = fetch(PY + page)
    for i in re.findall(r'<dt class="sig sig-object py" id="([^"]+)"', t):
        if label in ("encryption",):
            add("driver api pymongo / encryption", i, PY + page + "#" + i, "py:" + i, "yes", "no", "", kind="anchor:" + i)
        else:
            add(f"driver api pymongo / {label}", i, PY + page + "#" + i, "py:" + i, "yes", "no", "", kind="anchor:" + i)

NJ = "https://mongodb.github.io/node-mongodb-native/7.7/classes/"
NJC = ("MongoClient Db Collection AbstractCursor FindCursor AggregationCursor ListIndexesCursor ListCollectionsCursor ChangeStream "
       "ClientSession Admin GridFSBucket GridFSBucketReadStream GridFSBucketWriteStream BulkOperationBase OrderedBulkOperation "
       "UnorderedBulkOperation ClientEncryption").split()
DTS = open(os.path.join(HERE, "..", "..", "mongo-tests", "node", "node_modules", "mongodb", "mongodb.d.ts")).read()


def own_members(cls):
    """Members declared by the class itself in the driver's .d.ts (inherited EventEmitter/stream members are Node.js, not driver API)."""
    m = re.search(r"^export declare (?:abstract )?class " + cls + r"\b[^\n]*\{\n(.*?)^\}", DTS, re.S | re.M)
    names = set()
    for line in (m.group(1) if m else "").splitlines():
        mm = re.match(r"^    (?:static |readonly |protected |abstract |get |set )*([A-Za-z_$][A-Za-z0-9_$]*)\??[(:<]", line)
        if mm and not line.lstrip().startswith(("/", "*")):
            names.add(mm.group(1))
        elif re.match(r"^    (static )?(readonly )?([A-Z_]+) =", line):
            names.add(re.match(r"^    (?:static )?(?:readonly )?([A-Z_]+) =", line).group(1))
    return names


for c in NJC:
    t = fetch(NJ + c + ".html")
    if not t:
        NOTES.append(f"node class page missing: {c}"); continue
    own = own_members(c)
    if not own:
        NOTES.append(f"node class {c}: no members found in mongodb.d.ts")
    for i in re.findall(r'<section class="tsd-panel tsd-member[^"]*">\s*<a id="([^"]+)"', t):
        base = re.sub(r"-\d+$", "", i)
        if base not in own:
            continue          # inherited from Node.js (EventEmitter, Readable, Writable) or another driver class
        add(f"driver api node / {c}", f"{c}.{i}", NJ + c + ".html#" + i, f"node:{c}.{i}", kind="anchor:" + i)


for r in ROWS:
    if r["id"] == "js:security.javascriptEnabled":
        r["driver_reachable"], r["exclusion_proposed"] = "no", "yes"
        r["exclusion_reason"] = "mongod startup configuration option (security.javascriptEnabled), not reachable through a driver"
    if r["id"] == "wire:OP_INSERT":
        r["driver_reachable"], r["exclusion_proposed"] = "no", "yes"
        r["exclusion_reason"] = "legacy opcode removed in MongoDB 5.1 (the 7.0 manual marks it removed); no driver sends it"


AUTH_EXCL = {"authmech:x.509 (MONGODB-X509)": None,
             "authmech:AWS IAM (MONGODB-AWS)": "Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community",
             "authmech:OIDC (MONGODB-OIDC)": "Enterprise/Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community",
             "authmech:LDAP (PLAIN)": "Enterprise/Atlas-only per the docs availability table (core/authentication.md): not available in MongoDB Community",
             "authmech:Kerberos (GSSAPI)": "Enterprise-only per the docs availability table (core/authentication.md): not available in MongoDB Community"}
for r in ROWS:
    if AUTH_EXCL.get(r["id"]):
        r["exclusion_proposed"], r["exclusion_reason"] = "yes", AUTH_EXCL[r["id"]]
    if r["id"] in ("py:pymongo.mongo_client.MongoClient.bulk_write", "node:MongoClient.bulkWrite"):
        r["exclusion_proposed"], r["driver_reachable"] = "yes", "no"
        r["exclusion_reason"] = "client-level bulkWrite needs the MongoDB 8.0 `bulkWrite` command (absent from the 7.0 manual); real mongod 7.0.45 rejects it too (probe: identical error on both)"


def main():
    cols = "id category name docs_url driver_reachable exclusion_proposed exclusion_reason".split()
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, cols + ["_kind", "_check"])
        w.writeheader()
        for r in ROWS:
            w.writerow(r)
    open(os.path.join(HERE, "..", "build_notes.txt"), "w").write("\n".join(NOTES) + "\n")
    print(len(ROWS), "rows;", len(NOTES), "notes")


if __name__ == "__main__":
    main()
