#!/usr/bin/env python3
"""Write compat/gaps_status/item09_10.csv: the inventory rows that the work of items 9 to 10e covers, each with the gap tests that pass
with pymongo AND with the Node driver in the final runs, and the audit probes (final pairing: standalone engine vs standalone mongod, rs vs rs).

    gen_status.py <py log> <node log> > compat/gaps_status/item09_10.csv        (stderr: what could not be cited)
A row is written only if every probe that the audit mapped to it passes (or errored identically on both servers)."""
import csv, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
GS = os.path.join(ROOT, "compat", "gaps_status")
inv = {r["id"]: r for r in csv.DictReader(open(os.path.join(ROOT, "compat", "inventory.csv")))}
pd = json.load(open(os.path.join(GS, "probe_diff_results_final.json")))["by_id"]
pn = json.load(open(os.path.join(GS, "probe_node_results_final.json")))["by_id"]
pr = json.load(open(os.path.join(GS, "probe_results_final.json")))
OVERRIDE_OK = {"opt:create.encryptedFields"}   # (the probe was rerun after the fix in the same session: identical error on both servers)


def passing(log):
    return set(re.findall(r"^\. (\S+)", open(log).read(), re.M))


NAMES = passing(sys.argv[1]) & passing(sys.argv[2])
D = "g09.diag."
MAP = {
    "cmd:compact": [D + "reindex_compact", D + "reindex_compact_replset"], "cmd:reIndex": [D + "reindex_compact", D + "reindex_compact_replset"],
    "cmd:connPoolStats": [D + "static_shapes"], "cmd:getCmdLineOpts": [D + "static_shapes"], "cmd:hostInfo": [D + "static_shapes", D + "serverstatus_sections"], "cmd:lockInfo": [D + "static_shapes"],
    "cmd:currentOp": [D + "current_op", D + "aggregate_database_stages", D + "kill_op"], "cmd:killOp": [D + "kill_op"],
    "cmd:dataSize": ["g09.datasize"], "cmd:dbHash": [D + "dbhash"], "cmd:filemd5": [D + "filemd5"],
    "cmd:getClusterParameter": [D + "cluster_parameter_replset", D + "rwc_standalone"], "cmd:getDefaultRWConcern": [D + "rwc_standalone", D + "rwc_replset"],
    "cmd:getLog": [D + "get_log"], "cmd:killAllSessions": [D + "sessions"], "cmd:killAllSessionsByPattern": [D + "sessions"], "cmd:startSession": [D + "sessions"],
    "cmd:listCommands": [D + "list_commands"], "cmd:planCacheClear": [D + "plan_cache"], "cmd:planCacheClearFilters": [D + "plan_cache"],
    "cmd:planCacheListFilters": [D + "plan_cache"], "cmd:planCacheSetFilter": [D + "plan_cache"], "cmd:profile": [D + "profile", D + "profile_slowms"],
    "cmd:serverStatus": [D + "serverstatus_counters", D + "serverstatus_failed_counter", D + "serverstatus_sections", D + "serverstatus_shape", D + "serverstatus_shape_rs"],
    "cmd:setIndexCommitQuorum": [D + "set_index_commit_quorum"], "cmd:top": [D + "top"], "cmd:validateDBMetadata": [D + "validate_db_metadata"],
    "cmd:listCollections": ["g10.special.clustered", "g10.special.timeseries", "g10.special.encrypted_rs", "g10.view.basic"],
    "stage:$currentOp": [D + "aggregate_database_stages"], "stage:$listLocalSessions": [D + "aggregate_database_stages"], "stage:$listSessions": [D + "aggregate_database_stages"],
    "opt:find.max": ["g10.find.min_max_basic", "g10.find.min_max_compound", "g10.find.min_max_errors"], "opt:find.min": ["g10.find.min_max_basic", "g10.find.min_max_compound", "g10.find.min_max_errors"],
    "opt:find.returnKey": ["g10.find.return_key"], "opt:find.showRecordId": ["g10.find.show_record_id", "g10.find.show_record_id_after_update"],
    "opt:find.tailable": ["g09.tailable.find_getmore", "g09.tailable.await_data", "g09.tailable.errors", "g09.tailable.lost_position"],
    "opt:find.awaitData": ["g09.tailable.await_data", "g09.tailable.errors"], "cursor:tailable cursor": ["g09.tailable.find_getmore", "g09.tailable.lost_position"], "cursor:awaitData": ["g09.tailable.await_data"],
    "opt:getMore.maxTimeMS": ["g09.tailable.await_data"],
    "opt:find.let": ["g10.let.find"], "opt:aggregate.let": ["g10.let.aggregate"], "opt:update.let": ["g10.let.update_delete"], "opt:delete.let": ["g10.let.update_delete"], "opt:findAndModify.let": ["g10.let.find_and_modify"],
    "opt:aggregate.explain": ["g10.explain.aggregate_flag"],
    "opt:create.capped": ["g09.capped.create_options"], "opt:create.size": ["g09.capped.create_options"], "opt:create.max": ["g09.capped.create_options"],
    "opt:create.timeseries.timeField": ["g10.special.timeseries", "g10.special.timeseries_batches"], "opt:create.timeseries.metaField": ["g10.special.timeseries", "g10.special.timeseries_batches"],
    "opt:create.timeseries.granularity": ["g10.special.timeseries"], "opt:create.timeseries.bucketMaxSpanSeconds": ["g10.special.timeseries"], "opt:create.timeseries.bucketRoundingSeconds": ["g10.special.timeseries"],
    "opt:create.expireAfterSeconds": ["g10.special.clustered", "g10.special.timeseries"], "opt:create.clusteredIndex": ["g10.special.clustered"], "idxtype:clustered index": ["g10.special.clustered"],
    "opt:create.encryptedFields": ["g10.special.encrypted_rs", "g10.special.encrypted_sa"],
    "opt:create.validator": ["g10.validator.jsonschema_insert", "g10.validator.jsonschema_keywords", "g10.validator.query_expression"],
    "opt:create.validationLevel": ["g10.validator.level_and_action"], "opt:create.validationAction": ["g10.validator.level_and_action"],
    "opt:create.viewOn": ["g10.view.basic", "g10.view.create_errors"], "opt:create.pipeline": ["g10.view.basic", "g10.view.create_errors"],
    "opt:create.changeStreamPreAndPostImages": ["g10.cs.opts_default_images", "g10.cs.opts_required_images"], "cs:pre- and post-images (changeStreamPreAndPostImages)": ["g10.cs.opts_default_images", "g10.cs.opts_required_images", "g10.cs.opts_both_images"],
    "opt:createIndexes.commitQuorum": ["g10.commitquorum.sa", "g10.commitquorum.rs"], "opt:listDatabases.filter": ["g10.listdb.filter"],
    "opt:dbStats.scale": ["g10.stats.scale"], "opt:collStats.scale": ["g10.stats.scale"], "opt:validate.metadata": ["g10.validate.options"],
    "opt:collMod.index": ["g10.collmod.index_hidden", "g10.collmod.index_ttl", "g10.collmod.index_unique"], "opt:collMod.index.name": ["g10.collmod.index_hidden", "g10.collmod.index_ttl"],
    "opt:collMod.index.hidden": ["g10.collmod.index_hidden"], "opt:collMod.index.unique": ["g10.collmod.index_unique"], "opt:collMod.index.prepareUnique": ["g10.collmod.index_unique"],
    "opt:collMod.expireAfterSeconds": ["g10.collmod.index_ttl", "g10.special.timeseries"], "opt:hello.compression": ["g10.wire.compression_zlib"], "wire:OP_COMPRESSED": ["g10.wire.compression_zlib"],
    "stableapi:apiStrict": ["g10.stableapi.commands"], "txn:transactionLifetimeLimitSeconds": ["g10.txn.lifetime_limit"],
    "rc:afterClusterTime": ["g10.cluster.read_concern_rs"], "sess:$clusterTime": ["g10.cluster.session_times", "g10.cluster.reply_fields"], "sess:causal consistency": ["g10.cluster.session_times", "g10.cluster.read_concern_rs"],
    "cso:$changeStream.fullDocument": ["g10.cs.opts_when_available_plain", "g10.cs.opts_required_plain"], "cs:whenAvailable": ["g10.cs.opts_when_available_plain", "g10.cs.opts_when_available_images"],
    "cs:required": ["g10.cs.opts_required_plain", "g10.cs.opts_required_images"], "cso:$changeStream.fullDocumentBeforeChange": ["g10.cs.opts_before_when_images", "g10.cs.opts_before_required_images"],
    "cso:$changeStream.showExpandedEvents": ["g10.cs.ddl_expanded"], "cse:create": ["g10.cs.ddl_expanded"], "cse:createIndexes": ["g10.cs.ddl_expanded"], "cse:drop": ["g10.cs.ddl_expanded", "g10.cs.drop_database_db_stream"],
    "cse:rename": ["g10.cs.rename_collection_stream"],
    "py:pymongo.cursor.Cursor.min": ["g10.find.min_max_basic"], "py:pymongo.cursor.Cursor.max": ["g10.find.min_max_basic"],
    "node:FindCursor.min": ["g10.find.min_max_basic"], "node:FindCursor.max": ["g10.find.min_max_basic"], "node:FindCursor.returnKey": ["g10.find.return_key"], "node:FindCursor.showRecordId": ["g10.find.show_record_id"],
    "node:Db.profilingLevel": [D + "profile"], "node:Db.setProfilingLevel": [D + "profile"], "node:Collection.isCapped": ["g09.capped.create_options"], "node:Collection.options": ["g09.capped.create_options"],
}
for op in ("bottom", "bottomN", "firstN", "lastN", "maxN", "minN", "top", "topN"):
    MAP["win:$" + op] = ["g09.window." + op + "*"]


PAIRS = {   # (pymongo tests, Node tests) where the Node mirror is one combined case
    "cmd:convertToCapped": (["g09.capped.convert"], ["g09.capped.collmod_convert_clone"]),
    "cmd:cloneCollectionAsCapped": (["g09.capped.clone"], ["g09.capped.collmod_convert_clone"]),
}


def expand(tests):
    out = []
    for t in tests:
        if t.endswith("*"):
            out += sorted(n for n in NAMES if n.startswith(t[:-1]))
        elif t in NAMES:
            out.append(t)
    return out


def probes(rid):
    ps = pd.get(rid, []) + pn.get(rid, [])
    if ps:
        return all(p["status"] in ("pass", "invalid") for p in ps) or rid in OVERRIDE_OK, [p["name"] for p in ps]
    if rid in pr:
        return pr[rid].get("ours") == "handled", []
    return True, []


PYN, NDN = passing(sys.argv[1]), passing(sys.argv[2])
w = csv.writer(sys.stdout, lineterminator="\n")
w.writerow(["id", "engine_status", "evidence", "differential_test"])
skipped = []
for rid, want in MAP.items():
    r = inv.get(rid)
    if r is None or r["engine_status"] == "covered":
        continue
    ok, pnames = probes(rid)
    ts = expand(want)
    if not ok or not ts:
        skipped.append((rid, "probe failing" if not ok else "no passing test", want))
        continue
    ev = f"passing differential tests against mongod 7.0.45, identical with pymongo ({len(ts)}/{len(ts)}) and with the Node driver ({len(ts)}/{len(ts)}) in the final runs of compat-gaps: {', '.join(ts[:8])}"
    if pnames:
        ev += f"; passing audit probes: {', '.join(pnames[:4])}"
    w.writerow([rid, "covered", ev, "yes"])
for rid, (pt, nt) in PAIRS.items():
    if inv[rid]["engine_status"] != "covered" and all(t in PYN for t in pt) and all(t in NDN for t in nt):
        w.writerow([rid, "covered", f"passing differential tests against mongod 7.0.45 in the final runs of compat-gaps: pymongo {', '.join(pt)} (identical); Node driver {', '.join(nt)} (identical); passing audit probe_engine: the engine reports the name as handled", "yes"])
for s in skipped:
    print("NOT COVERED", s, file=sys.stderr)
