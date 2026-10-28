"""Item 9: diagnostics and administration commands: serverStatus, currentOp / killOp, top, hostInfo, getCmdLineOpts, connPoolStats,
listCommands, getLog, lockInfo, features, read/write concern defaults, cluster parameters, profile, dbHash, validateDBMetadata,
startSession, killAllSessions, plan cache commands, reIndex, compact, filemd5, setIndexCommitQuorum, fsync."""
import re
import threading
import time
from gaps_py import case
from pymongo.errors import OperationFailure

PLAN = "PlanExecutor error during aggregation :: caused by :: "


def tr(f):
    try:
        r = f()
        if isinstance(r, dict):
            r.pop("$clusterTime", None)
            r.pop("operationTime", None)
        return r
    except OperationFailure as e:
        return {"err": [e.code, e.details.get("codeName"), re.sub(r"UUID\(\"[0-9a-f-]+\"\)", "UUID(<u>)", e.details.get("errmsg") or "")]}


def shape(v, skip=()):
    """The structure of a value: keys and types, numbers by kind."""
    if isinstance(v, dict):
        return {k: shape(x, skip) for k, x in v.items() if k not in skip and k not in ("$clusterTime", "operationTime")}
    if isinstance(v, list):
        return [shape(v[0], skip)] if v else []
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return type(v).__name__
    return type(v).__name__


def cmd(client, db, c, **kw):
    return tr(lambda: client[db].command(c, **kw))


# ---- serverStatus ---------------------------------------------------------------------------

def _ss_shape(client):
    r = client.admin.command("serverStatus")
    r.pop("$clusterTime", None)
    r.pop("operationTime", None)
    pos = r.get("repl", {}).get("primaryOnlyServices")
    if pos is not None:   # (mongod lists these in a per-process hash order)
        r["repl"]["primaryOnlyServices"] = dict(sorted(pos.items()))
    r.pop("bucketCatalog", None)   # (there once a time-series collection was used in the process)
    cm = r["metrics"].pop("commands")
    r["metrics"].pop("apiVersions", None)   # (the apps that sent an apiVersion to the server so far)
    for k in ("locks", "defaultRWConcern", "transactions", "wiredTiger", "tcmalloc"):   # (what these hold depends on what the server did before)
        r.pop(k, None)
    return {"shape": shape(r), "commands_entries": sorted({str(sorted(set(v) & {"failed", "total"})) for v in cm.values() if isinstance(v, dict)})}


@case("g09.diag.serverstatus_shape", sa=True)
def _ss_sa(client, db):
    return _ss_shape(client)


@case("g09.diag.serverstatus_shape_rs", rs=True)
def _ss_rs(client, db):
    return _ss_shape(client)


@case("g09.diag.serverstatus_counters", sa=True)
def _ss_counters(client, db):
    a = client.admin.command("serverStatus")
    db.t.insert_many([{"_id": i} for i in range(3)])
    db.t.insert_one({"_id": 10})
    list(db.t.find({}))
    db.t.update_one({"_id": 1}, {"$set": {"x": 1}})
    db.t.update_many({}, {"$set": {"y": 1}})
    db.t.delete_one({"_id": 10})
    list(db.t.aggregate([{"$match": {}}]))
    db.t.count_documents({})
    cur = db.t.find({}).batch_size(1)
    list(cur)
    b = client.admin.command("serverStatus")

    def d(path):
        x, y = a, b
        for p in path.split("."):
            x, y = x[p], y[p]
        return y - x

    out = {k: d("opcounters." + k) for k in ("insert", "query", "update", "delete", "getmore", "command")}
    out.update({k: d("metrics.document." + k) for k in ("inserted", "deleted", "updated", "returned")})
    for c in ("insert", "find", "update", "delete", "aggregate", "getMore"):
        out["cmd." + c] = (b["metrics"]["commands"].get(c, {}).get("total", 0) - a["metrics"]["commands"].get(c, {}).get("total", 0))
    out["connections"] = [b["connections"]["current"] >= 1, b["connections"]["available"] > 0, b["uptimeMillis"] >= a["uptimeMillis"], b["pid"] > 0, b["version"], b["process"]]
    return out


@case("g09.diag.serverstatus_failed_counter", sa=True)
def _ss_failed(client, db):
    a = client.admin.command("serverStatus")["metrics"]["commands"]
    tr(lambda: db.command({"collMod": "nope", "validator": {}}))
    tr(lambda: db.command({"find": "t", "filter": {"$bogus": 1}}))
    b = client.admin.command("serverStatus")["metrics"]["commands"]
    return {c: [b[c]["total"] - a.get(c, {}).get("total", 0), b[c]["failed"] - a.get(c, {}).get("failed", 0)] for c in ("collMod", "find")}


@case("g09.diag.serverstatus_sections", sa=True)
def _ss_sections(client, db):
    r = client.admin.command({"serverStatus": 1, "repl": 0, "metrics": 0, "locks": 0, "wiredTiger": 0, "mem": 0, "network": 0, "opcounters": 0})
    r2 = client.admin.command({"serverStatus": 1, "connections": 0})
    return {"keys": sorted(set(r.keys()) - {"$clusterTime", "operationTime", "bucketCatalog"}), "keys2": sorted(set(r2.keys()) - {"$clusterTime", "operationTime", "bucketCatalog"}), "on_db": sorted(set(client[db.name].command("serverStatus")) - {"bucketCatalog"})[:5]}


# ---- hostInfo and the other static commands -----------------------------------------------------

@case("g09.diag.static_shapes", sa=True)
def _static(client, db):
    out = {}
    for name in ("hostInfo", "connPoolStats", "features", "lockInfo"):
        out[name] = shape(client.admin.command(name))
    out["hostInfo_other_db"] = shape(db.command("hostInfo"))
    out["serverStatus_other_db"] = sorted(set(db.command("serverStatus")) - {"bucketCatalog"})[:6]
    out["getCmdLineOpts"] = sorted(client.admin.command("getCmdLineOpts").keys())
    out["admin_only"] = {n: cmd(client, db.name, n) for n in ("getCmdLineOpts", "currentOp", "top", "lockInfo", "getLog")}
    return out


@case("g09.diag.list_commands", sa=True)
def _listcmds(client, db):
    r = client.admin.command("listCommands")["commands"]
    ours = {"find", "insert", "update", "delete", "aggregate", "createIndexes", "serverStatus", "listCommands", "currentOp", "dbHash", "profile", "collMod", "hello", "ping", "killOp"}
    return {"entries": {k: r[k] for k in sorted(ours) if k in r}, "has_find": "find" in r}


@case("g09.diag.get_log", sa=True)
def _getlog(client, db):
    g = client.admin.command({"getLog": "global"})
    return {"keys": sorted(g.keys()), "log_is_list": isinstance(g["log"], list), "lines_are_json": all(l.startswith("{") for l in g["log"][:5]), "total_int": isinstance(g["totalLinesWritten"], int),
            "bad": cmd(client, "admin", {"getLog": "nope"}), "bad_type": cmd(client, "admin", {"getLog": 1}), "other_db": cmd(client, db.name, {"getLog": "global"}), "names": "global" in client.admin.command({"getLog": "*"})["names"]}


# ---- currentOp / killOp / top ----------------------------------------------------------------

@case("g09.diag.current_op", sa=True)
def _current_op(client, db):
    r = client.admin.command({"currentOp": 1, "command.currentOp": {"$exists": True}})
    op = r["inprog"][0]
    out = {"keys": sorted(op.keys()), "op": op["op"], "ns": op["ns"], "type": op["type"], "active": op["active"], "command_keys": sorted(op["command"].keys()), "desc": op["desc"].startswith("conn"), "client": bool(re.match(r"^127\.0\.0\.1:\d+$", op["client"]))}
    out["metadata"] = sorted(op.get("clientMetadata", {}).keys())
    out["filter_none"] = client.admin.command({"currentOp": 1, "ns": "nope.x"})["inprog"]
    out["filter_op"] = len(client.admin.command({"currentOp": 1, "op": "command", "command.currentOp": {"$exists": True}})["inprog"])
    out["other_db"] = cmd(client, db.name, "currentOp")
    out["all"] = bool(client.admin.command({"currentOp": 1, "$all": True})["inprog"])
    return out


@case("g09.diag.kill_op", sa=True)
def _kill_op(client, db):
    db.t.insert_many([{"_id": i, "v": i} for i in range(20)])
    res = {}

    def slow():
        try:
            list(db.t.find({"$where": "function(){ sleep(2000); return true; }"}))
            res["r"] = "finished"
        except OperationFailure as e:
            res["r"] = [e.code, e.details.get("codeName")]

    th = threading.Thread(target=slow)
    th.start()
    opid = None
    for _ in range(40):
        time.sleep(0.1)
        ops = client.admin.command({"currentOp": 1, "ns": f"{db.name}.t"})["inprog"]
        if ops:
            opid = ops[0]["opid"]
            break
    out = {"found": opid is not None}
    if opid is not None:
        out["found_op"] = [ops[0]["op"], ops[0]["command"].get("find"), ops[0]["secs_running"] >= 0, ops[0]["microsecs_running"] > 0]
        out["kill"] = client.admin.command({"killOp": 1, "op": opid})
    th.join()
    out["result"] = res.get("r")
    out["nonexistent"] = client.admin.command({"killOp": 1, "op": 99999999})
    out["errors"] = {"no_op": cmd(client, "admin", {"killOp": 1}), "str_op": cmd(client, "admin", {"killOp": 1, "op": "shard:1"}), "other_db": cmd(client, db.name, {"killOp": 1, "op": 1})}
    return out


@case("g09.diag.top", sa=True)
def _top(client, db):
    db.t.insert_many([{"_id": i} for i in range(5)])
    db.t.insert_one({"_id": 9})
    list(db.t.find({}))
    list(db.t.find({}))
    db.t.update_one({"_id": 1}, {"$set": {"a": 1}})
    db.t.delete_one({"_id": 9})
    cur = db.t.find({}).batch_size(2)
    list(cur)
    db.t.count_documents({})
    r = client.admin.command("top")["totals"]
    mine = r[f"{db.name}.t"]
    return {"note": r["note"], "keys": sorted(mine.keys()), "cell": shape(mine["total"]), "counts": {k: v["count"] for k, v in mine.items()}, "ns_sorted": list(r.keys()) == ["note"] + sorted(k for k in r if k != "note"),
            "other_db": cmd(client, db.name, "top")}


# ---- read / write concern defaults, cluster parameters -------------------------------------------

@case("g09.diag.rwc_standalone", sa=True)
def _rwc_sa(client, db):
    return {"get": cmd(client, "admin", "getDefaultRWConcern"), "set": cmd(client, "admin", {"setDefaultRWConcern": 1, "defaultWriteConcern": {"w": 1}}),
            "cp_get": cmd(client, "admin", {"getClusterParameter": "*"}), "cp_set": cmd(client, "admin", {"setClusterParameter": {"changeStreamOptions": {"preAndPostImages": {"expireAfterSeconds": 60}}}})}


@case("g09.diag.rwc_replset", rs=True)
def _rwc_rs(client, db):
    out = {}
    # (the write concern default, once set, cannot be unset: start from a known state)
    client.admin.command({"setDefaultRWConcern": 1, "defaultWriteConcern": {"w": 1}})
    client.admin.command({"setDefaultRWConcern": 1, "defaultReadConcern": {}})
    a = client.admin.command("getDefaultRWConcern")
    out["initial"] = shape(a)
    out["initial_vals"] = {k: a[k] for k in ("defaultReadConcern", "defaultWriteConcern", "defaultWriteConcernSource", "defaultReadConcernSource")}
    b = cmd(client, "admin", {"setDefaultRWConcern": 1, "defaultWriteConcern": {"w": 1}})
    out["set_w1"] = {k: v for k, v in b.items() if k not in ("updateOpTime", "updateWallClockTime", "localUpdateWallClockTime")} if "err" not in b else b
    out["set_w1_keys"] = sorted(b.keys()) if "err" not in b else None
    c = cmd(client, "admin", {"setDefaultRWConcern": 1, "defaultReadConcern": {"level": "majority"}})
    out["set_rc"] = {k: v for k, v in c.items() if k not in ("updateOpTime", "updateWallClockTime", "localUpdateWallClockTime")} if "err" not in c else c
    for name, s in {"none": {}, "w0": {"defaultWriteConcern": {"w": 0}}, "no_w": {"defaultWriteConcern": {"j": True}}, "level_bad": {"defaultReadConcern": {"level": "snapshot"}}, "no_level": {"defaultReadConcern": {"foo": 1}},
                    "wc_str": {"defaultWriteConcern": "x"}, "unset": {"defaultWriteConcern": {}, "defaultReadConcern": {}}, "unset_rc": {"defaultReadConcern": {}}, "in_memory": {"defaultReadConcern": {"level": "available"}}, "wc_j": {"defaultWriteConcern": {"w": "majority", "j": True, "wtimeout": 100}}}.items():
        r = cmd(client, "admin", {"setDefaultRWConcern": 1, **s})
        out[name] = r if "err" in r else {k: v for k, v in r.items() if k not in ("updateOpTime", "updateWallClockTime", "localUpdateWallClockTime")}
    out["get_in_memory"] = {k: v for k, v in client.admin.command({"getDefaultRWConcern": 1, "inMemory": True}).items() if k not in ("updateOpTime", "updateWallClockTime", "localUpdateWallClockTime", "$clusterTime", "operationTime")}
    client.admin.command({"setDefaultRWConcern": 1, "defaultReadConcern": {}})   # (leave the server as other cases expect it)
    return out


@case("g09.diag.cluster_parameter_replset", rs=True)
def _cp_rs(client, db):
    out = {}
    g = client.admin.command({"getClusterParameter": "*"})
    out["all"] = shape(g)
    out["one"] = shape(client.admin.command({"getClusterParameter": "changeStreamOptions"}))
    out["list"] = shape(client.admin.command({"getClusterParameter": ["changeStreamOptions"]}))
    out["unknown"] = cmd(client, "admin", {"getClusterParameter": "nope"})
    out["set"] = cmd(client, "admin", {"setClusterParameter": {"changeStreamOptions": {"preAndPostImages": {"expireAfterSeconds": 60}}}})
    p = client.admin.command({"getClusterParameter": "changeStreamOptions"})["clusterParameters"][0]
    out["after"] = {"id": p["_id"], "pp": p["preAndPostImages"], "time_set": p["clusterParameterTime"].time > 0}
    out["set_off"] = cmd(client, "admin", {"setClusterParameter": {"changeStreamOptions": {"preAndPostImages": {"expireAfterSeconds": "off"}}}})
    out["set_unknown"] = cmd(client, "admin", {"setClusterParameter": {"nope": {"a": 1}}})
    out["set_bad"] = cmd(client, "admin", {"setClusterParameter": {"changeStreamOptions": 5}})
    return out


# ---- profile --------------------------------------------------------------------------------

@case("g09.diag.profile", sa=True)
def _profile(client, db):
    out = {}
    db.command({"profile": 0, "slowms": 100, "sampleRate": 1.0, "filter": "unset"})   # (slowms and sampleRate are server-wide)
    db.t.insert_many([{"_id": i, "a": i} for i in range(5)])
    out["get"] = db.command({"profile": -1})
    out["l1"] = db.command({"profile": 1, "slowms": 50})
    out["get1"] = db.command({"profile": -1})
    list(db.t.find({"a": 1}))
    out["fast_not_logged"] = db["system.profile"].count_documents({}) if "system.profile" in db.list_collection_names() else 0
    out["l2"] = db.command({"profile": 2})
    list(db.t.find({"a": 1}))
    db.t.insert_one({"_id": 100})
    db.t.update_one({"_id": 1}, {"$set": {"b": 1}})
    db.t.delete_one({"_id": 100})
    list(db.t.aggregate([{"$match": {}}]))
    prof = list(db["system.profile"].find({"ns": f"{db.name}.t"}).sort("ts", 1))
    out["ops"] = [[p["op"], p["ns"], p.get("nreturned"), p.get("ninserted"), p.get("nMatched"), p.get("nModified"), p.get("ndeleted"), sorted(p["command"].keys())[:2], p.get("protocol"), isinstance(p["millis"], int)] for p in prof]
    out["keys"] = {p["op"] + ("_agg" if "aggregate" in p["command"] else ""): sorted(p.keys()) for p in prof}
    sp = next(db.list_collections(filter={"name": "system.profile"}))
    out["sysprof_opts"] = sp["options"]
    out["l3"] = db.command({"profile": 3})
    out["lneg"] = tr(lambda: db.command({"profile": -5}))
    out["lstr"] = tr(lambda: db.command({"profile": "x"}))
    out["sample"] = db.command({"profile": 0, "sampleRate": 0.5})
    out["sample_bad"] = tr(lambda: db.command({"profile": 0, "sampleRate": 2}))
    out["filter"] = db.command({"profile": 0, "filter": {"millis": {"$gt": 100}}})
    out["get2"] = db.command({"profile": -1})
    out["filter_unset"] = db.command({"profile": 0, "filter": "unset"})
    out["off"] = db.command({"profile": 0})
    db.command({"profile": 0, "slowms": 100, "sampleRate": 1.0, "filter": "unset"})
    n = db["system.profile"].count_documents({})
    list(db.t.find({}))
    out["off_no_more"] = db["system.profile"].count_documents({}) == n
    return out


@case("g09.diag.profile_slowms", sa=True)
def _profile_slow(client, db):
    db.t.insert_many([{"_id": i} for i in range(5)])
    db.command({"profile": 0, "slowms": 100, "sampleRate": 1.0, "filter": "unset"})
    db.command({"profile": 1, "slowms": 0})
    list(db.t.find({}))
    db.command({"profile": 1, "slowms": 100000})
    list(db.t.find({}))
    db.command({"profile": 0, "slowms": 100})
    return {"count": db["system.profile"].count_documents({"op": "query"})}


# ---- dbHash ---------------------------------------------------------------------------------

@case("g09.diag.dbhash", sa=True)
def _dbhash(client, db):
    db.a.insert_many([{"_id": i, "p": "x" * i} for i in range(6)])
    db.b.insert_many([{"_id": "k%d" % i, "n": [i, {"z": i}]} for i in range(4)])
    db.command({"create": "cap", "capped": True, "size": 4096})
    db.cap.insert_many([{"_id": 5 - i, "v": i} for i in range(5)])
    db.c_empty.insert_one({"_id": 1})
    db.c_empty.delete_one({"_id": 1})
    r = db.command("dbHash")
    out = {"collections": r["collections"], "md5": r["md5"], "capped": r["capped"], "uuids": sorted(r["uuids"].keys()), "keys": sorted(r.keys()), "host_is_str": isinstance(r["host"], str)}
    r2 = db.command({"dbHash": 1, "collections": ["a", "cap"]})
    out["subset"] = [r2["collections"], r2["md5"], r2["capped"]]
    db.a.insert_one({"_id": 99})
    out["changed"] = db.command("dbHash")["collections"]["a"] != r["collections"]["a"]
    bad = tr(lambda: db.command({"dbHash": 1, "collections": "a"}))
    out["bad"] = {"collections": bad.get("collections"), "capped": bad.get("capped"), "md5": bad.get("md5")}
    out["empty_db"] = client["empty_" + db.name].command("dbHash")["collections"]
    return out


# ---- validateDBMetadata -----------------------------------------------------------------------

@case("g09.diag.validate_db_metadata", sa=True)
def _vdm(client, db):
    db.t.insert_one({"_id": 1, "a": "x", "b": 1})
    db.t.create_index("a", name="text_ix", **{}) if False else None
    db.t.create_index([("a", "text")], name="text_ix")
    db.t.create_index("b", name="sparse_ix", sparse=True)
    db.t.create_index("b", name="partial_ix", partialFilterExpression={"b": {"$gt": 0}})
    db.t.create_index("a", name="ttl_ix", expireAfterSeconds=100)
    api = {"version": "1", "strict": True}
    out = {"strict": tr(lambda: client.admin.command({"validateDBMetadata": 1, "apiParameters": api, "db": db.name})), "non_strict": tr(lambda: client.admin.command({"validateDBMetadata": 1, "apiParameters": {"version": "1"}, "db": db.name})),
           "coll": tr(lambda: client.admin.command({"validateDBMetadata": 1, "apiParameters": api, "db": db.name, "collection": "t"})), "other_coll": tr(lambda: client.admin.command({"validateDBMetadata": 1, "apiParameters": api, "db": db.name, "collection": "zz"}))}
    out["no_params"] = tr(lambda: client.admin.command({"validateDBMetadata": 1}))
    out["no_version"] = tr(lambda: client.admin.command({"validateDBMetadata": 1, "apiParameters": {"strict": True}}))
    out["bad_strict"] = tr(lambda: client.admin.command({"validateDBMetadata": 1, "apiParameters": {"version": "1", "strict": "x"}}))
    return out


# ---- sessions, plan cache, reIndex, compact, filemd5, fsync --------------------------------------

@case("g09.diag.sessions", sa=True)
def _sessions(client, db):
    r = client.admin.command("startSession")
    sid = r["id"]
    out = {"shape": shape(r), "uuid_subtype": sid["id"].subtype, "timeout": r["timeoutMinutes"]}
    out["use"] = db.command({"insert": "t", "documents": [{"_id": 1}], "lsid": sid})["n"]
    out["kill_all"] = client.admin.command({"killAllSessions": []})
    out["kill_pattern"] = client.admin.command({"killAllSessionsByPattern": []})
    out["bad"] = cmd(client, "admin", {"killAllSessions": 1})
    out["bad_pattern"] = cmd(client, "admin", {"killAllSessionsByPattern": "x"})
    out["end"] = client.admin.command({"endSessions": [sid]})
    out["kill_sessions"] = client.admin.command({"killSessions": [{"id": sid["id"]}]})
    return out


@case("g09.diag.plan_cache", sa=True)
def _plan_cache(client, db):
    db.t.insert_many([{"_id": i, "a": i} for i in range(5)])
    db.t.create_index("a")
    out = {"list0": db.command({"planCacheListFilters": "t"})}
    out["set"] = tr(lambda: db.command({"planCacheSetFilter": "t", "query": {"a": 1}, "indexes": [{"a": 1}]}))
    out["list1"] = db.command({"planCacheListFilters": "t"})
    out["set_same"] = tr(lambda: db.command({"planCacheSetFilter": "t", "query": {"a": 1}, "indexes": [{"a": 1}]}))
    out["set_other"] = tr(lambda: db.command({"planCacheSetFilter": "t", "query": {"a": 2}, "sort": {"a": 1}, "indexes": [{"a": 1}]}))
    out["list2"] = db.command({"planCacheListFilters": "t"})["filters"].__len__()
    out["no_indexes"] = tr(lambda: db.command({"planCacheSetFilter": "t", "query": {"a": 1}}))
    out["no_query"] = tr(lambda: db.command({"planCacheSetFilter": "t", "indexes": [{"a": 1}]}))
    out["empty_indexes"] = tr(lambda: db.command({"planCacheSetFilter": "t", "query": {"a": 1}, "indexes": []}))
    out["clear_one"] = tr(lambda: db.command({"planCacheClearFilters": "t", "query": {"a": 1}}))
    out["list3"] = db.command({"planCacheListFilters": "t"})["filters"].__len__()
    out["clear_all"] = tr(lambda: db.command({"planCacheClearFilters": "t"}))
    out["list4"] = db.command({"planCacheListFilters": "t"})["filters"]
    out["clear_plan"] = tr(lambda: db.command({"planCacheClear": "t"}))
    out["missing_coll"] = [tr(lambda: db.command({"planCacheClear": "nope"})), tr(lambda: db.command({"planCacheListFilters": "nope"})), tr(lambda: db.command({"planCacheSetFilter": "nope", "query": {"a": 1}, "indexes": [{"a": 1}]}))]
    out["stats"] = tr(lambda: list(db.t.aggregate([{"$planCacheStats": {}}])))
    out["bad_name"] = tr(lambda: db.command({"planCacheClear": 5}))
    return out


@case("g09.diag.reindex_compact", sa=True)
def _reindex(client, db):
    db.t.insert_many([{"_id": i, "a": i} for i in range(5)])
    db.t.create_index("a", name="a_1")
    db.t.create_index([("a", -1), ("_id", 1)], name="a_id", unique=True)
    r = db.command({"reIndex": "t"})
    out = {"reindex": [r["nIndexesWas"], r["nIndexes"], [i["name"] for i in r["indexes"]], sorted(r.keys())]}
    out["missing"] = tr(lambda: db.command({"reIndex": "nope"}))
    out["compact"] = tr(lambda: db.command({"compact": "t"}))
    out["compact_force"] = tr(lambda: db.command({"compact": "t", "force": True}))
    out["compact_missing"] = tr(lambda: db.command({"compact": "nope"}))
    out["indexes_after"] = [i["name"] for i in db.t.list_indexes()]
    out["data_after"] = db.t.count_documents({})
    return out


@case("g09.diag.reindex_compact_replset", rs=True)
def _reindex_rs(client, db):
    db.t.insert_one({"_id": 1})
    return {"reindex": tr(lambda: db.command({"reIndex": "t"})), "reindex_missing": tr(lambda: db.command({"reIndex": "nope"})), "compact": tr(lambda: db.command({"compact": "t"})), "compact_force": tr(lambda: db.command({"compact": "t", "force": True})),
            "quorum": tr(lambda: db.command({"setIndexCommitQuorum": "t", "indexNames": ["x"], "commitQuorum": "majority"}))}


@case("g09.diag.filemd5", sa=True)
def _filemd5(client, db):
    import bson
    fid = bson.ObjectId("5f0000000000000000000001")
    db["fs.chunks"].insert_many([{"_id": bson.ObjectId("5f0000000000000000000010"), "files_id": fid, "n": 1, "data": bson.Binary(b"world")}, {"_id": bson.ObjectId("5f0000000000000000000011"), "files_id": fid, "n": 0, "data": bson.Binary(b"hello ")}])
    db["fs.chunks"].create_index([("files_id", 1), ("n", 1)], unique=True)
    return {"present": db.command({"filemd5": fid, "root": "fs"}), "missing": db.command({"filemd5": bson.ObjectId("5f00000000000000000000ff"), "root": "fs"}), "other_root": db.command({"filemd5": fid, "root": "zz"})}


@case("g09.diag.set_index_commit_quorum", sa=True)
def _siq(client, db):
    db.t.insert_one({"_id": 1})
    return {"r": tr(lambda: db.command({"setIndexCommitQuorum": "t", "indexNames": ["x"], "commitQuorum": "majority"})), "bad": tr(lambda: db.command({"setIndexCommitQuorum": 5, "indexNames": ["x"], "commitQuorum": "majority"}))}


@case("g09.diag.fsync", sa=True)
def _fsync(client, db):
    out = {"plain": client.admin.command("fsync")["numFiles"] >= 1, "lock": client.admin.command({"fsync": 1, "lock": True}), "unlock": client.admin.command("fsyncUnlock"), "unlock_again": cmd(client, "admin", "fsyncUnlock"),
           "other_db": cmd(client, db.name, "fsync"), "logrotate": client.admin.command("logRotate"), "logrotate_other_db": cmd(client, db.name, "logRotate")}
    return out


# ---- database-level aggregation sources -----------------------------------------------------------

@case("g09.diag.aggregate_database_stages", sa=True)
def _agg_db(client, db):
    def agg(dbname, pipe):
        return tr(lambda: client[dbname].command({"aggregate": 1, "pipeline": pipe, "cursor": {}})["cursor"]["firstBatch"])
    out = {}
    own = agg("admin", [{"$currentOp": {}}, {"$match": {"command.aggregate": {"$exists": True}}}, {"$project": {"op": 1, "ns": 1, "type": 1, "active": 1}}])
    out["current_op"] = own
    keys = agg("admin", [{"$currentOp": {}}, {"$match": {"command.aggregate": {"$exists": True}}}, {"$limit": 1}])
    out["current_op_keys"] = sorted(keys[0].keys()) if isinstance(keys, list) and keys else keys
    for name, pipe in {"not_admin": [{"$currentOp": {}}], "bad_opt": [{"$currentOp": {"foo": 1}}], "arg_type": [{"$currentOp": 5}], "twice": [{"$currentOp": {}}, {"$currentOp": {}}], "not_first": [{"$match": {}}, {"$currentOp": {}}],
                       "empty": [], "match": [{"$match": {}}], "group": [{"$group": {"_id": 1}}], "coll_stats": [{"$collStats": {}}], "facet": [{"$facet": {"a": []}}], "unknown": [{"$nosuchstage": {}}], "search": [{"$search": {}}],
                       "list_sessions": [{"$listSessions": {}}], "list_cached": [{"$listCachedAndActiveUsers": {}}], "local_bad_opt": [{"$listLocalSessions": {"foo": 1}}], "local_users": [{"$listLocalSessions": {"users": [{"user": "x", "db": "y"}]}}]}.items():
        out[name] = agg(db.name if name == "not_admin" else "admin", pipe)
    sid = client.admin.command("startSession")["id"]
    db.command({"insert": "t", "documents": [{"_id": 1}], "lsid": sid})
    sess = agg("admin", [{"$listLocalSessions": {"allUsers": True}}])
    out["local_sessions"] = [isinstance(sess, list) and any(s["_id"]["id"] == sid["id"] for s in sess), sorted(sess[0].keys()) if isinstance(sess, list) and sess else None, sorted(sess[0]["_id"].keys()) if isinstance(sess, list) and sess else None]
    client.admin.command({"endSessions": [sid]})
    sess2 = agg("admin", [{"$listLocalSessions": {"allUsers": True}}])
    out["after_end"] = isinstance(sess2, list) and not any(s["_id"]["id"] == sid["id"] for s in sess2)
    out["match_after"] = agg("admin", [{"$listLocalSessions": {}}, {"$match": {"nope": 1}}])
    return out
