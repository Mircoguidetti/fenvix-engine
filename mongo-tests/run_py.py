#!/usr/bin/env python3
"""Differential test runner (pymongo). Runs every script in tests.json against real MongoDB and
mongo-server, with a fresh database per test, and compares the step results.

Only two normalisations are applied: ObjectId values are replaced by a placeholder unless the
test supplied them (keep_oids), and top-level result lists are sorted when the test says the
order is unspecified (unordered). Everything else, including int32/int64/double types and field
order, is compared exactly via canonical Extended JSON.
"""
import argparse
import copy
import socket
import struct
import json
import os
import subprocess
import sys
import threading
import time
import uuid

from bson import Int64, ObjectId, json_util
from bson.json_util import CANONICAL_JSON_OPTIONS
from pymongo import MongoClient, errors
from pymongo.server_api import ServerApi

HERE = os.path.dirname(os.path.abspath(__file__))
SKIP = object()  # step result that is not part of the comparison (ours-only helpers)


def generated_docs(kind, n):
    assert kind == "basic"
    return [{"_id": i, "n": i, "grp": i % 10, "name": f"user{i}", "tag": ["x", "y", "z"][i % 3], "nested": {"k": i % 7}} for i in range(n)]


def pairs(d):
    return list(d.items())


def swap_db(v, dbname):
    if isinstance(v, str):
        return v.replace("$DB", dbname)
    if isinstance(v, dict):
        return {k: swap_db(x, dbname) for k, x in v.items()}
    if isinstance(v, list):
        return [swap_db(x, dbname) for x in v]
    return v


class Ctx:
    def __init__(self, uri, port, idle_secs, is_ours, server_script):
        self.uri, self.port, self.idle_secs, self.is_ours = uri, port, idle_secs, is_ours
        self.server_script = server_script
        self.client = MongoClient(uri, serverSelectionTimeoutMS=15000, socketTimeoutMS=60000)
        self.asserts = []

    def reconnect(self):
        self.client.close()
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=30000, socketTimeoutMS=60000)


def err_result(e, cmp_msg):
    if isinstance(e, errors.BulkWriteError):
        d = e.details
        we = []
        for w in d.get("writeErrors", []):
            x = {"index": w["index"], "code": w["code"]}
            if cmp_msg:
                x["errmsg"] = w.get("errmsg")
                if "keyPattern" in w:
                    x["keyPattern"], x["keyValue"] = w.get("keyPattern"), w.get("keyValue")
            we.append(x)
        return {"__bulk_error__": {k: d.get(k) for k in ("nInserted", "nMatched", "nModified", "nRemoved", "nUpserted")} | {"writeErrors": we, "upserted": d.get("upserted")}}
    if isinstance(e, errors.WriteError):  # includes DuplicateKeyError
        r = {"__error__": {"code": e.code}}
        if cmp_msg:
            r["__error__"]["msg"] = (e.details or {}).get("errmsg")
            r["__error__"]["keyPattern"] = (e.details or {}).get("keyPattern")
            r["__error__"]["keyValue"] = (e.details or {}).get("keyValue")
        return r
    if isinstance(e, errors.OperationFailure):
        r = {"__error__": {"code": e.code, "codeName": (e.details or {}).get("codeName")}}
        if cmp_msg:
            r["__error__"]["msg"] = (e.details or {}).get("errmsg")
        return r
    return {"__exception__": type(e).__name__, "text": str(e)[:200]}


def _shape(plan):
    if not isinstance(plan, dict):
        return None
    ins = plan.get("inputStages") or ([plan["inputStage"]] if "inputStage" in plan else [])
    s = {"stage": plan.get("stage")}
    if ins:
        s["in"] = [_shape(i) for i in ins]
    if plan.get("stage") == "IXSCAN":
        for k in ("indexName", "direction", "isMultiKey", "isUnique", "isSparse", "isPartial", "keyPattern", "indexBounds"):
            if k in plan:
                s[k] = plan[k]
    return s


def reduce_explain(doc, stats):
    out = {"plan": _shape(doc["queryPlanner"]["winningPlan"])}
    if stats:
        es = doc["executionStats"]
        out["stats"] = {k: es[k] for k in ("nReturned", "totalKeysExamined", "totalDocsExamined") if k in es}
    return out


def pick(doc, keys, reduce=None):
    if reduce == "collstats_stage":
        return doc
    if reduce == "collstats":
        return {"nindexes": doc["nindexes"], "count": doc["count"], "indexNames": sorted(doc["indexSizes"].keys()), "ok": doc["ok"]}
    if reduce == "explain":
        return reduce_explain(doc, False)
    if reduce == "explain_stats":
        return reduce_explain(doc, True)
    if keys is None:
        return {k: v for k, v in doc.items() if k not in ("operationTime", "$clusterTime", "localTime", "connectionId", "topologyVersion", "$db")}
    return {k: doc[k] for k in keys if k in doc}


def _strip_event(e):
    e = dict(e)
    for k in ("_id", "clusterTime", "wallTime"):
        e.pop(k, None)
    if "txnNumber" in e:
        e["txnNumber"], e["lsid"] = "<n>", "<lsid>"
    return e


def _collect(st, n):
    out = []
    for _ in range(8):
        if len(out) >= n:
            break
        e = st.try_next()
        if e is not None:
            out.append(_strip_event(e))
        elif len(out) >= n:
            break
    return out


def run_watch(ctx, db, dbname, step, coll):
    """Open a change stream, run `actions`, collect `expect` events; optionally resume after the last event."""
    target = ctx.client if step.get("cluster") else (coll if coll is not None else db)
    if step.get("cluster"):
        target = ctx.client
    opts = dict(step.get("options", {}))
    st = target.watch(step.get("pipeline", []), **opts)
    for a in step.get("actions", []):
        exec_step(ctx, db, dbname, dict(a))
    first = _collect(st, step.get("expect", 0))
    if "resume_actions" not in step:
        st.close()
        return first
    token = st.resume_token
    st.close()
    for a in step["resume_actions"]:
        exec_step(ctx, db, dbname, dict(a))
    st2 = target.watch(step.get("pipeline", []), **{**opts, step.get("resume_with", "resume_after"): token})
    second = _collect(st2, step.get("resume_expect", 0))
    st2.close()
    return {"first": first, "resumed": second}


def exec_step(ctx, db, dbname, step):
    op = step["op"]
    coll = db[step["coll"]] if "coll" in step else None
    if not hasattr(ctx, "sessions"):
        ctx.sessions = {}
    ses = ctx.sessions.get(step["session"]) if "session" in step else None
    skw = {"session": ses} if ses is not None else {}
    if op == "watch":
        return run_watch(ctx, db, dbname, step, coll)
    if op == "txn_start":
        s = ctx.client.start_session()
        s.start_transaction()
        ctx.sessions[step["name"]] = s
        return SKIP
    if op in ("txn_commit", "txn_abort"):
        s = ctx.sessions.pop(step["session"])
        try:
            (s.commit_transaction if op == "txn_commit" else s.abort_transaction)()
        finally:
            s.end_session()
        return "ok"
    if op == "with_txn":
        s = ctx.client.start_session()
        out = []

        def body(sess):
            out.clear()
            ctx.sessions["__wt"] = sess
            for st in step["steps"]:
                out.append(exec_step(ctx, db, dbname, dict(st, session="__wt")))
        try:
            s.with_transaction(body)
        finally:
            ctx.sessions.pop("__wt", None)
            s.end_session()
        return out
    if op == "insert_one":
        return {"inserted_id": coll.insert_one(step["doc"], **skw).inserted_id}
    if op == "insert_many":
        return {"inserted_ids": coll.insert_many(step["docs"], ordered=step.get("ordered", True), **skw).inserted_ids}
    if op == "insert_generated":
        docs = generated_docs(step["kind"], step["n"])
        for i in range(0, len(docs), 500):
            coll.insert_many(docs[i:i + 500], ordered=step.get("ordered", True))
        return {"inserted": len(docs)}
    if op in ("find", "find_one", "find_count"):
        kw = {}
        for k_src, k_dst in (("projection", "projection"), ("skip", "skip"), ("limit", "limit"), ("batch_size", "batch_size")):
            if k_src in step:
                kw[k_dst] = step[k_src]
        if "sort" in step:
            kw["sort"] = pairs(step["sort"])
        if "hint" in step:
            h = step["hint"]
            kw["hint"] = h if isinstance(h, str) else pairs(h)
        if op == "find_one":
            kw.pop("limit", None)
            d = coll.find_one(step.get("filter", {}), **kw, **skw)
            return list(d.keys()) if step.get("keys_only") and d else d
        cur = coll.find(step.get("filter", {}), **kw, **skw)
        return len(list(cur)) if op == "find_count" else list(cur)
    if op == "count_documents":
        kw = {k: step[k] for k in ("skip", "limit") if k in step}
        return coll.count_documents(step.get("filter", {}), **kw, **skw)
    if op == "estimated_count":
        return coll.estimated_document_count()
    if op == "aggregate":
        kw = {"batchSize": step["batch_size"]} if "batch_size" in step else {}
        return list(coll.aggregate(step["pipeline"], **kw, **skw))
    if op == "distinct":
        return coll.distinct(step["key"], step.get("filter", {}), **skw)
    if op in ("update_one", "update_many", "replace_one"):
        kw = {"upsert": step.get("upsert", False)}
        if "array_filters" in step:
            kw["array_filters"] = step["array_filters"]
        r = getattr(coll, op)(step["filter"], step["update"], **kw, **skw)
        return {"matched": r.matched_count, "modified": r.modified_count, "upserted_id": r.upserted_id}
    if op in ("delete_one", "delete_many"):
        return {"deleted": getattr(coll, op)(step["filter"], **skw).deleted_count}
    if op in ("find_one_and_update", "find_one_and_replace", "find_one_and_delete"):
        from pymongo import ReturnDocument
        kw = {}
        if "projection" in step:
            kw["projection"] = step["projection"]
        if "sort" in step:
            kw["sort"] = pairs(step["sort"])
        if op != "find_one_and_delete":
            kw["upsert"] = step.get("upsert", False)
            kw["return_document"] = ReturnDocument.AFTER if step.get("return_document") == "after" else ReturnDocument.BEFORE
        if "array_filters" in step:
            kw["array_filters"] = step["array_filters"]
        if op == "find_one_and_delete":
            return coll.find_one_and_delete(step["filter"], **kw, **skw)
        return getattr(coll, op)(step["filter"], step["update"], **kw, **skw)
    if op == "create_collection":
        db.create_collection(step["coll"])
        return "ok"
    if op == "drop_collection":
        coll.drop()
        return "ok"
    if op == "list_collection_names":
        return sorted(db.list_collection_names())
    if op == "list_collections":
        flt = step.get("filter", {})
        return sorted([{"name": c["name"], "type": c["type"]} for c in db.list_collections(filter=flt)], key=lambda c: c["name"])
    if op == "create_index":
        kw = {k: step[k] for k in ("unique", "name") if k in step}
        kw.update(step.get("opts", {}))
        return coll.create_index([tuple(k) for k in step["keys"]], **kw)
    if op == "drop_index":
        coll.drop_index(step["index"] if isinstance(step["index"], str) else [tuple(k) for k in step["index"]])
        return "ok"
    if op == "list_indexes":
        out = []
        for i in coll.list_indexes():
            out.append(dict(i) if step.get("full") else {k: i[k] for k in ("v", "key", "name", "unique") if k in i})
        return out
    if op in ("command", "command_raw"):
        return pick(db.command(swap_db(step["cmd"], dbname), **skw), step.get("pick"), step.get("reduce"))
    if op == "admin_command":
        return pick(ctx.client.admin.command(swap_db(step["cmd"], dbname)), step.get("pick"))
    if op == "db_in_list":
        return dbname in ctx.client.list_database_names()
    if op == "db_in_list_command":
        r = ctx.client.admin.command({"listDatabases": 1, "nameOnly": True})
        return any(d["name"] == dbname for d in r["databases"])
    if op == "drop_database":
        ctx.client.drop_database(dbname)
        return "ok"
    if op == "session_insert":
        with ctx.client.start_session() as s:
            coll.insert_one({"_id": 1}, session=s)
            return list(coll.find({}, session=s))
    if op == "ping_with_server_api":
        c = MongoClient(ctx.uri, server_api=ServerApi("1"), serverSelectionTimeoutMS=15000)
        try:
            return c.admin.command("ping")["ok"]
        finally:
            c.close()
    if op == "raw_op_query":
        return raw_op_query(ctx)
    if op == "insert_one_w0":
        from pymongo import WriteConcern
        r = coll.with_options(write_concern=WriteConcern(w=0)).insert_one(step["doc"])
        return {"acknowledged": r.acknowledged}
    if op == "client_with_compression":
        c = MongoClient(ctx.uri, compressors="zlib", serverSelectionTimeoutMS=15000)
        try:
            c[dbname][step["coll"]].insert_one({"_id": 1, "v": "x" * 2000})
            return list(c[dbname][step["coll"]].find())
        finally:
            c.close()
    if op == "cursor_batches":
        cmd = {"find": step["coll"], "sort": step["sort"]}
        if "batch_size" in step:
            cmd["batchSize"] = step["batch_size"]
        return raw_batches(db, step["coll"], db.command(cmd))
    if op == "cursor_batches_agg":
        cmd = {"aggregate": step["coll"], "pipeline": step["pipeline"], "cursor": {"batchSize": step["batch_size"]}}
        return raw_batches(db, step["coll"], db.command(cmd))
    if op == "cursor_kill_after":
        cur = coll.find({}, sort=[("_id", 1)], batch_size=step["batch_size"])
        got = [next(cur)["_id"] for _ in range(step["take"])]
        cur.close()
        return {"taken": len(got), "alive": cur.alive}
    if op == "cursor_close_midway":
        cur = coll.find({}, sort=[("_id", 1)], batch_size=step["batch_size"])
        got = [next(cur)["_id"] for _ in range(step["take"])]
        if ctx.is_ours:
            db.command({"tenantAdmin": 1, "close": True})
        got += [d["_id"] for d in cur]
        return {"n": len(got), "ids_ok": got == list(range(300))}
    if op == "getmore_bogus":
        return db.command({"getMore": Int64(123456789), "collection": step["coll"]})
    if op == "sleep":
        time.sleep(step["secs"])
        return SKIP
    if op == "tenant_close":
        if ctx.is_ours:
            db.command({"tenantAdmin": 1, "close": True})
        return SKIP
    if op == "expect_mode":
        if ctx.is_ours:
            got = db.command({"tenantAdmin": 1})["mode"]
            if got != step["mode"]:
                ctx.asserts.append(f"expected tenant mode {step['mode']!r}, found {got!r}")
        return SKIP
    if op == "restart_server":
        if ctx.is_ours:
            pid = int(open(os.path.join(HERE, "out", f"server-{ctx.port}.pid")).read())
            if step.get("kill"):
                os.kill(pid, 9)
                time.sleep(0.5)
            subprocess.run([ctx.server_script, str(ctx.port), str(ctx.idle_secs)], check=True, stdout=subprocess.DEVNULL)
            ctx.reconnect()
            db = ctx.client[dbname]
        return SKIP
    if op == "parallel_inserts":
        errs = []

        def work(w):
            try:
                for i in range(step["per_worker"]):
                    coll.insert_one({"_id": f"{w}-{i}", "w": w, "i": i})
            except Exception as e:  # noqa
                errs.append(repr(e))
        run_threads(work, step["workers"])
        return {"errors": errs}
    if op == "parallel_inc":
        errs = []

        def work(w):
            try:
                for _ in range(step["per_worker"]):
                    coll.update_one({"_id": 1}, {"$inc": {"n": 1}})
            except Exception as e:  # noqa
                errs.append(repr(e))
        run_threads(work, step["workers"])
        return {"errors": errs}
    if op == "parallel_dbs":
        res = {}

        def work(k):
            d = ctx.client[f"{dbname}_{k}"]
            d.c.insert_many([{"_id": i} for i in range(step["per_db"])])
            res[k] = d.c.count_documents({})
        run_threads(work, step["dbs"])
        for k in range(step["dbs"]):
            ctx.client.drop_database(f"{dbname}_{k}")
        return [res[k] for k in sorted(res)]
    if op == "parallel_read_write":
        bad = []

        def writer(w):
            for i in range(30):
                coll.insert_one({"_id": 1000 + w * 100 + i})

        def reader(_):
            for _ in range(30):
                if coll.count_documents({}) < 100:
                    bad.append("count dropped below 100")
        ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)] + [threading.Thread(target=reader, args=(r,)) for r in range(4)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        return {"bad": bad, "final": coll.count_documents({})}
    raise RuntimeError(f"unknown op {op}")


def raw_op_query(ctx):
    """Send a legacy OP_QUERY `isMaster` to admin.$cmd on a plain socket and parse the OP_REPLY."""
    import bson
    host, port = ctx.uri.split("//")[1].split("/")[0].split(":")
    body = struct.pack("<i", 0) + b"admin.$cmd\x00" + struct.pack("<ii", 0, -1) + bson.encode({"ismaster": 1, "helloOk": True})
    msg = struct.pack("<iiii", 16 + len(body), 4242, 0, 2004) + body
    with socket.create_connection((host, int(port)), timeout=10) as s:
        s.sendall(msg)
        hdr = b""
        while len(hdr) < 16:
            hdr += s.recv(16 - len(hdr))
        length, _, response_to, opcode = struct.unpack("<iiii", hdr)
        rest = b""
        while len(rest) < length - 16:
            rest += s.recv(length - 16 - len(rest))
    flags, cursor_id, starting_from, n_returned = struct.unpack("<iqii", rest[:20])
    doc = bson.decode(rest[20:])
    keep = ("ismaster", "helloOk", "maxBsonObjectSize", "maxMessageSizeBytes", "maxWriteBatchSize", "minWireVersion", "maxWireVersion", "readOnly", "ok")
    return {"opcode": opcode, "response_to": response_to, "response_flags": flags, "cursor_id": cursor_id, "starting_from": starting_from, "number_returned": n_returned, "doc": {k: doc[k] for k in keep if k in doc}}


def unescape(x):
    """Undo gen_tests' escaping of the legacy `$regex`/`$options` keys."""
    if isinstance(x, dict):
        return {("$" + k[2:] if k in ("@@regex", "@@options") else k): unescape(v) for k, v in x.items()}
    if isinstance(x, list):
        return [unescape(v) for v in x]
    return x


def run_threads(fn, n):
    ts = [threading.Thread(target=fn, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]


def raw_batches(db, coll, first):
    sizes = [len(first["cursor"]["firstBatch"])]
    ids = [d["_id"] for d in first["cursor"]["firstBatch"]]
    cid = first["cursor"]["id"]
    while cid:
        r = db.command({"getMore": cid, "collection": coll})
        sizes.append(len(r["cursor"]["nextBatch"]))
        ids += [d["_id"] for d in r["cursor"]["nextBatch"]]
        cid = r["cursor"]["id"]
    return {"batch_sizes": sizes, "ids_sorted": ids == sorted(ids), "n": len(ids)}


def subst_now(x):
    """Replace {"$nowOffsetMs": n} with the current time plus n ms (TTL tests)."""
    import datetime as _dt
    if isinstance(x, dict):
        if set(x) == {"$nowOffsetMs"}:
            return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0) + _dt.timedelta(milliseconds=int(x["$nowOffsetMs"]))
        return {k: subst_now(v) for k, v in x.items()}
    if isinstance(x, list):
        return [subst_now(v) for v in x]
    return x


def run_script(ctx, test, dbname):
    results = []
    db = ctx.client[dbname]
    ctx.asserts = []
    for step in copy.deepcopy(test["steps"]):
        try:
            step = subst_now(step)
            r = exec_step(ctx, db, dbname, step)
            if ctx.client is not None:
                db = ctx.client[dbname]  # a restart replaces the client
        except Exception as e:  # noqa
            r = err_result(e, step.get("cmp_msg", False))
            db = ctx.client[dbname]
        if r is not SKIP:
            results.append(r)
    return results


def normalize(x, keep_oids):
    if isinstance(x, ObjectId):
        return x if keep_oids else "<oid>"
    if isinstance(x, dict):
        return {k: normalize(v, keep_oids) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [normalize(v, keep_oids) for v in x]
    return x


def canon(x, test):
    x = normalize(x, test.get("keep_oids", False))
    if test.get("unordered") and isinstance(x, list):
        x = sorted(x, key=lambda e: json_util.dumps(e, json_options=CANONICAL_JSON_OPTIONS, sort_keys=True))
    return json_util.dumps(x, json_options=CANONICAL_JSON_OPTIONS, sort_keys=test.get("sort_keys", False))


def compare(test, real, ours):
    if len(real) != len(ours):
        return {"step": min(len(real), len(ours)), "real": f"{len(real)} results", "ours": f"{len(ours)} results"}
    for i, (a, b) in enumerate(zip(real, ours)):
        ca, cb = canon(a, test), canon(b, test)
        if ca != cb:
            return {"step": i, "op": test["steps"][min(i, len(test["steps"]) - 1)]["op"], "real": ca[:600], "ours": cb[:600]}
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tests", default=os.path.join(HERE, "tests.json"))
    ap.add_argument("--only", default="")
    ap.add_argument("--category", default="")
    ap.add_argument("--real", default="mongodb://localhost:27017")
    ap.add_argument("--ours", default="mongodb://localhost:27018")
    ap.add_argument("--ours-port", type=int, default=27018)
    ap.add_argument("--ours-idle-secs", type=int, default=60)
    ap.add_argument("--idle", default="mongodb://localhost:27019")
    ap.add_argument("--idle-port", type=int, default=27019)
    ap.add_argument("--idle-secs", type=int, default=3)
    ap.add_argument("--out", default=os.path.join(HERE, "out", "results_py.json"))
    a = ap.parse_args()
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    server_script = os.path.join(HERE, "server.sh")
    tests = unescape(json_util.loads(open(a.tests).read()))
    if a.only:
        tests = [t for t in tests if a.only in t["name"]]
    if a.category:
        tests = [t for t in tests if t["category"] == a.category]
    real = Ctx(a.real, 27017, 0, False, server_script)
    ours = Ctx(a.ours, a.ours_port, a.ours_idle_secs, True, server_script)
    idle = Ctx(a.idle, a.idle_port, a.idle_secs, True, server_script)
    out = []
    for n, t in enumerate(tests):
        dbname = f"dt{n}_{uuid.uuid4().hex[:8]}"
        our_ctx = idle if t.get("idle") else ours
        t0 = time.time()
        try:
            r = run_script(real, t, dbname)
            o = run_script(our_ctx, t, dbname)
            diff = compare(t, r, o)
            if our_ctx.asserts:
                diff = diff or {"step": -1, "real": "", "ours": "; ".join(our_ctx.asserts)}
            status = "pass" if diff is None else "fail"
        except Exception as e:  # noqa
            status, diff = "error", {"step": -1, "real": "", "ours": f"harness error: {e!r}"}
        for ctx in (real, our_ctx):
            try:
                ctx.client.drop_database(dbname)
            except Exception:  # noqa
                pass
        out.append({"name": t["name"], "group": t["group"], "category": t["category"], "status": status, "detail": diff, "secs": round(time.time() - t0, 2)})
        mark = "." if status == "pass" else "F"
        print(f"{mark} {t['name']}" + ("" if status == "pass" else f"\n    step {diff['step']} {diff.get('op','')}\n    real: {diff['real']}\n    ours: {diff['ours']}"), flush=True)
    json.dump(out, open(a.out, "w"), indent=1)
    for cat in ("core", "extra", "replset"):
        sel = [x for x in out if x["category"] == cat]
        if sel:
            print(f"{cat}: {sum(x['status']=='pass' for x in sel)}/{len(sel)} passed")


if __name__ == "__main__":
    main()
