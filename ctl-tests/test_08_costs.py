"""Object-store cost of every operation (GET-tier requests, PUT-tier requests, deletes, bytes) at 1,000 and 100,000
documents, measured from outside the server by a counting proxy between it and MinIO. Writes out/costs.json."""
import json
import os
import tempfile
import time

import pytest

import export_client as xc
from lib import Server, make_bucket, fill, wait_until, OUT, TOKEN, list_prefix, prefix_bytes
from proxy import CountingProxy

SIZES = [1000, 100000]
RESULTS = {}


def related(key, dbs):
    parts = key.split("/")
    if parts[0] in ("tenants", "ttl-hints"):
        return len(parts) > 1 and parts[1] in dbs
    if parts[0] == "control":
        if parts[1] == "usage":
            return False  # the metering records are not part of an operation
        return len(parts) > 2 and any(parts[2] == d or parts[2] == d + ".json" or parts[2].startswith(d + ".") for d in dbs)
    return False


def window(proxy, dbs, fn):
    proxy.reset()
    t0 = time.time()
    out = fn()
    el = time.time() - t0
    time.sleep(0.4)
    snap = proxy.snapshot()
    n = {}
    byt = {}
    for key, kinds in snap["by_prefix"].items():
        if related(key, dbs):
            for k, v in kinds.items():
                n[k] = n.get(k, 0) + v
    for key, kinds in snap["bytes_by_prefix"].items():
        if related(key, dbs):
            for k, v in kinds.items():
                byt[k] = byt.get(k, 0) + v
    # DELETE requests carry their keys in the body (S3 multi-delete), so they cannot be attributed by path: count them all
    dels = sum(v for kinds in snap["by_prefix"].values() for k, v in kinds.items() if k in ("DELETE", "DELETE_MULTI"))
    cost = {
        "get_tier": n.get("GET", 0) + n.get("GET_RANGE", 0) + n.get("HEAD", 0),
        "put_tier": n.get("PUT", 0) + n.get("PUT_PART", 0) + n.get("MP_CREATE", 0) + n.get("MP_COMPLETE", 0) + n.get("COPY", 0) + n.get("LIST", 0),
        "lists": n.get("LIST", 0), "copies": n.get("COPY", 0), "delete_requests_all_paths": dels,
        "get_bytes": byt.get("GET:out", 0) + byt.get("GET_RANGE:out", 0), "put_bytes": byt.get("PUT:in", 0) + byt.get("PUT_PART:in", 0),
        "seconds": round(el, 3), "by_kind": n,
    }
    return out, cost


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("costs")
    proxy = CountingProxy()
    srv = Server(bucket, "costs", idle=600, endpoint=proxy.endpoint, extra=["--usage-flush-secs", "3600", "--retention-interval-secs", "100000", "--pin-lease-ttl-secs", "120"]).start()
    yield srv, bucket, proxy
    srv.term()
    proxy.stop()
    json.dump(RESULTS, open(f"{OUT}/costs.json", "w"), indent=1)


def asleep(srv, db):
    srv.client()[db].command("tenantAdmin", close=True)
    wait_until(lambda: srv.admin.get(f"/v1/databases/{db}?storage=0")[1]["state"] == "asleep", 10, what="asleep")


@pytest.mark.parametrize("n", SIZES)
def test_costs(env, n):
    srv, bucket, proxy = env
    a = srv.admin
    c = srv.client()
    db = f"c{n}"
    R = RESULTS.setdefault(str(n), {})
    t0 = time.time()
    fill(c, db, "c", n)
    c[db]["c"].create_index([("grp", 1)])
    asleep(srv, db)
    R["_load_seconds"] = round(time.time() - t0, 1)
    R["_storage_bytes"] = prefix_bytes(bucket, f"tenants/{db}/")
    R["_objects"] = len(list_prefix(bucket, f"tenants/{db}/"))
    time.sleep(1)

    # create (an empty database) ------------------------------------------------------------------
    _, R["create"] = window(proxy, ["empty_" + db], lambda: a.post("/v1/databases", {"name": "empty_" + db}))
    # get / list ------------------------------------------------------------------------------------
    _, R["get"] = window(proxy, [db], lambda: a.get(f"/v1/databases/{db}"))
    _, R["get_without_storage"] = window(proxy, [db], lambda: a.get(f"/v1/databases/{db}?storage=0"))
    # the engine's own wake, for comparison -------------------------------------------------------------
    _, R["reference_wake_and_find_one"] = window(proxy, [db], lambda: c[db]["c"].find_one({"_id": 5}))
    asleep(srv, db)
    # fork, clone -------------------------------------------------------------------------------------
    r, R["fork_clone"] = window(proxy, [db, f"fk{n}"], lambda: a.post(f"/v1/databases/{db}/fork", {"target": f"fk{n}"}))
    assert r[0] == 201
    # fork, copy -------------------------------------------------------------------------------------
    r, R["fork_copy"] = window(proxy, [db, f"fc{n}"], lambda: a.post(f"/v1/databases/{db}/fork", {"target": f"fc{n}", "mode": "copy"}))
    assert r[0] == 201
    # retention + restore ----------------------------------------------------------------------------
    _, R["enable_retention"] = window(proxy, [db], lambda: a.put(f"/v1/databases/{db}/retention", {"window_secs": 3600, "interval_secs": 100000}))
    time.sleep(1.2)
    t = int(time.time() * 1000)
    time.sleep(1.2)
    c[db]["c"].update_many({"_id": {"$lt": max(10, n // 100)}}, {"$set": {"changed": 1}})   # 1 % of the documents
    asleep(srv, db)
    time.sleep(1)
    r, R["restore_1pct_changed"] = window(proxy, [db], lambda: a.post(f"/v1/databases/{db}/restore", {"to": t}))
    assert r[0] == 200, r
    R["restore_1pct_changed"]["keys"] = r[1]["keys"]
    asleep(srv, db)
    # export: snapshot, then the download ---------------------------------------------------------------
    r, R["export_snapshot"] = window(proxy, [db], lambda: a.post(f"/v1/databases/{db}/exports"))
    assert r[0] == 201
    eid = r[1]["id"]
    dest = tempfile.mkdtemp(prefix="dump-")

    def dl():
        return xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, eid, dest)
    ver, R["export_download"] = window(proxy, [db], dl)
    assert all(v == "ok" for v in ver.values())
    R["export_download"]["file_bytes"] = os.path.getsize(f"{dest}/{db}/c.bson")
    a.delete(f"/v1/exports/{eid}")
    # usage query -------------------------------------------------------------------------------------------
    _, R["usage_query"] = window(proxy, [db], lambda: a.get(f"/v1/databases/{db}/usage"))
    # keep awake: set, then the steady state per renewal ---------------------------------------------------
    _, R["keep_awake_set"] = window(proxy, [db], lambda: a.put(f"/v1/databases/{db}/keep-awake"))
    time.sleep(2)
    _, R["keep_awake_130s_steady_state"] = window(proxy, [db], lambda: time.sleep(130))
    a.delete(f"/v1/databases/{db}/keep-awake")
    asleep(srv, db)
    # erase (the db, and both forks) ------------------------------------------------------------------------
    for name in (f"fk{n}", f"fc{n}"):
        r, R["erase_" + name[:2]] = window(proxy, [name], lambda: a.delete(f"/v1/databases/{name}"))
        assert r[0] == 200
    r, R["erase_source"] = window(proxy, [db], lambda: a.delete(f"/v1/databases/{db}"))
    assert r[0] == 200, r
    # sanity claims -------------------------------------------------------------------------------------------
    assert R["fork_clone"]["get_bytes"] < 100_000 and R["fork_clone"]["put_bytes"] < 100_000, R["fork_clone"]
    assert R["fork_copy"]["copies"] >= 3 and R["fork_copy"]["put_bytes"] < 100_000
    assert R["erase_source"]["delete_requests_all_paths"] >= 1
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "by_kind"} if isinstance(v, dict) else v for k, v in R.items()}, indent=1))


def test_clone_cost_does_not_grow_with_the_data():
    a, b = RESULTS["1000"]["fork_clone"], RESULTS["100000"]["fork_clone"]
    assert b["get_tier"] <= a["get_tier"] * 1.5 + 5 and b["put_tier"] <= a["put_tier"] * 1.5 + 5, (a, b)
    assert b["get_bytes"] < 100_000 and b["put_bytes"] < 100_000
