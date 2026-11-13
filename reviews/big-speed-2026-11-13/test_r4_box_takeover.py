"""Review of big-speed fix 3: a customer's box warmed in process P1 must never serve stale data after another process
P2 took the database over, wrote to that customer (update, insert, delete) and moved the rows into new runs; P1 then
takes it back and reads the customer again. Both directions of box state: P1's box marked warm and its disk floor full."""
import os, signal, sys, time
sys.path.insert(0, "/home/user/bs/ctl-tests")
from lib import Server, make_bucket

ENV = {"FENVIX_TEST_DROP": "1"}
SEALED = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]


def snapshot(db, t):
    return sorted((d["_id"], d.get("v")) for d in db.checkpoints.find({"thread_id": t}))


def wait_serving(s, attempts=60):
    c = s.client()
    for _ in range(attempts):
        try:
            c.app.checkpoints.find_one({"thread_id": "nobody"})
            return c
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("no serving")


import pytest
OFF = ["--customer-box", "false", "--box-disk", "false", "--id-check-in-box", "false"]


@pytest.mark.parametrize("mode", ["on", "off"])
def test_box_not_stale_after_takeover_and_back(mode):
    x = SEALED + (OFF if mode == "off" else [])
    bucket = make_bucket("rv-box-to")
    p1 = Server(bucket, "rvboxA", idle=600, extra=x, env=ENV).start()
    p2 = Server(bucket, "rvboxB", idle=600, extra=x, env=ENV)
    try:
        c1 = p1.client()
        db1 = c1.app
        for i in range(8):
            t = f"t{i}"
            db1.checkpoints.insert_many([{"_id": f"{t}-{s}", "thread_id": t, "v": f"p1-{s}", "pad": "x" * 3000} for s in range(30)])
        st, r = p1.admin.post("/v1/test/databases/app/publish-drop", {})
        assert st == 200, r
        before = {f"t{i}": snapshot(db1, f"t{i}") for i in range(8)}   # P1 warms every box, floor fills
        box = c1.admin.command("serverStatus")["fenvix"]["customerBox"]
        print("P1 box after first reads:", {k: box[k] for k in ("warms", "warmBlocks") if k in box})
        os.kill(p1.proc.pid, signal.SIGSTOP)
        time.sleep(9)       # lease ttl 6 s
        p2.start()
        c2 = wait_serving(p2)
        db2 = c2.app
        for i in range(8):
            t = f"t{i}"
            db2.checkpoints.update_one({"_id": f"{t}-3", "thread_id": t}, {"$set": {"v": "p2-updated"}})
            db2.checkpoints.insert_one({"_id": f"{t}-new", "thread_id": t, "v": "p2-new"})
            db2.checkpoints.delete_one({"_id": f"{t}-7", "thread_id": t})
        st, r = p2.admin.post("/v1/test/databases/app/publish-drop", {})
        assert st == 200, r
        want = {f"t{i}": snapshot(db2, f"t{i}") for i in range(8)}
        d2 = db2.checkpoints.find_one({"_id": "t0-3", "thread_id": "t0"})
        print("MODE", mode, "P2 own point read t0-3:", d2 and d2.get("v"))
        print("MODE", mode, "before t0 n", len(before["t0"]), before["t0"][:5], "| P2 sees t0 n", len(want["t0"]), want["t0"][:5])
        print("MODE", mode, "P2 count all", db2.checkpoints.count_documents({}), "P2 serverStatus sealedRuns rowsFromRuns", c2.admin.command("serverStatus")["fenvix"]["sealedRuns"]["rowsFromRuns"])
        assert all(want[t] != before[t] for t in want)
        os.kill(p2.proc.pid, signal.SIGKILL)
        p2.proc.wait()
        os.kill(p1.proc.pid, signal.SIGCONT)
        time.sleep(9)
        c1 = wait_serving(p1)
        db1 = c1.app
        wrong = [t for t in want if snapshot(db1, t) != want[t]]
        # point reads by _id too (the box serves finds by customer; a point read goes through the same caches)
        for i in range(8):
            t = f"t{i}"
            d = db1.checkpoints.find_one({"_id": f"{t}-3", "thread_id": t})
            if d is None or d["v"] != "p2-updated":
                wrong.append(f"{t}-3 point read {d and d.get('v')}")
            if db1.checkpoints.find_one({"_id": f"{t}-7", "thread_id": t}) is not None:
                wrong.append(f"{t}-7 deleted by P2 but read by P1")
        for q in ({"_id": "t0-3", "thread_id": "t0"}, {"_id": "t0-3"}, {"_id": "t0-4", "thread_id": "t0"}, {"_id": "t0-new", "thread_id": "t0"}, {"thread_id": "t0", "v": "p2-updated"}):
            d = db1.checkpoints.find_one(q)
            print("MODE", mode, "P1 find_one", q, "->", d and d.get("v"))
        fl = c1.admin.command("serverStatus")["fenvix"]
        print("MODE", mode, "P1 after takeback: wrong", wrong, "customerBox", {k: v for k, v in fl["customerBox"].items() if isinstance(v, int)})
        assert not wrong, wrong
    finally:
        for p in (p1, p2):
            try:
                os.kill(p.proc.pid, signal.SIGCONT)
            except Exception:
                pass
            try:
                p.term()
            except Exception:
                pass
