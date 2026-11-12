"""Step 9 · test of the files programs/takeovers.py now keeps for diagnosis (aws-fixes, total review question 2 point 2):
every acknowledged _id with its time and server (takeovers-acked.jsonl.gz) and every missing _id at each of the three
reads (takeovers-missing.json), not only the first 50.
The program runs UNCHANGED (runpy) against test doubles: S3 is a dict whose agent acknowledges every command at once,
and the two pair processes are in-memory stores; "kill9" stops a process from confirming, and the double loses the last
LOSE acknowledged writes of the killed process at every kill (a planted loss), so the missing-id file must list exactly
those, with the server that had confirmed them. No engine, no network, $0.
Usage: python -m pytest -q step9/local/tests/test_takeover_ids.py   (needs pymongo and boto3 importable)"""
import gzip
import json
import os
import runpy
import sys
import tempfile
import threading
import time
import types

HERE = os.path.dirname(os.path.abspath(__file__))
PROG = os.path.join(HERE, "..", "..", "programs", "takeovers.py")
LOSE = 30


class ClientError(Exception):
    pass


class FakeS3:
    def __init__(self):
        self.o = {}

    def put_object(self, Bucket, Key, Body):  # noqa: N803
        self.o[Key] = Body
        if "/cmd-" in Key:   # the agent answers at once
            name, n = Key.split("/")[1], Key.rsplit("-", 1)[1].split(".")[0]
            cmd = json.loads(Body)
            STATE.act(name, cmd["action"])
            self.o[f"tk/{name}/ack-{n}.json"] = json.dumps({"ok": True, "t": time.time()}).encode()

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.o:
            raise ClientError("404")

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.o:
            raise ClientError("404")
        b = self.o[Key]
        return {"Body": types.SimpleNamespace(read=lambda: b)}


class Pair:
    """The two pair processes: one shared store (the pantry and the log), each process up or down."""

    def __init__(self):
        self.lock = threading.Lock()
        self.store, self.up, self.by = {}, {"server-1": False, "spare": False}, {}
        self.planted = []

    def act(self, name, action):
        with self.lock:
            if action == "start":
                self.up[name] = True
            elif action in ("kill9", "stop"):
                self.up[name] = False
                if action == "kill9":   # a planted loss: the last LOSE writes this process confirmed vanish
                    mine = [k for k, v in self.by.items() if v == name and k in self.store][-LOSE:]
                    for k in mine:
                        self.store.pop(k, None)
                    self.planted += mine


STATE = Pair()
IP = {"10.0.0.1": "server-1", "10.0.0.2": "spare"}


class FakeColl:
    def __init__(self, name):
        self.name = name

    def insert_one(self, doc):
        with STATE.lock:
            if not STATE.up[self.name]:
                raise ConnectionError("down")
            STATE.store[doc["_id"]] = doc
            STATE.by[doc["_id"]] = self.name

    def find(self, *a, **k):
        with STATE.lock:
            return [{"_id": x} for x in STATE.store]


class FakeClient:
    def __init__(self, uri, **kw):
        self.name = IP[uri.split("@")[1].split(":")[0]]
        # tenantStats answers like our server: each process's quaderno.failed is 1 (total review N4)
        self.admin = types.SimpleNamespace(command=lambda name, *_: {"quaderno": {"sent": 10, "failed": 1}}
                                           if name == "tenantStats" else {"ok": 1})

    def __getitem__(self, db):
        return _Db(self.name)

    def close(self):
        pass


class _Db:
    def __init__(self, name):
        self.name = name

    def get_collection(self, *a, **k):
        return FakeColl(self.name)

    def __getitem__(self, coll):
        return FakeColl(self.name)


def test_every_acknowledged_and_missing_id_is_saved():
    out = tempfile.mkdtemp()
    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.MongoClient = FakeClient
    fake_pymongo.WriteConcern = lambda **k: None
    fake_boto3 = types.ModuleType("boto3")
    s3 = FakeS3()
    fake_boto3.client = lambda *a, **k: s3
    fake_exc = types.ModuleType("botocore.exceptions")
    fake_exc.ClientError = ClientError
    saved = {k: sys.modules.get(k) for k in ("pymongo", "boto3", "botocore", "botocore.exceptions")}
    sys.modules.update({"pymongo": fake_pymongo, "boto3": fake_boto3, "botocore": types.ModuleType("botocore"),
                        "botocore.exceptions": fake_exc})
    os.environ["CM_PW"] = "x"
    sys.argv = ["takeovers.py", "bucket", out, "--a", "10.0.0.1", "--b", "10.0.0.2", "--ca", "/dev/null", "--n", "3",
                "--writers", "2", "--between-s", "0.3", "--settle-s", "0.1"]
    code = 0
    try:
        runpy.run_path(PROG, run_name="__main__")
    except SystemExit as e:
        code = e.code
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    s = json.load(open(os.path.join(out, "takeovers-summary.json")))
    acked = [json.loads(x) for x in gzip.open(os.path.join(out, "takeovers-acked.jsonl.gz"), "rt")]
    miss = json.load(open(os.path.join(out, "takeovers-missing.json")))
    assert len(acked) == s["acknowledged"] > 0
    # N4: 3 killed processes read just before their kill + the survivor: 4 batches took the fallback, and it is said
    assert s["log_fallback"]["batches"] == 4, s["log_fallback"]
    assert all(r["victim_quaderno_failed"] == 1 for r in s["takeovers"])
    assert "4 write batches took the S3-WAL fallback" in s["log_fallback"]["label"]
    assert all({"_id", "t", "server", "ip"} <= set(r) for r in acked)
    assert {r["server"] for r in acked} == {"server-1", "spare"}
    assert [r["t"] for r in acked] == sorted(r["t"] for r in acked)
    planted = set(STATE.planted)
    for label in ("at_once", "after_settle", "after_clean_restart"):
        got = {r["_id"] for r in miss["missing"][label]}
        assert got == planted, (label, len(got), len(planted))
        assert all(r["server"] == STATE.by[r["_id"]] for r in miss["missing"][label])
    assert s["lost"] == len(planted) > 0 and s["verdict"] == "FAIL"
    assert code == 1, code   # total review R6: a verdict that is not PASS exits 1, so the job turns red
    assert len(planted) > 50 and len(s["lost_ids_first"]) == 50   # the summary keeps 50; the file keeps every one


if __name__ == "__main__":
    test_every_acknowledged_and_missing_id_is_saved()
    print("PASS")
