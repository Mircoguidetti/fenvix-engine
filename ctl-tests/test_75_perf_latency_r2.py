"""perf-latency round 2: the engine defects found by the latency work.

- distinct over a whole collection is read a chunk of documents at a time (it held every document at once: OOM at 3 GB
  on the agent-checkpoint benchmark); the answer must be the one mongod gives, for values spread over many chunks,
  arrays, mixed number types, missing fields, nested paths, TTL-expired documents, and customer-keyed collections.
"""
import datetime
import random

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket

REF = "mongodb://127.0.0.1:27020/?directConnection=true"


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("plr2")
    srv = Server(bucket, "plr2", idle=300, extra=["--customer-key", "keyed=tid"]).start()
    ref = MongoClient(REF, serverSelectionTimeoutMS=5000)
    yield srv, ref
    srv.term()


def docs(n, seed):
    r = random.Random(seed)
    out = []
    for i in range(n):
        d = {"_id": i, "tid": f"t{r.randrange(40):03d}"}
        k = r.randrange(7)
        if k == 0:
            d["v"] = r.randrange(50)
        elif k == 1:
            d["v"] = float(r.randrange(50))          # 3 and 3.0 are one distinct value
        elif k == 2:
            d["v"] = [r.randrange(50), r.randrange(50), [r.randrange(5)]]   # elements, and a nested array as a value
        elif k == 3:
            d["v"] = f"s{r.randrange(30)}"
        elif k == 4:
            d["v"] = {"a": r.randrange(5)}
        elif k == 5:
            d["v"] = None
        # k == 6: no field
        d["n"] = {"x": r.randrange(9)} if r.random() < 0.7 else [{"x": r.randrange(9)}, {"x": 100}]
        d["pad"] = "p" * r.randrange(0, 3000)
        out.append(d)
    return out


@pytest.mark.parametrize("coll", ["plain", "keyed"])
def test_distinct_chunked_matches_mongod(env, coll):
    srv, ref = env
    c = srv.client()
    ours, theirs = c["plr2"][coll], ref["plr2"][coll]
    ours.drop()
    theirs.drop()
    ds = docs(700, 7)          # 700 documents: more than ten chunks of 64
    ours.insert_many(ds)
    theirs.insert_many(ds)
    def canon(vals):
        # 3 and 3.0 are one distinct value; which of them is returned follows the scan order of each server
        return sorted(repr(float(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) else repr(v) for v in vals)
    for key in ("v", "tid", "n.x", "missing", "_id"):
        a = c["plr2"].command("distinct", coll, key=key)["values"]
        # a non-empty query takes the path that reads every matching document at once (the one before this change)
        old = c["plr2"].command("distinct", coll, key=key, query={"_id": {"$gte": 0}})["values"]
        b = ref["plr2"].command("distinct", coll, key=key)["values"]
        assert a == old, key
        assert canon(a) == canon(b), key
        assert len(a) == len(b), key


def test_distinct_chunked_hides_ttl_expired(env):
    srv, ref = env
    c = srv.client()
    coll = c["plr2"]["ttl"]
    coll.drop()
    coll.create_index("at", expireAfterSeconds=3600)
    old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
    new = datetime.datetime.now(datetime.timezone.utc)
    coll.insert_many([{"_id": i, "at": old if i % 2 else new, "v": i} for i in range(300)])
    # the expired half is hidden from reads before the TTL monitor deletes it (the server runs with --no-ttl)
    got = c["plr2"].command("distinct", "ttl", key="v")["values"]
    assert sorted(got) == [i for i in range(300) if i % 2 == 0]
    assert sorted(got) == sorted(coll.distinct("v", {}))
