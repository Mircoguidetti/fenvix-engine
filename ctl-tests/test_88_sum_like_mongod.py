"""P-fix-read-amplification, second task: `$sum` and `$avg` give mongod 7.0's answer, to the last bit and the type.

The generator's check 3 found one `$sum` of 123 doubles where we answered 78996.43000000001 (the exact sum, Kahan's
summation) and mongod 7.0 answers 78996.43 (sessions/2026-11-12-generator-1tb/REPORT-2.md section 1b). mongod sums with
a double-double (`src/mongo/util/summation.h`, `DoubleDoubleSummation`) and answers its high part, which can differ from
the exact sum in the last bit (`mongo-server/src/dsum.rs`).

The test: the same documents in our server and in a mongod 7.0 the test starts (from PATH, the judge's environment);
`$sum` and `$avg` in `$group`, and `$sum`/`$avg` of arrays in `$project` and `$addFields`, over many groups of order
totals (the shape of the generator's case), and over mixes of int, long, double, decimal, infinities and NaN. Every
result must be equal in value and type. On the base binary the order totals differ in a few percent of the groups.
"""
import os
import random
import shutil
import socket
import subprocess
import tempfile
import time

import pytest
from bson import Decimal128, Int64
from pymongo import MongoClient

from lib import Server, make_bucket, free_port

MONGOD = shutil.which("mongod") or os.path.expanduser("~/.fenvix-judge/env/bin/mongod")
pytestmark = pytest.mark.skipif(not os.path.exists(MONGOD), reason="no mongod 7.0 to compare with")


@pytest.fixture(scope="module")
def both():
    port = free_port()
    d = tempfile.mkdtemp(prefix="sum-mongod-")
    m = subprocess.Popen([MONGOD, "--dbpath", d, "--port", str(port), "--bind_ip", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            break
        except OSError:
            time.sleep(0.2)
    ref = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=20000)
    assert ref.admin.command("buildInfo")["version"].startswith("7.0"), "the reference must be mongod 7.0"
    srv = Server(make_bucket("sumlike"), "sumlike", idle=600).start()
    ours = srv.client()
    yield ours, ref
    srv.kill9()
    m.kill()
    m.wait()
    shutil.rmtree(d, ignore_errors=True)


def load(clients, docs):
    for c in clients:
        c["sumdb"]["t"].drop()
        c["sumdb"]["t"].insert_many([dict(x) for x in docs])


def run(c, pipeline):
    return list(c["sumdb"]["t"].aggregate(pipeline))


def same(a, b):
    """Equal values of the same BSON types (NaN equal to NaN), recursively."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, float):
        return (a != a and b != b) or a == b
    if isinstance(a, Decimal128):
        return str(a) == str(b)
    return a == b


def order_totals(n_groups=400, seed=7):
    rng = random.Random(seed)
    docs = []
    for g in range(n_groups):
        for _ in range(rng.randint(5, 150)):
            docs.append({"g": g, "x": round(rng.uniform(5, 1500), 2)})
    return docs


def test_group_sum_and_avg_of_order_totals(both):
    ours, ref = both
    docs = order_totals()
    load((ours, ref), docs)
    p = [{"$group": {"_id": "$g", "s": {"$sum": "$x"}, "a": {"$avg": "$x"}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
    a, b = run(ours, p), run(ref, p)
    assert len(a) == len(b) == 400
    diff = [(x, y) for x, y in zip(a, b) if not same(x, y)]
    print(f"{len(diff)} of {len(b)} groups differ; first: {diff[:2]}")
    assert not diff


def test_sum_and_avg_of_arrays_in_project_and_addfields(both):
    ours, ref = both
    rng = random.Random(9)
    docs = [{"_id": i, "v": [round(rng.uniform(5, 1500), 2) for _ in range(rng.randint(5, 150))]} for i in range(300)]
    load((ours, ref), docs)
    for p in ([{"$project": {"s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}, {"$sort": {"_id": 1}}],
              [{"$addFields": {"s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}, {"$project": {"v": 0}}, {"$sort": {"_id": 1}}]):
        a, b = run(ours, p), run(ref, p)
        diff = [(x, y) for x, y in zip(a, b) if not same(x, y)]
        print(f"{p[0]}: {len(diff)} of {len(b)} differ; first: {diff[:2]}")
        assert len(a) == len(b) and not diff


MIXES = {
    "ints": [1, 2, 3],
    "int overflow to long": [2147483647, 1],
    "longs": [Int64(1), Int64(2), 3],
    "long overflow to double": [Int64(9223372036854775807), Int64(1)],
    "long exact past 2^53": [Int64(1 << 60), Int64(1), Int64(-(1 << 60))],
    "int and double": [1, 0.5],
    "doubles": [0.1, 0.2, 0.3],
    "decimals": [Decimal128("1.10"), Decimal128("2.20")],
    "decimal and int": [Decimal128("1"), 2],
    "decimal and double": [Decimal128("1"), 0.1],
    "decimal and long": [Decimal128("0.5"), Int64(10)],
    "infinity": [float("inf"), 1.0],
    "infinities": [float("inf"), float("-inf")],
    "nan": [float("nan"), 1.0],
    "decimal infinity": [Decimal128("Infinity"), 1],
    "non numbers ignored": [1, "a", None, 2.5, True],
    "empty": [],
}


@pytest.mark.parametrize("name", list(MIXES))
def test_type_promotion_like_mongod(both, name):
    ours, ref = both
    vals = MIXES[name]
    docs = [{"_id": i, "x": v} for i, v in enumerate(vals)] or [{"_id": 0}]
    load((ours, ref), docs)
    p = [{"$group": {"_id": None, "s": {"$sum": "$x"}, "a": {"$avg": "$x"}}}]
    a, b = run(ours, p), run(ref, p)
    print(name, a, b)
    assert same(a, b), f"{name}: ours {a}, mongod {b}"
    load((ours, ref), [{"_id": 0, "v": vals}])
    p = [{"$project": {"s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}]
    a, b = run(ours, p), run(ref, p)
    assert same(a, b), f"{name} in $project: ours {a}, mongod {b}"
