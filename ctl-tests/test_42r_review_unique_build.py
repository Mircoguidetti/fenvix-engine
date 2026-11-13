"""Review of fix-unique-txn: a unique index built in chunks must not end up holding a duplicate value.

A unique index on a collection that already has documents is built in chunks (`cmd_index.rs`, `build_step`), releasing the
database lock between chunks. While it is `Building`, writes maintain its entries but skip the uniqueness check
(`coll.rs`, `Writes::put` checks only `State::Ready`), and the build checks only the documents it still has to scan
(ahead of its cursor). Two writes of the same value on documents behind the cursor are never checked by anybody: the build
finishes, the index is `unique`, and it holds two documents with the same value (guarantee 4, and the same "two writers
create a duplicate unique value" hole the fix closes for transactions).

mongod (hybrid index build): the concurrent writes are accepted and the build fails at the end with DuplicateKey (11000), or
a write is refused. Never a finished unique index with a duplicate. The same assertion is checked on the reference mongod
when it is reachable on port 27020.
"""
import os
import threading
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, free_port, make_bucket

REAL = os.environ.get("REAL_MONGOD", "mongodb://127.0.0.1:27020/?directConnection=true")
# integration-6 (known failures): with 3,000 documents our build (one per chunk) now ends within the 0.5 s before the writes
# (it did not on integration-3), so the test failed without reaching its case; 20,000 keeps the build running past them
DOCS = 20000


@pytest.fixture(scope="module")
def ours():
    port = free_port()
    # one document per build chunk, so the build is still running when the writes arrive
    srv = Server(make_bucket("uniqbuild"), "uniqbuild", port=port, env={"MONGO_BUILD_CHUNK": "1"},
                 extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    c = srv.client()
    yield c
    c.close()
    srv.term()


@pytest.fixture(params=["ours", "mongod"])
def client(request, ours):
    if request.param == "ours":
        return ours
    c = MongoClient(REAL, serverSelectionTimeoutMS=2000)
    try:
        c.admin.command("ping")
    except PyMongoError:
        pytest.skip("reference mongod not reachable on 27020")
    return c


def test_unique_build_with_duplicate_written_behind_the_cursor(client):
    db = client.uq_review_build
    client.drop_database(db.name)
    co = db.c
    # _id 1000.. so that the writes below (_id 1 and 2) land behind the build cursor once the build has started
    co.insert_many([{"_id": 1000 + i, "k": i} for i in range(DOCS)])
    res = {}

    def build():
        try:
            co.create_index("k", unique=True)
            res["build"] = "ok"
        except OperationFailure as e:
            res["build"] = e.code

    t = threading.Thread(target=build)
    t.start()
    time.sleep(0.5)
    writes = []
    for _id in (1, 2):
        try:
            co.insert_one({"_id": _id, "k": -1})
            writes.append("ok")
        except OperationFailure as e:
            writes.append(e.code)
    build_was_running = t.is_alive()
    t.join()
    if not build_was_running and client.address[1] != 27020:
        pytest.fail("the build finished before the writes: the test did not exercise the window (raise DOCS)")
    unique = [i for i in co.list_indexes() if i["name"] == "k_1" and i.get("unique")]
    n = co.count_documents({"k": -1})
    # a finished unique index never holds two documents with the same value
    assert not (unique and n > 1), {"build": res.get("build"), "writes": writes, "docs_with_k=-1": n}
