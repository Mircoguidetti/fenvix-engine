"""C12 fixes after the review: what a command leaves on a database's entry for its own later steps must not reach the
maintenance that takes the entry outside a command (`Dbs::lock_outside` / `Dbs::try_lock_outside`).

Run against any binary with MONGO_SERVER_BIN; every test prints what it saw (JSON) for the report.
"""
import datetime
import json
import time

from bson import Int64

from lib import make_bucket, Server, free_port, BIN
import test_41_commit_retry as T


def _wrap_keep_ttl():
    """The tests' Server passes --no-ttl; this wrapper drops it so the TTL sweeper runs."""
    import os, stat, tempfile
    wrap = tempfile.NamedTemporaryFile("w", delete=False, suffix=".sh")
    wrap.write('#!/bin/bash\nargs=()\nfor a in "$@"; do [ "$a" != "--no-ttl" ] && args+=("$a"); done\nexec %s "${args[@]}"\n' % BIN)
    wrap.close()
    os.chmod(wrap.name, stat.S_IRWXU)
    return wrap.name


def test_ttl_sweeper_does_not_read_through_an_old_transaction_snapshot():
    """The database stays open. A transaction takes its snapshot while document 1 expires in 3 s; a plain update then moves
    its expiry one hour later; a second statement of the transaction leaves the transaction's snapshot on the entry. The TTL
    sweeper of open databases (every 1 s) must read the live state: document 1 is not expired and must stay. Before the
    fix the sweeper took the entry with `try_write()`, kept the old snapshot, saw the old date and deleted the document."""
    bucket = make_bucket("c12snap")
    port = free_port()
    P1 = Server(bucket, "c12snap1", port=port, holder="p1", idle=120, binary=_wrap_keep_ttl(),
                extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}", "--ttl-sweep-secs", "1"]).start()
    try:
        r1 = T.Raw(P1)
        db = r1["cs"]
        db.command({"createIndexes": "e", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
        now = datetime.datetime.now(datetime.timezone.utc)
        db.command({"insert": "e", "documents": [{"_id": 1, "at": now + datetime.timedelta(seconds=3)}, {"_id": 2, "at": now + datetime.timedelta(hours=1)}]})
        lsid = T._lsid()
        tx = {"lsid": lsid, "txnNumber": Int64(1), "autocommit": False}
        db.command({"find": "e", "filter": {"_id": 2}, "startTransaction": True, **tx})          # snapshot: 1 expires at +3 s
        db.command({"update": "e", "updates": [{"q": {"_id": 1}, "u": {"$set": {"at": now + datetime.timedelta(hours=1)}}}]})
        db.command({"find": "e", "filter": {"_id": 2}, **tx})                                     # leaves the snapshot on the entry
        time.sleep(7)                                                                             # expiry passed, sweeps every 1 s
        left = [d["_id"] for d in db.command({"find": "e", "filter": {}, "sort": {"_id": 1}})["cursor"]["firstBatch"]]
        try:
            r1.admin.command({"abortTransaction": 1, **tx})
        except Exception:
            pass
        got = {"left": left}
        print(json.dumps(got))
        assert left == [1, 2], got
    finally:
        P1.kill9()
