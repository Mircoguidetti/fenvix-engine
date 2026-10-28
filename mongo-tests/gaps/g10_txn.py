"""Item 10: transactionLifetimeLimitSeconds: a transaction that outlives it is aborted."""
import time
from gaps_py import case
from pymongo.errors import OperationFailure


def code(f):
    try:
        f()
        return "ok"
    except OperationFailure as e:
        return [e.code, e.details.get("codeName"), sorted(e.details.get("errorLabels", []))]


@case("g10.txn.lifetime_limit", rs=True)
def _lifetime(client, db):
    db.t.insert_one({"_id": 0})
    out = {}
    admin = client.admin
    try:
        out["set"] = admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 1}).get("was")
        with client.start_session() as s:
            s.start_transaction()
            db.t.insert_one({"_id": 1}, session=s)
            time.sleep(3.5)
            out["insert_after_expiry"] = code(lambda: db.t.insert_one({"_id": 2}, session=s))
            out["commit_after_expiry"] = code(lambda: s.commit_transaction())
        out["docs"] = sorted(d["_id"] for d in db.t.find())
        with client.start_session() as s:
            s.start_transaction()
            db.t.insert_one({"_id": 3}, session=s)
            s.commit_transaction()
        out["docs_after"] = sorted(d["_id"] for d in db.t.find())
    finally:
        admin.command({"setParameter": 1, "transactionLifetimeLimitSeconds": 60})
    return out
