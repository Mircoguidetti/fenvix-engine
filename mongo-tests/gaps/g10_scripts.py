"""Item 10: command-level differential scripts. The cases live in scripts/*.json (shared with the Node runner, node/g10_scripts.js):

    {"name": "g10.x.y", "mode": "std" | "rs" | "sa", "steps": [ <command document in extended JSON>, ... ]}

Each step is run as a command (database of the case; "__db": "admin" for admin, "$OTHER" for a second database), and the reply or the
error {code, codeName, message} is recorded. "$DB" and "$OTHER" inside strings stand for the database names. Step options:
"__skip": true (run, do not record), "__strip": [names] (remove these fields wherever they occur in the reply), "__sort": true (sort the
keys of the reply), "__shape": true (compare only the structure: keys and value types), "__sortarr": "a.b" (sort the array at that path), "__pause": ms (wait before the step), "__as": "name" (record under this key instead of the step number).
Replies are normalised: cursor ids (open / closed), collection and index-build UUIDs, $clusterTime and operationTime."""
import glob
import json
import os
import re
import time
from bson import json_util
from pymongo.errors import OperationFailure, PyMongoError
from gaps_py import case

HERE = os.path.dirname(os.path.abspath(__file__))
PLAN = "PlanExecutor error during aggregation :: caused by :: "
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
VOLATILE = ("$clusterTime", "operationTime", "electionId", "opTime")


def fresh_lsid():
    import base64
    return {"id": {"$binary": {"base64": base64.b64encode(os.urandom(16)).decode(), "subType": "04"}}}


def subst(x, db, other):
    if isinstance(x, str):
        return x.replace("$OTHER", other).replace("$DB", db)
    if isinstance(x, list):
        return [subst(v, db, other) for v in x]
    if isinstance(x, dict):
        return {subst(k, db, other): subst(v, db, other) for k, v in x.items()}
    return x


def strip(x, names):
    if isinstance(x, dict):
        return {k: strip(v, names) for k, v in x.items() if k not in names}
    if isinstance(x, list):
        return [strip(v, names) for v in x]
    return x


def shape(x):
    if isinstance(x, dict):
        return {k: shape(v) for k, v in x.items()}
    if isinstance(x, list):
        return [shape(x[0])] if x else []
    if isinstance(x, bool):
        return "bool"
    if isinstance(x, (int, float)):
        return "num"
    return type(x).__name__


def norm(r, names):
    r = strip(r, set(VOLATILE) | set(names))
    c = r.get("cursor") if isinstance(r, dict) else None
    if isinstance(c, dict) and "id" in c:
        c["id"] = "open" if c["id"] else "closed"
    return unify(r)


def unify(x):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            if k in ("uuid", "collectionUUID") and getattr(v, "subtype", None) == 4:
                out[k] = "<uuid>"
            else:
                out[k] = unify(v)
        return out
    if isinstance(x, list):
        return [unify(v) for v in x]
    if isinstance(x, str):
        return UUID_RE.sub("<uuid>", x).replace(PLAN, "")
    return x


def make(c):
    def f(client, db):
        other = client[db.name + "_x"]
        out = {}
        lsid = fresh_lsid()
        base = c.get("_base", int(time.time() * 1000) * 1000)
        try:
            for i, st in enumerate(c["steps"]):
                st = dict(st)
                opts = {k: st.pop(k) for k in list(st) if k.startswith("__")}
                if opts.get("__pause"):
                    time.sleep(opts["__pause"] / 1000)
                target = db
                d = opts.get("__db")
                if d == "admin":
                    target = client.admin
                elif d == "$OTHER":
                    target = other
                st = subst(st, db.name, other.name)
                if st.get("lsid") == "$LSID":
                    # pymongo puts the lsid of its own session on every command: keep that one and make the transaction numbers new
                    st.pop("lsid")
                    if "txnNumber" in st:
                        st["txnNumber"] = {"$numberLong": str(base + int(st["txnNumber"]["$numberLong"]))}
                cmd = json_util.loads(json.dumps(st))
                try:
                    r = target.command(cmd)
                    rec = norm(r, opts.get("__strip", []))
                    if opts.get("__sortarr"):
                        rec = sortarr(rec, opts["__sortarr"])
                    if opts.get("__shape"):
                        rec = shape(rec)
                except OperationFailure as e:
                    rec = {"err": [e.code, e.details.get("codeName"), unify(e.details.get("errmsg") or "")]}
                except PyMongoError as e:
                    if not opts.get("__expect_close"):
                        raise
                    rec = {"connection": type(e).__name__}   # (`shutdown`: the server closes the connection without a reply)
                if not opts.get("__skip"):
                    out[opts.get("__as") or f"s{i}"] = rec
            return out
        finally:
            try:
                client.drop_database(other.name)
            except PyMongoError:
                pass
    return f


def sortarr(rec, path):
    """Sort the array at the dotted path (by name, else by its JSON text): listCollections and distinct order is a hash order on the real server."""
    node = rec
    keys = path.split(".")
    for k in keys[:-1]:
        node = node[k]
    arr = node[keys[-1]]
    arr.sort(key=lambda x: x.get("name", "") if isinstance(x, dict) and "name" in x else json.dumps(x, sort_keys=True, default=str))
    return rec


_START = int(time.time() * 1000) * 1000
_N = [0]
for _path in sorted(glob.glob(os.path.join(HERE, "scripts", "*.json"))):
    for _c in json.load(open(_path)):
        _N[0] += 1
        _c["_base"] = _START + _N[0] * 1000
        mode = _c.get("mode", "std")
        case(_c["name"], rs=(mode == "rs"), sa=(mode == "sa"), fresh=(mode.split(":")[1] if mode.startswith("fresh:") else None), sort_keys=bool(_c.get("sort_keys")))(make(_c))
