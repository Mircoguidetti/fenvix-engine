"""Fix round of compaction 2 step 2: are the change-stream differences of a diff-suite run (customer keys on) ONLY the
customer field that `documentKey` now carries before `_id` (as a sharded mongod writes the shard key)? The reference
of the diff suites is an unsharded mongod, whose `documentKey` is `{_id}`.

For every failing `cs.*` test: drop every field but `_id` from each of our events' `documentKey`, compare again with
mongod's answer. A second kind is counted apart: an `updateLookup` update that changed the customer field itself (the
document moved to another customer). Ours then names the customer before the write and the lookup finds none (null), as
mongos 7.0.34 does for a shard key update (MEASURED); the unsharded reference returns the document. Usage: doc_key_only.py diff-py.json [diff-node.json ...]. Exit 1 if any other difference remains.
"""
import json
import sys


def strip(v):
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            if k == "documentKey" and isinstance(x, dict) and "_id" in x:
                out[k] = {"_id": strip(x["_id"])}
            else:
                out[k] = strip(x)
        return out
    if isinstance(v, list):
        return [strip(x) for x in v]
    return v


def load(s):
    try:
        return json.loads(s) if isinstance(s, str) else s
    except ValueError:
        return s


def moved(ours, real):
    """Our events with the lookup of a moved document put back (fullDocument null where our documentKey's customer
    differs from the document's customer after the write), or None if there is no such event."""
    if not (isinstance(ours, list) and isinstance(real, list) and len(ours) == len(real)):
        return None
    out, hit = [], False
    for o, r in zip(ours, real):
        dk = (o.get("documentKey") or {}) if isinstance(o, dict) else {}
        rf = r.get("fullDocument") if isinstance(r, dict) else None
        if isinstance(o, dict) and o.get("operationType") == "update" and o.get("fullDocument", 0) is None and isinstance(rf, dict) and any(k != "_id" and rf.get(k) != v for k, v in dk.items()):
            o = dict(o, fullDocument=rf)
            hit = True
        out.append(o)
    return out if hit else None


DK = __import__("re").compile(r'"documentKey": \{.*?"_id": ')


def text_only(ours, real):
    """The harness keeps 600 characters of each answer, so a long one is not JSON any more: drop the fields before
    `_id` in our documentKeys as text, and compare what both kept."""
    o = DK.sub('"documentKey": {"_id": ', ours)
    n = min(len(o), len(real))
    return o != ours and o[:n] == real[:n] and n > 0


bad = 0
for path in sys.argv[1:]:
    r = json.load(open(path))
    fails = [t for t in r if t["status"] != "pass" and (t.get("name") or t.get("id") or "").startswith("cs.")]
    only, move, other = [], [], []
    for t in fails:
        d = t.get("detail") or {}
        real, ours = load(d.get("real")), load(d.get("ours"))
        name = t.get("name") or t.get("id")
        if real is not None and strip(ours) == strip(real) and ours != real:
            only.append(name)
        elif isinstance(ours, str) and isinstance(real, str) and text_only(ours, real):
            only.append(name + " (truncated: compared as text)")
        elif moved(ours, real) is not None and strip(moved(ours, real)) == strip(real):
            move.append(name)
        else:
            other.append(name)
    print(f"{path}: {len(fails)} change-stream differences; only the customer field in documentKey: {len(only)}; "
          f"that and the lookup of a document moved to another customer (null, as on mongos): {len(move)} {move}; other: {other}")
    bad += len(other)
sys.exit(1 if bad else 0)
