#!/usr/bin/env python3
"""Step 2: fill engine_status / evidence / differential_test for every row of inventory_base.csv and write compat/inventory.csv.

Inputs (all produced by this repo's own tooling, nothing is guessed):
  mongo-tests/tests.json                       the differential suite (operation scripts run against mongo:7.0 and the engine)
  compat/results/py_main.json, py_retry.json   pymongo harness results  (retry = failures re-run with a correctly configured real mongod)
  compat/results/node_core.json, node_replset.json   Node.js harness results
  compat/probe_results.json                    one-request probes of name recognition on the engine and on real mongod (probe_engine.py)
  compat/probe_diff_results.json               extra differential probes written for this audit (probe_diff.py), same pass/fail rule
  compat/driver_map.py                         driver member -> server commands / client-only
Rules: covered = >=1 passing differential test exercising the row and none failing; partial = passing tests plus a failing test or a
documented limitation in the engine source; missing = the engine rejects it (probe or failing test) ; untested = the engine
recognises it but no differential test exercises it.
"""
import csv, json, os, re, sys, collections
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from features import features_of_test
import driver_map

C = os.path.join(HERE, "..")
inv = list(csv.DictReader(open(os.path.join(C, "inventory_base.csv"))))
known = {r["id"] for r in inv}
tests = json.load(open(os.path.join(C, "..", "mongo-tests", "tests.json")))
TESTS = {t["name"]: t for t in tests}


def load(p):
    p = os.path.join(C, "results", p)
    return {x["name"]: x for x in json.load(open(p))} if os.path.exists(p) else {}


py = load("py_main.json"); py.update(load("py_retry.json"))
node = {}
for f in ("node_core.json", "node_replset.json", "node_retry.json"):
    node.update(load(f))
probes = json.load(open(os.path.join(C, "probe_results.json"))) if os.path.exists(os.path.join(C, "probe_results.json")) else {}
pdiff = {}
for _f in ("probe_diff_results.json", "probe_node_results.json"):
    if os.path.exists(os.path.join(C, _f)):
        for _k, _v in json.load(open(os.path.join(C, _f))).get("by_id", {}).items():
            pdiff.setdefault(_k, []).extend(_v)

# A failing differential test is held against a row only when it is listed here (or, for driver rows, when the failing step's op is the member's op):
# a test usually exercises many operators in setup and only one of them can be the cause. Every failing test must be attributed or is listed
# as unattributed in the final report.
FAIL_ATTR = {
    "lookup.extra_let_pipeline_form": ["stage:$lookup"],           # $$var bound to a missing field compared with $eq matches null documents (real: missing only)
    "cs.ttl_delete_event": ["cse:delete"],                         # TTL expiry produced no delete event within the test's 3 s window
}

_unknown = sorted(k for k in pdiff if k not in known)
if _unknown:
    sys.exit("audit probes reference ids that are not in the inventory: " + ", ".join(_unknown))

# tests whose comparison cannot pass by construction (two wall-clock reads) or are known flaky: not counted as pass or fail
INCONCLUSIVE = {"update.extra.current_date": "compares $currentDate values read ~10 ms apart on two servers (cannot pass by construction)"}


def tstat(name):
    """pass / fail / None for one suite test across both harnesses: fail if either harness fails it, pass if some harness passes it."""
    s = [x[name]["status"] for x in (py, node) if name in x]
    if name in INCONCLUSIVE or not s:
        return None
    return "fail" if any(v != "pass" for v in s) else "pass"


IDX = collections.defaultdict(list)       # feature id -> [test names]
for t in tests:
    for f in features_of_test(t, known):
        IDX[f].append(t["name"])

# rows whose evidence is a family of existing suite tests that the operator walker cannot see (change-stream events, cursor behaviour, ...).
# values are exact test names or "re:<regex>" patterns over test names.
TESTMAP = {
    "cso:$changeStream.resumeAfter": ["cs.resume_after_token", "cs.resume_with_match"],
    "cso:$changeStream.startAfter": ["cs.start_after_token"],
    "cs:resume token": ["cs.resume_after_token", "cs.start_after_token", "cs.resume_with_match"],
    "cs:watch a collection": ["cs.insert", "cs.update_set", "cs.delete"],
    "cs:watch a database": ["cs.database_level_sees_all_collections", "cs.database_level_drop_event"],
    "cse:insert": ["cs.insert"], "cse:update": ["cs.update_set"], "cse:replace": ["cs.replace"], "cse:delete": ["cs.delete"],
    "cse:invalidate": ["cs.collection_drop_then_invalidate"],
    "cursor:batchSize": ["cursor.batch_size_small", "cursor.paging_1000"], "cursor:firstBatch": ["cursor.default_first_batch_101"],
    "cursor:nextBatch": ["cursor.paging_1000", "cursor.batch_size_small"],
    "txn:abortTransaction": ["txn.abort_discards_inserts", "txn.update_in_txn_abort", "txn.delete_in_txn_abort"],
    "opt:commitTransaction.txnNumber": ["txn.commit_makes_inserts_visible", "txn.update_in_txn_commit"],
    "opt:commitTransaction.autocommit": ["txn.commit_makes_inserts_visible", "txn.update_in_txn_commit"],
    "opt:abortTransaction.txnNumber": ["txn.abort_discards_inserts", "txn.update_in_txn_abort"],
    "opt:abortTransaction.autocommit": ["txn.abort_discards_inserts", "txn.update_in_txn_abort"],
    "wire:OP_MSG": ["admin.ping", "admin.buildinfo", "admin.list_databases"],
    "wire:OP_REPLY": ["admin.legacy_op_query_handshake"],
    "wire:OP_QUERY": ["admin.legacy_op_query_handshake"],
    "idxtype:multikey index": ["re:multikey"],
    "opt:hello.client": ["admin.hello", "admin.ping"],
}
for rid, pats in TESTMAP.items():
    for pt in pats:
        hit = [n for n in TESTS if (re.search(pt[3:], n) if pt.startswith("re:") else n == pt)]
        for n in hit:
            if n not in IDX[rid]:
                IDX[rid].append(n)


def pick_names(names, key, n=3):
    k = re.sub(r"[^a-z0-9]", "", key.lower())
    def score(nm):
        l = re.sub(r"[^a-z0-9]", "", nm.lower())
        return (("error" in nm or "err" in nm.split("_")[-1]), k not in l, len(nm))
    return sorted(names, key=score)[:n]


def harness_of(name):
    return "+".join(h for h, x in (("py", py), ("node", node)) if name in x and x[name]["status"] == "pass")


# ------------------------------------------------------------------------------ documented limitations of the engine (cited from source)
LIMITS = {
    "stage:$out": "engine rejects writing to another database ('writing to another database is not supported by this server', cmd_data.rs)",
    "stage:$merge": "engine rejects writing to another database ('writing to another database is not supported by this server', cmd_data.rs)",
    "stage:$bucketAuto": "granularity limited to POWERSOF2 and 1-2-5 (agg_more.rs); other ICU/R/E series rejected",
    "cmd:renameCollection": "renameCollection across databases returns NotImplemented (cmd_index.rs)",
    "wire:OP_COMPRESSED": "compression is not advertised in hello; OP_COMPRESSED frames are rejected (wire.rs); clients fall back to uncompressed",
}

rows, BYID = [], {}
EXCL_NOTE = {}


def generic(r):
    rid, kind, name = r["id"], r["id"].split(":", 1)[0], r["name"]
    names = IDX.get(rid, [])
    P = [n for n in names if tstat(n) == "pass"]
    F = [n for n in names if tstat(n) == "fail" and rid in FAIL_ATTR.get(n, [])]
    PP = [e["name"] for e in pdiff.get(rid, []) if e["status"] == "pass"]
    FP = [e for e in pdiff.get(rid, []) if e["status"] == "fail"]
    pr = probes.get(rid, {})
    fail_missing = any(e["kind"] in ("reject", "ignored") or e.get("fail") == "missing" for e in FP)
    good = P or PP
    if good and not F and not FP and rid not in LIMITS:
        status = "covered"
    elif good and (F or FP or rid in LIMITS):
        status = "partial"
    elif (F or FP) and not good:
        status = "missing" if (pr.get("ours") == "unknown" or (FP and fail_missing and not F)) else "partial"
    elif pr.get("ours") == "unknown" and pr.get("real") != "unknown":
        status = "missing"
    else:
        status = "untested"
    cite = pick_names(P, name)
    bits = []
    if cite:
        bits.append("passing differential tests: " + ", ".join(f"{n} [{harness_of(n)}]" for n in cite) + (f" (+{len(P)-len(cite)} more)" if len(P) > len(cite) else ""))
    if PP:
        bits.append("passing audit probes (compat/probes): " + ", ".join(PP[:3]) + (f" (+{len(PP)-3} more)" if len(PP) > 3 else ""))
    if F:
        bits.append("failing differential tests: " + ", ".join(F[:3]))
    if FP:
        bits.append("failing audit probes (compat/probes): " + ", ".join(f"{e['name']} [{e['kind']}]" for e in FP[:3]) + (f" (+{len(FP)-3} more)" if len(FP) > 3 else ""))
    if rid in LIMITS:
        bits.append("known difference: " + LIMITS[rid])
    if not names and not pdiff.get(rid):
        if pr.get("ours") == "unknown" and pr.get("real") != "unknown":
            bits.append("engine rejects it (probe: " + pr.get("ours_msg", "")[:100] + ")")
        elif pr.get("ours") == "unknown":
            bits.append("probe not valid for this name (real mongod also reports it unknown here)")
        elif pr:
            bits.append("engine accepts the request (probe) but no differential test exercises it")
        else:
            bits.append("no differential test and no probe")
    return status, "; ".join(bits), bool(P or F or PP or FP)


for r in inv:
    if r["id"].split(":", 1)[0] in ("py", "node"):
        continue
    r["engine_status"], r["evidence"], dt = generic(r)
    r["differential_test"] = "yes" if dt else "no"
    BYID[r["id"]] = r

RANK = {"missing": 0, "partial": 1, "untested": 2, "covered": 3}
for r in inv:
    if r["id"].split(":", 1)[0] not in ("py", "node"):
        continue
    side = "py" if r["id"].startswith("py:") else "node"
    res = py if side == "py" else node
    kind, deps = driver_map.classify(r)
    if kind.startswith("excluded:"):
        r["driver_reachable"], r["exclusion_proposed"], r["exclusion_reason"] = "yes", "yes", kind[9:]
        r["engine_status"], r["evidence"], r["differential_test"] = "untested", "no differential test; no engine surface (see exclusion_reason)", "no"
    elif kind == "client":
        r["exclusion_proposed"], r["exclusion_reason"] = "yes", "client-side only: the driver sends no request of its own for this member, so there is nothing for the engine to implement"
        r["engine_status"], r["evidence"], r["differential_test"] = "untested", "client-side only; not exercised against the server", "no"
    else:
        hits = driver_map.harness_hits(r, tests, side)
        P = [n for n in hits if n in res and res[n]["status"] == "pass"]
        mk = f"{driver_map.split_member(r)[0]}.{driver_map.canon(driver_map.split_member(r)[1])}"
        my_ops = {op for op, ms in driver_map.HARNESS.items() if mk in ms}
        F = []       # suite failures are attributed to server features (FAIL_ATTR), never to driver members; driver failures come from the audit probes
        dstat = [(d, BYID[d]["engine_status"] if d in BYID else "untested") for d in deps]
        miss = [d for d, st in dstat if st == "missing"]
        bits = []
        if P: bits.append(f"{side} harness calls it in passing differential tests: " + ", ".join(pick_names(P, r['name'].split('.')[-1])) + (f" (+{len(P)-3} more)" if len(P) > 3 else ""))
        if F: bits.append("failing: " + ", ".join(F[:3]))
        if deps: bits.append("server dependencies: " + ", ".join(f"{d}={st}" for d, st in dstat))
        if P and not F and not miss: st = "covered"
        elif P and (F or miss): st = "partial"
        elif F: st = "partial"
        elif miss and len(miss) == len(deps): st = "missing"
        elif miss: st = "partial"
        else: st = "untested"
        if not P and not F: bits.insert(0, f"not called by the {side} differential harness")
        PPd = [e for e in pdiff.get(r["id"], []) if e["status"] == "pass"]
        FPd = [e for e in pdiff.get(r["id"], []) if e["status"] == "fail"]
        if PPd: bits.append("passing audit probes (compat/probes): " + ", ".join(e["name"] for e in PPd[:3]))
        if FPd: bits.append("failing audit probes (compat/probes): " + ", ".join(f"{e['name']} [{e['kind']}]" for e in FPd[:3]))
        if (PPd or P) and not F and not FPd and not miss:
            st = "covered"
        elif (PPd or P) and (F or FPd or miss):
            st = "partial"
        elif FPd and not (P or PPd):
            st = "missing" if any(e["kind"] in ("reject", "ignored") or e.get("fail") == "missing" for e in FPd) else "partial"
        if deps and all(d in BYID and BYID[d]["exclusion_proposed"] == "yes" for d in deps) and r["exclusion_proposed"] != "yes":
            r["exclusion_proposed"] = "yes"
            r["exclusion_reason"] = "calls only commands/stages that are themselves proposed exclusions (" + "; ".join(sorted({BYID[d]["exclusion_reason"].split(" (")[0].split(";")[0] for d in deps})) + ")"
        r["engine_status"], r["evidence"], r["differential_test"] = st, "; ".join(bits), "yes" if (P or F or PPd or FPd) else "no"
    rows.append(r)
rows = [BYID[r["id"]] if r["id"] in BYID else r for r in inv]

# the docs list some operators twice (e.g. $accumulator under "Custom Aggregation Operators" and under accumulators); the engine implements one
ALIAS = {"expr:$accumulator": "acc:$accumulator"}
for r in rows:
    a = ALIAS.get(r["id"])
    if a and a in BYID:
        r["engine_status"] = BYID[a]["engine_status"]
        r["evidence"] = f"same operator as {a} (the docs list it in two places); {BYID[a]['evidence']}"
        r["differential_test"] = BYID[a]["differential_test"]

# per-row overrides decided by hand after reading the source/tests: id -> (status, evidence); see overrides.csv
ov_path = os.path.join(C, "overrides.csv")
if os.path.exists(ov_path):
    ov = {x["id"]: x for x in csv.DictReader(open(ov_path))}
    for r in rows:
        if r["id"] in ov:
            o = ov[r["id"]]
            r["engine_status"] = o["engine_status"]
            r["evidence"] = o["evidence"] if not o.get("append") else r["evidence"] + "; " + o["evidence"]
            if o.get("differential_test"):
                r["differential_test"] = o["differential_test"]

cols = "id category name docs_url driver_reachable exclusion_proposed exclusion_reason engine_status evidence differential_test".split()
with open(os.path.join(C, "inventory.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
c = collections.Counter(r["engine_status"] for r in rows)
print(len(rows), dict(c))
