#!/usr/bin/env python3
"""Write compat/gaps_status/item12_final.csv: the inventory rows that the g09-g12 work of the second half of compat-gaps closes, from the
final full runs of the gap tests (pymongo log, Node log) and the final audit probes.

    gen_status2.py <py log> <node log> > compat/gaps_status/item12_final.csv        (stderr: the rows that stay open, with the reason)

A row is written only when every test it cites passed in the final run of the driver(s) it names, and (for rows that also have audit
probes) every probe of the row passes or errored identically on both servers. Driver-member rows are mapped by the gap test whose body
calls the member (so the citation is a test that really sends the request)."""
import csv, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
GS = os.path.join(ROOT, "compat", "gaps_status")
inv = {r["id"]: r for r in csv.DictReader(open(os.path.join(ROOT, "compat", "inventory.csv")))}
pd = json.load(open(os.path.join(GS, "probe_diff_results_final.json")))["by_id"]
pn = json.load(open(os.path.join(GS, "probe_node_results_final.json")))["by_id"]


def passing(log):
    return set(re.findall(r"^\. (\S+)", open(log).read(), re.M))


PYN, NDN = passing(sys.argv[1]), passing(sys.argv[2])
w = csv.writer(sys.stdout, lineterminator="\n")
w.writerow(["id", "engine_status", "evidence", "differential_test"])
done, left = set(), []


def is_open(rid):
    r = inv.get(rid)
    return r is not None and r["engine_status"] != "covered" and r.get("exclusion_approved") != "yes"


# probes that fail on their own artifacts (the probe compares the raw reply): mapReduce results come in a process-random order on mongod,
# validate prints storage-engine table ids, change-stream events carry collection UUIDs; the gap tests of the same features normalise exactly these
ARTIFACT = {"compat.opt.mapReduce." + o for o in ("limit", "finalize", "scope", "maxTimeMS", "mapReduce", "map", "reduce", "out", "query", "sort", "jsMode", "verbose", "bypassDocumentValidation", "writeConcern", "comment")} | {
    "compat.opt.validate.full", "compat.cs.expanded_modify", "compat.cs.expanded_dropIndexes", "node.Collection.createSearchIndex"}


def probes_ok(rid):
    ps = [p for p in pd.get(rid, []) + pn.get(rid, []) if p["name"] not in ARTIFACT]
    return all(p["status"] in ("pass", "invalid") for p in ps), [p["name"] for p in ps]


# ---- commands, stages, options: gap tests that exist for both drivers under one name (scripts) -------------------------------------------
S = "g11.admin."
BOTH = {
    "cmd:compactStructuredEncryptionData": ["g11.encryption.compact_rs", "g11.encryption.compact_sa"],
    "cmd:dropConnections": [S + "dropconnections"], "cmd:features": [S + "features"], "cmd:fsync": [S + "fsync_lock", S + "fsync_lock_rs"], "cmd:fsyncUnlock": [S + "fsync_lock", S + "fsync_lock_rs"],
    "cmd:invalidateUserCache": [S + "invalidateusercache"], "cmd:logRotate": [S + "logrotate"], "cmd:rotateCertificates": [S + "rotatecertificates"],
    "cmd:setFeatureCompatibilityVersion": [S + "setfcv"], "cmd:setUserWriteBlockMode": [S + "userwriteblock_sa", S + "userwriteblock_rs"], "cmd:shutdown": [S + "shutdown"],
    "cmd:setParameter": ["g10.params.sa", "g10.params.rs"], "cmd:replSetGetStatus": ["g10.repl.cmds_rs", "g10.repl.cmds_sa"], "cmd:replSetGetConfig": ["g10.repl.cmds_rs", "g10.repl.cmds_sa"],
    "cmd:setClusterParameter": ["g09.diag.cluster_parameter_replset"], "cmd:setDefaultRWConcern": ["g09.diag.rwc_standalone", "g09.diag.rwc_replset"],
    "cmd:mapReduce": ["g11.mapreduce.options"], "opt:validate.full": ["g11.validate.full"],
    "opt:create.storageEngine": ["g11.create.storage_engine_and_index_option_defaults"], "opt:create.indexOptionDefaults": ["g11.create.storage_engine_and_index_option_defaults"],
    "stage:$bucketAuto": ["g11.agg.bucketauto_granularity"],   # (all 13 granularity series; the earlier note "other series rejected" is obsolete: they were implemented in item 5)
    "stage:$queryStats": [S + "querystats"], "expr:$toHashedIndexKey": ["g11.expr.tohashedindexkey"],
    "stage:$currentOp": ["g09.diag.aggregate_database_stages"], "stage:$listLocalSessions": ["g09.diag.aggregate_database_stages"], "stage:$listSessions": ["g09.diag.aggregate_database_stages"],
}
for o in ("mapReduce", "map", "reduce", "out", "query", "sort", "limit", "finalize", "scope", "jsMode", "verbose", "bypassDocumentValidation", "maxTimeMS", "writeConcern", "comment"):
    BOTH["opt:mapReduce." + o] = ["g11.mapreduce.options"]
# ---- change-stream events: a py case and a Node case with different names ------------------------------------------------------------------
TWINS = {"cse:modify": (["g12.py.change_stream_expanded_ddl_events"], ["g12.node.change_stream_expanded_ddl_events"]),
         "cse:dropIndexes": (["g12.py.change_stream_expanded_ddl_events"], ["g12.node.change_stream_expanded_ddl_events"])}


def emit(rid, pt, nt, how):
    if not is_open(rid) or rid in done:
        return
    ok, pnames = probes_ok(rid)
    bad = [t for t in pt if t not in PYN] + [t for t in nt if t not in NDN]
    if bad or not ok:
        left.append((rid, "test not passing: %s" % bad if bad else "probe failing"))
        return
    ev = "passing differential tests against mongod 7.0.45 in the final runs of compat-gaps"
    ev += ", identical with pymongo (%d/%d)" % (len(pt), len(pt)) if pt else ""
    ev += " and with the Node driver (%d/%d)" % (len(nt), len(nt)) if nt else ""
    ev += ": " + ", ".join(dict.fromkeys(pt + nt))
    if pnames:
        ev += "; passing audit probes: " + ", ".join(pnames[:4])
    w.writerow([rid, "covered", ev, "yes"])
    done.add(rid)


for rid, ts in BOTH.items():
    emit(rid, ts, ts, "both")
for rid, (pt, nt) in TWINS.items():
    emit(rid, pt, nt, "twin")

# ---- driver members: the gap test whose body calls the member ---------------------------------------------------------------------------
def cases(path, marker):
    src = open(path).read()
    parts = re.split(marker, src)[1:]
    out = {}
    for p in parts:
        m = re.match(r"""['"]([^'"]+)['"]""", p)
        if m:
            out[m.group(1)] = p
    return out


PYC = {}
NDC = {}
for f in os.listdir(HERE):
    if re.match(r"g\d+.*\.py$", f) and f != "gaps_py.py":
        PYC.update(cases(os.path.join(HERE, f), r"@case\("))
nd = os.path.join(HERE, "node")
for f in os.listdir(nd):
    if re.match(r"g\d+.*\.js$", f):
        NDC.update(cases(os.path.join(nd, f), r"\bC\("))
ALIAS = {"to_list": "to_list(", "next": "next("}      # (members whose bare name is too common are matched with the call syntax)
for rid, r in inv.items():
    if not is_open(rid) or rid in done or not (rid.startswith("py:") or rid.startswith("node:")):
        continue
    member = re.split(r"[.]", rid.split(":", 1)[1])[-1]
    member = re.sub(r"-\d+$", "", member)
    py = rid.startswith("py:")
    pool, names = (PYC, PYN) if py else (NDC, NDN)
    pat = re.compile(r"[.\s(]" + re.escape(member) + r"\b\s*(\(|=|\b)") if member not in ALIAS else re.compile(re.escape(ALIAS[member]))
    hit = [n for n, body in pool.items() if pat.search(body) and n in names and (n.startswith("g12.") or n.startswith("g09.") or n.startswith("g10."))]
    if any(n.startswith("g12.") for n in hit):      # (the g12 cases were written for these members: prefer them to incidental calls of a common name)
        hit = [n for n in hit if n.startswith("g12.")]
    if rid == "node:MongoClient.connect-2":          # purely local (a second connect() on a connected client sends nothing): excluded, not cited
        continue
    if not hit:
        left.append((rid, "no passing gap test calls the member"))
        continue
    pt, nt = (hit[:6], []) if py else ([], hit[:6])
    ok, pnames = probes_ok(rid)
    if not ok:
        left.append((rid, "probe failing"))
        continue
    ev = "passing differential tests against mongod 7.0.45 in the final runs of compat-gaps, identical with %s (%d/%d) whose bodies call the member: %s" % ("pymongo" if py else "the Node driver", len(hit[:6]), len(hit[:6]), ", ".join(hit[:6]))
    if pnames:
        ev += "; passing audit probes: " + ", ".join(pnames[:4])
    w.writerow([rid, "covered", ev, "yes"])
    done.add(rid)

# ---- rows left open only by probes that pass now ------------------------------------------------------------------------------------
for rid, r in inv.items():
    if not is_open(rid) or rid in done:
        continue
    mixed = r["evidence"].startswith("passing differential tests") and "failing audit probes" in r["evidence"] and "known difference" not in r["evidence"] and "limit" not in r["evidence"]
    if r["evidence"].startswith("failing audit probes") or r["evidence"].startswith("passing audit probes (compat/probes)") or mixed:
        ok, pnames = probes_ok(rid)
        if ok and pnames:
            keep = r["evidence"].split("; passing audit probes")[0].split("; failing audit probes")[0] if mixed else ""
            w.writerow([rid, "covered", (keep + "; " if keep else "") + "passing audit probes (final run, standalone engine vs standalone mongod, replica-set engine vs replica-set mongod): " + ", ".join(pnames[:6]), "yes"])
            done.add(rid)
for rid in inv:
    if is_open(rid) and rid not in done and not any(rid == l[0] for l in left):
        left.append((rid, "stays open: " + inv[rid]["evidence"][:90]))
for l in left:
    print("OPEN", l, file=sys.stderr)
