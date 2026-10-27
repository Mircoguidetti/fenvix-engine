#!/usr/bin/env python3
"""Quality gate, part 1: recount every category straight from the docs index pages with a different, simpler method than build_inventory.py
(raw line counts of table rows / list items / heading) and compare with the number of rows in inventory_base.csv. Writes compat/verify_counts.md."""
import csv, json, os, re, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch import fetch
HERE = os.path.dirname(os.path.abspath(__file__))
rows = list(csv.DictReader(open(os.path.join(HERE, "..", "inventory_base.csv"))))
D = "https://www.mongodb.com/docs/v7.0/"
R = D + "reference/"


def inv(prefix=None, cat=None):
    return [r for r in rows if (prefix and r["id"].startswith(prefix)) or (cat and r["category"].startswith(cat))]


def table_names(url):
    """unique first-column backticked names of every table row in a docs page (independent of mdparse.py)."""
    t = fetch(url) or ""
    out = []
    for line in t.splitlines():
        m = re.match(r"^\|\s*\[?`([^`|]+)`", line)
        if m and not line.startswith("| ---"):
            out.append(m.group(1))
    return out


out = []


def check(label, docs_count, inv_count, note="", allow=0):
    """allow: documented, explained difference (docs_count - inv_count)."""
    ok = "OK" if docs_count == inv_count else ("OK (explained)" if docs_count - inv_count == allow else "DIFF")
    out.append((label, docs_count, inv_count, ok, note))


# commands
cmd_docs = set(table_names(R + "command.md")) | set(table_names(R + "mql/crud-commands.md"))
LC = json.load(open(os.path.join(HERE, "..", "listCommands-mongo-7.0.json")))["commands"]
inv_cmds = {r["name"] for r in inv("cmd:")}
check("commands: docs index tables (command.md + crud-commands.md) all present", len(cmd_docs), len(cmd_docs & inv_cmds), "names in the docs tables that have an inventory row")
check("commands: server listCommands (mongo 7.0.45) all present", len(LC), len(set(LC) & inv_cmds), "every command the real server lists has a row")
extra = inv_cmds - set(LC) - cmd_docs
check("commands: rows not in either source", 0, len(extra), "should be 0: " + ",".join(sorted(extra)[:5]))
# operator families: raw table-row counts per landing page
QP = R + "mql/query-predicates/"
qn = sum(len(table_names(QP + s + ".md")) for s in "arrays bitwise comparison data-type misc logical geospatial".split())
check("query operators (7 sub-tables) + $text", qn + 1, len(inv("qop:")), "$text is absent from the docs tables but its page exists (added)")
check("projection operators (+1 row: expressions in projection)", len(table_names(R + "mql/projection.md")) + 1, len(inv("proj:")))
upd = table_names(R + "mql/update.md")
check("update operators (landing page tables; $[] and $[<identifier>] included)", len(upd), len(inv("upd:")))
stg = [n for n in table_names(R + "mql/aggregation-stages.md") if n.startswith("$")]
check("aggregation stages (landing page, $-names; the two non-$ rows of that page are the commands findAndModify/update and are not stages)", len(stg), len(inv("stage:")), "the Atlas stages $search/$searchMeta/$listSearchIndexes/$vectorSearch are in the docs table")
ex = table_names(R + "mql/expressions.md")
check("aggregation expressions (unique names; +$unsetField)", len(set(ex)) + 1, len(inv("expr:")), f"{len(ex)} table rows, {len(ex)-len(set(ex))} names listed under two headings")
check("accumulators", len(set(table_names(R + "mql/accumulators.md"))), len(inv("acc:")))
check("window functions", len(set(table_names(R + "mql/window-functions.md"))), len(inv("win:")))
# bson
bt = fetch(R + "bson-types.md")
check("bson types (table rows with a type number and alias)", len(re.findall(r"^\|[^|]+\|\s*-?\d+\s*\|\s*\"\w+\"", bt, re.M)), len(inv("bson:")))
check("bson binData subtypes (table 'Number | Subtype')", len(re.findall(r"^\|\s*\d+\s*\|\s*[A-Za-z]", bt.split("## Binary Data")[1].split("## ObjectId")[0], re.M)), len(inv("binsub:")))
co = fetch(R + "bson-type-comparison-order.md")
check("bson comparison order (numbered list of types)", len(re.findall(r"^\d+\. ", co.split("Aggregation expressions make")[0], re.M)), len(inv("bsonord:")))
# collation locales: independent recount of locale codes
loc = fetch(R + "collation-locales-defaults.md")
line = next(l for l in loc.splitlines() if l.startswith("| LanguageLocaleVariants"))
codes = re.findall(r"`([^`]+)`", line)
check("collation locales + variants (every backticked code in the table is a locale or a variant)", len(codes), len(inv("locale:")), "locale `bn` appears twice in the docs table (Bengali; Bengali with variant traditional): counted once", allow=1)
# change events
check("change stream event types (table rows)", len(table_names(R + "change-events.md")), len(inv("cse:")))
check("read concern levels", len(re.findall(r'^\| \[`"(?:local|available|majority|linearizable|snapshot)"`\]', fetch(R + "read-concern.md"), re.M)), len([r for r in inv("rc:") if r["name"].startswith("level")]))
check("read preference modes", len(re.findall(r"^\| \[`(?:primary|primaryPreferred|secondary|secondaryPreferred|nearest)`\]", fetch(D + "core/read-preference.md"), re.M)), len([r for r in inv("rp:") if r["name"] in ("primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest")]))
check("index types (H2 headings on core/indexes/index-types.md; geospatial = 2 rows, + compound wildcard)", len(re.findall(r"^## ", fetch(D + "core/indexes/index-types.md"), re.M)), len(inv("idxtype:")) - 2, "8 headings; geospatial split into 2d/2dsphere (+1), compound wildcard added (+1): 10 rows")
check("index properties (H2 headings on core/indexes/index-properties.md)", len(re.findall(r"^## ", fetch(D + "core/indexes/index-properties.md"), re.M)), len(inv("idxprop:")))
# drivers
PY = "https://pymongo.readthedocs.io/en/stable/api/"
for page, label in [("pymongo/mongo_client.html", "MongoClient"), ("pymongo/database.html", "Database"), ("pymongo/collection.html", "Collection"), ("pymongo/cursor.html", "Cursor"),
                    ("pymongo/command_cursor.html", "CommandCursor"), ("pymongo/change_stream.html", "ChangeStream"), ("pymongo/client_session.html", "ClientSession"),
                    ("pymongo/operations.html", "operations"), ("gridfs/index.html", "GridFS"), ("gridfs/grid_file.html", "grid_file"), ("pymongo/encryption.html", "encryption")]:
    t = fetch(PY + page)
    n = len(set(re.findall(r'<dt class="sig sig-object py" id="([^"]+)"', t)))
    check(f"pymongo {label}: documented API entries (html anchors)", n, len([r for r in rows if r["category"] == f"driver api pymongo / {label}"]))
dts = open(os.path.join(HERE, "..", "..", "mongo-tests", "node", "node_modules", "mongodb", "mongodb.d.ts")).read()
for c in "MongoClient Db Collection AbstractCursor FindCursor AggregationCursor ListIndexesCursor ListCollectionsCursor ChangeStream ClientSession Admin GridFSBucket GridFSBucketReadStream GridFSBucketWriteStream BulkOperationBase OrderedBulkOperation UnorderedBulkOperation ClientEncryption".split():
    t = fetch(f"https://mongodb.github.io/node-mongodb-native/7.7/classes/{c}.html") or ""
    anchors = re.findall(r'<section class="tsd-panel tsd-member[^"]*">\s*<a id="([^"]+)"', t)
    m = re.search(r"^export declare (?:abstract )?class " + c + r"\b[^\n]*\{\n(.*?)^\}", dts, re.S | re.M)
    body = m.group(1) if m else ""
    own = {re.sub(r"-\d+$", "", a) for a in anchors if re.search(r"^    (?:static |readonly |protected |abstract |get |set )*" + re.escape(re.sub(r"-\d+$", "", a)) + r"\??[(:<]", body, re.M) or re.search(r"^    (?:static )?(?:readonly )?" + re.escape(re.sub(r"-\d+$", "", a)) + r" =", body, re.M)}
    got = len([r for r in rows if r["category"] == f"driver api node / {c}"])
    check(f"node {c}: members documented in typedoc AND declared by the class in mongodb.d.ts (anchors incl. -N overloads: {len([a for a in anchors if re.sub(chr(45)+'[0-9]+$','',a) in own])})", len([a for a in anchors if re.sub(r"-\d+$", "", a) in own]), got)
with open(os.path.join(HERE, "..", "verify_counts.md"), "w") as f:
    f.write("| check | docs count | inventory rows | result | note |\n|---|---:|---:|---|---|\n")
    for l, a, b, ok, n in out:
        f.write(f"| {l} | {a} | {b} | {ok} | {n} |\n")
bad = [o for o in out if o[3] == "DIFF"]
print(f"{len(out)} checks, {len(bad)} differ")
for o in bad: print(o)
