#!/usr/bin/env python3
"""Build the customer-facing compatibility page from the audit data.

    python3 tools/compat-page/build.py            # writes docs/compatibility.md and docs/compatibility.json
    python3 tools/compat-page/build.py --check    # exits 1 if the files on disk differ from what the data produces

Inputs (all read from the repository, nothing is fetched):
    compat/inventory.csv        the rows and their engine status (single source of every number)
    compat/coverage.py          its validate() is run on the CSV; the tests compare our counts with its output
    compat/COVERAGE.md          header only: audit date, reference server, drivers, engine commit
    compat/UNVERIFIED.md        limits of the audit (selected bullets, quoted)
    compat/exclusions_decided.md  groups of rows left out on purpose
    compat-scan.md              usage of features in 10 applications (table of section 3, limits of section 2)
    compat/scan_extra.json      usage of features that table does not measure

The output is a pure function of those files (no clock, no randomness), so building twice gives identical bytes.
The only date that is not in the files is the date the inventory was last committed; it comes from
`git log -1 -- compat/inventory.csv` (never from HEAD or the clock), is null in the JSON and "not available" on the page
when git or the full history is missing (zip, shallow clone), and can be given with --inventory-date. Percentages are truncated to one decimal, never rounded up.

Status mapping (the page's four statuses; "audit_status" keeps the CSV value)
    supported      engine_status == covered and differential_test == yes
    partial        engine_status == partial
    not_tested     engine_status == untested, and engine_status == covered with differential_test == no
                   (covered by specification tests only: no reference server exists, so no comparison test)
    not_supported  engine_status == missing
Rows with exclusion_approved == yes are not counted anywhere (same denominator as compat/coverage.py).

JSON schema (schema_version 1; machine-checked by validate_json() below, which tests/ uses)
    schema_version        int, always 1
    generated             {audit_report_date, inventory_last_changed (or null), inventory_last_commit (or null)}
    engine                {commit, branch, commit_source, describes_current_inventory (bool), note}
    reference_server      {name, version}
    drivers_tested        [{name, version}]
    status_definitions    {supported, partial, not_tested, not_supported}  plain-language text
    summary               {total, supported, partial, not_tested, not_supported, percent_supported}
    audit_rule_covered    {covered, total, percent, specification_only}   what compat/coverage.py calls covered
    areas                 [{id, title, description, counts{total,supported,partial,not_tested,not_supported},
                            percent_supported, categories[]}]
    categories            [{category, total, covered, partial, untested, missing}]   exactly compat/coverage.py --detail
    features              [{id, name, area, category, status, audit_status, note, evidence_id, evidence_tests[]}]
    top_gaps              [{rank, title, area, rows, status_counts{partial,not_tested,not_supported}, row_ids[],
                            usage{measured, repos, of, hits, features[], source}}]
    exclusions            {rows, groups[{group, rows, reason}]}
    limits                [{id, text, source}]
    usage_scan            {repositories[{name, commit}], scan_date, features_ranked, limits[]}
    data_notes            [{id, text}]   disagreements between the source files, reported, not resolved
percent_* values are numbers truncated (floor) to one decimal. features[].evidence_id is the inventory row id: its
`evidence` column in compat/inventory.csv is the proof; evidence_tests lists test/probe names found in that text.
"""
import argparse
import collections
import csv
import importlib.util
import json
import math
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

STATUS_ORDER = ("supported", "partial", "not_tested", "not_supported")
STATUS_LABEL = {"supported": "Supported", "partial": "Partial", "not_tested": "Not tested", "not_supported": "Not supported"}
STATUS_DEFINITIONS = {
    "supported": "A comparison test against real MongoDB 7.0 passes, and none fails: the same request gives the same result on MongoDB and on Fenvix.",
    "partial": "It works, with a difference from MongoDB that is stated next to the feature.",
    "not_tested": "The code exists, but no comparison test against MongoDB exists. This is not a claim that it works.",
    "not_supported": "The engine rejects the feature with an error, or accepts it and ignores it.",
}

AREAS = [
    ("queries", "Queries and operators", "Query operators, projection, update operators, sort and hint, cursors, the find and count family of commands, and server-side JavaScript ($where, $function, $accumulator)."),
    ("aggregation", "Aggregation", "Pipeline stages, expressions, accumulators, window functions, the aggregate command and mapReduce."),
    ("indexes", "Indexes", "Index types and properties, and the commands that create, list and drop indexes, including search-index commands."),
    ("types", "Data types", "BSON types, binary subtypes and the order in which values of different types compare."),
    ("writes", "Writes and bulk operations", "insert, update, delete, findAndModify, bulk writes and write concern."),
    ("transactions", "Transactions and concurrency", "Multi-document transactions, sessions, read concern and read preference."),
    ("changestreams", "Change streams", "Change streams, their options and the events they deliver."),
    ("gridfs", "GridFS", "The GridFS file-storage API of the drivers."),
    ("collation", "Collation and sorting", "Collation options and the collation locales (language-specific sort order)."),
    ("security", "Authentication and security", "Authentication mechanisms, user and role management commands."),
    ("drivers", "Drivers and protocol", "The wire protocol, TLS, compression, Stable API, retryable writes and the members of the pymongo and Node.js driver APIs."),
    ("admin", "Admin commands", "Collection and database management, diagnostics, server administration, plan cache and replica-set status commands."),
]
AREA_TITLE = {k: t for k, t, _ in AREAS}

_TOP = {
    "query operators": "queries", "projection operators": "queries", "update operators": "queries",
    "sort and hint specifications": "queries", "cursors": "queries", "server-side javascript": "queries",
    "aggregation stages": "aggregation", "aggregation expressions": "aggregation", "accumulators": "aggregation",
    "window functions": "aggregation",
    "index types": "indexes", "index properties": "indexes",
    "bson types": "types", "bson binary subtypes": "types", "bson comparison order": "types",
    "bulk write": "writes", "write concern": "writes",
    "transactions": "transactions", "read concern": "transactions", "read preference": "transactions",
    "change streams": "changestreams", "change stream events": "changestreams",
    "gridfs": "gridfs",
    "collation": "collation", "collation locales": "collation",
    "authentication mechanisms": "security",
    "driver protocol": "drivers", "driver api node": "drivers", "driver api pymongo": "drivers",
}
_COMMANDS = {
    "aggregation": "aggregation", "atlas search": "indexes", "authentication": "security",
    "collection and database management": "admin", "crud": "writes", "diagnostics": "admin",
    "handshake and driver utility": "drivers", "index management": "indexes", "query plan cache": "admin",
    "replication": "admin", "server administration": "admin", "sessions": "transactions",
    "transactions": "transactions", "undocumented": "admin", "user and role management": "security",
}
_OPTIONS = {
    "find": "queries", "count": "queries", "distinct": "queries", "explain": "queries", "getMore": "queries",
    "killCursors": "queries",
    "aggregate": "aggregation", "mapReduce": "aggregation",
    "createIndexes": "indexes", "dropIndexes": "indexes", "listIndexes": "indexes",
    "insert": "writes", "update": "writes", "delete": "writes", "findAndModify": "writes",
    "abortTransaction": "transactions", "commitTransaction": "transactions",
    "hello": "drivers",
    "collMod": "admin", "collStats": "admin", "create": "admin", "dbStats": "admin", "drop": "admin",
    "dropDatabase": "admin", "listCollections": "admin", "listDatabases": "admin", "renameCollection": "admin",
    "validate": "admin",
}


def area_of(category):
    """Area id of an inventory category string such as 'commands / crud'. Unknown categories stop the build."""
    top, _, sub = category.partition(" / ")
    table = {"commands": _COMMANDS, "command options": _OPTIONS}.get(top)
    area = (table or _TOP).get(sub if table else top)
    if area is None:
        sys.exit(f"build.py: category {category!r} has no area; add it to AREAS mapping in tools/compat-page/build.py")
    return area


def pct(num, den):
    """Percentage truncated (never rounded up) to one decimal."""
    return math.floor(1000 * num / den) / 10 if den else 0.0


# ---------------------------------------------------------------- inputs

def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        return f.read()


def load_coverage_module():
    spec = importlib.util.spec_from_file_location("compat_coverage", os.path.join(ROOT, "compat", "coverage.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_inventory():
    cov = load_coverage_module()
    rows = cov.load(os.path.join(ROOT, "compat", "inventory.csv"))
    errs = cov.validate(rows)
    if errs:
        sys.exit("build.py: compat/inventory.csv fails compat/coverage.py validation:\n  " + "\n  ".join(errs[:20]))
    return rows


def page_status(row):
    s = row["engine_status"]
    if s == "covered":
        return "supported" if row["differential_test"] == "yes" else "not_tested"
    return {"partial": "partial", "untested": "not_tested", "missing": "not_supported"}[s]


def parse_header(text):
    """COVERAGE.md header: audit date, reference server, engine commit, driver versions, headline numbers."""
    def need(pattern, what):
        m = re.search(pattern, text)
        if not m:
            sys.exit(f"build.py: compat/COVERAGE.md header changed, cannot find {what} (pattern {pattern!r})")
        return m
    d = need(r"Generated by `[^`]+` from `compat/inventory.csv` on (\d{4}-\d{2}-\d{2})", "the audit date")
    ref = need(r"Reference server: `mongo:7\.0` = mongod (\d+\.\d+\.\d+)", "the reference server version")
    eng = need(r"Engine: `mongo-server` built from `([0-9a-f]{7,40})` \(branch `([^`]+)`", "the engine commit")
    py = need(r"pymongo (\d+\.\d+)", "the pymongo version")
    node = need(r"mongodb \(Node\.js\) (\d+\.\d+)", "the Node.js driver version")
    ov = need(r"\*\*(\d+) / (\d+) covered = ([\d.]+)%\*\*", "the overall coverage line")
    ex = re.search(r"The inventory has (\d+) rows; (\d+) are proposed exclusions", text)
    return {"date": d.group(1), "mongod": ref.group(1), "commit": eng.group(1), "branch": eng.group(2),
            "pymongo": py.group(1), "node": node.group(1),
            "covered": int(ov.group(1)), "total": int(ov.group(2)), "percent": float(ov.group(3)),
            "proposed_exclusions": int(ex.group(2)) if ex else None}


def parse_exclusions(text, approved_in_csv):
    m = re.search(r"\| Group \| Rows \| Reason \|\n\|[-| ]+\|\n((?:\|.*\n)+)", text)
    if not m:
        sys.exit("build.py: compat/exclusions_decided.md: the table '| Group | Rows | Reason |' is missing")
    groups, total = [], None
    for line in m.group(1).splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 3:
            sys.exit(f"build.py: compat/exclusions_decided.md: unexpected table row {line!r}")
        if cells[0].startswith("**Total"):
            total = int(cells[1].strip("*"))
        else:
            groups.append({"group": cells[0], "rows": int(cells[1]), "reason": cells[2]})
    if total is None or sum(g["rows"] for g in groups) != total:
        sys.exit("build.py: compat/exclusions_decided.md: group rows do not add up to its Total row")
    if total != approved_in_csv:
        sys.exit(f"build.py: exclusions_decided.md says {total} excluded rows, inventory.csv has {approved_in_csv} with exclusion_approved=yes")
    return {"rows": total, "groups": groups}


def parse_bullets(text):
    """'- **lead** body' bullets of a markdown file -> [(lead, full text)]."""
    out = []
    for line in text.splitlines():
        m = re.match(r"- \*\*(.+?)\*\*(.*)$", line)
        if m:
            out.append((m.group(1), line[2:]))
    return out


def parse_limits(unverified_text):
    bullets = parse_bullets(unverified_text)
    if not bullets:
        sys.exit("build.py: compat/UNVERIFIED.md has no '- **lead** text' bullets any more")

    def bullet(match):
        hit = [b for lead, b in bullets if match in lead]
        if len(hit) != 1:
            sys.exit(f"build.py: compat/UNVERIFIED.md: expected exactly one bullet whose lead contains {match!r}, found {len(hit)}")
        return hit[0]
    m = re.search(r"Replica-set behaviour was tested on single-node replica sets only \([^)]*\)\.", unverified_text)
    if not m:
        sys.exit("build.py: compat/UNVERIFIED.md: the sentence about single-node replica sets is missing")
    return [
        {"id": "replica-set", "text": m.group(0), "source": "compat/UNVERIFIED.md"},
        {"id": "atlas-only", "text": bullet("Atlas-only features"), "source": "compat/UNVERIFIED.md"},
        {"id": "drivers", "text": bullet("Drivers."), "source": "compat/UNVERIFIED.md"},
        {"id": "covered-not-exhaustive", "text": bullet("exhaustively correct"), "source": "compat/UNVERIFIED.md"},
        {"id": "later-versions", "text": bullet("Docs v7.0 pages for later versions"), "source": "compat/UNVERIFIED.md"},
        {"id": "client-only-members", "text": bullet("Client-only driver members"), "source": "compat/UNVERIFIED.md"},
    ]


SCAN_HEADER = "| # | Feature | Kind | Repos using it (application code) | Occurrences (app) | Occurrences (tests only) | Our status |"
SCAN_ROW = re.compile(r"^\| (\d+) \| `(.+?)` \| (.+?) \| (\d+)/(\d+) \| (\d+) \| (\d+) \| (.+?) \|$")


def parse_scan(md_text, extra):
    """compat-scan.md section 3 (ranking), section 1 (repositories), section 2 (limits) plus compat/scan_extra.json."""
    sec = re.search(r"^## 3\. Ranking of all features used in application code\n(.*?)(?=^## )", md_text, re.S | re.M)
    if not sec:
        sys.exit("build.py: compat-scan.md: section '## 3. Ranking of all features used in application code' not found")
    body = sec.group(1)
    lines = body.splitlines()
    if SCAN_HEADER not in lines:
        sys.exit("build.py: compat-scan.md: the section 3 table header changed; expected\n  " + SCAN_HEADER)
    i = lines.index(SCAN_HEADER) + 2
    feats = {}
    while i < len(lines) and lines[i].startswith("|"):
        m = SCAN_ROW.match(lines[i])
        if not m:
            sys.exit(f"build.py: compat-scan.md: cannot parse ranking row {lines[i]!r}")
        rank = int(m.group(1))
        if rank != len(feats) + 1:
            sys.exit(f"build.py: compat-scan.md: ranking rows out of sequence at rank {rank}")
        feats[m.group(2)] = {"kind": m.group(3), "repos": int(m.group(4)), "of": int(m.group(5)),
                             "hits": int(m.group(6)), "hits_tests": int(m.group(7))}
        i += 1
    n = re.search(r"(\d+) features appear in at least one repository", body)
    if not n or int(n.group(1)) != len(feats):
        sys.exit(f"build.py: compat-scan.md says {n.group(1) if n else '?'} ranked features, parsed {len(feats)}")
    sec1 = re.search(r"^## 1\. Repositories\n(.*?)(?=^## )", md_text, re.S | re.M)
    repos = []
    for line in (sec1.group(1).splitlines() if sec1 else []):
        m = re.match(r"^\| ([^|]+?) \| `([0-9a-f]+)` \| scanned \|", line)
        if m:
            repos.append({"name": m.group(1), "commit": m.group(2)})
    if not repos or any(f["of"] != len(repos) for f in feats.values()):
        sys.exit("build.py: compat-scan.md: the repository table does not match the 'N/10' denominators")
    d = re.search(r"Scanned (\d{4}-\d{2}-\d{2})", md_text)
    if not d:
        sys.exit("build.py: compat-scan.md: scan date not found")
    sec2 = re.search(r"^## 2\. What was counted, and how reliable it is\n(.*?)(?=^## )", md_text, re.S | re.M)
    wanted = ("Scope.", "Over-counting:", "Under-counting:", "Same name, two roles.", "Several repositories are libraries or platforms")
    got = {lead: text for lead, text in parse_bullets(sec2.group(1) if sec2 else "")}
    missing = [w for w in wanted if w not in got]
    if missing:
        sys.exit(f"build.py: compat-scan.md section 2 no longer has the bullets {missing}")
    limits = [got[w] for w in wanted]
    ex = {}
    for repo, v in extra.items():
        for name, c in v["features"].items():
            e = ex.setdefault(name, {"repos": 0, "hits": 0})
            e["repos"] += c["app"] > 0
            e["hits"] += c["app"]
    ex_commits = {r: v["commit"] for r, v in extra.items()}
    return {"features": feats, "extra": ex, "repos": repos, "scan_date": d.group(1), "limits": limits,
            "extra_commits": ex_commits}


# map an inventory id to the usage-scan feature(s) that measure it
EXTRA_BY_ID = [
    (r"^(coll|locale):", "collation"),
    (r"^tls:", "TLS options (tls=true, ssl=true, tlsCAFile, ...)"),
    (r"^sess:(txnNumber|retryWrites)", "retryable writes (retryWrites mentioned; the drivers enable it by default)"),
    (r"^sorthint:\$natural", "$natural sort/hint"),
    (r"^authmech:(x\.509|AWS|OIDC|Kerberos)", "X.509 / AWS / OIDC / Kerberos auth mechanisms"),
    (r"^wire:OP_COMPRESSED", "compression option (compressors=)"),
    (r"^(cmd|opt):mapReduce", "mapReduce"),
    (r"^(cmd|opt):collMod", "collMod"),
]


def camel(s):
    parts = s.split("_")
    return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:])


def usage_names(row_id):
    """Candidate names in the two scans for an inventory id."""
    prefix, _, name = row_id.partition(":")
    out = []
    if prefix in ("qop", "stage", "expr", "acc", "win", "upd", "proj", "projop"):
        out = [name, name + " (update)", name + " (aggregation stage)", name + " (aggregation accumulator)"]
        if prefix == "upd":
            out = [name + " (update)", name]
    elif prefix in ("node", "py"):
        last = camel(name.split(".")[-1])
        out = [last]
        if last == "watch":
            out.append("watch (change streams)")
    return out


def usage_of(row_id, scan):
    """(repos, hits, scan feature) for an inventory id, or None when neither scan measures it."""
    for pat, feat in EXTRA_BY_ID:
        if re.match(pat, row_id) and feat in scan["extra"]:
            e = scan["extra"][feat]
            return e["repos"], e["hits"], feat
    for n in usage_names(row_id):
        f = scan["features"].get(n)
        if f:
            return f["repos"], f["hits"], n
    return None


# ---------------------------------------------------------------- text of rows

def clean_ev(ev):
    ev = ev.strip()
    return ev[:1].upper() + ev[1:]


def plain_note(row, status):
    ev = row["evidence"].strip()
    if status == "supported":
        if "not called by" in ev and "server dependencies:" in ev:
            return "A driver member that sends commands covered by passing comparison tests against MongoDB 7.0; no failing one."
        return "Passing comparison test against MongoDB 7.0; no failing one."
    if status == "not_tested" and row["engine_status"] == "covered":
        lim = re.search(r"limit: (.*)$", ev)
        return ("Implemented, but not compared with MongoDB: there is no reference server for it. It was tested against the documented behaviour only."
                + (f" Limit stated by the audit: {lim.group(1)}" if lim else ""))
    if status == "not_tested":
        return "The code exists but no comparison test exists. " + clean_ev(ev)
    if status == "not_supported":
        return "The engine rejects or ignores this. " + clean_ev(ev)
    m = re.search(r"differs from mongod (\d+\.\d+\.\d+) and is listed in \S+: (.*)$", ev)
    if m:
        why = re.sub(r"^see g07\.locale\.(\S+)$", r"same cause as locale \1", m.group(2))
        return f"Results differ from MongoDB {m.group(1)}: {why}"
    m = re.search(r"failing audit probes \([^)]*\): (\S+) \[differ\]", ev)
    if m:
        return f"A comparison probe ({m.group(1)}) shows a difference from MongoDB; the audit data does not say in words what differs."
    return clean_ev(re.sub(r"^passing [^;]*?; ", "", ev))


TEST_TOKEN = re.compile(r"(?<![\w/.:@=-])[A-Za-z_][\w]*(?:\.[\w@=*-]+)+")
FILE_EXT = (".py", ".js", ".json", ".md", ".rs", ".txt", ".csv")


def evidence_tests(ev):
    seen = []
    for t in TEST_TOKEN.findall(ev):
        t = t.rstrip(".")
        if t.endswith(FILE_EXT) or t.startswith(("mongod", "pymongo", "mongo-", "compat/")) or t in seen:
            continue
        seen.append(t)
    return seen[:12]


# ---------------------------------------------------------------- model

def counts_of(rows):
    c = collections.Counter(r["_status"] for r in rows)
    return {"total": len(rows), **{s: c[s] for s in STATUS_ORDER}}


def git(*args):
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def inventory_commit_and_date():
    """(commit, date) of the last commit that touched compat/inventory.csv, or (None, None) when git cannot say.

    Depends only on the history of that file: not on HEAD, the clock, the refs present in the clone or the abbreviation
    length. A shallow clone is trusted only if the answer is not one of its cut-off commits."""
    shallow = git("rev-parse", "--is-shallow-repository")
    if shallow not in ("true", "false"):
        return None, None
    line = git("log", "-1", "--format=%H %cs", "--", "compat/inventory.csv")
    if not line:
        return None, None
    h, d = line.split()
    if shallow == "true":
        # a commit at the cut-off of a shallow clone shows every file as added there, so it says nothing about the file
        gd = git("rev-parse", "--git-dir")
        path = os.path.join(ROOT, gd, "shallow")
        if not os.path.exists(path) or h in open(path).read().split():
            return None, None
    return h[:7], d


def build_model(inventory_date=None):
    cov = load_coverage_module()
    all_rows = load_inventory()
    for r in all_rows:
        r["_status"] = page_status(r)
    rows = [r for r in all_rows if r["exclusion_approved"] != "yes"]
    for r in rows:
        r["_area"] = area_of(r["category"])
    excluded = [r for r in all_rows if r["exclusion_approved"] == "yes"]
    hdr = parse_header(read("compat/COVERAGE.md"))
    exclusions = parse_exclusions(read("compat/exclusions_decided.md"), len(excluded))
    limits = parse_limits(read("compat/UNVERIFIED.md"))
    extra = json.loads(read("compat/scan_extra.json"))
    scan = parse_scan(read("compat-scan.md"), extra)

    total = counts_of(rows)
    spec_only = sum(r["engine_status"] == "covered" and r["differential_test"] == "no" for r in rows)
    covered = sum(r["engine_status"] == "covered" for r in rows)

    inv_commit, inv_date = inventory_commit_and_date()
    if inventory_date:
        inv_date = inventory_date

    # data notes: disagreements between the sources, reported not resolved
    notes = []
    if (hdr["covered"], hdr["total"]) != (covered, len(rows)):
        notes.append(("coverage-report-stale",
                      f"compat/COVERAGE.md (dated {hdr['date']}) reports {hdr['covered']} of {hdr['total']} covered = {hdr['percent']}%, "
                      f"but compat/inventory.csv now computes {covered} of {len(rows)} covered = {pct(covered, len(rows))}% with compat/coverage.py. "
                      "The CSV was updated after that report (later gap-closing work), so this page uses the CSV and COVERAGE.md is out of date. "
                      f"The engine commit in that header ({hdr['commit']}, branch {hdr['branch']}) belongs to the older state; the build the current inventory was run on is not recorded in the data."
                      + (f" The report also says {hdr['proposed_exclusions']} proposed exclusions; the founder-approved exclusions in the CSV number {len(excluded)}." if hdr["proposed_exclusions"] not in (None, len(excluded)) else "")))
    if spec_only:
        notes.append(("specification-only-rows",
                      f"compat/coverage.py counts {spec_only} rows as covered although no comparison test against MongoDB exists for them "
                      f"(specification tests only). By this page's definition they are Not tested, so Supported is {total['supported']} "
                      f"and not {covered}."))
    unv = read("compat/UNVERIFIED.md")
    tls = [r for r in rows if r["id"].startswith(("tls:", "authmech:x.509"))]
    if "marked `missing`" in unv and tls and all(r["engine_status"] != "missing" for r in tls):
        notes.append(("unverified-stale",
                      "compat/UNVERIFIED.md says TLS and X.509 are marked missing without a comparison test; in compat/inventory.csv they are now "
                      "covered by comparison tests on the same certificates. The CSV is used; that bullet is not repeated on this page."))
    diff_commits = [r["name"] for r in scan["repos"] if scan["extra_commits"].get(r["name"], r["commit"])[:7] != r["commit"][:7]]
    if diff_commits:
        notes.append(("scan-commits-differ",
                      f"The two usage scans were taken at different commits of {len(diff_commits)} of the {len(scan['repos'])} repositories "
                      "(compat-scan.md and compat/scan_extra.json record different commits)."))
    notes.append(("scan-status-column",
                  "The 'Our status' column of compat-scan.md comes from an older compatibility table and is not used here; "
                  "status always comes from compat/inventory.csv."))
    # features
    features = []
    for r in sorted(rows, key=lambda r: r["id"]):
        features.append({
            "id": r["id"], "name": r["name"], "area": r["_area"], "category": r["category"],
            "status": r["_status"], "audit_status": r["engine_status"],
            "note": plain_note(r, r["_status"]), "evidence_id": r["id"],
            "evidence_tests": evidence_tests(r["evidence"]),
        })
    byid = {r["id"]: r for r in rows}

    # categories exactly as coverage.py --detail
    cats = []
    for k, c in cov.table(rows, lambda r: r["category"]).items():
        cats.append({"category": k, "total": c["total"], "covered": c["covered"], "partial": c["partial"],
                     "untested": c["untested"], "missing": c["missing"]})

    areas = []
    for key, title, desc in AREAS:
        arows = [r for r in rows if r["_area"] == key]
        c = counts_of(arows)
        areas.append({"id": key, "title": title, "description": desc, "counts": c,
                      "percent_supported": pct(c["supported"], c["total"]),
                      "categories": sorted({r["category"] for r in arows})})

    # top gaps: rows that are partial or not supported, grouped, ranked by real-application usage
    groups = collections.OrderedDict()
    for r in sorted(rows, key=lambda r: r["id"]):
        if r["_status"] in ("partial", "not_supported"):
            top = r["category"].split(" / ")[0]
            key = "collation" if top in ("collation", "collation locales") else r["category"]
            groups.setdefault(key, []).append(r)
    gaps = []
    for key, grows in groups.items():
        us = [u for u in (usage_of(r["id"], scan) for r in grows) if u]
        best = max(us, key=lambda u: (u[0], u[1])) if us else None
        sc = collections.Counter(r["_status"] for r in grows)
        title = ("Collation (options and locales)" if key == "collation"
                 else key[:1].upper() + key[1:].replace("bson", "BSON").replace("gridfs", "GridFS"))
        gaps.append({
            "title": title, "area": grows[0]["_area"], "rows": len(grows),
            "status_counts": {s: sc[s] for s in ("partial", "not_tested", "not_supported")},
            "row_ids": [r["id"] for r in grows],
            "usage": ({"measured": True, "repos": best[0], "of": len(scan["repos"]), "hits": best[1],
                       "features": sorted({u[2] for u in us}),
                       "source": "compat/scan_extra.json" if best[2] in scan["extra"] else "compat-scan.md"}
                      if best else {"measured": False, "repos": None, "of": len(scan["repos"]), "hits": None, "features": [],
                                    "source": "not measured"}),
        })
    gaps.sort(key=lambda g: (not g["usage"]["measured"], -(g["usage"]["repos"] or 0), -(g["usage"]["hits"] or 0), -g["rows"], g["title"]))
    for i, g in enumerate(gaps, 1):
        g["rank"] = i

    engine_current = (hdr["covered"], hdr["total"]) == (covered, len(rows))
    model = {
        "schema_version": 1,
        "generated": {"audit_report_date": hdr["date"], "inventory_last_changed": inv_date, "inventory_last_commit": inv_commit},
        "engine": {"commit": hdr["commit"], "branch": hdr["branch"], "commit_source": "header of compat/COVERAGE.md",
                   "describes_current_inventory": engine_current,
                   "note": ("The header names this build." if engine_current else
                            "The header names this build, but the inventory has been updated since; the build the current statuses were measured on is not recorded in the data.")},
        "reference_server": {"name": "mongod", "version": hdr["mongod"]},
        "drivers_tested": [{"name": "pymongo", "version": hdr["pymongo"]}, {"name": "mongodb (Node.js)", "version": hdr["node"]}],
        "status_definitions": STATUS_DEFINITIONS,
        "summary": {**total, "percent_supported": pct(total["supported"], total["total"])},
        "audit_rule_covered": {"covered": covered, "total": len(rows), "percent": pct(covered, len(rows)), "specification_only": spec_only},
        "areas": areas,
        "categories": cats,
        "features": features,
        "top_gaps": gaps,
        "exclusions": exclusions,
        "limits": limits,
        "usage_scan": {"repositories": scan["repos"], "scan_date": scan["scan_date"], "features_ranked": len(scan["features"]),
                       "limits": scan["limits"]},
        "data_notes": [{"id": i, "text": t} for i, t in notes],
    }
    return model, rows, byid


# ---------------------------------------------------------------- JSON schema + validator

_COUNTS = {"type": "object", "required": ["total", *STATUS_ORDER], "properties": {k: {"type": "integer", "minimum": 0} for k in ("total", *STATUS_ORDER)}}
_STR, _INT = {"type": "string"}, {"type": "integer", "minimum": 0}
_NUM = {"type": "number", "minimum": 0}
SCHEMA = {
    "type": "object",
    "required": ["schema_version", "generated", "engine", "reference_server", "drivers_tested", "status_definitions", "summary",
                 "audit_rule_covered", "areas", "categories", "features", "top_gaps", "exclusions", "limits", "usage_scan", "data_notes"],
    "properties": {
        "schema_version": {"type": "integer", "enum": [1]},
        "generated": {"type": "object", "required": ["audit_report_date", "inventory_last_changed", "inventory_last_commit"],
                      "properties": {"audit_report_date": _STR, "inventory_last_changed": {"type": ["string", "null"]},
                                     "inventory_last_commit": {"type": ["string", "null"]}}},
        "engine": {"type": "object", "required": ["commit", "branch", "commit_source", "describes_current_inventory", "note"],
                   "properties": {"commit": _STR, "branch": _STR, "commit_source": _STR, "describes_current_inventory": {"type": "boolean"}, "note": _STR}},
        "reference_server": {"type": "object", "required": ["name", "version"], "properties": {"name": _STR, "version": _STR}},
        "drivers_tested": {"type": "array", "items": {"type": "object", "required": ["name", "version"], "properties": {"name": _STR, "version": _STR}}},
        "status_definitions": {"type": "object", "required": list(STATUS_ORDER), "properties": {k: _STR for k in STATUS_ORDER}},
        "summary": {"type": "object", "required": ["total", *STATUS_ORDER, "percent_supported"],
                    "properties": {**_COUNTS["properties"], "percent_supported": _NUM}},
        "audit_rule_covered": {"type": "object", "required": ["covered", "total", "percent", "specification_only"],
                               "properties": {"covered": _INT, "total": _INT, "percent": _NUM, "specification_only": _INT}},
        "areas": {"type": "array", "items": {"type": "object", "required": ["id", "title", "description", "counts", "percent_supported", "categories"],
                                             "properties": {"id": {"type": "string", "enum": [a[0] for a in AREAS]}, "title": _STR, "description": _STR,
                                                            "counts": _COUNTS, "percent_supported": _NUM, "categories": {"type": "array", "items": _STR}}}},
        "categories": {"type": "array", "items": {"type": "object", "required": ["category", "total", "covered", "partial", "untested", "missing"],
                                                  "properties": {"category": _STR, "total": _INT, "covered": _INT, "partial": _INT, "untested": _INT, "missing": _INT}}},
        "features": {"type": "array", "items": {"type": "object",
                                                "required": ["id", "name", "area", "category", "status", "audit_status", "note", "evidence_id", "evidence_tests"],
                                                "properties": {"id": _STR, "name": _STR, "area": {"type": "string", "enum": [a[0] for a in AREAS]}, "category": _STR,
                                                               "status": {"type": "string", "enum": list(STATUS_ORDER)},
                                                               "audit_status": {"type": "string", "enum": ["covered", "partial", "untested", "missing"]},
                                                               "note": _STR, "evidence_id": _STR, "evidence_tests": {"type": "array", "items": _STR}}}},
        "top_gaps": {"type": "array", "items": {"type": "object", "required": ["rank", "title", "area", "rows", "status_counts", "row_ids", "usage"],
                                                "properties": {"rank": _INT, "title": _STR, "area": {"type": "string", "enum": [a[0] for a in AREAS]}, "rows": _INT,
                                                               "status_counts": {"type": "object", "required": ["partial", "not_tested", "not_supported"],
                                                                                 "properties": {k: _INT for k in ("partial", "not_tested", "not_supported")}},
                                                               "row_ids": {"type": "array", "items": _STR},
                                                               "usage": {"type": "object", "required": ["measured", "repos", "of", "hits", "features", "source"],
                                                                         "properties": {"measured": {"type": "boolean"}, "repos": {"type": ["integer", "null"]},
                                                                                        "of": _INT, "hits": {"type": ["integer", "null"]},
                                                                                        "features": {"type": "array", "items": _STR}, "source": _STR}}}}},
        "exclusions": {"type": "object", "required": ["rows", "groups"],
                       "properties": {"rows": _INT, "groups": {"type": "array", "items": {"type": "object", "required": ["group", "rows", "reason"],
                                                                                             "properties": {"group": _STR, "rows": _INT, "reason": _STR}}}}},
        "limits": {"type": "array", "items": {"type": "object", "required": ["id", "text", "source"], "properties": {"id": _STR, "text": _STR, "source": _STR}}},
        "usage_scan": {"type": "object", "required": ["repositories", "scan_date", "features_ranked", "limits"],
                       "properties": {"repositories": {"type": "array", "items": {"type": "object", "required": ["name", "commit"], "properties": {"name": _STR, "commit": _STR}}},
                                      "scan_date": _STR, "features_ranked": _INT, "limits": {"type": "array", "items": _STR}}},
        "data_notes": {"type": "array", "items": {"type": "object", "required": ["id", "text"], "properties": {"id": _STR, "text": _STR}}},
    },
}

_TYPES = {"object": dict, "array": list, "string": str, "boolean": bool, "null": type(None)}


def _is(v, t):
    if t == "integer":
        return isinstance(v, int) and not isinstance(v, bool)
    if t == "number":
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    return isinstance(v, _TYPES[t])


def validate_json(doc, schema=SCHEMA, path="$"):
    """Minimal JSON-schema check (type, enum, required, properties, items, minimum). Returns a list of error strings."""
    errs = []
    ts = schema.get("type")
    ts = [ts] if isinstance(ts, str) else ts
    if ts and not any(_is(doc, t) for t in ts):
        return [f"{path}: expected {ts}, got {type(doc).__name__}"]
    if "enum" in schema and doc not in schema["enum"]:
        errs.append(f"{path}: {doc!r} not in {schema['enum']}")
    if "minimum" in schema and isinstance(doc, (int, float)) and doc < schema["minimum"]:
        errs.append(f"{path}: {doc} < {schema['minimum']}")
    if isinstance(doc, dict):
        for k in schema.get("required", []):
            if k not in doc:
                errs.append(f"{path}: missing {k!r}")
        for k, sub in schema.get("properties", {}).items():
            if k in doc:
                errs += validate_json(doc[k], sub, f"{path}.{k}")
        extra = set(doc) - set(schema.get("properties", {}))
        if schema.get("properties") and extra:
            errs.append(f"{path}: unexpected keys {sorted(extra)}")
    if isinstance(doc, list) and "items" in schema:
        for i, v in enumerate(doc):
            errs += validate_json(v, schema["items"], f"{path}[{i}]")
    return errs


# ---------------------------------------------------------------- markdown

def esc(s):
    return s.replace("|", "\\|").replace("\n", " ")


def names(rows, cap=10):
    shown = [f"`{esc(r['name'])}`" for r in rows[:cap]]
    more = f", and {len(rows) - cap} more" if len(rows) > cap else ""
    return ", ".join(shown) + more


EXCLUSION_TEXT = [
    ("Client-only driver members", "Properties, option holders and helpers that live inside the driver and never send a request (for example a cursor's `closed` flag). There is nothing for a server to implement."),
    ("Client-side encryption helpers", "Field-level encryption helpers (libmongocrypt, key management) run inside the driver, on the client."),
    ("Internal server-to-server", "Commands that MongoDB's own servers send to each other (config servers, shards, donors). Applications cannot call them."),
    ("Replication", "Replica-set administration and internal replication commands, not issued by application code."),
    ("Sharding", "Sharding administration (adding shards, balancer, zones, resharding)."),
    ("Audit", "Auditing is an Enterprise/Atlas feature of MongoDB."),
    ("Enterprise/Atlas-only authentication", "Kerberos, LDAP, OIDC and AWS IAM sign-in exist only in MongoDB Enterprise or Atlas."),
    ("Removed in 5.1", "The client-level `bulkWrite` needs the MongoDB 8.0 `bulkWrite` command, which MongoDB 7.0 does not have; `OP_INSERT` was removed in MongoDB 5.1."),
    ("Undocumented", "Commands that mongod 7.0.45 lists but the 7.0 manual does not document, and that no driver API uses."),
    ("security.javascriptEnabled", "A startup option of mongod, not reachable through a driver."),
    ("Not producible", "`$vectorSearch` (Atlas Vector Search only) and an error label that cannot be induced on either server."),
    ("Purely local driver member", "A second `connect()` on an already connected Node.js client, which sends no request."),
]


def exclusion_text(group, reason):
    for key, text in EXCLUSION_TEXT:
        if key in group:
            return text
    return reason


def fmt_pct(p):
    return f"{p:.1f}%"


def render_md(m, rows, byid):
    L = []
    w = L.append
    s, a = m["summary"], m["audit_rule_covered"]
    eng, gen = m["engine"], m["generated"]
    w("# Fenvix compatibility with MongoDB 7.0")
    w("")
    w("This page is for developers who want to move a MongoDB application to Fenvix. It lists what was compared with a real MongoDB server, what works with a difference, and what does not work. It is generated from an audit of the engine; it is not marketing material, and it says \"not measured\" where the audit has no data (performance and price are not covered at all).")
    w("")
    w(f"> **Audit report date:** {gen['audit_report_date']}. **Reference server:** MongoDB (mongod) {m['reference_server']['version']}. "
      f"**Drivers tested:** pymongo {m['drivers_tested'][0]['version']} and Node.js mongodb {m['drivers_tested'][1]['version']}. "
      + ("" if eng["describes_current_inventory"] else
         f"**Engine build:** the report header names commit `{eng['commit']}` (branch `{eng['branch']}`), but the statuses below were updated after that header was written and the data does not record the build they were measured on. See [Audit date and engine build](#audit-date-and-engine-build)."))
    w("")
    w("## How to read this page")
    w("")
    w("Every feature in the audit has one of four statuses:")
    w("")
    w("| Status | Meaning |")
    w("|---|---|")
    for k in STATUS_ORDER:
        w(f"| **{STATUS_LABEL[k]}** | {m['status_definitions'][k]} |")
    w("")
    w("A feature counts here when it is something an application can reach through a driver. Features that were left out on purpose are listed [below](#left-out-on-purpose) and are not in any count. The complete list, one row per feature with its status and the id of its evidence, is in [`compatibility.json`](compatibility.json).")
    w("")
    w("## Overall summary")
    w("")
    w(f"Of **{s['total']}** features counted, **{s['supported']}** are Supported ({fmt_pct(s['percent_supported'])}), "
      f"{s['partial']} are Partial, {s['not_tested']} are Not tested and {s['not_supported']} are Not supported.")
    w("")
    w("| Status | Features | Share |")
    w("|---|---:|---:|")
    for k in STATUS_ORDER:
        w(f"| {STATUS_LABEL[k]} | {s[k]} | {fmt_pct(pct(s[k], s['total']))} |")
    w(f"| **All counted features** | **{s['total']}** | |")
    w("")
    w("Shares are truncated to one decimal place, never rounded up, so they can add up to slightly less than 100%.")
    w("")
    if a["specification_only"]:
        w(f"The audit's own tool (`compat/coverage.py`) counts a feature as \"covered\" if it passes tests, and reports **{a['covered']} of {a['total']} = {fmt_pct(a['percent'])}**. "
          f"That figure includes {a['specification_only']} features that have no comparison against MongoDB at all (MongoDB 7.0 Community has no such feature, for example the Atlas Search commands). "
          "This page does not call those Supported; they are Not tested.")
        w("")
    w("### By area")
    w("")
    w("| Area | Features | Supported | Partial | Not tested | Not supported |")
    w("|---|---:|---:|---:|---:|---:|")
    for ar in m["areas"]:
        c = ar["counts"]
        w(f"| [{ar['title']}](#{re.sub(r'[^a-z0-9 -]', '', ar['title'].lower()).replace(' ', '-')}) | {c['total']} | {c['supported']} | {c['partial']} | {c['not_tested']} | {c['not_supported']} |")
    w(f"| **All areas** | **{s['total']}** | **{s['supported']}** | **{s['partial']}** | **{s['not_tested']}** | **{s['not_supported']}** |")
    w("")
    w("## Areas")
    w("")
    notable = collections.defaultdict(list)
    for r in sorted(rows, key=lambda r: r["id"]):
        if r["_status"] != "supported":
            notable[r["_area"]].append(r)
    any_workaround = any("workaround" in r["evidence"].lower() for r in rows if r["_status"] != "supported")
    for ar in m["areas"]:
        c = ar["counts"]
        w(f"### {ar['title']}")
        w("")
        w(ar["description"])
        w("")
        if c["supported"] == c["total"]:
            w(f"**All {c['total']} features are Supported.**")
        else:
            w(f"**{c['supported']} of {c['total']} features are Supported** ({fmt_pct(ar['percent_supported'])}); {c['partial']} Partial, {c['not_tested']} Not tested, {c['not_supported']} Not supported.")
        w("")
        rs = notable.get(ar["id"], [])
        if rs:
            w("Notable gaps:")
            w("")
            for st in ("not_supported", "partial", "not_tested"):
                sub = [r for r in rs if r["_status"] == st]
                if sub:
                    w(f"- **{STATUS_LABEL[st]} ({len(sub)}):** {names(sub)}")
            w("")
            wk = [r for r in rs if "workaround" in r["evidence"].lower()]
            for r in wk:
                w(f"- Workaround recorded for `{esc(r['name'])}`: {plain_note(r, r['_status'])}")
            if wk:
                w("")
    w("## Not supported yet, and used by real applications")
    w("")
    ns = [g for g in m["top_gaps"] if g["status_counts"]["not_supported"]]
    if s["not_supported"] == 0:
        w("**No counted feature currently has the status Not supported.** The gaps that remain are the Partial features below, ranked by how many of the 10 scanned applications use the feature. Features with status Not tested are not ranked here; they are listed in their areas above.")
    else:
        w("Features that are Not supported or Partial, grouped, ranked by how many of the 10 scanned applications use them.")
    w("")
    w("| # | Gap | Features | Status | Repositories using it (of 10) | Mentions in application code |")
    w("|---:|---|---:|---|---|---|")
    for g in m["top_gaps"]:
        st = ", ".join(f"{n} {STATUS_LABEL[k].lower()}" for k, n in g["status_counts"].items() if n)
        u = g["usage"]
        if u["measured"]:
            w(f"| {g['rank']} | {esc(g['title'])} | {g['rows']} | {st} | {u['repos']} of {u['of']} | {u['hits']} |")
        else:
            w(f"| {g['rank']} | {esc(g['title'])} | {g['rows']} | {st} | not measured | not measured |")
    w("")
    meas = [g for g in m["top_gaps"] if g["usage"]["measured"]]
    for g in meas:
        w(f"- {g['title']}: usage counted from the scan entry {', '.join('`' + f + '`' for f in g['usage']['features'])} ({g['usage']['source']}). The count is the highest among the {g['rows']} features of this group.")
    if meas:
        w("")
    w("### What the usage scan can and cannot tell")
    w("")
    sc = m["usage_scan"]
    w(f"The scan looked at {len(sc['repositories'])} public repositories ({', '.join(r['name'] for r in sc['repositories'])}), scanned on {sc['scan_date']}, and counted how many of them use each MongoDB feature in application code (tests excluded). It is text matching, not a run of the applications. Limits, as written in the scan report:")
    w("")
    for t in sc["limits"]:
        w(f"- {t}")
    w("")
    w("In short: ten repositories is a small sample, several of them are frameworks or platforms rather than applications, and a count of \"3 of 10\" means three repositories mention the feature, not that three applications depend on it. Features the scans do not look for are marked *not measured*, not zero.")
    w("")
    w("## Left out on purpose")
    w("")
    ex = m["exclusions"]
    w(f"{ex['rows']} items of the MongoDB 7.0 surface were left out of every count above, by a product decision recorded in the audit. They are things an application does not call, or things that belong to running MongoDB itself. Fenvix is a managed service, so replica-set administration, sharding and server-to-server traffic are the operator's side, not something your application code issues.")
    w("")
    w("| Left out | Items | What it is |")
    w("|---|---:|---|")
    for g in ex["groups"]:
        w(f"| {esc(g['group'])} | {g['rows']} | {esc(exclusion_text(g['group'], g['reason']))} |")
    w(f"| **Total** | **{ex['rows']}** | |")
    w("")
    w("Every other row is counted, including admin commands such as `shutdown`, `setParameter` and `fsync`.")
    w("")
    w("## Known differences from MongoDB")
    w("")
    part = [f for f in m["features"] if f["status"] == "partial"]
    if any_workaround:
        w("Where the audit evidence records a workaround it is shown in the area section above.")
    else:
        w("The audit evidence records no workaround for any of these.")
    w("")
    if part:
        w(f"Every Partial feature ({len(part)}):")
        w("")
        w("| Feature | Area | Difference from MongoDB | Evidence id |")
        w("|---|---|---|---|")
        for f in part:
            w(f"| `{esc(f['name'])}` | {AREA_TITLE[f['area']]} | {esc(f['note'])} | `{esc(f['evidence_id'])}` |")
    else:
        w("No feature is Partial.")
    w("")
    w("## What was tested and what was not")
    w("")
    w(f"- **Reference server:** MongoDB mongod {m['reference_server']['version']}. A feature is Supported only if the same request gives the same result on that server and on Fenvix.")
    w("- **Performance and price:** not measured. This page says nothing about either.")
    for l in m["limits"]:
        w(f"- {l['text']}")
    w("")
    w("## Audit date and engine build")
    w("")
    w(f"- Audit report dated **{gen['audit_report_date']}**.")
    if gen["inventory_last_changed"]:
        w(f"- The inventory file the numbers come from was last changed on **{gen['inventory_last_changed']}** (repository commit `{gen['inventory_last_commit']}`).")
    else:
        w("- The date and commit of the last change to the inventory file: not available (the build ran without a full git history).")
    w(f"- Engine commit named in the audit report header: `{eng['commit']}` (branch `{eng['branch']}`). {eng['note']}")
    w("- This page and `compatibility.json` are generated by `tools/compat-page/build.py` from `compat/inventory.csv`. Nothing on this page is edited by hand. To regenerate it after a new audit run, see [`compatibility-regenerate.md`](compatibility-regenerate.md).")
    w("")
    if m["data_notes"]:
        w("### Notes on the source data")
        w("")
        w("Where the audit files disagree with each other, this page reports it instead of choosing silently:")
        w("")
        for n in m["data_notes"]:
            w(f"- {n['text']}")
        w("")
    return "\n".join(L).rstrip("\n") + "\n"


def render_json(m):
    return json.dumps(m, indent=2, ensure_ascii=False) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="do not write; exit 1 if docs/ is out of date")
    ap.add_argument("--out-dir", default=os.path.join(ROOT, "docs"))
    ap.add_argument("--inventory-date", help="YYYY-MM-DD, overrides the date taken from git log")
    args = ap.parse_args(argv)
    model, rows, byid = build_model(args.inventory_date)
    errs = validate_json(model)
    if errs:
        sys.exit("build.py: generated JSON violates its schema:\n  " + "\n  ".join(errs[:20]))
    out = {"compatibility.md": render_md(model, rows, byid), "compatibility.json": render_json(model)}
    for n in model["data_notes"]:
        print("NOTE:", n["text"], file=sys.stderr)
    stale = False
    for name, text in out.items():
        path = os.path.join(args.out_dir, name)
        if args.check:
            cur = open(path, encoding="utf-8").read() if os.path.exists(path) else None
            if cur != text:
                print(f"{path} is out of date", file=sys.stderr)
                stale = True
        else:
            os.makedirs(args.out_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(text)
            print("wrote", os.path.relpath(path, ROOT))
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
