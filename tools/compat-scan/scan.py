#!/usr/bin/env python3
"""Scans public repositories (shallow git clones, one at a time, deleted after each scan) for the
MongoDB features they use, so missing features can be implemented in order of real-world use.

    scan.py run    [--tmp DIR] [--out results.json] [--only NAME]   clone + scan every repo
    scan.py report [--in results.json] [--out ../../compat-scan.md]   build the Markdown report

Counting is plain text matching and over- and under-counts in known ways (see the report).
The main metric is the number of repositories whose *application* code uses a feature; test
files are counted separately and never contribute to that metric.
"""
import argparse
import collections
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))

REPOS = [
    "https://github.com/RocketChat/Rocket.Chat",
    "https://github.com/parse-community/parse-server",
    "https://github.com/NodeBB/NodeBB",
    "https://github.com/wekan/wekan",
    "https://github.com/danny-avila/LibreChat",
    "https://github.com/novuhq/novu",
    "https://github.com/appsmithorg/appsmith",
    "https://github.com/Countly/countly-server",
    "https://github.com/payloadcms/payload",
    "https://github.com/openedx/edx-platform",
]

# ---------------------------------------------------------------------------------------------
# What is a MongoDB dependency
# ---------------------------------------------------------------------------------------------
# (manifest file-name regex, [(token regex, label)])
DEP_TOKENS = [
    (r'"mongodb"\s*:', "npm: mongodb"), (r'"mongoose"\s*:', "npm: mongoose"), (r'"@nestjs/mongoose"\s*:', "npm: @nestjs/mongoose"),
    (r'"@payloadcms/db-mongodb"\s*:', "npm: @payloadcms/db-mongodb"), (r'"(?:mongoose-[\w-]+|@typegoose/typegoose)"\s*:', "npm: mongoose plugin/typegoose"),
    (r'(?im)^\s*(?:pymongo|motor|mongoengine|djongo|beanie|django-mongodb-backend|mongomock)\b', "python: pymongo/motor/mongoengine"),
    (r'(?i)["\']?(?:pymongo|motor|mongoengine|djongo|beanie)["\']?\s*[=<>~!,\]]', "python: pymongo/motor/mongoengine (pyproject/setup)"),
    (r'spring-boot-starter-data-mongodb|spring-data-mongodb|mongodb-driver|mongo-java-driver|mongodb-driver-sync|mongodb-driver-reactivestreams', "jvm: spring-data-mongodb / mongodb-driver"),
    (r'go\.mongodb\.org/mongo-driver|gopkg\.in/mgo', "go: mongo-driver"),
    (r"(?im)^\s*gem\s+['\"](?:mongoid|mongo)['\"]", "ruby: mongoid/mongo"),
    (r'"mongodb/mongodb"', "php: mongodb"),
]
MANIFESTS = re.compile(r"^(package\.json|requirements[\w.-]*\.(?:txt|in)|pyproject\.toml|setup\.py|setup\.cfg|Pipfile|pom\.xml|build\.gradle(?:\.kts)?|go\.mod|Gemfile|composer\.json)$")
TEST_ONLY_DEPS = re.compile(r"mongodb-memory-server|mongomock", re.I)

PRUNE_DIRS = {
    "node_modules", ".git", "vendor", "vendors", "third_party", "thirdparty", "dist", "build", "out", "target", ".next", ".nuxt",
    "coverage", "bower_components", "__pycache__", ".venv", "venv", "env", "site-packages", ".tox", ".gradle", ".idea", ".turbo",
    ".yarn", ".pnpm-store", "static_vendor", "public_vendor", "generated", "__generated__", ".cache", "storybook-static", ".meteor",
}
LOCKFILES = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "npm-shrinkwrap.json", "poetry.lock", "Pipfile.lock", "composer.lock", "Cargo.lock", "go.sum", "Gemfile.lock", "bun.lockb"}
SCAN_EXT = {".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx", ".py", ".java", ".kt", ".kts", ".scala", ".groovy", ".go", ".rb", ".php", ".cs", ".vue", ".svelte", ".coffee"}
MAX_FILE_BYTES = 1_000_000

TEST_PATH = re.compile(
    r"(?:^|/)(?:tests?|__tests__|specs?|e2e|cypress|playwright|fixtures?|__mocks__|mocks?|testing|test-utils|testutils|integration[-_]?tests?|ee-tests|unit-tests|test_data|testdata|stories)(?:/|$)"
    r"|(?:^|/)src/test/|\.(?:test|spec|e2e|stories)\.[a-z]+$|(?:^|/)test_[^/]*\.py$|_test\.(?:py|go)$|(?:Test|Tests|IT|Spec)\.(?:java|kt|scala|groovy)$|(?:^|/)conftest\.py$"
)


def is_minified(name: str, head: str) -> bool:
    if re.search(r"\.min\.[a-z]+$|\.bundle\.[a-z]+$|\.chunk\.[a-z]+$|-min\.js$|\.map$", name):
        return True
    lines = head.splitlines()[:5]
    return bool(lines) and max(len(x) for x in lines) > 3000  # a single enormous line: generated or minified


# ---------------------------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------------------------
QUERY_OPS = "eq ne gt gte lt lte in nin exists type and or nor not expr jsonSchema mod regex text where all elemMatch size bitsAllSet bitsAnySet bitsAllClear bitsAnyClear geoIntersects geoWithin near nearSphere box center centerSphere geometry maxDistance minDistance polygon meta slice options search".split()
UPDATE_OPS = "set unset inc setOnInsert push pull pullAll addToSet pop rename min max mul currentDate each position bit".split()
STAGE_OPS = "match group sort limit skip project unwind lookup addFields replaceRoot replaceWith facet sortByCount bucket bucketAuto count out merge sample geoNear graphLookup unionWith redact collStats indexStats planCacheStats densify fill setWindowFields searchMeta changeStream documents".split()
ACC_OPS = "sum avg first last stdDevPop stdDevSamp mergeObjects accumulator function top bottom topN bottomN firstN lastN maxN minN median percentile".split()
EXPR_OPS = ("add subtract multiply divide abs ceil floor round pow sqrt trunc ln log log10 exp concat substr substrCP substrBytes toLower toUpper trim ltrim rtrim split strLenCP strLenBytes strcasecmp "
            "cond ifNull switch arrayElemAt filter map reduce concatArrays isArray indexOfArray range reverseArray zip toString toInt toDouble toLong toDecimal toDate toObjectId toBool convert "
            "dateToString dateFromString dateFromParts dateToParts year month dayOfMonth dayOfWeek dayOfYear hour minute second millisecond week isoWeek isoWeekYear isoDayOfWeek dateAdd dateSubtract dateDiff dateTrunc "
            "literal let arrayToObject objectToArray getField setField unsetField anyElementTrue allElementsTrue setUnion setIntersection setDifference setEquals setIsSubset regexMatch regexFind regexFindAll "
            "replaceOne replaceAll cmp rand sampleRate isNumber binarySize bsonSize mergeObjects sortArray tsSecond tsIncrement indexOfBytes indexOfCP not").split()

# Occurrences of "$name" are roles; most names are unambiguous. These need the surrounding text.
ROLE_SPLIT = {"min", "max", "push", "addToSet", "set", "unset"}
NAME_ROLE = {}
for _group, _names in (("query", QUERY_OPS), ("update", UPDATE_OPS), ("stage", STAGE_OPS), ("accumulator", ACC_OPS), ("expression", EXPR_OPS)):
    for _n in _names:
        NAME_ROLE.setdefault(_n, _group)
KNOWN_OPS = set(NAME_ROLE)
KNOWN_OPS.discard("comment")  # also a JSON-schema keyword

QUOTED = re.compile(r"""["'`]\$([A-Za-z][A-Za-z0-9]*)["'`]""")
# unquoted key: `$in: [...]`; not a GraphQL variable declaration (`$limit: Int`, `$first: Int = 10`)
UNQUOTED = re.compile(r"""(?<![\w$"'`.])\$([A-Za-z][A-Za-z0-9]*)\s*:(?!\s*\[?[A-Z][A-Za-z_]*!?\]?!?\s*(?:[,)=]|$))""", re.M)
AGG_WINDOW = re.compile(r"aggregate|\$group|\$project|\$match|\$lookup|pipeline|\$addFields|\$unwind|\$facet", re.I)
UPDATE_WINDOW = re.compile(r"update|\$inc|\$setOnInsert|\$pull|findAndModify|upsert", re.I)


def role_of(name: str, text: str, pos: int) -> str:
    """Disambiguate names that are both an update operator and an aggregation operator/stage."""
    if name not in ROLE_SPLIT:
        return NAME_ROLE[name]
    window = text[max(0, pos - 500): pos + 300]
    agg, upd = bool(AGG_WINDOW.search(window)), bool(UPDATE_WINDOW.search(window))
    if name in ("min", "max", "push", "addToSet"):
        return "accumulator" if (agg and "$group" in window) else "update"
    if name in ("set", "unset"):
        return "stage" if (agg and not upd) else "update"
    return NAME_ROLE[name]


# ---------------------------------------------------------------------------------------------
# Methods and indexes
# ---------------------------------------------------------------------------------------------
# A method counts only in files that look like they talk to MongoDB.
MONGO_HINT = re.compile(r"mongo|\.collection\(|getCollection|mongoose|pymongo|motor|\bcol\.|\bModel\b|\.model\(|Schema\(|@Document|@Query|MongoTemplate|Collection<", re.I)
METHODS = [
    ("findOneAndUpdate", r"\b(?:findOneAndUpdate|find_one_and_update|findAndModify|find_and_modify|findByIdAndUpdate)\b"),
    ("findOneAndReplace", r"\b(?:findOneAndReplace|find_one_and_replace)\b"),
    ("findOneAndDelete", r"\b(?:findOneAndDelete|find_one_and_delete|findByIdAndDelete|findByIdAndRemove|findOneAndRemove)\b"),
    ("bulkWrite", r"\b(?:bulkWrite|bulk_write|initializeOrderedBulkOp|initializeUnorderedBulkOp|BulkOperations|BulkWriteOperation|bulk_operations)\b"),
    ("distinct", r"\.distinct\s*\("),
    ("watch (change streams)", r"\.watch\s*\(|\bchangeStream\b|\$changeStream|watch_collection|\.changeStream\("),
    ("startSession", r"\bstartSession\s*\(|\bstart_session\s*\("),
    ("withTransaction / startTransaction", r"\bwithTransaction\b|\bwith_transaction\b|\bstartTransaction\b|\bstart_transaction\b|\bcommitTransaction\b|MongoTransactionManager|TransactionalOperator"),
    ("arrayFilters", r"\barrayFilters\b|\barray_filters\b"),
    ("positional update path ($, $[], $[id])", r"""["'`][A-Za-z_][\w.]*\.\$(?:\[\w*\])?(?:\.[\w.]+)?["'`]\s*:|["'`][A-Za-z_][\w.]*\.\$\[\w*\][\w.]*["'`]"""),
    ("pipeline-style update", r"(?s)\b(?:updateOne|updateMany|update_one|update_many|findOneAndUpdate|find_one_and_update)\s*\([^;]{0,400}?,\s*\[\s*\{\s*[\"']?\$(?:set|addFields|project|unset|replaceRoot|replaceWith)\b"),
    ("renameCollection", r"\brenameCollection\b|\brename_collection\b"),
    ("aggregate", r"\.aggregate\s*\(|\bAggregation\.newAggregation\b"),
    ("find (reference)", r"\.find\s*\(|\.findOne\s*\(|\.find_one\s*\("),
    ("insertMany / insert_many", r"\binsertMany\s*\(|\binsert_many\s*\("),
    ("updateMany / update_many", r"\bupdateMany\s*\(|\bupdate_many\s*\("),
    ("countDocuments / count_documents", r"\bcountDocuments\s*\(|\bcount_documents\s*\(|\bestimatedDocumentCount\s*\("),
]
METHOD_RX = [(n, re.compile(rx)) for n, rx in METHODS]
INDEX_CALL = re.compile(r"(?:\.index|\.createIndex|\.ensureIndex|\.create_index|\.createIndexes|\bIndexModel)\s*\(", re.I)

INDEX_KINDS = [
    ("index: TTL (expireAfterSeconds)", re.compile(r"\bexpireAfterSeconds\b|\bexpire_after_seconds\b|\bexpireAfter\s*=")),
    ("index: partialFilterExpression", re.compile(r"\bpartialFilterExpression\b|\bpartial_filter_expression\b")),
    ("index: sparse", re.compile(r"\bsparse\s*[:=]\s*(?:true|True)\b")),
    ("index: unique", re.compile(r"\bunique\s*[:=]\s*(?:true|True)\b")),
]
# Kinds identified by a bare string/constant are only counted inside an index call or an index annotation,
# otherwise `type: 'text'`, canvas `getContext('2d')` or SQL `TEXT` would be counted.
TYPED_INDEX = [
    ("index: text", re.compile(r"""["']text["']|\bpymongo\.TEXT\b|\bTEXT\b""")),
    ("index: 2dsphere / 2d (geo)", re.compile(r"""["'](?:2dsphere|2d)["']|\bGEO2D\b|\bGEOSPHERE\b|\bpymongo\.GEO""")),
    ("index: hashed / wildcard", re.compile(r"""["']hashed["']|\$\*\*|\bwildcardProjection\b|\bHASHED\b""")),
]
FIELD_INDEX_OPTION = re.compile(r"""\bindex\s*:\s*["'](text|2dsphere|2d|hashed)["']""")
ANNOTATION_INDEX = [
    ("index: text", re.compile(r"@TextIndexed\b|TextIndexDefinition\b")),
    ("index: 2dsphere / 2d (geo)", re.compile(r"@GeoSpatialIndexed\b|GeoSpatialIndexType\b")),
]
INDEX_DEF_LINE = re.compile(r"(?:\.index|\.createIndex|\.ensureIndex|\.create_index|\.createIndexes)\s*\(\s*([\{\[])", re.I)


def call_args(text: str, start: int, limit: int = 800) -> str:
    """Text of a call's arguments from just after the opening parenthesis up to its matching ')'."""
    depth, i, end = 1, start, min(len(text), start + limit)
    quote = None
    while i < end:
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 1
            elif ch == quote:
                quote = None
        elif ch in "\"'`":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                return text[start:i]
        i += 1
    return text[start:end]


def count_keys_js(body: str) -> int:
    return len(re.findall(r"""(?:^|[,{\s])["']?[\w.$-]+["']?\s*:\s*(?:-?\d|["'](?:text|2dsphere|2d|hashed)["'])""", body))


def compound_index_count(text: str) -> int:
    n = 0
    for m in INDEX_CALL.finditer(text):
        seg = call_args(text, m.end())
        if seg.lstrip().startswith("{"):
            first = call_args(seg, seg.index("{") + 1)  # the key document only, not the options
            if count_keys_js(first) >= 2:
                n += 1
        elif seg.lstrip().startswith("["):  # python: [("a", 1), ("b", -1)]
            first = call_args(seg, seg.index("[") + 1)
            if len(re.findall(r"\(\s*['\"][\w.$-]+['\"]\s*,", first)) >= 2:
                n += 1
    n += len(re.findall(r"@CompoundIndex\b", text))
    # mongoengine: meta = {'indexes': [('a', 'b'), ...]} -> tuples of two or more field names
    for m in re.finditer(r"""['"]indexes['"]\s*:\s*\[""", text):
        seg = text[m.end(): m.end() + 800]
        n += len(re.findall(r"""\(\s*['"][-+$#*]?[\w.]+['"]\s*,\s*['"][-+$#*]?[\w.]+['"]""", seg[: seg.find("]\n") if "]\n" in seg else 800]))
    return n


# ---------------------------------------------------------------------------------------------
# Scanning one repository
# ---------------------------------------------------------------------------------------------
def find_dependencies(root: str) -> dict:
    found = collections.OrderedDict()
    test_only = []
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d not in PRUNE_DIRS or d == ".meteor"]
        rel = os.path.relpath(dp, root)
        if rel.count(os.sep) > 6:
            dns[:] = []
            continue
        if os.path.basename(dp) == ".meteor" and "packages" in fns:
            txt = open(os.path.join(dp, "packages"), errors="ignore").read()
            if re.search(r"(?m)^\s*mongo(?:@[\d.]+)?\s*(?:#.*)?$", txt):
                found.setdefault("meteor: built-in mongo package", []).append(os.path.relpath(os.path.join(dp, "packages"), root))
        for fn in fns:
            if not MANIFESTS.match(fn):
                continue
            path = os.path.join(dp, fn)
            try:
                if os.path.getsize(path) > 2_000_000:
                    continue
                txt = open(path, errors="ignore").read()
            except OSError:
                continue
            for rx, label in DEP_TOKENS:
                if re.search(rx, txt):
                    found.setdefault(label, []).append(os.path.relpath(path, root))
            if TEST_ONLY_DEPS.search(txt):
                test_only.append(os.path.relpath(path, root))
    return {"found": {k: v[:5] + ([f"... +{len(v) - 5} more"] if len(v) > 5 else []) for k, v in found.items()}, "test_only_manifests": test_only[:3]}


def scan_repo(root: str) -> dict:
    zero = lambda: {"app": 0, "test": 0}
    ops = collections.defaultdict(zero)           # key: "$name|role"
    methods = collections.defaultdict(zero)
    index_kinds = collections.defaultdict(zero)
    files = {"scanned_app": 0, "scanned_test": 0, "skipped_large_or_minified": 0}
    repo_files = collections.defaultdict(lambda: {"app": set(), "test": set()})
    locations = collections.defaultdict(list)  # key -> ["file:line", ...] of application-code hits (results9)

    def note(key, text, pos, rel, kind):
        if kind == "app" and len(locations[key]) < 400:
            locations[key].append(f"{rel}:{text.count(chr(10), 0, pos) + 1}")

    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d not in PRUNE_DIRS]
        for fn in fns:
            if fn in LOCKFILES:
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext not in SCAN_EXT:
                continue
            path = os.path.join(dp, fn)
            rel = os.path.relpath(path, root).replace(os.sep, "/")
            try:
                size = os.path.getsize(path)
                if size > MAX_FILE_BYTES:
                    files["skipped_large_or_minified"] += 1
                    continue
                text = open(path, errors="ignore").read()
            except OSError:
                continue
            if is_minified(fn, text[:20000]):
                files["skipped_large_or_minified"] += 1
                continue
            kind = "test" if TEST_PATH.search(rel) else "app"
            files["scanned_" + kind] += 1
            low = text.lower()
            # (a) operators
            if "$" in text:
                for rx in (QUOTED, UNQUOTED):
                    for m in rx.finditer(text):
                        name = m.group(1)
                        if name in KNOWN_OPS:
                            ops[f"${name}|{role_of(name, text, m.start())}"][kind] += 1
                            repo_files[f"${name}|{role_of(name, text, m.start())}"][kind].add(rel)
                            note(f"${name}|{role_of(name, text, m.start())}", text, m.start(), rel, kind)
            hinted = bool(MONGO_HINT.search(text))
            # (b) methods
            if hinted:
                for name, rx in METHOD_RX:
                    hits = [m.start() for m in rx.finditer(text)]
                    c = len(hits)
                    if c:
                        methods[name][kind] += c
                        repo_files[name][kind].add(rel)
                        for h in hits:
                            note(name, text, h, rel, kind)
            # (c) indexes
            if hinted and ("index" in low or "unique" in low or "expire" in low):
                for name, rx in INDEX_KINDS:
                    hits = [m.start() for m in rx.finditer(text)]
                    c = len(hits)
                    if c:
                        index_kinds[name][kind] += c
                        repo_files[name][kind].add(rel)
                        for h in hits:
                            note(name, text, h, rel, kind)
                typed = collections.Counter()
                for m in INDEX_CALL.finditer(text):
                    seg = call_args(text, m.end())
                    for name, rx in TYPED_INDEX:
                        if rx.search(seg):
                            typed[name] += 1
                            note(name, text, m.start(), rel, kind)
                for m in FIELD_INDEX_OPTION.finditer(text):
                    k2 = {"text": "index: text", "hashed": "index: hashed / wildcard"}.get(m.group(1), "index: 2dsphere / 2d (geo)")
                    typed[k2] += 1
                    note(k2, text, m.start(), rel, kind)
                for name, rx in ANNOTATION_INDEX:
                    typed[name] += len(rx.findall(text))
                    for m in rx.finditer(text):
                        note(name, text, m.start(), rel, kind)
                for name, c in typed.items():
                    if c:
                        index_kinds[name][kind] += c
                        repo_files[name][kind].add(rel)
                c = compound_index_count(text)
                if c:
                    index_kinds["index: compound"][kind] += c
                    repo_files["index: compound"][kind].add(rel)
                c = len(INDEX_CALL.findall(text)) + len(re.findall(r"@Indexed\b|@CompoundIndex\b|\bindex\s*:\s*true\b|\bindexes\s*=\s*\[", text))
                if c:
                    index_kinds["index: any definition"][kind] += c
                    repo_files["index: any definition"][kind].add(rel)
    examples = {k: {"app": sorted(v["app"])[:3], "test": sorted(v["test"])[:2]} for k, v in repo_files.items() if v["app"] or v["test"]}
    return {"files": files, "operators": dict(ops), "methods": dict(methods), "indexes": dict(index_kinds), "examples": examples, "locations": dict(locations)}


def run_one(url: str, tmp: str) -> dict:
    name = url.rstrip("/").split("/")[-1]
    entry = {"repo": url, "name": name}
    dest = os.path.join(tmp, name)
    shutil.rmtree(dest, ignore_errors=True)
    t0 = time.time()
    try:
        r = subprocess.run(["git", "clone", "--depth", "1", "--quiet", url, dest], capture_output=True, text=True, timeout=900)
        if r.returncode != 0:
            entry.update(status="skipped", reason="clone failed: " + (r.stderr.strip().splitlines() or ["unknown error"])[-1][:200])
            return entry
        commit = subprocess.run(["git", "-C", dest, "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
        size = subprocess.run(["du", "-sk", dest], capture_output=True, text=True).stdout.split()[0]
        entry.update(commit=commit, clone_kb=int(size), clone_secs=round(time.time() - t0, 1))
        deps = find_dependencies(dest)
        entry["dependencies"] = deps
        if not deps["found"]:
            entry.update(status="skipped", reason="no MongoDB driver, Mongoose, Spring Data MongoDB, pymongo, motor, mongoengine or Meteor mongo dependency found in any manifest" + (f" (only a test-only dependency in {', '.join(deps['test_only_manifests'])})" if deps["test_only_manifests"] else ""))
            return entry
        entry.update(status="scanned", **scan_repo(dest))
        return entry
    except subprocess.TimeoutExpired:
        entry.update(status="skipped", reason="clone timed out after 900 s")
        return entry
    finally:
        shutil.rmtree(dest, ignore_errors=True)  # one clone at a time


def cmd_run(a):
    base = a.tmp or tempfile.gettempdir()
    tmp = tempfile.mkdtemp(prefix="compat-scan-", dir=base)
    results = []
    try:
        for url in REPOS:
            if a.only and a.only.lower() not in url.lower():
                continue
            print(f"== {url}", flush=True)
            e = run_one(url, tmp)
            print(f"   {e['status']}" + (f": {e['reason']}" if e["status"] == "skipped" else f" ({e['files']['scanned_app']} app files, {e['files']['scanned_test']} test files, {sum(v['app'] for v in e['operators'].values())} operator hits)"), flush=True)
            results.append(e)
            assert not os.path.exists(os.path.join(tmp, e["name"])), "clone was not deleted"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    json.dump({"scanned_at": time.strftime("%Y-%m-%d"), "repos": results}, open(a.out, "w"), indent=1)
    print(f"wrote {a.out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--tmp", default=None)
    r.add_argument("--out", default=os.path.join(HERE, "results.json"))
    r.add_argument("--only", default="")
    r.set_defaults(fn=cmd_run)
    rp = sub.add_parser("report")
    rp.add_argument("--in", dest="inp", default=os.path.join(HERE, "results.json"))
    rp.add_argument("--out", default=os.path.join(HERE, "..", "..", "compat-scan.md"))
    rp.set_defaults(fn=lambda a: __import__("report").main(a))
    args = ap.parse_args()
    sys.path.insert(0, HERE)
    args.fn(args)
