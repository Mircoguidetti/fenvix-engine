"""Builds compat-scan.md from results.json (see scan.py)."""
import collections
import json
import os

SUPPORTED = "supported"
DIFF = "supported with differences"
NOT = "not supported"

# ---- status of each feature, taken from results5.md (and the direct probes noted) --------------
QUERY_OK = set("eq ne gt gte lt lte in nin exists and or nor not".split())
STAGE_OK = set("match group sort limit skip project count".split())
ACC_OK = set("sum avg first last min max push count".split())  # $addToSet / others are not
UPDATE_OK = set("set unset inc".split())


def status_of(name: str, role: str):
    """-> (status, note) for an operator in a given role."""
    if role == "query":
        if name in QUERY_OK:
            return SUPPORTED, ""
        return NOT, ""
    if role == "update":
        if name in UPDATE_OK:
            return SUPPORTED, ""
        return NOT, ""
    if role == "stage":
        if name in STAGE_OK:
            return SUPPORTED, ""
        return NOT, ""
    if role == "accumulator":
        if name in ACC_OK:
            return SUPPORTED, "$group accumulator" + (" ($first/$last/$push work but are outside the requested scope)" if name in ("first", "last", "push") else "")
        return NOT, ""
    return NOT, "aggregation expression: no expression operators except field paths and literals"


METHOD_STATUS = {
    "findOneAndUpdate": (NOT, "`findAndModify` command; includes Mongoose `findByIdAndUpdate`"),
    "findOneAndReplace": (NOT, "`findAndModify` command"),
    "findOneAndDelete": (NOT, "`findAndModify` command; includes `findByIdAndDelete`/`findOneAndRemove`"),
    "bulkWrite": (SUPPORTED, "driver lowers it to insert/update/delete; probed directly, results identical to MongoDB 7.0 (ordered and unordered)"),
    "distinct": (SUPPORTED, "outside the requested scope, passes the differential tests"),
    "watch (change streams)": (NOT, "no change streams (also not available on a standalone mongod)"),
    "startSession": (SUPPORTED, "sessions are accepted and ignored; passes `admin.session_insert`"),
    "withTransaction / startTransaction": (NOT, "no transactions; a standalone `mongod` has none either, production deployments use replica sets"),
    "aggregate": (DIFF, "subset of stages: $match, $group, $sort, $limit, $skip, $project, $count"),
    "find (reference)": (SUPPORTED, ""),
    "insertMany / insert_many": (SUPPORTED, ""),
    "updateMany / update_many": (SUPPORTED, ""),
    "countDocuments / count_documents": (SUPPORTED, ""),
}
INDEX_STATUS = {
    "index: unique": (SUPPORTED, "enforced on insert, update, upsert and index build; a partial/sparse filter on a unique index is ignored"),
    "index: compound": (DIFF, "definition recorded and listed; queries never use it (they scan); a unique compound index is enforced"),
    "index: text": (DIFF, "definition recorded only; `$text` queries are not supported, so the index has no effect"),
    "index: TTL (expireAfterSeconds)": (DIFF, "definition recorded only; **documents never expire**"),
    "index: partialFilterExpression": (DIFF, "recorded only; the filter is not applied"),
    "index: sparse": (DIFF, "recorded only; not applied"),
    "index: 2dsphere / 2d (geo)": (DIFF, "recorded only; geo queries ($near, $geoWithin, ...) are not supported"),
    "index: hashed / wildcard": (DIFF, "recorded only"),
    "index: any definition": (DIFF, "context row: any index definition (only `unique` has an effect)"),
}
ROLE_LABEL = {"query": "query operator", "update": "update operator", "stage": "aggregation stage", "accumulator": "aggregation accumulator", "expression": "aggregation expression"}
SPLIT = {"min", "max", "push", "addToSet", "set", "unset"}


def display(key: str):
    if "|" in key:
        name, role = key.split("|")
        label = f"${name[1:]}" if name.startswith("$") else name
        if name[1:] in SPLIT:
            label += f" ({'update' if role == 'update' else 'aggregation ' + role})"
        st, note = status_of(name[1:], role)
        return label, ROLE_LABEL[role], st, note
    if key in METHOD_STATUS:
        return key, "driver/ODM method", *METHOD_STATUS[key]
    return key, "index definition", *INDEX_STATUS[key]


def main(a):
    data = json.load(open(a.inp))
    repos = data["repos"]
    scanned = [r for r in repos if r["status"] == "scanned"]
    skipped = [r for r in repos if r["status"] != "scanned"]
    n = len(scanned)
    feats = collections.defaultdict(lambda: {"repos_app": [], "repos_test": [], "app": 0, "test": 0, "per_repo": {}, "examples": {}})
    for r in scanned:
        for section in ("operators", "methods", "indexes"):
            for key, c in r[section].items():
                f = feats[key]
                f["app"] += c["app"]
                f["test"] += c["test"]
                f["per_repo"][r["name"]] = c
                if c["app"]:
                    f["repos_app"].append(r["name"])
                elif c["test"]:
                    f["repos_test"].append(r["name"])
                if key in r.get("examples", {}) and r["examples"][key]["app"]:
                    f["examples"][r["name"]] = r["examples"][key]["app"][0]
    rows = []
    for key, f in feats.items():
        label, kind, st, note = display(key)
        rows.append({"key": key, "label": label, "kind": kind, "status": st, "note": note, **f, "nrepos": len(f["repos_app"])})
    rows.sort(key=lambda x: (-x["nrepos"], -x["app"], x["label"]))
    app_rows = [r for r in rows if r["nrepos"] > 0]

    def table(rs, with_repos=False):
        head = "| # | Feature | Kind | Repos using it (application code) | Occurrences (app) | Occurrences (tests only) | Our status |" + (" Repos |" if with_repos else "")
        sep = "|---|---|---|---|---|---|---|" + ("---|" if with_repos else "")
        out = [head, sep]
        for i, r in enumerate(rs, 1):
            line = f"| {i} | `{r['label']}` | {r['kind']} | {r['nrepos']}/{n} | {r['app']} | {r['test']} | {r['status']} |"
            if with_repos:
                line += f" {', '.join(r['repos_app'])} |"
            out.append(line)
        return "\n".join(out)

    md = []
    md.append("# MongoDB feature usage in real applications\n")
    md.append(f"Purpose: rank MongoDB features by how many real applications use them, so the most-used missing ones are implemented first. Scanned {data['scanned_at']} with `tools/compat-scan/scan.py` over shallow clones (`git clone --depth 1`) of public repositories, one at a time, each deleted after its scan. No GitHub API and no paid service was used. \"Our status\" is taken from the compatibility table in `results5.md`; where that table has no row (`bulkWrite`, transactions) I probed the feature directly against `mongo:7.0` and the server.\n")
    md.append("## 1. Repositories\n")
    md.append("| Repository | Commit | Result | MongoDB dependency found | Application files | Test files | Size of clone (KiB) |\n|---|---|---|---|---|---|---|")
    for r in repos:
        if r["status"] == "scanned":
            deps = "; ".join(f"{k} ({v[0]})" for k, v in list(r["dependencies"]["found"].items())[:3])
            md.append(f"| {r['name']} | `{r.get('commit','')}` | scanned | {deps} | {r['files']['scanned_app']} | {r['files']['scanned_test']} | {r.get('clone_kb','')} |")
        else:
            md.append(f"| {r['name']} | `{r.get('commit','')}` | **skipped** | | | | |")
    md.append("")
    if skipped:
        md.append("Skipped:\n")
        for r in skipped:
            md.append(f"- **{r['name']}**: {r['reason']}")
        md.append("")
    else:
        md.append("No repository was skipped: every clone succeeded and every repository has a MongoDB dependency.\n")
    nsk = sum(r["files"]["skipped_large_or_minified"] for r in scanned)
    md.append(f"Totals over the {n} scanned repositories: {sum(r['files']['scanned_app'] for r in scanned)} application files and {sum(r['files']['scanned_test'] for r in scanned)} test files scanned; {nsk} files skipped as larger than 1 MB, minified or generated.\n")

    md.append("## 2. What was counted, and how reliable it is\n")
    md.append("""- **Scope.** Files with extensions `.js .jsx .mjs .cjs .ts .tsx .py .java .kt .scala .groovy .go .rb .php .cs .vue .svelte .coffee`. Excluded: `node_modules`, `vendor`, `third_party`, `dist`, `build`, `out`, `target`, `.next`, `coverage`, virtualenvs, `.git`, generated folders, lockfiles, `*.min.*`, bundles, source maps, files over 1 MB and files whose first lines exceed 3,000 characters. **Test files** (paths under `test(s)`, `__tests__`, `spec`, `e2e`, `cypress`, `fixtures`, `mocks`, `src/test`, `*.test.*`, `*.spec.*`, `test_*.py`, `*Test.java`, ...) are counted separately and never contribute to the main metric.
- **Main metric:** the number of repositories in which the feature appears at least once in *application* code. Secondary: total occurrences in application code, and (separate column) occurrences that appear only in tests.
- **(a) Operators.** A `$name` counts when it is a quoted string (`"$in"`, `'$lookup'`) or an unquoted object key (`$in: [...]`, also inside Spring `@Query("{ $or: [...] }")`) and `name` is a known MongoDB operator. GraphQL variable declarations (`$limit: Int`) are filtered out; `$comment` is ignored (JSON-schema keyword).
- **Same name, two roles.** `$min`, `$max`, `$push`, `$addToSet`, `$set` and `$unset` exist both as update operators and as aggregation accumulators/stages, and their status differs. The role is guessed from the surrounding ~800 characters (an aggregation context with `$group`/`aggregate`/`pipeline` versus an `update`/`$inc`/`upsert` context). This is a heuristic and can misclassify.
- **(b) Methods** are counted only in files that look MongoDB-related (mention `mongo`, `mongoose`, `.collection(`, `Model`, `Schema(`, `@Document`, ...), because names like `aggregate`, `distinct` and `watch` are common elsewhere. Mongoose shortcuts are included (`findByIdAndUpdate` counts as `findOneAndUpdate`, `findByIdAndDelete` as `findOneAndDelete`).
- **(c) Indexes** are detected from option names and call shapes in MongoDB-related files (`expireAfterSeconds`, `partialFilterExpression`, `sparse: true`, `unique: true`, `'2dsphere'`, `'text'`, `.index({a:1, b:-1})`, `createIndex`, `create_index([...])`, `@CompoundIndex`, `@Indexed`, mongoengine `meta = {'indexes': [...]}`). Compound-index detection needs the key list to be written inline at the call, so indexes built from variables are missed.
- **Over-counting:** string matching cannot tell a MongoDB query from a similarly shaped literal (for example the same operator names in a query-builder that merely emits them, a JSON fixture inside a source file, or API documentation strings). Repositories that *translate* queries (Parse Server, Payload's database adapters) show many operators that applications built on them would not use directly.
- **Under-counting:** Java/Kotlin code that builds queries with Spring Data's `Criteria`/`Query` API or Python code that uses ODM query builders (Appsmith and edx-platform show few `$` strings for this reason); operators built from variables or concatenated strings, queries in `.json`/`.yml`/`.graphql` files, indexes defined through helper functions, and any language not in the extension list.
- **Several repositories are libraries or platforms** (Parse Server, Payload, Rocket.Chat, Novu), so their usage reflects what a framework needs, which is a fair proxy for what a MongoDB-compatible server must support to host them.
""")

    md.append(f"## 3. Ranking of all features used in application code\n")
    md.append(f"{len(app_rows)} features appear in at least one repository's application code. Sorted by number of repositories (of {n}), then by occurrences. Features that appear only in tests are not listed.\n")
    md.append(table(app_rows))
    md.append("")
    notes = [r for r in app_rows if r["note"]]
    if notes:
        md.append("Notes on status:\n")
        for r in notes:
            if r["kind"] in ("driver/ODM method", "index definition") or r["status"] != NOT or "expression" in r["kind"]:
                md.append(f"- `{r['label']}`: {r['note']}")
        md.append("")

    nsup = [r for r in app_rows if r["status"] == NOT]
    md.append("## 4. Not supported features, ranked\n")
    md.append(f"{len(nsup)} of the {len(app_rows)} features used by at least one repository are not supported.\n")
    md.append(table(nsup, with_repos=True))
    md.append("")
    top = nsup[:10]
    md.append("### Recommended implementation order (top 10 not-yet-supported features by number of repositories)\n")
    md.append("| Priority | Feature | Kind | Repos (app) | Occurrences (app) | Repositories | Example file in the heaviest user |\n|---|---|---|---|---|---|---|")
    for i, r in enumerate(top, 1):
        heavy = max(r["repos_app"], key=lambda nm: r["per_repo"][nm]["app"])
        ex = r["examples"].get(heavy, "")
        md.append(f"| {i} | `{r['label']}` | {r['kind']} | {r['nrepos']}/{n} | {r['app']} | {', '.join(r['repos_app'])} | {heavy}: `{ex}` |")
    md.append("")
    if len(nsup) > 10:
        md.append("Next five: " + ", ".join(f"`{r['label']}` ({r['nrepos']}/{n})" for r in nsup[10:15]) + ".\n")

    md.append("## 5. Secondary indexes\n")
    md.append("| Index kind | Repos defining it (application code) | Occurrences (app) | Our status |\n|---|---|---|---|")
    idx_rows = []
    for key in ("index: compound", "index: text", "index: TTL (expireAfterSeconds)", "index: unique", "index: partialFilterExpression", "index: sparse", "index: 2dsphere / 2d (geo)", "index: hashed / wildcard", "index: any definition"):
        f = feats.get(key)
        k = len(f["repos_app"]) if f else 0
        occ = f["app"] if f else 0
        st = INDEX_STATUS[key][0]
        idx_rows.append((key, k, occ, st))
        who = f" ({', '.join(f['repos_app'])})" if f and f["repos_app"] else ""
        md.append(f"| {key.replace('index: ', '')} | {k}/{n}{who} | {occ} | {st} |")
    md.append("")
    k = {key: kk for key, kk, _, _ in idx_rows}
    half = n / 2
    k_text_q = len(feats['$text|query']['repos_app']) if '$text|query' in feats else 0
    verdict = lambda kk: "in most repositories" if kk > half else ("in about half" if kk == half else "in a minority of repositories")
    md.append(f"**Do compound, TTL and text indexes appear in most repositories?** Compound indexes: {k['index: compound']}/{n} ({verdict(k['index: compound'])}); TTL indexes: {k['index: TTL (expireAfterSeconds)']}/{n} ({verdict(k['index: TTL (expireAfterSeconds)'])}); text indexes: {k['index: text']}/{n} ({verdict(k['index: text'])}); unique indexes: {k['index: unique']}/{n}. Caveat: text and geo indexes are detected only when the key list is written inline at the call, and `$text` queries appear in {k_text_q}/{n} repositories (against {k['index: text']}/{n} inline text-index definitions), so text indexes are under-counted; treat the real figure as at least `$text`'s. Our server records every index definition but only enforces `unique`; compound and text definitions have no effect on queries, and TTL indexes silently never expire documents, which is a functional gap, not just a performance one.\n")

    md.append("## 6. Per-repository detail\n")
    for r in scanned:
        top5 = sorted(((k, v) for k, v in {**r["operators"], **r["methods"], **r["indexes"]}.items() if v["app"] and display(k)[2] == NOT), key=lambda kv: -kv[1]["app"])[:6]
        md.append(f"- **{r['name']}** (`{r.get('commit','')}`): {r['files']['scanned_app']} app / {r['files']['scanned_test']} test files; most-used unsupported features: " + (", ".join(f"`{display(k)[0]}` ×{v['app']}" for k, v in top5) or "none") + ".")
    md.append("\n## Reproduce\n")
    md.append("```bash\npython3 tools/compat-scan/scan.py run --tmp /some/scratch/dir      # clones (depth 1) and scans each repo in turn, deleting each clone; writes tools/compat-scan/results.json\npython3 tools/compat-scan/scan.py report                          # writes compat-scan.md\n```\n")
    open(a.out, "w").write("\n".join(md))
    print(f"wrote {a.out}: {n} repos scanned, {len(skipped)} skipped, {len(app_rows)} features, {len(nsup)} not supported")
