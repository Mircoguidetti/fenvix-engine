"""Extract, for every differential test in mongo-tests/tests.json, the set of inventory ids it exercises.

Context matters because operator names are shared between roles ($set: update operator vs stage, $sum: expression vs
accumulator vs window function, $elemMatch: query vs projection). Document bodies that tests insert (`docs`, `doc`) are
never scanned: only filters, updates, pipelines, projections, sorts, commands and options count as "exercised".
A feature id is only reported if it exists in the inventory (`known`)."""
import re

UPDATE_OPS_ARRAYMOD = ("$each", "$position", "$slice", "$sort")


class Walker:
    def __init__(self, known):
        self.known = known
        self.F = set()

    def add(self, fid):
        if fid in self.known:
            self.F.add(fid)

    # ---- query predicates
    def q(self, x, top=True):
        if isinstance(x, list):
            for v in x: self.q(v)
        elif isinstance(x, dict):
            for k, v in x.items():
                if k.startswith("$"):
                    self.add("qop:" + k)
                    if k == "$expr": self.e(v)
                    elif k in ("$and", "$or", "$nor", "$not", "$elemMatch", "$all", "$in", "$nin"): self.q(v)
                    elif k == "$jsonSchema": self.schema(v)
                    elif isinstance(v, (dict, list)): self.gen_q(v)
                else:
                    self.q(v, False)

    def gen_q(self, x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k.startswith("$"): self.add("qop:" + k)
                self.gen_q(v)
        elif isinstance(x, list):
            for v in x: self.gen_q(v)

    def schema(self, x):
        pass

    # ---- projection of find
    def proj(self, p):
        if not isinstance(p, dict): return
        for k, v in p.items():
            if k.endswith(".$") or k == "$": self.add("proj:$")
            if isinstance(v, dict):
                for op, arg in v.items():
                    if op in ("$slice", "$meta", "$elemMatch"):
                        self.add("proj:" + op)
                        if op == "$elemMatch": self.q(arg)
                    elif op.startswith("$"):
                        self.add("expr:" + op); self.e(arg)
                    else:
                        self.e(v)

    def sort(self, s):
        if isinstance(s, dict):
            for k, v in s.items():
                if isinstance(v, dict) and "$meta" in v:
                    self.add("proj:$meta")
                    if v.get("$meta") == "textScore": self.add("sorthint:$meta textScore (sort)")
                if k == "$natural": self.add("sorthint:$natural (sort)")

    # ---- update documents
    def u(self, x):
        if isinstance(x, list):
            self.pipeline(x); return
        if not isinstance(x, dict): return
        for op, spec in x.items():
            self.add("upd:" + op)
            if not isinstance(spec, dict): continue
            for k, v in spec.items():
                if re.search(r"\$\[\]", k): self.add("upd:$[]")
                if re.search(r"\$\[[A-Za-z0-9_]+\]", k): self.add("upd:$[<identifier>]")
                if re.search(r"\.\$(\.|$)", k) or k == "$": self.add("upd:$")
                if isinstance(v, dict):
                    for m in v:
                        if m in UPDATE_OPS_ARRAYMOD: self.add("upd:" + m)
                if op == "$pull": self.q(v)
        # `$set`-style specs with aggregation-expression values are not allowed in classic updates

    # ---- aggregation
    def pipeline(self, p):
        if not isinstance(p, list): return
        for st in p:
            if not isinstance(st, dict): continue
            for name, arg in st.items():
                self.add("stage:" + name)
                if name == "$match": self.q(arg)
                elif name in ("$group",): self.group(arg)
                elif name in ("$bucket", "$bucketAuto"):
                    if isinstance(arg, dict):
                        self.e(arg.get("groupBy")); self.group(arg.get("output", {}), no_id=True)
                elif name == "$setWindowFields": self.window(arg)
                elif name == "$lookup":
                    if isinstance(arg, dict):
                        self.pipeline(arg.get("pipeline")); self.e(arg.get("let"))
                elif name == "$unionWith":
                    if isinstance(arg, dict): self.pipeline(arg.get("pipeline"))
                elif name == "$facet":
                    if isinstance(arg, dict):
                        for pl in arg.values(): self.pipeline(pl)
                elif name == "$graphLookup":
                    if isinstance(arg, dict): self.q(arg.get("restrictSearchWithMatch"))
                elif name == "$geoNear":
                    if isinstance(arg, dict): self.q(arg.get("query"))
                elif name == "$merge":
                    if isinstance(arg, dict) and isinstance(arg.get("whenMatched"), list): self.pipeline(arg["whenMatched"])
                elif name in ("$sort", "$limit", "$skip", "$count", "$unset", "$sample", "$out", "$indexStats", "$collStats", "$changeStream"):
                    if name == "$sort": self.sort(arg)
                elif name == "$densify" or name == "$fill":
                    self.e(arg)
                else:
                    self.e(arg)

    def group(self, g, no_id=False):
        if not isinstance(g, dict): return
        for k, v in g.items():
            if k == "_id" and not no_id:
                self.e(v)
            elif isinstance(v, dict):
                for op, arg in v.items():
                    if op.startswith("$"):
                        self.add("acc:" + op); self.e(arg)
                        if op == "$accumulator" and isinstance(arg, dict):
                            for f in arg: self.add("js:$accumulator." + f)
            else:
                self.e(v)

    def window(self, w):
        if not isinstance(w, dict): return
        self.e(w.get("partitionBy"))
        out = w.get("output")
        for k, spec in (out.items() if isinstance(out, dict) else []):
            if isinstance(spec, dict):
                for op, arg in spec.items():
                    if op == "window": continue
                    if op.startswith("$"):
                        self.add("win:" + op); self.e(arg)

    def e(self, x):
        if isinstance(x, list):
            for v in x: self.e(v)
        elif isinstance(x, dict):
            for k, v in x.items():
                if k == "$function" and isinstance(v, dict):
                    for f in v: self.add("js:$function." + f)
                if k.startswith("$"):
                    self.add("expr:" + k)
                    if k == "$literal": continue
                    if k == "$meta": continue
                self.e(v)


def step_features(w, step):
    """Features of one step (recursing into with_txn / watch sub-steps)."""
    op = step["op"]
    A = w.add
    f = step.get("filter")
    if op in ("find", "find_one", "find_count"):
        A("cmd:find"); w.q(f); w.proj(step.get("projection")); w.sort(step.get("sort"))
        h = step.get("hint")
        if isinstance(h, dict) and "$natural" in h or (isinstance(h, list) and any(x and x[0] == "$natural" for x in h)): A("sorthint:$natural (hint)")
        for k, o in (("filter", "filter"), ("projection", "projection"), ("sort", "sort"), ("hint", "hint"), ("limit", "limit"),
                     ("skip", "skip"), ("batch_size", "batchSize")):
            if (k in step and k != "filter") or (k == "filter" and f):
                A("opt:find." + o)
        if op == "find_one": A("opt:find.limit")
    elif op == "count_documents":
        A("cmd:aggregate"); w.q(f)
        if "skip" in step: A("stage:$skip")
        if "limit" in step: A("stage:$limit")
        A("stage:$match"); A("stage:$group")
    elif op == "estimated_count":
        A("cmd:count")
    elif op == "aggregate":
        A("cmd:aggregate"); w.pipeline(step.get("pipeline"))
        if "batch_size" in step: A("opt:aggregate.cursor")
    elif op == "distinct":
        A("cmd:distinct"); A("opt:distinct.key"); w.q(f)
        if f: A("opt:distinct.query")
    elif op in ("update_one", "update_many", "replace_one"):
        A("cmd:update"); w.q(f); w.u(step.get("update"))
        A("opt:update.updates[].q"); A("opt:update.updates[].u")
        if op == "update_many": A("opt:update.updates[].multi")
        if step.get("upsert"): A("opt:update.updates[].upsert")
        if "array_filters" in step: A("opt:update.updates[].arrayFilters"); w.q(step["array_filters"])
    elif op in ("delete_one", "delete_many"):
        A("cmd:delete"); w.q(f); A("opt:delete.deletes[].q"); A("opt:delete.deletes[].limit")
    elif op in ("find_one_and_update", "find_one_and_replace", "find_one_and_delete"):
        A("cmd:findAndModify"); w.q(f); A("opt:findAndModify.query")
        if op == "find_one_and_update": w.u(step.get("update")); A("opt:findAndModify.update")
        if op == "find_one_and_replace": A("opt:findAndModify.update")
        if op == "find_one_and_delete": A("opt:findAndModify.remove")
        if "sort" in step: A("opt:findAndModify.sort"); w.sort(step["sort"])
        if "projection" in step: A("opt:findAndModify.fields"); w.proj(step["projection"])
        if step.get("upsert"): A("opt:findAndModify.upsert")
        if step.get("return_document") == "after": A("opt:findAndModify.new")
        if "array_filters" in step: A("opt:findAndModify.arrayFilters"); w.q(step["array_filters"])
    elif op in ("insert_one", "insert_many", "insert_generated", "insert_one_w0", "parallel_inserts"):
        A("cmd:insert"); A("opt:insert.documents")
        if op == "insert_many": A("opt:insert.ordered")
        if op == "insert_one_w0": A("opt:insert.writeConcern"); A("wc:w: 0 (unacknowledged)")
    elif op == "create_collection": A("cmd:create")
    elif op == "drop_collection": A("cmd:drop")
    elif op in ("list_collection_names", "list_collections"): A("cmd:listCollections")
    elif op == "drop_database": A("cmd:dropDatabase")
    elif op in ("db_in_list", "db_in_list_command"): A("cmd:listDatabases")
    elif op == "list_indexes": A("cmd:listIndexes")
    elif op == "drop_index": A("cmd:dropIndexes")
    elif op == "create_index":
        A("cmd:createIndexes")
        index_features(w, step)
    elif op in ("command", "command_raw", "admin_command"):
        command_features(w, step.get("cmd") or {})
    elif op == "session_insert":
        A("cmd:insert"); A("cmd:endSessions")
    elif op == "ping_with_server_api":
        A("cmd:ping"); A("stableapi:apiVersion")
    elif op == "client_with_compression":
        A("wire:OP_COMPRESSED"); A("cmd:insert"); A("cmd:find")
    elif op == "raw_op_query":
        A("wire:OP_QUERY"); A("cmd:isMaster")
    elif op in ("cursor_batches", "cursor_batches_agg", "cursor_kill_after", "cursor_close_midway", "getmore_bogus"):
        A("cmd:getMore")
        if op == "cursor_batches": A("cmd:find"); A("opt:find.batchSize")
        if op == "cursor_batches_agg": A("cmd:aggregate"); w.pipeline(step.get("pipeline"))
        if op in ("cursor_kill_after", "cursor_close_midway"): A("cmd:killCursors"); A("cmd:find")
    elif op == "watch":
        A("stage:$changeStream")
        w.pipeline(step.get("pipeline"))
        for s in step.get("actions", []): step_features(w, s)
    elif op == "with_txn":
        for s in step.get("steps", []): step_features(w, s)
        A("cmd:commitTransaction")
    elif op == "txn_commit": A("cmd:commitTransaction")
    elif op == "txn_abort": A("cmd:abortTransaction")


INDEX_OPT = {"unique": "unique", "sparse": "sparse", "expireAfterSeconds": "expireAfterSeconds", "partialFilterExpression": "partialFilterExpression",
             "collation": "collation", "weights": "weights", "default_language": "default_language", "language_override": "language_override",
             "textIndexVersion": "textIndexVersion", "2dsphereIndexVersion": "2dsphereIndexVersion", "bits": "bits", "min": "min", "max": "max",
             "wildcardProjection": "wildcardProjection", "storageEngine": "storageEngine", "name": "name", "hidden": "hidden", "v": "v"}
PROP = {"unique": "unique index", "sparse": "sparse index", "expireAfterSeconds": "TTL index", "partialFilterExpression": "partial index",
        "hidden": "hidden index", "collation": "case-insensitive index"}


def index_features(w, step, opts=None):
    A = w.add
    keys = step.get("keys") or step.get("key") or []
    if isinstance(keys, dict): keys = list(keys.items())
    kinds = [str(k[1]) if isinstance(k, (list, tuple)) else "" for k in keys]
    names = [str(k[0]) if isinstance(k, (list, tuple)) else "" for k in keys]
    if len(keys) == 1 and kinds[0] in ("1", "-1"): A("idxtype:single field index")
    if len(keys) > 1: A("idxtype:compound index")
    if "text" in kinds: A("idxtype:text index")
    if "2dsphere" in kinds: A("idxtype:2dsphere")
    if "2d" in kinds: A("idxtype:2d")
    if "hashed" in kinds: A("idxtype:hashed index")
    if any(n.endswith("$**") for n in names):
        A("idxtype:wildcard index")
        if len(keys) > 1: A("idxtype:compound wildcard index")
    o = dict(step.get("opts") or {})
    for k in ("unique", "name", "sparse", "expireAfterSeconds", "partialFilterExpression", "collation", "hidden"):
        if k in step: o[k] = step[k]
    for k, v in o.items():
        if k in INDEX_OPT: A("opt:createIndexes.indexes[]." + INDEX_OPT[k])
        if k in PROP and v not in (False, None): A("idxprop:" + PROP[k])
        if k == "partialFilterExpression": w.q(v)
        if k == "collation": collation_features(w, v)


def collation_features(w, c):
    if not isinstance(c, dict): return
    for k in c:
        w.add("coll:" + k)
    loc = c.get("locale")
    if isinstance(loc, str):
        w.add("locale:" + loc)
        if loc == "simple": w.add("coll:simple (binary) collation")


def command_features(w, cmd, depth=0):
    if not isinstance(cmd, dict) or not cmd: return
    name = next(iter(cmd))
    cname = {"findandmodify": "findAndModify", "ismaster": "isMaster", "buildinfo": "buildInfo"}.get(name, name)
    w.add("cmd:" + cname)
    for k, v in cmd.items():
        if k == name: continue
        w.add(f"opt:{cname}.{k}")
        if k in ("filter", "query", "q"): w.q(v)
        if k == "projection": w.proj(v)
        if k == "sort": w.sort(v)
        if k == "pipeline": w.pipeline(v)
        if k in ("update", "u"): w.u(v)
        if k == "validator": w.q(v)
        if k == "collation": collation_features(w, v)
        if k == "readConcern" and isinstance(v, dict) and "level" in v: w.add(f'rc:level "{v["level"]}"')
        if k == "writeConcern" and isinstance(v, dict):
            for wk, wv in v.items():
                if wk == "w" and wv == "majority": w.add('wc:w: "majority"')
                elif wk == "w" and isinstance(wv, int) and wv > 0: w.add("wc:w: <number>")
                elif wk == "w" and wv == 0: w.add("wc:w: 0 (unacknowledged)")
                elif wk == "j": w.add("wc:j")
                elif wk == "wtimeout": w.add("wc:wtimeout")
        if k in ("updates", "deletes", "indexes") and isinstance(v, list):
            pre = k + "[]."
            for item in v:
                if not isinstance(item, dict): continue
                for ik, iv in item.items():
                    w.add(f"opt:{cname}.{pre}{ik}")
                if k == "updates": w.q(item.get("q")); w.u(item.get("u"))
                if k == "deletes": w.q(item.get("q"))
                if k == "indexes":
                    ks = item.get("key", {})
                    index_features(w, {"keys": list(ks.items()), "opts": {x: y for x, y in item.items() if x != "key"}})
        if name == "explain" and k == "explain": pass
    if name == "explain" and isinstance(cmd.get("explain"), dict):
        command_features(w, cmd["explain"], depth + 1)
    if name == "count": w.q(cmd.get("query"))
    if name == "aggregate": w.pipeline(cmd.get("pipeline"))
    if name == "findAndModify" or name == "findandmodify":
        w.q(cmd.get("query")); w.u(cmd.get("update"))


def unescape(x):
    if isinstance(x, dict):
        return {("$" + k[2:] if k in ("@@regex", "@@options") else k): unescape(v) for k, v in x.items()}
    if isinstance(x, list):
        return [unescape(v) for v in x]
    return x


def features_of_test(test, known):
    test = unescape(test)
    w = Walker(known)
    for st in test["steps"]:
        step_features(w, st)
    return w.F
