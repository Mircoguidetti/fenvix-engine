"""Trasforma le traiettorie pubbliche di DAB (Data Agent Benchmark, UC Berkeley + Hasura,
arXiv 2603.20576, file "all_trajectories" su Google Drive) in eventi di lettura.

Ogni run_N.json è la conversazione di un agente (5 modelli, 54 domande, 50 tentativi per domanda)
su 2-3 database (SQLite, DuckDB, PostgreSQL, MongoDB). Si tengono le chiamate query_db e list_db.
Il banco è di sola lettura: nessuna scrittura, quindi le tre regole di validità coincidono.

Le traiettorie NON hanno l'ora: i tempi sono un modello (STIMATO), dichiarato qui.
  - dentro una conversazione: un turno dell'agente ogni STEP secondi (default 10 s, il tempo di
    una risposta del modello); le chiamate dello stesso turno hanno lo stesso istante;
  - ambiti:
      sessione:  ogni conversazione da sola;
      paralleli: i 50 tentativi della stessa domanda partono insieme (Berkeley, "field agents");
      condiviso: tutte le conversazioni dello stesso modello sullo stesso insieme di dati,
                 con inizi a caso in un PERIODO (default 24 h): tanti agenti di un'azienda che
                 fanno domande (anche uguali) allo stesso database durante il giorno.
"""
import ast, glob, json, os, random, re

try:
    import sqlglot
    from sqlglot import exp
except Exception:  # pragma: no cover
    sqlglot = None


def norm_ws(s):
    return re.sub(r"\s+", " ", s.strip().rstrip(";")).strip()


def is_mongo(q):
    q = q.strip()
    if not q.startswith("{"):
        return None
    try:
        o = json.loads(q)
    except Exception:
        return None
    return o if isinstance(o, dict) else None


def strip_leaves(o):
    if isinstance(o, dict):
        return {k: strip_leaves(v) for k, v in sorted(o.items())}
    if isinstance(o, list):
        return [strip_leaves(v) for v in o]
    return "?"


_cache = {}


def sql_parse(q):
    if q in _cache:
        return _cache[q]
    r = None
    if sqlglot is not None:
        try:
            r = sqlglot.parse_one(q, read="duckdb")
        except Exception:
            try:
                r = sqlglot.parse_one(q)
            except Exception:
                r = None
    _cache[q] = r
    return r


def keys(db_name, q):
    """-> (qkey esatta, forma senza costanti, sotto-unità, collezione, tipo)"""
    m = is_mongo(q)
    if m is not None:
        coll = str(m.get("collection"))
        exact = json.dumps(m, sort_keys=True)
        shape = json.dumps(strip_leaves(m), sort_keys=True)
        subs = set()
        if isinstance(m.get("filter"), dict) and m.get("filter"):
            subs.add("F:" + coll + json.dumps(m["filter"], sort_keys=True))
        pipe = m.get("pipeline")
        if isinstance(pipe, list):
            for k in range(1, len(pipe)):
                subs.add("P:" + coll + json.dumps(pipe[:k], sort_keys=True))
        return f"{db_name}|{exact}", f"{db_name}|{shape}", subs, coll, "mongo"
    t = sql_parse(q)
    if t is None:
        e = norm_ws(q)
        sh = re.sub(r"'[^']*'", "?", e)
        sh = re.sub(r"\b\d+(\.\d+)?\b", "?", sh)
        return f"{db_name}|{e}", f"{db_name}|{sh}", set(), "?", "sql_nonparsata"
    try:
        exact = t.sql(normalize=True, pretty=False)
    except Exception:
        exact = norm_ws(q)
    try:
        t2 = t.copy()
        for lit in list(t2.find_all(exp.Literal)):
            lit.replace(exp.Placeholder())
        shape = t2.sql(normalize=True)
    except Exception:
        shape = exact
    subs = set()
    tables = sorted({tb.name.lower() for tb in t.find_all(exp.Table)})
    try:
        for sel in t.find_all(exp.Select):
            if sel is t:
                continue
            subs.add("S:" + sel.sql(normalize=True))
        # unità di scansione: tabella + condizioni che toccano solo lei (sola tabella nel FROM)
        for sel in t.find_all(exp.Select):
            w = sel.args.get("where")
            frm = sel.args.get("from")
            if w is not None and frm is not None and not sel.args.get("joins"):
                subs.add("W:" + frm.sql(normalize=True) + " " + w.sql(normalize=True))
    except Exception:
        pass
    return f"{db_name}|{exact}", f"{db_name}|{shape}", subs, (tables[0] if tables else "?"), "sql"


def load_run(path):
    d = json.load(open(path))
    res = {}
    for m in d:
        if m.get("role") == "tool":
            res[m.get("tool_call_id")] = m.get("content") or ""
    calls = []
    turn = 0
    for m in d:
        if m.get("role") != "assistant" or not m.get("tool_calls"):
            continue
        tcs = m["tool_calls"]
        if isinstance(tcs, str):
            try:
                tcs = ast.literal_eval(tcs)
            except Exception:
                try:
                    tcs = json.loads(tcs)
                except Exception:
                    continue
        for tc in tcs:
            fn = tc.get("function") or {}
            name = fn.get("name")
            if name not in ("query_db", "list_db"):
                continue
            a = fn.get("arguments")
            if isinstance(a, str):
                try:
                    a = json.loads(a)
                except Exception:
                    a = {"raw": a}
            content = res.get(tc.get("id"), "")
            ok = "executed successfully" in content[:200]
            body = content.split("The result is:", 1)[-1] if ok else content
            calls.append({"turn": turn, "tool": name, "args": a if isinstance(a, dict) else {"raw": str(a)},
                          "ok": ok, "size": len(body.encode())})
        turn += 1
    return calls


def all_runs(root):
    out = []
    for f in sorted(glob.glob(os.path.join(root, "*", "*", "query*", "run_*.json"))):
        parts = f.split(os.sep)
        model, ds, q, run = parts[-4], parts[-3], parts[-2], parts[-1]
        out.append((model, ds, q, int(run[4:-5]), f))
    return out


def events(runs_calls, scope, step=10.0, period=86400.0, seed=7, only_ok=False):
    """runs_calls: lista di (model, ds, q, run, calls)."""
    rnd = random.Random(seed)
    ev = []
    for model, ds, q, run, calls in runs_calls:
        if scope == "sessione":
            db, t0 = f"{model}/{ds}/{q}/{run}", 0.0
        elif scope == "paralleli":
            db, t0 = f"{model}/{ds}/{q}", 0.0
        else:
            db, t0 = f"{model}/{ds}", rnd.random() * period
        for c in calls:
            if only_ok and not c["ok"]:
                continue
            a = c["args"]
            dbn = str(a.get("db_name"))
            if c["tool"] == "list_db":
                qk, sh, subs, coll, kind = f"{dbn}|LIST", f"{dbn}|LIST", set(), "*", "list"
            else:
                qq = a.get("query")
                if not isinstance(qq, str):
                    qq = json.dumps(qq, sort_keys=True)
                qk, sh, subs, coll, kind = keys(dbn, qq)
            ev.append({"t": t0 + c["turn"] * step, "db": db, "coll": coll, "kind": "read", "qkey": qk,
                       "shape": sh, "toks": [], "size": c["size"], "grp": kind, "subs": subs,
                       "model": model, "ds": ds})
    return ev
