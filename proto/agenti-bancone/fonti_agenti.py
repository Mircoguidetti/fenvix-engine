"""M25 · dalle traiettorie degli agenti alle conversazioni per bancone.py.

Una conversazione è un dict:
  db     database a cui appartiene (le conversazioni dello stesso db condividono i dati condivisi)
  id     identificativo unico
  start  inizio della conversazione (s)
  main   gruppo del cliente/entità del compito (per la politica c), o None
  ev     lista di (t, tipo, oggetto, gruppo)
           tipo 'r' lettura, 'w' scrittura, 's' segnale (rivela il gruppo, nessun dato letto)
           gruppo None = dato condiviso; altrimenti il cliente/entità a cui appartiene
  passi  durate (s) dei passi del modello dopo la risposta di uno strumento (solo tracce con l'ora)

I gruppi e gli oggetti dei clienti sono già separati per conversazione (ogni tentativo è un cliente diverso);
gli oggetti condivisi no (sono gli stessi per tutto il database).
"""
import glob
import gzip
import json
import os
import random
import re
from datetime import datetime

TAU2 = os.environ.get("TAU2", "/tmp/tau2")

# ---------------------------------------------------------------- tipi degli strumenti (dal codice di tau2)

_TYPES = None


def tool_types():
    global _TYPES
    if _TYPES is None:
        _TYPES = {}
        for f in glob.glob(f"{TAU2}/src/tau2/domains/*/*.py"):
            src = open(f).read()
            for m in re.finditer(r"@is_(?:discoverable_)?tool\(ToolType\.(\w+)[^)]*\)\s*\n\s*def (\w+)", src):
                _TYPES.setdefault(m.group(2), m.group(1))
    return _TYPES


def ttype(name):
    t = tool_types().get(name)
    if t:
        return t
    if re.match(r"(get|find|search|list|check)_", name or ""):
        return "READ"
    return "WRITE"


# indici di ricerca del cliente: sempre caldi, sono segnali (SOGLIE.md)
SIGNAL_TOOLS = re.compile(r"^(find_user_id_by_\w+|get_customer_by_\w+|get_user_information_by_(name|email))$")
# strumenti che non toccano i dati
IGNORE = {"calculate", "transfer_to_human_agents", "get_current_time", "list_discoverable_agent_tools",
          "list_discoverable_user_tools", "unlock_discoverable_agent_tool", "give_discoverable_user_tool",
          "rewrite_context", "think"}
KB_TOOLS = {"KB_search", "grep", "KB_search_bm25", "KB_search_dense", "shell"}


# ---------------------------------------------------------------- proprietà dei dati (generica)

def _unwrap(tab):
    if isinstance(tab, dict) and "data" in tab and isinstance(tab["data"], (dict, list)):
        tab = tab["data"]
    if isinstance(tab, list):
        out = {}
        for r in tab:
            if isinstance(r, dict):
                rid = next((r[k] for k in r if k.endswith("_id") and isinstance(r[k], str)), None)
                if rid:
                    out[rid] = r
        return out
    return tab if isinstance(tab, dict) else {}


def _idlike(s):
    return isinstance(s, str) and 2 < len(s) < 48 and " " not in s and any(c.isdigit() for c in s)


def _walk(x, out):
    if isinstance(x, dict):
        for k, v in x.items():
            if _idlike(k):
                out.add(k)
            _walk(v, out)
    elif isinstance(x, list):
        for v in x:
            _walk(v, out)
    elif _idlike(x):
        out.add(x)


def load_db(dom):
    p = f"{TAU2}/data/tau2/domains/{dom}"
    if os.path.exists(f"{p}/db.json"):
        return json.load(open(f"{p}/db.json"))
    import tomllib
    return tomllib.load(open(f"{p}/db.toml", "rb"))


class Owners:
    """id -> padrone (utente/cliente) o None (condiviso). Generico sui database di tau2.

    1. tabella users/customers: la chiave del record è il padrone; tutti gli id dentro il record (liste di
       ordini, metodi di pagamento, linee, bollette) sono suoi.
    2. record con un campo user_id/customer_id: suoi.
    3. un id citato da record di un solo padrone diventa suo (es. il telefono di una linea); citato da più
       padroni (piani, voli, prodotti) resta condiviso.
    Quanto pesa la parte condivisa: byte JSON dei record condivisi sul totale.
    """

    def __init__(self, dom):
        db = load_db(dom)
        self.users = set()
        own = {}
        tables = {k: _unwrap(v) for k, v in db.items()}
        for tname, tab in tables.items():
            if tname in ("users", "customers"):
                for rid, r in tab.items():
                    uid = r.get("user_id") or r.get("customer_id") or rid
                    self.users.add(uid)
                    own[rid] = uid
                    own[uid] = uid
                    ids = set()
                    _walk(r, ids)
                    for i in ids:
                        own.setdefault(i, uid)
        for tname, tab in tables.items():
            for rid, r in tab.items():
                if isinstance(r, dict):
                    u = r.get("user_id") or r.get("customer_id")
                    if u:
                        own[rid] = u
                        own.setdefault(u, u)
                        self.users.add(u)
        refs = {}
        for tname, tab in tables.items():
            for rid, r in tab.items():
                u = own.get(rid)
                if u is None or not isinstance(r, dict):
                    continue
                for k, v in r.items():
                    if k.endswith("_id") and isinstance(v, str) and v != rid:
                        refs.setdefault(v, set()).add(u)
        for i, us in refs.items():
            if i not in own and len(us) == 1:
                own[i] = next(iter(us))
        # al contrario: un record senza padrone che cita un id di un solo padrone (movimenti di un conto,
        # segnalazioni fatte da un utente) è suo
        for tname, tab in tables.items():
            for rid, r in tab.items():
                if own.get(rid) is not None or not isinstance(r, dict):
                    continue
                us = {own[v] for k, v in r.items() if (k.endswith("_id") or k.endswith("er_id"))
                      and isinstance(v, str) and v != rid and own.get(v)}
                if len(us) == 1:
                    own[rid] = next(iter(us))
        self.own = own
        tot = sh = 0
        self.shared_tables = {}
        for tname, tab in tables.items():
            for rid, r in tab.items():
                b = len(json.dumps(r))
                tot += b
                if own.get(rid) is None:
                    sh += b
                    self.shared_tables[tname] = self.shared_tables.get(tname, 0) + b
        if dom == "banking_knowledge":
            kb = sum(os.path.getsize(f) for f in glob.glob(f"{TAU2}/data/tau2/domains/{dom}/documents/*"))
            sh += kb
            tot += kb
            self.shared_tables["documents (base di conoscenza)"] = kb
        self.bytes_total, self.bytes_shared = tot, sh
        # storia media di un cliente in byte (per H)
        per = {}
        for tname, tab in tables.items():
            for rid, r in tab.items():
                u = own.get(rid)
                if u:
                    per[u] = per.get(u, 0) + len(json.dumps(r))
        self.hist_mean = sum(per.values()) / max(1, len(per))

    def owner_of_args(self, args):
        found = []
        for k, v in (args or {}).items():
            vals = v if isinstance(v, list) else [v]
            for x in vals:
                if isinstance(x, dict):
                    found += self.owner_of_args(x)
                elif isinstance(x, str):
                    if x in self.users:
                        found.append(x)
                    elif x in self.own and self.own[x]:
                        found.append(self.own[x])
        return found


def owners_in_result(content, users):
    out = []
    if not content:
        return out
    for m in re.finditer(r'"(?:user_id|customer_id)"\s*:\s*"([^"]+)"', content):
        if m.group(1) in users:
            out.append(m.group(1))
    s = content.strip().strip('"')
    if s in users:
        out.append(s)
    return out


# ---------------------------------------------------------------- tau2 (articolo e classifica)

def _ts(s):
    return datetime.fromisoformat(s).timestamp() if s else None


def _compact_from_full(s):
    m2 = []
    for m in s["messages"]:
        tcs = m.get("tool_calls") or []
        calls = [[tc.get("id"), tc.get("name"), tc.get("arguments") or {}, tc.get("requestor")] for tc in tcs]
        c = m.get("content")
        if not isinstance(c, str):
            c = json.dumps(c) if c is not None else ""
        m2.append([m.get("role"), m.get("timestamp"), calls or None, m.get("id"),
                   bool(m.get("error")) if m.get("error") is not None else None, m.get("requestor"), c[:600]])
    return {"id": s["id"], "task_id": s.get("task_id"), "trial": s.get("trial"), "start": s.get("start_time"),
            "m": m2}


def tau2_paper_files():
    fs = sorted(glob.glob(f"{TAU2}/data/tau2/results/final/*_4trials.json"))
    out = []
    for f in fs:
        b = os.path.basename(f)
        for dom in ("retail", "airline", "telecom"):
            if f"_{dom}_default_" in b or f"_{dom}_base_" in b:
                out.append((b[:-5], dom, f))
    return out


def tau2_paper_sims(path):
    d = json.load(open(path))
    return [_compact_from_full(s) for s in d["simulations"]]


def tau2_board_files(dirp):
    out = []
    for f in sorted(glob.glob(f"{dirp}/*.jsonl.gz")):
        tag = os.path.basename(f)[:-9]
        dom = tag.split("__")[-1]
        out.append((tag, dom, f))
    return out


def tau2_board_sims(path):
    with gzip.open(path, "rt") as f:
        return [json.loads(l) for l in f]


def _unwrap_call(name, args):
    """call_discoverable_agent_tool(agent_tool_name, arguments) -> (nome vero, argomenti veri)."""
    if name in ("call_discoverable_agent_tool", "call_discoverable_user_tool"):
        n2 = args.get("agent_tool_name") or args.get("user_tool_name") or args.get("tool_name") or name
        a2 = args.get("arguments") or {}
        if isinstance(a2, str):
            try:
                a2 = json.loads(a2)
            except Exception:
                a2 = {}
        return n2, a2 if isinstance(a2, dict) else {}
    return name, args


def tau2_convs(dbtag, dom, sims, owners):
    """Conversazioni di un file di tau2. Solo le chiamate dell'agente (non quelle del telefono dell'utente)."""
    convs = []
    for s in sims:
        cid = f"{dbtag}#{s['id']}"
        msgs = s["m"]
        res = {}
        for m in msgs:
            if m[0] == "tool":
                res[m[3]] = m
        ev = []
        steps = []
        prev_role, prev_t = None, None
        main = None
        start = _ts(s.get("start")) or (_ts(msgs[0][1]) if msgs else 0.0)
        for m in msgs:
            role, ts = m[0], _ts(m[1])
            if role == "assistant" and m[2] and prev_role == "tool" and ts and prev_t:
                steps.append(ts - prev_t)
            if ts:
                prev_role, prev_t = role, ts
            if role != "assistant" or not m[2]:
                continue
            for cid_call, name, args, req in m[2]:
                if req not in (None, "assistant"):
                    continue
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        args = {}
                name, args = _unwrap_call(name, args or {})
                if name in IGNORE:
                    continue
                tm = res.get(cid_call)
                t_call = ts
                t_res = _ts(tm[1]) if tm and tm[1] else ts
                err = bool(tm[4]) if tm else True
                content = tm[6] if tm else ""
                arg_owners = owners.owner_of_args(args)
                res_owners = owners_in_result(content, owners.users) if not err else []
                if SIGNAL_TOOLS.match(name):
                    for u in res_owners + arg_owners:
                        ev.append((t_res, "s", None, f"{cid}|{u}"))
                        main = main or f"{cid}|{u}"
                    continue
                if name in KB_TOOLS:
                    ev.append((t_call, "r", f"{dbtag}|kb|{name}|{json.dumps(args, sort_keys=True)}", None))
                    continue
                typ = ttype(name)
                grp = arg_owners[0] if arg_owners else None
                if grp is None and typ == "READ" and res_owners:
                    grp = res_owners[0]
                g = f"{cid}|{grp}" if grp else None
                if g and main is None:
                    main = g
                # segnali (politica a): ogni padrone negli argomenti, al momento della chiamata
                for u in set(arg_owners):
                    ev.append((t_call, "s", None, f"{cid}|{u}"))
                key = json.dumps(args, sort_keys=True)
                if typ == "READ":
                    if err:
                        continue
                    if g:
                        ev.append((t_call, "r", f"{cid}|{name}|{key}", g))
                    else:
                        ev.append((t_call, "r", f"{dbtag}|{name}|{key}", None))
                    # i padroni rivelati dal risultato (es. dettagli di un ordine) sono segnali dopo
                    for u in set(res_owners) - set(arg_owners):
                        ev.append((t_res, "s", None, f"{cid}|{u}"))
                elif typ == "WRITE" and not err:
                    ev.append((t_call, "w", f"{cid}|{name}|{key}" if g else f"{dbtag}|{name}|{key}", g))
        ev.sort(key=lambda e: e[0])
        convs.append({"db": dbtag, "id": cid, "start": start, "main": main, "ev": ev, "passi": steps,
                      "dom": dom})
    return convs


# ---------------------------------------------------------------- tau-bench originale (senza ora)

class StepModel:
    """Tempi STIMATI per le tracce senza ora: passi del modello e risposte dell'utente presi a caso dalle
    distribuzioni MISURATE su tau2 (articolo)."""

    def __init__(self, agent_steps, user_steps, seed=11):
        self.a = sorted(x for x in agent_steps if 0 < x < 600)
        self.u = sorted(x for x in user_steps if 0 < x < 600)
        self.r = random.Random(seed)

    def agent(self):
        return self.r.choice(self.a)

    def user(self):
        return self.r.choice(self.u)


def taubench_convs(path, dom, owners, sm, conc=4.0, seed=5):
    """tau-bench (gpt-4o, Claude 3.5 Sonnet): messaggi in formato OpenAI, senza ora.
    Le conversazioni partono come un processo di Poisson con in media `conc` conversazioni insieme."""
    d = json.load(open(path))
    rnd = random.Random(seed)
    dbtag = "taub|" + os.path.basename(path)[:-5]
    convs = []
    t0 = 0.0
    mean_dur = 120.0
    for i, x in enumerate(d):
        cid = f"{dbtag}#{x['task_id']}#{x.get('trial', 0)}#{i}"
        t0 += rnd.expovariate(conc / mean_dur)
        t = t0
        ev = []
        main = None
        msgs = x["traj"]
        res = {m.get("tool_call_id"): m for m in msgs if m.get("role") == "tool"}
        for m in msgs:
            role = m.get("role")
            if role == "user":
                t += sm.user()
                continue
            if role != "assistant":
                continue
            t += sm.agent()
            for tc in m.get("tool_calls") or []:
                fn = tc.get("function") or {}
                name = fn.get("name")
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    args = {}
                if name in IGNORE or name is None:
                    continue
                tm = res.get(tc.get("id"))
                content = (tm or {}).get("content") or ""
                err = content.startswith("Error")
                arg_owners = owners.owner_of_args(args)
                res_owners = owners_in_result(content[:600], owners.users) if not err else []
                if SIGNAL_TOOLS.match(name):
                    for u in res_owners + arg_owners:
                        ev.append((t + 0.01, "s", None, f"{cid}|{u}"))
                        main = main or f"{cid}|{u}"
                    continue
                typ = ttype(name)
                grp = arg_owners[0] if arg_owners else (res_owners[0] if typ == "READ" and res_owners else None)
                g = f"{cid}|{grp}" if grp else None
                main = main or g
                for u in set(arg_owners):
                    ev.append((t, "s", None, f"{cid}|{u}"))
                key = json.dumps(args, sort_keys=True)
                if typ == "READ" and not err:
                    ev.append((t, "r", f"{cid}|{name}|{key}" if g else f"{dbtag}|{name}|{key}", g))
                    for u in set(res_owners) - set(arg_owners):
                        ev.append((t + 0.01, "s", None, f"{cid}|{u}"))
                elif typ == "WRITE" and not err:
                    ev.append((t, "w", f"{cid}|{name}|{key}" if g else f"{dbtag}|{name}|{key}", g))
        ev.sort(key=lambda e: e[0])
        convs.append({"db": dbtag, "id": cid, "start": t0, "main": main, "ev": ev, "passi": [], "dom": dom})
    return convs


# ---------------------------------------------------------------- DAB (agenti sui dati, senza ora)

_TBL = re.compile(r"\b(?:from|join)\s+([\"`\[]?[\w.]+[\"`\]]?)", re.I)


def _tables(db_name, q):
    q = q if isinstance(q, str) else json.dumps(q)
    out = set()
    try:
        j = json.loads(q)
        if isinstance(j, dict):
            c = j.get("collection") or j.get("coll") or j.get("from")
            if c:
                out.add(str(c))
    except Exception:
        pass
    m = re.search(r"db\.(\w+)\.", q)
    if m:
        out.add(m.group(1))
    for m in _TBL.finditer(q):
        out.add(m.group(1).strip('"`[]').lower())
    return out or {"?"}


def dab_convs(root, sm, conc=4.0, seed=9, max_per_q=10):
    """DAB: ogni conversazione lavora su un dataset (l'entità). Letture = query_db/list_db.
    Ogni conversazione ha la sua copia fredda del dataset (gruppo per conversazione).
    Tengo al massimo max_per_q tentativi per domanda e modello."""
    rnd = random.Random(seed)
    convs = []
    for model in sorted(os.listdir(root)):
        for ds in sorted(os.listdir(f"{root}/{model}")):
            dbtag = f"dab|{model}|{ds}"
            t0 = 0.0
            for q in sorted(os.listdir(f"{root}/{model}/{ds}")):
                runs = sorted(glob.glob(f"{root}/{model}/{ds}/{q}/run_*.json"))[:max_per_q]
                for r in runs:
                    try:
                        msgs = json.load(open(r))
                    except Exception:
                        continue
                    cid = f"{dbtag}#{q}#{os.path.basename(r)}"
                    t0 += rnd.expovariate(conc / 120.0)
                    t = t0
                    ev = []
                    g = f"{cid}|{ds}"
                    for m in msgs:
                        if m.get("role") == "user":
                            continue
                        if m.get("role") != "assistant":
                            continue
                        t += sm.agent()
                        tcs = m.get("tool_calls") or []
                        for tc in tcs:
                            fn = tc.get("function") or tc
                            name = fn.get("name")
                            args = fn.get("arguments") or {}
                            if isinstance(args, str):
                                try:
                                    args = json.loads(args)
                                except Exception:
                                    args = {}
                            if name == "list_db":
                                ev.append((t, "s", None, g))
                            elif name == "query_db":
                                dbn = args.get("db_name", "?")
                                ev.append((t, "s", None, g))
                                for tb in _tables(dbn, args.get("query", "")):
                                    ev.append((t, "r", f"{cid}|{dbn}|{tb}", g))
                    convs.append({"db": dbtag, "id": cid, "start": t0, "main": g, "ev": ev, "passi": [],
                                  "dom": "dab"})
    return convs


# ---------------------------------------------------------------- SWE-bench (agenti di codice, con l'ora)

_READ_CMD = re.compile(r"\b(cat|sed|head|tail|nl|grep|less|awk|view|rg)\b")
_PATH = re.compile(r"(?<![\w/.-])((?:/[\w.+-]+)+|(?:\./)?[\w.+-]+(?:/[\w.+-]+)+|[\w+-]+\.(?:py|pyx|pyi|c|h|cfg|toml|ini|rst|txt|md|json|yaml|yml|js|ts|html|css))(?![\w/])")


def _extra(x):
    if isinstance(x, dict):
        return x
    try:
        import ast
        return ast.literal_eval(x)
    except Exception:
        return {}


def swe_convs(root):
    """mini-swe-agent su SWE-bench Verified: entità = il repository del compito (/testbed).
    Lettura = un file citato da un comando di lettura. File fuori da /testbed (librerie installate) = condivisi.
    Ora: fine del comando (`timestamp` della risposta dello strumento); inizio = `created` della prima
    risposta del modello. Passo del modello = tempo tra la fine di un comando e la fine del successivo
    (comprende anche l'esecuzione del comando, che di solito è breve)."""
    convs = []
    for f in sorted(glob.glob(f"{root}/*.traj.json")):
        d = json.load(open(f))
        iid = d.get("instance_id") or os.path.basename(f)
        repo = iid.split("__")[0]
        cid = f"swe#{iid}"
        g = f"{cid}|{repo}"
        tool_t = {}
        for m in d["messages"]:
            if m.get("role") == "tool":
                ex = _extra(m.get("extra"))
                if ex.get("timestamp"):
                    tool_t[m.get("tool_call_id")] = float(ex["timestamp"])
        ev = []
        start = None
        times = []
        for m in d["messages"]:
            if m.get("role") != "assistant":
                continue
            ex = _extra(m.get("extra"))
            if start is None:
                mm = re.search(r"'created': (\d+)", str(m.get("extra")))
                if mm:
                    start = float(mm.group(1)) - 1.0
            acts = ex.get("actions") or []
            for a in acts:
                if not isinstance(a, dict):
                    continue
                t = tool_t.get(a.get("tool_call_id"))
                if t is None:
                    continue
                times.append(t)
                cmd = a.get("command", "")
                ev.append((t, "s", None, g))
                if not _READ_CMD.search(cmd):
                    continue
                for p in set(x for x in _PATH.findall(cmd)):
                    p2 = p[2:] if p.startswith("./") else p
                    if p2.startswith("/testbed/"):
                        p2 = p2[len("/testbed/"):]
                    if p2.startswith("/"):
                        if p2.startswith(("/dev", "/tmp", "/proc")):
                            continue
                        ev.append((t, "r", f"swe-shared|{p2}", None))
                    else:
                        ev.append((t, "r", f"{cid}|{p2}", g))
        if not ev:
            continue
        times = sorted(set(times))
        steps = [b - a for a, b in zip(times, times[1:])]
        if start is None or start > times[0]:
            start = times[0] - 1.0
        ev.sort(key=lambda e: e[0])
        convs.append({"db": f"swe|{repo}", "id": cid, "start": start, "main": g, "ev": ev, "passi": steps,
                      "dom": "swe"})
    return convs
