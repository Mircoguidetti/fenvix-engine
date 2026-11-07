"""Point 1 and 2: for every agent run that writes, its length, writes, customers written, read-your-own-writes and
multi-document writes. See METHOD.md for the definitions.

Every loader returns a list of runs; a run is a dict
  src, dom, model, id
  t0, t1          first and last message time (s); None if the trace has no clock
  calls           list of (t, kind, name, args, content) with kind 'r' or 'w' (successful agent calls only)
and `measure(run, db)` adds the metrics.
"""
import ast
import glob
import gzip
import json
import os
import random
import re
from datetime import datetime

from docs_model import DB, IGNORE, KB_TOOLS, Mapper, ttype, unwrap_call

DATA = os.environ.get("DR_DATA", "/home/user/data")
TAU2 = os.environ.get("TAU2", f"{DATA}/tau2")
BOARD = os.environ.get("BOARD", f"{DATA}/tau2-board")
TAUB = os.environ.get("TAUB", f"{DATA}/taub/historical_trajectories")
DAB = os.environ.get("DAB", f"{DATA}/dabraw/all_trajectories")
SWE = os.environ.get("SWE", f"{DATA}/swem/trajs")

GAP_CAP = 120.0


def _ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


# ---------------------------------------------------------------- tau2 (paper and 2026 leaderboard)

def _compact(s):
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
    out = []
    for f in sorted(glob.glob(f"{TAU2}/data/tau2/results/final/*_4trials.json")):
        b = os.path.basename(f)
        for dom in ("retail", "airline", "telecom"):
            if f"_{dom}_default_" in b or f"_{dom}_base_" in b:
                out.append((b.split(f"_{dom}_")[0], dom, f))
    return out


def tau2_board_files():
    out = []
    for f in sorted(glob.glob(f"{BOARD}/*.jsonl.gz")):
        tag = os.path.basename(f)[:-9]
        out.append((tag.split("__")[0], tag.split("__")[-1], f))
    return out


def tau2_sims(path):
    if path.endswith(".gz"):
        with gzip.open(path, "rt") as f:
            return [json.loads(l) for l in f]
    return [_compact(s) for s in json.load(open(path))["simulations"]]


def tau2_runs(src, model, dom, sims):
    runs = []
    for s in sims:
        msgs = s["m"]
        res = {m[3]: m for m in msgs if m[0] == "tool"}
        times = [_ts(m[1]) for m in msgs if _ts(m[1])]
        calls = []
        gaps = []
        agent_gaps = []   # gaps that end in an agent or tool message (the simulated customer's turns left out)
        prev = None
        for m in msgs:
            t = _ts(m[1])
            if t is not None:
                if prev is not None and t >= prev:
                    gaps.append(t - prev)
                    if m[0] != "user":
                        agent_gaps.append(t - prev)
                prev = t
            if m[0] != "assistant" or not m[2]:
                continue
            for cid, name, args, req in m[2]:
                if req not in (None, "assistant"):
                    continue
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        args = {}
                name, args = unwrap_call(name, args or {})
                if name in IGNORE:
                    continue
                tm = res.get(cid)
                if tm is None or tm[4]:
                    continue  # failed call: nothing read or written
                content = tm[6] or ""
                if content.startswith("Error") or content.startswith('"Error'):
                    continue
                if name in KB_TOOLS:
                    calls.append((t, "r", name, args, ""))
                    continue
                typ = ttype(name)
                if typ == "READ":
                    calls.append((t, "r", name, args, content))
                elif typ == "WRITE":
                    calls.append((t, "w", name, args, content))
        runs.append({"src": src, "dom": dom, "model": model, "id": f"{src}|{model}|{dom}|{s['id']}",
                     "t0": min(times) if times else None, "t1": max(times) if times else None,
                     "gaps": gaps, "agent_gaps": agent_gaps, "calls": calls})
    return runs


# ---------------------------------------------------------------- tau-bench original (no clock)

class StepModel:
    """ESTIMATED clock for traces without one: each agent message takes a model step and each user message a user
    step, both drawn from the distributions MEASURED on the tau2 paper (as M25)."""

    def __init__(self, agent_steps, user_steps, seed=11):
        self.a = sorted(x for x in agent_steps if 0 < x < 600)
        self.u = sorted(x for x in user_steps if 0 < x < 600)
        self.r = random.Random(seed)

    def agent(self):
        return self.r.choice(self.a)

    def user(self):
        return self.r.choice(self.u)

    @staticmethod
    def median(xs):
        s = sorted(xs)
        return s[len(s) // 2] if s else 0.0


def paper_step_distributions():
    """Model step = tool result -> next assistant message; user step = assistant message -> next user message."""
    a, u = [], []
    for model, dom, f in tau2_paper_files():
        for s in tau2_sims(f):
            prev_role, prev_t = None, None
            for m in s["m"]:
                t = _ts(m[1])
                if t is None:
                    continue
                if prev_t is not None:
                    if m[0] == "assistant" and prev_role == "tool":
                        a.append(t - prev_t)
                    elif m[0] == "user" and prev_role == "assistant":
                        u.append(t - prev_t)
                prev_role, prev_t = m[0], t
    return a, u


def taubench_runs(sm, deterministic=False):
    runs = []
    am, um = StepModel.median(sm.a), StepModel.median(sm.u)
    for f in sorted(glob.glob(f"{TAUB}/*.json")):
        b = os.path.basename(f)[:-5]
        dom = "retail" if b.endswith("retail") else "airline"
        model = b.rsplit("-", 1)[0]
        for i, x in enumerate(json.load(open(f))):
            msgs = x["traj"]
            t = 0.0
            calls, gaps, agent_gaps = [], [], []
            for j, m in enumerate(msgs):
                role = m.get("role")
                if role == "user":
                    dt = um if deterministic else sm.user()
                    t += dt
                    gaps.append(dt)
                    continue
                if role != "assistant":
                    continue
                dt = am if deterministic else sm.agent()
                t += dt
                gaps.append(dt)
                agent_gaps.append(dt)
                # results: the tool messages right after this assistant message, in call order (the Claude
                # trajectories number every call "0", so ids cannot be used)
                following = []
                for m2 in msgs[j + 1:]:
                    if m2.get("role") != "tool":
                        break
                    following.append(m2)
                for ci, tc in enumerate(m.get("tool_calls") or []):
                    fn = tc.get("function") or {}
                    name = fn.get("name")
                    args = fn.get("arguments") or {}
                    if isinstance(args, str):
                        try:
                            args = json.loads(args or "{}")
                        except Exception:
                            args = {}
                    if name in IGNORE or name is None:
                        continue
                    content = (following[ci] if ci < len(following) else {}).get("content") or ""
                    if content.startswith("Error"):
                        continue
                    typ = ttype(name)
                    if typ in ("READ", "WRITE"):
                        calls.append((t, "r" if typ == "READ" else "w", name, args, content[:600]))
            runs.append({"src": "taubench", "dom": dom, "model": model, "id": f"taub|{b}|{x['task_id']}|{i}",
                         "t0": 0.0, "t1": t, "gaps": gaps, "agent_gaps": agent_gaps, "calls": calls})
    return runs


# ---------------------------------------------------------------- metrics of one tau run

# append-only records that log the conversation rather than act for the customer: kept out in the "core" variant
LOG_WRITES = {"log_verification", "request_human_agent_transfer"}


def measure(run, db: DB, exclude=()):
    calls = [c for c in run["calls"] if not (c[1] == "w" and c[2] in exclude)]
    # the run's own customer: the first one it reads (fallback for documents whose owner cannot be resolved)
    mp0 = Mapper(db)
    main = None
    for t, k, name, args, content in calls:
        if k == "r":
            o = mp0.read_owners(mp0.reads(name, args, content))
            if o:
                main = sorted(o)[0]
                break
        else:
            o = db.owner_of_args(args)
            if o:
                main = o[0]
                break
    mp = Mapper(db, fallback_owner=main)
    written = {}          # doc key -> Doc
    write_calls = []      # (t, name, [doc keys])
    unknown = []
    ryw_doc = ryw_cust = False
    ryw_reads = 0
    first_access = {}     # customer -> first time any read touched them
    for t, k, name, args, content in calls:
        if k == "r":
            rd = mp.reads(name, args, content)
            owners = mp.read_owners(rd)
            for o in owners:
                first_access.setdefault(o, t)
            if written:
                hit = any(d.matches(tb, key) for d in written.values() for tb, key in rd if key is not None)
                if hit:
                    ryw_doc = True
                    ryw_reads += 1
                w_owners = {d.owner for d in written.values() if d.owner}
                if owners & w_owners:
                    ryw_cust = True
        else:
            docs, rule = mp.writes(name, args)
            if docs is None:
                unknown.append(name)
                continue
            keys = []
            for d in docs:
                if d.id is None:
                    continue
                written.setdefault(d.key, d)
                keys.append(d.key)
                if d.owner:
                    first_access.setdefault(d.owner, t)
            write_calls.append((t, name, keys))
    n_w = len(write_calls)
    custs = sorted({d.owner for d in written.values() if d.owner})
    shared = sorted({f"{d.table}/{d.id}" for d in written.values() if not d.owner})
    # documents per write call, and across calls
    call_docs = [set(k) for _, _, k in write_calls]
    within = any(len(s) >= 2 for s in call_docs)
    all_docs = set().union(*call_docs) if call_docs else set()
    # across calls: two or more write calls whose document sets are not all the same single document
    cross = n_w >= 2 and len(all_docs) >= 2
    out = {
        "n_writes": n_w, "write_tools": [w[1] for w in write_calls], "unknown_write_tools": unknown,
        "docs_written": len(all_docs), "customers_written": custs, "n_customers": len(custs),
        "shared_written": shared, "ryw_doc": ryw_doc, "ryw_reads": ryw_reads, "ryw_cust": ryw_cust or ryw_doc,
        "multi_doc_cross_calls": cross, "multi_doc_within_call": within, "owner_fallbacks": mp.fallbacks,
    }
    if run["t0"] is not None and run["t1"] is not None:
        out["length"] = run["t1"] - run["t0"]
        out["length_capped"] = sum(min(g, GAP_CAP) for g in run.get("gaps") or [])
        out["length_agent_only"] = sum(min(g, GAP_CAP) for g in run.get("agent_gaps") or [])
        if write_calls and write_calls[-1][0] is not None:
            tw = [w[0] for w in write_calls if w[0] is not None]
            fa = [first_access[c] for c in custs if first_access.get(c) is not None]
            out["span_first_write_last_write"] = tw[-1] - tw[0]
            out["span_txn"] = tw[-1] - (min(fa) if fa else tw[0])
            out["t_first_write_from_start"] = tw[0] - run["t0"]
    return out


# ---------------------------------------------------------------- DAB (data agents): do they write at all?

_DAB_WRITE = re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?"
                        r"(?:TABLE|VIEW|INDEX)|DROP\s+(?:TABLE|VIEW)|ALTER\s+TABLE|\$out|\$merge|insertOne|"
                        r"insertMany|updateOne|updateMany|deleteOne|deleteMany|replaceOne)", re.I)


def dab_summary():
    runs = queries = wq = 0
    writing = []
    for f in sorted(glob.glob(f"{DAB}/*/*/*/run_*.json")):
        runs += 1
        w = False
        for m in json.load(open(f)):
            for tc in m.get("tool_calls") or []:
                fn = tc.get("function") or tc
                if fn.get("name") != "query_db":
                    continue
                a = fn.get("arguments")
                a = a if isinstance(a, str) else json.dumps(a)
                queries += 1
                if _DAB_WRITE.search(a):
                    wq += 1
                    w = True
        if w:
            writing.append(os.path.relpath(f, DAB))
    return {"runs": runs, "queries": queries, "write_queries": wq, "writing_runs": len(writing),
            "writing_run_files": writing}


# ---------------------------------------------------------------- SWE-bench (code agents)

_READ_CMD = re.compile(r"\b(cat|sed|head|tail|nl|grep|less|awk|view|rg|diff)\b")
_EXEC_CMD = re.compile(r"\b(python3?|pytest|tox|make|bash\s+\S+\.sh|sh\s+\S+\.sh)\b")
_PATH = re.compile(r"(?<![\w/.-])((?:/[\w.+-]+)+|(?:\./)?[\w.+-]+(?:/[\w.+-]+)+|[\w+-]+\."
                   r"(?:py|pyx|pyi|c|h|cfg|toml|ini|rst|txt|md|json|yaml|yml|js|ts|html|css))(?![\w/])")
_REDIR = re.compile(r"(?<![0-9&])>>?\s*([^\s;&|<>]+)")
_TEE = re.compile(r"\btee\s+(?:-a\s+)?([^\s;&|]+)")
_SED_I = re.compile(r"\bsed\s+(?:-[a-zA-Z]*\s+)*-i\S*\s+(?:(?:-e\s+)?(?:'[^']*'|\"[^\"]*\"|\S+)\s+)+?([^\s;&|']+)\s*(?:$|[;&|])")
_PYWRITE = re.compile(r"open\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"][wa]")
_MVCP = re.compile(r"\b(?:cp|mv)\s+(?:-\S+\s+)*\S+\s+([^\s;&|]+)")
_RM = re.compile(r"\brm\s+(?:-\S+\s+)*([^\s;&|]+)")
_PATCH = re.compile(r"\b(patch|git\s+apply|git\s+checkout|git\s+stash|git\s+restore)\b")


def _norm(p):
    p = p.strip("'\"")
    if p.startswith("./"):
        p = p[2:]
    if p.startswith("/testbed/"):
        p = p[len("/testbed/"):]
    return p


def swe_writes(cmd):
    """Paths a shell command writes (inside the repository; /dev, /tmp and /proc excluded). '*' = unknown path."""
    out = set()
    for rx in (_REDIR, _TEE, _PYWRITE, _MVCP, _RM):
        for m in rx.finditer(cmd):
            out.add(_norm(m.group(1)))
    for line in cmd.split("\n"):
        if re.search(r"\bsed\b[^|;&]*\s-i", line):
            toks = [t for t in re.split(r"\s+", line.strip()) if t]
            for t in toks[::-1]:
                if _PATH.fullmatch(t.strip("'\"")) or re.fullmatch(r"[\w./+-]+\.\w+", t.strip("'\"")):
                    out.add(_norm(t))
                    break
    if _PATCH.search(cmd):
        out.add("*")
    out = {p for p in out if p and not p.startswith(("/dev", "/tmp", "/proc", "&")) and p not in ("1", "2")}
    return out


def _extra(x):
    if isinstance(x, dict):
        return x
    try:
        return ast.literal_eval(x)
    except Exception:
        return {}


def swe_runs():
    runs = []
    for f in sorted(glob.glob(f"{SWE}/*.traj.json")):
        d = json.load(open(f))
        iid = d.get("instance_id") or os.path.basename(f)
        tool_t = {}
        for m in d["messages"]:
            if m.get("role") == "tool":
                ex = _extra(m.get("extra"))
                if ex.get("timestamp"):
                    tool_t[m.get("tool_call_id")] = float(ex["timestamp"])
        start = None
        cmds = []
        for m in d["messages"]:
            if m.get("role") != "assistant":
                continue
            ex = _extra(m.get("extra"))
            if start is None:
                mm = re.search(r"'created': (\d+)", str(m.get("extra")))
                if mm:
                    start = float(mm.group(1)) - 1.0
            for a in ex.get("actions") or []:
                if isinstance(a, dict) and tool_t.get(a.get("tool_call_id")) is not None:
                    cmds.append((tool_t[a["tool_call_id"]], a.get("command", "")))
        if not cmds:
            continue
        cmds.sort()
        if start is None or start > cmds[0][0]:
            start = cmds[0][0] - 1.0
        written = set()
        wt = []
        ryw_strict = ryw_loose = False
        for t, c in cmds:
            reads = set()
            if _READ_CMD.search(c):
                reads = {_norm(p) for p in _PATH.findall(c)}
            if written:
                if reads & written or ("*" in written and reads):
                    ryw_strict = True
                if reads & written or _EXEC_CMD.search(c) or ("*" in written and reads):
                    ryw_loose = True
            w = swe_writes(c)
            if w:
                written |= w
                wt.append(t)
        times = [t for t, _ in cmds]
        gaps = [b - a for a, b in zip([start] + times, times)]
        r = {"src": "swe", "dom": "swe", "model": "gemini-3.5-flash", "id": f"swe|{iid}", "repo": iid.split("__")[0],
             "n_writes": len(wt), "docs_written": len(written - {"*"}) + (1 if "*" in written else 0),
             "n_customers": 1 if wt else 0, "customers_written": [iid.split("__")[0]] if wt else [],
             "shared_written": [], "ryw_doc": ryw_strict, "ryw_cust": ryw_loose,
             "multi_doc_cross_calls": len(wt) >= 2 and len(written) >= 2, "multi_doc_within_call": False,
             "length": times[-1] - start, "length_capped": sum(min(g, GAP_CAP) for g in gaps),
             "length_agent_only": sum(min(g, GAP_CAP) for g in gaps)}
        if wt:
            r["span_first_write_last_write"] = wt[-1] - wt[0]
            r["span_txn"] = wt[-1] - start
            r["t_first_write_from_start"] = wt[0] - start
        runs.append(r)
    return runs
