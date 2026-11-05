"""K2b · on the client machine: load our kitchen with the same databases as Atlas, from a load plan
(load-plans/NAME.txt), with the founder's script unchanged.

For every 'carica' line the plan is checked against the Atlas state file of the same STATE (database name, size,
client size, dormant share, seed: the things that decide which clients exist and which are dormant). The load then
stops at the fraction Atlas reached (load_until.py), so the fake year ends on the same day. 'giro' lines (the 2 KB
collection) run after the 'carica' of the same STATE. Different STATEs run at the same time.

Usage: load.py PLAN ATLAS_STATES WORK OUT
  ATLAS_STATES/<state>/risultati_p1.json   the Atlas state files (code-irlanda, the commit the Atlas round started from)
  WORK/<state>/                            one folder per database: script copy, our state file, raw data
  OUT/load/                                terminals, load-summary.json
FENVIX_P1_URI in the environment (our server). Exit code 0 = every step done and every fraction within tolerance."""
import datetime as dt
import json
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
GB = 1 << 30
YEAR_START = dt.datetime(2025, 11, 1, tzinfo=dt.timezone.utc)  # T_INIZIO of the script
YEAR_S = 365 * 86400
TOLERANCE = 0.005  # half a point of the target: about 1.8 days of the fake year


def find(name):
    for d in (HERE, os.path.join(HERE, "irlanda")):
        if os.path.exists(os.path.join(d, name)):
            return os.path.join(d, name)
    raise FileNotFoundError(name)


def option(args, name, default=None):
    for i, x in enumerate(args):
        if x == name and i + 1 < len(args):
            return args[i + 1]
    return default


def read_plan(path):
    steps = []
    for n, line in enumerate(open(path), 1):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        p = shlex.split(line)
        if len(p) < 2 or p[1] not in ("carica", "giro"):
            raise SystemExit(f"plan line {n}: expected 'STATE carica|giro OPTIONS'")
        for banned in ("--file", "--cartella-dati", "--forza"):
            if banned in p[2:]:
                raise SystemExit(f"plan line {n}: {banned} is set by load.py")
        steps.append({"line": n, "state": p[0], "cmd": p[1], "args": p[2:]})
    return steps


def end_day(fraction):
    return (YEAR_START + dt.timedelta(seconds=min(fraction, 1.0) * YEAR_S)).strftime("%Y-%m-%d")


def check_against_atlas(step, atlas):
    """The options that decide the clients must be the Atlas ones. Returns (fraction to stop at, problems)."""
    a = step["args"]
    cfg = atlas.get("config") or {}
    car = atlas.get("caricamento") or {}
    problems = []
    if option(a, "--db") != atlas.get("db"):
        problems.append(f"--db {option(a, '--db')} but Atlas loaded {atlas.get('db')}")
    gb = float(option(a, "--gb", "1"))
    mb = float(option(a, "--mb-cliente", "2.0"))
    quota = float(option(a, "--quota-riservati", "0.30"))
    seed = int(option(a, "--seme", "42"))
    threads = int(option(a, "--thread", "4"))
    n = max(50, int(gb * 1024 / mb))
    for what, ours, theirs in (("gb", gb, cfg.get("gb")), ("mb per client", mb, cfg.get("mb_cliente")),
                               ("seed", seed, cfg.get("seme")), ("clients", n, cfg.get("clienti")),
                               ("dormant", int(n * quota), cfg.get("riservati")), ("threads", threads, cfg.get("thread"))):
        if theirs is not None and ours != theirs:
            problems.append(f"{what}: plan {ours}, Atlas {theirs}")
    if not car.get("byte_bson") or not cfg.get("gb"):
        problems.append("the Atlas state file has no load size (caricamento.byte_bson)")
        return 1.0, problems
    reached = car["byte_bson"] / (cfg["gb"] * GB)
    fraction = 1.0 if car.get("motivo_fine") == "obiettivo raggiunto" else reached
    return fraction, problems


def run_state(state, steps, atlas_dir, work, out, results, lock):
    d = os.path.join(work, state)
    os.makedirs(os.path.join(d, "dati"), exist_ok=True)
    shutil.copy(find("prova_infinite.py"), d)
    shutil.copy(find("load_until.py"), d)
    atlas = json.load(open(os.path.join(atlas_dir, state, "risultati_p1.json")))
    res = {"state": state, "db": atlas.get("db"), "steps": [], "ok": True}
    for st in steps:
        term = os.path.join(out, f"{state}-line{st['line']:02d}-{st['cmd']}.txt")
        t0 = time.time()
        if st["cmd"] == "carica":
            fraction, problems = check_against_atlas(st, atlas)
            if problems:
                res["ok"] = False
                res["steps"].append({"line": st["line"], "cmd": "carica", "refused": problems})
                break
            res["atlas_fraction"] = round((atlas["caricamento"]["byte_bson"]) / (atlas["config"]["gb"] * GB), 6)
            res["atlas_end_day"] = end_day(res["atlas_fraction"])
            res["stop_fraction"] = fraction
            cmd = [sys.executable, "load_until.py", repr(fraction), "carica", "--file", "risultati_p1.json"] + st["args"]
        else:
            cmd = [sys.executable, "prova_infinite.py", "giro", "--file", "risultati_p1.json", "--cartella-dati", "dati"] \
                + st["args"]
        with open(term, "w") as t:
            rc = subprocess.run(cmd, cwd=d, stdout=t, stderr=subprocess.STDOUT).returncode
        info = {"line": st["line"], "cmd": st["cmd"], "rc": rc, "seconds": round(time.time() - t0, 1)}
        ours = json.load(open(os.path.join(d, "risultati_p1.json"))) if os.path.exists(
            os.path.join(d, "risultati_p1.json")) else {}
        if st["cmd"] == "carica":
            car = ours.get("caricamento") or {}
            gb = float(option(st["args"], "--gb"))
            reached = (car.get("byte_bson") or 0) / (gb * GB)
            info.update({"reached_fraction": round(reached, 6), "end_day": end_day(reached), "reason": car.get("motivo_fine"),
                         "bson_gb": round((car.get("byte_bson") or 0) / GB, 3), "documents": car.get("documenti"),
                         "mb_s": car.get("mb_s")})
            target = res["stop_fraction"]
            if rc != 0 or not car.get("finito") or abs(reached - target) > TOLERANCE and not (target >= 1.0 and reached >= 1.0):
                res["ok"] = False
                info["problem"] = f"reached {100 * reached:.2f}% instead of {100 * min(target, 1.0):.2f}% (code {rc})"
        else:
            g = ours.get("grandi") or {}
            info.update({"2kb_done": bool(g.get("finito")), "2kb_clients": g.get("clienti"), "2kb_bytes": g.get("byte")})
            if rc != 0 or not g.get("finito"):
                res["ok"] = False
                info["problem"] = f"2 KB collection not finished (code {rc})"
        res["steps"].append(info)
        if not res["ok"]:
            break
    with lock:
        results.append(res)


def main():
    plan, atlas_dir, work, out = sys.argv[1:5]
    out = os.path.join(out, "load")
    os.makedirs(out, exist_ok=True)
    steps = read_plan(plan)
    by_state = {}
    for st in steps:
        by_state.setdefault(st["state"], []).append(st)
    for s, ss in by_state.items():
        if ss[0]["cmd"] != "carica":
            raise SystemExit(f"state {s}: the first step must be 'carica'")
        if not os.path.exists(os.path.join(atlas_dir, s, "risultati_p1.json")):
            raise SystemExit(f"state {s}: no Atlas state file in {atlas_dir}")
    results, lock = [], threading.Lock()
    t0 = time.time()
    ths = [threading.Thread(target=run_state, args=(s, ss, atlas_dir, work, out, results, lock)) for s, ss in by_state.items()]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    results.sort(key=lambda r: r["state"])
    ok = all(r["ok"] for r in results)
    summary = {"ok": ok, "seconds": round(time.time() - t0, 1), "states": results}
    json.dump(summary, open(os.path.join(out, "load-summary.json"), "w"), indent=1)
    for r in results:
        c = next((x for x in r["steps"] if x["cmd"] == "carica"), {})
        print(f"{'OK  ' if r['ok'] else 'FAIL'} {r['state']:8s} {r.get('db')}: reached {100 * c.get('reached_fraction', 0):.2f}% "
              f"(Atlas {100 * r.get('atlas_fraction', 0):.2f}%), fake year up to {c.get('end_day')} (Atlas "
              f"{r.get('atlas_end_day')}), {c.get('bson_gb')} GB BSON, {c.get('mb_s')} MB/s"
              + "".join(f"; {x.get('problem') or x.get('refused')}" for x in r["steps"] if x.get("problem") or x.get("refused")))
    print(f"LOAD: {'DONE' if ok else 'FAILED'} in {summary['seconds']:.0f} s")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
