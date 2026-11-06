"""K2c (K4 of the reviewer of K2b) · on the client machine, after every size: did our cook really serve the clients of
the plan, phase by phase? Until now only the local test checked it (check_runs.py, point 2); on AWS nobody did.
Three checks, all from files the round left (no connection):
  1. per step and per phase: the clients in the script's measures (fase_*.json, every dict with an integer "i" and a
     result) are the
     clients of the dry plan for that step and phase (piano_giro16 of the script, run dry by code-irlanda's
     simula_programma.py on the same starting state files the size used);
  2. per step: the clients the step consumed in the dry plan (as compare_plans.py, the plan Atlas really followed);
  3. per database: the clients that became "used" during the size (state file and register after the size minus
     before it) are exactly the plan's clients of that database (the reviewer's real_vs_plan.py method on Atlas).
A step whose phases left no measure file at all (a step that broke) is a difference too: the size is then not one to one.
Usage: check_reads.py STATES_BEFORE STATES_AFTER PASSI_TXT DATA_DIR OUT_JSON
Prints the differences and the last line "READS: ONE TO ONE (...)" (exit 0) or "READS: NOT ONE TO ONE: ..." (exit 1)."""
import contextlib
import glob
import io
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
for d in (os.path.join(HERE, "irlanda"), HERE):
    if os.path.exists(os.path.join(d, "simula_programma.py")):
        sys.path.insert(0, d)
        break
import simula_programma as S  # noqa: E402

SKIP_PHASES = {"inizio", "fine", "carico_massimo"}  # no clients of the plan (inizio, fine) or no new clients (massimo)


def phase_key(f):
    return "parallelo" if str(f).startswith("parallelo") else str(f)


def clients_in(x, out):
    """every planned task (dict with "query" and an integer "i") anywhere in x"""
    if isinstance(x, dict):
        if "query" in x and isinstance(x.get("i"), int):
            out.add(x["i"])
        for v in x.values():
            clients_in(x=v, out=out)
    elif isinstance(x, list):
        for v in x:
            clients_in(v, out)
    return out


def dry_plan(states, passi):
    """(giro name -> phase -> clients) from piano_giro16, and (step label -> state, clients consumed)"""
    phases, steps = {}, []
    real_plan, real_consume = S.m.piano_giro16, S.consuma

    def plan(st, a, nome, fasi):
        r = real_plan(st, a, nome, fasi)
        p = r[0] if isinstance(r, tuple) else r
        g = phases.setdefault(nome, {})
        for f, v in (p or {}).items():
            g.setdefault(phase_key(f), set()).update(clients_in(v, set()))
        return r

    def consume(cartella, opz, nome_passo):
        att, gr, conti = real_consume(cartella, opz, nome_passo)
        w = nome_passo.split()  # "<program> passo <k> <type> <state>" (the dormienti step runs on a temporary copy)
        steps.append({"step": nome_passo, "state": w[4] if len(w) > 4 else os.path.basename(cartella.rstrip("/")),
                      "giro": S.P.opzione(opz, "--giro"), "clients": set(att)})
        return att, gr, conti

    S.m.piano_giro16, S.consuma = plan, consume
    S.FERMI.clear()
    try:
        with tempfile.TemporaryDirectory() as t, contextlib.redirect_stdout(io.StringIO()):
            c = os.path.join(t, "stati")
            shutil.copytree(states, c)
            S.esegui_programma(c, passi, os.path.basename(os.path.dirname(os.path.abspath(passi))) or "program")
    finally:
        S.m.piano_giro16, S.consuma = real_plan, real_consume
    return phases, steps, list(S.FERMI)


def measured(data_dir):
    """(giro, phase) -> clients in the measures; giro -> number of phase files"""
    got, files = {}, {}
    for f in glob.glob(os.path.join(data_dir, "**", "fase_*.json"), recursive=True):
        try:
            d = json.load(open(f))
        except Exception:
            continue
        giro = d.get("giro")
        files[giro] = files.get(giro, 0) + 1

        fase = d.get("fase")

        def walk(x):
            # a measure: an integer client "i" and a result ("ms" for the parallel ones, "freddo"/"errori" for the single
            # ones); the plan's own tasks in the file (no result) are not counted, so a planned client never read is seen
            if isinstance(x, dict):
                if isinstance(x.get("i"), int) and any(k in x for k in ("ms", "freddo", "errori", "solo_piano")):
                    got.setdefault((giro, phase_key(x.get("fase") or fase)), set()).add(x["i"])
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)
        walk(d)
    return got, files


def main():
    before, after, passi, data, out = sys.argv[1:6]
    phases, steps, stops = dry_plan(before, passi)
    got, files = measured(data)
    diff, rows, n_ok = [], [], 0
    # 1. per step (giro) and phase
    for giro, ph in sorted(phases.items()):
        for f, plan in sorted(ph.items()):
            if f in SKIP_PHASES or not plan:
                continue
            seen = got.get((giro, f), set())
            ok = seen == plan
            n_ok += ok
            rows.append({"giro": giro, "phase": f, "plan": len(plan), "measured": len(seen), "same": ok,
                         "plan_not_read": sorted(plan - seen)[:20], "read_not_in_plan": sorted(seen - plan)[:20]})
            if not ok:
                diff.append(f"{giro} {f}: plan {len(plan)}, measured {len(seen)} (missing {len(plan - seen)}, "
                            f"outside the plan {len(seen - plan)})")
    # measures of a (giro, phase) the plan does not have
    for (giro, f), seen in sorted(got.items(), key=lambda x: (str(x[0][0]), x[0][1])):
        if f in SKIP_PHASES or giro not in phases or f in phases[giro]:
            continue
        if seen:
            diff.append(f"{giro} {f}: {len(seen)} clients measured in a phase the plan does not have")
    # 2. steps that consume clients but left no phase file
    for s in steps:
        if s["clients"] and not files.get(s["giro"]):
            diff.append(f"{s['step']} ({s['giro']}): {len(s['clients'])} planned clients, no phase file")
    # 3. per database: newly used clients == the plan's clients of that database
    per_state = {}
    for s in steps:
        per_state.setdefault(s["state"], set()).update(s["clients"])
    used = []
    for st, plan in sorted(per_state.items()):
        a0, _ = S.usati_di(os.path.join(before, st))
        try:
            a1, _ = S.usati_di(os.path.join(after, st))
        except FileNotFoundError:
            a1 = a0
        new = a1 - a0
        ok = new == plan
        used.append({"state": st, "plan": len(plan), "newly_used": len(new), "same": ok})
        if not ok:
            diff.append(f"state {st}: plan {len(plan)} clients, newly used {len(new)} (missing {len(plan - new)}, "
                        f"outside the plan {len(new - plan)})")
    diff += [f"FERMO in the dry plan: {x}" for x in stops]
    json.dump({"one_to_one": not diff, "phases": rows, "per_state": used, "differences": diff},
              open(out, "w"), indent=1)
    for r in used:
        print(f"{'same' if r['same'] else 'DIFF'} state {r['state']}: plan {r['plan']}, newly used {r['newly_used']}")
    for x in diff[:30]:
        print("DIFFERENT " + x)
    total = sum(r["plan"] for r in rows)
    if diff:
        print(f"READS: NOT ONE TO ONE: {len(diff)} differences ({'; '.join(diff[:3])})")
        sys.exit(1)
    print(f"READS: ONE TO ONE ({n_ok} phases, {total} planned reads, {sum(r['plan'] for r in used)} clients in "
          f"{len(used)} databases, all served)")


if __name__ == "__main__":
    main()
