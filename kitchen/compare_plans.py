"""K2b · the one-to-one check, before any measurement ($0, no connection): the whole program, dry, twice: once on the
Atlas state files (what Atlas read in that round) and once on ours (merge_states.py). Same steps, same clients in every
step, same 2 KB clients. It uses code-irlanda's simula_programma.py unchanged (the real script --a-secco for every
step, then the consumption with the script's own functions), and records the clients each step consumes.

Usage: compare_plans.py ATLAS_STATES OUR_STATES PROGRAM_PASSI_TXT OUT_DIR
Prints "PLANS: SAME" (exit 0) or "PLANS: DIFFERENT" / "PLANS: FERMO" (exit 1). OUT_DIR gets both dry runs and
plans.json (per step: clients read on Atlas, and whether ours are the same)."""
import contextlib
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


def dry_run(states, program, log_path):
    rec = []
    real = S.consuma

    def recording(cartella, opz, nome_passo):
        att, gr, conti = real(cartella, opz, nome_passo)
        rec.append({"step": nome_passo, "clients": sorted(att), "clients_2kb": sorted(gr)})
        return att, gr, conti

    S.consuma = recording
    S.FERMI.clear()
    buf = io.StringIO()
    with tempfile.TemporaryDirectory() as t:
        c = os.path.join(t, "stati")
        shutil.copytree(states, c)
        with contextlib.redirect_stdout(buf):
            S.esegui_programma(c, program, os.path.basename(os.path.dirname(program)))
            print("== never-read clients left")
            S.restano(c)
    S.consuma = real
    stops = list(S.FERMI)
    open(log_path, "w").write(buf.getvalue() + "".join(f"FERMO: {x}\n" for x in stops))
    return rec, stops


def main():
    atlas, ours, program, out = sys.argv[1:5]
    os.makedirs(out, exist_ok=True)
    ra, fa = dry_run(atlas, program, os.path.join(out, "dry-run-atlas.txt"))
    ro, fo = dry_run(ours, program, os.path.join(out, "dry-run-kitchen.txt"))
    steps, diff = [], []
    for k in range(max(len(ra), len(ro))):
        a = ra[k] if k < len(ra) else {"step": "missing", "clients": [], "clients_2kb": []}
        o = ro[k] if k < len(ro) else {"step": "missing", "clients": [], "clients_2kb": []}
        same = a == o
        steps.append({"step": a["step"], "clients": len(a["clients"]), "clients_2kb": len(a["clients_2kb"]),
                      "same_on_kitchen": same, "atlas_clients": a["clients"]})
        if not same:
            diff.append(f"{a['step']}: Atlas {len(a['clients'])} clients, kitchen {len(o['clients'])} "
                        f"(in common {len(set(a['clients']) & set(o['clients']))})")
    json.dump({"steps": steps, "fermo_atlas": fa, "fermo_kitchen": fo, "different": diff},
              open(os.path.join(out, "plans.json"), "w"), indent=1)
    for s in steps:
        print(f"{'same' if s['same_on_kitchen'] else 'DIFF'} {s['step']}: {s['clients']} clients"
              + (f", {s['clients_2kb']} of 2 KB" if s["clients_2kb"] else ""))
    for x in fa:
        print("FERMO on the Atlas states: " + x)
    for x in fo:
        print("FERMO on our states: " + x)
    for x in diff:
        print("DIFFERENT " + x)
    if fo or (fa != fo):
        print("PLANS: FERMO")
        sys.exit(1)
    if diff or not ra:
        print("PLANS: DIFFERENT")
        sys.exit(1)
    print(f"PLANS: SAME ({len(steps)} steps, {sum(s['clients'] for s in steps)} clients, as on Atlas)")


if __name__ == "__main__":
    main()
