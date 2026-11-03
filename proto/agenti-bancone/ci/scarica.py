"""M25 · scarica dal runner le traiettorie della classifica tau2 (S3 pubblico di Sierra, senza credenziali)
e ne tiene solo i campi che servono: ruolo, ora, chiamate (nome, argomenti), risposta (primi 600 caratteri).
Uso: python scarica.py <run.json> <cartella_uscita>"""
import gzip, json, os, sys, time, urllib.request, urllib.error

BASE = "https://sierra-tau-bench-public.s3.us-west-2.amazonaws.com/submissions"


def get(url, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=300) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
            time.sleep(3)
        except Exception:
            time.sleep(3)
    return None


def compact_sim(s):
    out = {"id": s.get("id"), "task_id": s.get("task_id"), "trial": s.get("trial"),
           "start": s.get("start_time"), "end": s.get("end_time"), "m": []}
    for m in s.get("messages") or []:
        tcs = m.get("tool_calls")
        if isinstance(tcs, str):
            tcs = None
        calls = [[tc.get("id"), tc.get("name"), tc.get("arguments") or {}, tc.get("requestor")] for tc in (tcs or [])]
        c = m.get("content")
        if not isinstance(c, str):
            c = json.dumps(c) if c is not None else ""
        out["m"].append([m.get("role"), m.get("timestamp"), calls or None, m.get("id"),
                         bool(m.get("error")) if m.get("error") is not None else None,
                         m.get("requestor"), c[:600]])
    return out


def main():
    cfg = json.load(open(sys.argv[1]))
    od = sys.argv[2]
    os.makedirs(od, exist_ok=True)
    conti = {"file": [], "byte_scaricati": 0}
    for sub, dom, fname in cfg["tau2_classifica"]:
        tag = f"{sub}__{dom}"
        url = f"{BASE}/{sub}/trajectories/{fname}"
        raw = get(url)
        lay = "flat"
        sims = None
        if raw is not None:
            conti["byte_scaricati"] += len(raw)
            try:
                d = json.loads(raw)
                sims = d.get("simulations") or []
            except Exception as e:
                print(tag, "json rotto", e, flush=True)
        if not sims:
            lay = "dir"
            idx = get(f"{BASE}/{sub}/trajectories/{dom}/results.json")
            if idx is not None:
                conti["byte_scaricati"] += len(idx)
                d = json.loads(idx)
                sims = d.get("simulations") or []
                if not sims or not sims[0].get("messages"):
                    lst = d.get("simulation_index") or sims
                    sims = []
                    for e in lst[: cfg.get("max_sim_dir", 2000)]:
                        b = get(f"{BASE}/{sub}/trajectories/{dom}/simulations/{e['id']}.json")
                        if b:
                            conti["byte_scaricati"] += len(b)
                            sims.append(json.loads(b))
        n = len(sims or [])
        print(tag, lay, n, "conversazioni", flush=True)
        conti["file"].append({"tag": tag, "layout": lay, "conversazioni": n, "url": url})
        if not n:
            continue
        with gzip.open(f"{od}/{tag}.jsonl.gz", "wt") as f:
            for s in sims:
                f.write(json.dumps(compact_sim(s)) + "\n")
        sims = None
    json.dump(conti, open(f"{od}/conti.json", "w"), indent=1)


main()
