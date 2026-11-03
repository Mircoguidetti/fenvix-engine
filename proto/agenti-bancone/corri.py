"""M25 · tutte le corse. Uso: python corri.py risultati/tutto.json

Variabili: TAU2 (clone di tau2-bench), TAUB (clone di tau-bench), DAB (all_trajectories di M18), SWE (trajs/).
"""
import json
import os
import sys
import time
from collections import defaultdict

import numpy as np

import bancone
import fonti_agenti as fonti

HERE = os.path.dirname(os.path.abspath(__file__))
TAUB = os.environ.get("TAUB", "/tmp/taub/historical_trajectories")
DAB = os.environ.get("DAB", "/tmp/dabraw/all_trajectories")
SWE = os.environ.get("SWE", "/tmp/swem/trajs")
BOARD = f"{HERE}/tracce/tau2-classifica"

ALL = bancone.POLICIES
SHORT = ("", "a", "b", "c", "ab", "cb", "abc")


def variants(convs, H, H2, k=4):
    """Caso base e varianti di sensibilità (SOGLIE.md)."""
    v = {"base": bancone.run_source(convs, k=k, hist_bytes=H)}
    v[f"storia_{int(H2 / 1e6)}MB"] = bancone.run_source(convs, k=k, hist_bytes=H2, policies=SHORT)
    v["permanenza_5min"] = bancone.run_source(convs, k=k, hist_bytes=H, ttl=300, policies=SHORT)
    v["bancone_nvme"] = bancone.run_source(convs, k=k, hist_bytes=H, nvme=True, policies=SHORT)
    v["s3_lento"] = bancone.run_source(convs, k=k, hist_bytes=H, med=50.0, p99=400.0, policies=SHORT)
    v["condivisi_freddi_per_conversazione"] = bancone.run_source(convs, k=k, hist_bytes=H,
                                                                shared_mode="conv", policies=SHORT)
    return v


def tau2_all():
    paper, board = [], []
    owners = {}
    files = []
    for tag, dom, f in fonti.tau2_paper_files():
        owners.setdefault(dom, fonti.Owners(dom))
        cs = fonti.tau2_convs(tag, dom, fonti.tau2_paper_sims(f), owners[dom])
        paper += cs
        files.append({"fonte": "articolo", "file": tag, "dominio": dom, "conversazioni": len(cs)})
    for tag, dom, f in fonti.tau2_board_files(BOARD):
        owners.setdefault(dom, fonti.Owners(dom))
        cs = fonti.tau2_convs(tag, dom, fonti.tau2_board_sims(f), owners[dom])
        board += cs
        files.append({"fonte": "classifica", "file": tag, "dominio": dom, "conversazioni": len(cs)})
    return paper, board, owners, files


def step_model_from_paper():
    ag, us = [], []
    for tag, dom, f in fonti.tau2_paper_files():
        for s in fonti.tau2_paper_sims(f):
            prev = None
            for m in s["m"]:
                t = fonti._ts(m[1])
                if prev and t:
                    if m[0] == "assistant":
                        ag.append(t - prev)
                    elif m[0] == "user" and not m[2]:
                        us.append(t - prev)
                if t:
                    prev = t
    return fonti.StepModel(ag, us), {"passo_agente_mediana_s": round(float(np.median(ag)), 2),
                                      "risposta_utente_mediana_s": round(float(np.median(us)), 2)}


def info(convs):
    return {"conversazioni": len(convs), "con_cliente": sum(1 for c in convs if c["main"]),
            "passi_modello": bancone.steps_summary(convs), "margine": bancone.first_read_delay(convs)}


def main():
    out = {}
    T = time.time()
    paper, board, owners, files = tau2_all()
    out["tau2_file"] = files
    out["condivisi_byte"] = {d: {"quota_condivisa": round(100 * o.bytes_shared / o.bytes_total, 1),
                                 "tabelle_condivise_byte": o.shared_tables, "totale_byte": o.bytes_total,
                                 "storia_media_cliente_byte": round(o.hist_mean)} for d, o in owners.items()}
    print("tau2 letti", time.time() - T, flush=True)
    out["tau2_articolo"] = {"info": info(paper), **variants(paper, 2e6, 20e6)}
    print("tau2 articolo", time.time() - T, flush=True)
    out["tau2_classifica"] = {"info": info(board), **variants(board, 2e6, 20e6)}
    print("tau2 classifica", time.time() - T, flush=True)
    by = defaultdict(list)
    for c in paper + board:
        by[c["dom"]].append(c)
    out["tau2_per_dominio"] = {d: {"info": info(cs), "base": bancone.run_source(cs, policies=SHORT),
                                   "condivisi_freddi_per_conversazione": bancone.run_source(
                                       cs, policies=("", "a", "c"), shared_mode="conv")}
                               for d, cs in by.items()}
    print("tau2 domini", time.time() - T, flush=True)
    sm, smi = step_model_from_paper()
    out["modello_tempi_senza_ora"] = smi
    tb = []
    for f, dom in (("gpt-4o-retail", "retail"), ("gpt-4o-airline", "airline"),
                   ("sonnet-35-new-retail", "retail"), ("sonnet-35-new-airline", "airline")):
        tb += fonti.taubench_convs(f"{TAUB}/{f}.json", dom, owners.get(dom) or fonti.Owners(dom), sm)
    out["taubench"] = {"info": info(tb), **variants(tb, 2e6, 20e6)}
    print("taubench", time.time() - T, flush=True)
    dc = fonti.dab_convs(DAB, sm, max_per_q=10)
    out["dab"] = {"info": info(dc), "base": bancone.run_source(dc, hist_bytes=200e6, policies=SHORT),
                  "storia_2000MB": bancone.run_source(dc, hist_bytes=2e9, policies=SHORT),
                  "storia_20MB": bancone.run_source(dc, hist_bytes=20e6, policies=SHORT),
                  "s3_lento": bancone.run_source(dc, hist_bytes=200e6, med=50.0, p99=400.0, policies=SHORT)}
    print("dab", time.time() - T, flush=True)
    sw = fonti.swe_convs(SWE)
    out["swe"] = {"info": info(sw), **variants(sw, 20e6, 200e6)}
    print("swe", time.time() - T, flush=True)
    json.dump(out, open(sys.argv[1], "w"), indent=1)


if __name__ == "__main__":
    main()
