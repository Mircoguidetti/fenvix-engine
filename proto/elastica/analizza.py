#!/usr/bin/env python3
"""M17: legge le curve per minuto (proto/elastica/curve/<giro>/<parte>/*.npz), le unisce per famiglia,
lancia sim.bolletta su ogni curva e scrive risultati/riassunto.txt e risultati/risultati.json.
Uso: python3 analizza.py <giro> [<giro> ...]  (giri più recenti dopo: un nome uguale sostituisce il precedente)
"""
import glob
import json
import os
import sys

import numpy as np

import sim

QUI = os.path.dirname(os.path.abspath(__file__))


def carica(giri):
    file = {}
    for g in giri:
        for p in glob.glob(f"{QUI}/curve/{g}/*/*.npz"):
            file[os.path.basename(p)[:-4]] = p
    serie = {}
    for nome, p in sorted(file.items()):
        d = np.load(p)
        tot = d["r"] + d["w"]
        if len(tot) < 3:
            continue
        serie[nome] = dict(m0=int(d["m0"]), r=d["r"], w=d["w"], tot=tot, t_first=int(d["t_first"]), n=int(d["n"]))
    return serie


def taglia(x):
    """toglie i minuti vuoti all'inizio e alla fine, e il primo e l'ultimo minuto (parziali)."""
    nz = np.nonzero(x)[0]
    if len(nz) < 3:
        return x[:0]
    return x[nz[0] + 1: nz[-1]]


def somma(serie, pref):
    ss = [s for k, s in serie.items() if k.startswith(pref)]
    if not ss:
        return None, 0
    a = min(s["m0"] for s in ss)
    b = max(s["m0"] + len(s["tot"]) for s in ss)
    out = np.zeros(b - a)
    for s in ss:
        out[s["m0"] - a: s["m0"] - a + len(s["tot"])] += s["tot"]
    return out, len(ss)


def curve(serie):
    c = {}
    for k, s in serie.items():
        if k.startswith("tw") or k.startswith("me"):
            x = taglia(s["tot"].astype(float))
            if len(x) >= 1440:
                c[k] = x
    for fam in ("al", "te"):
        x, n = somma(serie, fam)
        if x is not None:
            c[f"{fam}-somma{n}"] = taglia(x)
    return {k: v for k, v in c.items() if len(v) >= 1440}  # almeno un giorno


def r0(x):
    return int(round(x))


def descrivi(nome, x):
    giorni = len(x) / 1440
    h = x[: (len(x) // 60) * 60].reshape(-1, 60).mean(axis=1)
    return (f"{nome}: {giorni:.1f} giorni, picco/media al minuto {x.max() / x.mean():.2f}, "
            f"all'ora {h.max() / h.mean():.2f}, minimo/media all'ora {h.min() / h.mean():.2f}, "
            f"p99/media al minuto {np.percentile(x, 99) / x.mean():.2f}")


VARIANTI = [
    ("base", {}),
    ("avvio10", dict(avvio_s=10)),
    ("avvio300", dict(avvio_s=300)),
    ("margine10", dict(margine=0.10)),
    ("margine50", dict(margine=0.50)),
    ("scrittura081", dict(w_rapporto=0.81)),
    ("xlarge", dict(tipo="c7g.xlarge")),
    ("portiereB1", dict(portiere=True)),
    ("inf_fatt_ora", dict(fatt_ora=True)),
    ("inf_min_M10", dict(tmin=0)),
    ("inf_min_M50", dict(tmin=4)),
    ("quaderno28k", dict(quaderno_tetto=28000)),
]


def main():
    giri = sys.argv[1:] or ["prova"]
    serie = carica(giri)
    cs = curve(serie)
    os.makedirs(f"{QUI}/risultati", exist_ok=True)
    righe, js = [], {}
    righe.append("Curve (MISURATO sulle tracce):")
    for k, x in cs.items():
        righe.append("  " + descrivi(k, x))
    for k, s in serie.items():
        if k.startswith("al") or k.startswith("te"):
            continue
        righe.append(f"  sorgente {k}: n={s['n']} t_first={s['t_first']} minuti={len(s['tot'])} "
                     f"scritture={s['w'].sum() / max(1, s['tot'].sum()):.2f}")
    for fam in ("al", "te"):
        tf = [s["t_first"] for k, s in serie.items() if k.startswith(fam)]
        if tf:
            wq = sum(s["w"].sum() for k, s in serie.items() if k.startswith(fam)) / max(
                1, sum(s["tot"].sum() for k, s in serie.items() if k.startswith(fam)))
            righe.append(f"  {fam}: {len(tf)} dischi, t_first min {min(tf)} mediana {int(np.median(tf))} max {max(tf)}, "
                         f"quota scritture {wq:.2f}")
    for k, x in cs.items():
        js[k] = {}
        for prof in "ab":
            for nv, kw in VARIANTI:
                b = sim.bolletta(x, prof, **kw)
                js[k][f"{prof}/{nv}"] = b
                if nv == "base":
                    ip, ir, fe = b["inf_pred"], b["inf_reat"], b["fen"]
                    righe.append(
                        f"\n[{k} profilo {prof}] picco/media {b['picco_su_media']:.2f}\n"
                        f"  Infinite predittiva: calcolo ${r0(ip['costo'])} (standby ${r0(ip['standby'])}), totale "
                        f"${r0(b['tot_inf_pred'])}; taglie {ip['taglie']}; minuti in sovraccarico {ip['over_min']}, "
                        f"oltre M60 {ip['over_m60_min']}\n"
                        f"  Infinite reattiva: calcolo ${r0(ir['costo'])}, totale ${r0(b['tot_inf_reat'])}; taglie "
                        f"{ir['taglie']}; minuti in sovraccarico {ir['over_min']}\n"
                        f"  Fenvix: camerieri ${r0(fe['costo'])} (medi {fe['camerieri_medi']:.1f}, max "
                        f"{fe['camerieri_max']}), fisse ${r0(b['fisse_tot'])} {b['fisse']} (quaderni {b['quaderni']}, minuti oltre il quaderno {b['quaderno_over_min']}), "
                        f"totale ${r0(b['tot_fen'])}; minuti in sovraccarico {fe['over_min']:.1f}, CPU oltre "
                        f"{fe['quota_cpu_oltre'] * 100:.3f}%\n"
                        f"  flotta di camerieri pagata sul picco ${r0(b['flotta_picco'])}; CPU usata ideale Fenvix "
                        f"${r0(b['ideale_fe'])}; CPU usata ideale Infinite (solo primario, prezzo per vCPU di M60) "
                        f"${r0(b['ideale_inf_primario'])}\n"
                        f"  RAPPORTO Infinite pred / Fenvix = {b['tot_inf_pred'] / b['tot_fen']:.2f}; reattiva / Fenvix = "
                        f"{b['tot_inf_reat'] / b['tot_fen']:.2f}; calcolo Infinite pred / camerieri = "
                        f"{ip['costo'] / fe['costo']:.2f}")
            sens = []
            for nv, _ in VARIANTI[1:]:
                b = js[k][f"{prof}/{nv}"]
                sens.append(f"{nv}: Fen ${r0(b['tot_fen'])} (camerieri ${r0(b['fen']['costo'])}, sovracc. "
                            f"{b['fen']['over_min']:.1f} min) Inf pred ${r0(b['tot_inf_pred'])} rapporto "
                            f"{b['tot_inf_pred'] / b['tot_fen']:.2f}")
            righe.append("  sensibilità: " + "\n               ".join(sens))
    txt = "\n".join(righe)
    open(f"{QUI}/risultati/riassunto.txt", "w").write(txt + "\n")
    json.dump(js, open(f"{QUI}/risultati/risultati.json", "w"), indent=1, default=float)
    print(txt)


if __name__ == "__main__":
    main()
