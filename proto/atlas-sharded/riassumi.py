#!/usr/bin/env python3
"""M19: legge risultati/risultati.json e scrive risultati/riassunto.txt (mediane per famiglia)."""
import json
import os
import sys

import numpy as np

QUI = os.path.dirname(os.path.abspath(__file__))
SOGLIA = 0.47


def fam(nome):
    return nome[:2]


def med(v):
    return float(np.median(v)) if v else float("nan")


def main():
    f = sys.argv[1] if len(sys.argv) > 1 else "risultati.json"
    R = json.load(open(f"{QUI}/risultati/{f}"))
    righe = []
    P = righe.append

    def sel(prof, us, opz="", famiglie=("me", "tw")):
        return {k.split("|")[0]: v for k, v in R.items()
                if k.split("|")[1] == prof and k.split("|")[2] == str(us) and k.split("|")[3] == opz
                and fam(k.split("|")[0]) in famiglie}

    for opz, titolo in (("", "BASE"), ("forma_vera=True", "FORMA VERA (solo A)"), ("taglia_dim=M50", "SHARD SU M50"),
                        ("taglia_dim=M80", "SHARD SU M80"), ("squilibrio=0.0", "SHARD IN PARI"),
                        ("pav_min=M50", "PAVIMENTO M50 (RAM)"), ("pav_min=M60", "PAVIMENTO M60 (RAM)")):
        P(f"\n===== {titolo} =====")
        for prof in ("A", "B", "C"):
            for us in (300, 600):
                for famiglie, etich in ((("me", "tw"), "Meta+Twitter"), (("me",), "Meta"), (("tw",), "Twitter"),
                                        (("al",), "Alibaba"), (("te",), "Tencent")):
                    d = sel(prof, us, opz, famiglie)
                    if not d:
                        continue
                    n = len(d)
                    noi = [v["noi"] for v in d.values()]
                    P(f"\n[{prof} {us}µs {etich} n={n}] media scritture {med([x['media_scritture'] for x in noi]):.0f}/s, "
                      f"picco {med([x['picco_scritture'] for x in noi]):.0f}/s, k {med([v['k'] for v in d.values()]):.2f}")
                    P(f"  noi: prezzo centro ${med([x['centro']['prezzo'] for x in noi]):.0f} (costo "
                      f"${med([x['centro']['costo'] for x in noi]):.0f}); migliore ${med([x['migliore']['prezzo'] for x in noi]):.0f}, "
                      f"peggiore ${med([x['peggiore']['prezzo'] for x in noi]):.0f}")
                    for caso in ("migliore", "peggiore"):
                        vo = {kk: round(med([x[caso]['voci'][kk] for x in noi])) for kk in noi[0][caso]['voci']}
                        P(f"    voci {caso}: {vo}; quaderni sul picco ${med([x[caso]['quaderni_picco'] for x in noi]):.0f}")
                    for v in ("i", "i_reat", "ii", "iii"):
                        aa = [x["atlas"][v] for x in d.values() if v in x["atlas"]]
                        if not aa:
                            continue
                        ore = {}
                        for a in aa:
                            for t, h in a["ore_per_taglia_per_shard"].items():
                                ore.setdefault(t, []).append(h)
                        ore = {t: round(med(h + [0] * (len(aa) - len(h))), 0) for t, h in ore.items()}
                        rc = [a["rapporto"]["centro"] for a in aa]
                        P(f"  Atlas ({v}): {aa[0]['S']} shard (min {min(a['S'] for a in aa)} max {max(a['S'] for a in aa)}), "
                          f"pavimento {aa[0]['pavimento']}, ore/mese per shard {ore}; taglia di dimensionamento o sopra "
                          f"{100 * med([a['quota_taglia_alta'] for a in aa]):.0f}% del tempo; calcolo ${med([a['calcolo'] for a in aa]):.0f}, "
                          f"totale ${med([a['totale'][0] for a in aa]):.0f}–{med([a['totale'][1] for a in aa]):.0f}; "
                          f"sovraccarico {med([a['minuti_sovraccarico_per_shard'] for a in aa]):.1f} min/mese per shard")
                        P(f"     RAPPORTO centro {100 * med(rc):.0f}% [curve {100 * min(rc):.0f}–{100 * max(rc):.0f}], forbice "
                          f"{100 * med([a['rapporto']['migliore'] for a in aa]):.0f}–{100 * med([a['rapporto']['peggiore'] for a in aa]):.0f}%")
                    if opz == "" and "ii" in list(d.values())[0]["atlas"]:
                        # quota di clienti con discesa spenta perché la media pesata passi la soglia
                        p = med([x["centro"]["prezzo"] for x in noi])
                        ai = med([x["atlas"]["i"]["totale"][2] for x in d.values()])
                        aii = med([x["atlas"]["ii"]["totale"][2] for x in d.values()])
                        aiii = med([x["atlas"]["iii"]["totale"][2] for x in d.values()])
                        need = p / SOGLIA
                        for nome, ax in (("ii", aii), ("iii", aiii)):
                            q = (need - ai) / (ax - ai) if ax > ai else float("inf")
                            P(f"  quota di clienti in ({nome}) perché il rapporto medio scenda sotto il 47%: "
                              f"{100 * q:.0f}% (Atlas medio che serve ${need:.0f})")
    P("\n===== SCALA (profilo A, Meta+Twitter, variante i e iii, centro) =====")
    for us in (300, 600):
        for tbmodo in ("2 TB fissi", "TB proporzionali"):
            for sc in (0.25, 0.5, 1.0, 2.0, 4.0, 8.0):
                if sc == 1.0:
                    opz = ""
                elif tbmodo == "2 TB fissi":
                    opz = f"scala={sc}"
                else:
                    opz = f"scala={sc},tb={2.0 * sc * 1.25}"
                d = sel("A", us, opz)
                if not d:
                    continue
                parti = []
                for v in ("i", "iii"):
                    aa = [x["atlas"][v] for x in d.values()]
                    parti.append(f"{v}: {med([a['S'] for a in aa]):.0f} shard, Atlas ${med([a['totale'][2] for a in aa]):.0f}, "
                                 f"rapporto {100 * med([a['rapporto']['centro'] for a in aa]):.0f}% "
                                 f"(forbice {100 * med([a['rapporto']['migliore'] for a in aa]):.0f}–"
                                 f"{100 * med([a['rapporto']['peggiore'] for a in aa]):.0f}%)")
                noi = [x["noi"] for x in d.values()]
                P(f"  {us}µs {tbmodo} picco {med([x['picco_scritture'] for x in noi]):.0f}/s "
                  f"(TB {list(d.values())[0]['tb']:.1f}): noi ${med([x['centro']['prezzo'] for x in noi]):.0f}; "
                  + "; ".join(parti))
    txt = "\n".join(righe)
    open(f"{QUI}/risultati/riassunto.txt", "w").write(txt + "\n")
    print(txt)


if __name__ == "__main__":
    main()
