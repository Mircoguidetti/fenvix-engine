"""M24 · riassunto dei numeri che decidono e conti sul profilo B. Uso: python analisi.py > risultati/riassunto.txt"""
import json

R = "risultati"
d = {k: json.load(open(f"{R}/{k}.json")) for k in ("tau2", "gh", "gh_bot", "ecom", "twitter")}

# caso che decide per fonte (SOGLIE.md)
CASI = [
    ("tau2 (cliente noto dopo l'identificazione)", d["tau2"]["identificazione"]["bancone10"]),
    ("tau2 (app con login)", d["tau2"]["login"]["bancone10"]),
    ("GH Archive senza bot", d["gh"]["tempo1h"]),
    ("GH Archive con bot", d["gh_bot"]["tempo1h"]),
    ("REES46 e-commerce", d["ecom"]["tempo1h"]),
]

# profilo B (B13, M21): 35 letture fredde/s, base costo M21 con dati raggruppati
FRED = 35.0
SEC = 2_592_000
GET = 0.0004 / 1000
NVME = 0.17
INF = (6500, 7440, 8900)
BASE = {"chat": (780, 910, 3.6), "eventi": (900, 1050, 6.8)}


def costo(r_per_fredda, s_servite, perm_s, hb, gwarm, gfind):
    risc_s = FRED * r_per_fredda
    get_piu = risc_s * gwarm * SEC * GET
    get_meno = FRED * s_servite * gfind * SEC * GET
    spazio_gb = risc_s * perm_s * hb / 1e9
    return get_piu - get_meno + spazio_gb * NVME, get_piu, get_meno, spazio_gb, risc_s * hb / 1e6


print("== letture fredde precedute dallo stesso utente (stretta), tutte le fredde e solo dati propri ==")
for nome, c in CASI:
    print(f"{nome}: fredde {c['fredde']} su {c['letture_misurate']} misurate ({c['quota_fredde']}%), "
          f"su dati condivisi {c['fredde_su_dati_condivisi']}%")
    print(f"   tutte, entro 1/5/30/300 s: {c['preceduta_stretta']}")
    print(f"   solo dati propri:          {c['solo_dati_propri_preceduta_stretta']}")
    print(f"   lettore (prompt alla lettera): {c['preceduta_lettore']}")

print("\n== riscaldamento simulato (stretta, permanenza 30 min, H = 2 MB), e confronto con l'ingenuo ==")
for nome, c in CASI:
    s = c["risc_stretta_ttl1800_2MB"]
    s20 = c["risc_stretta_ttl1800_20MB"]
    nv = c["ingenuo_successore"]
    own_ratio = s["riscaldamenti"] / s["fredde_proprie"] if s["fredde_proprie"] else float("nan")
    print(f"{nome}: servite {s['servite']}% di tutte, {s['servite_proprie']}% delle proprie "
          f"(in attesa {s['in_attesa_proprie']}%); riscaldamenti per fredda propria {own_ratio:.2f}; inutili {s['inutili']}%; "
          f"permanenza media {s['permanenza_media_s']:.0f} s")
    print(f"   p50/p99/p999 profilo B, tutte le fredde: {s['p50_p99_p999_profiloB_ms']}  senza riscaldamento: "
          f"{c['senza_risc_p50_p99_p999_profiloB_ms']}")
    print(f"   p50/p99/p999 profilo B se le fredde fossero tutte dati propri: {s['p50_p99_p999_profiloB_solo_propri_ms']} "
          f"(H = 20 MB: {s20['p50_p99_p999_profiloB_solo_propri_ms']})")
    print(f"   ingenuo (successore, senza utente): servite {nv['servite']}%, precisione {nv['precisione']}%, "
          f"anticipi per fredda {nv['anticipi_per_fredda']:.2f}")
    for ttl in (300, 7200):
        x = c[f"risc_stretta_ttl{ttl}_2MB"]
        print(f"   permanenza {ttl // 60} min: servite proprie {x['servite_proprie']}%, inutili {x['inutili']}%")

print("\n== costi sul profilo B (20 TB, 35 letture fredde/s), riscaldamenti per lettura fredda propria ==")
for nome, c in CASI:
    s = c["risc_stretta_ttl1800_2MB"]
    r = s["riscaldamenti"] / s["fredde_proprie"] if s["fredde_proprie"] else 0
    sv = (s["servite_proprie"] or 0) / 100
    for hb, hn in ((2e6, "2 MB"), (20e6, "20 MB")):
        lo = costo(r, sv, s["permanenza_media_s"], hb, 4, 6.8)
        hi = costo(r, sv, s["permanenza_media_s"], hb, 14, 3.6)
        print(f"{nome}, H = {hn}: costo in più ${lo[0]:,.0f} – ${hi[0]:,.0f} al mese "
              f"(GET in più ${lo[1]:,.0f}–{hi[1]:,.0f}, GET risparmiate ${hi[2]:,.0f}–{lo[2]:,.0f}, "
              f"bancone {lo[3]:,.0f} GB, caricati {lo[4]:,.0f} MB/s)")
        for kind, (b0, b1, _) in BASE.items():
            p_lo = (b0 + lo[0]) / 0.55 / INF[1] * 100
            p_hi = (b1 + hi[0]) / 0.55 / INF[1] * 100
            p_worst = (b1 + hi[0]) / 0.55 / INF[0] * 100
            print(f"      {kind}: prezzo {p_lo:.0f}–{p_hi:.0f}% di Infinite (al peggio {p_worst:.0f}%)")

print("\n== Twitter (controllo: il client è un servizio frontend) ==")
for cl, r in d["twitter"].items():
    k = "tempo1h" if "tempo1h" in r else "bancone10"
    x = r[k]
    s = x["risc_lettore_ttl1800_2MB"]
    print(f"{cl} ({k}): client {r['client']}, fredde {x['quota_fredde']}%, precedute dallo stesso client "
          f"{x['preceduta_lettore']}, servite dal riscaldamento per client {s['servite']}%")
