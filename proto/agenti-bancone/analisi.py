"""M25 · riassunto e conti sul profilo B. Uso: python analisi.py > risultati/riassunto.txt"""
import json
import math

d = json.load(open("risultati/tutto.json"))

FRED = 35.0            # letture fredde al secondo, profilo B (B13)
SEC = 2_592_000
GET = 0.0004 / 1000    # DICHIARATO, NUMBERS.md
NVME = 0.17            # $/GB al mese (M21)
INF = (6500, 7440, 8900)
BASE = (780, 1050)     # costo M21 con i dati raggruppati (chat basso, eventi alto)
TB20 = 20_000          # GB
SHARED_F = (0.0005, 0.01)   # quota condivisa sul profilo B, STIMATO (vedi rapporto)
COPIE = 3              # una copia del bancone condiviso per zona


def kwarm(H):
    return int(min(64, max(4, math.ceil(H / 8e6))))


def costo(r, H, shared_on):
    """r: riassunto di una politica. Ritorna (basso, alto, dettagli). GET risparmiate 3,6–6,8 per lettura
    servita (M21); GET di un riscaldamento k–2k (1–2 collezioni)."""
    R = r.get("risc_per_fredda") or 0.0
    S = (r.get("servite") or 0.0) / 100
    perm = r.get("permanenza_media_s") or 0.0
    risc_s = FRED * R
    k = kwarm(H)
    gp_lo, gp_hi = risc_s * k * SEC * GET, risc_s * 2 * k * SEC * GET
    gm_lo, gm_hi = FRED * S * 3.6 * SEC * GET, FRED * S * 6.8 * SEC * GET
    hist_gb = risc_s * perm * H / 1e9
    sh_lo = SHARED_F[0] * TB20 * COPIE if shared_on else 0.0
    sh_hi = SHARED_F[1] * TB20 * COPIE if shared_on else 0.0
    lo = gp_lo - gm_hi + (hist_gb + sh_lo) * NVME
    hi = gp_hi - gm_lo + (hist_gb + sh_hi) * NVME
    return lo, hi, {"risc_al_s": round(risc_s, 1), "MB_al_s": round(risc_s * H / 1e6, 1),
                    "GET_piu_al_mese": [round(gp_lo), round(gp_hi)], "GET_risparmiate_al_mese": [round(gm_lo), round(gm_hi)],
                    "bancone_storie_GB": round(hist_gb), "bancone_condivisi_GB": [round(sh_lo), round(sh_hi)]}


def prezzo(lo, hi):
    return ((BASE[0] + lo) / 0.55 / INF[1] * 100, (BASE[1] + hi) / 0.55 / INF[1] * 100,
            (BASE[1] + hi) / 0.55 / INF[0] * 100)


NOMI = {"nessuna": "nessuna", "a": "(a) primo segnale", "b": "(b) condivisi fissi", "c": "(c) segnale esplicito",
        "ab": "a+b", "cb": "c+b", "abc": "a+b+c"}

FONTI = [("tau2_articolo", "tau2, risultati dell'articolo (2025, 4 modelli, retail/airline/telecom, 4 tentativi)", 2e6),
         ("tau2_classifica", "tau2, classifica 2026 (6+4 modelli, 4 domini con banking, 4 tentativi)", 2e6),
         ("taubench", "tau-bench originale (gpt-4o, Claude 3.5 Sonnet; tempi STIMATI)", 2e6),
         ("dab", "DAB, agenti sui dati (5 modelli, 54 domande, 10 tentativi; tempi STIMATI; entità = dataset)", 200e6),
         ("swe", "SWE-bench Verified, mini-swe-agent con Gemini 3.5 Flash (entità = repository)", 20e6)]

print("== Fonti ==")
for k, nome, H in FONTI:
    i = d[k]["info"]
    p = i["passi_modello"]
    m = i["margine"]
    print(f"{nome}: {i['conversazioni']} conversazioni, {i['con_cliente']} con cliente/entità riconosciuti")
    if p:
        print(f"   passo del modello dopo uno strumento (MISURATO): mediana {p['mediana_s']} s, p10 {p['p10_s']} s, p90 {p['p90_s']} s")
    if m["da_inizio"]:
        print(f"   prima lettura fredda del cliente: {m['da_inizio']['mediana_s']} s dopo l'inizio (p10 {m['da_inizio']['p10_s']} s); "
              f"{m['da_primo_segnale']['mediana_s']} s dopo il primo segnale (p10 {m['da_primo_segnale']['p10_s']} s, "
              f"sotto 0,2 s nel {m['da_primo_segnale']['sotto_0_2s']}%)")
print(f"tempi per le tracce senza ora (dall'articolo tau2): {d['modello_tempi_senza_ora']}")

print("\n== Quanto pesa la parte condivisa nei database dei banchi (byte JSON) ==")
for dom, x in d["condivisi_byte"].items():
    print(f"{dom}: {x['quota_condivisa']}% condivisa ({', '.join(x['tabelle_condivise_byte'])}), totale {x['totale_byte'] / 1e6:.2f} MB, "
          f"storia media di un cliente {x['storia_media_cliente_byte']} byte")

print("\n== Politiche, caso base (bancone in RAM, permanenza 30 min, S3 scenario medio di M15) ==")
for k, nome, H in FONTI:
    b = d[k]["base"]
    print(f"\n-- {nome} (storia H = {H / 1e6:.0f} MB, caricamento mediana/p99 {b['_caricamento_ms_mediana_p99']} ms)")
    for pol, r in b.items():
        if pol.startswith("_"):
            continue
        print(f"{NOMI[pol]}: servite {r['servite']}% delle fredde (proprie {r['servite_proprie']}%, condivise "
              f"{r['servite_condivise']}%), in attesa {r['in_attesa']}%, su S3 {r['su_s3']}%; fredde {r['fredde']} "
              f"({r['fredde_condivise_quota']}% condivise); risc. per fredda {r['risc_per_fredda']}, inutili {r['inutili']}%; "
              f"p50/p99/p99,9 profilo B {r.get('p50_p99_p999_profiloB_ms')} ms, traccia {r.get('p50_p99_p999_traccia_ms')} ms")

print("\n== Varianti (a+b+c e c+b): servite, p99 profilo B ==")
for k, nome, H in FONTI:
    for vname, v in d[k].items():
        if vname in ("info", "base"):
            continue
        s = "; ".join(f"{NOMI[p]} {v[p]['servite']}% p99 {v[p]['p50_p99_p999_profiloB_ms'][1]} ms p99,9 {v[p]['p50_p99_p999_profiloB_ms'][2]}"
                      for p in ("nessuna", "a", "c", "cb", "abc") if p in v)
        print(f"{k} / {vname}: {s}")

print("\n== tau2 per dominio (articolo + classifica), caso base ==")
for dom, x in d["tau2_per_dominio"].items():
    b = x["base"]
    cf = x["condivisi_freddi_per_conversazione"]
    print(f"{dom}: {x['info']['conversazioni']} conv.; fredde condivise {b['nessuna']['fredde_condivise_quota']}% "
          f"(se i condivisi fossero freddi a ogni conversazione: {cf['nessuna']['fredde_condivise_quota']}%); "
          + "; ".join(f"{NOMI[p]} {b[p]['servite']}%" for p in ("a", "b", "c", "cb", "abc"))
          + f"; con condivisi freddi per conversazione: a {cf['a']['servite']}%, c {cf['c']['servite']}%")

print("\n== Costi sul profilo B (20 TB, 35 letture fredde/s; base M21 $780–1.050; Infinite $7.440, minimo $6.500) ==")
for k, nome, H in FONTI:
    b = d[k]["base"]
    for pol in ("a", "b", "c", "cb", "abc"):
        r = b[pol]
        lo, hi, det = costo(r, H, "b" in pol)
        p = prezzo(lo, hi)
        print(f"{k} {NOMI[pol]} (H {H / 1e6:.0f} MB): costo in più ${lo:,.0f} – ${hi:,.0f} al mese; {det}; "
              f"prezzo {p[0]:.0f}–{p[1]:.0f}% di Infinite (al peggio {p[2]:.0f}%)")
    alt = [v for v in d[k] if v.startswith("storia_")]
    for vname in alt:
        H2 = float(vname.split("_")[1][:-2]) * 1e6
        r = d[k][vname]["abc"]
        lo, hi, det = costo(r, H2, True)
        p = prezzo(lo, hi)
        print(f"{k} a+b+c con H {H2 / 1e6:.0f} MB: costo in più ${lo:,.0f} – ${hi:,.0f}; {det}; prezzo {p[0]:.0f}–{p[1]:.0f}% "
              f"(al peggio {p[2]:.0f}%)")

print("\n== p99 contro il passo del modello (a+b+c, profilo B, bancone NVMe) ==")
for k, nome, H in FONTI:
    v = d[k].get("bancone_nvme") or d[k]["base"]
    p99 = v["abc"]["p50_p99_p999_profiloB_ms"][1]
    p999 = v["abc"]["p50_p99_p999_profiloB_ms"][2]
    st = d[k]["info"]["passi_modello"] or {"mediana_s": d["modello_tempi_senza_ora"]["passo_agente_mediana_s"]}
    print(f"{k}: p99 {p99} ms, p99,9 {p999} ms; passo mediano {st['mediana_s']} s -> p99 = {100 * p99 / 1e3 / st['mediana_s']:.3f}% del passo")
