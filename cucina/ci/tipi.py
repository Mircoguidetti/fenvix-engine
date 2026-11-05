"""Cucina minima (K2) · i tipi di macchina ammessi, con vCPU, CPU (x86_64 o aarch64) e prezzo on-demand Linux in
eu-west-1 in dollari l'ora. Prezzi DICHIARATI: listino ufficiale AWS (AWS Price List, file eu-west-1 del 2026-11-04,
copia in fenvix-brain sessions/2026-11-05-inventario-cucina-minima/prezzi/); c7i.2xlarge, c7i.4xlarge e c7i.8xlarge
sono il doppio, 4 e 8 volte c7i.xlarge (DICHIARATO c7i.2xlarge 0,383 dal revisore del piano B).
t4g.small: in modo "unlimited" (quello di serie delle t4g) AWS fa pagare in piu' i crediti CPU oltre la base; qui si
mette nel conto, per eccesso, come se la macchina li usasse sempre tutti e due: 2 vCPU x 0,04 $ = 0,08 $ l'ora in piu'.

Uso (stampa su stdout; codice 1 = rifiutato, con il motivo):
  tipi.py controlla-taglie '["c7i.large","m7i.large"]'      -> una taglia per riga, se tutte ammesse e senza doppioni
  tipi.py arch TIPO                                          -> x86_64 o aarch64
  tipi.py vcpu TIPO
  tipi.py archi '["c7i.large","c7g.large"]'                 -> le CPU che servono, separate da spazio
  tipi.py cliente QUOTA '["c7i.large"]' [CLIENTE] [COPIE]   -> "CLIENTE COPIE SERVONO" (la macchina cliente piu' grande
                                                               che sta nella quota insieme alla taglia piu' grande)
  tipi.py spesa-minuto DISCO_GB TIPO...                      -> dollari di un minuto per queste macchine accese
                                                               (macchina + disco gp3 + IPv4 pubblico)
"""
import json
import sys

# tipo: (vCPU, CPU, dollari l'ora)
SERVER = {
    "c7i.large": (2, "x86_64", 0.0958),
    "c7i.xlarge": (4, "x86_64", 0.1915),
    "m7i.large": (2, "x86_64", 0.1124),
    "c7g.medium": (1, "aarch64", 0.0388),
    "c7g.large": (2, "aarch64", 0.0775),
    "m7g.large": (2, "aarch64", 0.0910),
    "t4g.small": (2, "aarch64", 0.0184 + 2 * 0.04),  # piu' i crediti CPU "unlimited", per eccesso
}
CLIENTE = {  # macchina che manda il carico (lo script), sempre x86_64
    "c7i.xlarge": (4, "x86_64", 0.1915),
    "c7i.2xlarge": (8, "x86_64", 0.383),
    "c7i.4xlarge": (16, "x86_64", 0.766),
    "c7i.8xlarge": (32, "x86_64", 1.532),
}
# copie dello script nel carico massimo per macchina cliente: come l'Irlanda (20 su 32 vCPU, 10 su 16, 5 su 8: circa
# 0,6 copie per vCPU, cosi' la macchina resta sotto l'80% di CPU) e 2 su 4 vCPU.
COPIE = {"c7i.xlarge": 2, "c7i.2xlarge": 5, "c7i.4xlarge": 10, "c7i.8xlarge": 20}
TUTTI = {**SERVER, **CLIENTE}
EBS_GB_MESE = 0.088   # gp3, DICHIARATO (listino AWS eu-west-1)
IPV4_ORA = 0.005      # IPv4 pubblico, DICHIARATO (AWS)


def no(msg):
    print(msg)
    sys.exit(1)


def taglie(js):
    try:
        t = json.loads(js)
    except Exception:
        no(f"taglie non leggibili: {js!r} (esempio: [\"c7i.large\", \"m7i.large\"])")
    if isinstance(t, str):
        t = [t]
    if not isinstance(t, list) or not t or not all(isinstance(x, str) for x in t):
        no("taglie: serve una lista di tipi, per esempio [\"c7i.large\", \"m7i.large\"]")
    fuori = [x for x in t if x not in SERVER]
    if fuori:
        no(f"taglie non ammesse: {', '.join(fuori)} (ammesse: {', '.join(SERVER)})")
    if len(set(t)) != len(t):
        no("taglie: la stessa taglia due volte")
    if len(t) > 4:
        no("taglie: al massimo 4 per giornata (circa 45 minuti ciascuna, STIMATO, dentro le 6 ore di GitHub)")
    return t


def main():
    c = sys.argv[1]
    if c == "controlla-taglie":
        print("\n".join(taglie(sys.argv[2])))
    elif c == "arch":
        print(TUTTI[sys.argv[2]][1])
    elif c == "vcpu":
        print(TUTTI[sys.argv[2]][0])
    elif c == "archi":
        print(" ".join(sorted({SERVER[x][1] for x in taglie(sys.argv[2])})))
    elif c == "cliente":
        quota = float(sys.argv[2])
        t = taglie(sys.argv[3])
        vmax = max(SERVER[x][0] for x in t)
        scelto = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] not in ("", "auto") else None
        if scelto:
            if scelto not in CLIENTE:
                no(f"cliente {scelto} non ammesso (ammessi: {', '.join(CLIENTE)})")
            prova = [scelto]
        else:
            prova = sorted(CLIENTE, key=lambda x: -CLIENTE[x][0])
        for x in prova:
            servono = CLIENTE[x][0] + vmax
            if servono <= quota:
                copie = int(sys.argv[5]) if len(sys.argv) > 5 and sys.argv[5] not in ("", "auto") else COPIE[x]
                print(f"{x} {copie} {servono}")
                return
        x = prova[-1]
        no(f"quota vCPU {quota:g}: non basta nemmeno per {x} ({CLIENTE[x][0]} vCPU) piu' la taglia piu' grande "
           f"({vmax} vCPU): servono {CLIENTE[x][0] + vmax}")
    elif c == "spesa-minuto":
        gb = float(sys.argv[2])
        tot = 0.0
        for x in sys.argv[3:]:
            p = TUTTI.get(x, (0, "", max(v[2] for v in TUTTI.values())))[2]  # tipo sconosciuto: il prezzo piu' alto
            tot += (p + gb * EBS_GB_MESE / 730 + IPV4_ORA) / 60
        print(f"{tot:.6f}")
    else:
        no(f"comando sconosciuto: {c}")


if __name__ == "__main__":
    main()
