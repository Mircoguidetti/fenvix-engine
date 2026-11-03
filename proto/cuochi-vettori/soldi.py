"""M20, punto 5: dal rapporto di CPU misurato ai dollari, sui clienti medio e grande di B10.

Uso: python3 soldi.py <R95> [<R95 binario>]
Ipotesi (STIMATO): Atlas dimensiona 10 ricerche/s per vCPU (DICHIARATO, blog "sizing search nodes");
a noi servono 10*R ricerche/s per vCPU (R = rapporto misurato contro Lucene). Stessa RAM e stesso disco
per l'indice (nessun vantaggio di memoria contato). Almeno 2 nodi (Atlas: per pezzo; noi: uno per zona).
Prezzi Atlas: B10 (DICHIARATO, pagina prezzi MongoDB, us-east-1, 29/09/2026).
Prezzi AWS on-demand us-east-1: c7gd.4xlarge $0,7258/h 16 vCPU 32 GiB 950 GB NVMe (DICHIARATO, Holori);
m7gd $0,2136/h per 4 vCPU 16 GiB 237 GB (DICHIARATO in B8); r7gd: STIMATO $0,2722/h per 4 vCPU 32 GiB.
Margine 45%: prezzo = costo / 0,55.
"""
import itertools, sys

H = 730.0
# (nome, vCPU, RAM GB, $/h, disco GB o None = non vincola)
ATLAS = {
    "High CPU": [("S20", 2, 4, 0.12), ("S30", 4, 8, 0.24), ("S40", 8, 16, 0.48), ("S50", 16, 32, 0.99),
                 ("S60", 32, 64, 1.77), ("S70", 48, 96, 2.50), ("S80", 64, 128, 3.26)],
    "Low CPU": [("S30", 1, 8, 0.11), ("S40", 2, 16, 0.21), ("S50", 4, 32, 0.43), ("S60", 8, 64, 0.89),
                ("S80", 16, 128, 1.68), ("S90", 32, 256, 3.12), ("S100", 48, 384, 4.40), ("S110", 64, 512, 5.87)],
    "Storage-Optimized": [("S40-S", 2, 16, 0.25), ("S50-S", 4, 32, 0.52), ("S60-S", 8, 64, 0.94),
                          ("S80-S", 16, 128, 1.89), ("S90-S", 32, 256, 3.76)],
}
DISK_SO = {"S40-S": 421, "S50-S": 843, "S60-S": 1687, "S80-S": 3375, "S90-S": 6750}
# famiglie AWS per 4 vCPU (si scala in multipli fino a 64 vCPU): ($/h, RAM, NVMe)
AWS = {"c7gd": (0.7258 / 4, 8, 237.5), "m7gd": (0.2136, 16, 237), "r7gd": (0.2722, 32, 237)}

# clienti B10: RAM usabile richiesta per nodo (GB), disco per nodo (GB), pezzi (shard) Atlas
# Noi (STIMATO, conto mio): RAM = riassunti + grafo (32 vicini x 4 byte = 128 B per vettore), tutto in RAM;
# disco NVMe = vettori pieni per il ricalcolo (4 KB a 1.024 dimensioni) + indice. Totali, un pezzo solo se entra.
CLIENTI = {
    "medio int8": dict(ram=59, disk=270, shards=1, qps=[50, 250], extra=65, ram_o=57.6, disk_o=263),
    "medio binario": dict(ram=9.2, disk=270, shards=1, qps=[50, 250], extra=65, ram_o=12.8, disk_o=218),
    "grande int8": dict(ram=294, disk=1300, shards=2, qps=[500, 2500], extra=247, ram_o=576, disk_o=2624),
    "grande binario": dict(ram=46, disk=1300, shards=2, qps=[500, 2500], extra=247, ram_o=128, disk_o=2176),
}


def atlas_best(c, qps, kinds=("High CPU", "Low CPU", "Storage-Optimized")):
    best = None
    for kind in kinds:
        for name, v, ram, price in ATLAS[kind]:
            if ram * 0.9 < c["ram"]:
                continue
            if kind == "Storage-Optimized" and DISK_SO[name] < c["disk"]:
                continue
            per_shard_cpu = qps / 10.0 / c["shards"]  # la CPU si divide tra i pezzi (prudente per noi)
            n = max(2, -(-per_shard_cpu // v))
            cost = n * c["shards"] * price * H
            if best is None or cost < best[0]:
                best = (cost, f"{int(n * c['shards'])} x {name} {kind}")
    return best


def ours_best(c, qps, R):
    need_cpu = qps / (10.0 * R)
    ram_tot, disk_tot = c["ram_o"], c["disk_o"]
    best = None
    for pieces in (1, 2, 3, 4):  # un pezzo se entra in una macchina, altrimenti il minimo di pezzi
        for fam, (p4, ram4, nv4) in AWS.items():
            for units in (0.5, 1, 2, 4, 8, 12, 16):  # 2..64 vCPU
                v, ram, nv, price = 4 * units, ram4 * units, nv4 * units, p4 * units
                if ram * 0.9 < ram_tot / pieces or nv < disk_tot / pieces:
                    continue
                n = max(2, -(-(need_cpu / pieces) // v))
                cost = n * pieces * price * H + c["extra"]
                if best is None or cost < best[0]:
                    best = (cost, f"{int(n * pieces)} x {fam} da {int(v)} vCPU" + (f" in {pieces} pezzi" if pieces > 1 else ""))
        if best:
            return best
    return best


def main():
    R = float(sys.argv[1])
    Rb = float(sys.argv[2]) if len(sys.argv) > 2 else R
    for name, c in CLIENTI.items():
        r = Rb if "binario" in name else R
        for qps in c["qps"]:
            a = atlas_best(c, qps)
            ah = atlas_best(c, qps, ("High CPU",))
            aso = atlas_best(c, qps, ("Storage-Optimized",))
            o = ours_best(c, qps, r)
            price = o[0] / 0.55
            line = (f"{name}, {qps}/s: Atlas migliore ${a[0]:,.0f} ({a[1]}); High CPU "
                    f"{'$%s' % format(round(ah[0]), ',') if ah else 'n/d'}; Storage-Opt "
                    f"{'$%s' % format(round(aso[0]), ',') if aso else 'n/d'} | noi costo ${o[0]:,.0f} ({o[1]}), "
                    f"prezzo ${price:,.0f} = {100 * price / a[0]:.0f}% del migliore Atlas")
            if ah:
                line += f", {100 * price / ah[0]:.0f}% di High CPU"
            if aso:
                line += f", {100 * price / aso[0]:.0f}% di Storage-Opt"
            print(line)
    # formula pura sulla CPU (regime con tante ricerche, nodi grandi)
    pa_hc = 3.26 / 64
    pa_so = 1.89 / 16
    po = AWS["c7gd"][0] / 4
    print(f"Solo CPU: Atlas High CPU ${pa_hc:.4f}/vCPU-ora, Storage-Opt ${pa_so:.4f}; noi c7gd costo ${po:.4f}, "
          f"prezzo ${po / 0.55:.4f}. Prezzo/Atlas = {po / 0.55 / pa_hc / R:.2f} (High CPU), "
          f"{po / 0.55 / pa_so / R:.2f} (Storage-Opt) con R={R}")


if __name__ == "__main__":
    main()
