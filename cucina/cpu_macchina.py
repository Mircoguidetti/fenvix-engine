"""P1 Irlanda v2 · CPU di tutta la macchina, da /proc/stat, ogni N secondi (di serie 5), finche' non viene fermato.
Una riga JSON per campione: {"t": ora UTC, "cpu": percento occupato di tutte le vCPU, "vcpu": quante, "carico1": load}.
Serve a dire, dopo la fase carico_massimo a piu' copie, se il limite era la macchina (CPU alta) o no.
Uso: cpu_macchina.py FILE [SECONDI]"""
import datetime as dt
import json
import os
import sys
import time


def leggi():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + (v[4] if len(v) > 4 else 0)  # idle + iowait
    return sum(v), idle


def main():
    out, passo = sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
    tot0, idle0 = leggi()
    while True:
        time.sleep(passo)
        tot1, idle1 = leggi()
        dtot = max(tot1 - tot0, 1)
        r = {"t": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
             "cpu": round(100.0 * (dtot - (idle1 - idle0)) / dtot, 1), "vcpu": os.cpu_count(),
             "carico1": os.getloadavg()[0]}
        with open(out, "a") as f:
            f.write(json.dumps(r) + "\n")
        tot0, idle0 = tot1, idle1


if __name__ == "__main__":
    main()
