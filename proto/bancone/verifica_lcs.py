"""Verifica: stesso flusso (oracleGeneral esportato dal simulatore), stesse taglie, con libCacheSim.
Uso: python3 -I verifica_lcs.py flusso.og taglia1 taglia2 ...  -> una riga JSON per politica e taglia."""
import sys, json, libcachesim as l
path, caps = sys.argv[1], [int(x) for x in sys.argv[2:]]
for name in ["LRU", "S3FIFO", "ARC", "WTinyLFU", "Sieve", "TwoQ"]:
    for cap in caps:
        r = l.TraceReader(path, l.TraceType.ORACLE_GENERAL_TRACE)
        c = getattr(l, name)(cache_size=cap)
        mr, bmr = c.process_trace(r)
        print(json.dumps({"kind": "libcachesim", "policy": name, "cap": cap, "miss_ratio": round(mr, 6), "byte_miss_ratio": round(bmr, 6)}), flush=True)
