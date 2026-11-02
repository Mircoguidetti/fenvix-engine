"""Riassunto di cu-prova giri."""
import json, sys
for f in sys.argv[1:]:
    rs = json.load(open(f))
    print("##", f)
    tot = {"cop": 0, "sca": 0, "car": 0, "cpu": 0, "get": 0, "put": 0, "copy": 0}
    for r in rs:
        e = r["esito"]
        if r["giro"] > 0:
            tot["cop"] += e["byte_copiati"]; tot["sca"] += e["byte_scaricati_dati"] + e["byte_scaricati_meta"]; tot["car"] += e["byte_caricati"]
            tot["cpu"] += e["cpu_ms"]; tot["get"] += e["s3"]["get"]; tot["put"] += e["s3"]["put"]; tot["copy"] += e["s3"]["copia"]
        line = f"  giro {r['giro']:2d}: oggetto {r['byte_oggetto']/2**20:.0f} MB, morti {r['byte_morti']/2**20:.0f} MB ({r['byte_morti']/r['byte_oggetto']*100:.0f}%), blocchi {r['blocchi']}, meta {r['byte_meta']/2**20:.1f} MB; copiati {e['byte_copiati']/2**20:.0f} MB, scaricati {(e['byte_scaricati_dati']+e['byte_scaricati_meta'])/2**20:.0f} MB, caricati {e['byte_caricati']/2**20:.0f} MB, vivi copiati {e['frazione_copiata']*100:.1f}%, CPU {e['cpu_ms']} ms, riscritti {e['blocchi_puliti_riscritti']}"
        if r.get("letture_cucita"):
            lc, lr = r["letture_cucita"], r["letture_riscritta"]
            line += f"\n           letture p99 cucita {lc['p99_ns']/1000:.1f} us, riscritta {lr['p99_ns']/1000:.1f} us = {lc['p99_ns']/lr['p99_ns']:.2f}x (p50 {lc['p50_ns']/1000:.1f} / {lr['p50_ns']/1000:.1f}); riscritta {r['byte_riscritta']/2**20:.0f} MB; verifica {r['verifica']['uguali']} sbagliate {r['verifica']['letture_sbagliate']}"
        print(line)
    n = len(rs) - 1
    print(f"  totale giri 1-{n}: copiati {tot['cop']/2**30:.2f} GB, scaricati {tot['sca']/2**30:.2f} GB, caricati {tot['car']/2**30:.2f} GB, CPU {tot['cpu']/1000:.1f} s, GET {tot['get']}, PUT {tot['put']}, COPY {tot['copy']}")
