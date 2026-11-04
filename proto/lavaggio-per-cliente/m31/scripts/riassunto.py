#!/usr/bin/env python3
"""Riassunto leggibile di risultati/*.jsonl di M31 (tempi, insieme, q5, scritture). Uso: riassunto.py file.jsonl..."""
import json, sys
for f in sys.argv[1:]:
    for l in open(f):
        j = json.loads(l)
        t = j.get("tipo")
        if t == "tempi":
            print(f"\n{j['etichetta']} | {j['variante']} | {j['gruppo']} ({j['clienti']} clienti) | pile {j['pile']} | GET/find {j['get_per_find_media']:.2f} | doppie {j['get_in_piu_doppia_pct']:.1f}% | GET ms p50 {j['get_ms']['p50']:.1f} p99 {j['get_ms']['p99']:.1f}")
            for q, r in j["query"].items():
                fr, ca, d, g = r["freddo_ms"], r["caldo_ms"], r["freddo_meno_caldo_ms"], r["get_per_find"]
                print(f"  {q}: freddo p50 {fr['p50']:.1f} p95 {fr['p95']:.1f} p99 {fr['p99']:.1f} | caldo p50 {ca['p50']:.2f} p99 {ca['p99']:.2f} | f-c p50 {r['p50_freddo_meno_p50_caldo']:.1f} | <5ms {fr['quota_sotto_5ms']:.0%} >20ms {fr['quota_sopra_20ms']:.0%} | GET p50 {g['p50']:.0f} p99 {g['p99']:.0f} | byte/GET {r['byte_per_get']['media']/1024:.0f} KB | doc {r['documenti_media']:.0f}")
        elif t == "insieme":
            fm = j["find_ms"]
            print(f"insieme {j['etichetta']} k={j['k']} {j['q']}: p50 {fm['p50']:.1f} p99 {fm['p99']:.1f} | GET/s {max(g['get_al_secondo'] for g in j['giri']):.0f} | in volo max {max(g['max_get_in_volo'] for g in j['giri'])}")
        elif t == "q5":
            for k, r in j["q5"].items():
                if k == "senza_indice":
                    print(f"Q5 {j['etichetta']} senza indice: {r['ms']:.0f} ms, {r['get']} GET, {r['byte']/2**20:.0f} MB, trovati {r['trovati']}")
                elif r["freddo_ms"]["n"]:
                    print(f"Q5 {j['etichetta']} {k}: freddo p50 {r['freddo_ms']['p50']:.0f} p99 {r['freddo_ms']['p99']:.0f} caldo p50 {r['caldo_ms']['p50']:.1f} | GET p50 {r['get']['p50']:.0f} | byte p50 {r['byte']['p50']/2**20:.1f} MB | documenti p50 {r['documenti']['p50']:.0f}")
        elif t == "scritture":
            la = j["latenza_fino_a_s3_ms"]; c = j["con_il_lavaggio"]
            print(f"scritture {j['etichetta']}: {j['documenti_al_secondo']:.0f} doc/s | latenza p50 {la['p50']:.1f} p99 {la['p99']:.1f} | byte scritti/inseriti {c['byte_scritti_per_byte_inserito']:.2f} (WAL {c['byte_wal_per_byte_inserito']:.2f}) | PUT/GB {c['put_per_gb']:.0f}")
        elif t == "tempi-clienti":
            print(f"clienti {j['etichetta']}: misurati {j['clienti_misurati']}, in più varianti {j['clienti_in_piu_varianti']}")
