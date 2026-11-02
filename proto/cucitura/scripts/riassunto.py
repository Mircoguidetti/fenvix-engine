"""Riassunto dei risultati di cu-prova prova (file JSON in risultati/)."""
import json, statistics, sys
for f in sys.argv[1:]:
    d = json.load(open(f))
    r = d["riscrittura"]
    pr = statistics.median(l["p99_ns"] for l in d["letture_riscritta"])
    print(f"## {f}: {d['scatole']}x{d['mb']}MB id={d['id']} canc={d['canc']} blocco={d['blocco_kb']}KB recenti={d["recenti"]} buco_get={d.get("buco_get_kb",0)}KB ops={d['operazioni']}")
    print(f"  riscrittura: scaricati {r['byte_scaricati_dati']/2**20:.0f} MB, caricati {r['byte_caricati']/2**20:.0f} MB, {r['ms_totale']} ms, CPU {r['cpu_ms']} ms (scarico {r['cpu_ms_scarico']}, fusione {r['cpu_ms_fusione']}, carico {r['cpu_ms_carico']}), GET {r['s3']['get']} PUT {r['s3']['put']}, p99 letture {pr/1000:.1f} us")
    for (modo, e, v, morti), (_, ls) in zip(d["cuciture"], d["letture_cucite"]):
        pc = statistics.median(l["p99_ns"] for l in ls)
        tot = e["byte_copiati"] + e["byte_scaricati_dati"] + e["byte_scaricati_meta"]
        print(f"  {modo}{' (RIPIEGO: riscritto tutto)' if e.get('ripiego') else ''}: copiati {e['byte_copiati']/2**20:.0f} MB (morti {e['byte_copiati_morti']/2**20:.0f}) in {e['parti_copiate']} parti, scaricati {e['byte_scaricati_dati']/2**20:.0f} MB + meta {e['byte_scaricati_meta']/2**20:.1f} MB, caricati {e['byte_caricati']/2**20:.0f} MB; "
              f"frazione vivi copiati {e['frazione_copiata']*100:.1f}%, copiati/(copiati+scaricati) {e['byte_copiati']/tot*100:.1f}%; uscita {e['byte_uscita']/2**20:.0f} MB (morti {morti/2**20:.0f}); "
              f"{e['ms_totale']} ms, CPU {e['cpu_ms']} ms (piano {e['cpu_ms_piano']}, scarico {e['cpu_ms_scarico']}, fusione {e['cpu_ms_fusione']}, carico {e['cpu_ms_carico']}); GET {e['s3']['get']} PUT {e['s3']['put']} COPY {e['s3']['copia']}; "
              f"intrusi {e['blocchi_intrusi']}/{e['blocchi_ingressi']} blocchi, primo giro {e['blocchi_scaricati_1']}, puliti riscritti {e['blocchi_puliti_riscritti']}; "
              f"p99 {pc/1000:.1f} us = {pc/pr:.2f}x; canc rimaste {v['cancellazioni_rimaste']}, giuste {v['letture_giuste']} sbagliate {v['letture_sbagliate']}")
