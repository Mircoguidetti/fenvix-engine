import json
car={json.loads(l)["etichetta"]:json.loads(l) for l in open("risultati-rev/carichi.jsonl")}
mis={}
try:
  for l in open("risultati-rev/misure.jsonl"):
    j=json.loads(l); mis[j["etichetta"]]=j
except FileNotFoundError: pass
for et,c in car.items():
  r=c["regime"]; u=r["utente_byte"]
  l0=None
  wa_tot=r["put_sst_byte"]/u
  out=dict(et=et, wa_tot=round(wa_tot,2), put_per_gb=round(r["put_sst"]/(u/2**30)), get_per_gb=round(r["get_sst"]/(u/2**30)),
    pile_media=round(r["pile_media"],2), l0_media=round(r["l0_media"],2), sr_media=round(r["sr_media"],2), pile_p90=r["pile_p90"], pile_max=r["pile_max"],
    mbs=round(u/2**20/r["secondi"],2), fine=c["fine"])
  m=mis.get(et)
  if m:
    q=m["query"]; out.update(find=round(m["find_tipica"],2), pile_misura=m["pile_media"], **{k:(round(v["get_media"],2),v["get_p90"],round(v["byte_media"]/1024),round(v["docs_media"],1)) for k,v in q.items()})
  print(json.dumps(out))
