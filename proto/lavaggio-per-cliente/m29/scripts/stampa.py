import json,sys
for l in open(sys.argv[1] if len(sys.argv)>1 else "risultati/riassunto.jsonl"):
  d=json.loads(l); m=d.pop('misure',{})
  print(d)
  v=None
  for k,v in m.items():
    print(' ',k, 'pile',v['pile'],'l0',v['pile_l0'],'tipica',v['find_tipica'])
    for q in ['Q1','Q2','Q3','SCALDA','Q1-dormiente','SCALDA-dormiente']:
      if q in v: print('    ',q,v[q], v['s3_bancone'][q])
  if v: print(' ', v['bancone'], v['pile_mb'], v['indici_mb'], v['filtri_mb'])
