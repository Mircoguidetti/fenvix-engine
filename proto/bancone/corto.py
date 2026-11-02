import sys, json, glob
sys.path.insert(0, '.')
from analisi import carica, costo
rows, info, warm, ver, lcs = carica(sys.argv[1:])
traces = sorted({k[0] for k in rows})
for t in traces:
    print("==", t)
    for c in sorted({k[1] for k in rows if k[0]==t}):
        line=[]
        for b in (0.02,0.05,0.1,0.2,0.5):
            d=rows.get((t,c,b))
            if not d: continue
            cs=costo(d)
            line.append(f"{int(b*100)}%: s3 {d['to_s3']*100:.1f} ex {d['to_ex']*100:.1f} p99 {d['medio_doppia']['p99']:.1f} ${cs['extra']:.0f}")
        print(f"  {c:<26}", " | ".join(line))
