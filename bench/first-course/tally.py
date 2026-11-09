import json,statistics as st,sys,os
R=sys.argv[1]
def pct(xs,p):
    xs=sorted(xs); return xs[min(len(xs)-1,int(round(p/100*(len(xs)-1))))]
for n in sys.argv[2:]:
    f=f"{R}/{n}/result.json"
    if not os.path.exists(f): continue
    r=json.load(open(f)); rows=r["rows"]; s=r["summary"]
    q1=st.mean(x["q1"]["gets"] for x in rows); lat=st.mean(sum(l["gets"] for l in x["later"]) for x in rows)
    v1=st.mean(x["v1_q1"]["gets"]+sum(l["gets"] for l in x["v1_later"]) for x in rows)
    q1ms=[x["q1"]["ms"] for x in rows]
    lms=[l["ms"] for x in rows for l in x["later"]]
    g=r["gap_stats"]
    print(f"{n}: share {s['all']['share']*100:.1f}% (active {s['active']['share']*100:.1f}, uniform {s['uniform']['share']*100:.1f}, written {s['written']['share']*100:.1f}, not {s['not_written']['share']*100:.1f}); wrong {s['wrong_answers']}; "
          f"v2 Q1 GETs {q1:.2f} ms p50 {pct(q1ms,50):.2f} p99 {pct(q1ms,99):.2f}; v2 later GETs/visit {lat:.2f} -> visit total {q1+lat:.2f} (v1 total {v1:.2f}); "
          f"later ms p50 {pct(lms,50):.2f} p99 {pct(lms,99):.2f}; gap msgs {g['messages']} passes {g['compaction_passes']} cGets {g['compaction'].get('compactGets')} cPuts {g['compaction'].get('compactPuts')} cpu {g['server_cpu_secs']:.1f}s hook {g['first_course_write_hook_ms']:.0f}ms applied {g['first_course_writes_applied']} trimmed {g['trimmed']}")
    for k in ("sizes_after_visit1","sizes_after_gap","sizes_after_visit2"):
        if r.get(k): print("   ",k,{a:(round(b) if isinstance(b,float) else b) for a,b in r[k].items()})
