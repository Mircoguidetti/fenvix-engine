import json,sys
D='/home/claude/fenvix-engine/proto/quaderno/results/q8-corsie/rev-q8b/'
pc=[json.loads(l) for l in open(D+'percore.jsonl')]
for l in open(D+'corsie.jsonl'):
    r=json.loads(l); n=r['normal']; L=n['lat']; s=r['sys']; ld=r['leader']
    w=[p for p in pc if r['t_start']<=p['t']<=r['t_start']+s['secs']]
    c0=[p['cpu0'] for p in w]; c1=[p['cpu1'] for p in w]
    m=lambda x:sum(x)/max(1,len(x)); hi=lambda x:sum(1 for v in x if v>=95)/max(1,len(x))*100
    reps={k:v for k,v in s.items() if k.startswith('r') and isinstance(v,dict)}
    d=r['leader_diag'].get('corsie',{}); t=r['leader_diag'].get('tappe',{})
    print(f"{r['tag']} L{r['level']} {r['phase']}: p50 {L['p50_us']/1e3:.2f} p99 {L['p99_us']/1e3:.2f} p999 {L['p999_us']/1e3:.2f} busy{n['busy_retries']} rej{n['rejected']} | mach {s['machine_busy_pct']:.0f}% core0 media {m(c0):.0f}% (>=95%: {hi(c0):.0f}% dei secondi) core1 {m(c1):.0f}% ({hi(c1):.0f}%) | capo {reps[ld]['cores']:.2f} | attesa_corsia_leggeri {t.get('attesa_corsia_leggeri')} durevole {t.get('durevole_primo')} conferma {t.get('conferma')} rifiuti_quota {d.get('rifiuti_quota')} ammessi_sopra {d.get('ammessi_sopra')} | noisy cores {[ (k,round(v,2)) for k,v in s.items() if k.startswith('gen_') and v]}")
