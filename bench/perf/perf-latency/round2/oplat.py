import json, sys, collections
def q(v,p): v=sorted(v); return v[min(len(v)-1,int(p*len(v)))]
keys=['save_step','resume[write/none]','resume[write/short]','resume[write/long]','resume[write/first]','resume[read]','time_travel','list','list_limit10']
for path in sys.argv[1:]:
    g=collections.defaultdict(list)
    for l in open(path):
        o=json.loads(l); k=o['op']
        if o['phase'] not in ('write','read'): continue
        if k=='resume': k=f"resume[{o['phase']}/{o.get('gap')}]" if o['phase']=='write' else 'resume[read]'
        g[k].append(o['ms'])
    print(path.split('/')[-1], '  '.join(f"{k}={q(g[k],.5):.1f}/{q(g[k],.99):.1f}(n{len(g[k])})" for k in keys if g[k]))
