import json,sys
for d in sys.argv[1:]:
    s=json.load(open(d+'/summary.json')); L=s['latency_ms']
    keys=['save_step','resume[write/none]','resume[write/short]','resume[write/long]','resume[write/first]','resume[read]','resume[cold]','time_travel','list','list_limit10','erase_user']
    print(d, 'ok',s['ok'],'wrong',s['wrong_reads'],'stored/logical',s['bytes']['stored_per_logical'])
    print('  '+'  '.join(f"{k}={L[k]['p50']:.1f}/{L[k]['p99']:.1f}" for k in keys if k in L))
