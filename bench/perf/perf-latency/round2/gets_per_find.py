import sys, statistics, collections
L=open(sys.argv[1]).read().splitlines()
finds=[];gets=[]
for l in L:
    if l.startswith('WAITS ') and 'cmd=find' in l and 'coll=checkpoints' in l and ' u=' in l:
        d=dict(kv.split('=',1) for kv in l.split()[1:]); finds.append((float(d['u']),float(d['u'])+float(d['ms']),d))
    elif l.startswith('WAITS_GET'):
        d=dict(kv.split('=',1) for kv in l.split()[1:6])
        gets.append((float(d['u']),int(d['bytes']),l.split('range=')[1].split(' path=')[0],l.split('path=')[1].split(' in_cmd=')[0]))
# each GET is printed by two counting layers: keep one per (start, range, path)
seen=set(); g2=[]
for g in gets:
    k=(round(g[0],1),g[2],g[3])
    if k in seen: continue
    seen.add(k); g2.append(g)
n=int(sys.argv[2]) if len(sys.argv)>2 else 60
cold=finds[-2*n:][:n]
per=[[x for x in g2 if a-1<=x[0]<=b and '/fx/run/' in x[3] or (a-1<=x[0]<=b and 'compacted' in x[3])] for a,b,_ in cold]
ms=[float(d['ms']) for _,_,d in cold]
print('cold finds', len(cold), 'server ms p50 %.1f' % statistics.median(ms), 'GETs p50', statistics.median(len(g) for g in per), 'p90', sorted(len(g) for g in per)[int(.9*len(per))], 'KB p50 %.0f' % (statistics.median(sum(x[1] for x in g) for g in per)/1024))
