import re,sys,collections
txt=open(sys.argv[1]).read().split('=== ')[1:]
c=collections.Counter(); idle=0
for s in txt:
    fr=[re.sub(r'^#\d+\s+(0x[0-9a-f]+ in )?','',l).split(' (')[0] for l in s.splitlines()[1:]]
    if not fr or any('epoll_wait' in f for f in fr[:4]): idle+=1; continue
    ours=[f for f in fr if re.match(r'<?(tenant_bench|mongo_server|slatedb|object_store|aws_lc|lz4|zstd|bson|hyper|reqwest|rustls)',f)]
    c[' | '.join(ours[:int(sys.argv[2]) if len(sys.argv)>2 else 4])]+=1
print('samples',len(txt),'idle',idle)
for k,v in c.most_common(30): print(v, k[:420])
