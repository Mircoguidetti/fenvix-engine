import re,collections,sys
txt=open(sys.argv[1]).read().split('\n')
stacks=[];cur=[]
for l in txt:
    if l.startswith('#0 '):
        if cur: stacks.append(cur)
        cur=[l]
    else: cur.append(l)
stacks.append(cur)
idle=re.compile(r'syscall|futex|epoll_wait|pthread_cond|nanosleep|__GI___poll|clock_nanosleep|park')
act=[s for s in stacks if not idle.search(s[0])]
cat=collections.Counter()
for s in act:
    j='\n'.join(s)
    if 'execute_compaction_job' in j: cat['slatedb compaction']+=1
    elif 'chglog' in j: cat['chglog (archive)']+=1
    elif 'insert' in j and 'mongo_server' in j: cat['insert command']+=1
    elif 'flush' in j.lower() or 'wal' in j.lower(): cat['flush/wal']+=1
    elif 'mongo_server' in j: cat['other mongo_server']+=1
    else: cat['other']+=1
print(len(stacks),'stacks',len(act),'active',dict(cat))
