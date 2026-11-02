import re,sys
t=open(sys.argv[1]).read()
skip={'ccand','cmem','cpins','cread','cused','cst','seen','wcyc','owner','nPoints','used','marked'} if len(sys.argv)<3 else set()
prev={}
for blk in re.split(r'\nState \d+: ',t)[1:]:
    head=blk.split('\n')[0]; m=re.match(r'<(\w+(\([^)]*\))?)',head)
    cur={}
    for line in blk.split('\n')[1:]:
        mm=re.match(r'/\\ (\w+) = (.*)',line)
        if mm: cur[mm.group(1)]=mm.group(2)
    ch={k:v for k,v in cur.items() if prev.get(k)!=v and k not in skip}
    print(m.group(1) if m else head[:30], '|', '; '.join(f'{k}={v}' for k,v in ch.items()) if prev else '')
    prev=cur
