import re,sys
t=open(sys.argv[1]).read()
keys=sys.argv[2].split(',') if len(sys.argv)>2 else ['boxSt','mf','members','pins','fst','fpin','wst','win','wbox','wcyc','cst','ccand','cmem','cread','cused','marked','cycle','rst','rheld','rcyc']
for blk in re.split(r'\nState \d+: ',t)[1:]:
    head=blk.split('\n')[0]
    m=re.match(r'<(\w+)',head); print('--',m.group(1) if m else head[:60], re.findall(r'\((\w+)\)|line \d+',head)[:1])
    for line in blk.split('\n')[1:]:
        for k in keys:
            if line.startswith(f'/\\ {k} ='): print('   ',line[3:])
