import gzip, csv, sys, collections
out = open('/home/user/rv/ck/threads.tsv', 'w')
for n in (10, 40, 100, 300):
    ck = collections.defaultdict(list)   # thread -> [(cid, size)]
    wr = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(gzip.open(f'rows_{n}.csv.gz', 'rt')):
        if r['table'] == 'mongo.checkpoints':
            ck[r['thread_id']].append((r['row_id'].split(':')[0], int(r['raw_bytes'])))
        elif r['table'] == 'mongo.checkpoint_writes':
            cid = r['row_id'].split(':')[0]
            wr[r['thread_id']][cid].append(int(r['raw_bytes']))
    for t, l in ck.items():
        l.sort()
        cks = ','.join(str(s) for _, s in l)
        ws = ';'.join(','.join(str(x) for x in wr[t].get(c, [])) for c, _ in l)
        out.write(f'{n}\t{t}\t{cks}\t{ws}\n')
