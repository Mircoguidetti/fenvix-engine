#!/usr/bin/env bash
# Terzo giro: il formato lcs (ha il tipo di operazione?) per Alibaba, Tencent, Meta.
{
for d in alibaba tencentBlock metaKV 2026-alibaba-ebs; do
  echo "== lcs $d"; aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_lcs/$d/ | head -40
  aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_lcs/$d/ | awk '{s+=$3; n++} END {print "file", n, "byte", s}'
  aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_lcs/$d/ | awk '{print $4}' | sed 's#/[^/]*$##' | sort | uniq -c
done
} > "$OUT/elenco3.txt" 2>&1
# il file lcs piu' piccolo di alibaba e di tencent: leggo l'intestazione e i primi record con libcachesim
for d in alibaba tencentBlock metaKV; do
  f=$(aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_lcs/$d/ | awk '$3>100000' | sort -k3 -n | head -1 | awk '{print $4}')
  echo "$d $f" >> "$OUT/sbirciata3.txt"
  aws s3 cp --no-sign-request --quiet "s3://cache-datasets/$f" /tmp/$d.lcs.zst
  python3 - /tmp/$d.lcs.zst >> "$OUT/sbirciata3.txt" 2>&1 <<'PY'
import sys, libcachesim as l, struct, subprocess, collections
p=sys.argv[1]
raw=subprocess.run(["zstd","-dc",p],capture_output=True).stdout[:8192+64]
print("intestazione: start", struct.unpack_from("<Q",raw,0), "version", struct.unpack_from("<Q",raw,8192-8))
r=l.TraceReader(p, l.TraceType.LCS_TRACE)
ops=collections.Counter(); n=0
for req in r:
    if n<12: print(req.clock_time, req.obj_id, req.obj_size, req.op, getattr(req,'ttl',None))
    ops[str(req.op)]+=1; n+=1
    if n>=200000: break
print("n", n, ops.most_common())
PY
done
exit 0
