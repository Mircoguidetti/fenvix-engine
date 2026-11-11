#!/usr/bin/env bash
# loop.sh <tag> <binary> <mode> <runs>: runs probe.py; keeps the run folder and bucket only when a document was lost
tag=$1 bin=$2 mode=$3 n=$4
export PATH=/root/.fenvix-judge/env/bin:$PATH
for i in $(seq 1 $n); do
  d=/home/user/runs/$tag-$i
  rm -rf $d
  timeout 900 python /home/user/trace/probe.py $d $bin $mode > $d.out 2>&1
  r=$(tail -1 $d.out)
  echo "$tag run $i: $r" >> /home/user/runs/$tag.summary
  if echo "$r" | grep -q '"n_missing_merged": 0, "n_missing_slatedb": 0' && ! echo "$r" | grep -q sigterm_hang; then
    b=$(python -c "import json;print(json.load(open('$d/result.json'))['bucket'])" 2>/dev/null)
    rm -rf $d/bodies
    [ -n "$b" ] && rm -rf /root/.fenvix-judge/data/minio/$b
  fi
  rm -rf /root/.fenvix-judge/data/minio/.minio.sys/tmp/.trash/* 2>/dev/null
done
