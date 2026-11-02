#!/usr/bin/env bash
# Secondo giro: formati lcs e parquet (hanno il tipo di operazione?), tracce Twitter originali, e un'occhiata ai byte.
L() { aws s3 ls --no-sign-request --recursive "s3://cache-datasets/$1" 2>&1 | head -${2:-80}; }
{
echo "== lcs radice"; aws s3 ls --no-sign-request s3://cache-datasets/cache_dataset_lcs/
echo "== parquet radice"; aws s3 ls --no-sign-request s3://cache-datasets/cache_dataset_parquet/
echo "== twitter memcache"; L cache_trace_twitter_memcache/ 120
for d in 2020_alibabaBlock 2020_tencentBlock 2020_twitter 2022_metaKV; do
  echo "== lcs $d"; L cache_dataset_lcs/$d/ 60
  echo "== parquet $d"; L cache_dataset_parquet/$d/ 60
done
echo "== alibaba oracleGeneral 1M e 10K"; L cache_dataset_oracleGeneral/2020_alibabaBlock/1M/ 10; L cache_dataset_oracleGeneral/2020_alibabaBlock/10K/ 50; L cache_dataset_oracleGeneral/2020_alibabaBlock/1K/ 20
echo "== tencent oracleGeneral tutto (conteggio per cartella)"; aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_oracleGeneral/2020_tencentBlock/ | awk '{print $4}' | sed 's#/[^/]*$##' | sort | uniq -c
aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_oracleGeneral/2020_alibabaBlock/ | awk '{s+=$3; n++} END {print "alibaba og file", n, "byte", s}'
aws s3 ls --no-sign-request --recursive s3://cache-datasets/cache_dataset_oracleGeneral/2020_tencentBlock/ | awk '{s+=$3; n++} END {print "tencent og file", n, "byte", s}'
aws s3 cp --no-sign-request s3://cache-datasets/cache_dataset_txt/2020_tencentBlock/README - 
} > "$OUT/elenco2.txt" 2>&1
# un'occhiata ai primi byte
aws s3 cp --no-sign-request s3://cache-datasets/cache_dataset_oracleGeneral/2020_alibabaBlock/100K/alibabaBlock_110.oracleGeneral.zst /tmp/a.zst
python3 - <<'PY' > "$OUT/sbirciata.txt" 2>&1
import struct, subprocess
raw = subprocess.run(["zstd","-dc","/tmp/a.zst"],capture_output=True).stdout
n=len(raw)//24; print("record", n)
for i in list(range(15))+list(range(n-5,n)):
    t,o,s,nx = struct.unpack_from("<IQIq", raw, i*24); print(t,o,s,nx)
import collections
sz=collections.Counter(struct.unpack_from("<IQIq",raw,i*24)[2] for i in range(n)); print(sz.most_common(10))
PY
( aws s3 cp --no-sign-request s3://cache-datasets/cache_dataset_txt/2020_alibabaBlock/alibabaBlock2020.csv.zst - | zstd -dc | head -20 ) > "$OUT/sbirciata_ali_txt.txt" 2>&1
( aws s3 cp --no-sign-request s3://cache-datasets/cache_dataset_txt/2020_tencentBlock/tencentBlock.zst - | zstd -dc | head -20 ) > "$OUT/sbirciata_ten_txt.txt" 2>&1
( aws s3 cp --no-sign-request s3://cache-datasets/cache_dataset_txt/2022_metaKV/202401_kv_traces_all_sort.csv.zst - | zstd -dc | head -20 ) > "$OUT/sbirciata_meta_txt.txt" 2>&1
( aws s3 cp --no-sign-request s3://cache-datasets/cache_dataset_txt/2022_metaKV/kvcache_202401/kvcache_traces_1.csv.zst - | zstd -dc | head -20 ) > "$OUT/sbirciata_meta1_txt.txt" 2>&1
exit 0
