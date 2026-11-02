#!/usr/bin/env bash
# Elenca i file delle tracce nel bucket pubblico (lettura anonima), per scegliere il campione.
for p in cache_dataset_txt/2020_alibabaBlock/ cache_dataset_txt/2020_tencentBlock/ cache_dataset_txt/2022_metaKV/ \
         cache_dataset_txt/2024_metaKV/ cache_dataset_txt/2020_twitter/ cache_dataset_oracleGeneral/2020_twitter/ \
         cache_dataset_oracleGeneral/2020_alibabaBlock/ cache_dataset_oracleGeneral/2020_tencentBlock/ \
         cache_dataset_oracleGeneral/2022_metaKV/; do
  echo "== $p"; aws s3 ls --no-sign-request --recursive "s3://cache-datasets/$p" | head -400
done > "$OUT/elenco.txt" 2>&1
aws s3 ls --no-sign-request s3://cache-datasets/cache_dataset_txt/ > "$OUT/radice_txt.txt" 2>&1
aws s3 ls --no-sign-request s3://cache-datasets/ > "$OUT/radice.txt" 2>&1
aws s3 ls --no-sign-request s3://cachelib-workload-sharing/ --recursive 2>&1 | head -300 > "$OUT/cachelib.txt"
wc -l "$OUT"/*.txt
