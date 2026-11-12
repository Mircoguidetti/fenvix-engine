export PATH=/root/.fenvix-judge/env/bin:$PATH
cd /home/user/rv/ctl-tests; M=/root/.fenvix-judge/data/minio
for spec in "$@"; do IFS=: read b mode n <<< "$spec"; for i in $(seq 1 $n); do
  if [ $mode = off ]; then E="FENVIX_SEALED_RUNS=off"; elif [ $mode = s2d ]; then E="FENVIX_SEALED_RUNS=2 FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1 FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_DERIVE=1 FENVIX_SEALED_CODEC=zstd"; else E="X=1"; fi
  echo "$b $mode run $i: $(env $E MONGO_SERVER_BIN=/home/user/bin-$b timeout 300 python -m pytest -q -s -p no:cacheprovider test_56_stale_reader_sst.py 2>&1 | grep -E '^answers|passed|failed' | tr '\n' ' ' | cut -c1-260)"
  for x in $M/rvw-stalereader*; do rm -rf "$x" "$M/.minio.sys/buckets/$(basename $x)"; done
done; done; echo DONE
