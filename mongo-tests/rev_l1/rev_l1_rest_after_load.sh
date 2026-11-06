W=$HOME/load/l1-load/fix
$HOME/bins/ms-l1 --listen 127.0.0.1:27801 --store local --local-dir $W/store --replica-set rs0 > $W/server-rest.log 2>&1 &
SPID=$!
sleep 3
python3 -c "
from pymongo import MongoClient
c=MongoClient('mongodb://127.0.0.1:27801/?directConnection=true')
for d in ('app_data_4','app_data_6'): c[d].rev_rest.insert_one({'x':1})
print('opened')
"
for i in $(seq 1 40); do
  sleep 15
  F=$(df --output=avail -k $W | tail -1)
  L="t=$((i*15)) free_gb=$((F/1048576))"
  for t in app_data_4 app_data_6; do L="$L | $t $($HOME/fenvix-engine/target/release/examples/rev_l1_store_check $W/store $t | tr '\n' ' ')"; done
  echo "$L" >> $W/rest.log
  [ $F -lt 2000000 ] && echo "LOW DISK" >> $W/rest.log && break
done
kill $SPID; wait $SPID; echo DONE >> $W/rest.log
