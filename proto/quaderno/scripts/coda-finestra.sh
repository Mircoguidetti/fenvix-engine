#!/bin/bash
# Giro 6 (M16): la finestra da sola (examples/finestra.rs), UN database, finestra vera: 600.000 record = 24 s a
# 25.000 scritture al secondo, 1.500.000 = 60 s a 25.000/s. Thread principale sul core 0, indicizzatori sul core 1.
# Albero = /root/finestra-albero (finestra del giro 5, stesso programma), hash = questo branch.
cd "$(dirname "$0")/.."
O=results/g6-finestra.jsonl
H=target/release/examples/finestra
A=/root/finestra-albero
one() { # bin nome mix keys w
  local r=$(taskset -c 0 $1 1 $4 $5 $(($5 + 400000)) $3 1)
  echo "{\"finestra\":\"$2\",\"r\":$r}" | tee -a $O
}
for rep in 1 2; do
  for k in 8 5; do
    for w in 600000 1500000; do one $H hash 2 $k $w; one $A albero 2 $k $w; done
    one $H hash 1 $k 600000; one $A albero 1 $k 600000
  done
done
echo FINE >> results/g6-finestra.FINE
