# M22 · rami usa e getta per gli agenti: quanto costa un Postgres fermo

Prova locale, $0. Il motore non è toccato. Soglie e metodo scritti prima: `SOGLIE.md`.
Macchina: 2 vCPU, 7 GB di RAM, nessuno swap, Linux 6.18. PostgreSQL 16.15 (quello disponibile qui).

## File

- `pg_rami.py`: N rami Postgres. Ogni ramo è un'istantanea btrfs (copy-on-write a blocchi, come i cloni ZFS di Xata)
  della cartella dati spenta del padre, più un postmaster suo. Misura creazione, RAM (RSS, PSS per tipo), prima lettura
  dopo 30 s da acceso, da sospeso (SIGSTOP/SIGCONT) e da spento (CHECKPOINT + arresto, poi riavvio a cache calda e fredda).
- `eng_rami.py`: N fork `clone` del nostro motore (`POST /v1/databases/{db}/fork`) su un solo mongo-server con
  `--store local`. Misura fork, RAM del processo, prima lettura da aperto e da chiuso, cancellazione.
- `durata_tentativi.py`: durata dei tentativi e pause tra le chiamate sulle 12 tracce di tau2-bench usate da M18
  (`sierra-research/tau2-bench`, commit 19f13ea3, `data/tau2/results/final`).
- `riassunto.py`: stampa breve dei JSON di `pg_rami.py`.
- `risultati/`: JSON e testo di ogni corsa.

## Corse

- `pg-1g-neon.json`: 50 rami, padre 1 GB (pgbench scala 68), shared_buffers 128 MB, max_connections 112.
- `pg-1g-stretta.json`: 50 rami, padre 1 GB, shared_buffers 16 MB, max_connections 20.
- `pg-1g-neon-pesante.json`: 10 rami, padre 1 GB, 10.000 letture e 2.000 scritture per ramo invece di 100 e 20.
- `pg-12g-neon.json`: 50 rami, padre 12 GB (scala 800; il più grande che entrava: la scala 1100 ha riempito il disco).
- `eng-1g.json`: 50 rami del motore, padre 1 milione di documenti da circa 1 KB (1,16 GB su disco dopo il lavaggio).
- `durata-tau2.json`: 4.448 tentativi.

## Rifare

```
# root; btrfs su file in loop
truncate -s 24G rami.img && mkfs.btrfs rami.img && mount -o loop rami.img /mnt/rami
btrfs subvolume create /mnt/rami/base1   # initdb + pgbench -i -I dtGvp -s 68, vacuum freeze, checkpoint, stop
python3 pg_rami.py --base /mnt/rami/base1 --n 50 --conf neon --out risultati/pg-1g-neon.json
cargo build --release -p mongo-server && python3 eng_rami.py --docs 1000000 --n 50 --out risultati/eng-1g.json
python3 durata_tentativi.py <tau2>/data/tau2/results/final risultati/durata-tau2.json
```
