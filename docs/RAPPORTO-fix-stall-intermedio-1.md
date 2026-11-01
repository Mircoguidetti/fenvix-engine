# Rapporto intermedio 1 — branch fix-stall

Stato al 2026-11-01, lavoro ancora in corso. Ogni numero è MISURATO (uscito da una prova su questa macchina: VM con 4 vCPU,
15 GB di RAM, store locale su disco o MinIO locale) oppure STIMATO (calcolato o dedotto). Nessuna spesa su cloud.

## Branch

- Ripreso `origin/fix-stall` dai 4 commit della sessione precedente; main non è andato avanti (il branch parte dall'ultimo
  main, jemalloc incluso), quindi nessun rebase né merge.
- Aggiunti finora: un commit di sola indentazione in `dbs.rs` e tre commit su `tools/stall/repro.sh` (registra compattazioni,
  richieste del controllo L0 e insert più lento; cancella i dati di ogni corsa; distingue un errore dello store da uno stallo;
  numero di documenti configurabile). Nessuna modifica alla logica del fix.

## 1. Causa: verificata

Build di diagnosi (main + `tools/stall/diag.patch`), caricamento di bench-mongod (200.000 documenti da ~1 KB, insertMany da
500, 16 in parallelo), store locale, motore su 1 core: **3 stalli su 3** (MISURATO). In ognuno:
- una sola richiesta di compattazione in tutta la corsa, quando L0 è arrivato a 4 SST; la compattazione porta L0 da 4 a 0 in
  2,5-3,1 s (MISURATO);
- da 6,0 a 7,4 s dopo quella richiesta tutte le scritture sono ferme dentro slatedb (`Db::write` → `maybe_apply_backpressure`),
  quindi prima che scadano i 10 s dopo i quali main avrebbe potuto chiedere un'altra compattazione (MISURATO);
- durante lo stallo: L0 = 8 sia nella vista del writer sia nel manifest salvato, coda di compattazione vuota, lock di
  manutenzione libero, CPU del motore 0,8% di un core (8 tick in 10 s), tutti i 15 thread fermi in attesa (MISURATO);
- il dump dei task tokio mostra un insert fermo nella backpressure di slatedb mentre tiene il lock del database, e un altro
  insert in coda su quel lock (MISURATO).

In parole semplici: su main la compattazione di un database aperto veniva chiesta solo da una scrittura appena finita, al
massimo una volta ogni 10 s. Sotto inserimenti in blocco L0 passa da 4 a 8 SST in meno di 10 s; a 8 slatedb smette di scaricare
la memtable, la memoria non scaricata arriva al limite e ogni scrittura si ferma. Nessuna scrittura finisce più, quindi nessuno
chiede più la compattazione: il blocco è permanente. La diagnosi dei commit precedenti è corretta.

Main senza diagnosi: 3 stalli su 3 anche con 2 core su store locale, 3 su 3 su MinIO con 1 core, 3 su 3 su MinIO con 2 core, e
in tutte queste 9 corse il motore non si è fermato entro 30 s da SIGTERM (MISURATO).

## 2. test_28

- Su main: **fallisce** (56 lotti da 500 accettati, poi timeout di 60 s; 1 sola compattazione) (MISURATO).
- Con il fix: **passa** (200 lotti in 11,0 s, lotto più lento 2,0 s, 10 compattazioni, 11 richieste del controllo L0) (MISURATO).

## 3. Criterio dei 20+20 caricamenti: raggiunto

Binario del fix (commit e2c084d; i commit aggiunti dopo non cambiano il codice del motore), nessun blocco (MISURATO):
- store locale, 1 core: 20 su 20, durata media 13,4 s (12,1-15,2), insert più lento 0,9 s;
- store locale, 2 core: 20 su 20, media 9,3 s (8,6-11,4), insert più lento 0,9 s;
- MinIO, 1 core: 20 su 20, media 14,9 s (13,2-16,4), insert più lento 1,5 s;
- MinIO, 2 core: 20 su 20, media 10,1 s (9,4-10,8), insert più lento 1,0 s.
In tutte le 80 corse: da 3 a 6 compattazioni per caricamento, nessuna chiusura sporca a SIGTERM, nessun errore di compattazione.

Nota: una prima serie MinIO 2 core aveva dato 14 "stalli" su 20. Non erano stalli del motore: il disco era pieno perché ogni
corsa lasciava il suo bucket (~0,3 GB) e MinIO rispondeva `XMinioStorageFull` a ogni scrittura. Serie scartata e ripetuta dopo
aver corretto lo script.

Suite ctl-tests completa: ancora da eseguire.

## 5. c9a4beb (produzione), in corso

- Con il caricamento standard (200.000 documenti) **nessun blocco**: 20 su 20 (5 per combinazione) (MISURATO). Ma la prova non
  è significativa: in c9a4beb il memtable è quello di default di slatedb (64 MiB), quindi 200 MB di dati fanno ~3 SST in L0 e
  la soglia di compattazione (4) non viene mai raggiunta (0 compattazioni in tutte le corse, MISURATO).
- Con 2.000.000 di documenti, impostazioni di produzione, store locale, 1 core: 3 caricamenti su 3 finiti ma lenti (279-343 s),
  con insert fermi fino a 34 s, e in tutti e 3 la chiusura a SIGTERM ha rinunciato al flush finale (chiusura sporca) (MISURATO).
  Con 2 core: 2 caricamenti finiti finora, 237-245 s, insert più lento 3,1 s (MISURATO).
- Ancora da fare: MinIO, la controprova con memtable da 8 MiB e lo stesso caricamento grande con il fix.

## 6. Confronto con mongod

Da fare (dopo la suite).
