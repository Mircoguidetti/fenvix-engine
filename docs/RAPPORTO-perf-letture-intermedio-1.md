# Rapporto intermedio 1 – perf-letture

Stato al punto intermedio: confronto di partenza fatto, profilo per strato fatto, quattro correzioni nello strato MongoDB fatte e misurate (una quinta in misura). Il rapporto finale sarà `docs/perf-letture.md` con la PR in bozza.

Ogni numero è etichettato: MISURATO (misurato su questa macchina in questo lavoro) o STIMATO (calcolato o dedotto).

## In breve

- Su questa macchina mongod 8.0 è molto più veloce che nel lavoro bench-mongod: circa 18.000-22.000 letture puntuali al secondo su 1 core (MISURATO, run da 20 s), contro 9.137 nel rapporto precedente. Per questo, prima delle correzioni, il motore con jemalloc stava a circa 0,17 volte mongod sul database grande e 0,42 sui 100 piccoli (STIMATO, dal punto migliore degli sweep da 20 s).
- Il profilo dice una cosa diversa da quella attesa: SlateDB non è il 18% ma il 69% della CPU delle letture sul database grande (MISURATO). Però solo l'8,6% della CPU serve a leggere il documento richiesto: il resto di SlateDB sono 4 letture di metadati che lo strato MongoDB fa a ogni find (elenco delle collezioni, definizione degli indici, opzioni della collezione due volte) (MISURATO).
- Quattro correzioni nello strato MongoDB, una per commit, insieme danno circa +49% sulle letture del database grande e +31% sui 100 piccoli (STIMATO, prodotto dei guadagni misurati uno per uno).
- La leva più grande rimasta sono 3 letture di metadati per comando, che valgono il 61% della CPU delle letture sul database grande e il 36% sui 100 piccoli (MISURATO, profilo dopo le correzioni). Toglierle bene richiede una cache dei metadati dentro `mongo-server/src/dbs.rs`, che le regole chiedono di non toccare: mi fermo lì e la descrivo sotto.

## Come misuro

- Macchina: VM con 4 vCPU Xeon Cascade Lake, 15 GB di RAM, condivisa (il tempo rubato dall'hypervisor sulla CPU 0 è circa l'1% in ogni run, MISURATO da `/proc/stat`).
- Motore: main attuale (`658c724`, con jemalloc), release, `--store local`. mongod 8.0 in Docker. Database sulla CPU 0, generatore di carico (lo stesso di bench-mongod, in Go) sulle CPU 1-3.
- Dati: 1 database da 200.000 documenti da 1 KB, oppure 100 database da 2.000.
- Il motore parte a ogni run da una copia dello stesso snapshot dei dati, caricato una volta. Il caricamento del database grande in un colpo solo si blocca (lo stallo noto di fix-stall): 6 tentativi su 6 con 4 client e 6 su 6 con 1 client, sempre intorno ai 100.000-120.000 documenti (MISURATO). A blocchi da 10.000 documenti con 12 s di pausa tra un blocco e l'altro non si blocca (MISURATO, 1 caricamento su 1). Non ho lavorato sullo stallo.
- Ogni correzione è misurata con run alternate vecchio binario / nuovo binario (A, B, A, B...), 3 per parte, 30 s di riscaldamento e 30 s di misura, e riporto la mediana. Il rumore tra una run e l'altra dello stesso binario arriva al 10-20% (MISURATO): le correzioni sotto il 5% non si separano dal rumore.
- Profili: `perf record` a 299 Hz con stack DWARF per 30 s, nomi Rust decodificati con rustfilt; ogni campione è dato a uno strato guardando tutto lo stack (`bench/perf/layers.py`).

## Confronto di partenza (main con jemalloc, prima delle correzioni)

Punto migliore degli sweep da 20 s (MISURATO; i rapporti sono STIMATI):

- Letture puntuali, 1 database grande: motore 3.811 op/s, mongod 22.128 op/s, rapporto 0,17.
- Letture puntuali, 100 database piccoli: motore 7.505 op/s, mongod 18.069 op/s, rapporto 0,42.
- 50% letture e 50% aggiornamenti, 1 database grande: motore 1.866 op/s, mongod 5.793 op/s, rapporto 0,32.
- 50% letture e 50% aggiornamenti, 100 database: motore 3.266 op/s, mongod 6.093 op/s, rapporto 0,54.

Le misure finali, più lunghe (60 s di riscaldamento, 60 s di misura, 3 run), saranno nel rapporto finale.

## Profilo per strato, prima delle correzioni (MISURATO)

- Letture, database grande: SlateDB 68,9%, MongoDB 29,4%, rete e runtime 1,7%. Dentro SlateDB, per conto di quale lettura dello strato MongoDB: elenco delle collezioni (controllo "è una vista?") 16,3%, opzioni della collezione lette una seconda volta 13,8%, definizione degli indici 12,8%, opzioni della collezione 11,6%, il documento richiesto 8,6%.
- Letture, 100 database: SlateDB 50,0%, MongoDB 48,5%.
- Letture e aggiornamenti, database grande: SlateDB 73,0% (di cui scrittura dei file WAL e SST sullo store locale 28,3%), MongoDB 22,3%.
- Letture e aggiornamenti, 100 database: SlateDB 60,1%, MongoDB 36,5%.
- Dentro lo strato MongoDB (letture, database grande): copie di memoria dello stato dei future asincroni 5,6%, kernel per socket 7,7%, BSON 2,1%, allocazioni 2,6%, logica dei comandi 10,5%.

## Correzioni fatte (un commit ciascuna)

1. `find` legge le opzioni della collezione dalla vista che ha già, invece di aprire una seconda vista e rileggerle da SlateDB. Guadagno: +14,3% letture sul database grande, +9,5% sui 100 piccoli (MISURATO).
2. Niente più copie da 10-22 KB di stato asincrono a ogni comando: gli involucri sono funzioni semplici, il future di ogni comando è messo in un Box una volta sola, `authorize` non viene costruito quando l'autenticazione è spenta. Il future di `route` passa da 21,6 KB a 792 byte (MISURATO). Guadagno: +21,2% sul database grande, +16,7% sui 100 piccoli (MISURATO).
3. Il ciclo della connessione passa il comando invece di copiarlo. Guadagno: +7,7% sul database grande, +2,7% sui 100 piccoli, +2,8% su letture e aggiornamenti (MISURATO; il nuovo binario vince 8 coppie su 9).
4. Il registro di `currentOp` condivide il comando invece di copiarlo; chiave di sessione senza un `format!` per byte. Guadagno non separabile dal rumore (MISURATO: −4,4%, +6,3%, +7,3%); nel profilo valeva dall'1% al 2% della CPU (MISURATO), quindi circa 1-3% (STIMATO).
5. In misura: decodifica dei documenti BSON senza la copia intermedia che fa `Document::from_reader`.

## Profilo dopo le correzioni 1-4 (MISURATO)

- Letture, database grande: SlateDB 77,4%, MongoDB 21,5%. Dentro SlateDB: elenco delle collezioni 24,4%, definizione degli indici 18,4%, opzioni della collezione 17,8%, il documento 11,4%.
- Letture, 100 database: SlateDB 59,9%, MongoDB 39,0%. Dentro SlateDB: elenco delle collezioni 19,0%, il documento 17,4%, indici 9,0%, opzioni 7,9%.
- Copie di stato asincrono scese da 5,6% a 1,0% (database grande) e da 11,0% a 1,6% (100 database).

## Cosa blocca le soglie: serve dbs.rs

Le 3 letture di metadati per comando (elenco delle collezioni, indici, opzioni) sono il 61% della CPU sul database grande e il 36% sui 100 piccoli (MISURATO). Toglierle porterebbe il motore a circa 2,5 volte sul database grande e 1,5 volte sui 100 piccoli rispetto a ora (STIMATO dalle quote del profilo), cioè sopra le soglie di 0,5 e 0,8 volte mongod (STIMATO).

Per toglierle serve una cache dei metadati che sappia quando i metadati cambiano. Il posto giusto è `Entry` in `mongo-server/src/dbs.rs`, perché solo lì passano tutte le scritture (`apply_mode`), le aperture, le chiusure e le perdite di lease. Una cache fuori da dbs.rs non è sicura: con le letture durevoli (il default) una lettura subito dopo una scrittura può ancora vedere i metadati vecchi finché la scrittura non è durevole, e una cache riempita in quel momento resterebbe sbagliata. Le regole chiedono di non toccare dbs.rs: mi fermo qui su questo punto e nel rapporto finale descrivo il cambio proposto.

## File

- Script: `bench/perf/` (bench.py, ab.py, profile.sh, layers.py, run_ctl.sh, loadgen).
- Dati grezzi: `bench/perf/results/raw.jsonl`; profili per strato: `bench/perf/results/prof-*.layers.txt`.
