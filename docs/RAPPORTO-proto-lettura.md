# Rapporto: proto-lettura

Branch `proto-lettura` (da `origin/main`). Codice in `proto/lettura`, interfaccia in `proto/INTERFACCIA.md`, documento
tecnico in `docs/proto-lettura.md`, risultati grezzi in `proto/lettura/risultati/`. Tutto in locale (4 core, 15 GB
di RAM, un disco), MinIO compilato dai sorgenti (commit 7aac2a2c5b7c, febbraio 2026). Nessuna spesa cloud.

Ogni numero è MISURATO (uscito da una prova su questa macchina) o STIMATO (calcolato da misure o da ipotesi dette).

## In breve

__SINTESI__

## Il registro e l'interfaccia

Il registro è simulato dietro l'interfaccia comune (un processo, commit confermati subito, finestra di 60 s in RAM,
servito anche via TCP). Su `proto-quaderno` c'era già `proto/INTERFACCIA.md`: mi sono allineato a quella (nessun punto
concordato cambiato) e ho scritto in `proto/INTERFACCIA.md` di questo branch cinque differenze proposte:

1. `fuori_finestra` porta `potato_fino_a`, la posizione fino a cui i record sono usciti dalla finestra: il nodo
   d'ingresso rilegge il manifest solo quando quello in cache non la copre. È il controllo `M >= inizioFinestra` di
   `docs/protocollo.md` §6.2, fatto senza un giro in più.
2. Un esito `delta(somma)` per le chiavi che nella finestra hanno solo delta: oggi proto-quaderno risponde
   `fuori_finestra` e la lettura perderebbe i delta non ancora su S3 (lo dice anche il suo INTERFACCIA.md come limite).
3. `stato_scarico(db)`.
4. `pendenti` con byte, numero di record e posizione già su S3 (proto-quaderno dà solo database e posizione del più
   vecchio): servono allo scarico per dimensione e alla riparazione dopo un worker morto.
5. Manifest per gruppo di database (già compatibile con la conferma multi-database di proto-quaderno).

Tre regole di `docs/protocollo.md` (branch tla-registro) da rivedere alla luce delle misure: rileggere il manifest ogni
5 s costa circa $0,21 al mese per database di sole GET (STIMATO, 518.400 GET al mese); scaricare ogni 5 s costa circa
$5,18 al mese per database di PUT (STIMATO); cancellare subito l'SST dopo un 412 è sicuro solo dopo aver riletto il
manifest, perché il 412 può arrivare al ritentativo automatico di un PUT già riuscito.

Formato SST: non ho potuto riusare quello di SlateDB 0.17. Il costruttore di SST è privato al crate; il lettore
pubblico (`SstReader`) esiste ma fa da sé le sue GET (piè di pagina, poi meta), quindi non si possono né contare né
ridurre le richieste della lettura fredda. Ho scritto un formato con lo stesso schema (blocchi, indice, Bloom, versioni
con la posizione come sequenza) e un meta leggibile con un solo GET a intervallo scritto nel manifest. Dal motore ho
riusato lo spazio chiavi (`doc/{collezione}/{id}`, `idx/...`) e la versione di object_store (0.14.2).

## Passo 1: scarico da chiunque — SUPERATO

Prova: registro simulato con 8 database, 400 commit al secondo da 1 a 3 scritture (put, e 5% ciascuno di delete e
delta), valori da 100 a 1.500 byte; 3 worker sugli stessi database; ogni 2-15 s un worker a caso ucciso con SIGKILL e
riavviato; in più ogni worker si fa `abort()` da solo con probabilità 0,2% in 5 punti delicati (dopo il PUT dell'SST,
dopo il manifest, dopo la conferma, e i due della compattazione). Durata 1 ora, poi svuotamento e verifica record per
record: ogni scrittura confermata (dal log del registro) deve stare in esattamente una voce degli SST elencati nei
manifest, con tipo e crc del valore giusti; manifest e registro devono essere d'accordo sulla posizione scaricata.

Risultato, MISURATO:
- 408 uccisioni SIGKILL e 269 abort spontanei in un'ora;
- 1.440.069 record confermati, 2.879.083 scritture attese, 2.879.083 trovate esattamente una volta;
- zero perse, zero doppie, zero estranee, zero valori sbagliati, zero run mancanti, manifest e registro allineati
  per tutti gli 8 database;
- 26.780 scarichi riusciti, 33.973 conflitti sul manifest risolti dal PUT condizionale (35.615 risposte 412),
  1.235 riparazioni (worker morto tra manifest e conferma), 954 orfani tolti dalla pulizia, 6 orfani rimasti
  (scritti dopo l'ultima pulizia), 180 SST in attesa di cancellazione;
- nessuna risposta 304 da MinIO.

Controllo negativo, MISURATO: lo stesso banco con un worker che scrive il manifest senza condizione perde 16.214
scritture su 48.092 in 60 s. Il verificatore vede gli errori.

MinIO e il bug del 304, MISURATO (`proto/lettura/scripts/cond_minio.py`, `risultati/cond_minio.json`): PUT con
If-Match sbagliato risponde 412 e non scrive; If-None-Match: * su oggetto esistente risponde 412; If-Match su
oggetto assente risponde 404; PUT con If-None-Match uguale all'etag risponde 412; 32 PUT in gara con lo stesso
If-Match: 1 risponde 200 e 31 rispondono 412, e lo stesso con 32 If-None-Match: *. Il 304 compare solo sul GET con
If-None-Match, dove è corretto. In questa versione il bug non c'è. Il worker tratta comunque un eventuale 304 come
conflitto e lo conta a parte.

Due difetti di prestazione trovati da questa prova, corretti, ma presenti nella corsa di un'ora:
- la regola "troppe run, fondi le più recenti" rifondeva sempre una run grande finita in testa. Nell'ora la
  compattazione ha riscritto circa 142 GB per 2,1 GB scaricati, 66 volte (MISURATO). La correttezza non cambia,
  anzi la prova è più dura (8.939 compattazioni concorrenti). Con la regola corretta (limite 30 run, si fonde la
  coppia adiacente più piccola) la stessa prova di 10 minuti fa 3,5 volte, con 63 uccisioni e 40 abort e di nuovo
  zero perse e zero doppie su 480.489 scritture (MISURATO, `risultati/passo1-prova-10min-regola-nuova`);
- prima ancora, con 3 worker ognuno credeva "fermo" un database scaricato dagli altri e lo ricompattava tutto ogni
  30 s (amplificazione circa 170 volte, MISURATO). Ora il riposo si decide dall'ora dell'ultimo scarico nel manifest.
Per questi due difetti la prova di un'ora è stata ripartita due volte da zero; i numeri sopra sono della terza corsa,
completa.

Il cestino interno di MinIO (oggetti cancellati) si svuota troppo piano sotto molte cancellazioni: 14 GB in pochi
minuti (MISURATO). È un effetto di MinIO in locale, non di S3: lo script `scripts/pulisci_cestino.sh` toglie le voci
più vecchie di 20 s.

## Passo 2: velocità — SUPERATO

Prova: un database, 20.000 commit al secondo da una scrittura di 1.200 byte su chiavi casuali; worker limitato a 2
core (`taskset -c 2,3`), registro con il generatore e MinIO sugli altri 2; scarico a 1 s di età o 8 MB; 5 minuti, il
primo scartato.

Risultato, MISURATO: 20.000 commit/s generati e 20.020 record/s scaricati (25,1 MB/s di SST, circa 3 scarichi al
secondo, 875 in tutto); il worker usa 0,11 core; ritardo massimo tra commit e S3 671 ms; al massimo 12,7 MB non
ancora su S3; nessun errore.

Senza compattazione: la fusione l'ho misurata a parte (`pl-fusione`): 472-676 MB/s per core su 4 run da 16 MB, in due misure (MISURATO).
Con un'amplificazione di circa 5 per una compattazione a livelli sul flusso di 25 MB/s, sono circa 125 MB/s, meno di
0,2 core (STIMATO). Lo scarico più la compattazione stanno ampiamente in 2 core; il collo di bottiglia vero sarebbe
la banda verso S3 (circa 25 MB/s di PUT più 125 MB/s di riscrittura, STIMATO).

## Passo 3: lettura composta — SUPERATO

Prova: 4 database da 50.000 documenti da 1 KB (circa 210 MB) precaricati in SST; durante la prova 2.000 scritture al
secondo su chiavi casuali, scarico ogni 2 s e compattazione; letture puntuali alla posizione confermata, 30% su chiavi
scritte da poco e 70% su chiavi qualsiasi, 8 in parallelo; la replica è raggiunta via TCP locale (un giro di rete per
ogni lettura); 200.000 letture dopo aver riscaldato la cache. Alla fine si fermano le scritture e si confronta ogni
chiave scritta con il modello.

Risultato, MISURATO:
- cache in RAM: tutte le letture p50 86 µs, p99 466 µs, p99,9 2,3 ms; quelle servite dagli SST (79.726) p99 679 µs;
  quelle dalla finestra p99 379 µs;
- cache su disco locale (file per blocco; la cache di pagina del sistema li tiene in RAM, quindi è un caso favorevole):
  tutte p99 747 µs; dagli SST p99 1.171 µs; dalla finestra p99 541 µs;
- verifica: 87.676 e 92.425 chiavi scritte lette giuste, zero sbagliate; tutte le chiavi del precarico presenti.

Senza cache (solo come dato, 5.000 letture, MISURATO): letture dagli SST p50 27,6 ms, p99 67,6 ms, circa 8,5 GET per
lettura (senza cache ogni lettura rilegge il meta di ogni run che attraversa, più il blocco).

La verifica dei valori ha trovato un errore mio, corretto: gli SST del precarico avevano lo stesso nome in tutti i
database e la cache usava il nome come chiave.

## Passo 4: database fermo — SUPERATO

Prova: per ogni configurazione, da 5 a 30 database su MinIO; per ciascuno un nodo d'ingresso nuovo (cache vuota,
nessun manifest) fa una lettura puntuale; si contano le richieste S3 e il tempo, anche con un ritardo artificiale di
30 ms per GET (S3 Standard).

Risultato, MISURATO (richieste S3 per la prima risposta):
- database piccolo (100 documenti, circa 100 KB) compattato in una run: 2 (manifest + SST intero);
- 10 MB o 100 MB compattati: 3 (manifest + meta + blocco);
- con 3 run non compattate: da 2 a 4 (piccolo), 5 (10 MB e 100 MB).
Mai più di 5. La seconda lettura dello stesso database costa 0 o 1 GET.

Tempi con 30 ms per GET, MISURATO: database compattato 69 ms (piccolo) e 103-105 ms (grande); con 3 run 103-109 ms se i
meta si chiedono in parallelo, fino a 174 ms se in serie. STIMATO su S3 Standard (20-40 ms per GET, GET in serie
manifest → meta → blocco): da 40-80 ms (2 GET) a 60-120 ms (3 GET in serie), più un giro verso il registro. Dentro i
100-200 ms dell'ipotesi.

Un database fermo ha di norma una run sola: il worker fonde tutto quando il manifest dice che non si scarica da un po'.
Il caso a 5 richieste è il database che si ferma subito dopo uno scarico, prima della compattazione.

## Passo 5: costo S3 per database piccolo

__PASSO5__

## Passo 6: schema dei prefissi S3 (solo progetto)

__PASSO6__

## Cosa manca per collegarlo a proto-quaderno

Dettagli in `docs/proto-lettura.md`. In breve: un adattatore del tratto `Registro` sopra il client di proto-quaderno; i
tre campi in più sul filo (`potato_fino_a`, esito delta, byte in `pendenti`); un solo scaricatore (quello di
proto-lettura, con SST indicizzati); verificare la RAM della finestra quando lo scarico è lento (passo 5); allineare
object_store (0.11 là, 0.14.2 qui e nel motore). Fuori dal prototipo: scansioni e query su indice composte (c'è una
`scan` semplice, non misurata), collegamento a `mongo-server`, chiave di sessione.

## Limiti di queste misure

- Registro simulato: un processo, commit confermati subito, nessuna replica, nessun fdatasync. Le latenze di lettura
  hanno un giro TCP locale verso la replica, non tra zone (in zone diverse si aggiungono circa 0,5-1 ms, STIMATO).
- Tutto su una macchina da 4 core e un disco: MinIO, worker, banchi e cache si contendono CPU e disco. La prima corsa
  del passo 5, fatta insieme al passo 1, è stata invalidata per questo.
- La cache "su disco" del passo 3 era servita dalla cache di pagina del sistema.
