# Rapporto: proto-lettura

Branch `proto-lettura` (da `origin/main`). Codice in `proto/lettura`, interfaccia in `proto/INTERFACCIA.md`, documento
tecnico in `docs/proto-lettura.md`, risultati grezzi in `proto/lettura/risultati/`. Tutto in locale (4 core, 15 GB
di RAM, un disco), MinIO compilato dai sorgenti (commit 7aac2a2c5b7c, febbraio 2026). Nessuna spesa cloud.

Ogni numero è MISURATO (uscito da una prova su questa macchina) o STIMATO (calcolato da misure o da ipotesi dette).

## In breve

- Passo 1, scarico da chiunque: SUPERATO. In un'ora, con 3 worker sugli stessi database, 408 uccisioni SIGKILL e 269
  abort spontanei, 2.879.083 scritture confermate, zero perse e zero doppie (MISURATO). MinIO risponde 412 e non 304
  ai PUT condizionali falliti: il bug non c'è in questa versione (MISURATO).
- Passo 2, velocità: SUPERATO. 20.000 scritture/s da 1,2 KB, cioè 25 MB/s, scaricate con 0,11 core; ritardo
  massimo verso S3 0,7 s (MISURATO).
- Passo 3, lettura composta: SUPERATO. p99 delle letture servite dagli SST 0,68 ms con cache in RAM e 1,17 ms con
  cache su disco; nessun valore sbagliato nella verifica (MISURATO). Senza cache, da MinIO, p99 68 ms (MISURATO).
- Passo 4, database fermo: SUPERATO. Al massimo 5 richieste S3 per la prima risposta, 2-3 se il database è
  compattato (MISURATO); con 30 ms per GET la prima risposta arriva in 69-174 ms (MISURATO), 40-200 ms su S3
  Standard (STIMATO).
- Passo 5, costo per database piccolo: SUPERATO solo con una variante che non era tra le due proposte. Scarico ogni
  10 minuti in cui un database piccolo (fino a 4 MB) viene riscritto intero in un solo SST: $0,048 al mese per
  database, di cui GET $0,005 (MISURATO a regime, poi riportato al mese). Le due varianti proposte non stanno nelle
  soglie: (a) scarico a 10 minuti con run aggiunte costa $0,16-0,18, quasi tutto di GET; (b) oggetto condiviso per
  server costa $0,22-0,26, tutto di GET (MISURATO). Il costo della variante buona si paga in RAM del registro: fino a
  690 MB ogni 1.000 database per replica (MISURATO).
- Passo 6: schema dei prefissi S3 progettato, con hash del gruppo in testa alla chiave; con la variante buona,
  100.000 database stanno tre ordini di grandezza sotto i limiti per prefisso (STIMATO).
- Lungo la strada ho trovato e corretto sei difetti miei (descritti sotto), due dei quali hanno fatto ripartire la
  prova di un'ora del passo 1. La prima corsa del passo 5 è stata invalidata perché fatta insieme al passo 1 sulla
  stessa macchina.

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

## Passo 5: costo S3 per database piccolo — SUPERATO con la variante a2, NON SUPERATO con (a) e (b)

Prova (`pl-costo`): 1.000 database, ognuno con 500 documenti da 1 KB (circa 0,5 MB) precaricati negli SST; ogni
database riceve 1 scrittura (aggiornamento di un documento) e 2 letture puntuali al secondo, chiavi scelte con
distribuzione zipf 0,99 (poche chiavi calde, come nei carichi YCSB); una sola cache da 1 GB su disco locale per i
nodi d'ingresso di tutti i 1.000 database; un worker; registro con finestra di 60 s; 1 ora. Si contano tutte le
richieste a MinIO lato client (worker e lettori) e lato server (contatori del bucket): i due conteggi coincidono in
tutte le corse. Il costo al mese per database è (richieste nell'ora) x 720 / 1.000, a $0,005 ogni 1.000 PUT o LIST e
$0,0004 ogni 1.000 GET; i DELETE (`DeleteObjects` dopo la compattazione) sono contati ma su AWS non si pagano. Il
"regime" è la seconda mezz'ora: nella prima i dati di base sono ancora freschi e gli scarichi lenti non sono ancora
partiti, quindi l'ora intera sottostima.

Riferimento, scarico ogni 60 s, un SST e un manifest per database (`base`, misurato 15 minuti, dal minuto 5 al 20):
circa 105.800 PUT al mese per database, $0,53 di soli PUT (MISURATO, riportato al mese). Conferma il conto "un PUT al
minuto costa già $0,216": qui sono due PUT al minuto (SST e manifest) più la compattazione.

Variante (a), scarico per dimensione o età lunga (1 MB o 10 minuti), un SST e un manifest per database, run aggiunte e
compattate (MISURATO, 1 ora, zero errori di lettura su 7.200.012):
- costo a regime $0,185 al mese per database: GET $0,127 (317.000 al mese), PUT $0,058 (11.500 al mese); sull'ora
  intera $0,158. Fuori da entrambe le soglie.
- Da dove vengono le GET: dopo ogni scarico ogni database ha una run nuova da circa 600 KB, letta a blocchi da 16 KB
  (circa 70 GET per database per scarico), e i blocchi della base vengono sfrattati e riletti perché base più run
  recenti superano 1 MB per database, la sua quota di cache.
- RAM trattenuta nel registro: fino a 690 MB ogni 1.000 database (media 350 MB), per ogni replica. Sono i 10 minuti di
  scritture non ancora scaricate più 60 s.
- Complessità: bassa (come la base).

Variante (b), un oggetto condiviso per server: un solo gruppo con i 1.000 database, un SST condiviso e un manifest
condiviso a ogni scarico (ogni 60 s), compattazione del gruppo (MISURATO, 1 ora, zero errori su 7.200.006):
- costo a regime $0,256 al mese per database: GET $0,255 (638.000 al mese), PUT $0,0006 (115 al mese); sull'ora
  intera $0,223. I PUT diventano trascurabili, ma le GET sono fuori soglia di 5 volte.
- Da dove vengono le GET: i dati escono dalla finestra del registro dopo circa 2 minuti, quindi le letture di dati
  di 2-10 minuti fa vanno agli oggetti condivisi; ogni minuto ogni database ha una fetta nuova (circa 60 KB) in un
  oggetto da 60 MB, e la cache da 1 GB non tiene fette, base e meta dei 1.000 database insieme.
- RAM trattenuta nel registro: fino a 71 MB ogni 1.000 database. È il vantaggio di questa variante.
- Complessità: alta. Compattazione di gruppo che riscrive gigabyte (7,6 GB letti e 4,4 GB scritti nell'ora); la
  prima lettura di un database fermo deve leggere il meta di tutte le run del gruppo (molto più di 5 richieste);
  spostare, cancellare o ripristinare un database tocca gli oggetti di tutti; un database caldo rallenta gli altri.
  Sotto questo carico il banco è rimasto indietro fino a 15 s e il p99 di lettura è salito a 1 s (MISURATO).

Variante (a2), aggiunta da me: come (a), ma se il database è piccolo (tutte le sue run insieme fino a 4 MB) lo
scarico riscrive tutto il database in un solo SST (base fusa con i record nuovi) invece di aggiungere una run
(MISURATO, 1 ora, zero errori su 7.200.006, p99 di lettura 408 µs):
- costo a regime $0,048 al mese per database: GET $0,0052 (13.100 al mese), PUT $0,043 (8.640 al mese: un SST e un
  manifest ogni 10 minuti); sull'ora intera $0,041. Dentro entrambe le soglie (GET sotto $0,05, totale sotto $0,15).
- Perché funziona: ogni database è sempre un oggetto solo da circa 0,5 MB, che sta nella sua quota di cache e si
  rilegge con un GET intero a ogni scarico (più il manifest); tra uno scarico e l'altro non c'è nessuna GET.
- RAM trattenuta nel registro: come (a), fino a 690 MB ogni 1.000 database per replica.
- Costo nascosto: il worker riscrive 0,5 MB per database ogni 10 minuti (3,3 GB scritti nell'ora per 3 milioni di
  record, circa 1,1 volte i dati nuovi) e rilegge la base quando non l'ha in cache (5.800 GET nell'ora, già contate).
  Sopra i 4 MB la riscrittura costa troppa banda e il database passa alla variante (a), con il suo problema di GET.
- Complessità: bassa, poche righe in più nello scarico (`riscrivi_max` in `src/scarico.rs`), provata anche con il
  banco di uccisione (3 minuti, 16 uccisioni e 11 abort, zero perse e zero doppie su 71.790 scritture, MISURATO).

Dato in più (20 minuti, MISURATO): con la cache a oggetti interi fino a 1 MB, invece che a blocchi, le varianti (a) e
(b) mandano la cache in un ciclo continuo di sfratti (fino a 741.738 sfratti in 20 minuti) perché ogni database vuole
più della sua quota di 1 MB. La macchina si è sovraccaricata e ho fermato la corsa. Risultati in
`risultati/passo5-cache-oggetti-interi-20min`.

Cosa decide il costo, in una riga: per un database piccolo le GET si tengono basse solo se i dati di ogni database
stanno nella sua quota di cache in un solo oggetto, e i PUT solo se si scarica raramente. Scaricare raramente costa
RAM nel registro: 690 MB ogni 1.000 database per replica con 10 minuti (STIMATO: con 5 minuti la RAM si dimezza e i PUT
raddoppiano, $0,086 al mese di soli PUT).

Limiti: un solo nodo d'ingresso (con più nodi d'ingresso senza instradamento per database le GET si moltiplicano per
il numero di nodi che servono lo stesso database); database da 0,5 MB e chiavi zipf (database più grandi o letture
uniformi danno più GET); manifest riletto solo quando serve, come proposto in `proto/INTERFACCIA.md`.

## Passo 6: schema dei prefissi S3 (solo progetto)

Limiti di S3 da rispettare: circa 3.500 richieste al secondo di tipo PUT/COPY/POST/DELETE e 5.500 di tipo GET/HEAD per
prefisso partizionato. S3 divide un bucket in partizioni per prefisso, gradualmente, quando il carico cresce; un picco
improvviso su un prefisso solo dà `503 SlowDown`.

Schema (già usato dal prototipo, `proto/lettura/src/manifest.rs`):

    {h}/{gruppo}/manifest
    {h}/{gruppo}/sst/{posizione massima, 16 cifre esadecimali}-{casuale}.sst

- `gruppo` è l'id del database (scarico per database) o l'id del gruppo condiviso (variante b).
- `h` sono cifre esadecimali di un hash del gruppo, **all'inizio della chiave**: i database si spalmano in modo
  uniforme su 16^n prefissi, e S3 può dividere il bucket lungo `h` senza che un prefisso si porti dietro un pezzo
  troppo grande del carico. Il prototipo usa 2 cifre (256 prefissi); per flotte grandi bastano 3 cifre (4.096).
- Niente data o posizione in testa alla chiave: un prefisso crescente nel tempo concentra tutte le scritture nuove
  sull'ultima partizione.
- Il nome dell'SST è unico (posizione massima + casuale) e non si riusa mai: nessuna sovrascrittura, nessun bisogno
  di versioni.

Conti per una flotta, con le frequenze misurate nel passo 5 (variante a2: 12 PUT e circa 18 GET all'ora per database,
compresi worker e lettori). STIMATO: 100.000 database fanno circa 330 PUT/s e 500 GET/s in tutto; su 256 prefissi
sono circa 1,3 PUT/s e 2 GET/s per prefisso, tre ordini di grandezza sotto i limiti. Anche se S3 tenesse tutto il
bucket in una sola partizione (bucket nuovo), 330 PUT/s stanno sotto 3.500. Con la variante (a) le GET sarebbero
circa 440 all'ora per database: 12.000 GET/s per 100.000 database, ancora sotto 5.500 per prefisso con 256 prefissi.

Dove si può sforare e cosa fare:

- **Database caldo** (passo 2: 20.000 scritture/s): circa 3 SST e 3 manifest al secondo, più le GET di lettura dei
  nodi d'ingresso che non hanno i blocchi in cache. Se un database da solo avvicina 5.500 GET/s su un prefisso, i suoi
  SST si spostano in sottoprefissi per hash dell'id: `{h}/{gruppo}/sst/{hh}/{id}.sst` (il manifest elenca la chiave
  completa, quindi lettori e worker non cambiano).
- **Manifest di un database caldo**: è un solo oggetto, letto da ogni nodo d'ingresso a ogni scarico. Con 1.000 nodi
  d'ingresso e uno scarico al secondo sono 1.000 GET/s su una chiave: dentro il limite, ma è il primo punto a saturare.
  Rimedio proposto: il registro porta nella conferma di scarico anche le run nuove (id, intervallo del meta), così i
  nodi d'ingresso aggiornano il manifest in memoria senza GET; la GET resta per chi parte da zero.
- **Riavvio di molti nodi d'ingresso insieme**: ogni nodo rilegge i manifest dei database che serve. Con le chiavi
  spalmate da `h` il picco si divide su tutte le partizioni; i nodi devono comunque ritentare con attesa crescente sui
  `503`.
- **Pulizia degli orfani** (LIST per gruppo): conta come PUT per prezzo e limite. Una volta al giorno per gruppo, o
  solo per i gruppi in cui un worker è morto a metà (il manifest può segnarlo), costa poco (passo 5).
- **Cancellazioni dopo la compattazione**: `DeleteObjects` toglie fino a 1.000 chiavi per richiesta; su AWS il DELETE
  non si paga, ma conta nei 3.500 al secondo per prefisso (STIMATO: il limite si applica per oggetto). Con le chiavi
  spalmate da `h` non è un problema.
- **Bucket nuovo con molto carico subito**: S3 parte con poche partizioni. Per una cella nuova di molti database si
  chiede ad AWS la pre-partizione sul primo carattere di `h`, o si distribuiscono le celle su più bucket (per esempio
  un bucket ogni 50.000 database).
- **Worker**: qualunque worker può prendere qualunque gruppo; conviene assegnare i gruppi per hash coerente di `h` ai
  worker, così ognuno ha in cache gli SST che compatta e i conflitti sul manifest restano rari (nel passo 1, con 3
  worker sugli stessi 8 database, i conflitti sono frequenti ma innocui).


## Cosa manca per collegarlo a proto-quaderno

Dettagli in `docs/proto-lettura.md`. In breve: un adattatore del tratto `Registro` sopra il client di proto-quaderno; i
tre campi in più sul filo (`potato_fino_a`, esito delta, byte in `pendenti`); un solo scaricatore (quello di
proto-lettura, con SST indicizzati); verificare la RAM della finestra quando lo scarico è lento (passo 5); allineare
object_store (0.11 là, 0.14.2 qui e nel motore). Fuori dal prototipo: scansioni e query su indice composte (c'è una
`scan` semplice, non misurata), collegamento a `mongo-server`, chiave di sessione.

## Limiti di queste misure

- Registro simulato: un processo, commit confermati subito, nessuna replica, nessun fdatasync. Le latenze di lettura
  hanno un giro TCP locale verso la replica, non tra zone (in zone diverse si aggiungono circa 0,5-1 ms, STIMATO).
- Difetti miei trovati e corretti: nomi uguali degli SST di base e chiave di cache (passo 3); compattazione completa
  "a riposo" decisa dalla memoria locale invece che dal manifest; regola "troppe run" che rifondeva una run grande
  (passo 1); cache su disco che poteva leggere un blocco a metà scrittura (8 letture fallite su circa 2,4 milioni
  nella prima corsa del passo 5, MISURATO); lavoro di CPU del worker sui thread asincroni; cache a oggetti interi che
  non regge con 1 MB per database (passo 5).
- Tutto su una macchina da 4 core e un disco: MinIO, worker, banchi e cache si contendono CPU e disco. La prima corsa
  del passo 5, fatta insieme al passo 1, è stata invalidata per questo.
- La cache "su disco" del passo 3 era servita dalla cache di pagina del sistema.
