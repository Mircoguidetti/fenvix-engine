# Rapporto proto-quaderno (S2): registro "senza cuoco", prototipo e banchi

Branch `proto-quaderno` (da origin/main). Tutto in locale: nessuna spesa su AWS o altri cloud, S3 simulato con MinIO
compilato in locale. Codice in `proto/quaderno` (crate Rust autonomo), interfaccia in `proto/INTERFACCIA.md`, risultati
grezzi in `proto/quaderno/results/`. Ogni numero è MISURATO (su questa macchina: 4 vCPU, 16 GB, un disco cloud) o
STIMATO.

## 1. In breve

- Il prototipo funziona: tre repliche in tre processi, un leader che valida e ordina, conferma a 2 su 3 dopo
  fdatasync, finestra di 60 s per chiave su ogni replica, letture alla posizione S, scarico su MinIO, proxy di guasti.
- **Banco 1 superato.** Validazione sul leader: 21,2 µs per record da 5 chiavi, cioè 47.000 record/s per core
  (MISURATO). Commit completi con una mutazione da 200 byte: 26.315–27.007 record/s con p99 3,7–3,9 ms (MISURATO,
  48 client, 3 ripetizioni).
- **Banco 2 non superato: mi sono fermato qui, come chiesto.** La RAM va bene: circa 82 KB per database piccolo
  (MISURATO), sotto i 256 KB, e 0,8–1,1 GB per replica, sotto i 3 GB. Le latenze invece no:
  - col carico completo (30.000 scritture/s, 100.000 letture/s) il sistema crolla: 2.580 commit/s confermati e code
    di decine di secondi (MISURATO);
  - anche al 25% del carico la p99 delle letture dalla finestra è 19,5 ms contro una soglia di 1 ms, e "leggi la tua
    scrittura" 8,9 ms contro 3 ms (MISURATO);
  - al 50% i commit confermati sono 14.662/s, sotto i 15.000 (MISURATO).
- Banchi 3–7 non eseguiti (regola: stop alla prima soglia saltata). Gli script ci sono; più sotto le prove brevi di
  correttezza su guasti del leader e partizioni, che **non** sostituiscono i banchi 4 e 5.
- La causa principale, secondo me (STIMATO dalle misure di CPU e dai profili), è il prototipo più la macchina, non il
  disegno. Ogni replica fa tutto su un solo thread: commit, replica, letture e lavoro di scarico. Le letture aspettano
  in coda dietro il resto. Inoltre tre repliche, generatore, proxy, MinIO e scaricatore stanno su 4 vCPU, e il solo
  generatore usa 1,16 core. Non l'ho dimostrato: cosa servirebbe per dimostrarlo è al §5.

## 2. Una scelta di lettura da confermare (banco 1)

La soglia 1b dice "commit completi con mutazioni da 200 byte", senza dire quante per record. L'ho letta come **una
mutazione da 200 byte per record**: il testo chiede a parte "record da 5 mutazioni da 1 KB", quindi il caso base non
sembra da 5. Con questa lettura la soglia è superata con margine. Con l'altra lettura (5 mutazioni da 200 byte per
record) la soglia **non** è raggiunta su questa macchina:
- con 32 client: 13.008–14.226 record/s, p99 4,3–4,8 ms (MISURATO);
- con più client il throughput scende e la p99 sale a 16–26 ms (MISURATO).

Se la lettura giusta era la seconda, la prima soglia saltata è questa e non il banco 2.

## 3. Risultati

### Banco 1a: costo della validazione sul leader, isolato (soglia: sotto 30 µs per record con 5 chiavi)

Un thread solo, finestra a regime: 6 milioni di versioni, 10.000 database, 5 chiavi casuali da 200 byte per record.
Tre giri:
- validazione sola: 6,6 µs per record (MISURATO);
- validazione + codifica della voce + indicizzazione nella finestra + potatura: 21,1–21,3 µs per record, cioè
  47.000–47.300 record/s per core (MISURATO).

Superata. All'inizio erano 27,5 µs: il 16% della CPU del leader se ne andava in confronti di chiavi che seguivano
puntatori. Ora le chiavi fino a 30 byte stanno dentro i nodi dell'albero.

### Banco 1b: commit completi (soglia: almeno 15.000 record/s confermati, p99 sotto 5 ms)

Client in ciclo chiuso, modalità validata, traffico tra repliche attraverso il proxy, tre ripetizioni per punto su un
gruppo appena creato. Record con una mutazione da 200 byte:
- 16 client: 10.472–11.246 record/s, p99 3,1–3,2 ms (MISURATO);
- 32 client: 18.758–20.659 record/s, p99 3,2–3,8 ms (MISURATO);
- 48 client: 26.315–27.007 record/s, p99 3,7–3,9 ms (MISURATO);
- 64 client: 28.083–31.396 record/s, p99 4,1–4,7 ms (MISURATO);
- 96 client: 21.663–38.373 record/s, p99 7,2–16,6 ms (MISURATO): oltre la capacità della macchina.

Superata da 32 client in su. Il percorso di un commit (diagnostica dentro il leader, 1 mutazione, 64 client):
- dall'aggiunta al fdatasync locale: mediana 0,8 ms (MISURATO);
- dall'aggiunta all'ack del primo follower: mediana 1,4 ms (MISURATO);
- conferma: mediana 1,4 ms (MISURATO).

Record da 5 mutazioni da 200 byte: vedi §2.

### Banco 1c: record da 5 mutazioni da 1 KB (dato, non soglia)

- 16 client: 6.689–7.011 record/s, cioè 34,3–35,9 MB/s di dati utili, p99 4,4–4,8 ms (MISURATO);
- 32 e 64 client: throughput più basso (19,5–33,1 MB/s) e p99 35–64 ms (MISURATO).

Il testo prevedeva che qui il limite fosse il disco. Su questa macchina **non** lo è: 3 scrittori in parallelo con
fdatasync ogni 256 KB arrivano a 466 MB/s (MISURATO). Il limite è il costo per record nelle tre repliche sulla stessa
CPU (STIMATO dai profili).

### Banco 2: carico misto (non superato)

Carico: 10.000 database piccoli a 1 scrittura/s, un database caldo a 20.000 scritture/s, 100.000 letture/s (metà su
chiavi appena scritte, quindi nella finestra), 1.000 "leggi la tua scrittura"/s. Scaricatore su MinIO attivo. Il
carico è in ciclo aperto e la latenza parte dall'istante pianificato.

- **(a) Solo i 10.000 database piccoli, 4 minuti, per la RAM:**
  - RSS 802–813 MB per replica contro 9–10 MB a vuoto, cioè 81–82 KB per database piccolo (MISURATO): **soglia della
    RAM superata**, sia per database (sotto 256 KB) sia per replica (sotto 3 GB);
  - commit 9.916/s, p50 2,4 ms, ma p99 30,6 ms (MISURATO): già a 10.000 scritture/s il ciclo della replica ha pause
    dovute al lavoro di scarico (vedi §5).
- **(b) Carico completo, 120 s:** crollo.
  - 2.580 commit/s confermati e 7.095 letture/s servite (MISURATO);
  - p99 dei commit e delle letture oltre 100 s (MISURATO): le code crescono senza limite;
  - CPU media in core (MISURATO): repliche 0,58, 0,53 e 0,55; generatore 1,16; MinIO 0,10; proxy 0,08;
    scaricatore 0,05. Totale circa 3 core su 4.
- **(c) Lo stesso carico ridotto, per vedere le latenze a macchina non satura:**
  - al 25% (7.500 scritture/s, 25.000 letture/s): commit 7.336/s confermati, p50 3,0 ms, p99 62,8 ms. Letture dalla
    finestra p50 1,4 ms, **p99 19,5 ms** (soglia 1 ms). "Leggi la tua scrittura" p50 0,4 ms, **p99 8,9 ms** (soglia
    3 ms). Tutto MISURATO;
  - al 50%: commit **14.662/s** (soglia 15.000), p99 337 ms; letture dalla finestra p99 273 ms (MISURATO).
- Prove esplorative (uscita non salvata nei risultati, MISURATO): sole scritture a 30.000/s → 28.102/s confermate,
  p50 3,1 ms, p99 177 ms. Letture a 100.000/s offerte → ne vengono servite circa 70.000/s.

Anche al 25% del carico la p50 delle letture è 1,4 ms: una lettura dalla finestra costa pochi microsecondi di CPU, ma
attraversa cinque passaggi tra thread (client, thread di rete, thread principale della replica e ritorno) su CPU
contese. Le code di coda (p99) vengono dalle pause del thread principale.

### Banchi 3–7

Non eseguiti. Script pronti, ma mai eseguiti per intero: `scripts/banco3.sh` … `banco7.sh`.

Prove brevi di correttezza fatte durante lo sviluppo (non sono i banchi 4 e 5: niente misura dei 10 s, niente
porcupine):
- SIGKILL del leader, riavvio dopo 4–5 s, più una partizione del leader (anche dai client) di 6 s, con 2.000
  database: 252.000 commit confermati; su ciascuna delle tre repliche 0 mancanti, 0 in posizione diversa, 0 id
  duplicati. 46.311 commit ambigui rinviati con lo stesso id: tutti con la stessa posizione (MISURATO). Il leader
  isolato si è dimesso dopo 1 s (controllo del quorum).
- Replica follower isolata 15 s con letture lineari (posizione_confermata dal leader, poi get su una replica a caso,
  anche quella isolata): 195.995 letture, 0 più vecchie della posizione confermata, 0 letture servite dalla replica
  isolata durante l'isolamento (MISURATO).
- Dopo l'ultima modifica dell'interfaccia: SIGKILL del leader, 202.000 commit, 0 persi, 0 duplicati; 147.857 letture
  lineari, 0 vecchie (MISURATO).

Il controllo di linearizzabilità con porcupine (`proto/quaderno/lincheck`, Go, porcupine 1.3.1) è stato eseguito
solo sullo storico breve dell'ultima prova (43.510 scritture e 147.857 letture in 15 s, senza partizione): esito
"Ok" in 0,3 s (MISURATO). Lo storico di 10 minuti con partizione del banco 4 non esiste, perché il banco 4 non è
stato raggiunto.

## 4. Cosa è stato costruito

Un solo binario, `pq`, con questi comandi.

- **`pq replica`**: una replica. Le tre repliche sono tre processi sulla stessa macchina, ognuna con la sua cartella
  e il suo registro.
  - Raft scritto da zero: pre-voto, fedeltà al leader, controllo del quorum, replica a lotti in pipeline, conferma
    solo per voci del mandato corrente, barriera del nuovo leader.
  - Conferma quando 2 repliche su 3 hanno fatto fdatasync.
  - Posizione = orologio ibrido 48+16 bit, assegnato dal leader.
  - Validazione sul leader: modalità validata, cieca, e dipendenze `esiste`/`valore`.
  - Finestra di 60 s indicizzata per chiave su ogni replica.
  - Letture: `get`/`scan` alla posizione S; `posizione_confermata` lineare (ReadIndex); `attendi`.
  - Scarico: `scaricabili`/`conferma_scarico` (la conferma passa dal registro).
  - Potatura della finestra dopo lo scarico + 60 s; tetto di RAM con sfratto dei valori su disco; segmenti del
    registro preallocati, cancellati quando sono su S3 e su tutte e tre le repliche.
- **`pq proxy`**: proxy TCP che può ritardare, scartare in silenzio (partizione), chiudere, scartare un verso solo o
  perdere pezzi a caso. Tutto il traffico tra repliche passa da qui; nei banchi 4–5 anche quello dei client.
- **`pq flusher`**: lo scaricatore. Porta i record confermati su MinIO in SST, aggiorna un manifest per gruppo con PUT
  condizionale, compatta, conferma lo scarico al leader.
- **Generatori e controlli:** `pq commit | mixed | hot | lin | failover` sono i generatori dei banchi; `pq valbench`
  è il banco 1a; `pq check-lin` controlla le letture vecchie; `lincheck/` è porcupine.
- **`scripts/cluster.py`**: avvia e ferma il gruppo; uccide, sospende e isola le repliche; gestisce MinIO e lo
  scaricatore.

Ripreso dagli altri branch (in sola lettura):
- da journal3z: il formato dei frame (lunghezza + CRC32) e il disegno del proxy;
- da walbench: i file del registro preallocati.

Il resto di journal3z non era riusabile: è costruito attorno a uno scrittore per database con lease su S3, mentre qui
c'è un leader unico per il gruppo.

Difetti trovati e corretti durante il lavoro:
- una voce di servizio confermata perdeva il corpo prima di essere scritta su disco, e la replica moriva al riavvio;
- il ciclo della replica smaltiva fino a 50.000 eventi prima di passare le voci al disco e alla rete (latenza alta);
- il generatore di carico aveva lock globali che lo serializzavano a 130.000 operazioni/s.

Come rifare tutto (da `proto/quaderno`; servono Rust, Go e MinIO in `$MINIO`, cartella di lavoro `$PQ_WORK`):

    cargo build --release && cargo test --release
    scripts/banco1.sh results/banco1          # ~20 min
    scripts/banco2.sh results/banco2 120      # ~12 min
    scripts/banco3.sh … scripts/banco7.sh     # pronti, non eseguiti

## 5. Perché il banco 2 non passa, e cosa servirebbe

Misurato:
- ogni replica usa circa mezzo core in tutto, eppure il crollo arriva prima;
- la diagnostica del ciclo del leader mostra giri isolati fino a 60–90 ms;
- senza letture e a 10.000 scritture/s la p99 dei commit è già 30 ms.

Stimato:
- **Il thread principale della replica fa troppo.** Oltre a commit e replica fa:
  - le letture;
  - `pendenti`, che scorre 10.000 database;
  - `scaricabili`, risposte fino a 4 MB;
  - l'applicazione di voci `conferma_scarico` con migliaia di database;
  - la potatura.

  Ognuna di queste blocca le letture in coda. Servirebbe:
  - servire le letture fuori dal thread principale, con un indice leggibile in concorrenza (per esempio per
    database);
  - rendere incrementale il lavoro di scarico.
- **La macchina.** Tre repliche, proxy, MinIO, scaricatore e un generatore da 130.000 operazioni/s su 4 vCPU. Il
  generatore da solo usa 1,16 core.

  Un banco onesto del banco 2 vuole il generatore su un'altra macchina, e idealmente una macchina per replica. Ma la
  consegna chiedeva tutto in locale, e qui c'è una macchina sola.

Sul disegno non vedo un problema dimostrato. Una lettura dalla finestra costa pochi microsecondi di CPU: con
33.000 letture/s per replica sono 0,1–0,2 core (STIMATO dal banco 1a). Il risultato però è da provare, non da
assumere.

## 6. Aggiunte proposte da proto-lettura

Lette in `proto/INTERFACCIA.md` del branch proto-lettura, sezione "Differenze di proto-lettura".

1. **`fuori_finestra(potato_fino_a, etag)`: accettata e implementata.**
2. **Esito `delta(pos, somma, potato_fino_a)`: accettata con una correzione, implementata.**
   - Il problema: un delta resta nella finestra fino a 60 s anche dopo essere finito su S3. Se il manifest del
     lettore ha `scaricato` oltre `potato_fino_a`, la somma "di tutti i delta ≤ S" conta due volte quelli già negli
     SST.
   - La correzione: `get` e `scan` hanno il parametro facoltativo `dal` = `scaricato` del manifest che il lettore
     userà. La replica somma solo i delta in `(dal, S]`; se `dal < potato_fino_a` risponde `fuori_finestra` (il
     manifest va riletto).
   - In pratica `sst_get` di proto-lettura legge gli SST alla posizione S con un manifest qualsiasi che abbia
     `scaricato ≥ potato_fino_a`. Senza `dal` il doppio conteggio è possibile.
   - Test unitario: `window::tests::delta_con_base_negli_sst`.
3. **`stato_scarico(db...) -> (scaricato, etag, potato_fino_a)`: accettata e implementata** (più database per
   richiesta).
4. **`pendenti` con record, byte e `scaricato`: accettata e implementata.**
5. **Manifest per gruppo: già compatibile.** Lo scaricatore del prototipo fa già così.

Sulle tre regole di `docs/protocollo.md` che proto-lettura propone di rivedere sono d'accordo. Lo scaricatore del
prototipo:
- scarica per età (10 s) e in gruppi, non ogni 5 s per database;
- se il PUT condizionale del manifest fallisce, non cancella l'SST (resta un orfano), così non rischia di cancellarne
  uno già entrato nel manifest.

## 7. Differenze rispetto a docs/protocollo.md (branch tla-registro)

Il documento è comparso durante il lavoro (ultima versione letta: commit 91c783a).

Allineato nel prototipo:
- dipendenze dichiarate `(chiave, esiste|valore)` con due controlli (ultima scrittura strutturale / ultima
  qualsiasi). La modalità "delta + dipendenza esiste" per `$inc` su `_id` è nel generatore del banco 3, non misurata;
- rifiuto di una S più nuova del registro del leader (`S_SCONOSCIUTA`): risponde "non sono il leader", e il client
  cerca il leader e rinvia;
- già così fin dall'inizio: conferma solo per voci del mandato corrente, barriera, controllo del prefisso, ReadIndex,
  regola "una replica serve solo fino alla propria posizione confermata", pre-voto;
- nomi degli SST con 128 bit casuali.

Diverso:
- **posizioni.** Il documento usa l'indice del log; l'interfaccia comune un orologio ibrido convertibile in Timestamp
  BSON. Il prototipo tiene entrambi: indice per Raft, posizione ibrida crescente con l'indice. L'ordine è lo stesso.
  Da decidere quale scrivere nel documento;
- **`pronto`.** Il documento rifiuta i commit finché la barriera non è confermata. Il prototipo tiene le mappe
  aggiornate su ogni replica a ogni voce (servono alle letture) e accetta subito: i commit si confermano comunque
  dopo la barriera;
- **tabella degli id.** Il prototipo memorizza la posizione, non la risposta già calcolata;
- **letture.** Il documento fa restituire i record grezzi in `(M, S]`; l'interfaccia comune restituisce il valore
  composto a S, con `delta`/`fuori_finestra`. Il parametro `dal` (§6) avvicina le due versioni;
- **scarico.** Il documento ha un manifest per database e `SCARICATO` mandato alle repliche. Il prototipo raggruppa
  i database piccoli e fa passare `conferma_scarico` dal registro (tutte le repliche lo applicano nello stesso punto);
- **tempi di elezione.** Il prototipo usa 1–2 s invece di 150–300 ms: su 4 vCPU satura, 300 ms darebbero elezioni
  spurie;
- **contropressione.** Il documento rifiuta i commit oltre metà disco; il prototipo ha solo il tetto di RAM con
  sfratto su disco;
- **coda per chiave nel nodo d'ingresso (§4.4):** non implementata;
- **§4.6, chiave di sessione per i ritentativi oltre la finestra:** compatibile (è una scrittura normale), non usata
  dai generatori.

## 8. Semplificazioni rispetto a un sistema vero

- Una macchina sola: le tre "zone" condividono 4 vCPU e un disco. Le latenze contengono la contesa di CPU e non la
  latenza vera tra zone.
- Valori opachi; il `delta` è un i64 da sommare.
- Una richiesta di commit tocca un solo database. Niente indici unici né metadati di collezione.
- Il nodo d'ingresso è il generatore stesso, non un server che parla il protocollo di MongoDB.
- Un solo scaricatore, con lo stato compattato nella propria RAM. SST senza indice e mai letti (sono il lavoro di
  proto-lettura).
- Membri fissi: niente sostituzione di una replica che perde il disco.
- Deduplica degli id solo finché il record è nella finestra.
- Il tetto di RAM limita i valori in RAM, non l'indice per chiave. Con lo scarico fermo a lungo l'indice cresce
  comunque (banco 6, non eseguito).
- Nessuna autenticazione né cifratura.

## 9. Cosa cambierà con 3 macchine in 3 zone

- **Latenza dei commit.** Il viaggio tra zone AWS, 0,5–2 ms andata e ritorno (STIMATO), si aggiunge al fdatasync del
  follower (0,8 ms di mediana qui, MISURATO). Mi aspetto il commit a 2–3 ms di mediana e sotto 5 ms di p99 con dischi
  NVMe locali o gp3 (STIMATO).
- **CPU.** Ogni replica sulla sua macchina. Dal banco 1a, un core del leader regge circa 47.000 record/s da 5 chiavi
  (STIMATO), più delle soglie. Le letture vanno comunque spostate fuori dal thread principale (§5), altrimenti le
  latenze di coda restano legate al lavoro del leader.
- **Elezioni.** Tempi di 150–300 ms diventano sicuri. Il ritorno delle scritture dopo la morte del leader scende sotto
  1 s più la rilevazione nei client (STIMATO).
- **Rete vera.** Ci saranno perdite parziali, ritardi asimmetrici e riordini, non partizioni pulite. Il protocollo li
  tollera per costruzione, ma i tempi di rilevazione cambiano.
- **Dischi.** Tre dischi indipendenti invece di uno condiviso.
- **S3 vero.** PUT da 20–100 ms invece di pochi ms su MinIO (STIMATO). Lo scaricatore deve lavorare in parallelo su
  più gruppi.
