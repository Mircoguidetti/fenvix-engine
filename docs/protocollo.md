# Protocollo del registro "senza cuoco"

Stato: prosa precisa del protocollo nuovo, base per il prototipo `proto-quaderno`. Le scelte sulle
scritture contese e sulle letture fresche sono motivate in `docs/tla-registro-decisioni.md`; ciò che è
stato verificato con TLC e ciò che non lo è sta in `docs/tla-registro.md`. Ogni numero è STIMATO salvo
indicazione contraria.

Convenzione: "deve" è una regola di sicurezza (violarla può perdere o inventare dati); "dovrebbe" è
una regola di comportamento o di prestazione.

## 1. Parti in gioco

- **Registro**: un unico log ordinato, condiviso da tutti i database del cluster. Ogni voce ha una
  **posizione** (intero crescente, assegnato dal leader), un'**epoca** (il mandato del leader che l'ha
  creata) e un **record**.
- **Replica** (3, una per zona): tiene il registro su disco locale (ogni voce confermata al leader solo
  dopo `fdatasync`) e in RAM gli ultimi 60 s indicizzati per database, collezione e chiave. Una delle
  tre è il **leader**.
- **Leader**: l'unica replica che accetta commit. Assegna le posizioni, valida, replica, conferma,
  tiene in RAM le mappe di validazione e la tabella degli id.
- **Nodo d'ingresso** (quanti si vuole, senza stato durevole): parla il protocollo di MongoDB con i
  client, compone le letture, prepara le scritture, gestisce i tentativi.
- **Worker** (quanti si vuole, senza stato durevole): scarica i record confermati su S3 in blocchi
  immutabili (SST) e compatta.
- **S3**: per ogni database un **manifest** (lista di SST, posizione `M` fino alla quale il registro è
  incluso negli SST, ETag) e gli SST. Il manifest si scrive solo con PUT condizionale sull'ETag. Gli
  SST hanno nomi unici mai riusati.

## 2. Posizioni e record

Posizioni: interi da 1 in su. Il leader assegna la posizione `Len(log) + 1` a ogni voce che aggiunge
al proprio log. Una posizione **confermata** è una posizione tale che il leader ha contato la voce su
2 repliche di 3 dopo `fdatasync`, e tutte le voci precedenti sono confermate. Una posizione non
confermata può essere riassegnata a un'altra voce da un leader successivo (regola di Raft). Una
posizione confermata non cambia mai più. I client vedono solo posizioni confermate.

Un **record** di scrittura contiene:

- `id`: identità della scrittura data dal client, `(sessione, numero di transazione)` di MongoDB, o un
  id generato dal nodo d'ingresso per i comandi senza sessione;
- `db`: il database;
- `S`: la posizione dell'istantanea su cui il nodo d'ingresso ha preparato la scrittura;
- `dipendenze`: insieme di coppie `(chiave, modo)` con modo `esiste` o `valore`;
- `scritture`: insieme di `(chiave, tipo, payload)` con tipo `strutturale` (inserimento, cancellazione,
  sostituzione; il payload è il documento intero o la lapide) oppure `delta` (operatori di modifica
  da applicare al valore corrente);
- `risposta`: la risposta già calcolata per il client (contatori, documento restituito da
  `findAndModify`), così che un tentativo ripetuto possa restituire lo stesso esito.

Le "chiavi" sono chiavi del deposito: `(db, collezione, _id)` per i documenti,
`(db, collezione, indice, valore)` per le voci degli indici unici, `(db, collezione)` per i metadati di
collezione.

Record speciali: `barriera` (voce vuota che il nuovo leader aggiunge all'inizio del proprio mandato),
`configurazione` (cambio dell'insieme delle repliche, non trattato in questa fase).

## 3. Stato

### 3.1 Replica (durevole, salvo indicazione)

- `epoca`: il mandato più alto visto. Non decresce.
- `votato`: la replica a cui ha dato il voto in `epoca`, o nessuna.
- `log`: le voci, contigue da `inizioDisco + 1` all'ultima.
- `confermataLocale` (volatile, ricostruibile): l'ultima posizione confermata che il leader le ha
  comunicato dentro una replicazione che ha superato il controllo del prefisso (§5.3).
- `finestra` (volatile): indice per chiave delle voci degli ultimi 60 s; `inizioFinestra` è la
  posizione più bassa indicizzata.
- `M[db]` (volatile): l'ultima posizione scaricata su S3 che le è stata comunicata per ciascun database;
  `inizioDisco` non supera mai `min M[db]`.
- `ruolo`: `seguace`, `candidato` o `leader`.

### 3.2 Leader (in più, volatile)

- `combacia[r]`: per ogni replica, la posizione più alta che si sa combaciare col proprio log.
- `confermata`: la posizione confermata corrente.
- `ultimaStrutturale[chiave]`, `ultimaQualsiasi[chiave]`: le due mappe di validazione (§4.2), per le
  chiavi toccate dalle voci del proprio log con posizione maggiore o uguale a `inizioValidazione`.
- `inizioValidazione`: la posizione più bassa coperta dalle mappe. Un commit con `S` minore viene
  rifiutato. Avanza col tempo (60 s), ma la regola è per posizione: la sicurezza non dipende dall'orologio.
- `tabellaId`: `id -> (posizione, risposta)` per le voci con posizione maggiore o uguale a
  `inizioValidazione`.
- `pronto`: vero solo dopo che la barriera del proprio mandato è confermata e le mappe sono ricostruite.

### 3.3 Nodo d'ingresso (volatile)

- `S_ingresso`: la posizione confermata più alta di cui ha notizia.
- `ultimoContattoLeader`: l'istante dell'ultima risposta del leader.
- `leader`: indirizzo ed epoca del leader noto.
- per database: la vista del manifest (`M`, lista SST, ETag) e una cache dei blocchi SST.
- per chiave: la coda delle scritture validate in volo (§4.4).

### 3.4 Client (nel driver)

- `gettone`: la posizione più alta vista dalla sessione, trasportata in `$clusterTime` /
  `afterClusterTime`, che i driver di MongoDB già propagano.

## 4. Scritture

### 4.1 Preparazione nel nodo d'ingresso

1. Scegli `S` come per una lettura (§6.1).
2. Leggi i documenti necessari a `S` (§6.2) e calcola `dipendenze`, `scritture`, `risposta` secondo
   la classificazione di `docs/tla-registro-decisioni.md` sezione A. In breve: filtro solo su `_id` e
   soli operatori di modifica, documento esistente a `S`: scritture `delta`, dipendenza `(_id, esiste)`;
   inserimento, cancellazione, sostituzione, upsert che inserisce: scritture `strutturale`, dipendenza
   `(_id, esiste)`; `findAndModify`, filtro su altri campi, indici unici, transazioni, validatori di
   schema: dipendenza `(chiave, valore)` sulle chiavi di cui si è letto il contenuto. Ogni chiave
   d'indice unico inserita o rimossa è una scrittura `strutturale` con dipendenza `(chiave, valore)`.
3. Se esiste già una scrittura validata in volo sulla stessa chiave da questo nodo d'ingresso, aspetta
   il suo esito e ricomincia dal passo 1 (§4.4).
4. Manda `COMMIT(id, db, S, dipendenze, scritture, risposta)` al leader noto.

### 4.2 Validazione nel leader

Alla ricezione di `COMMIT`, nell'ordine:

1. Se `pronto` è falso: rispondi `RIPROVA(epoca, leader)`.
2. Se `id` è in `tabellaId`: rispondi `CONFERMATO(posizione, risposta)` memorizzate (se la posizione
   non è ancora confermata, rispondi quando lo sarà). Nessuna nuova voce.
3. Se `S < inizioValidazione`: rispondi `RIFIUTATO(S_TROPPO_VECCHIA, confermata)`.
4. Se `S > ultima posizione del proprio log`: rispondi `RIFIUTATO(S_SCONOSCIUTA, confermata)` (il nodo
   d'ingresso ha letto da un leader più nuovo: questo leader è deposto senza saperlo).
5. Per ogni dipendenza `(k, esiste)`: se `ultimaStrutturale[k] > S` rifiuta. Per ogni dipendenza
   `(k, valore)`: se `ultimaQualsiasi[k] > S` rifiuta. In caso di rifiuto rispondi
   `RIFIUTATO(CONFLITTO, chiavi in conflitto, confermata)`.
6. Altrimenti assegna la posizione `C = Len(log) + 1`, aggiungi la voce `(C, epoca, record)` al proprio
   log, aggiorna `ultimaQualsiasi[k] := C` per ogni chiave scritta e `ultimaStrutturale[k] := C` per
   ogni chiave scritta con tipo `strutturale`, aggiungi `id -> (C, risposta)` a `tabellaId`.
7. Replica (§5.2). Quando `C` è confermata rispondi `CONFERMATO(C, risposta, confermata)`.

La validazione usa tutte le voci del proprio log, anche quelle non ancora confermate: due commit
concorrenti in conflitto nello stesso lotto devono vedersi. Una voce non confermata del leader può
sparire solo se il leader viene deposto; in quel caso spariscono anche tutte le voci successive, e
nessun esito è stato dato per nessuna di esse.

Il passo 6 e l'`fdatasync` si fanno a lotti: il leader accumula le voci arrivate in una finestra breve
(dell'ordine del millisecondo, STIMATO), le scrive con un solo `fdatasync` e le manda in un solo
`APPEND`.

### 4.3 Esito nel nodo d'ingresso

- `CONFERMATO(C, risposta)`: rispondi al client con `risposta` e gettone `C`; aggiorna `S_ingresso`.
- `RIFIUTATO(CONFLITTO)` o `RIFIUTATO(S_TROPPO_VECCHIA)`: ricomincia da §4.1 con una `S` nuova, con
  lo stesso `id`. Il numero di tentativi è limitato (proposta: 20, STIMATO); oltre, rispondi al client
  con un errore ritentabile (`WriteConflict`-like, etichetta `RetryableWriteError`).
- `RIFIUTATO(S_SCONOSCIUTA)`, `RIPROVA`, `NON_LEADER(leader, epoca)`, nessuna risposta entro il
  tempo massimo: aggiorna il leader noto (dalla risposta, o chiedendo a una replica qualsiasi
  `CHI_E_IL_LEADER`) e rimanda lo stesso `COMMIT` con lo stesso `id`. La tabella degli id del nuovo
  leader rende il tentativo innocuo (§5.5).

### 4.4 Coda per chiave

Per una scrittura con almeno una dipendenza `valore`, il nodo d'ingresso tiene per chiave la scrittura
in volo. Una seconda scrittura validata sulla stessa chiave aspetta la prima e si prepara dopo. Le
scritture con sole dipendenze `esiste` non aspettano: non conflittano tra loro.

### 4.5 Transazioni multi-documento

Il nodo d'ingresso tiene lo stato della transazione (istantanea `S`, letture, scritture accumulate)
finché il client manda `commitTransaction`; allora manda un solo `COMMIT` con tutte le scritture e con
dipendenza `(k, valore)` per ogni chiave scritta. Letture non validate (isolamento a istantanea, come
MongoDB). Un `abortTransaction` scarta tutto senza toccare il registro. Se il nodo d'ingresso muore a
metà, la transazione è persa e il client riceve `TransientTransactionError` alla prossima operazione.

### 4.6 Sessioni e scritture ritentabili (passo 5)

La tabella degli id del leader dura quanto la finestra (60 s). Un tentativo ripetuto più tardi, senza
aver mai ricevuto risposta, verrebbe eseguito due volte (il controllo negativo `RetryBeyondWindow` di
`tla-registro/ValidatingLog.tla` lo mostra). Per chiudere il buco senza stato in più nel leader:

1. ogni scrittura ritentabile scrive anche la **chiave di sessione** `(db, sessione, lsid)` con valore
   `(numero di transazione, risposta)`: è una chiave normale, scrittura `strutturale`, che finisce nella
   finestra delle repliche e poi negli SST come tutte le altre;
2. prima di inviare un tentativo, il nodo d'ingresso legge la chiave di sessione a `S` (nella stessa
   lettura con cui legge il documento). Se contiene già quel numero di transazione, risponde al client
   con la `risposta` memorizzata e non invia nulla;
3. il leader continua a deduplicare per id dentro la finestra e a rifiutare `S < inizioValidazione`.

Perché basta: sia `p` la posizione del primo tentativo confermato. Un secondo tentativo preparato a `S`
con `S >= p` vede la chiave di sessione e non invia. Con `S < p`: se `p` è ancora nella finestra il leader
lo deduplica; se `p` è uscito dalla finestra allora `S < p < inizioValidazione` e il leader rifiuta
"S troppo vecchia"; il nodo d'ingresso rilegge a una `S` più recente, che è almeno
`inizioValidazione > p`, e vede la chiave di sessione. Nessun caso porta a una seconda voce. Verificato
in `ValidatingLog.tla` con `SessionLookup = TRUE` e `RetryBeyondWindow = TRUE` (modello
`ValidatingLog-sessioni`): `NoDuplicateIds` e `OneOutcome` valgono, cioè un commit ritentato restituisce
il primo esito anche se il leader che lo aveva confermato è morto. La chiave di sessione va potata (per
esempio dopo 30 minuti senza uso, come `localLogicalSessionTimeoutMinutes` di MongoDB): fuori da questa
fase.

## 5. Replicazione, conferma, cambio di leader

### 5.1 Epoche e voto

Il leader è scelto tra le repliche come in Raft. Una replica senza notizie del leader per il tempo di
elezione (proposta: 150-300 ms casuale, STIMATO) diventa candidata: `epoca := epoca + 1`,
`votato := sé`, salva, manda `CHIEDI_VOTO(epoca, ultimaPosizione, ultimaEpoca)` alle altre.

Una replica concede il voto (`VOTO(epoca, sì)`) se e solo se: l'`epoca` richiesta è maggiore della
propria, oppure uguale e non ha ancora votato in quest'epoca; e il log del candidato è almeno
aggiornato quanto il proprio (`ultimaEpoca` del candidato maggiore della propria, oppure uguale e
`ultimaPosizione` maggiore o uguale). Prima di rispondere salva `epoca` e `votato` su disco. In ogni
caso adotta l'`epoca` ricevuta se maggiore della propria e torna seguace.

Con 2 voti su 3 (il proprio compreso) il candidato è leader dell'epoca.

### 5.2 Replicazione

Il leader manda a ogni seguace `APPEND(epoca, posPrecedente, epocaPrecedente, voci, confermata)`.
`voci` sono le voci del proprio log dopo `posPrecedente`, in ordine, al più un lotto. Un `APPEND` senza
voci è un heartbeat (ogni 50 ms, STIMATO).

### 5.3 Ricezione di APPEND in una replica

1. Se `epoca < propria epoca`: rispondi `RIFIUTO(propria epoca)`. Il mittente, se è leader, torna
   seguace.
2. Adotta `epoca` se maggiore; torna seguace; azzera il tempo di elezione.
3. Controllo del prefisso: se il proprio log non ha una voce a `posPrecedente` con epoca
   `epocaPrecedente` (con `posPrecedente = 0` il controllo passa sempre; se `posPrecedente` è sotto
   `inizioDisco` il controllo passa, perché tutto sotto `inizioDisco` è confermato e in S3),
   rispondi `RIFIUTO(epoca, ultimaPosizione)` e fermati.
4. Se una voce ricevuta è in conflitto con una propria (stessa posizione, epoca diversa), tronca il
   proprio log da quella posizione in avanti (tutte le voci troncate sono non confermate).
5. Aggiungi le voci nuove, `fdatasync`.
6. `confermataLocale := min(confermata, ultima posizione propria)`. Questo passo è dopo il controllo
   del prefisso: per costruzione le voci fino a `confermataLocale` coincidono con quelle del leader.
7. Aggiorna la finestra in RAM con le voci nuove. Rispondi `ACK(epoca, ultimaPosizione)`.

### 5.4 Conferma nel leader

Alla ricezione di `ACK(epoca, p)` da `r`: `combacia[r] := max(combacia[r], p)`. Sia `c` la posizione
più alta tale che almeno 2 repliche su 3 (il leader compreso, che combacia con sé stesso dopo il proprio
`fdatasync`) hanno `combacia >= c` **e** la voce a `c` ha l'epoca del leader corrente. Se `c > confermata`:
`confermata := c`, rispondi `CONFERMATO` ai `COMMIT` delle posizioni fino a `c`, e porta `confermata`
nei prossimi `APPEND`.

Alla ricezione di `RIFIUTO(epoca, ultimaPosizione)` con epoca uguale: `posPrecedente` per quella
replica scende a `min(posPrecedente - 1, ultimaPosizione)` e si rimanda. Con epoca maggiore: torna
seguace.

La condizione "la voce a `c` ha l'epoca del leader corrente" è la regola di Raft che impedisce di
contare come confermata una voce di un mandato precedente prima che una voce del mandato corrente la
copra. È per questo che il nuovo leader aggiunge subito una barriera.

### 5.5 Nuovo leader: barriera e ricostruzione

Appena eletto, il leader:

1. imposta `pronto := falso`, `combacia[r] := 0` per gli altri;
2. aggiunge al proprio log la voce `barriera` con la propria epoca e la replica;
3. quando la barriera è confermata, tutte le voci del proprio log sono confermate;
4. ricostruisce `ultimaStrutturale`, `ultimaQualsiasi` e `tabellaId` leggendo le voci del proprio log
   da `inizioValidazione` (la posizione di 60 s fa, o `inizioFinestra` se più alta) all'ultima;
5. `pronto := vero`.

Fino a `pronto` i `COMMIT` ricevono `RIPROVA`. Si deve ricostruire da tutte le voci del log, confermate
e non: tutte quelle presenti nel log del nuovo leader saranno confermate in quest'ordine. Un `COMMIT`
ripetuto il cui `id` è nel log ricostruito riceve l'esito memorizzato: la posizione e la risposta
sono quelle del primo tentativo, anche se il leader di allora è morto. Un `id` assente dal log del nuovo
leader non ha mai avuto esito (il vecchio leader non ha mai risposto `CONFERMATO` per esso, perché una
voce confermata è su 2 repliche di 3 e quindi nel log di ogni nuovo leader): il tentativo viene
trattato come nuovo.

### 5.6 Avanzamento della finestra

Ogni 60 s circa il leader alza `inizioValidazione` alla posizione della prima voce più recente di
60 s e scarta dalle mappe e dalla tabella le chiavi e gli id la cui posizione è più bassa. È sicuro
perché i `COMMIT` con `S < inizioValidazione` vengono rifiutati: un conflitto con una voce più vecchia
di `inizioValidazione` è impossibile con `S >= inizioValidazione`. Le repliche fanno lo stesso con
`inizioFinestra`.

## 6. Letture

### 6.1 Scelta di `S` nel nodo d'ingresso

- Default (`readConcern` `local`, `majority`, nessuno): `S := max(gettone del client, S_ingresso)`.
  Se `ora - ultimoContattoLeader > T_max` (proposta 1 s, STIMATO), prima chiedi `POSIZIONE` al leader
  e aggiorna `S_ingresso`.
- `readConcern: linearizable`: `S := max(gettone del client, risposta a POSIZIONE)`.
- Il leader risponde a `POSIZIONE` con `confermata` solo dopo aver ricevuto, dopo la richiesta, un
  `ACK` di heartbeat da almeno un'altra replica nella propria epoca (ReadIndex). Un leader deposto non
  lo ottiene e non risponde; il nodo d'ingresso va a cercare il leader nuovo. Le richieste pendenti
  di un nodo d'ingresso si servono con un solo heartbeat.

### 6.2 Composizione

1. Prendi la vista del manifest del database (`M`, SST). Se è più vecchia di qualche secondo
   (STIMATO: 5 s), rileggila da S3.
2. Manda a una replica (la più vicina) `LEGGI(db, chiavi o collezione, M, S)`.
3. La replica deve rispondere con i record solo se `S <= confermataLocale` e `M >= inizioFinestra`
   (o `M >= inizioDisco`, servendo dal disco). Altrimenti risponde `NON_HO_S(confermataLocale)` oppure
   `SCARTATO_SOTTO_M(inizioFinestra, inizioDisco)`. In ogni risposta la replica mette `confermataLocale`.
4. Su `NON_HO_S`: prova le altre repliche; se nessuna ha `S`, aspetta e riprova fino a un tempo massimo
   (proposta 2 s, STIMATO), poi errore ritentabile. Su `SCARTATO_SOTTO_M`: rileggi il manifest da S3
   e ricomincia.
5. Leggi i valori a `M` dagli SST (cache o S3) e applica i record ricevuti in ordine di posizione:
   una scrittura `strutturale` sostituisce il valore (o lo cancella), un `delta` si applica al valore
   corrente con la semantica degli operatori di MongoDB. Le voci di indice si applicano allo stesso modo
   per le query su indice.
6. Rispondi al client con gettone `S`. Aggiorna `S_ingresso` con la `confermataLocale` ricevuta.

La regola del passo 3 è ciò che rende innocua una replica isolata ma viva: la sua `confermataLocale`
è ferma al momento dell'isolamento, e per quelle posizioni le voci non cambiano più. Servire in base
alla lunghezza della coda sarebbe sbagliato: una replica può avere a una posizione non confermata una
voce che un leader più nuovo ha sostituito.

### 6.3 Letture che non sono per chiave

Una query con filtro su un campo indicizzato usa l'indice negli SST a `M` e chiede alla replica tutte
le voci della collezione in `(M, S]` (al più 60 s di scritture di quella collezione), unendole. Un
`count`, un `distinct`, una `aggregate` fanno lo stesso. Il costo è lineare nelle scritture recenti
della collezione: va misurato.

## 7. Scarico su S3 e compattazione

### 7.1 Scarico

Un worker, per un database `d`, ogni pochi secondi (proposta 5 s, STIMATO):

1. legge il manifest di `d`: `M`, lista SST, ETag;
2. chiede a una replica `LEGGI_INTERVALLO(d, M, fino)`, con `fino` la `confermataLocale` della
   replica; la replica risponde solo con voci confermate (`fino <= confermataLocale`) e solo se
   `M >= inizioDisco`;
3. costruisce l'SST con le voci `(M, fino]` di `d` e lo scrive su S3 con un nome unico mai usato
   (identificatore casuale di 128 bit nel nome). Un nome derivato dall'intervallo (`M`-`fino`) basterebbe
   per l'unicità del contenuto, ma un nome derivato dalla sola `M` è sbagliato: due worker che partono
   dalla stessa `M` e arrivano a `fino` diversi si sovrascriverebbero l'oggetto;
4. scrive il manifest con `M := fino` e la lista SST più il nuovo, con PUT condizionale `If-Match: ETag`;
5. se il PUT condizionale fallisce, un altro worker ha vinto: il proprio SST è un orfano da cancellare
   (subito, o con una raccolta periodica degli SST non elencati in nessun manifest da più di un'ora);
6. se riesce, dice alle repliche `SCARICATO(d, fino)`; le repliche aggiornano `M[d]` e possono alzare
   `inizioDisco` fino a `min M[d]` su tutti i database (con un margine).

Due worker sullo stesso database non perdono né duplicano nulla: il manifest che vince descrive sempre un
prefisso esatto del registro, e ogni SST elencato contiene esattamente l'intervallo che il manifest gli
attribuisce.

### 7.2 Compattazione

Un worker legge il manifest, fonde alcuni SST in uno nuovo (nome unico), scrive il manifest con il nuovo
al posto dei vecchi e stesso `M`, con PUT condizionale. I vecchi SST si cancellano dopo un periodo di
grazia, perché un nodo d'ingresso con una vista vecchia del manifest può ancora leggerli.

### 7.3 Database inattivi

Un database senza scritture nuove non ha nulla da scaricare; il suo `M[d]` resta indietro senza fermare
`inizioDisco` se la regola è `inizioDisco <= min M[d]` sui database che hanno voci sopra `M[d]`. Il
worker visita comunque ogni database con voci non scaricate almeno ogni 5 s.

## 8. Guasti, uno per uno

- **Replica si ferma e riparte con il disco intatto.** Riparte seguace con `epoca`, `votato`, `log`
  dal disco; `confermataLocale` riparte da 0 e risale col primo `APPEND` che supera il controllo del
  prefisso; la finestra in RAM si ricostruisce dal disco. Fino ad allora rifiuta le letture
  (`NON_HO_S`). Nessuna perdita: le voci confermate sono su un'altra replica e il leader la riporta
  in pari con gli `APPEND`.
- **Replica perde il disco.** Non deve rientrare con la stessa identità (voterebbe due volte in
  un'epoca e potrebbe far eleggere un leader senza una voce confermata). Rientra come replica nuova
  tramite una voce `configurazione` (fuori da questa fase). Nel frattempo le altre due formano la
  maggioranza: ogni voce confermata è su almeno una di esse, e la regola del voto (§5.1) fa eleggere
  quella che la ha. Il sistema continua con 2 repliche, senza tolleranza a un secondo guasto.
- **Leader si ferma.** I seguaci eleggono un nuovo leader in uno o pochi tempi di elezione (STIMATO:
  150-600 ms). Il nuovo leader aggiunge la barriera, ricostruisce, accetta (§5.5). I `COMMIT` in volo
  senza risposta vengono rimandati dai nodi d'ingresso con lo stesso `id` e ricevono o l'esito
  memorizzato o un nuovo esito, mai due esiti.
- **Leader isolato con una minoranza (partizione).** Non ottiene più 2 `ACK` su 3: non conferma più
  nulla. La maggioranza elegge un nuovo leader con epoca maggiore. Il vecchio continua ad aggiungere
  voci al proprio log senza confermarle; al primo messaggio con epoca maggiore torna seguace e le voci
  non confermate vengono troncate dal controllo del prefisso. I nodi d'ingresso che parlavano col
  vecchio leader vedono tempi scaduti e `S_SCONOSCIUTA`, cercano il nuovo e rimandano con lo stesso `id`.
- **Due leader convinti nello stesso istante.** Possibile per qualche centinaio di millisecondi. Solo
  quello con l'epoca maggiore può confermare; l'altro non può ottenere 2 `ACK` su 3 perché i votanti
  del nuovo hanno già l'epoca nuova e rifiutano i suoi `APPEND`.
- **Replica isolata ma viva.** Non riceve `APPEND`: la sua `confermataLocale` si ferma. Serve letture
  solo fino a lì (§6.2). Diventerà candidata e alzerà la propria epoca: le sue richieste di voto non
  ottengono maggioranza (la maggioranza vede un leader). Al rientro, il suo `CHIEDI_VOTO` con epoca alta
  può deporre il leader corrente e causare un'elezione (il noto disturbo di Raft); si mitiga con la
  pre-votazione (chiedere "mi voteresti?" senza alzare l'epoca), consigliata ma non necessaria alla
  sicurezza.
- **Nodo d'ingresso si ferma.** I client riconnettono a un altro nodo d'ingresso. Le scritture
  ritentabili (`id` di sessione) vengono rimandate dal driver e trovano l'esito nella tabella degli id
  se dentro la finestra (60 s) o nella chiave di sessione (§4.6) altrimenti. Le transazioni aperte sono
  perse.
- **Messaggi duplicati, in ritardo, fuori ordine.** Ogni messaggio porta l'epoca ed è idempotente:
  `APPEND` per posizione (una voce già presente con la stessa epoca non cambia nulla), `ACK` per massimo,
  `VOTO` per epoca, `COMMIT` per `id`, `SCARICATO` per massimo, PUT del manifest per ETag.
- **`fdatasync` fallisce o il disco dà errori.** La replica si ferma e si considera "disco perso".
- **S3 non risponde.** Le scritture continuano finché il disco delle repliche ha spazio (il registro
  cresce sotto `inizioDisco` fermo). Le letture servono quanto hanno in cache più la finestra; i blocchi
  non in cache falliscono con errore ritentabile. Sopra una soglia di disco (STIMATO: metà) il leader
  rifiuta i `COMMIT` con un errore ritentabile (contropressione).
- **Orologi sbagliati.** Nessuna regola di sicurezza dipende dall'orologio: epoche, posizioni,
  `inizioValidazione` e `inizioFinestra` sono per posizione. L'orologio decide solo quando avanzare le
  finestre e quando scadono i tempi di elezione e di attesa.
- **Un worker muore a metà scarico.** Al più un SST orfano, raccolto dopo. Il manifest è intatto.
- **Due worker sullo stesso database.** Uno vince il PUT condizionale, l'altro cancella il proprio
  SST. Nessuna perdita né duplicazione (§7.1).

## 9. Che cosa non copre questo documento

Cambio dell'insieme delle repliche (voce `configurazione`), autenticazione tra parti, cifratura,
multi-regione, limiti di spazio per database, raccolta degli SST orfani, metadati delle collezioni e
degli indici (strada separata, serializzata per collezione), potatura delle chiavi di sessione, misure di
prestazione.
