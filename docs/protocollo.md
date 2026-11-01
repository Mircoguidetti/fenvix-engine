# Protocollo del registro "senza cuoco"

Stato: prosa precisa del protocollo nuovo, base per il prototipo `proto-quaderno`. Le scelte sulle
scritture contese e sulle letture fresche sono motivate in `docs/tla-registro-decisioni.md`; ciò che è
stato verificato con TLC e ciò che non lo è sta in `docs/tla-registro.md`. Ogni numero è STIMATO salvo
indicazione contraria.

Convenzione: "deve" è una regola di sicurezza (violarla può perdere o inventare dati); "dovrebbe" è
una regola di comportamento o di prestazione.

Giro 2 (1 novembre 2026): corretti i due errori trovati dal revisore della specifica (§5.3 punto 6,
§6.1), chiuso il buco delle scritture tra 60 s e lo scarico (§6.2: la replica serve dal disco),
aggiunti la ricarica da S3 (§5.7), indice e posizione separati (§2), "su 404 rileggi il manifest"
(§7.2), le voci degli indici non unici (§4.7), il conto della memoria con tetto, la cantina e il freno
come li fa il prototipo `proto-quaderno` al giro 3 (§7.4–7.8), il record di taglio dei rami (§10) e
più quaderni per cucina (§11). Le parti nuove verificate con TLC sono indicate; il resto è prosa.

## 1. Parti in gioco

- **Registro** (quaderno): un log ordinato, condiviso dai database del suo **gruppo**. Una cucina ha
  più registri: uno per gruppo di database piccoli e uno dedicato per ogni database grande (§11). Tutto
  ciò che segue vale per un registro; un database appartiene a un solo registro alla volta. Ogni voce
  ha un **indice** (contiguo, per la replicazione), una **posizione** (crescente, per letture e
  scatole, §2), un'**epoca** (il mandato del leader che l'ha creata) e un **record**.
- **Replica** (3, una per zona): tiene il registro su disco locale (ogni voce confermata al leader solo
  dopo `fdatasync`) da `inizioDisco` all'ultima voce, e in RAM una cache degli ultimi 60 s indicizzati
  per database, collezione e chiave, più un indice **chiave → posizioni** (senza valori) per tutto ciò
  che è sopra `M[db]` e non ancora su S3 (§6.2). Una delle tre è il **leader**.
- **Leader**: l'unica replica che accetta commit. Assegna le posizioni, valida, replica, conferma,
  tiene in RAM le mappe di validazione e la tabella degli id.
- **Nodo d'ingresso** (quanti si vuole, senza stato durevole): parla il protocollo di MongoDB con i
  client, compone le letture, prepara le scritture, gestisce i tentativi.
- **Worker** (quanti si vuole, senza stato durevole): scarica i record confermati su S3 in blocchi
  immutabili (SST) e compatta.
- **S3**: per ogni database un **manifest** (lista di SST, posizione `M` fino alla quale il registro è
  incluso negli SST, ETag) e gli SST. Il manifest si scrive solo con PUT condizionale sull'ETag. Gli
  SST hanno nomi unici mai riusati.

## 2. Indici, posizioni e record

Ogni voce ha due numeri, assegnati insieme dal leader:

- l'**indice**: intero contiguo (1, 2, 3, …). Lo usa solo la replicazione: controllo del prefisso
  (§5.3), `posPrecedente` e il suo abbassamento dopo un rifiuto (§5.4), conferma per maggioranza, voto
  (§5.1), ricarica (§5.7). Il modello formale lavora sugli indici (il leader assegna `Len(log) + 1`).
- la **posizione**: intero strettamente crescente con l'indice, ma non contiguo. Nel prototipo è un
  orologio logico ibrido (48 bit di millisecondi più 16 bit di contatore,
  `max(ultima + 1, adesso_ms << 16)`), trasportato come Timestamp BSON. La usano tutti gli altri: `S`,
  `C`, `M`, `inizioValidazione`, `inizioFinestra`, i gettoni dei client, i nomi e gli intervalli delle
  scatole.

Regola (deve): indice e posizione crescono insieme, cioè `indice(a) < indice(b)` se e solo se
`posizione(a) < posizione(b)`. Quindi ogni confronto fatto sulle posizioni dà lo stesso esito che sugli
indici, e le prove del modello valgono per le posizioni. L'aritmetica invece non si trasporta: "la voce
precedente" è `indice - 1`, mai `posizione - 1`, e un intervallo `(M, S]` di posizioni si traduce
negli indici delle voci con posizione in quell'intervallo. Ogni replica tiene la mappa dalla posizione
all'indice per le voci che ha (sono in ordine: basta una ricerca binaria sul disco) e, per l'ultima voce
cancellata dal disco, la coppia `(indice, posizione, epoca)`. Se l'orologio della macchina torna
indietro, la posizione cresce di 1 per voce (`ultima + 1`) e la regola resta vera.

Una voce **confermata** è una voce che il leader ha contato su 2 repliche di 3 dopo `fdatasync`
(§5.4), con tutte le precedenti. Un indice non confermato può essere riassegnato a un'altra voce da un
leader successivo (regola di Raft), con un'altra posizione. Una voce confermata non cambia mai più, né
nell'indice né nella posizione. I client vedono solo posizioni confermate. Una posizione `S` usata per
leggere è sempre la posizione di una voce confermata, o una posizione tra due voci confermate (vale come
la più alta delle due che non la supera).

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

Le voci degli indici non unici non sono nel record: si calcolano dai documenti (§4.7).

Record speciali: `barriera` (voce vuota che il nuovo leader aggiunge all'inizio del proprio mandato),
`taglio(db, ramo)` (il punto esatto in cui nasce un ramo del database, §10), `configurazione` (cambio
dell'insieme delle repliche, non trattato in questa fase).

## 3. Stato

### 3.1 Replica (durevole, salvo indicazione)

- `epoca`: il mandato più alto visto. Non decresce.
- `votato`: la replica a cui ha dato il voto in `epoca`, o nessuna.
- `log`: le voci, contigue per indice da `inizioDisco` all'ultima, più `(indice, posizione, epoca)`
  dell'ultima voce cancellata dal disco (serve al controllo del prefisso e al voto, come lo snapshot di
  Raft).
- `inizioDisco`: l'indice della prima voce ancora sul disco. Sale solo dopo uno scarico (§7.1 punto 6)
  o una ricarica da S3 (§5.7). Deve: `inizioDisco` non supera mai la prima voce non scaricata di
  nessun database, cioè ogni voce sotto `inizioDisco` è in S3 per il suo database; e non supera mai
  `inizioFinestra`.
- `confermataLocale` (volatile, ricostruibile): l'ultima posizione confermata che il leader le ha
  comunicato dentro una replicazione che ha superato il controllo del prefisso, limitata alla parte
  ricevuta in quella replicazione (§5.3 punto 6).
- `finestra` (volatile): cache in RAM delle voci degli ultimi 60 s, indicizzate per chiave;
  `inizioFinestra` è la posizione più bassa nella cache. Serve a rispondere più in fretta, non decide che
  cosa si può servire (§6.2).
- `indiceDisco` (volatile, ricostruibile dal disco): per ogni chiave con voci sopra `M[db]`, le posizioni
  di quelle voci e dove stanno sul disco, senza valori; e per collezione l'elenco delle chiavi toccate.
  Copre tutto ciò che è sopra `M[db]`, finestra e disco. Quando la RAM non basta, la sua parte vecchia
  esce in file ordinati sul disco locale (la cantina, §7.5).
- `M[db]` (volatile): l'ultima posizione scaricata su S3 che le è stata comunicata per ciascun database;
  `ultima[db]`: la posizione più alta di una voce del database sul disco (per il bollettino, §6.4).
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

0. Se il freno (§7.7) è attivo per il database o per il registro: rispondi subito `FRENO(attesa)`
   (errore ritentabile), prima di mettere il `COMMIT` in coda. Il rifiuto deve partire prima di ogni
   attesa: un rifiuto che arriva dopo il tempo massimo del client diventa un esito ignoto (difetto
   misurato nel prototipo al giro 3, banco 6).
1. Se `pronto` è falso: rispondi `RIPROVA(epoca, leader)`.
2. Se `id` è in `tabellaId`: rispondi `CONFERMATO(posizione, risposta)` memorizzate (se la posizione
   non è ancora confermata, rispondi quando lo sarà). Nessuna nuova voce.
3. Se `S < inizioValidazione`: rispondi `RIFIUTATO(S_TROPPO_VECCHIA, confermata)`.
4. Se `S > ultima posizione del proprio log`: rispondi `RIFIUTATO(S_SCONOSCIUTA, confermata)` (il nodo
   d'ingresso ha letto da un leader più nuovo: questo leader è deposto senza saperlo).
5. Per ogni dipendenza `(k, esiste)`: se `ultimaStrutturale[k] > S` rifiuta. Per ogni dipendenza
   `(k, valore)`: se `ultimaQualsiasi[k] > S` rifiuta. In caso di rifiuto rispondi
   `RIFIUTATO(CONFLITTO, chiavi in conflitto, confermata)`.
6. Altrimenti assegna l'indice `ultimo indice + 1` e la posizione `C` (maggiore dell'ultima, §2),
   aggiungi la voce `(indice, C, epoca, record)` al proprio log, aggiorna `ultimaQualsiasi[k] := C` per ogni chiave scritta e `ultimaStrutturale[k] := C` per
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

La lettura della chiave di sessione è una lettura normale (§6.2): scatole fino a `M` più le voci in
`(M, S]` dalla replica. Cade quasi sempre proprio tra 60 s e lo scarico, quindi funziona solo perché la
replica serve dal disco e non solo dalla finestra (§6.2 punto 3). Giro 2: verificato con la lettura vera
al posto dell'oracolo, insieme allo scarico, al disco che si svuota e a un cambio di leader (modello
`LettureScarico-sessioni`, `docs/tla-registro.md`).

### 4.7 Voci degli indici non unici

Domanda aperta del revisore: due `$set` concorrenti sullo stesso documento, su un campo con indice non
unico, sono due delta ciechi senza conflitto (dipendono solo da `(_id, esiste)`). Se ognuno dei due nodi
d'ingresso scrivesse nel record le voci d'indice calcolate dal valore letto a `S` ("togli la voce del
valore vecchio, aggiungi quella del nuovo"), alla fine resterebbero due voci d'indice per un documento
con un solo valore, e una query sull'indice restituirebbe il documento per un valore che non ha più.

Decisione (deve): **le voci degli indici non unici non stanno nel record. Si calcolano sempre dai
documenti**, in ordine di registro:

1. allo scarico, il worker applica le voci di `(M, fino]` al valore di ogni documento toccato a `M` (che
   ha già, perché deve comunque scrivere il valore nuovo o fondere i delta) e scrive nella scatola le voci
   d'indice del valore finale, togliendo quelle del valore a `M`;
2. alla lettura per indice (§6.3), il nodo d'ingresso usa l'indice delle scatole a `M` per i documenti
   non toccati in `(M, S]`, e per i documenti toccati in `(M, S]` (che riceve comunque, perché chiede
   tutte le voci della collezione in quell'intervallo) applica le voci al valore e controlla il filtro sul
   valore risultante. Un documento toccato è escluso dai candidati presi dall'indice a `M` e rivalutato.

Così i delta ciechi restano senza conflitto anche su campi indicizzati (un contatore ordinato in una
classifica non serializza), e l'indice nelle scatole è sempre quello del valore finale. Gli indici
**unici** restano come sono (§2): la loro voce è una chiave del deposito, scrittura `strutturale` con
dipendenza `valore`, perché il leader deve vedere i duplicati. Una scrittura che cambia un campo con
indice unico non è mai un delta cieco.

Costo: il worker deve avere il valore a `M` di ogni documento toccato. Per i database piccoli c'è già
(variante a2, §7.1); per i grandi è una lettura per documento toccato dalla cache del worker o da S3
(STIMATO: con 30.000 scritture al secondo su chiavi diverse e scatole da 64 MB, decine di migliaia di
blocchi letti per scarico; va misurato). Per il database da 1 TB è il punto da misurare nella prova del
database grande.

## 5. Replicazione, conferma, cambio di leader

### 5.1 Epoche e voto

Il leader è scelto tra le repliche come in Raft. Una replica senza notizie del leader per il tempo di
elezione (proposta: 150-300 ms casuale, STIMATO) diventa candidata: `epoca := epoca + 1`,
`votato := sé`, salva, manda `CHIEDI_VOTO(epoca, ultimoIndice, ultimaEpoca)` alle altre.

Una replica concede il voto (`VOTO(epoca, sì)`) se e solo se: l'`epoca` richiesta è maggiore della
propria, oppure uguale e non ha ancora votato in quest'epoca; e il log del candidato è almeno
aggiornato quanto il proprio (`ultimaEpoca` del candidato maggiore della propria, oppure uguale e
`ultimoIndice` maggiore o uguale). Prima di rispondere salva `epoca` e `votato` su disco. In ogni
caso adotta l'`epoca` ricevuta se maggiore della propria e torna seguace. Se la replica ha cancellato
tutto il disco dopo uno scarico, `ultimoIndice` e `ultimaEpoca` sono quelli dell'ultima voce cancellata.

Con 2 voti su 3 (il proprio compreso) il candidato è leader dell'epoca.

### 5.2 Replicazione

Il leader manda a ogni seguace `APPEND(epoca, posPrecedente, epocaPrecedente, voci, confermata)`.
`posPrecedente` è un **indice** (§2); `voci` sono le voci del proprio log dopo `posPrecedente`, in
ordine, al più un lotto. Un `APPEND` senza voci è un heartbeat (ogni 50 ms, STIMATO). Il leader può
mandare solo voci che ha ancora sul disco: se il seguace è sotto l'`inizioDisco` del leader serve la
ricarica da S3 (§5.7).

### 5.3 Ricezione di APPEND in una replica

1. Se `epoca < propria epoca`: rispondi `RIFIUTO(propria epoca)`. Il mittente, se è leader, torna
   seguace.
2. Adotta `epoca` se maggiore; torna seguace; azzera il tempo di elezione.
3. Controllo del prefisso: se il proprio log non ha una voce all'indice `posPrecedente` con epoca
   `epocaPrecedente` (con `posPrecedente = 0` il controllo passa sempre; se `posPrecedente` è sotto
   `inizioDisco` il controllo passa, perché tutto sotto `inizioDisco` è confermato e in S3; se è
   l'ultima voce cancellata si confronta la coppia conservata), rispondi
   `RIFIUTO(epoca, ultimoIndice)` e fermati.
4. Scorri le voci ricevute in ordine. Se una voce ricevuta è in conflitto con una propria (stesso
   indice, epoca diversa), tronca il proprio log da quell'indice in avanti (tutte le voci troncate sono
   non confermate) e aggiungi da lì le voci ricevute. Una voce ricevuta uguale a una propria (stesso
   indice, stessa epoca) si salta. **Se non c'è conflitto non si tronca nulla**: le voci proprie dopo
   l'ultima ricevuta restano, anche se vengono da un leader deposto (regola di Raft).
5. `fdatasync` delle voci nuove.
6. Deve: `confermataLocale := max(confermataLocale, min(confermata, posPrecedente + numero di voci
   ricevute))` (in indici, poi tradotta in posizione). **Non** `min(confermata, ultima posizione propria)`:
   le voci dopo l'ultima ricevuta non sono state confrontate con quelle del leader e possono essere
   voci mai confermate di un leader deposto. Errore del primo giro trovato dal revisore: con la regola
   vecchia un battito con `posPrecedente = 0` fa credere confermata a una replica la propria barriera di
   un'epoca deposta; TLC lo trova in 1 s (controllo negativo `CommitByTail`, `docs/tla-registro.md`).
   Succede nella realtà: dopo un rifiuto il leader abbassa `posPrecedente`, e un battito di 50 ms può
   partire prima delle voci.
7. Aggiorna l'indice del disco e la cache in RAM con le voci nuove (e toglie quelle troncate).
   Rispondi `ACK(epoca, posPrecedente + numero di voci ricevute)`.

### 5.4 Conferma nel leader

Alla ricezione di `ACK(epoca, p)` da `r`: `combacia[r] := max(combacia[r], p)`. Sia `c` l'indice
più alto tale che almeno 2 repliche su 3 (il leader compreso, che combacia con sé stesso dopo il proprio
`fdatasync`) hanno `combacia >= c` **e** la voce a `c` ha l'epoca del leader corrente. Se `c > confermata`:
`confermata := c`, rispondi `CONFERMATO` ai `COMMIT` delle posizioni fino a `c`, e porta `confermata`
nei prossimi `APPEND`.

Alla ricezione di `RIFIUTO(epoca, ultimoIndice)` con epoca uguale: `posPrecedente` per quella
replica scende a `min(posPrecedente - 1, ultimoIndice)` (indici, non posizioni: con l'orologio ibrido
"posizione meno 1" non è una voce) e si rimanda. Se scende sotto il proprio `inizioDisco`, il leader dice
alla replica di ricaricarsi da S3 (§5.7). Con epoca maggiore: torna seguace.

La condizione "la voce a `c` ha l'epoca del leader corrente" è la regola di Raft che impedisce di
contare come confermata una voce di un mandato precedente prima che una voce del mandato corrente la
copra. È per questo che il nuovo leader aggiunge subito una barriera. Giro 2: senza questa condizione
l'errore (figura 8 dell'articolo di Raft) richiede tre cambi di leader; TLC lo trova con 4 epoche e
non con 3 (controlli `CommitOldEpoch`), e con la regola il modello a 4 epoche è esplorato tutto senza
violazioni (`docs/tla-registro.md`).

### 5.5 Nuovo leader: barriera e ricostruzione

Appena eletto, il leader:

1. imposta `pronto := falso`, `combacia[r] := 0` per gli altri;
2. aggiunge al proprio log la voce `barriera` con la propria epoca e la replica;
3. quando la barriera è confermata, tutte le voci del proprio log sono confermate;
4. ricostruisce `ultimaStrutturale`, `ultimaQualsiasi` e `tabellaId` leggendo dal proprio disco le
   voci del proprio log da `inizioValidazione` (la posizione di 60 s fa, o la propria `inizioFinestra`
   se più alta) all'ultima;
5. `pronto := vero`.

Fino a `pronto` i `COMMIT` ricevono `RIPROVA` e le richieste `POSIZIONE` non ricevono risposta (§6.1).
Si deve ricostruire da tutte le voci del log, confermate e non: tutte quelle presenti nel log del nuovo
leader saranno confermate in quest'ordine. Giro 2: ricostruire solo dalla parte confermata duplica un
id (controllo negativo `RebuildCommittedOnly`, TLC in pochi secondi). Un `COMMIT`
ripetuto il cui `id` è nel log ricostruito riceve l'esito memorizzato: la posizione e la risposta
sono quelle del primo tentativo, anche se il leader di allora è morto. Un `id` assente dal log del nuovo
leader non ha mai avuto esito (il vecchio leader non ha mai risposto `CONFERMATO` per esso, perché una
voce confermata è su 2 repliche di 3 e quindi nel log di ogni nuovo leader): il tentativo viene
trattato come nuovo.

### 5.6 Avanzamento della finestra

Ogni 60 s circa il leader alza `inizioValidazione` alla posizione della prima voce più recente di
60 s e scarta dalle mappe e dalla tabella le chiavi e gli id la cui posizione è più bassa. È sicuro
perché i `COMMIT` con `S < inizioValidazione` vengono rifiutati: un conflitto con una voce più vecchia
di `inizioValidazione` è impossibile con `S >= inizioValidazione`. Deve: `inizioValidazione` avanza solo
su voci confermate (se la maggioranza è ferma da più di 60 s la finestra aspetta). Le repliche fanno lo
stesso con `inizioFinestra`: la cache in RAM si svuota dal basso, ma le voci restano sul disco e
nell'indice del disco fino allo scarico (§6.2).

### 5.7 Ricarica da S3 (replica rimasta indietro)

Il leader non può mandare voci che ha già cancellato dal disco. Una replica che riparte dopo un fermo
lungo, o che è stata lenta, può trovarsi sotto l'`inizioDisco` del leader. Allora, come lo snapshot di
Raft:

1. il leader risponde al suo `RIFIUTO` con `RICARICA(inizioDisco del leader)`;
2. il punto di ricarica `P` è la voce sotto l'`inizioDisco` del leader, di cui il leader manda
   `(indice, posizione, epoca)`. Ogni voce fino a `P` è in S3 per il suo database (è la regola di
   `inizioDisco`, §3.1). La replica rilegge il manifest dei database di cui ha bisogno solo quando le
   serve, come un nodo d'ingresso (§6.2); per conto proprio non ha nulla da copiare da S3;
3. se il proprio log ha a quell'indice una voce con la stessa epoca, la replica tiene le proprie voci
   successive; altrimenti butta tutto il log (le voci buttate non sono confermate: una voce confermata
   dopo `P` implica, per il controllo del prefisso, la stessa voce a `P`);
4. `inizioDisco := P + 1`, `confermataLocale := max(confermataLocale, P)`, conserva
   `(indice, posizione, epoca)` della voce a `P` per il voto e il prefisso; svuota cache e indice del disco
   sotto `P`;
5. da qui riprende con gli `APPEND` normali. Non serve leggere le scatole: i valori sotto `P` sono in
   S3 e la replica non li serve mai (§6.2 punto 3 chiede `M >= potatoFinoA`). Fino a che non ha di
   nuovo `M[db]` per un database risponde `FUORI_FINESTRA` con `P`.

Verificato con TLC nel modello `LettureScarico-disco1` (azione `Reload`), con il disco che si svuota
(`TrimR`), un cambio di leader e l'invariante `DurableOrS3`: ogni voce confermata è in S3 o sul disco,
sopra `inizioDisco`, di una replica viva.

## 6. Letture

### 6.1 Scelta di `S` nel nodo d'ingresso

- Default (`readConcern` `local`, `majority`, nessuno): `S := max(gettone del client, S_ingresso)`.
  Se `ora - ultimoContattoLeader > T_max` (proposta 1 s, STIMATO), prima chiedi `POSIZIONE` al leader
  e aggiorna `S_ingresso`.
- `readConcern: linearizable`: `S := max(gettone del client, risposta a POSIZIONE)`.
- Deve: il leader risponde a `POSIZIONE` solo se è **`pronto`** (la barriera del proprio mandato è
  confermata, §5.5) e solo dopo aver ricevuto, dopo la richiesta, un `ACK` di heartbeat da almeno
  un'altra replica nella propria epoca (ReadIndex). Errore del primo giro trovato dal revisore: senza
  "pronto", un leader appena eletto ha come `confermata` quella vecchia, che può essere più bassa di una
  scrittura già confermata al client dal leader precedente; la lettura lineare risponderebbe senza
  quella scrittura (controllo negativo `LinNotReady`, TLC in pochi secondi). Un leader non pronto tiene la
  richiesta finché lo diventa (al più un giro di conferma della barriera). Un leader deposto non ottiene
  l'`ACK` e non risponde; il nodo d'ingresso va a cercare il leader nuovo. Le richieste pendenti di un
  nodo d'ingresso si servono con un solo heartbeat.
- Deve: `S := max(S, M)` dove `M` è la posizione del manifest che il nodo d'ingresso userà (le scatole a
  `M` contengono già tutto fino a `M`, che è confermata).

### 6.2 Composizione

1. Prendi la vista del manifest del database (`M`, SST) dalla cache. Non la si rilegge a intervalli
   fissi: una rilettura ogni 5 s per 1.000 database sarebbero circa 518.000 GET al mese per database e
   nodo d'ingresso (STIMATO in `proto/INTERFACCIA.md` di proto-lettura, 4 volte la soglia di costo). La
   si rilegge solo quando la replica dice che la vista non basta più (punto 4), cioè al più una volta per
   scarico, oppure quando una scatola risponde 404 (§7.2).
2. Manda a una replica (la più vicina, nella stessa zona) `LEGGI(db, chiavi o collezione, M, S)`.
3. Deve: la replica risponde con i record solo se `S <= confermataLocale` e `M >= potatoFinoA(db)`, dove
   **`potatoFinoA(db)` è la posizione dell'ultima voce cancellata dal disco** (cioè la voce sotto
   `inizioDisco`), non l'inizio della finestra in RAM. La finestra è solo una cache: dentro la finestra
   la replica risponde dalla RAM, sotto risponde dall'indice del disco (posizioni in RAM o nella cantina,
   valori dal registro su disco). Così le scritture tra 60 s e lo scarico sono servite. Altrimenti
   risponde `NON_HO_S(confermataLocale)` oppure `FUORI_FINESTRA(potatoFinoA, etagUltimoManifest)`. In
   ogni risposta la replica mette `confermataLocale`. La replica non risponde mai `FUORI_FINESTRA` solo
   perché la base di una chiave non è nel registro: il registro copre `(M, S]` per posizione, non per
   chiave; se per una chiave ha solo delta, restituisce i delta (o la loro somma) e il nodo d'ingresso li
   applica al valore letto dagli SST (differenza 2 di proto-lettura; il "limite noto" di proto-quaderno
   che rispondeva `fuori_finestra` perdeva quei delta). Per un database che la replica conosce senza
   voci sul disco, `potatoFinoA(db) = M[db]` per quel database (la replica ricorda `M[db]` per ogni
   database del registro); per un database che non conosce affatto (appena ripartita) risponde
   `FUORI_FINESTRA` con l'`inizioDisco` globale.
4. Su `NON_HO_S`: prova le altre repliche; se nessuna ha `S`, aspetta e riprova fino a un tempo massimo
   (proposta 2 s, STIMATO), poi errore ritentabile. Su `FUORI_FINESTRA(potatoFinoA)`: rileggi il manifest
   da S3 (ne basta uno con `M >= potatoFinoA`) e ricomincia. Con la regola del punto 3 succede solo dopo
   uno scarico, quindi al più una volta per scarico e per nodo d'ingresso.
5. Leggi i valori a `M` dagli SST (cache o S3) e applica i record ricevuti in ordine di posizione:
   una scrittura `strutturale` sostituisce il valore (o lo cancella), un `delta` si applica al valore
   corrente con la semantica degli operatori di MongoDB. Le voci degli indici unici si applicano allo
   stesso modo; quelle degli indici non unici si ricalcolano dai documenti (§4.7).
6. Rispondi al client con gettone `S`. Aggiorna `S_ingresso` con la `confermataLocale` ricevuta.

La regola del passo 3 è ciò che rende innocua una replica isolata ma viva: la sua `confermataLocale`
è ferma al momento dell'isolamento, e per quelle posizioni le voci non cambiano più. Servire in base
alla lunghezza della coda sarebbe sbagliato: una replica può avere a una posizione non confermata una
voce che un leader più nuovo ha sostituito.

Perché la regola vecchia (`potatoFinoA = inizioFinestra - 1`) non andava: con lo scarico ogni 10
minuti, una scrittura di 2 minuti fa non è più nella finestra e non è ancora in S3. Ogni lettura del
database riceveva `FUORI_FINESTRA`, rileggeva il manifest, trovava la stessa `M` e restava ferma fino
allo scarico: fino a circa 9 minuti (STIMATO). Non perdeva dati, bloccava. TLC lo mostra con
l'invariante `ReadServable` (controllo `ServeRamOnly`). Con la regola nuova, servire anche quando il
disco non ha più `(M, S]` restituirebbe una lettura senza le voci cancellate: controllo `NoServeGuard`,
TLC trova la lettura sbagliata.

Quanto costa in RAM l'indice del disco: una voce per scrittura non scaricata, posizione e riferimento
al disco senza valore, circa 40–60 byte (STIMATO). A 30.000 scritture al secondo per 600 s sono 18
milioni di voci, circa 0,7–1,1 GB per replica (STIMATO dal revisore), contro i 20–25 GB di un indice con i
valori (STIMATO). Nel prototipo una scheda per record non scaricato costa circa 300 byte (MISURATO da
S2, giro 2): per questo serve il tetto (§7.4) e la cantina (§7.5), che porta la parte vecchia dell'indice
su disco locale (una chiave ogni 128 in RAM).

### 6.3 Letture che non sono per chiave

Una query con filtro su un campo indicizzato usa l'indice negli SST a `M` e chiede alla replica tutte
le voci della collezione in `(M, S]`, unendole come in §4.7: i documenti toccati in `(M, S]` si
rivalutano sul valore composto, gli altri si prendono dall'indice a `M`. Un `count`, un `distinct`, una
`aggregate` fanno lo stesso. **`(M, S]` non è "al più 60 s"** (testo del primo giro, sbagliato): è tutto
ciò che non è ancora scaricato, fino all'età massima di scarico (10 minuti) o alla soglia di dimensione.
Per questo la replica tiene nell'indice del disco anche l'elenco delle chiavi toccate per collezione, e
risponde con le sole voci della collezione chiesta, non con tutto l'intervallo. Il costo è lineare nelle
scritture non scaricate della collezione. Per un database grande e caldo (30.000 scritture al secondo,
scarico per dimensione a 32–64 MB, §7.1) sono circa 160.000–320.000 voci per collezione nel caso
peggiore (STIMATO: 64 MB a 200 byte per voce); è il motivo per cui i pezzi caldi si scaricano per
dimensione e non per età. Va misurato nella prova del database grande.

### 6.4 Bollettino (avvisi del quaderno ai camerieri)

Per non chiedere al quaderno a ogni lettura, il nodo d'ingresso può tenere dei **fatti** del tipo
"il database `d` (o la chiave `k`) non ha voci con posizione in `(M, P]`". Regole (proposta di "letture
e affinità" con le correzioni del suo revisore; modello `Bollettino.tla`, `docs/tla-registro.md`):

1. Regola della conoscenza completa (deve). Un fatto detto da una replica con `P <= confermataLocale`
   nel momento in cui lo dice è vero per sempre. Il nodo d'ingresso compone una lettura a `S` senza
   `LEGGI` solo se ha un fatto con `P >= S` e usa un manifest con `M` uguale o più alto di quello del
   fatto. Mai `S > P`: una lettura che ignora una voce confermata a `C <= S` prepara una scrittura con
   `S` che il leader valida solo sopra `S`, e l'aggiornamento va perso.
2. `ISCRIVITI` (nodo d'ingresso verso la replica della sua zona). La replica risponde con una
   **fotografia** costruita dall'indice del disco, non dalla finestra: per ogni database del registro
   `ultima[db]`, la posizione più alta di una voce sopra `M[db]` sul disco, o "niente sopra `M[db]`".
   Una fotografia fatta solo sulla finestra di 60 s dimentica le scritture tra 60 s e lo scarico
   (errore 1 del revisore).
3. `ANNUNCIO(confermataLocale, [(db, collezione, posizioneMax, [chiavi con posizione])],
   [(db, M)])` a ogni `APPEND` appeso. Deve: ogni voce appesa (confermata o no) compare in un annuncio
   prima che un annuncio porti una `confermataLocale` che la copre; gli annunci vanno in ordine di
   posizione.
4. Il nodo d'ingresso tiene `sapereFinoA` (massimo delle `confermataLocale` ricevute), `ultima[db]` e,
   per chiave, la posizione più alta annunciata. Deve: i database e le chiavi toccati si **uniscono
   sempre**, da qualunque annuncio, anche vecchio o duplicato, e da ogni nuova fotografia (errore 3: dopo
   il riavvio di una replica gli annunci ripartono da una `confermataLocale` più bassa). Quando adotta un
   manifest nuovo con `M2`, toglie solo le chiavi con posizione fino a `M2` (errore 2).
5. Dopo una nuova iscrizione, per ogni database con `ultima > M` nella fotografia, il nodo d'ingresso
   chiede al quaderno per ogni chiave (livello 1) finché il suo manifest non supera la posizione della
   fotografia: la fotografia non porta le chiavi (errore 4).
6. Lettura: si salta `LEGGI` solo se `S <= sapereFinoA`, `S >= M` e il database (o la chiave) non ha
   voci annunciate sopra `M`. Vale anche per `linearizable`, con la `S` dal leader (errore 6).
7. Se `sapereFinoA` non sale da più di `T_max` (STIMATO 150–200 ms, tre heartbeat), il nodo d'ingresso
   chiede `POSIZIONE` al leader e va su un'altra replica (replica isolata ma viva, errore 5). È
   freschezza, non sicurezza.
8. I cambi di metadati (`drop`, `rename`, `createIndex`) passano per la strada dei metadati (separata, §12), non
   per il registro dei dati: un fatto "niente su `d` dopo `M`" non dice nulla di un `drop`. Finché la
   strada dei metadati non passa dal bollettino, il nodo d'ingresso non salta la domanda per una
   collezione il cui catalogo è cambiato dopo la sua vista (errore 8).

## 7. Scarico su S3 e compattazione

### 7.1 Scarico: quando e come

**Quando si scarica un database** (regole del fondatore del 1 novembre e variante a2 della Sessione
Lettura; "dovrebbe", perché la sicurezza non dipende dalla cadenza: una scrittura è confermata solo
con 2 copie su 3 su disco). Un worker scarica un database `d` quando vale una di queste:

1. **età**: il record non scaricato più vecchio di `d` ha più di 10 minuti (proposta);
2. **dimensione**: i byte non scaricati di `d` superano una soglia che dipende dalla classe del
   database:
   - **database piccolo** (tutte le sue scatole insieme fino a 4 MB): soglia 1 MB, e lo scarico
     **riscrive tutto il database in una sola scatola** (base fusa con le voci nuove) invece di
     aggiungerne una (variante a2). Costo misurato in locale con MinIO: circa $0,048 al mese per
     database con 1 scrittura e 2 letture al secondo, contro $0,185 aggiungendo scatole (MISURATO dalla
     Sessione Lettura, passo 5). Sopra i 4 MB la riscrittura costa troppa banda e il database passa
     alla regola media;
   - **database medio**: soglia 1 MB, scatole aggiunte e fuse dalla compattazione (§7.2);
   - **database grande, o pezzo caldo di un database grande**: soglia **32–64 MB**. A 1 MB un database
     da 6 MB/s di scritture farebbe circa 6 scarichi al secondo, circa 31 milioni di PUT al mese, circa
     $155 (STIMATO dal revisore); a 64 MB circa uno ogni 10 s, circa 490.000 PUT al mese contando
     scatola e manifest, circa $2,4 (STIMATO, $0,005 ogni 1.000 PUT). Il prezzo è un `(M, S]` più grande
     per le letture per collezione (§6.3) e un indice del disco più grande (§6.2);
3. **pressione di RAM**: il conto della memoria del registro (§7.4) supera il 70% del tetto. Allora i
   worker scaricano subito, senza aspettare età né dimensione, cominciando dai database con più byte non
   scaricati ancora in RAM (liberano più RAM per ogni PUT), poi dai più vecchi, con un limite di velocità
   (64 MB/s per replica nel prototipo) perché lo scarico non rubi la rete e la CPU ai commit. Nel
   prototipo, con 2.000 database piccoli a 1 scrittura al secondo e scarico a 10 minuti, il registro
   tiene circa 685 KB per database al picco (MISURATO da S2, giro 3); con 10.000 database sarebbero
   circa 6,9 GB per replica (STIMATO), sopra il tetto di una cucina media (8 GiB, tetto al 60% = 4,8 GB).
   Quindi lì decide la pressione: i database piccoli si scaricano prima dei 10 minuti e il costo in PUT
   sale (STIMATO, da misurare nella giornata C);
4. **emergenza**: è caduta una copia del registro (§7.6).

Non si scarica a intervalli fissi: un PUT di SST più un PUT di manifest ogni 5 s per database sarebbero
circa 1.036.800 PUT al mese, $5,18 al mese per database (STIMATO in `proto/INTERFACCIA.md` di
proto-lettura). Per i database piccoli si possono anche mettere più database in un oggetto condiviso e
in un manifest di gruppo (lo stato di scarico per database resta separato: `scaricato[d]`), ma la
Sessione Lettura ha misurato che costa più GET (variante b, $0,256 al mese per database): la regola per
i piccoli è a2. Le repliche espongono `pendenti(limite)` con, per database, byte e numero di record non
su S3, byte ancora in RAM, età del più vecchio e `scaricato[d]`.

**Il passo per un database `d`** (o per un gruppo):

1. legge il manifest di `d`: `M`, lista SST, ETag;
2. chiede a una replica `LEGGI_INTERVALLO(d, M, fino)`, con `fino` la `confermataLocale` della
   replica; la replica risponde solo con voci confermate (`fino <= confermataLocale`) e solo se
   `M >= potatoFinoA(d)` (ha ancora tutto sul disco);
3. costruisce l'SST con le voci `(M, fino]` di `d` (con le voci degli indici non unici ricalcolate dai
   documenti, §4.7) e lo scrive su S3 con un nome unico mai usato (identificatore casuale di 128 bit nel
   nome). Un nome derivato dall'intervallo (`M`-`fino`) basterebbe per l'unicità del contenuto, ma un
   nome derivato dalla sola `M` è sbagliato: due worker che partono dalla stessa `M` e arrivano a `fino`
   diversi si sovrascriverebbero l'oggetto;
4. scrive il manifest con `M := fino` e la lista SST più il nuovo, sempre con PUT condizionale
   (`If-Match: ETag`, o `If-None-Match: *` per il primo). Mai un PUT senza condizione, nemmeno per
   ritentare: nel prototipo un PUT partito prima di una sospensione di S3 e arrivato dopo ha coperto un
   manifest più nuovo e ha perso 998 scritture confermate (MISURATO da S2, giro 3, tentativo 1);
5. se il PUT condizionale fallisce (412), prima di toccare il proprio SST **rilegge il manifest**: se il
   proprio SST vi compare, il PUT era riuscito e la risposta si era persa (il ritentativo automatico del
   client HTTP riceve il 412 del proprio stesso PUT): il lavoro è fatto, si prosegue dal punto 6. Se non
   compare, un altro worker ha vinto e il proprio SST è un orfano da cancellare (o da lasciare alla
   raccolta periodica degli SST non elencati in nessun manifest da più di un'ora). Cancellare senza
   rileggere perde dati: il controllo negativo `DeleteOrphanBlind` di `LettureScarico.tla` lo mostra;
6. se riesce, dice alle repliche `SCARICATO(d, fino)`; le repliche aggiornano `M[d]` e possono alzare
   `inizioDisco` fino alla prima voce non scaricata di qualsiasi database (con un margine), mai sopra
   `inizioFinestra`.

Due worker sullo stesso database non perdono né duplicano nulla: il manifest che vince descrive sempre un
prefisso esatto del registro, e ogni SST elencato contiene esattamente l'intervallo che il manifest gli
attribuisce.

Le tre regole che la Sessione Lettura ha fatto cambiare al primo giro restano: niente rilettura del
manifest ogni 5 s (§6.2 punto 1), niente scarico ogni 5 s (sopra), niente cancellazione dell'SST
orfano dopo un 412 senza rileggere il manifest (punto 5).

### 7.2 Compattazione, periodo di grazia e 404

Un worker legge il manifest, fonde alcuni SST in uno nuovo (nome unico), scrive il manifest con il nuovo
al posto dei vecchi e stesso `M`, con PUT condizionale. I vecchi SST si cancellano dopo un periodo di
grazia, perché un nodo d'ingresso con una vista vecchia del manifest può ancora leggerli.

Deve: **su 404 di una scatola, il nodo d'ingresso (o il worker) rilegge il manifest e ricomincia la
lettura**. Il periodo di grazia da solo non basta: il manifest si rilegge solo su `FUORI_FINESTRA` (§6.2),
e un database fermo non riceve mai `FUORI_FINESTRA`, quindi un nodo d'ingresso può tenere per ore una
vista che elenca scatole già cancellate dalla compattazione. Il manifest nuovo descrive lo stesso
contenuto con scatole diverse, quindi la lettura ripetuta dà lo stesso risultato. Un 404 sul manifest
stesso (database cancellato) è un errore per il client, non un ritentativo. Il periodo di grazia
proposto resta almeno 24 ore (revisore dei rami: costa un giorno di scatole vecchie, per 1 TB riscritto
al giorno circa $23 al mese, STIMATO), e si può accorciare quando la regola del 404 è provata. Con i rami
(§10) cancella solo il raccoglitore della famiglia, mai la compattazione.

### 7.3 Database inattivi

Un database senza scritture nuove non ha nulla da scaricare; il suo `M[d]` resta indietro senza fermare
`inizioDisco`, perché la regola è "prima voce non scaricata di qualsiasi database" e lui non ne ha. Il
worker visita comunque ogni database con voci non scaricate almeno ogni 5 s (solo `pendenti`, nessuna
richiesta S3).

### 7.4 Conto della memoria e tetto

Deve (per la disponibilità, non per la sicurezza dei dati): il registro non muore mai per memoria. Le
tre copie tengono gli stessi dati, quindi un esaurimento della memoria le colpisce tutte allo stesso
modo (guasto correlato): nel banco 6 del giro 2 due repliche su tre sono morte così (MISURATO da S2).

1. **Un solo conto** per replica: valori in RAM, schede per record non scaricato, versioni dell'indice,
   chiavi, slot del registro, tabella degli id, memoria della cantina (§7.5) e delle code di rete.
2. **Tetto**: per difetto il 60% della RAM della macchina (nella cucina media sulla stessa macchina c'è
   anche il cameriere).
3. Il controllo usa **il maggiore tra il conto e la memoria vera del processo** (RSS), letta ogni
   secondo: il conto a costi fissi sottostima, nel prototipo circa la metà dell'RSS (MISURATO, 812 MB di
   conto contro 1,6 GB di RSS). In produzione il conto va preso dai contatori dell'allocatore.
4. **Soglie** (frazioni del tetto, quelle del prototipo): al 65% i valori in RAM si sfrattano sul
   disco (restano le posizioni); al 70% partono la cantina e lo scarico per pressione (§7.1 punto 3);
   al 100% il freno di memoria rifiuta i commit finché non si scende sotto il 95% (§7.7). Serve margine:
   mentre scrive un pezzo di cantina la RAM sale di circa 350 MB oltre la soglia (MISURATO).
5. L'allocatore deve restituire la memoria al sistema dopo un arretrato (nel prototipo jemalloc con
   pulizia di sfondo: con glibc l'RSS restava 0,4–0,5 GB sopra il normale, MISURATO).

### 7.5 Cantina: i fogli vecchi su disco locale

Se S3 non risponde, lo scarico non può liberare RAM. Allora (deve per la disponibilità):

1. sopra il 70% del tetto, i record non scaricati più vecchi di 60 s escono dalla RAM: le loro
   posizioni vanno in file ordinati per `(database, chiave, posizione)` sul disco locale (la **cantina**;
   S2 la chiama "dispensa locale"), che puntano al registro su disco per i valori; in RAM resta un
   indice rado (una chiave ogni 128). I valori non si copiano: stanno già nel registro;
2. si scelgono i database con più byte non scaricati ancora in RAM, al più un pezzo ogni 2 s, al più
   256 database e 50.000 record per database per pezzo (regole venute dai tentativi falliti del banco 6);
3. le letture (§6.2) scendono nella cantina quando la RAM non basta: più lente (qualche ms), corrette;
4. lo scarico legge prima la cantina, dal pezzo più vecchio, poi la RAM;
5. un pezzo di cantina si cancella quando tutti i suoi database sono scaricati oltre le sue posizioni;
   un segmento del registro solo quando tutti i suoi record sono scaricati (regola di `inizioDisco`).

La cantina non cambia nulla per la sicurezza: è un indice di ciò che è già sul disco sopra
`inizioDisco`, e la regola "si cancella dal disco solo ciò che è in S3" resta (modello `LettureScarico`,
invarianti `NothingLost` e `DurableOrS3`). Limite noto: le schede della cantina crescono ancora con la
durata del guasto, circa 0,5 MB/s al ritmo del banco 6 (STIMATO da S2); per guasti di ore servono la
fusione dei pezzi di cantina oppure il freno, che porta alla sola lettura. Un guasto di S3 di ore porta
quindi alla sola lettura, mai alla morte. Dopo un riavvio con un arretrato grande l'indice del disco va
ricostruito a pezzi, non tutto in RAM (non provato).

### 7.6 Scarico d'emergenza quando cade una copia

Quando una replica è dichiarata persa (nessun `ACK` per qualche secondo, proposta 5–10 s, STIMATO,
per non scattare su una pausa breve della rete o del garbage collector), le altre due **scaricano tutto
il non scaricato**, gradualmente (con il limite di velocità del punto 3 di §7.1, dai database più
grossi), e intanto la regia avvia la copia di rimpiazzo (voce `configurazione`, fuori da questa fase).
Lo scopo è ridurre ciò che dipende da 2 dischi invece che da 3 finché la copia nuova non è in pari.

**Lo scarico d'emergenza non copre il guasto di S3.** Se è S3 a non rispondere non c'è dove scaricare,
e le copie rimaste sono nello stesso stato: nel banco 6 del giro 2 la copia è caduta proprio perché S3
era fermo (MISURATO da S2). In quel caso la protezione sono la cantina (§7.5) e il freno (§7.7). Non è
nel prototipo al giro 3.

### 7.7 Freno (contropressione), per database e per registro

Deve (per la disponibilità): sopra le soglie, il leader rallenta e poi rifiuta, invece di riempire disco
e memoria. Tre gradini sul disco occupato da registro e cantina (soglie da legare al disco vero, per
esempio metà, tre quarti e nove decimi; nel prototipo 2,5, 3 e 3,5 GB):

1. **rallenta** i database che scrivono di più (almeno il 20% del totale o almeno 1.000 scritture al
   secondo): gettoni a ritmo ridotto, attesa al più 100 ms. I piccoli non vengono toccati: paga il
   vicino rumoroso;
2. **rifiuta** con un errore ritentabile (`FRENO`, che il nodo d'ingresso traduce in un errore con
   etichetta `RetryableWriteError` di MongoDB), subito, prima di mettere il commit in coda (§4.2 punto
   0). I driver MongoDB ritentano una scrittura una volta sola: il nodo d'ingresso tiene lui i
   ritentativi con attesa crescente fino a un tempo massimo (proposta 2 s, STIMATO) prima di rispondere
   al client. Nel prototipo 63.095 commit sono finiti con esito ignoto invece che rifiutati in modo
   pulito durante il freno (MISURATO, banco 6, giro 3): è il motivo della regola "rifiuta prima di
   accodare";
3. **sola lettura** per il registro.

In più il freno di memoria (§7.4 punto 4). Il freno è **per database e per registro, senza
coordinatore centrale**: ogni leader decide da solo, con i contatori che ha già (byte e scritture al
secondo per database, disco e memoria propri). Un database ha anche una quota di disco propria (i byte
del registro che trattiene perché non scaricati): se la supera, per esempio perché il suo scarico è
bloccato da un manifest conteso, il freno scatta solo per lui. Limite: i segmenti del registro sono
condivisi, quindi il disco trattenuto da quel database resta occupato finché il suo scarico non
riparte; per questo un database grande ha un registro suo (§11). Con un guasto di S3 di una regione
tutte le celle passano dagli stessi gradini, ognuna per conto suo: nessuna ha bisogno di parlare con le
altre.

### 7.8 Allarme e recupero

- Allarme (per la sala di controllo) quando il record non scaricato più vecchio supera una soglia
  (proposta 120 s; 60 s nei banchi), con età, memoria e disco, ripetuto ogni 30 s finché dura.
- Dopo il guasto: scarico a piena velocità, prima la cantina dal pezzo più vecchio, poi restituzione
  della memoria al sistema. Nel prototipo, dopo 10 minuti di S3 fermo a 15.000 scritture al secondo,
  il recupero completo (cantina vuota, record più vecchio sotto 30 s, freno a zero) ha richiesto 126 s,
  con zero scritture confermate perse su 948.628 controllate (MISURATO da S2, giro 3).

## 8. Guasti, uno per uno

- **Replica si ferma e riparte con il disco intatto.** Riparte seguace con `epoca`, `votato`, `log`
  dal disco; `confermataLocale` riparte da 0 e risale col primo `APPEND` che supera il controllo del
  prefisso; cache in RAM e indice del disco si ricostruiscono dal disco (a pezzi, §7.5). Fino ad allora
  rifiuta le letture (`NON_HO_S`). Nessuna perdita: le voci confermate sono su un'altra replica e il
  leader la riporta in pari con gli `APPEND`, oppure, se è rimasta sotto l'`inizioDisco` del leader,
  con la ricarica da S3 (§5.7).
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
  `APPEND` per indice (una voce già presente con la stessa epoca non cambia nulla), `ACK` per massimo,
  `VOTO` per epoca, `COMMIT` per `id`, `SCARICATO` per massimo, PUT del manifest per ETag.
- **`fdatasync` fallisce o il disco dà errori.** La replica si ferma e si considera "disco perso".
- **S3 non risponde.** Le scritture continuano finché il disco delle repliche ha spazio (il registro
  cresce sopra `inizioDisco` fermo); la RAM resta sotto il tetto grazie alla cantina (§7.5); sopra le
  soglie di disco il freno rallenta, poi rifiuta con errore ritentabile, poi mette in sola lettura
  (§7.7). Le letture servono quanto hanno in cache più il registro (finestra, indice del disco,
  cantina); i blocchi delle scatole non in cache falliscono con errore ritentabile. Lo scarico
  d'emergenza non aiuta (§7.6). Allarme dopo 120 s (§7.8).
- **Una copia del registro cade.** Le altre due continuano (maggioranza) e scaricano tutto il non
  scaricato, gradualmente (§7.6); la copia si rimpiazza.
- **Orologi sbagliati.** Nessuna regola di sicurezza dipende dall'orologio: epoche, posizioni,
  `inizioValidazione` e `inizioFinestra` sono per posizione. L'orologio decide solo quando avanzare le
  finestre e quando scadono i tempi di elezione e di attesa.
- **Un worker muore a metà scarico.** Al più un SST orfano, raccolto dopo. Il manifest è intatto. Se
  muore tra il PUT del manifest e `SCARICATO`, il prossimo worker vede dal manifest (o da `scaricato[d]`
  in `pendenti`) che `M` è già avanzata e manda `SCARICATO` senza scaricare di nuovo.
- **Due worker sullo stesso database.** Uno vince il PUT condizionale, l'altro cancella il proprio
  SST. Nessuna perdita né duplicazione (§7.1).

## 9. Regge per un database da 1 TB?

Per la sicurezza sì: nessuna regola dipende dalla dimensione dei dati; mappe del leader, tabella degli
id e indice del disco dipendono dalle scritture recenti, non dai dati. Per la scala valgono queste
condizioni, scritte qui perché senza di esse il protocollo funziona solo per i database piccoli:

- un database grande ha **un registro suo** (§11): un registro ha un solo leader e un solo thread di
  validazione, circa 46.000 record al secondo per core al massimo (MISURATO da S2, banco 1a) e
  17.000–22.000 con il costo per richiesta di oggi (STIMATO dal revisore di S2);
- scarico per dimensione a 32–64 MB per i pezzi caldi (§7.1), indice del disco con chiavi per collezione
  (§6.2, §6.3), cantina e freno per database (§7.5, §7.7);
- il manifest di un database da 1 TB con scatole da 64 MB ha circa 16.000 voci, 1–2 MB (STIMATO dal
  revisore), riscritto a ogni scarico: serve un manifest a livelli (un indice di manifest, come i
  checkpoint di Delta Lake). Non è ancora scritto né provato;
- le voci degli indici non unici ricalcolate allo scarico (§4.7) costano una lettura per documento
  toccato: da misurare.

## 10. Rami (fork): il record di taglio

Il fork di un database (ramo istantaneo, anche da 1 TB, copiando solo il manifest) esiste già nel motore
di oggi (`mongo-server/src/ctl/fork.rs`), ma lì ferma il padre per circa un secondo (MISURATO dal
revisore dei rami, 1,3 s su MinIO). Nel disegno nuovo il taglio passa dal registro:

1. Il nodo d'ingresso (o la regia) scrive prima il ramo nel **registro della famiglia** del database
   ("fork in corso dal padre `d`, ramo `r`"), su S3 con PUT condizionale, prima che esista qualsiasi
   cosa condivisa. Il raccoglitore della famiglia tratta un fork in corso come se usasse tutte le scatole
   del manifest del padre.
2. Poi manda al leader del registro del padre un `COMMIT` con il record speciale `taglio(d, r)`. Il
   leader gli dà la posizione `P` come a ogni altra scrittura. Ogni scrittura di `d` sta quindi per forza
   prima o dopo `P`, senza zone grigie; una transazione è un solo record, quindi sta tutta prima o tutta
   dopo: il ramo non vede mai mezza transazione.
3. Quando `P` è confermata, un worker fa uno **scarico forzato di `d` fino a `P` esatta** (non oltre:
   `fino = P`), con la regola normale (§7.1). Il manifest del ramo elenca solo scatole che finiscono
   entro `P`: le scatole del padre fino a `P`, condivise, e niente dopo.
4. Il manifest del ramo si scrive con `If-None-Match: *`, con `M = P` e un `orologio` di partenza: il
   ramo prende posizioni maggiori di `P`. Deve: il ramo non si legge filtrando "posizione `<= P`" dal
   padre, perché le scritture del ramo prendono anch'esse posizioni maggiori di `P` dallo stesso orologio,
   e un filtro le mescolerebbe con quelle del padre dopo `P`. Il ramo è un database nuovo, con un nome
   nuovo, con il proprio manifest, e (di solito) nello stesso registro del padre.
5. Scritture in volo: una scrittura confermata al client prima del taglio è nel ramo; una non ancora
   confermata finisce da una parte sola, decisa dal leader. Il client non può scrivere sul ramo finché
   il suo manifest non esiste.
6. La tabella degli id e le chiavi di sessione del ramo partono vuote (database nuovo); i change stream
   del padre non continuano sul ramo: un token di ripresa del padre usato sul ramo deve dare
   `ChangeStreamHistoryLost` (il token contiene l'id del database).
7. Raccolta: il lavaggio di un database toglie scatole dal suo manifest ma non le cancella mai; cancella
   solo il raccoglitore della famiglia, dopo il periodo di grazia (§7.2), e solo scatole che nessun
   manifest vivo della famiglia elenca e nessun fork in corso può usare. Cancellare il padre non cancella
   le sue scatole finché un ramo le usa. Su 404 di una scatola, rileggi il manifest (§7.2).

Un database grande diviso in pezzi su più registri (§11) ha bisogno di un taglio coordinato (stessa
posizione dell'orologio ibrido su ogni registro): è coerente solo perché al lancio non ci sono
transazioni tra registri. Da specificare in TLA+ insieme alla raccolta di famiglia (proposta del
revisore dei rami); non è nei modelli di questo giro.

## 11. Più registri per cucina

Una cucina ha **più registri** (quaderni), ognuno con le sue 3 repliche, il suo leader e le sue epoche,
indipendenti:

- **un registro per gruppo di database piccoli e medi** (per esempio qualche migliaio di database per
  registro, numero da fissare con le misure di RAM e CPU della giornata C);
- **un registro dedicato per ogni database grande**, e più avanti uno per pezzo di un database
  grande diviso.

Perché: un registro ha un solo leader e un solo thread di validazione (§9); un database da 1 TB con
decine di migliaia di scritture al secondo, ognuna con 5–10 mutazioni per gli indici, riempie da solo
il registro di una cucina; e un database con lo scarico bloccato trattiene il disco di tutto il suo
registro (§7.7). Con un registro dedicato il vicino rumoroso ferma solo sé stesso.

Che cosa cambia nel protocollo: niente nelle regole, che valgono per registro. Ogni record, `M[db]`,
manifest, fotografia del bollettino e voce `taglio` appartiene a un solo registro. Il nodo d'ingresso
sa quale registro serve ogni database (dalla regia, con un'epoca della mappa) e manda `COMMIT`, `LEGGI`
e `ISCRIVITI` al registro giusto. Le posizioni di registri diversi non si confrontano tra loro: un
gettone del client vale per il registro del database su cui è stato preso (in `$clusterTime` il nodo
d'ingresso porta il gettone del registro del database del comando; per un client che usa database in
registri diversi serve un gettone per registro, da specificare insieme al formato di `$clusterTime`).

**Le transazioni tra registri restano fuori al lancio.** Una transazione deve toccare database dello
stesso registro (un solo record); una che tocca database in registri diversi riceve un errore chiaro.
Lo stesso vale per una transazione su due pezzi di un database diviso in registri diversi. Se un giorno
arrivano, servono un protocollo a due fasi tra registri e un nuovo modello TLA+.

Spostare un database da un registro a un altro (ribilanciare la cucina) è un cambio di proprietà a
livello di regia, da specificare come la riconfigurazione: il vecchio registro scrive una voce di
chiusura per il database, lo scarica fino a lì, il nuovo registro parte da quella `M` con un'epoca della
mappa più alta. Non è in questo giro.

## 12. Che cosa non copre questo documento

Cambio dell'insieme delle repliche (voce `configurazione`) e rimpiazzo della copia caduta, spostamento
di un database tra registri, transazioni tra registri, autenticazione tra parti, cifratura,
multi-regione (lo stesso protocollo con le 3 repliche in 3 regioni: cambiano solo i tempi), manifest a
livelli per i database grandi, raccolta degli SST orfani e raccoglitore della famiglia dei rami,
metadati delle collezioni e degli indici (strada separata, serializzata per collezione, da far passare
anche dal bollettino), potatura delle chiavi di sessione, misure di prestazione.
