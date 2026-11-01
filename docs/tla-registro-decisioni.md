# Registro "senza cuoco": decisioni su scritture contese e letture fresche

Stato: consegna intermedia del lavoro `tla-registro` (branch `tla-registro`). Le scelte qui sotto sono
quelle che il protocollo in prosa (`docs/protocollo.md`) e le specifiche TLA+ (`tla-registro/`) adottano.
Ogni numero è etichettato MISURATO (dove) o STIMATO (come). Nessuna misura è stata presa in questa fase:
tutti i numeri sono STIMATI, a partire dal giro di 3-4 ms per un commit indicato nel brief.

## Il disegno in due righe (per fissare i nomi)

Un solo registro (log) con 3 repliche in 3 zone e un leader. Il leader riceve dai nodi d'ingresso
delle scritture già preparate, le valida, assegna a ciascuna una posizione `C` e la conferma quando
2 repliche su 3 l'hanno scritta con `fdatasync`. Le 3 repliche tengono in RAM gli ultimi 60 s del
registro indicizzati per chiave. I dati stabili sono blocchi immutabili (SST) su S3, in una sola copia.
Un nodo d'ingresso compone una lettura a posizione `S` come: valore nell'SST (che copre il registro
fino a una posizione `M` scritta nel manifest) più i record del registro con posizione in `(M, S]`,
chiesti a una replica. Una scrittura preparata a `S` porta con sé `S`, il proprio id, le chiavi che
scrive e le chiavi da cui dipende. Il leader la rifiuta se un record già committato con posizione
maggiore di `S` ha toccato una chiave da cui dipende.

Nomi usati sotto:

- `S`: posizione dell'istantanea su cui il nodo d'ingresso ha letto e preparato.
- `C`: posizione assegnata dal leader al record (sempre `C > S`).
- "confermata": la posizione più alta tale che tutti i record fino a essa sono su 2 repliche di 3.
- "finestra": gli ultimi 60 s del registro che il leader e le repliche tengono in RAM.

## A. Scritture contese

### Il problema

Con la validazione ottimistica pura ("tutto validato"), due scritture concorrenti sulla stessa
chiave sono un conflitto: la seconda ad arrivare al leader ha `S` minore della `C` della prima e
viene rifiutata. Il nodo d'ingresso rilegge a una `S` nuova, ricompone, rimanda. Ogni giro costa
3-4 ms (STIMATO, dal brief: un giro leader + 2 fdatasync + risposta).

Tre casi tipici:

1. Documento caldo con `$inc` da 100 client (contatore, "mi piace", saldo aggregato).
2. Indice unico (email, codice ordine): inserimenti concorrenti con valori diversi, raramente uguali.
3. `findAndModify` in ciclo (coda di lavoro: "prendi il primo elemento pronto e marcalo preso").

### Opzione (a): tutto validato

Ogni record dichiara le chiavi che scrive, e il leader rifiuta se una di esse è stata scritta dopo `S`.

Caso 1, 100 client che fanno `$inc` sullo stesso documento nello stesso istante, tutti con la stessa
`S`: il primo al leader passa, gli altri 99 falliscono e ripartono. Al giro dopo, uno passa e 98
falliscono. In totale 100 giri: l'ultimo client aspetta 100 × 3,5 ms ≈ 350 ms (STIMATO), il leader
valuta 100 + 99 + ... + 1 ≈ 5.000 richieste per 100 commit utili (STIMATO: 50 volte il lavoro
necessario), e il nodo d'ingresso ricompone 5.000 volte. La capacità su una chiave calda è di un
commit per giro, cioè 250-330 commit al secondo (STIMATO: 1 / 3-4 ms). Non c'è garanzia che un
client sfortunato non perda sempre (fame).

Caso 2, indice unico: due inserimenti conflittano solo se scrivono la stessa chiave d'indice (stesso
valore unico). Con valori diversi non c'è conflitto: la validazione costa poco e serve davvero.
Il secondo di due inserimenti con lo stesso valore fallisce la validazione, rilegge a `S` nuova,
trova il valore già presente e risponde al client con l'errore di chiave duplicata (E11000) di MongoDB.
Esito giusto.

Caso 3, `findAndModify` in ciclo: come il caso 1. Se la query sceglie "il primo pronto" tramite un
indice, tutti i worker scelgono lo stesso documento a ogni giro, e passa uno per giro. MongoDB fa la
stessa serializzazione, ma con un lucchetto in memoria che costa decine di microsecondi, non millisecondi.

Giudizio: corretto e semplice, ma il caso 1 è inaccettabile per un database che vuole essere usato
come MongoDB: un contatore caldo è il primo pattern che un'applicazione prova.

### Opzione (b): scritture "cieche" applicate in ordine di registro, senza validazione

Idea: una scrittura che non dipende da ciò che ha letto (put, delete, `$inc`) viene accettata dal
leader senza controllo, messa in ordine di registro, e il suo effetto viene calcolato da chi compone la
lettura: valore nell'SST, poi i record della finestra applicati uno dopo l'altro in ordine di posizione.
`$inc` più `$inc` commutano; `$set` dopo `$set` vince l'ultimo in ordine di registro, come in
un'esecuzione seriale. Il caso 1 diventa: 100 record nel registro, zero rifiuti, un solo giro per
tutti (STIMATO: 3-4 ms per ciascuno, in parallelo). La capacità sulla chiave calda è quella del
registro, non più un commit per giro.

Il problema di (b) presa alla lettera è la risposta al client, che MongoDB definisce in base allo
stato del documento nel momento in cui la scrittura si applica:

- `updateOne({_id: x}, {$inc: {n: 1}})` risponde `matchedCount: 1` se il documento esiste. Se tra `S`
  e `C` qualcuno cancella il documento, la scrittura cieca si applicherebbe a un documento che non c'è
  più (e non deve farlo rinascere: non è un upsert). Il nodo d'ingresso avrebbe risposto "1 trovato,
  1 modificato" per un'operazione che in realtà non ha fatto nulla: una bugia osservabile.
- `insertOne` deve fallire se `_id` esiste: non è mai cieco rispetto all'esistenza.
- `deleteOne({_id: x})` risponde `deletedCount: 1` o `0` a seconda che il documento esista.

Quindi "cieco rispetto al valore" non vuol dire "cieco rispetto all'esistenza". Serve una
distinzione, ed è quella dell'opzione (c).

### Opzione (c), raccomandata: dipendenze dichiarate, con due modi

Ogni record porta, oltre a `S`, al proprio id e alle chiavi scritte, un elenco di **dipendenze**: le
chiavi di cui il nodo d'ingresso si è fidato quando ha preparato la scrittura, ciascuna con un modo:

- modo `esiste`: la scrittura si è fidata solo del fatto che il documento esistesse (o non
  esistesse) a `S`, non del suo contenuto;
- modo `valore`: la scrittura si è fidata del contenuto del documento (o della chiave d'indice) a `S`.

Ogni record classifica inoltre ciascuna chiave che scrive come scrittura **strutturale** (crea, cancella
o sostituisce per intero: insert, delete, replace, l'inserimento di un upsert) oppure **delta**
(modifica di campi con operatori: `$inc`, `$set`, `$unset`, `$push`, ... su un documento che esiste).

Il leader tiene, per ogni chiave toccata nella finestra, due posizioni: `ultimaStrutturale(k)` e
`ultimaQualsiasi(k)`. La regola di validazione è:

- una dipendenza `(k, esiste)` fallisce se `ultimaStrutturale(k) > S`;
- una dipendenza `(k, valore)` fallisce se `ultimaQualsiasi(k) > S`;
- se `S` è più vecchia dell'inizio della finestra il leader non può validare e rifiuta ("S troppo
  vecchia": il nodo d'ingresso rilegge).

Se nessuna dipendenza fallisce, il record ottiene una posizione e aggiorna le due mappe per ogni chiave
che scrive (la strutturale solo se la scrittura è strutturale). Tutti i record, anche quelli senza
dipendenze, aggiornano le mappe: altrimenti chi valida non li vedrebbe.

Un `$inc` su un documento per `_id` ha una sola dipendenza, `(_id, esiste)`: conflitta solo con una
cancellazione, un inserimento o una sostituzione dello stesso documento avvenuti dopo `S`, mai con un
altro `$inc` o `$set`. Il caso 1 passa in un giro con zero rifiuti (STIMATO), e la risposta al client
è giusta: se qualcuno ha cancellato il documento dopo `S`, il `$inc` viene rifiutato, rilegge, non
trova il documento e risponde `matchedCount: 0`, come avrebbe fatto MongoDB con l'ordine
"cancellazione, poi update".

Il caso dell'upsert caldo, `updateOne({_id: k}, {$inc: {n: 1}}, {upsert: true})`, il più comune per i
contatori: la decisione "inserisci o modifica" dipende solo dall'esistenza, quindi anche qui la
dipendenza è `(k, esiste)`. Il primo client inserisce (scrittura strutturale); i client che avevano
preparato un inserimento con `S` precedente vengono rifiutati una volta sola, rileggono, trovano il
documento e ripartono come delta. Dal secondo giro in avanti nessun rifiuto.

Il caso 2 (indice unico) resta validato come in (a), sulla chiave d'indice: dipendenza
`(chiave d'indice, valore)` per ogni valore unico che il record inserisce o cambia. Costo e conflitti
come in (a): pochi e giusti.

Il caso 3 (`findAndModify` in ciclo) resta validato: il client legge il documento restituito, quindi la
dipendenza è `(_id, valore)`. Qui (c) non aiuta e non può aiutare: la semantica è intrinsecamente
seriale. Mitigazioni, in ordine di costo:

1. Coda per chiave nel nodo d'ingresso: se c'è già una scrittura validata in volo sulla stessa chiave
   dallo stesso nodo d'ingresso, la successiva aspetta l'esito e si prepara sulla finestra aggiornata
   invece di andare al leader e fallire. Zero lavoro sprecato dentro un nodo d'ingresso; la capacità
   sulla chiave calda resta un commit per giro (STIMATO 250-330/s), ma senza i 5.000 tentativi inutili.
   Tra nodi d'ingresso diversi la contesa resta (al massimo un rifiuto per nodo d'ingresso per giro).
2. Scelta casuale tra i primi `k` candidati della query, quando la query non impone un ordine
   (`findOneAndUpdate` senza `sort`): i worker si disperdono su documenti diversi e i conflitti
   scendono di un fattore circa `k` (STIMATO).
3. Documentare che i contatori vanno fatti con `$inc` (delta cieco) e non con "leggi, somma, scrivi".

Opzione (c2), considerata e rinviata: "ri-esecuzione sul leader". Per un `findAndModify` su un
documento per `_id`, il leader, che ha la finestra, potrebbe ri-eseguire filtro e update sul valore
corrente invece di rifiutare. Rinviata perché mette la semantica degli update di MongoDB nel punto più
caldo del sistema, e perché la finestra del leader contiene delta, non valori interi: ricostruire il
valore corrente richiede l'SST, che il leader non ha. Resta una possibile ottimizzazione futura, non
necessaria per la correttezza.

### Quali comandi MongoDB cadono in quale classe

Classe "delta cieco", dipendenza solo `(chiave, esiste)`, mai in conflitto con altri delta:

- `updateOne` / `updateMany` / `bulkWrite` di tipo update, con filtro solo su `_id` (anche `_id: {$in: [...]}`),
  senza `upsert` e senza indici unici toccati, con soli operatori di modifica: `$inc`, `$mul`, `$set`,
  `$unset`, `$min`, `$max`, `$rename`, `$currentDate`, `$bit`, `$push`, `$addToSet`, `$pop`, `$pull`,
  `$pullAll`, `$setOnInsert` (ignorato senza upsert). Il filtro per `_id` ha letto solo l'esistenza.
- lo stesso con `upsert: true`: se il documento esisteva a `S` è un delta; se non esisteva è un
  inserimento strutturale. In entrambi i casi la dipendenza è `(_id, esiste)`.
- `deleteOne` / `deleteMany` con filtro solo su `_id`: scrittura strutturale (tombstone), dipendenza
  `(_id, esiste)` (serve per rispondere `deletedCount` giusto).
- `replaceOne` con filtro solo su `_id`: scrittura strutturale, dipendenza `(_id, esiste)`.
- `insertOne` / `insertMany`: scrittura strutturale, dipendenza `(_id, esiste)` (deve essere assente)
  più, se la collezione ha indici unici oltre a `_id`, una dipendenza `(chiave d'indice, valore)` per
  ciascuno. Due inserimenti di `_id` diversi non conflittano mai.

Classe "validata", almeno una dipendenza `(chiave, valore)`:

- `findAndModify` in tutte le forme (`findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`):
  il client riceve il documento, quindi dipende dal valore: `(_id del documento scelto, valore)`.
- `updateOne` / `updateMany` / `deleteOne` / `deleteMany` / `replaceOne` con un filtro su campi diversi da
  `_id` (il pattern "aggiorna se `version = 7`" e tutte le query con indice): dipendenza
  `(_id di ogni documento scelto, valore)`. Per `updateMany` e `deleteMany` si valida documento per
  documento, come fa MongoDB (che non promette atomicità tra documenti e ricontrolla il filtro su ogni
  documento al momento della scrittura); i documenti rifiutati vengono ritentati da soli.
- update con l'operatore posizionale `$` o con `arrayFilters` e filtro su un campo dell'array: il
  filtro ha letto il contenuto, quindi `(_id, valore)`.
- ogni scrittura che tocca una chiave d'indice unico: `(chiave d'indice vecchia e nuova, valore)`.
- transazioni multi-documento (`commitTransaction`): dipendenza `(chiave, valore)` per ogni chiave
  scritta, nessuna dipendenza per le chiavi solo lette. È l'isolamento a istantanea di MongoDB: un
  conflitto scrittura-scrittura dopo l'istantanea dà `WriteConflict`; una lettura non protegge. Un
  livello serializzabile (dipendenza `valore` anche sulle chiavi lette) è possibile a costo di più
  rifiuti; non è richiesto per la compatibilità e non lo adottiamo ora.
- scritture su collezioni con un validatore di schema (`validator`) e scritture su documenti vicini al
  limite di 16 MB: il nodo d'ingresso controlla validatore e dimensione a `S`, quindi il risultato
  dipende dal valore: `(_id, valore)`. Nelle collezioni senza validatore e sui documenti piccoli il
  controllo a `S` basta, con l'approssimazione descritta sotto.

Fuori classe (strada dei metadati, non del registro dati): `createIndex` con `unique: true` su una
collezione piena, `drop`, `rename`, `create`: operazioni rare, serializzate per collezione da un record
di metadati con dipendenza `valore` sulla chiave della collezione. Non trattate in questa fase.

### Scostamenti da MongoDB accettati con la scelta (c), da scrivere nella compatibilità

- `modifiedCount` di un delta cieco è calcolato a `S`. Se tra `S` e `C` un altro delta ha portato il
  campo allo stesso valore che il nostro `$set` imposta, MongoDB risponderebbe `modifiedCount: 0` e noi
  `1`. L'effetto sui dati è identico; cambia solo il contatore nella risposta.
- Il limite di 16 MB e il validatore di schema sono controllati a `S` per i delta ciechi su documenti
  lontani dal limite e in collezioni senza validatore. Nel caso raro in cui due delta concorrenti
  portino il documento sopra il limite, il documento supera il limite; il nodo d'ingresso lo riporta
  sotto la soglia prudenziale alla modifica successiva. Da misurare se la soglia (proposta: 15 MB,
  STIMATO) basta.

### Raccomandazione

Adottare (c). Costo in più rispetto ad (a): un bit per chiave scritta (strutturale o delta), un modo
per dipendenza, e una seconda posizione per chiave nella mappa del leader. Beneficio: il caso 1 e
l'upsert caldo passano senza rifiuti; indici unici, `findAndModify` e transazioni tengono la semantica
di MongoDB. La specifica `ValidatingLog.tla` modella esattamente questa regola, con le due mappe e la
loro ricostruzione al cambio di leader.

## B. Letture fresche

### Che cosa può andare storto

Una lettura a `S` prende dall'SST il valore fino a `M` e da una replica i record in `(M, S]`. Due errori
possibili: servire dati più vecchi di quanto il client ha già visto (regressione), e servire un record
che non è quello committato a quella posizione. Il secondo è il più insidioso. Un vecchio leader può
avere scritto un record `X` alla posizione `p` solo su se stesso, poi morire; il nuovo leader, che non ha
`X`, committa un altro record `Y` alla stessa posizione `p` su due repliche. La vecchia replica (il
vecchio leader) è viva ma isolata: ha ancora `X` a `p`. Se un nodo d'ingresso le chiede "dammi i record
fino a `S = p`" e la replica risponde perché la sua coda arriva a `p`, il nodo d'ingresso serve `X`,
che non è mai stato committato. Il "la replica ha la posizione" non basta.

### Quale `S` sceglie il nodo d'ingresso

Tre posizioni contano:

1. `S_cliente`: la posizione più alta che la sessione del client ha visto, restituita in ogni risposta
   (sia la `C` delle sue scritture, sia la `S` delle sue letture). È il gettone di sessione, l'analogo
   di `operationTime` / `afterClusterTime` di MongoDB. Il driver lo rimanda con ogni richiesta.
2. `S_ingresso`: la posizione confermata più alta che il nodo d'ingresso conosce. La impara da ogni
   risposta del leader ai suoi commit (che porta la confermata corrente) e dalle risposte delle
   repliche alle sue letture (ogni risposta porta la `confermata_locale` della replica).
3. `S_leader`: la confermata corrente chiesta al leader adesso (lettura "ReadIndex").

Regola di default (letture con `readConcern` `local` o `majority`, cioè quasi tutte):
`S = max(S_cliente, S_ingresso)`. Garantisce: leggi le tue scritture, letture monotone, scritture
monotone, letture causali tra client che si passano il gettone (come le sessioni causali di MongoDB).
La freschezza rispetto agli altri client è limitata dal ritardo con cui il nodo d'ingresso impara la
confermata: una frazione del periodo di heartbeat in condizioni normali (STIMATO: pochi millisecondi).

Limite di anzianità: un nodo d'ingresso che non ha avuto contatto col leader da più di `T_max`
(proposta: 1 s, STIMATO) non si fida più della propria `S_ingresso` e chiede `S_leader` prima di
servire. Così anche un nodo d'ingresso isolato non serve dati più vecchi di `T_max` più un giro.

Regola per `readConcern: linearizable` (e per chi chiede esplicitamente la lettura fresca):
`S = max(S_cliente, S_leader)`. Il leader risponde con la propria confermata solo dopo aver avuto, dopo
la richiesta, una risposta di heartbeat da almeno un'altra replica (così un leader deposto non risponde:
è il ReadIndex di Raft). Costo: un giro al leader, ammortizzabile raggruppando le richieste pendenti
di un nodo d'ingresso (STIMATO: un giro intra-regione, sotto il millisecondo, per un lotto di letture).

In tutti i casi `S` è una posizione confermata: il nodo d'ingresso la impara solo da fonti che
riportano posizioni confermate, e il leader risponde a un commit solo dopo la conferma. Le posizioni
confermate non vengono mai riassegnate (una posizione non confermata può esserlo: il record `X` di
sopra), quindi un gettone è sempre valido.

### Che cosa fa la replica, anche se isolata ma viva

La replica tiene `confermata_locale`: l'ultima posizione confermata che il leader le ha comunicato,
e la aggiorna solo dentro una replicazione che ha superato il controllo di coerenza del prefisso (come
il `commitIndex` di un follower Raft). Per costruzione i record della replica fino a `confermata_locale`
sono esattamente quelli committati.

Richiesta di lettura: "record delle chiavi `K` con posizione in `(M, S]`". La replica risponde solo se:

- `S <= confermata_locale` (altrimenti risponde "non ho ancora `S`" con la propria `confermata_locale`), e
- `M >= inizio della propria finestra` (altrimenti "ho scartato sotto `M`": il nodo d'ingresso deve
  prima aggiornare il manifest e quindi `M`).

Se entrambe valgono restituisce i record e la propria `confermata_locale`. La risposta è giusta anche
se la replica è isolata da un'ora: la sua `confermata_locale` è ferma, quindi può servire solo `S` che
erano già confermate quando è stata isolata, e per quelle posizioni i record non cambiano più. La
replica isolata dell'esempio sopra ha `confermata_locale < p` e rifiuta: il nodo d'ingresso prova
un'altra replica. La `S` la decide il nodo d'ingresso, mai la replica; la replica può solo dire sì o no.

Quando la replica dice "non ho `S`", il nodo d'ingresso prova le altre due; se nessuna ha `S`
(possibile per pochi millisecondi dopo un commit, o se il nodo d'ingresso ha un gettone cliente
appena restituito da un altro nodo d'ingresso) aspetta e riprova con un tempo massimo, poi risponde
con un errore ritentabile.

Le scritture usano la stessa `S` della lettura con cui sono state preparate. Se `S` è vecchia la
scrittura rischia solo più rifiuti (la validazione copre `(S, C]`), mai un esito sbagliato; se è più
vecchia della finestra il leader la rifiuta e il nodo d'ingresso rilegge.

### Riassunto delle scelte di B

- `S` di default: massimo tra gettone del client e confermata nota al nodo d'ingresso, con un limite
  di anzianità oltre il quale si chiede al leader.
- `S` lineare su richiesta: confermata del leader dopo un giro di heartbeat (ReadIndex).
- La replica serve `(M, S]` solo se `S <= confermata_locale` e `M` è dentro la sua finestra. Mai in
  base alla lunghezza della propria coda.
- Ogni risposta al client porta la posizione vista, e il driver la rimanda.

La specifica delle letture (passo 4) modella la regola della replica e il controllo negativo "servi se
hai la posizione in coda", che deve produrre esattamente il controesempio descritto sopra.
