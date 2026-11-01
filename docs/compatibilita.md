# Compatibilità di Fenvix con MongoDB

Ultima misura: 1 novembre 2026. Motore: `fenvix-engine`, ramo `main`, commit `658c724`. Versione di riferimento:
MongoDB 8.0. Tutti i numeri di questa pagina sono misurati su quella versione del motore; i dettagli e i file grezzi
sono in `compat/misura/`.

Questa pagina dice cosa funziona, cosa no e come lo abbiamo misurato. Dove non abbiamo misurato lo diciamo.

## In breve

- Fenvix parla il protocollo di MongoDB: i driver ufficiali si collegano senza modifiche. Abbiamo provato Node.js
  (driver 7.8.0), Python (PyMongo 4.19.0), Go (driver 2.4.0) e la shell mongosh 2.14.1: connessione, letture e
  scritture, aggregazioni, transazioni, indici e change stream funzionano con tutti e quattro.
- Sui test ufficiali di MongoDB (formato "unified" delle specifiche dei driver) passano 626 test su 697 eseguiti,
  cioè l'89,8%. Senza contare il comando `bulkWrite` tra collezioni, novità della 8.0 che non abbiamo ancora, passa il
  94,4%.
- Su 108 comandi e operatori tra i più usati, 100 si comportano come documentato da MongoDB 8.0, 3 funzionano in
  parte, 2 non sono supportati e 3 danno un risultato sbagliato.
- `mongodump` e `mongorestore` (100.13.0) funzionano: un database di prova di 20.124 documenti è uscito e rientrato
  con tutti i documenti identici byte per byte.
- Abbiamo trovato due errori che toccano i dati, descritti sotto. Vanno corretti prima del lancio.

## Come abbiamo misurato

**Test scritti da MongoDB, non da noi.** Ogni driver ufficiale di MongoDB deve passare una raccolta di test pubblici,
scritti in un formato comune ("unified test format", repository `github.com/mongodb/specifications`). Abbiamo fatto
girare quei test contro Fenvix, con PyMongo 4.19.0 e con la copia dei test che PyMongo 4.19.0 usa. Il runner è in
`compat/misura/unified_runner.py`: è piccolo e leggibile, chiunque può rifare la misura.

**Ogni test ha un esito: passato, fallito o saltato, e ogni test saltato ha un motivo.** Un test si salta quando
chiede qualcosa che la misura non può dare: uno sharded cluster, una versione di MongoDB più vecchia della 8.0, un
server con autenticazione, oppure `configureFailPoint`, un comando di prova che serve a simulare guasti di rete (in
MongoDB va acceso a mano e Atlas non lo offre). I numeri in percentuale sono sempre calcolati sui test eseguiti.

**Una prova a mano per ogni problema.** Ogni gruppo di test falliti è stato riprodotto con una richiesta diretta, per
essere sicuri che il problema sia del motore e non del runner.

**Driver e strumenti veri.** Un piccolo programma per ogni driver, mongosh, mongodump e mongorestore.

## Risultati per area dei test ufficiali

- **Lettura e scrittura (CRUD)**: 306 passati su 345 eseguiti (88,7%). Dei 39 falliti, 29 sono il comando `bulkWrite`
  tra collezioni della 8.0; 4 sono l'opzione `sort` di `updateOne`/`replaceOne` (8.0), che è ignorata.
- **Transazioni**: 107 su 113 (94,7%). Fallisce la creazione di collezioni e indici dentro una transazione, e c'è un
  errore che perde dati (sotto).
- **Change stream**: 39 su 45 (86,7%). Mancano il campo `disambiguatedPaths` e alcuni controlli di errore del server.
- **Sessioni**: 5 su 19 (26,3%). Le sessioni "snapshot" non funzionano come in MongoDB (sotto).
- **GridFS**: 39 su 39 (100%).
- **Monitoraggio dei comandi**: 29 su 32 (90,6%).
- **API versionata (Stable API)**: 36 su 37 (97,3%).
- **Consistenza causale**: 16 su 17 (94,1%).
- **Retryable reads**: 17 su 17; **retryable writes**: 7 su 7. Attenzione: qui quasi tutti i test (498 su 522) sono
  saltati, quasi sempre perché simulano guasti con `configureFailPoint` (439) o chiedono l'autenticazione (52). Il comportamento dopo un guasto non è misurato.
- **Run command**: 9 su 9; **read e write concern**: 10 su 10; **gestione collezioni**: 6 su 7.
- **Indici**: i test ufficiali di quest'area riguardano solo gli indici di ricerca di Atlas, che non abbiamo misurato.
  Gli indici normali sono provati nei comandi più usati, qui sotto.

In tutto 1.592 test: 626 passati, 71 falliti, 895 saltati.

## Comandi e operatori più usati

Abbiamo provato 108 funzioni, ognuna confrontata con il risultato che il manuale di MongoDB 8.0 documenta.

**Funzionano (100):** tutti gli operatori di query comuni (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`,
`$nin`, `$and`, `$or`, `$nor`, `$not`, `$exists`, `$type`, `$regex`, `$expr`, `$mod`, `$all`, `$elemMatch`,
`$size`, `$jsonSchema`, `$where`, operatori sui bit, `$text`, `$near`, `$geoWithin`); proiezioni, ordinamento,
collation, cursori, `countDocuments`, `distinct`, `explain`; tutti gli operatori di update (`$set`, `$unset`, `$inc`,
`$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$setOnInsert`, `$push` con modificatori, `$addToSet`, `$pop`,
`$pull`, `$pullAll`, `$bit`, posizionali `$`, `$[]`, `$[id]`, update con pipeline); insert, update, delete,
findAndModify, bulkWrite di una collezione, errore 11000 sui duplicati; aggregazione (`$match`, `$group` con gli
accumulatori, `$project`, `$addFields`, `$sort`, `$limit`, `$skip`, `$unwind`, `$count`, `$sortByCount`, `$facet`,
`$bucket`, `$replaceRoot`, `$sample`, `$lookup` semplice e con pipeline, `$graphLookup`, `$unionWith`, `$out` e
`$merge` su una collezione, `$setWindowFields`, `$densify`, le espressioni comuni, `mapReduce`, change stream);
indici (semplici, composti, multikey, unique, parziali, sparse, TTL, testo, 2dsphere, hashed, wildcard, hint,
collMod, Atlas Search di base); amministrazione (create con validator e capped, view, timeseries, rename, statistiche,
serverStatus, currentOp, getMore e killCursors); transazioni (commit, abort, withTransaction, letture causali);
utenti e ruoli. Con il server in modalità `--auth`: login SCRAM-SHA-256 e SCRAM-SHA-1, password sbagliata rifiutata
(errore 18), comandi senza login rifiutati (errore 13), ruolo `read` che legge ma non scrive.

**Funzionano in parte (3):**
- `buildInfo` e `hello` dicono versione 7.0.45: i driver quindi spengono da soli le funzioni della 8.0.
- `dropDatabase` cancella dati e collezioni, ma il database resta nell'elenco di `listDatabases`.
- `killSessions` e `killAllSessions` rispondono ok ma non chiudono le transazioni aperte, che tengono i loro lock fino
  alla scadenza (60 secondi). `endSessions` invece le chiude.

**Non supportati (2):** il comando `bulkWrite` tra collezioni (8.0); `$out` e `$merge` in un aggregate lanciato sul
database (per esempio dopo `$documents`).

**Risultato sbagliato (3):** `updateOne` con `sort` (8.0) modifica un documento diverso da quello indicato dal sort;
le letture snapshot vedono scritture più recenti dello snapshot; `let` del comando aggregate rifiuta
`{$literal: "$bar"}`.

## Errori che toccano i dati

1. **Transazioni: scritture perse al commit.** Dentro una transazione, se un `findOneAndUpdate`,
   `findOneAndReplace` o `findOneAndDelete` non trova nessun documento, le scritture fatte prima nella stessa
   transazione spariscono al commit, e il commit risponde ok. Le scritture fatte dopo restano. Riproduzione:
   `compat/misura/repro_txn_lost_write.py`.
2. **`updateOne` e `replaceOne` con `sort`.** L'opzione `sort` (novità della 8.0) è accettata ma ignorata: viene
   modificato il primo documento trovato invece di quello scelto dal sort. Vale anche dentro `bulkWrite`.

Nessuno dei test ha fatto cadere il server.

## La porta d'uscita: mongodump e mongorestore

Abbiamo riempito un database con 20.000 documenti che usano tutti i tipi BSON comuni, dieci indici di tipi diversi
(unique, composto, TTL, parziale, sparse, testo, 2dsphere, hashed, wildcard, con collation), una collezione con
validator, una capped, una view e un file GridFS da 3 MB. Poi `mongodump` (in cartella e come archivio gzip) e
`mongorestore` in un database nuovo.

Risultato: dump e restore finiscono senza errori; tutti i 20.124 documenti sono identici byte per byte; validator,
collezione capped, view e file GridFS sono identici; gli indici sono identici tranne uno. L'indice di testo, rimesso
dentro Fenvix, prende un peso in più su un campo `_fts` (nessun effetto sui documenti normali: le ricerche danno gli
stessi risultati).

Non abbiamo potuto provare il passaggio verso un MongoDB 8.0 vero e quello da MongoDB 8.0 verso Fenvix: in questo
ambiente non c'era un mongod 8.0. I file prodotti da mongodump hanno il formato standard e si rileggono
correttamente, ma la prova con un MongoDB vero va ancora fatta.

## Cosa non abbiamo misurato

- Il comportamento dopo guasti di rete (retry automatici): 571 test ufficiali richiedono `configureFailPoint`.
- Sharded cluster e load balancer (Fenvix non li espone).
- Il driver Java, e l'ultima versione del driver Go (2.9.2).
- Il passaggio dei dati da e verso un MongoDB 8.0 vero.
- Prestazioni e prezzi: questa pagina parla solo di comportamento.
