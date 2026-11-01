# Risultati grezzi dei test ufficiali (formato unified)

Generato da `classifica.py` a partire da `results/*.jsonl`. Numeri MISURATI su questa macchina.

In tutto: 1592 test, 626 passati, 71 falliti, 895 saltati. Sui test eseguiti passa il 89.8%.

## Per area

- **causal-consistency**: 16 passati, 1 falliti, 1 saltati (94.1% dei test eseguiti).
  Falliti: 1 mancante.
  Saltati: 1 runner.
- **change-streams**: 39 passati, 6 falliti, 44 saltati (86.7% dei test eseguiti).
  Falliti: 4 diverso, 2 mancante.
  Saltati: 2 req:minServerVersion, 21 failpoint, 19 req:maxServerVersion, 2 req:topology.
- **collezioni**: 6 passati, 1 falliti, 0 saltati (85.7% dei test eseguiti).
  Falliti: 1 diverso.
- **crud**: 306 passati, 39 falliti, 92 saltati (88.7% dei test eseguiti).
  Falliti: 4 bug-dati, 32 mancante, 3 diverso.
  Saltati: 9 failpoint, 78 req:maxServerVersion, 5 runner.
- **gridfs**: 39 passati, 0 falliti, 6 saltati (100.0% dei test eseguiti).
  Saltati: 6 runner.
- **indici**: 0 passati, 0 falliti, 15 saltati (nessun test eseguito).
  Saltati: 15 req:atlas-search.
- **monitoraggio-comandi**: 29 passati, 3 falliti, 33 saltati (90.6% dei test eseguiti).
  Falliti: 1 mancante, 2 diverso.
  Saltati: 4 req:maxServerVersion, 6 failpoint, 22 runner, 1 req:topology.
- **read-write-concern**: 10 passati, 0 falliti, 1 saltati (100.0% dei test eseguiti).
  Saltati: 1 runner.
- **retryable-reads**: 17 passati, 0 falliti, 363 saltati (100.0% dei test eseguiti).
  Saltati: 327 failpoint, 4 runner, 32 req:auth.
- **retryable-writes**: 7 passati, 0 falliti, 135 saltati (100.0% dei test eseguiti).
  Saltati: 112 failpoint, 3 req:maxServerVersion, 20 req:auth.
- **run-command**: 9 passati, 0 falliti, 12 saltati (100.0% dei test eseguiti).
  Saltati: 4 req:topology, 1 failpoint, 7 runner.
- **sessioni**: 5 passati, 14 falliti, 15 saltati (26.3% dei test eseguiti).
  Falliti: 5 bug-risposta, 9 diverso.
  Saltati: 3 req:topology, 3 req:maxServerVersion, 9 failpoint.
- **transazioni**: 107 passati, 6 falliti, 174 saltati (94.7% dei test eseguiti).
  Falliti: 5 mancante, 1 bug-dati.
  Saltati: 86 failpoint, 79 req:topology, 1 runner, 6 req:secondario, 2 req:auth.
- **versioned-api**: 36 passati, 1 falliti, 4 saltati (97.3% dei test eseguiti).
  Falliti: 1 mancante.
  Saltati: 4 req:serverParameter.

## Fallimenti, dal più grave

### Bug che toccano i dati

- transazione: un findAndModify che non trova niente cancella al commit le scritture precedenti della stessa transazione (il commit risponde ok) (1 test)
  - transazioni/findOneAndDelete.json: findOneAndDelete
- updateOne/replaceOne con sort (novità 8.0): il sort è ignorato e viene modificato un altro documento (4 test)
  - crud/bulkWrite-updateOne-sort.json: BulkWrite updateOne with sort option
  - crud/bulkWrite-replaceOne-sort.json: BulkWrite replaceOne with sort option
  - crud/replaceOne-sort.json: ReplaceOne with sort option
  - crud/updateOne-sort.json: UpdateOne with sort option

### Bug di risposta (dati giusti, risposta sbagliata)

- sessioni snapshot: le letture vedono scritture fatte dopo l'inizio dello snapshot (5 test)
  - sessioni/snapshot-sessions.json: Find operation with snapshot
  - sessioni/snapshot-sessions.json: Distinct operation with snapshot
  - sessioni/snapshot-sessions.json: Aggregate operation with snapshot
  - sessioni/snapshot-sessions.json: Mixed operation with snapshot
  - sessioni/snapshot-sessions.json: Write commands with snapshot session do not affect snapshot reads

### Funzionalità mancanti

- $out e $merge in un aggregate a livello di database (es. dopo $documents) (3 test)
  - crud/db-aggregate-write-readPreference.json: Database-level aggregate with $out includes read preference for 5.0+ server
  - crud/db-aggregate-write-readPreference.json: Database-level aggregate with $merge includes read preference for 5.0+ server
  - crud/aggregate-merge-errorResponse.json: aggregate $merge DuplicateKey error is accessible
- bulkWrite tra collezioni (comando bulkWrite di MongoDB 8.0): il server non ha il comando e si dichiara 7.0, quindi i driver lo rifiutano prima di inviarlo (34 test)
  - causal-consistency/causal-consistency-clientBulkWrite.json: clientBulkWrite includes afterClusterTime in causally consistent session
  - crud/client-bulkWrite-errors.json: an individual operation fails during an ordered bulkWrite
  - crud/client-bulkWrite-errors.json: an individual operation fails during an unordered bulkWrite
  - crud/client-bulkWrite-errors.json: detailed results are omitted from error when verboseResults is false
  - crud/client-bulkWrite-errors.json: a bulk write with only errors does not report a partial result
  - crud/client-bulkWrite-updateOne-sort.json: client bulkWrite updateOne with sort option
  - crud/create-null-ids.json: inserting _id with type null via clientBulkWrite
  - crud/client-bulkWrite-results.json: client bulkWrite with verboseResults: true returns detailed results
  - crud/client-bulkWrite-results.json: client bulkWrite with verboseResults: false omits detailed results
  - crud/client-bulkWrite-results.json: client bulkWrite defaults to verboseResults: false
  - crud/client-bulkWrite-mixed-namespaces.json: client bulkWrite with mixed namespaces
  - crud/client-bulkWrite-ordered.json: client bulkWrite with ordered: false
  - crud/client-bulkWrite-ordered.json: client bulkWrite with ordered: true
  - crud/client-bulkWrite-ordered.json: client bulkWrite defaults to ordered: true
  - crud/client-bulkWrite-delete-options.json: client bulk write delete with collation
  - crud/client-bulkWrite-delete-options.json: client bulk write delete with hint
  - crud/client-bulkWrite-update-options.json: client bulkWrite update with arrayFilters
  - crud/client-bulkWrite-update-options.json: client bulkWrite update with collation
  - crud/client-bulkWrite-update-options.json: client bulkWrite update with hint
  - crud/client-bulkWrite-update-options.json: client bulkWrite update with upsert
  - crud/client-bulkWrite-update-pipeline.json: client bulkWrite updateOne with pipeline
  - crud/client-bulkWrite-update-pipeline.json: client bulkWrite updateMany with pipeline
  - crud/client-bulkWrite-options.json: client bulkWrite comment
  - crud/client-bulkWrite-options.json: client bulkWrite bypassDocumentValidation
  - crud/client-bulkWrite-options.json: client bulkWrite let
  - crud/client-bulkWrite-options.json: client bulkWrite bypassDocumentValidation: false is sent
  - crud/client-bulkWrite-options.json: client bulkWrite writeConcern
  - crud/client-bulkWrite-options.json: client bulkWrite inherits writeConcern from client
  - crud/client-bulkWrite-options.json: client bulkWrite writeConcern option overrides client writeConcern
  - crud/client-bulkWrite-replaceOne-sort.json: client bulkWrite replaceOne with sort option
  - monitoraggio-comandi/unacknowledged-client-bulkWrite.json: A successful mixed client bulkWrite
  - transazioni/client-bulkWrite.json: client bulkWrite in a transaction
  - transazioni/client-bulkWrite.json: client writeConcern ignored for client bulkWrite in transaction
  - versioned-api/crud-api-version-1.json: client bulkWrite appends declared API version
- campo updateDescription.disambiguatedPaths degli eventi di change stream (2 test)
  - change-streams/change-streams-disambiguatedPaths.json: disambiguatedPaths is present on updateDescription when an ambiguous path is present
  - change-streams/change-streams-disambiguatedPaths.json: disambiguatedPaths returns array indices as integers
- creare collezioni e indici dentro una transazione (MongoDB lo permette dalla 4.4) (3 test)
  - transazioni/create-collection.json: explicitly create collection using create command
  - transazioni/create-index.json: create index on a non-existing collection
  - transazioni/create-index.json: create index on a collection created within the same transaction

### Comportamenti diversi

- $out con readConcern linearizable: accettato (MongoDB dà errore) (1 test)
  - crud/aggregate-out-readConcern.json: readConcern linearizable with out stage
- _id con chiave che inizia per $: accettato (MongoDB lo rifiuta) (1 test)
  - crud/insertOne-dots_and_dollars.json: Inserting document with dollar-prefixed key in _id yields server-side error
- change stream: array troncato riportato come updatedFields invece di truncatedArrays (1 test)
  - change-streams/change-streams.json: Test array truncation
- change stream: stadio non valido nel pipeline accettato all'apertura (1 test)
  - change-streams/change-streams-errors.json: Change Stream should error when an invalid aggregation stage is passed in
- change stream: togliere _id con $project non dà l'errore 280 del server (2 test)
  - change-streams/change-streams-errors.json: Change Stream should error when _id is projected out
  - change-streams/change-streams.json: Test server error on projecting out _id
- collezione timeseries: _id duplicati rifiutati (in MongoDB le timeseries non hanno indice unico su _id) (1 test)
  - collezioni/timeseries-collection.json: insertMany with duplicate ids
- find con min/max e hint {_id: 1}: il server dice che manca l'hint (1 test)
  - monitoraggio-comandi/find.json: A successful find with options
- find con returnKey: documento vuoto invece della chiave dell'indice usato (1 test)
  - monitoraggio-comandi/find.json: A successful find with showRecordId and returnKey
- let del comando aggregate: {$literal: '$bar'} è rifiutato come accesso a un campo (1 test)
  - crud/aggregate-let.json: Aggregate with let option
- sessioni snapshot: le scritture e i comandi di elenco con readConcern snapshot sono accettati (MongoDB li rifiuta) (9 test)
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on insertOne with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on insertMany with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on deleteOne with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on updateOne with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on findOneAndUpdate with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on listDatabases with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on listCollections with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on listIndexes with snapshot
  - sessioni/snapshot-sessions-unsupported-ops.json: Server returns an error on runCommand with snapshot

## Perché i test saltati sono saltati

- `failpoint`: richiedono configureFailPoint, un aggancio di prova del server per simulare guasti (in mongod va acceso con enableTestCommands, in Atlas non c'è): il motore non lo ha.
- `req:maxServerVersion`: valgono solo per versioni di MongoDB più vecchie della 8.0.
- `req:minServerVersion`: valgono solo per versioni più nuove della 8.0.
- `req:topology`: richiedono uno sharded cluster o un load balancer.
- `req:auth`: richiedono il server con autenticazione accesa (qui spenta).
- `req:secondario`: richiedono un nodo secondario: il nostro replica set ha un solo membro.
- `req:serverParameter`: richiedono un parametro del server (per esempio acceptApiVersion2).
- `req:atlas-search`: indici di ricerca di Atlas (Atlas Search): fuori dal perimetro.
- `runner`: operazioni che il nostro runner non implementa (limite della misura, non del motore).
- `spec-skip`: segnati da saltare nel file stesso.
