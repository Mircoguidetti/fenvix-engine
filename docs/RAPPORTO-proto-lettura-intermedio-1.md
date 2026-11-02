# Rapporto intermedio 1: proto-lettura

Branch `proto-lettura`, creato da `origin/main`. Codice in `proto/lettura` (crate Rust autonomo, fuori dal workspace
del motore). Tutto in locale: MinIO compilato dai sorgenti (commit 7aac2a2c5b7c, febbraio 2026), nessun cloud.

## A che punto sono

Il codice dei passi 1-5 è scritto e provato in piccolo. Le due prove lunghe girano adesso, in parallelo:

- passo 1 (scarico da chiunque, 3 worker, uccisioni casuali, 1 ora);
- passo 5 (costo S3 per database piccolo, tre varianti, 1 ora ciascuna, bucket separati).

I passi 2, 3 e 4 misurano tempi: li faccio dopo, a macchina libera. Il passo 6 è solo progetto.

## Cosa c'è nel codice

- Un registro simulato dietro l'interfaccia comune (in memoria, con log su file per la verifica, servito anche via
  TCP così i worker possono essere processi separati da uccidere).
- Un formato SST proprio, sullo schema di SlateDB (blocchi, indice, filtro di Bloom, versioni con la posizione come
  sequenza). Non ho potuto riusare il formato di SlateDB 0.17: il costruttore di SST è privato al crate, e il lettore
  pubblico fa le sue GET senza controllo, quindi non si possono contare né limitare le richieste. Riusato invece lo
  spazio chiavi del motore (`doc/{collezione}/{id}`).
- Manifest per gruppo (un database, o molti database per l'oggetto condiviso), scritto solo con PUT condizionale.
- Worker di scarico e compattazione senza stato; lettore (nodo d'ingresso) che compone finestra del registro + SST
  dal manifest, con cache in RAM o su disco locale.
- `proto/INTERFACCIA.md`: allineata a quella di proto-quaderno, con 5 differenze proposte (nessun punto concordato
  cambiato) e 3 regole di `docs/protocollo.md` (branch tla-registro) che conviene rivedere.

## Risultati già misurati

- Scritture condizionali su MinIO (`proto/lettura/scripts/cond_minio.py`, uscita in
  `proto/lettura/risultati/cond_minio.json`). MISURATO: un PUT con If-Match sbagliato risponde 412
  PreconditionFailed e non scrive; If-None-Match: * su oggetto esistente risponde 412; If-Match su chiave assente
  risponde 404; 32 PUT in gara con lo stesso If-Match: 1 risponde 200 e 31 rispondono 412 (stesso risultato con 32
  If-None-Match: *). Il 304 compare solo dove deve, sul GET con If-None-Match. Il bug del 304 al posto di 412 non c'è
  in questa versione di MinIO. Il worker conta comunque a parte gli eventuali 304 e li tratta come conflitto.
- Controllo negativo del passo 1. MISURATO: con un worker che scrive il manifest senza condizione, in 60 secondi la
  verifica trova 16.214 scritture perse su 48.092. Il verificatore vede gli errori.
- Prove brevi del passo 1 con PUT condizionale. MISURATO: 60 s con 14 uccisioni e 4 abort, e 90 s con 9 uccisioni e
  5 abort: zero perse, zero doppie, zero estranee, zero valori sbagliati.

## Problemi trovati e corretti

- Gli SST di base del precarico avevano lo stesso nome in tutti i database e la cache dei lettori usava il nome come
  chiave: letture di un database servite con i blocchi di un altro. Trovato dalla verifica dei valori del passo 3.
  Corretto (nomi unici, chiave di cache con il gruppo). Non tocca il passo 1.
- Con 3 worker, ognuno considerava "a riposo" un database scaricato dagli altri e lo ricompattava per intero ogni
  30 s: amplificazione di scrittura di circa 170 volte (MISURATO) e disco quasi pieno. Corretto: il riposo si decide
  dall'ora dell'ultimo scarico scritta nel manifest. Dopo la correzione circa 3 volte (MISURATO, prova di 90 s). La
  prima corsa di un'ora del passo 1 è stata fermata a 12 minuti e rilanciata da zero.
- Il cestino interno di MinIO (oggetti già cancellati) si svuota troppo lentamente sotto molte cancellazioni: 14 GB in
  pochi minuti (MISURATO). È un effetto di MinIO locale, non di S3. Uno script (`scripts/pulisci_cestino.sh`) toglie
  le voci più vecchie di 1 minuto.

## Prossimi passi

Risultati del passo 1 e del passo 5 a fine corsa; poi passi 2, 3, 4; poi progetto del passo 6, rapporto finale e PR
in bozza.
