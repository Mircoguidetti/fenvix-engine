# proto-lettura: scarico su S3 e lettura composta del disegno "senza cuoco"

Codice: `proto/lettura` (crate Rust autonomo, `cargo build --release` dentro la cartella). Interfaccia con il registro:
`proto/INTERFACCIA.md`. Rapporto completo con tutti i numeri: `docs/RAPPORTO-proto-lettura.md`. Risultati grezzi:
`proto/lettura/risultati/`.

Tutte le misure sono su una macchina da 4 core e 15 GB di RAM, con MinIO in locale (compilato dai sorgenti, commit
7aac2a2c5b7c), un solo disco condiviso da MinIO, cache e banchi. Nessun servizio cloud.

## Come funziona

**Registro (simulato).** `src/registro.rs`: un processo fa da leader e da replica; ogni commit è confermato subito.
Posizioni come in proto-quaderno (48 bit di ms + 16 di contatore). Per database tiene in RAM i record non ancora su
S3 più quelli scaricati da meno di 60 s, indicizzati per chiave (`BTreeMap` chiave -> versioni). `get(db, chiave, S)`
compone le versioni ≤ S (anche i delta); se non ce ne sono risponde `fuori_finestra` con `potato_fino_a`, la
posizione fino a cui i record sono usciti dalla finestra. `src/net.rs` lo serve via TCP (frame + bincode), così i
worker possono essere processi separati da uccidere. Un log facoltativo su file registra ogni record confermato (db,
posizione, chiavi, crc dei valori) per la verifica del passo 1.

**SST.** `src/sst.rs`: `[blocchi][meta][piè di pagina 64 byte]`. Voce = chiave (id del database 16 byte + chiave del
motore `doc/...` o `idx/...`), posizione, tipo (put, delete, delta), valore. Ordine per chiave crescente e posizione
decrescente; le versioni di una chiave non si dividono tra blocchi; crc32 per blocco e per meta. Il meta (indice dei
blocchi con prima e ultima chiave, filtro di Bloom a 10 bit per chiave, elenco dei database con il loro intervallo di
blocchi) si legge con un solo GET a intervallo, il cui intervallo è scritto nel manifest. Stesso schema di SlateDB
0.17, ma SlateDB non si poteva riusare per scrivere: il costruttore di SST è privato al crate, e il lettore pubblico
(`SstReader`) fa le sue GET (piè di pagina, poi meta) senza che si possano contare o limitare.

**Manifest.** `src/manifest.rs`: JSON per gruppo (un database, o molti database nella variante "oggetto condiviso").
Contiene `scaricato[db]` (posizione fino a cui il database è negli SST elencati), le run dalla più nuova alla più
vecchia (id, intervallo di posizioni, dimensione, intervallo del meta), le run in attesa di cancellazione, l'ora
dell'ultimo scarico e dell'ultima pulizia degli orfani. Si scrive solo con PUT condizionale: If-Match sull'etag letto,
If-None-Match: * alla creazione.

**Scarico da chiunque.** `src/scarico.rs`, `src/bin/pl-worker.rs`. Un worker senza stato durevole:
1. prende dal registro i database con record non su S3 (`pendenti`) e sceglie quelli con il record più vecchio oltre
   l'età o con abbastanza byte;
2. parte dal manifest (l'ultimo che ha scritto lui, con il suo etag, o un GET); se il manifest è più avanti del
   registro (un worker è morto tra manifest e conferma) ripara con `conferma_scarico`;
3. `scaricabili`, tiene solo i record oltre `manifest.scaricato[db]`, costruisce l'SST e lo scrive con un nome nuovo;
4. PUT condizionale del manifest con la run in testa; se riesce, `conferma_scarico(db, fino_a, etag)`; se riceve 412
   rilegge il manifest: se la run c'è (risposta persa e ritentata) è un successo, altrimenti cancella il proprio SST;
   se l'esito è incerto lascia l'SST, che diventa un orfano tolto dalla pulizia (solo se più vecchio di un'ora nel
   costo, 30 s nella prova di uccisione); un SST scritto da troppo tempo non entra più nel manifest.

Compattazione: fonde run adiacenti (stessa classe di dimensione, a gruppi di 4; oltre 30 run la coppia adiacente più
piccola; tutto in una run quando il gruppo è fermo da un po', deciso dall'ora dell'ultimo scarico nel manifest). Tiene
le versioni più nuove dell'orizzonte (2 minuti) più una per chiave; le cancellazioni vecchie spariscono solo se la
fusione arriva alla run più vecchia. Le run tolte si cancellano dopo un periodo di grazia.

**Lettura composta.** `src/lettura.rs`, `src/cache.rs`. Il nodo d'ingresso chiede alla replica `get(db, chiave, S)`.
Se la risposta è nella finestra ha finito; se è `fuori_finestra(potato)` usa il manifest in cache se
`scaricato[db] >= potato`, altrimenti lo rilegge (una GET per scarico, non una per lettura né una ogni pochi secondi).
Poi le run dalla più nuova: Bloom, indice, blocco, prima versione ≤ S. Cache dei meta e dei blocchi in RAM o su disco
locale, con budget e LRU. Unità di trasferimento: l'SST intero se è al più 1 MB, altrimenti il meta, poi la fetta del
database nell'SST se piccola, altrimenti il solo blocco.

## Come si riproduce

    # MinIO locale su 127.0.0.1:9000 (minioadmin/minioadmin), python con boto3 per creare i bucket
    proto/lettura/scripts/pulisci_cestino.sh &          # vedi rapporto: cestino di MinIO
    python3 proto/lettura/scripts/cond_minio.py          # PUT condizionali di MinIO
    cd proto/lettura && cargo build --release && cargo test --release
    python3 scripts/bucket.py --svuota caos && ./target/release/pl-caos --dur-s 3600 --bucket caos --out /tmp/caos   # passo 1
    ./target/release/pl-caos --dur-s 60 --senza-cas --bucket caos-neg --out /tmp/neg                                # controllo negativo
    bash scripts/velocita.sh 300 /tmp/velocita                                                                       # passo 2
    ./target/release/pl-fusione                                                                                      # fusione per core
    ./target/release/pl-lettura --cache ram|disco|nessuna --verifica --bucket lettura --out x.json                   # passo 3
    ./target/release/pl-freddo --bucket freddo --out x.json                                                          # passo 4
    ./target/release/pl-costo --variante base|a|b --bucket costo-x --out x.jsonl                                     # passo 5

Ogni banco vuole il suo bucket vuoto (`scripts/bucket.py --svuota nome`).

## Cosa manca per collegarlo a proto-quaderno

1. **Un adattatore** che implementi il tratto `Registro` (`src/iface.rs`) sopra il client di proto-quaderno
   (`proto/quaderno/src/client.rs`, `Cluster`): `Req::Get/Scan/Confirmed/Wait/Flushable/FlushDone/Pending`, con i
   casi `NotLeader` (vai dal leader) e `Behind` (prova un'altra replica). Il resto di proto-lettura (worker, lettore,
   banchi) non cambia.
2. **Tre campi in più sul filo di proto-quaderno** (elencati in `proto/INTERFACCIA.md`): `potato_fino_a` in
   `OutOfWindow` (senza, il nodo d'ingresso non sa quando il manifest in cache non basta più); l'esito `Delta` per le
   chiavi con soli delta nella finestra (oggi la lettura li perde); byte e `scaricato` in `Pending` (lo scarico per
   dimensione e la riparazione dopo un worker morto).
3. **Un solo scaricatore**: proto-quaderno ha il suo (`flusher.rs`, SST a sequenza di frame senza indice, stato
   compattato nella RAM del worker). Va sostituito con quello di proto-lettura, che scrive SST con indice e Bloom
   leggibili dal lettore e non tiene stato.
4. **La regola di potatura della finestra con lo scarico lento.** Con lo scarico a 10 minuti (variante a del passo 5)
   le repliche tengono in RAM fino a 10 minuti di scritture: va verificato che la finestra di proto-quaderno regga la
   RAM misurata nel passo 5, e che `inizioDisco` resti sotto `min M[db]` (docs/protocollo.md §3.1).
5. **Stessa versione di object_store**: proto-quaderno usa la 0.11, proto-lettura e il motore la 0.14.2.
6. **Fuori dal prototipo**: scansioni e query su indice composte (c'è una `scan` semplice, non misurata), metadati di
   collezione, collegamento al motore (`mongo-server` legge oggi da SlateDB), la chiave di sessione di
   `docs/protocollo.md` §4.6.
