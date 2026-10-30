# Pacchetto di risveglio e livello tiepido — esplorazione

- **Branch:** `wake-bundle-explore`, nato dalla punta di `round3` = **`b6ae7bed9d041828279d7aad3fb3d51ce596b37b`** (30/10/2026).
- **Niente AWS.** Tutte le misure sono su **MinIO locale** (compilato da sorgente, stessa macchina: 4 vCPU, 15 GB). Il "ritardo S3" è **simulato**: ogni richiesta aspetta 10, 20 o 30 ms prima di partire. **È un modello, non S3 vero.**
- **Come leggere i numeri.** Ogni numero porta una di queste etichette:
  - **[misurato]**: misurato in locale, con file e comando per rifarlo.
  - **[codice]**: letto nel codice, con file e riga.
  - **[stimato]**: un calcolo, con il ragionamento.
  - **[non so]**: non verificato.
- I file sorgente di SlateDB sono quelli della versione **0.17.0** (`~/.cargo/registry/src/*/slatedb-0.17.0/…`, abbreviato `SL:`), e `slatedb-txn-obj-0.17.0` (abbreviato `TXN:`).
- Le righe del nostro codice si riferiscono al commit di partenza `b6ae7be`, salvo dove è scritto "branch".

---

## 0. Problemi di correttezza trovati (leggere per primi)

### P1 — `dropDatabase` su un fork può togliere la protezione ai file prima di essere sicuro di poter cancellare

**Stato: [codice], verificato da me. Non riprodotto con un test.**

Cosa succede:
- `Entry::drop_database` chiama prima `fork::on_drop` (`mongo-server/src/dbs.rs:663`). `on_drop` cancella i segnaposto `control/forks/{sorgente}/{fork}` e rilascia i checkpoint che il fork tiene sulla sorgente (`mongo-server/src/ctl/fork.rs:199-212`, `purge::release_pins`).
- **Solo dopo** prova a prendere il lease (`dbs.rs:670-672`). Prima ancora scrive nel giornale locale (`dbs.rs:664-667`).

Se il lease è di un altro processo, la richiesta fallisce (`LeaseHeld`), ma la protezione è già stata tolta. Lo stesso vale se il giornale locale è fermo, o se il processo si ferma a metà. Il risultato è un fork **vivo e leggibile** che non tiene più fermi i file della sorgente. A quel punto la compattazione e la GC della sorgente possono cancellare SST che il fork legge ancora: il fork perde dati.

Inoltre la cancellazione degli oggetti segue l'ordine della LIST (non "prima i manifest"). E non scrive un record d'intento, diversamente da `erase` (`ctl/erase.rs:48`).

Correzione suggerita, non fatta in questo branch:
- in `drop_database`, prendere il lease e fare tutti i controlli **prima** di `on_drop`;
- scrivere un intento come fa `erase`;
- cancellare prima i manifest.

### P2 — Il ripristino all'avvio annulla gli intenti di fork di tutti i processi

**Stato: [non so], dedotto, non verificato.**

All'avvio, il ripristino annulla **ogni** intento di fork in `control/ops/*.json` (`ctl/ops.rs:59-72`). Questo include i fork che un altro processo vivo sta ancora facendo. Con più processi sullo stesso bucket va controllato.

### Nel codice di risveglio attuale

Non ho trovato problemi di correttezza nel percorso di risveglio attuale (lease → lettore → marker → lettura). Le scelte prudenti sono corrette:
- il marker si controlla dopo il lease;
- la riapertura con replay avviene se il marker manca.

---

## 1. In breve

| | Oggi (round3) | Prototipo "pacchetto nel marker" |
|---|---|---|
| Richieste per risveglio pulito | **13** [misurato] | **4** [misurato] |
| Tappe in fila (cammino critico) | **10** [misurato] | **3** [misurato] |
| Tempo con +30 ms per richiesta (modello) | **333 ms** [misurato] | **103 ms** [misurato] |
| Richieste per risveglio sporco (replay) | **34** [misurato] | **29** [misurato] |
| Tempo sporco con +30 ms | **531 ms** [misurato] | **368 ms** [misurato] |
| PUT in più alla chiusura | — | **0** [misurato] |
| Byte in più scritti a ogni chiusura | — | **+8,7 KB** [misurato] (tetto: circa 320 KB [codice]) |

Il prototipo non tocca SlateDB. Usa due accorgimenti:
- un "involucro" sull'object store, che risponde da solo alle letture di apertura di SlateDB;
- la scrittura del pacchetto **dentro il marker pulito**, che la chiusura scrive già.

La correttezza regge su **una sola verifica dopo il lease**: "il manifest successivo, N+1, non esiste". Ci sono 6 test, e 4 di questi falliscono se si toglie la verifica.

---

## 2. Domanda 1 — Da dove vengono le richieste

### 2.1 Risveglio pulito: 13 richieste, 10 tappe

**Misura.** `tools/wake-bundle/measure.py`, serie `baseline`, file `results-wake-bundle/wake.json`. Mediana su 20 risvegli per ogni ritardo; il numero è identico a 0, 10, 20 e 30 ms.

**Configurazione.** È quella di default di round3, quindi anche `--process-lease true`. Database da 50 documenti, chiuso pulito.

**Perché 13 e non 12 come in `results-load.md` (Task 4).** Lì la prima lettura faceva 2 GET_RANGE. Qui ne fa 3, perché i 50 documenti stanno in 2 blocchi e il `find` legge prima le chiavi di metadati (nel blocco 1) e poi il documento (nel blocco 2). Il resto della sequenza è lo stesso della traccia del Task 4 (`loadtest/results/t4/trace.json`, `raw_first_run_clean`).

Traccia reale con +30 ms (`wake.json`, `baseline/30ms`, `example_clean`; inizio in ms):

| # | Richiesta | Oggetto S3 | Chi la fa (file sorgente) | Perché | Inizio | Tappa |
|---|---|---|---|---|---|---|
| 1 | GET | `_lease` | nostro: `mongo-server/src/lease.rs:244` (o `:350` in modalità per processo) | legge il lease e il suo ETag | 0 | 1 |
| 2 | PUT condizionale (If-Match) | `_lease` | nostro: `lease.rs:284-296` / `:411` | prende il lease: è il fencing tra processi | 33 | 2 |
| 3 | LIST | `manifest/` | SlateDB: `SL:db/builder.rs:1928` `StoredManifest::try_load` → `TXN:object_store.rs:429-430` | trova l'ultimo manifest | 69 | 3 |
| 4 | HEAD | `_clean` | nostro: `src/lifecycle.rs:367` (in parallelo con l'apertura) | chiuso pulito? | 69 | 3 (parallela) |
| 5 | GET | `manifest/…0004.manifest` (458 B) | SlateDB: `TXN:object_store.rs:436` | legge l'ultimo manifest | 102 | 4 |
| 6 | GET (404) | `gc/manifest.boundary` | SlateDB: `TXN:lib.rs:666-682` → `TXN:object_store.rs:285-293` | verifica che il manifest non sia "dietro il confine" della GC. Il file manca, quindi niente cache: `TXN:object_store.rs:246-251` | 135 | 5 |
| 7 | GET (404) | `manifest/…0005.manifest` | SlateDB: `SL:db_reader.rs:980`, **secondo** `StoredManifest::load` sullo stesso store. La versione in cache → sonda N+1: `TXN:object_store.rs:400-419` | rilegge l'ultimo manifest (doppione) | 168 | 6 |
| 8 | GET (404) | `gc/manifest.boundary` | SlateDB, come la #6 | doppione | 201 | 7 |
| 9 | GET (404) | `manifest/…0005.manifest` | SlateDB: il poller del lettore scatta **subito** al primo giro (`SL:db_reader.rs:1014`, `SL:dispatcher.rs:1019` "the first tick is always immediate") | controllo di aggiornamento | 234 | in parallelo, non blocca |
| 10 | GET_RANGE (100 B) | `compacted/…sst` | SlateDB: indice dell'SST, `SL:tablestore.rs:527-575`. Nessun filtro Bloom sotto 1.000 chiavi (`SL:config.rs:1109`, `SL:tablestore.rs:410`) | prima lettura | 234 | 8 |
| 11 | GET (404) | `gc/manifest.boundary` | SlateDB (poller) | doppione | 267 | in parallelo |
| 12 | GET_RANGE (3,8 KB) | stesso SST | SlateDB: blocco dati 1 (metadati della collezione) | prima lettura | 267 | 9 |
| 13 | GET_RANGE (4,0 KB) | stesso SST | SlateDB: blocco dati 2 (il documento) | prima lettura | 300 | 10 |

**Il cammino critico (10 tappe in fila):**

```
GET _lease → PUT _lease → [LIST manifest ‖ HEAD _clean] → GET man.N → GET boundary
           → GET man.N+1 → GET boundary → GET_RANGE indice → GET_RANGE blocco1 → GET_RANGE blocco2
(fuori dal cammino: GET man.N+1 + GET boundary del poller)
```

Il testo di partenza parlava di "5-6 tappe". Con questa traccia le tappe sono **10** [misurato]: ogni GET di SlateDB aspetta la precedente.

Il modello torna: tempo ≈ tappe × ritardo + tempo locale.
- 10 × 30 ms + ~33 ms = 333 ms [misurato].
- 10 × 10 ms + ~31 ms = 132 ms [misurato].

### 2.2 Risveglio sporco (marker assente): 34 richieste, 16 tappe

**Misura.** Serie `baseline`, `unclean`. Come nel Task 4, il marker viene tolto da `tenantTrace`. Il WAL oltre il manifest è vuoto, quindi **non** ci sono GET di WAL da rigiocare. Dopo un crash vero ce ne sarebbero in più [non so quante: dipende da quanto WAL era in sospeso].

| Richieste | Cosa | Chi |
|---|---|---|
| 2 | lease GET + PUT | nostro |
| 1 | HEAD `_clean` (404) | nostro: `src/lifecycle.rs:367` |
| 5 | LIST + GET N + boundary + GET N+1 + boundary: **apertura "speculativa" senza replay** | SlateDB, chiamata da noi: `lifecycle.rs:367` |
| 5 | le stesse 5, ripetute: il lettore viene chiuso e **riaperto con replay**, con un nuovo `ManifestStore` e quindi senza cache | nostro: `lifecycle.rs:375-377`; SlateDB come sopra |
| 8 | HEAD `wal/…0003…0130.sst`, **in parallelo** (1 tappa): ricerca esponenziale dell'ultimo WAL, 8 sonde per giro | SlateDB: `SL:wal/slatedb/store.rs:500-562`, chiamata da `SL:db_reader.rs:660-683` |
| 3 | GET_RANGE indice + 2 blocchi | SlateDB |
| 2 | GET N+1 + boundary del poller | SlateDB (in parallelo) |
| 8 | di nuovo le 8 HEAD del WAL: il poller vede lo stesso manifest e rifà la ricerca del WAL | SlateDB: `SL:db_reader.rs:548-549` → `:407-440` (in parallelo) |

La traccia del Task 4 dava 29 richieste. La differenza (+5) viene dal blocco dati in più e da un secondo giro del poller che lì non c'era.

### 2.3 Chiusura pulita dopo scritture: 5 PUT e 4 GET

**[misurato]** `close_cost_p50` in `wake.json`. Sono:
- il flush della memtable (SST L0 + manifest);
- il marker `_clean`;
- il rilascio del lease.

Il dettaglio per oggetto non è stato tracciato [non so].

---

## 3. Domanda 2 — Cosa si può togliere

Il risparmio è calcolato sul risveglio pulito di oggi, cioè 13 richieste e 10 tappe.

### Senza toccare SlateDB (configurazione, cache, nostro livello)

| # | Opzione | Richieste risparmiate | Tappe risparmiate | Stato |
|---|---|---|---|---|
| A | **Pacchetto nel marker + verifica N+1** (prototipo, §4) | **−9** (13 → 4) | **−7** (10 → 3) | [misurato] |
| B | Leggere il marker **in parallelo al lease** e, se manca, aprire **subito con replay**. Così si evita l'apertura speculativa doppia | sporco: **−5** (34 → 29) | sporco: **−5** (16 → 11) | [misurato] (fa parte del prototipo) |
| C | Creare `gc/manifest.boundary` (valore `0`, PUT con `Create`) alla nascita del database. Dopo la prima lettura riuscita SlateDB lo tiene in cache (`TXN:object_store.rs:285-293`): le 3 GET diventano 1. È sicuro perché il file mancante vale già 0 (`TXN:object_store.rs:246-251`). Nei database dove la GC ha già cancellato manifest, il file probabilmente esiste già [non so quanti] | −2 | −1 | [stimato] |
| D | **Cache in RAM riusata tra risvegli.** Oggi ogni apertura usa un "ambito" di cache nuovo (`src/lifecycle.rs:159-172`), quindi indice e blocchi si rileggono sempre da S3, anche se lo stesso server li aveva appena letti. Con un ambito stabile per (database, incarnazione) un secondo risveglio sullo stesso server li trova in RAM | −3 GET_RANGE | −3 | [stimato]. Sicuro per gli SST compattati, che hanno nomi unici e non cambiano mai. Il WAL non passa dalla cache del lettore (`SL:db/builder.rs:1980`, WAL store senza cache); per il writer [non so], va verificato |
| E | Cache su disco di SlateDB (`CachedObjectStore`, §5) | −3 GET_RANGE se i blocchi sono sul disco | −3 | [stimato] |
| F | Lease "alla cieca": se l'ultimo a rilasciare il lease è stato questo server, conosce l'ETag del rilascio e può fare subito il PUT condizionale, saltando la GET. Se qualcuno l'ha toccato nel frattempo, il PUT fallisce e si rifà il percorso normale | −1 | −1 | [stimato] |

### Con una modifica (o fork) di SlateDB

| # | Opzione | Richieste | Tappe | Stato |
|---|---|---|---|---|
| G | Leggere il manifest **una volta sola** all'apertura del lettore. Oggi lo legge due volte: `SL:db/builder.rs:1928` e `SL:db_reader.rs:980` | −2 | −2 | [codice] |
| H | Il poller del lettore **non** deve scattare subito (`SL:dispatcher.rs` primo tick immediato) | −2 pulito / −10 sporco | 0 (sono in parallelo) | [codice] |
| I | Ricordare in cache anche "boundary assente" (`TXN:object_store.rs:246-251`): è l'alternativa a C dentro SlateDB | −2 | −1 | [codice] |
| J | API "apri il lettore da questi byte di manifest + questi indici". È la versione pulita di A, senza involucro (§4.2) | come A | come A | [stimato] |

**Il minimo teorico** resta il lease: GET + PUT, cioè 2 tappe. Con il pacchetto nel record del lease (§4.3) oppure con F, si scende a **2 tappe e 3 richieste** [stimato], cioè circa 66 ms con +30 ms.

---

## 4. Domanda 3 — Pacchetto di risveglio

### 4.1 Variante A — dentro il nostro livello (prototipo, fatto)

**Cosa c'è nel branch:**
- `src/wake_bundle.rs`: il pacchetto e l'involucro `BundleStore`;
- `src/lifecycle.rs`: `prefetch_marker`, `wake_with`, la chiusura;
- `mongo-server/src/dbs.rs`: `ensure_open`;
- il flag `--wake-bundle`, **spento di default**.

**Alla chiusura pulita.** Il writer chiude. Poi il marker `_clean`, che si scriveva già, contiene:
- `FXWB1`;
- id e byte dell'ultimo manifest scritto o letto dalla sessione;
- gli intervalli di SST compattati letti o scritti: SST interi fino a 64 KiB, in tutto al massimo 256 KiB.

È **lo stesso PUT** di prima: 0 richieste in più. Pesa circa 8,7 KB per un database da 50 documenti [misurato].

**Al risveglio:**
1. GET `_lease` ‖ GET `_clean`, in parallelo. Prima era HEAD; la GET ha lo stesso prezzo.
2. PUT condizionale del lease.
3. **Verifica:** HEAD `manifest/N+1`. Gli id dei manifest sono consecutivi (`TXN:object_store.rs:410-411`: "a missing successor means the cached object is the latest version"). Ogni apertura di un writer e ogni compattazione o checkpoint scrive N+1. Nei test, togliere questa verifica fa perdere una scrittura confermata: il test `crash_after_the_bundle_replays_the_wal` fallisce.
4. Se N+1 non esiste, il lettore SlateDB si apre attraverso `BundleStore`, che risponde da solo a:
   - LIST di `manifest/`, che restituisce solo N;
   - GET di N;
   - GET di N+1 e oltre, che risponde 404;
   - GET di `gc/manifest.boundary`, che risponde 404. È sicuro perché la GC sposta il confine solo su manifest che cancella, mai sull'ultimo (`SL:garbage_collector/manifest_gc.rs:97-138`);
   - GET_RANGE di SST compattati presenti nel pacchetto.
5. Se N+1 esiste, il pacchetto è vecchio: si rifà il percorso di oggi, compreso il controllo del marker dopo il lease.

**Le risposte "mutevoli"** ("N è l'ultimo", "boundary assente") valgono solo per 2 s dopo la verifica. Si spengono prima che si apra un writer (`stop_serving`). Le risposte "immutabili" (un manifest N, un pezzo di SST con nome ULID) restano valide sempre.

### 4.2 Variante B — dentro SlateDB (richiede una fork)

Aggiungere a `DbReaderBuilder` un'opzione "stato iniziale": `(manifest_id, byte del manifest, indici/filtri/blocchi già noti)`. SlateDB:
- salterebbe LIST, GET e doppia lettura;
- verificherebbe da solo N+1 con una HEAD;
- metterebbe gli indici nella sua cache.

Alla chiusura `Db::close` restituirebbe lo stesso stato da salvare.

Vantaggi rispetto ad A:
- niente involucro che "indovina" i percorsi;
- regge anche un cambio del formato dei percorsi in una versione futura.

Svantaggio: una fork di SlateDB da mantenere.

Come richieste e tappe è uguale ad A [stimato].

### 4.3 Variante A2 — pacchetto nel record del lease [stimato, non costruita]

Il rilascio del lease (un PUT che c'è già) porta il pacchetto. Il risveglio legge il lease (GET) e trova già il pacchetto. La verifica N+1 parte **in parallelo** al PUT di acquisizione. È sicuro, perché la verifica parte dopo la GET del lease e il PUT è condizionato a quella GET.

Risultato: **3 richieste, 2 tappe**.

Svantaggi:
- il record del lease, letto anche dalle altre sessioni, diventa più grosso;
- va gestito il caso del record "di processo" (`lease.rs:346-414`).

### 4.4 Correttezza, caso per caso

| Caso | Cosa succede con la variante A | Sicuro? |
|---|---|---|
| **Pacchetto vecchio rispetto al manifest vero** (compattazione, checkpoint, writer arrivati dopo la chiusura) | N+1 esiste → percorso di oggi. Test: `stale_bundle_after_a_writer_is_not_used` e `manifest_change_without_a_writer_falls_back` | **Sì** |
| **Due processi** | Il pacchetto si usa solo dopo il PUT condizionale del lease riuscito. La verifica N+1 parte dopo la GET del lease. Un altro writer avrebbe scritto N+1 (`SL` apertura writer = nuovo manifest con epoch più alto) | **Sì** |
| **Marker letto prima del lease** (lettura anticipata) | Da solo non basta. Esempio: dopo la lettura del marker, un writer apre, scrive e muore. Il lease scade e noi lo prendiamo. Il marker letto era vecchio. Per questo un marker **senza** pacchetto viene riletto dopo il lease. Un marker **con** pacchetto vale solo insieme alla verifica N+1: il writer morto ha scritto N+1 aprendo | **Sì** (test `crash_after_the_bundle_replays_the_wal`) |
| **Takeover con fencing** | Chi prende il lease scaduto passa dalla verifica. Se il vecchio proprietario era un writer, N+1 esiste e si va al replay normale | **Sì** |
| **Crash tra scrittura del pacchetto e del manifest** | Il pacchetto è scritto **dopo** che `Db::close` è tornato: il manifest finale esiste già. Un crash prima del marker significa niente marker, quindi replay. Se il writer di chiusura scrive un manifest in più dopo quello che riporta (succede: id+1), la sessione prende il più recente che ha visto. Se ne sfugge uno, la verifica N+1 lo trova e si va al percorso normale: costa, ma non è un errore | **Sì** |
| **GC dei file citati dal pacchetto** | Se N è l'ultimo (verificato), i suoi SST sono referenziati e la GC li tiene. Se una compattazione arriva **dopo** la verifica, è la stessa situazione di oggi (il lettore resta su N fino al poll, `LONG_POLL` 1 h, `src/lifecycle.rs:36`). La GC aspetta `--sst-gc-min-unref-secs` prima di cancellare | **Come oggi** |
| **Restore (PITR) nello stesso nome** | Il restore scrive con il writer normale (`ctl/pitr.rs`, `diff_and_apply` → `g.apply`): l'apertura del writer cancella il marker e scrive N+1 | **Sì** |
| **Fork (clone)** | Il fork scrive `_clean` = `clean` senza pacchetto (`ctl/fork.rs:150`): primo risveglio col percorso di oggi | **Sì** |
| **Erase / drop + ricreazione** | Il prefisso viene cancellato, marker compreso. Un database ricreato non ha pacchetto | **Sì per il pacchetto su S3.** **No per una copia locale** senza incarnazione (§5) |
| **Writer subito dopo un risveglio col pacchetto** (entro i 2 s) | `stop_serving` prima dell'apertura del writer. Anche senza, il PUT del manifest di SlateDB è una creazione condizionale e non sovrascrive: il writer aspetterebbe la fine della finestra. Test `writer_after_a_bundle_wake_sees_the_real_manifests` | **Sì** |

**Dove non è sicuro.** Il pacchetto si fida di una regola di SlateDB: "ogni cambio di stato scrive il manifest N+1 con id consecutivo". Se una versione futura di SlateDB cambiasse questa regola (id non consecutivi, cambi di stato senza nuovo manifest), la verifica non basterebbe più. Il test `crash_after_the_bundle_replays_the_wal` è il guardiano: va eseguito a ogni aggiornamento di SlateDB.

### 4.5 Costo del pacchetto

| | Variante A (nel marker) | Variante "oggetto separato" |
|---|---|---|
| PUT in più per chiusura | **0** [misurato] | 1 [stimato] |
| Byte in più per chiusura | +8,7 KB [misurato] (tetto circa 320 KB [codice]) | uguali |
| Richieste risparmiate per risveglio | 9: 1 LIST (fascia PUT) + 8 GET [misurato] | uguali |

**Prezzi** (S3 Standard us-east-1, `loadtest/results/s3_prices.json`: 0,005 $/1.000 PUT/LIST, 0,0004 $/1.000 GET):
- risveglio oggi: 2 × 0,000005 + 11 × 0,0000004 = **0,0000144 $**;
- risveglio con il pacchetto: 1 × 0,000005 + 3 × 0,0000004 = **0,0000062 $**;
- risparmio: **0,0000082 $ per risveglio**, cioè 8,2 $ per milione di risvegli (−57%) [stimato].

**Quando conviene un oggetto separato.** Costa 1 PUT per chiusura (0,000005 $) contro 0,0000082 $ risparmiati per risveglio. Conviene solo se più del 61% delle chiusure è seguito da un risveglio che usa il pacchetto. Per questo il prototipo lo mette **nel marker**: lì conviene sempre.

**Spazio** [stimato]:
- 8,7 KB per database × 1 milione di database = 8,7 GB, circa 0,21 $/mese (0,0245 $/GB-mese, eu-central-1, `results-aws-test3.md:83-86`);
- nel caso peggiore (tetto pieno) circa 7,8 $/mese.

**Quando il pacchetto è vecchio** (database compattato dopo la chiusura): si pagano 2 richieste in più di oggi, la GET più pesante del marker e la HEAD N+1, cioè circa 15 richieste e 11 tappe [stimato dal codice, non misurato].

Quanti risvegli trovano il pacchetto vecchio dipende da quanto spesso la manutenzione compatta dopo la chiusura [non so]. La chiusura chiede una compattazione se L0 ≥ 4 (`src/lifecycle.rs`, `L0_COMPACT_THRESHOLD`). La soluzione naturale è far riscrivere il marker alla manutenzione dopo la compattazione, **con PUT condizionato all'ETag del marker letto**. Così non si "resuscita" un marker che un writer ha appena cancellato. Non è costruita.

---

## 5. Domanda 4 — Livello tiepido (disco locale)

### Cosa offre già SlateDB 0.17

- **Cache in RAM di blocchi, indici e filtri** (`DbCache`/foyer). La usiamo già, una per processo (`--block-cache-mb`, `--meta-cache-mb`, `src/lifecycle.rs:156-168`). Però **ogni apertura ha un ambito nuovo** (`:171-172`), quindi tra due risvegli non si riusa nulla (opzione D, §3).
- **Cache su disco di oggetti**: `slatedb::cached_object_store::CachedObjectStore` (pubblica, `SL:lib.rs:85`; costruttore `SL:cached_object_store/object_store.rs:166`; opzioni `SL:config.rs:1775-1835`). Le sue regole:
  - mette in cache **solo SST compattati**, mai manifest né WAL (`SL:cached_object_store/policy.rs:17-38, 68-71`);
  - la chiave è il percorso dell'oggetto. Un SST compattato ha nome ULID e non cambia mai, quindi una voce **non può diventare vecchia**;
  - legge a pezzi di 4 MB (`part_size_bytes`): la prima lettura di un SST piccolo scarica tutto l'SST;
  - dimensione massima di default 16 GB;
  - per lo scarto pesca 2 voci a caso e butta quella usata meno di recente (`SL:cached_object_store/storage_fs.rs:998-1055`);
  - opzionali: `cache_on_flush` e `cache_on_compaction`, che mettono in cache gli SST appena scritti;
  - opzionale: `preload_disk_cache_on_startup`.

### Cosa va scritto

1. **Una sola `CachedObjectStore` per processo**, sotto il nostro store e sotto SlateDB, così vede i "tag" delle chiamate. Oggi le opzioni sono per singolo `Db`/`DbReader`: con migliaia di database aperti si avrebbero migliaia di "spazzini" (evictor) sulla stessa cartella [stimato, da evitare].
2. **La copia locale del pacchetto**, scritta alla chiusura insieme al marker.
3. **Svuotamento per database** all'erase. La cancellazione passa dalla cache solo se passa da `CachedObjectStore` (`object_store.rs`, `delete_stream`). Il nostro `purge` usa lo store grezzo, quindi i byte resterebbero sul disco fino allo scarto. Per il GDPR serve una cancellazione esplicita della cartella del database.

### La copia locale del pacchetto e il "hot move" (journal3z, §5.7)

Dal design (`origin/journal3z:docs/journal3z-design.md`, righe 220-230):
- per lo spostamento A → B, A smette di scrivere, aspetta il commit e rilascia il lease;
- B prende il lease con un epoch più alto e apre;
- l'upload da parte di A prima del rilascio è **facoltativo** (riga 227), quindi B può rigiocare il giornale e scrivere su S3;
- se A è bloccato, B prende il lease dopo la scadenza e "il fence fa il resto" (riga 228);
- nessun dato viene copiato tra server;
- il design non dice nulla di cache dati.

**Regole perché la cache locale non dia mai dati vecchi:**

1. **I blocchi SST** sono sicuri per nome (ULID), anche per A → B → A. Si arriva a un blocco solo attraverso un manifest appena verificato.
2. **Il pacchetto locale** non basta da solo. Prima di usarlo, A deve sapere che nessuno ha cambiato lo stato da quando l'ha scritto. La verifica "N+1 assente" copre B che scrive o compatta. Non copre però **drop + ricreazione**: un database nuovo con lo stesso nome ricomincia dal manifest 1 e il suo N potrebbe non avere successore.

   Servono quindi due HEAD in parallelo:
   - HEAD `manifest/N` con **lo stesso ETag** salvato nel pacchetto. Due manifest con byte identici citano gli stessi SST con nome unico, quindi descrivono gli stessi dati;
   - HEAD `manifest/N+1` assente.

   Oppure serve un'**incarnazione** del database nel record del lease. È la D4 del design journal3z (righe 24, 59, 264-267: "non fatta").
3. **La cache si usa solo da chi tiene il lease.** Non estende mai la proprietà (`lease.rs`, `valid`/`check_owner`).
4. **Erase**: il server proprietario svuota subito la sua cartella. Gli altri server hanno copie ormai irraggiungibili (la verifica fallisce), ma con byte del cliente. Servono uno svuotamento diffuso a tutti, oppure una cifratura per incarnazione da poter "dimenticare" (citata come opzionale nel design, riga 276).

**Conto per il risveglio dalla copia locale** [stimato]:
- GET lease → PUT lease ‖ HEAD N ‖ HEAD N+1;
- 4 richieste, 2 tappe;
- rispetto al pacchetto su S3 risparmia una GET, ma non tappe.

Il vero guadagno del disco locale sono **i blocchi dei database grandi**, cioè la prima lettura oltre il pacchetto.

**Dimensione sul disco** [stimato]:
- pacchetto: 8,7 KB tipico, 320 KB massimo;
- 100.000 database per server = da 0,9 a 32 GB;
- i blocchi hanno un tetto configurabile (16 GB di default).

---

## 6. Domanda 5 — File condivisi tra database da uno stesso modello

**Fonte.** Analisi del codice fatta da un sotto-agente. Ho ricontrollato io `fork.rs:138-143`, `erase.rs:38-43`, `fork.rs:199-212`, `dbs.rs:662-695` e `SL:clone.rs:223-258`.

**Come si sceglie il modo.** **`ENGINE_FORK_MODE` non esiste** nel codice: grep vuoto su tutto il repo. Il modo si sceglie nel corpo HTTP: `POST /v1/databases/{db}/fork {"target","mode"}`, con default `"clone"` (`ctl/api.rs:57-61`). I modi accettati sono `clone` e `copy` (`fork.rs:80-82`).

**Il clone condivide davvero gli SST** [codice]:
- `Admin::create_clone_builder_from_source(CloneSourceSpec::new(src))` (`fork.rs:140-141`);
- il manifest del clone ha un `ExternalDb` con l'elenco **completo** degli SST della sorgente (`SL:manifest/mod.rs:992-1034`), e i percorsi puntano alla sorgente (`SL:paths.rs:22-36`);
- il WAL rimasto viene **copiato** (`SL:wal/slatedb/admin.rs:128-156`). Dopo `quiesce` (`fork.rs:55-72`) è vuoto o quasi.

**Chi usa i file.** Non c'è un contatore. Ci sono due registri:
1. **In SlateDB**: un checkpoint "finale" senza scadenza per ogni fork e per ogni antenato, scritto nel manifest della sorgente (`SL:clone.rs:223-258`).
2. **Nel nostro livello**: un segnaposto `control/forks/{antenato}/{fork}` (`fork.rs:130-135`), contato con una LIST (`fork.rs:38-51`).

**GC** [codice]:
- la GC di SlateDB tiene ogni SST citato dall'ultimo manifest **e da ogni manifest fissato da un checkpoint** (`SL:garbage_collector/compacted_gc.rs`), quindi la compattazione della sorgente non cancella i file del clone. C'è un test: `ctl-tests/test_02_fork.py:207-237`;
- la GC del clone elenca solo la propria cartella;
- la nostra `gc_tenant_ssts` salta i database con checkpoint (`src/lifecycle.rs:1303-1306`);
- in passato un bug cancellava tutti i checkpoint e faceva perdere dati; ora è corretto: la compattazione cancella solo checkpoint senza nome e con scadenza (`src/lifecycle.rs:908` al commit di partenza, `results-ops.md:12`).

**Cancellazione** [codice]:
- l'erase della sorgente con fork vivi risponde **409 `has_forks`** (`ctl/erase.rs:40-43`). `dropDatabase` dà un errore equivalente (`fork.rs:201-204`);
- l'erase di un fork rilascia prima i checkpoint, poi cancella (`ctl/purge.rs:41-57, 81-97`);
- attenzione al **problema P1** (§0) nel percorso `dropDatabase`.

**Costo misurato di un fork clone** (`results-ops/costs.json:67-82`, `results-ops.md:44,82`):
- 9 LIST, 19 GET, 1 HEAD, 11 PUT;
- circa 3 KB scritti;
- circa 1,3 s, indipendente dalla dimensione dei dati.

**10.000 database da un modello: cosa non va oggi** [codice + stimato]:
- i fork della stessa sorgente sono **in fila**: c'è un lock sulla sorgente, `quiesce` e il lease (`fork.rs:83, 124-128`). Circa 1,3 s × 10.000 ≈ 3,6 ore;
- ogni clone fissa una **versione diversa** del manifest del modello. Quindi:
  - restano 10.000 manifest;
  - ciascuno contiene circa k checkpoint;
  - in tutto circa n²/2 × 50 B ≈ 2,5 GB [stimato];
  - una GC del modello legge fino a 10.000 manifest.
- il manifest più recente del modello contiene 10.000 checkpoint, circa 0,5 MB [stimato], ed è riscritto a ogni creazione o cancellazione di clone, con contesa [stimato]. Nessun limite esplicito in SlateDB 0.17 [codice, grep].

**Cosa servirebbe per costo quasi zero** [stimato, non costruito]:
1. **Modello congelato.**
   - Creare il modello, chiuderlo pulito e compattarlo tutto.
   - Prendere **un solo checkpoint con nome e senza scadenza** (`Admin::create_detached_checkpoint`, come in `ctl/pitr.rs:146`).
   - Non aprirlo più come writer. Un nuovo modello avrà un nuovo nome.
2. **Clonare da quel checkpoint**: `CloneSourceSpec::with_checkpoint` (`SL:db/builder.rs:2067`). Così tutti i clone fissano **la stessa versione** del manifest e niente cresce in modo quadratico.
   - SlateDB scrive comunque un checkpoint finale per clone. Si può cancellarlo subito, perché basta quello del modello congelato.
3. **Niente `quiesce`, lock e lease** per un modello congelato: i clone possono partire in parallelo.
   - Costo per clone: circa 20 PUT-tier + 20 GET-tier, cioè circa 0,0001 $ e circa 1 $ per 10.000 [stimato].
   - Spazio: solo il manifest del clone, che però contiene l'elenco degli SST del modello, quindi cresce con la dimensione del modello.
4. **Modificare un clone**: scrive SST propri. Con la compattazione riscrive anche i dati del modello e alla fine si "stacca" (detach GC di SlateDB, `SL:garbage_collector/detach_gc.rs`). Lo spazio cresce solo per i clone che cambiano.
5. **Cancellare in sicurezza**:
   - il segnaposto per clone resta (si scrive prima di condividere, si cancella dopo l'erase del clone);
   - il modello si cancella solo quando la LIST di `control/forks/{modello}/` è vuota, con un flag "modello in ritiro" scritto con PUT condizionale per chiudere la gara con i clone in creazione;
   - un contatore unico con CAS metterebbe in fila tutte le 10.000 operazioni sulla stessa chiave: meglio la LIST.

---

## 7. Domanda 6 — Misure in locale

**AVVERTENZA:** MinIO locale + ritardo **simulato** per ogni richiesta. **Non è S3 vero.** Su S3 vero i tempi variano da richiesta a richiesta, le code sono più lunghe e i PUT sono più lenti delle GET.

**Come rifarle:**
- server: `target/release/mongo-server` del branch, con o senza `--wake-bundle`;
- ritardo: regola `delay_all MS` nel file `MONGO_TEST_FAULTS_FILE` (nuova, `mongo-server/src/faults.rs`), che fa aspettare MS ms **ogni** richiesta prima di partire;
- script: `tools/wake-bundle/measure.py --bin target/release/mongo-server --out results-wake-bundle/wake.json --n 25 --delays 0,10,20,30`;
- 25 database per serie, i primi 5 scartati, quindi 20 risvegli per numero;
- ogni database ha 50 documenti ed è chiuso pulito;
- risveglio = `tenantTrace` con `find` per `_id`; "sporco" = marker tolto come nel Task 4.

**Tappe in fila** = la catena più lunga di richieste in cui ognuna parte dopo la fine della precedente, calcolata dai tempi della traccia.

**Criteri decisi prima di misurare e non cambiati:**
- il prototipo è "promettente" se, a 30 ms, **dimezza il tempo** del risveglio pulito;
- non deve aggiungere **nessun PUT** alla chiusura;
- tutti i test di sicurezza devono passare.

**Esito: tutti e tre soddisfatti.**

### Risveglio pulito (mediane, 20 risvegli per riga) [misurato]

| Ritardo per richiesta | Oggi: richieste / tappe / tempo | Pacchetto: richieste / tappe / tempo | Tempo risparmiato |
|---|---|---|---|
| 0 ms (MinIO puro) | 13 / 10 / 12,4 ms | 4 / 3 / 5,8 ms | −53% |
| 10 ms | 13 / 10 / 131,6 ms | 4 / 3 / 41,7 ms | −68% |
| 20 ms | 13 / 10 / 231,3 ms | 4 / 3 / 72,2 ms | −69% |
| 30 ms | 13 / 10 / 332,8 ms | 4 / 3 / 103,2 ms | −69% |

### Risveglio sporco (marker assente) [misurato]

| Ritardo | Oggi | Con lettura anticipata del marker (parte del prototipo) |
|---|---|---|
| 0 ms | 34 / 16 / 18,2 ms | 29 / 11 / 14,2 ms |
| 10 ms | 34 / 16 / 203,2 ms | 29 / 11 / 141,1 ms |
| 20 ms | 34 / 16 / 367,2 ms | 29 / 11 / 255,4 ms |
| 30 ms | 34 / 16 / 530,9 ms | 29 / 11 / 368,1 ms |

### Chiusura (database con writer aperto, 10 chiusure, mediana) [misurato]

| | PUT | GET | Byte scritti |
|---|---|---|---|
| Oggi | 5 | 4 | 9.120 |
| Pacchetto | 5 | 4 | 17.798 (+8.678) |

### Contatori del prototipo [misurato]

`tenantStats.wakeBundle`: in ogni serie 25 risvegli con pacchetto usato, 0 pacchetti vecchi, 50 chiusure con pacchetto. Il caso "pacchetto vecchio" non compare in queste serie, perché nessuna compattazione è avvenuta tra chiusura e risveglio. È coperto solo dai test.

### Test del prototipo [misurato]

Comando: `cargo test --release -p tenant-bench --lib wake_bundle`. Risultato: 6 su 6 passati.

| Test | Cosa controlla |
|---|---|
| `bundle_wake_needs_only_the_probe` | apertura + prima lettura = 1 sola richiesta (la verifica), dati giusti |
| `stale_bundle_after_a_writer_is_not_used` | pacchetto letto prima di un writer: si vedono i dati nuovi |
| `crash_after_the_bundle_replays_the_wal` | pacchetto letto, poi un writer conferma una scrittura e muore senza chiudere: la scrittura **non** si perde |
| `manifest_change_without_a_writer_falls_back` | checkpoint dopo la chiusura: si torna al percorso normale |
| `writer_after_a_bundle_wake_sees_the_real_manifests` | un writer subito dopo, con un manifest nuovo scritto da altri: corretto e senza attese |
| `encode_decode_roundtrip` | un pacchetto troncato non viene creduto |

**Controprova.** Togliendo la verifica N+1, falliscono 4 test su 6, compreso quello del crash (scrittura persa). Togliendo `stop_serving`, fallisce il test del writer (attesa di 2 s).

**Prove fatte su:**
- InMemory (test);
- MinIO (misure).

**Prove non fatte:**
- test end-to-end via protocollo Mongo con crash reale (`kill -9`) e prototipo acceso [non so];
- suite di compatibilità con `--wake-bundle` [non so].

---

## 8. Domanda 7 — Piano di misura su S3 vero (NON eseguito)

**Scopo.** Confermare su S3 vero il guadagno di tempo e di richieste del pacchetto, e controllare che non ci siano errori.

### Ambiente

- **Macchina:** 1 EC2 `c7g.large` (2 vCPU) **nella stessa regione del bucket**, `eu-central-1`, la stessa di `results-aws-test3.md`. Il runner GitHub di quella prova non era nella regione (`results-aws-test3.md:9`), quindi le sue latenze non valgono.
- **Bucket:** nuovo, dedicato, S3 Standard, cancellato alla fine.

### Carico

1. **Preparazione:** 1.000 database da 50 documenti, chiusi puliti. 500 col binario attuale e 500 col binario del branch con `--wake-bundle`.
2. **Risvegli:** per ogni binario, 500 risvegli puliti e 500 sporchi (marker tolto), uno alla volta, con `tenantTrace`. In tutto 2.000 risvegli.
3. **Pacchetto vecchio:** 100 database del gruppo "pacchetto" con una compattazione forzata dopo la chiusura, per misurare il caso vecchio.
4. **Correttezza:** 50 database con scrittura confermata seguita da `kill -9`, poi risveglio con il pacchetto letto prima del crash. Ogni scrittura confermata deve essere riletta.

### Richieste e costo atteso

Prezzi eu-central-1 (`results-aws-test3.md:83-86`): 0,0054 $/1.000 PUT-tier, 0,00043 $/1.000 GET-tier.

| Voce | PUT-tier | GET-tier | Costo |
|---|---|---|---|
| Preparazione, ≈ 20 PUT-tier + 30 GET-tier per database [stimato da `close_cost` e apertura del writer] | 20.000 | 30.000 | 0,108 + 0,013 = 0,121 $ |
| 2.000 risvegli + chiusure, ≤ 3 PUT-tier + 31 GET-tier ciascuno [misurato in locale, caso peggiore sporco] | 6.000 | 62.000 | 0,032 + 0,027 = 0,059 $ |
| Pacchetto vecchio + crash (150 database, ≈ 40 PUT-tier + 60 GET-tier) | 6.000 | 9.000 | 0,032 + 0,004 = 0,036 $ |
| Margine × 3 (ripetizioni, errori, GC) | | | ≈ 0,65 $ |
| Spazio: ≈ 0,4 GB per qualche giorno | | | < 0,01 $ |

**Totale S3 atteso: circa 0,22 $, con margine fino a 0,65 $.**

**Esclusi da questa cifra:**
- l'EC2: `c7g.large`, circa 0,08 $/ora [non so il prezzo esatto in eu-central-1, va letto dal listino], circa 2 ore, quindi circa 0,2 $;
- il traffico, che è nullo nella stessa regione.

### Criteri di successo (fissati ora)

1. Risveglio pulito con pacchetto: tempo p50 **almeno −50%** rispetto al binario attuale.
2. Richieste per risveglio pulito con pacchetto **≤ 5**.
3. **Zero** scritture confermate perse nella prova con `kill -9`.
4. Zero errori di lettura.
5. Caso "pacchetto vecchio": dati giusti e al massimo **+3 richieste** rispetto a oggi.

### Criteri di arresto

- spesa stimata dai contatori del server oltre **2 $**;
- **una sola** lettura sbagliata o scrittura persa (si ferma tutto e si conservano i dati);
- tasso di errori S3 oltre l'1%.

**Il fondatore autorizza dopo aver letto la cifra: circa 0,22 $ di S3 (massimo 2 $ con arresto automatico) + circa 0,2 $ di EC2.**

---

## 9. Cosa non ho potuto verificare

- **Tempi su S3 vero.** Qui c'è solo un modello con ritardo fisso. Su S3 le code (p99) e la differenza tra PUT e GET cambieranno i numeri [non so di quanto].
- **Il risveglio sporco dopo un crash vero.** Qui il WAL da rigiocare era vuoto: le GET del WAL non sono contate.
- **Quanti risvegli trovano il pacchetto vecchio** in produzione (compattazioni dopo la chiusura) [non so].
- **Le opzioni C, D, E, F, G, H, I**: stimate dal codice, non misurate.
- **Il caso writer per l'opzione D** (riuso della cache RAM): non ho verificato se il writer mette blocchi WAL nella cache condivisa.
- **Il problema P1** è letto nel codice, non riprodotto con un test. **P2** è solo dedotto.
- **La modalità lease per database** (`--process-lease false`): non rimisurata. Il risveglio fa sempre GET + PUT del lease, come nel Task 4.
- **Suite di compatibilità Mongo e `kill -9` con `--wake-bundle` acceso**: non eseguiti.
- **Il comportamento di SlateDB dopo 0.17**: la verifica N+1 si basa sugli id consecutivi dei manifest. Va ricontrollata a ogni aggiornamento.

---

## 10. Raccomandazione

**In ordine:**

1. **Subito, piccolo:** correggere **P1** (lease e intento prima di togliere la protezione nel `dropDatabase` di un fork).
2. **Lettura anticipata del marker + apertura diretta con replay quando manca** (opzione B):
   - 5 richieste e 5 tappe in meno sul risveglio sporco;
   - poco codice, nessun rischio nuovo: un marker assente porta solo a un replay in più.
3. **Pacchetto nel marker (variante A)**, dopo:
   - la prova su S3 vera del §8;
   - la suite Mongo e `kill -9` con il flag acceso.

   Più la riscrittura del marker da parte della manutenzione dopo una compattazione, con PUT condizionato all'ETag. È il guadagno grande: da 13 a 4 richieste, da 10 a 3 tappe, 0 PUT in più.
4. **Cache RAM riusata tra risvegli** (opzione D), con ambito per (database, incarnazione).
5. **Incarnazione nel record del lease** (D4 di journal3z). Serve sia alla copia locale del pacchetto sia al hot move.
6. **Solo dopo:**
   - cache su disco (`CachedObjectStore` unico per processo, con svuotamento per database all'erase);
   - pacchetto nel record del lease (A2, 2 tappe).
7. **Fork di SlateDB** (G, H, I, variante B): solo se i guadagni restanti valgono il costo di mantenerla. Oggi A ottiene quasi tutto senza fork.
8. **Modelli da 10.000 database:** modello congelato + clone da un checkpoint con nome (§6). È un lavoro separato, da progettare con test sulla cancellazione.
