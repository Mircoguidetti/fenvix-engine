# M21 · Soglie e metodo (scritte prima di misurare, 2026-11-03)

## Domanda

Quante GET su S3 costa una find fredda vera (indice secondario, documenti scritti in giorni diversi), e quanto scendono con due rimedi.

## Soglie (del revisore B13, confermate dal coordinatore)

- REGGE: una find fredda tipica sotto 10 GET con almeno un rimedio, e prezzo al cliente sotto il 47% di Infinite sul profilo B di B13.
- MUORE: sopra 36 GET per find fredda tipica anche con i rimedi.
- In mezzo: FERITA.
- "Find fredda tipica" = la media pesata delle tre query sotto, con il mix dichiarato: 60% ultimi 20 di un utente, 20% un documento per _id, 20% intervallo di date di un utente. Riporto anche ogni query da sola: il verdetto guarda anche la peggiore.

## Metodo

- Niente motore toccato. Prototipo in `proto/granularita-freddi/` che usa la stessa libreria di storage del motore (SlateDB 0.17.0, object_store 0.14.2), la stessa disposizione delle chiavi (`doc/{coll}/{_id codificato}`, `idx/{coll}/{indice}/{componenti}{_id}{lunghezza}`, codifica di keyenc.rs per stringhe, date e ObjectId), le stesse impostazioni del motore (memtable/L0 8 MB, nessuna compressione, blocchi da 4 KB) e lo stesso piano di lettura del motore: scansione dell'indice con read-ahead 64 KB e max 4 fetch, poi `get_many` dei documenti con 32 GET in parallelo (`coll.rs`, `dbs.rs`).
- Finto S3: `object_store::local::LocalFileSystem` su disco, avvolto da un contatore che conta ogni GET (intera, a intervallo, HEAD) e i byte. Ogni chiamata `get_opts` è una richiesta S3 vera (lo stesso punto in cui conta `src/counting.rs` del motore).
- Lavaggio come in produzione: compattatore SlateDB a livelli per taglia (size-tiered, opzioni di default, come `compact_tenant` del motore) che gira in parallelo al caricamento. Alla fine si misura lo stato che il lavaggio ha lasciato, senza un lavaggio completo artificiale (lo riporto a parte se lo faccio).
- Bancone (cache) mio, che separa blocchi di dati da indici dei blocchi e filtri: (S0) tutto vuoto a ogni query; (S1) indici dei blocchi e filtri caldi, blocchi di dati vuoti a ogni query. Ogni query parte con i blocchi di dati vuoti.
- Latenza: il contatore può aggiungere un ritardo fisso a ogni GET (20 ms). Il tempo della query diviso 20 ms dà i "giri in fila" (GET che aspettano altre GET). La latenza su S3 vero = giri × latenza di una GET (DICHIARATA/STIMATA in NUMBERS.md).
- Carichi: (1) storico chat, utenti con attività zipf, messaggi scritti in molti giorni, indice {user:1, ts:-1}; (2) eventi per dispositivo, ogni dispositivo scrive a intervalli regolari, indice {device:1, ts:-1}. Dimensioni dichiarate nel rapporto.
- Query: Q1 ultimi 20 di un utente; Q2 un documento per _id; Q3 intervallo di date di un utente (30 giorni, al massimo 100 documenti). 300+ query per caso, utenti scelti a caso tra quelli con almeno 20 documenti.
- Rimedi: (a) dati raggruppati per utente (la chiave del documento comincia con utente e data: il lavaggio, che ordina per chiave, li mette vicini; la find per _id passa da un piccolo puntatore); (b) blocchi da 64 KB con indici dei blocchi e filtri sempre sul bancone. Anche (a)+(b).
- Bancone per 20 TB: byte di indici dei blocchi e filtri misurati per GB salvato, moltiplicati per i GB del profilo B.
- Soldi: GET al mese = 35 letture fredde al secondo × GET per find × 2.592.000 s × $0,0004/1000 (DICHIARATO, NUMBERS.md). Costo del profilo B = costo di B13 cucina condivisa ($630) meno i suoi GET ($48) più i GET misurati più il bancone in più. Prezzo = costo / 0,55. Confronto con Infinite $7.440 (centro B13) e forchetta $6.500–8.900 (revisore).

## Cosa NON misura

- Non passa dal protocollo MongoDB (il percorso BSON/wire non tocca S3). Se c'è tempo, una controprova piccola con il `mongo-server` vero.
- Non misura la latenza di S3 vero: solo i giri in fila.
- Il database è di qualche GB, non 20 TB: il numero di GET per find dipende da quante scatole sovrapposte ci sono (sorted run e L0) e da quanto sono sparsi i documenti, non dalla taglia totale; il rapporto dice dove questo non vale.
