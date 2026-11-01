# Interfaccia comune tra proto-quaderno (S2, registro) e proto-lettura (S3, letture)

Questa copia sta sul branch `proto-lettura`. Il testo di riferimento è `proto/INTERFACCIA.md` del branch
`proto-quaderno` (commit 0ceee47, invariato al 68b5d4f), che è già implementato là. proto-lettura lo segue; i tipi
sono in `proto/lettura/src/iface.rs` con gli stessi nomi e la stessa codifica di `proto/quaderno/src/codec.rs`
(posizione, tipo put/delete/delta, delta = i64 little-endian). Il registro di proto-lettura è **simulato** dietro il
tratto `Registro` (`proto/lettura/src/registro.rs`, in processo o via TCP in `net.rs`): un solo processo, ogni commit
confermato subito, finestra di 60 s in RAM, log facoltativo su file per la verifica.

Più sotto: le **differenze** di proto-lettura rispetto alla versione di proto-quaderno, e due punti di
`docs/protocollo.md` (branch `tla-registro`) che le misure di costo del passo 5 sconsigliano.

## Tipi (invariati)

- **Posizione** (u64): orologio logico ibrido del leader, 48 bit di millisecondi + 16 bit di contatore,
  `max(ultima + 1, adesso_ms << 16)`. Timestamp BSON: `secondi = ms / 1000`,
  `incremento = ((ms % 1000) << 16) | contatore`.
- **Chiave**: id del database (16 byte) + chiave dello spazio unico del motore (`doc/...` o `idx/...`).
- **Valore**: byte opachi; `delta` = i64 LE da sommare al valore precedente letto come i64 LE (assente = 0).

## Richiesta di commit e risposta (invariate)

`CommitReq { txn, db, read_pos, mode: validata|cieca, writes: [(chiave, put|delete|delta, valore)], reads, session }`
→ `committed(C)` | `conflict(chiave)` | `ambiguous`. Il registro simulato implementa la validazione come
proto-quaderno (conflitto se una chiave scritta o letta ha una versione > `read_pos`, o se `read_pos` è sotto
l'inizio della finestra) e la deduplicazione per id e per (lsid, txnNumber) finché il record è nella finestra.

## Replica

- `get(db, chiave, S)` → `valore(pos)` | `assente(pos)` | `fuori_finestra` | `indietro(pos)` (come proto-quaderno).
- `scan(db, da, a, S, limite)`, `posizione_confermata()`, `attendi(C, timeout)`: come proto-quaderno.

## Scarico

- `scaricabili(dbs, fino_a, max_byte)`, `conferma_scarico([(db, fino_a)], etag)`, `pendenti(limite)`: come
  proto-quaderno, con più database per richiesta.

## Differenze di proto-lettura (proposte; nessun punto del testo concordato è cambiato)

1. **`fuori_finestra` porta `potato_fino_a` (ed `etag`)**. `potato_fino_a` è la posizione fino alla quale i record
   del database sono usciti dalla finestra. Chi compone la lettura deve usare un manifest con
   `scaricato[db] >= potato_fino_a`; se quello in cache è più indietro, lo rilegge. È lo stesso controllo di
   `docs/protocollo.md` §6.2 punto 3 (`M >= inizioFinestra`), ma fatto dal nodo d'ingresso invece che dalla replica,
   e senza un giro in più. `etag` è l'ultimo manifest confermato al registro (informativo). Senza questo campo il
   nodo d'ingresso non sa quando il manifest in cache non basta più, e deve rileggerlo a intervalli fissi (vedi
   sotto: costa troppo).
2. **Nuovo esito `delta(pos, somma, potato_fino_a)`**. In proto-quaderno, se la finestra ha solo delta per una
   chiave e la base è negli SST, la replica risponde `fuori_finestra`: ma i delta della finestra non ancora su S3
   non sono negli SST, quindi la lettura li perderebbe (il "limite noto" scritto in INTERFACCIA.md di
   proto-quaderno). Con `delta` la replica restituisce la somma dei delta ≤ S e il nodo d'ingresso la applica al
   valore letto dagli SST. Implementato nel registro simulato e nel lettore.
3. **`stato_scarico(db)` → `(scaricato, etag, potato_fino_a)`**: serve alla scansione composta (per sapere quale
   manifest basta) e al worker. In proto-quaderno l'informazione c'è in `pendenti` solo per i database con record
   non scaricati.
4. **`pendenti` porta anche `scaricato` del registro**: il worker lo confronta con `scaricato` del manifest per
   riparare il caso "worker morto tra PUT del manifest e `conferma_scarico`" senza una richiesta in più per database.
5. **Manifest per gruppo**: `conferma_scarico` riceve l'etag di un manifest che può coprire più database (variante
   "oggetto condiviso" del passo 5). Già compatibile con la firma multi-database di proto-quaderno.

## Due regole di `docs/protocollo.md` da rivedere (misure del passo 5 in `docs/RAPPORTO-proto-lettura.md`)

- §6.2 punto 1: "se la vista del manifest è più vecchia di qualche secondo (5 s), rileggila da S3". Per un nodo
  d'ingresso che serve 1.000 database sono 1.000 GET ogni 5 s: circa 518.000 GET al mese per database e per nodo
  d'ingresso, $0,21 al mese per database solo di GET (STIMATO), 4 volte la soglia di $0,05. Con il punto 1 delle
  differenze il manifest si rilegge solo quando la finestra non copre più la distanza (una volta per scarico).
- §7.1: "ogni pochi secondi (5 s)" per database: un PUT di SST + un PUT di manifest ogni 5 s sono circa 1.036.800 PUT
  al mese, $5,18 al mese per database (STIMATO). Per i database piccoli lo scarico va fatto per dimensione o età lunga,
  oppure in oggetti condivisi (passo 5).
- §7.1 punto 5: "se il PUT condizionale fallisce [...] il proprio SST è un orfano da cancellare (subito ...)".
  Cancellarlo subito è sicuro solo dopo aver riletto il manifest: se il 412 arriva al ritentativo automatico del
  client HTTP di un PUT già riuscito (risposta persa), l'SST è nel manifest e cancellarlo perde dati. Il worker di
  proto-lettura rilegge il manifest prima di cancellare (`conflitti_vinti_dopo` nelle statistiche).
