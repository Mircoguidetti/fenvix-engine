# Interfaccia comune tra proto-quaderno (S2, registro) e proto-lettura (S3, letture)

Stato: implementata in `proto/quaderno` (codifica in `proto/quaderno/src/codec.rs`, client in
`proto/quaderno/src/client.rs`). Le aggiunte rispetto al testo concordato sono marcate **[aggiunta]** e riportate anche
nel rapporto `docs/proto-quaderno.md`. Nessun punto del testo concordato è stato cambiato.

## Tipi

- **Posizione** (`Pos`, u64): orologio logico ibrido assegnato dal leader. 48 bit alti = millisecondi dall'epoca Unix,
  16 bit bassi = contatore. Il leader assegna `max(ultima posizione + 1, adesso_ms << 16)`: strettamente crescente
  lungo il registro, anche attraverso i cambi di leader (il nuovo leader parte dall'ultima posizione del suo registro).
  Se il contatore supera 65.535 nello stesso millisecondo, il riporto passa nei millisecondi (resta monotona).
  - Conversione in Timestamp BSON: `secondi = ms / 1000`, `incremento = ((ms % 1000) << 16) | contatore`
    (massimo 65.535.999 < 2^32). È monotona e reversibile (`pos_to_bson_ts`, `bson_ts_to_pos`).
- **Chiave**: id del database (16 byte) + chiave dello spazio unico del motore (`doc/...` o `idx/...`). Sul filo l'id
  del database viaggia una volta per richiesta, la chiave dello spazio unico per ogni scrittura.
- **Valore**: byte opachi. Tipo `delta` **[precisazione del prototipo]**: il valore è un i64 little-endian da sommare al
  valore precedente letto come i64 little-endian (assente = 0), come `$inc` su un campo. Un motore vero userà un delta
  BSON; il registro deve solo saperlo comporre in ordine.

## Richiesta di commit

`CommitReq { txn: [u8;16], db: [u8;16], read_pos: Pos, mode: validata|cieca, writes: [(chiave, put|delete|delta,
valore)], reads: [chiave] (opzionale, vuoto = nessuna), session: (lsid, txnNumber) (opzionale) }`

- **validata**: il leader controlla ogni chiave scritta e ogni chiave dichiarata in `reads`: conflitto se la chiave ha
  una versione con posizione > `read_pos` (anche una versione accettata ma non ancora confermata: viene prima
  nell'ordine). Conflitto anche se `read_pos` è più vecchia dell'inizio della finestra del database (non si può più
  sapere): si rilegge e si riprova.
- **cieca**: nessun controllo; le scritture (put/delete/delta) si applicano nell'ordine del registro.
- Tutte le scritture di una richiesta sono dello stesso database **[semplificazione]**: transazioni su più database
  non sono coperte dal prototipo.

## Risposta

`committed(C)` | `conflict(chiave)` | `ambiguous`.
- `ambiguous`: il leader ha perso la guida con la voce ancora in volo (o il client non ha avuto risposta). Si rinvia
  **con lo stesso id**: se la voce era entrata nel registro si ottiene la stessa `C`; altrimenti viene validata e
  messa in ordine adesso. Il rinvio è riconosciuto finché il record è nella finestra (60 s dopo lo scarico su S3).
  Stessa regola per la coppia (lsid, txnNumber).
- **[aggiunta, giro 3] `freno(retry_ms, motivo)`**: contropressione. Il commit NON è entrato nel registro; si ritenta
  dopo `retry_ms` con lo stesso id. Motivi: 1 rallentato (il database scrive troppo e la coda di attesa ha superato
  100 ms), 2 disco del registro sopra la soglia di rifiuto, 3 sola lettura (disco sopra l'ultima soglia), 4 memoria del
  quaderno sopra il tetto. Per il driver MongoDB equivale a un errore con etichetta `RetryableWriteError`.
- **[aggiunta di trasporto]** `not_leader(id del leader o -1)`: la replica non è il leader; il client va dal leader
  indicato. Non è un esito: la richiesta non è entrata nel registro.

## Replica (ogni replica, non solo il leader)

- `get(db, chiave, S) -> valore(posizione della versione) | assente | fuori_finestra`.
  - Valido per `S ≤` posizione confermata nota alla replica. Se la replica non ha ancora applicato `S`, aspetta fino a
    `wait_ms` **[parametro aggiunto]**, poi risponde **`indietro(posizione confermata nota)` [aggiunta]**: è il caso
    della replica isolata, che così non serve mai letture vecchie; il lettore prova un'altra replica.
  - `fuori_finestra(potato_fino_a, etag)`: la finestra non basta (la chiave non ha versioni nella finestra, oppure
    `S` precede la prima versione rimasta): la risposta sta negli SST. **[aggiunta di proto-lettura, accettata]**
    `potato_fino_a` = posizione fino alla quale i record del database sono usciti dalla finestra: chi legge deve
    usare un manifest con `scaricato >= potato_fino_a`; `etag` = ultimo manifest confermato al registro.
  - `assente` con posizione 0 si ha solo per un database la cui storia è interamente nella finestra (nato dopo
    l'avvio del registro, mai potato).
  - Delta: la replica compone la catena di delta fino al primo put/delete. Se la base è fuori dalla finestra
    risponde **`delta(pos, somma, potato_fino_a)` [aggiunta di proto-lettura, accettata con una correzione]**: il
    valore è (valore negli SST al manifest usato) + `somma`. **Correzione**: un delta resta nella finestra fino a 60 s
    anche dopo essere finito su S3, quindi la somma va fatta solo sui delta che il manifest di chi legge non contiene
    già. Per questo `get` e `scan` hanno il parametro facoltativo **`dal`** = `scaricato` del manifest che chi legge
    userà: `somma` = delta in `(dal, S]`. Se `dal < potato_fino_a` la risposta è `fuori_finestra` (manifest troppo
    vecchio, va riletto). Con `dal = 0` la somma comprende tutti i delta della finestra ≤ S (corretta solo se il
    manifest usato ha `scaricato` = `potato_fino_a`).
  - **[aggiunta]** `get` con `spec = vero`, solo sul leader: ultima versione accettata, anche non confermata. Serve
    alla modalità validata sulla chiave calda (banco 3): il valore letto non arriva mai all'utente se il commit che ne
    dipende non viene confermato (le voci si confermano in ordine).
- `scan(db, da, a, S, limite)`: le chiavi della finestra in `[da, a)` con il loro esito alla posizione `S` (stesse
  regole di `get`). Chi compone la lettura le fonde con gli SST.
- `posizione_confermata()`: **lineare**. Il leader risponde dopo un giro di heartbeat confermato dalla maggioranza
  (ReadIndex di Raft) e solo dopo aver confermato una voce del proprio mandato: ogni commit confermato prima della
  chiamata ha posizione ≤ del risultato. Un follower risponde `not_leader` **[precisazione]**.
- `attendi(C, timeout)`: ritorna la posizione confermata della replica appena è ≥ `C`, altrimenti `indietro` allo
  scadere.

## Scarico

- `scaricabili(db, fino_a) -> record`: i record confermati del database con posizione in `(già_su_S3, min(fino_a,
  posizione confermata)]`, in ordine, con le loro scritture. **[trasporto]** si possono chiedere più database in una
  richiesta; la risposta è limitata a ~4 MB: chi scarica conferma fino all'ultima posizione ricevuta e richiede.
- `conferma_scarico(db, fino_a, etag_del_manifest)`: va al leader, entra nel registro come voce di servizio e si
  applica alla conferma su tutte le repliche. Un record esce dalla finestra quando è su S3 **e** più vecchio di 60 s.
  **[trasporto]** più database per richiesta.
- **[aggiunta]** `pendenti(limite)`: database con record non ancora su S3: posizione del più vecchio e
  **[aggiunta di proto-lettura, accettata]** numero di record e byte non su S3 e `scaricato` del registro;
  **[aggiunta, giro 3]** byte non su S3 ancora in RAM (non passati alla dispensa su disco).
- **[aggiunta, giro 3]** `status` riporta il conto unico della memoria del quaderno e il tetto, il disco usato e le
  soglie del freno, la dispensa (run e byte), l'età del record non su S3 più vecchio, l'allarme e lo stato del freno.
  Lo scaricatore lo usa per lo scarico per pressione (sopra il 70% del tetto si scarica subito, dai database con più
  byte non su S3, con un limite di velocità).
- **[aggiunta di proto-lettura, accettata]** `stato_scarico(db...) -> (scaricato, etag, potato_fino_a)` per ogni
  database chiesto.
- **[proposta di proto-lettura, già compatibile]** un manifest può coprire più database: `conferma_scarico` riceve lo
  stesso etag per tutti (lo scaricatore del prototipo fa già così, 16 gruppi più il database caldo da solo).

## Giro 4 (S16): lotti, freno, compressione

- `CommitBatch(Vec<CommitReq>)`: più commit in un messaggio dal cameriere al capo. Risposta `Batch(Vec<Resp>)` con un
  esito per commit nello stesso ordine, oppure una risposta sola (`NotLeader`, `Busy`) che vale per tutto il lotto. I
  commit di un lotto passano dal thread principale in un evento solo, quindi vanno nello stesso giro di disco
  (fdatasync) e nello stesso messaggio di replica. Nel registro restano una voce per commit.
- Freno: con il livello 2 o più i commit (anche in lotto) si rifiutano già nel thread di rete con `Busy`.
- Client: dopo un `Busy`, se l'attesa suggerita più un tentativo intero (1,5 s) non sta prima della scadenza, il
  commit finisce subito come rifiutato (errore ritentabile), mai con un ultimo tentativo di pochi ms.
- Replica compressa (facoltativa, `PQ_ZSTD=livello`): un messaggio tra repliche che inizia con il byte 0x5a è
  `0x5a | lunghezza originale u32 | zstd(messaggio)`.
