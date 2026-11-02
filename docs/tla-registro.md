# tla-registro: che cosa è verificato con TLC, e che cosa no

Stato: lavoro del branch `tla-registro`. Specifiche, modelli e output di TLC in `tla-registro/`. Il
protocollo in prosa è `docs/protocollo.md`; le scelte di progetto in `docs/tla-registro-decisioni.md`.
Ogni numero è MISURATO (dall'output di TLC salvato in `tla-registro/output/` o
`tla-registro/journal3z-vecchio/`) o STIMATO.

Strumenti: OpenJDK 21 già presente nel contenitore; `tla2tools.jar` dalla release ufficiale
`tlaplus/tlaplus` tag `v1.8.0` (il jar scaricato si presenta come TLC versione 2026.10.30, build
rolling di quella release). Macchina: 4 core, 15 GB di RAM (MISURATO). Nessuna spesa su cloud.
Riproduzione: `tla-registro/run.sh` (tutti i modelli) o `tla-registro/run.sh <nome-modello>`.

## 1. La specifica vecchia (Journal3Z.tla, disegno con leader per database e lease)

Copiata da `origin/journal3z` in `tla-registro/journal3z-vecchio/` senza toccare quel branch. Non era
mai stata eseguita (lo dice `docs/journal3z-tla.md` di quel branch). Per farla girare sono servite
quattro correzioni meccaniche, tutte di esecuzione e nessuna di protocollo, ognuna con il proprio output
di TLC salvato (`tlc-output-1` ... `tlc-output-4`):

1. in `MC.tla` i valori `n1, n2, n3` erano usati prima della riga `CONSTANTS` che li dichiara;
2. le celle del log mescolavano record e la stringa `"T"` (cella scartata): TLC non confronta un record
   con una stringa. Ora `T` è il record `[e |-> 0, b |-> FALSE]` (le voci vere hanno epoca almeno 1);
3. in `RecvAppend`, `l0[m.prev]` veniva valutato con `prev = 0` (TLC valuta entrambi i rami di una
   disgiunzione dentro un'azione): aggiunta la guardia `m.prev >= 1`;
4. stessa cosa per la stringa `"none"` di `WEntry`, ora il record `NoEntry`.

Esito (`tlc-output-5-esecuzione-limitata.txt`), modello `MC.cfg` originale: 3 nodi, `MaxEpoch = 2`,
`MaxIdx = 2`, simmetria sui nodi, 4 worker, heap 10 GB:

- nessuna violazione delle 7 invarianti (`TypeOK`, `Durability`, `NoTwoAcks`, `OrderedEpochs`,
  `LogMatching`, `TrimSafe`, `S3Agrees`) negli stati esplorati;
- esecuzione NON completata: fermata dopo 30 minuti con 108,4 milioni di stati generati, 13,8 milioni
  di stati distinti, 7,0 milioni in coda ancora da esplorare, profondità 22 (MISURATO). La coda
  cresceva ancora: lo spazio degli stati di quel modello è molto più grande dell'ora disponibile. Il
  processo è stato ucciso dal sistema per memoria (uscita 137) quando è partito in parallelo un secondo
  TLC con heap 6 GB; il budget di un'ora per questo passo era comunque quasi finito e non è stato
  investito altro tempo.

Che cosa dice e non dice: l'esplorazione in ampiezza fino a profondità 22 non ha trovato violazioni; non
è una prova per il modello, e il modello è del disegno vecchio. La causa della dimensione è il
modello dei messaggi (insieme che non si svuota mai: ogni messaggio può essere consegnato in ritardo,
più volte o mai), che moltiplica gli interleaving. Non lo riprendiamo: il disegno è cambiato.

## 2. ValidatingLog.tla: il leader che valida

File: `tla-registro/ValidatingLog.tla`, modello `MCValidatingLog.tla`, configurazioni
`ValidatingLog-*.cfg`.

### Che cosa modella

- 3 repliche, un leader eletto come in Raft: elezione atomica con epoca nuova e voti della maggioranza,
  concessi solo a un candidato con il log almeno aggiornato quanto il votante; il nuovo leader aggiunge
  la barriera e ricostruisce dal proprio log (dalla finestra in su) le due mappe per chiave (ultima
  scrittura strutturale, ultima scrittura qualsiasi) e la tabella degli id.
- Replicazione con controllo del prefisso e troncamento delle voci in conflitto; il seguace impara la
  confermata solo sul prefisso che combacia. Conferma con 2 repliche su 3 e solo su voci dell'epoca
  corrente (regola di Raft).
- Scritture: 3 transazioni fisse su 3 chiavi. `t1` è un `$inc` su `k1` (delta, dipendenza
  `(k1, esiste)`); `t2` cancella o sostituisce `k1` e fa un delta su `k3` (dipendenze `esiste` su
  entrambe); `t3` legge `k1` (dipendenza `(k1, valore)`) e scrive `k2` con un delta. Ogni transazione
  può essere inviata al più 2 volte (ritentativi), con una `S` qualsiasi tra 0 e la posizione confermata
  più alta. Il leader valida con la regola di `docs/tla-registro-decisioni.md`: rifiuta se una
  dipendenza `esiste` ha una scrittura strutturale dopo `S`, se una dipendenza `valore` ha una
  scrittura qualsiasi dopo `S`, se `S` è sotto l'inizio della finestra o sopra il proprio log. Un id
  già in tabella non produce una seconda voce.
- La finestra del leader avanza di una posizione alla volta su voci confermate, dimenticando chiavi
  e id usciti. Una replica può essere persa (disco) una volta.
- Un leader deposto che non lo sa resta "leader" nel modello: continua ad accettare e a replicare
  verso chi ha ancora la sua epoca, e non può confermare.

### Invarianti verificate (nomi nel file)

1. `NoConflictingCommits`: tra i record confermati non ci sono due record che si sovrappongono su una
   chiave validata con la `S` del secondo minore della posizione del primo (nel modo che conta:
   qualsiasi scrittura per le dipendenze `valore`, solo strutturale per le `esiste`).
2. `StableOrder`: il prefisso confermato di ogni replica è un prefisso della storia confermata
   (variabile fantasma `cmtLog`): un cambio di leader non riordina.
3. `NoDuplicateIds` e `OneOutcome`: una transazione compare al più una volta tra i confermati e il
   client riceve al più un esito, che corrisponde alla voce confermata: un cambio di leader non duplica.
4. `CommitMonotonic` (proprietà di azione): la confermata di ogni replica e la storia confermata non
   tornano mai indietro.
5. `Durability` e `LeaderCompleteness`: ogni record confermato è nel log di una replica viva e nel
   log del leader dell'epoca più alta, anche dopo la perdita di una replica.
6. `MapsCorrect`: mappe e tabella id di ogni leader coincidono con quelle ricavabili dal suo log.

### Risultati

Modello base `ValidatingLog-base2` (tutti i controlli negativi FALSE, `MaxEpoch = 2` cioè un cambio di
leader, al più 2 voci per transazione, 1 perdita, simmetria sulle repliche, 4 worker, heap 8 GB):
esplorazione COMPLETA, nessuna violazione delle 8 invarianti né della proprietà di azione
`CommitMonotonic`. 56.450.952 stati generati, 18.264.273 stati distinti, profondità massima 32, 26
minuti e 34 secondi (MISURATO, `tla-registro/output/ValidatingLog-base2.txt`). Il grafo è completo:
entro questi limiti ogni comportamento del modello rispetta le invarianti.

Modello `ValidatingLog-sessioni` (passo 5: `RetryBeyondWindow = TRUE`, cioè il nodo d'ingresso può
ritentare un id che il leader ha dimenticato, e `SessionLookup = TRUE`, cioè legge la chiave di sessione a
`S` prima di inviare; invarianti `NoDuplicateIds` e `OneOutcome`): esplorazione COMPLETA, nessuna
violazione. 61.811.817 stati generati, 17.695.505 distinti, profondità 32, 22 minuti e 52 secondi
(MISURATO, `output/ValidatingLog-sessioni.txt`). Lo stesso modello senza `SessionLookup` è il controllo
`RetryBeyondWindow` qui sotto, che viola: la chiave di sessione chiude il buco. Un commit ritentato
restituisce il primo esito (la stessa posizione) anche dopo la morte del leader che lo aveva confermato.

Modello `ValidatingLog-base3` (`MaxEpoch = 3`, due cambi di leader, stesse altre dimensioni): esecuzione
NON completata, fermata al tetto di 40 minuti: 71.060.112 stati generati, 27.190.939 distinti, 14.115.234
ancora in coda, esplorazione in ampiezza arrivata alla profondità 18, nessuna violazione negli stati
esplorati (MISURATO, `output/ValidatingLog-base3.txt`). È evidenza limitata, non una prova: con due cambi
di leader lo spazio è almeno 3 volte quello del base e non è stato esaurito.

Controlli negativi, ognuno con una sola costante a TRUE, eseguiti con la specifica definitiva e 4 worker
(MISURATO, output in `tla-registro/output/`); i tempi sono quelli fino al primo controesempio:

- `NoValidate` (il leader non valida): `NoConflictingCommits` violata in 2 s, 6.168 stati distinti,
  profondità 10. Controesempio in parole: `t1` (`$inc` su `k1`) viene confermato alla posizione 2 con
  `S = 1`; `t3`, preparato anch'esso a `S = 1`, ha letto `k1` prima del `$inc` e viene confermato alla
  posizione 3: ha deciso su un valore già cambiato.
- `AcceptOldS` (il leader accetta `S` sotto l'inizio della finestra): `NoConflictingCommits` violata in
  10 s, 162.543 stati, profondità 13. `t1` confermato a 2; la finestra avanza fino a 3 e il leader
  dimentica che `k1` è stata scritta a 2; `t3` arriva con `S = 1`, la mappa non ricorda nulla, passa.
  È il motivo della regola "S sotto la finestra: rifiuta".
- `NoRebuild` (il nuovo leader parte con mappe e tabella id vuote): `NoDuplicateIds` violata in 8 s,
  172.856 stati, profondità 14. `t1` confermato dal leader `r1` a 2; `r2` eletto e pronto; il nodo
  d'ingresso rimanda `t1` (risposta persa); `r2` non ha `t1` in tabella e lo scrive una seconda volta
  alla posizione 4. Con la ricostruzione, `r2` risponde con la posizione 2.
- `RetryBeyondWindow` (ritentativo di un id che il leader ha già dimenticato): `NoDuplicateIds`
  violata in 7 s, 105.903 stati, profondità 15. `t1` e `t2` confermati a 2 e 3; la finestra
  avanza oltre 2 e il leader dimentica l'id di `t1`; `t1` viene rimandato con `S = 3` e scritto di
  nuovo. Non è un difetto della specifica ma del protocollo così com'è: la deduplicazione dura quanto
  la finestra (60 s). Vale per un tentativo ripetuto più di 60 s dopo l'originale senza aver mai
  ricevuto risposta. È il buco che il passo 5 (sessioni) deve chiudere; il modello base lo esclude con
  il vincolo "si ritenta solo entro la finestra".
- `AckOnOne` (conferma con 1 replica su 3): `LeaderCompleteness` violata in meno di 1 s, 50 stati, profondità
  6. `r1` eletto e scrive la barriera; `r2` eletto da `{r2, r3}` con epoca 2; `r1`, che non lo sa,
  conferma la propria barriera da solo: un record "confermato" che il leader vero non ha e che
  sparirà.
- `NoVoteRestriction` (si vota un candidato col log indietro): `LeaderCompleteness` violata in 1 s,
  257 stati, profondità 7. `r1` replica la barriera a `r2`; `r3`, senza quella voce, viene eletto con il
  voto di `r2`; `r1` conferma la barriera (era su `r1` e `r2`, maggioranza): record confermato assente
  dal log del leader nuovo. È la restrizione del voto di Raft, indispensabile.

## 3. LettureScarico.tla: letture a S e scarico su S3

File: `tla-registro/LettureScarico.tla` (estende `ValidatingLog`), modello `MCLettureScarico.tla`,
configurazioni `LettureScarico-*.cfg`.

### Che cosa modella

Il modello incorpora le revisioni segnalate dalla sessione proto-lettura (`proto/INTERFACCIA.md` di quel
branch): la risposta del PUT del manifest può perdersi e il worker ritenta; dopo un 412 il worker rilegge
il manifest prima di cancellare il proprio oggetto. La frequenza di rilettura del manifest e la cadenza
dello scarico non sono nel modello (il nodo d'ingresso legge con una vista qualsiasi tra quelle
pubblicate e il worker scarica quando vuole): le regole "rileggi solo su FUORI_FINESTRA" e "scarica per
dimensione o età" sono scelte di costo che non toccano le invarianti, e il modello lo conferma perché le
invarianti valgono per ogni vista e ogni cadenza.

- Due client con il gettone (posizione più alta vista: le `C` delle proprie scritture confermate, le
  `S` delle proprie letture); 3 chiavi; letture di una chiave a `S` da una replica qualsiasi, anche
  isolata (una replica che non riceve più nulla dal leader), con una vista del manifest anche vecchia.
  La replica serve `(M, S]` solo se `S <= confermataLocale` e la propria finestra copre `M + 1`. Il
  nodo d'ingresso compone: scritture alla chiave negli SST della vista fino a `M`, poi quelle della
  replica in `(M, S]`.
- Due worker: leggono il manifest (posizione, lista oggetti, etag), prendono da una replica i record
  confermati da `M` a una posizione a scelta, scrivono l'oggetto con nome unico (posizione iniziale,
  finale, worker), poi il manifest con PUT condizionale sull'etag. Le repliche possono scartare dal disco
  fino alla posizione del manifest.

### Invarianti verificate

- `ReadsCorrect`: le scritture restituite da una lettura a `S` sono esattamente le scritture confermate
  alla chiave con posizione minore o uguale a `S` (nell'ordine).
- `MonotonicReads`: nessuna lettura servita è più vecchia del gettone del client.
- `UploadExact`: il contenuto di S3 secondo il manifest corrente è esattamente il prefisso confermato
  fino alla posizione del manifest: nessun record perso, nessuno duplicato.
- `NothingLost`: nulla scartato dal disco che non sia in S3; `ManifestMonotonic`: la posizione del
  manifest non torna indietro.

### Risultati

Modelli base, nessun controllo acceso. Le dimensioni sono state ridotte in due passi perché il modello
pieno non si esaurisce in tempi utili:

- `LettureScarico-base1` (3 transazioni, nessun cambio di leader, 1 lettura, 1 client, 2 worker, nessuna
  perdita, risposta del PUT che può perdersi): esecuzione NON completata, fermata al tetto di 60 minuti
  con 4 worker: 126.050.517 stati generati, 11.014.095 distinti, 4.310.371 in coda, profondità 22
  raggiunta, nessuna violazione negli stati esplorati (MISURATO, `output/LettureScarico-base1.txt`).
  Evidenza limitata.
- `LettureScarico-base0` (come `base1` ma con 2 transazioni, `t1` e `t2`): esecuzione NON completata,
  fermata al tetto di 40 minuti con 4 worker: 88.932.540 stati generati, 5.801.771 distinti, 299.301 in
  coda (in discesa: il grafo era quasi esaurito), profondità 32 raggiunta, nessuna violazione negli
  stati esplorati (MISURATO, `output/LettureScarico-base0.txt`). Un'esecuzione con un tetto più lungo
  dovrebbe completarlo (STIMATO: circa un'ora su questa macchina).
- `LettureScarico-base2` (con un cambio di leader e 2 letture): avviato e interrotto dopo pochi minuti
  per far posto agli altri; nessun risultato utile, resta come configurazione per una macchina più
  grande (`output/LettureScarico-base2-interrotto.txt`, prodotto da una versione precedente della
  specifica).

La lettura da replica isolata, la replica deposta che ha una voce poi sostituita, il manifest
condizionale e i due worker sono nei controlli negativi qui sotto, che con la specifica definitiva
producono il controesempio atteso in pochi secondi ciascuno.

Controlli negativi (MISURATO):

- `UnsafeServe` (la replica serve se ha `S` in coda, ignorando la confermata): `ReadsCorrect` violata
  in 2 s, 499 stati, profondità 6. Il leader scrive `t1` (`k1`) a 2 solo su se stesso; una lettura di
  `k1` a `S = 2` dalla replica leader restituisce la scrittura 2, che non è confermata e può sparire.
  Con la regola sicura la replica risponde "non ho ancora S".
- `IgnoreToken` (il nodo d'ingresso ignora il gettone): `MonotonicReads` violata in 2 s, 164 stati,
  profondità 6: un client che ha visto la posizione 2 riceve una lettura a 1.
- `UnconditionalPut` (manifest scritto senza condizione sull'etag): `ManifestMonotonic` violata in 18
  s, 50.545 stati, profondità 13. Due worker partono dallo stesso manifest a 0; `w2` scarica fino a 2 e
  pubblica; `w1`, che aveva preso solo fino a 1, pubblica dopo e il manifest torna a 1. Se le repliche
  avevano già scartato fino a 2, il record 2 non è più da nessuna parte.
- `NameByStart` (oggetto SST chiamato con la sola posizione iniziale): `UploadExact` violata in 9 s,
  19.231 stati, profondità 12. Due worker scrivono entrambi l'oggetto "0": uno con i record fino a 2,
  l'altro fino a 1; vince il manifest di quello a 1 ma l'oggetto contiene 2 record: il contenuto non
  corrisponde al manifest. I nomi degli oggetti devono essere unici per tentativo.
- `UnsafeFetch` (il worker prende dalla replica anche record non confermati): `UploadExact` violata
  in 2 s, 142 stati, profondità 5: finisce in S3 la barriera non ancora confermata di un leader.
- `DeleteOrphanBlind` (dopo un PUT condizionale fallito il worker cancella il proprio oggetto senza
  rileggere il manifest): `UploadExact` violata in 2 s, 807 stati distinti. Il PUT del manifest riesce
  ma la risposta si perde; il worker ritenta, riceve il 412 del proprio stesso PUT, crede di aver perso
  la gara e cancella un oggetto che il manifest elenca. Con la regola "rileggi il manifest prima di
  cancellare" (punto segnalato da proto-lettura, `docs/protocollo.md` §7.1 punto 5) l'invariante tiene
  nel modello base, che include la perdita della risposta.

## 4. Che cosa NON è coperto

- (Corretto al giro 2: la chiusura delle sessioni è modellata, vedi `ValidatingLog-sessioni` nella
  sezione 2 e `LettureScarico-sessioni` nella sezione 5.)
- Cambio dell'insieme delle repliche e rientro di una replica che ha perso il disco: nel modello la
  perdita è permanente.
- Rete esplicita: la replicazione è modellata come copia atomica di una voce dopo il prefisso comune
  (è ciò che il controllo del prefisso di Raft garantisce), senza messaggi duplicati o in ritardo.
  Le epoche e il controllo del prefisso sono però modellati, incluso il leader deposto che continua.
- Liveness: niente è dimostrato sul fatto che un commit o una lettura alla fine riescano; solo
  sicurezza.
- Il contenuto dei valori: una scrittura è identificata dalla sua posizione; la composizione dei delta
  (semantica degli operatori di MongoDB) non è modellata, solo la sequenza di scritture che il nodo
  d'ingresso riceve, che è ciò che la composizione deve applicare.
- La raccolta degli oggetti SST orfani, la compattazione, i database multipli (un solo manifest nel
  modello), i tempi (nessun orologio nel modello: la finestra avanza per posizione).
- Dimensioni: 3 repliche, 3 chiavi, 3 transazioni fisse (una per classe), al più 2 tentativi, 1-2 cambi
  di leader, 1 perdita, 2 letture, 2 worker. Nessuna garanzia oltre questi limiti.
- Il modello RetryBeyondWindow e i quattro modelli con `MaxEpoch = 1` o `MaxReads = 0` sono ridotti
  rispetto al base per trovare il controesempio in tempi brevi: lo dicono le rispettive configurazioni.

## 5. Giro 2 (1–2 novembre 2026): correzioni del revisore, disco, bollettino

Sessione nuova al posto di S1, sulle correzioni di `fenvix-brain/sessions/2026-11-01-revisore-s1/`.
Macchina: 2 core, 7 GB di RAM (MISURATO), metà di quella del primo giro: i modelli sono più piccoli.
TLC `tla2tools.jar` v1.8.0, Java 21, 2 worker, heap 5 GB. Tutti i numeri qui sotto sono MISURATI
dagli output in `tla-registro/output/`, salvo dove è scritto STIMATO.

### 5.1 Che cosa è cambiato nelle specifiche

`ValidatingLog.tla`:

- **APPEND con `posPrecedente` qualsiasi** (opzione `OldPrev`): `Replicate(l, f, p, n)` con `p` sotto
  il punto in cui i log combaciano e `n` in 0..1 (`n = 0` è il battito; il vecchio `Heartbeat` è
  sparito). Il seguace tronca solo dal primo conflitto e **tiene la propria coda** se le voci ricevute
  combaciano: è la semplificazione del primo giro (coda sempre troncata) che nascondeva l'errore di
  §5.3 punto 6. La confermata del seguace è `min(confermata, p + n)` (regola di Raft).
- **`diskStart`** per replica (inizioDisco): le voci sotto restano nella sequenza solo come
  (posizione, epoca), come lo snapshot di Raft; il leader non manda voci sotto il proprio `diskStart`.
- **Finestra dei seguaci che avanza** (opzione `FollowerWindow`).
- `Submit` diviso in `SubmitAt(l, t, s, seen)`, così che `LettureScarico` possa dare la `S` e la lettura
  della chiave di sessione da una lettura vera.
- Controlli negativi nuovi: `CommitByTail`, `ValidateCommittedOnly`, `RebuildCommittedOnly`,
  `CommitOldEpoch`.

`LettureScarico.tla`:

- `TrimR(r)`: la replica cancella dal disco fino a `manifest.pos + 1`, mai sopra la propria finestra.
  `Reload(f)`: la ricarica da S3 di un seguace rimasto sotto l'`inizioDisco` del leader.
- La replica serve **dal disco** (`potatoFinoA = inizioDisco`), e restituisce comunque solo ciò che ha
  (le voci sotto `diskStart` non ci sono più): così la guardia è messa alla prova davvero.
- `LinRead(c, l)`: la lettura lineare del revisore, con la regola "solo se `pronto`".
- `SubmitFromRead` (opzione `ReadPrepared`): la `S` di ogni scrittura è quella di una lettura composta
  servita davvero, e la chiave di sessione si legge con la stessa lettura (scatole della vista più
  quaderno), non con l'oracolo; la lettura deve essere esatta (`readOk`).
- Invarianti nuove: `DurableOrS3` (al posto di `Durability`), `ReadServable`, `TokenConfirmed`,
  `NothingLost` riscritta per replica.
- Controlli negativi nuovi: `ServeRamOnly`, `NoServeGuard`, `LinNotReady`.

`Bollettino.tla` (nuovo, sopra `ValidatingLog`): 2 database con il loro `M`, scarico astratto (la
correttezza dello scarico è già in `LettureScarico`), disco che si svuota fino alla prima voce non
scaricata, ricarica da S3, riavvio di una replica (confermataLocale a 0), un cameriere che si iscrive
(fotografia da finestra **e** disco, più `M` del database), riceve gli annunci voce per voce e la
confermata solo fino a ciò che ha annunciato, si stacca, adotta manifest nuovi potando le chiavi per
posizione, e legge senza chiedere al quaderno. Invarianti: `ReadsCorrect` (la lettura senza domanda dà
lo stesso risultato della lettura completa alla stessa `S`), `BollettinoVero`, `ChiaviVere`,
`SapereConfermato`, `NothingLost`, `StableOrder`.

### 5.2 Controlli negativi (ognuno con una sola regola rotta; tutti trovati)

Revisore della specifica (i suoi 1–4 e 6, più `CommitOldEpoch`):

- `CommitByTail` (§5.3 punto 6 del primo giro): `StableOrder` violata in 1 s, 73 stati distinti,
  traccia di 6 stati. r1 eletto (epoca 1) scrive la barriera solo su sé; r2 eletto con r3 (epoca 2)
  conferma la propria barriera alla stessa posizione 1; manda a r1 un battito con `posPrecedente = 0`;
  r1 prende `min(1, 1) = 1` e crede confermata la propria voce dell'epoca 1, mai confermata.
- `ValidateCommittedOnly`: `NoConflictingCommits` violata in 2 s, 4.602 stati distinti.
- `RebuildCommittedOnly`: `NoDuplicateIds` violata in 7 s, 52.907 stati distinti.
- `LinNotReady`: `ReadsCorrect` violata in 2 s, 2.617 stati distinti: un capo appena eletto, non ancora
  pronto, risponde a `POSIZIONE` con una confermata più bassa di una scrittura già confermata al client.
- `ServeRamOnly` (`potatoFinoA = inizioFinestra - 1`): `ReadServable` violata in 3 s, 4.045 stati
  distinti: tutte le finestre superano `M` senza scarico, nessuno serve più il database.
- `CommitOldEpoch`: con 4 epoche `LeaderCompleteness` violata in meno di 1 s, 161 stati distinti; con 3
  epoche **non trovata**: modello `ValidatingLog-commitoldepoch3` COMPLETO senza violazioni, 589.806
  stati distinti, profondità 30, 2 min 58 s. È la misura che serve un modello con 4 epoche.

Nuovi del giro 2:

- `NoServeGuard` (la replica serve anche se ha già cancellato dal disco parte di `(M, S]`):
  `ReadsCorrect` violata in 57 s, 196.997 stati distinti. Un cameriere con un manifest vecchio (`M = 0`)
  legge da una replica che ha già cancellato fino a 2: la lettura salta la scrittura 2.
- `ReadPrepared` con `NoServeGuard`: `ReadsCorrect` violata in 3 s (la `S` della scrittura viene da una
  lettura sbagliata).
- `LettureScarico-sessioni-senza` (stessa configurazione di `LettureScarico-sessioni` ma senza la
  lettura della chiave di sessione): `NoDuplicateIds` violata in 15 s, 58.238 stati distinti. Serve a
  mostrare che il modello delle sessioni con la lettura vera vede il buco.

Bollettino (i sei del revisore di letture e affinità, più tre del revisore della specifica):

- `FattoSottoS`: `ReadsCorrect` violata in 2 s, 5.480 stati distinti.
- `AnnuncioDopoConferma`: `BollettinoVero` violata in 6 s, 33.784 stati distinti.
- `FotografiaSoloFinestra` (errore 1 del revisore): `BollettinoVero` violata in 12 s, 95.465 stati.
- `FotoSenzaM` (fotografia che non dice fino a dove il database è scaricato, al posto di
  `FotografiaDopoTrim`): `BollettinoVero` violata in 1 min 39 s, 966.399 stati distinti.
- `SvuotaChiaviAlManifest` (errore 2): `ChiaviVere` violata in 10 s, 97.850 stati distinti.
- `ScartaAnnunciVecchi` (errore 3, con il riavvio della replica): `BollettinoVero` violata in 28 s,
  87.161 stati distinti (modello ridotto: 1 transazione, nessun cambio di capo).
- `FotoSostituisce` (`RiavvioReplica`: alla nuova iscrizione la fotografia sostituisce i toccati invece
  di unirsi): `BollettinoVero` violata in 2 s, 3.410 stati distinti. Nota: se la fotografia sostituisce
  **anche** `sapereFinoA` (lo abbassa a quello della replica nuova), il modello ridotto è COMPLETO senza
  violazioni (885.336 stati distinti, 7 min 56 s; output non conservato, sovrascritto dalla corsa
  successiva): l'errore vero è mescolare fatti nuovi con un `sapereFinoA` vecchio.
- `SMinoreDiM` (correzione 7): `ReadsCorrect` violata in 6 s, 9.455 stati distinti.
- `CapoNuovoSenzaAnnunci`: RISULTATO_CAPONUOVO

### 5.3 Modelli completi e limitati

RISULTATI_POSITIVI

### 5.4 Che cosa resta non coperto

- La rete esplicita (messaggi persi, duplicati, in ritardo): il giro 2 aggiunge il caso che conta di
  più, il battito con `posPrecedente` vecchio, ma resta una copia atomica.
- Riconfigurazione dei membri e rimpiazzo della copia caduta; spostamento di un database tra registri;
  transazioni tra registri (fuori al lancio); raccolta di famiglia dei rami e taglio coordinato.
- Liveness: niente. `ReadServable` è solo la forma di invariante del "le letture si possono servire".
- Orologio ibrido: il modello usa gli indici; la regola "indice e posizione crescono insieme"
  (`docs/protocollo.md` §2) è ciò che rende valide le prove anche per le posizioni.
- Tetto di memoria, cantina, freno, scarico per pressione o d'emergenza: sono regole di disponibilità;
  nel modello sono coperte solo in quanto lo scarico e la cancellazione dal disco possono avvenire in
  qualunque momento (ogni cadenza è un comportamento del modello) e la cantina è un indice di ciò che è
  già sul disco.
- Il bollettino con la strada dei metadati (correzione 8), la regola `T_max` (tempo), il livello 3
  (copia della finestra).
- Le voci degli indici non unici ricalcolate dai documenti (`docs/protocollo.md` §4.7): prosa, non
  modello.
