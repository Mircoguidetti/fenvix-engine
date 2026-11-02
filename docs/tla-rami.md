# tla-rami: la raccolta di famiglia sulla carta

Stato: lavoro del branch `tla-rami` (nato da `tla-registro`). Specifica, modelli e output di TLC in
`tla-rami/`. Compito: raccomandazione 2 del rapporto `sessions/2026-11-01-revisore-database-rami` di
fenvix-brain. Solo carta: nessun cambio al motore, nessuna spesa cloud.

Ogni numero è MISURATO (dall'output di TLC salvato in `tla-rami/output/`) o STIMATO.
Strumenti: OpenJDK 21 del contenitore, `tla2tools.jar` della release ufficiale v1.8.0 di
`tlaplus/tlaplus` (si presenta come TLC 2026.10.30). Macchina: 2 core, 7 GB di RAM (MISURATO).
Riproduzione: `tla-rami/run.sh` (tutti) o `tla-rami/run.sh Famiglia-base` (uno). I file `.cfg` si
rigenerano con `python3 tla-rami/gen.py`.

## 1. Che cosa modella `Famiglia.tla`

Oggetti su S3:

- **scatole** (SST): immutabili, ognuna con il suo proprietario (il prefisso del database che l'ha
  scritta) e il giro in cui è nata;
- **un foglio per database** (manifest): l'insieme di scatole che il database legge;
- **il registro della famiglia**: un solo oggetto con i membri e le **spille**. Una spilla trattiene un
  insieme di scatole: è un fork in corso (le scatole della sorgente al momento del fork) o un punto di
  ripristino (le scatole del foglio in quell'istante).

Attori e azioni:

- **scrittura** di un database in due passi: prima la scatola nuova su S3, poi il foglio (scarico o
  lavaggio). Il lavaggio può fondere qualsiasi 1 o 2 scatole del foglio, anche ereditate: così il ramo
  si "stacca" dal padre. Una scrittura può anche fallire a metà e lasciare la scatola orfana;
- **fork** in quattro passi: spilla nel registro (insieme alla lettura delle scatole della sorgente),
  foglio del ramo, ingresso del ramo nella famiglia (lo stesso PUT del registro toglie la spilla),
  oppure annullamento a qualsiasi passo. La sorgente è il foglio attuale di un membro **o un punto di
  ripristino**: il ripristino a un istante come "ramo da un segnalibro vecchio". I rami di rami sono
  ammessi;
- **punto di ripristino**: creazione (spilla del foglio attuale) e scadenza;
- **cancellazione di un database**: esce dal registro con i suoi punti, il suo foglio sparisce, le sue
  scatole restano su S3;
- **il raccoglitore della famiglia**, in passi separati come su S3: LIST delle scatole (candidati),
  lettura del registro, lettura dei fogli dei membri uno alla volta, nuova lettura del registro (se è
  cambiato rinuncia al giro), poi cancella i candidati che nessun foglio letto e nessuna spilla usa e
  che ha visto inutilizzati in `Grace` giri di fila;
- **lettori** (camerieri) che prendono il foglio di un membro e lo tengono, anche vecchio, prima di
  leggere le scatole. Con la regola del 404 un lettore che trova una scatola sparita rilegge il foglio.

Il tempo è contato in **giri completati del raccoglitore**. `Hold` è quanti giri può durare una
scrittura in volo; `RHold` quanto è vecchio al massimo il foglio di un lettore senza la regola del 404;
`Grace` è l'età minima: i giri passati da quando la scatola è uscita dall'ultimo foglio o dall'ultima
spilla (o dalla nascita, se non è mai entrata in un foglio). Chi toglie una scatola (lavaggio,
cancellazione del database, scadenza del punto, fork annullato) scrive anche quando l'ha tolta.
Il raccoglitore rinuncia al giro anche quando il suo giro è durato troppo (è già passato un giro
intero dal suo LIST): con un solo raccoglitore non succede mai, con due sì.

Invarianti (nomi nel file):

1. `NoDangling`: nessun foglio vivo (membro, o ramo già scritto e non ancora membro) punta a una
   scatola cancellata. È "il ramo non perde dati".
2. `PinsLive`: ogni spilla (fork in corso, punto di ripristino) punta solo a scatole vive. È "il punto
   di ripristino funziona ancora".
3. `ReadersOK`: nessun lettore riceve "scatola non trovata" su un database che esiste ancora. È il
   "sst not found" già visto nel motore.
4. `InFlightLive` e `TypeOK`: controlli di contorno.

Dimensione del modello `base`: database {d1, d2} (d1 radice), 3 scatole, 1 punto di ripristino,
1 lettore, 1 raccoglitore, 3 giri, `Grace = 1`, `Hold = 0`, `RHold = 1`. Varianti: `reread` (regola
del 404, lettori con fogli vecchi quanto si vuole), `catena` con 3 database (rami di rami) e nessun
lettore, `due-raccoglitori` (senza lettore), `grande` con `Grace = 2`, `Hold = 1`, `RHold = 2` e 4 giri. Per stare nella
memoria la configurazione usa una `VIEW`: due stati che differiscono solo in campi che nessuno leggerà
più (i dati di un'operazione già finita, sovrascritti dalla prossima prima di essere letti) contano
come uno. Non toglie comportamenti.

## 2. Esiti

Tutti i numeri sono MISURATI dagli output in `tla-rami/output/` (un file per modello, stesso nome del
`.cfg`). Una ricerca "completa" vuol dire che TLC ha esplorato tutti gli stati raggiungibili del
modello, non una parte.

Modelli con le regole giuste, devono passare:

- `base`: nessuna violazione, ricerca completa, 4.118.114 stati distinti (22.925.443 generati),
  profondità 28, 2 minuti e 19 secondi.
- `reread` (regola del 404, il cameriere tiene il foglio quanto vuole): nessuna violazione, completa,
  4.115.439 stati distinti, profondità 28, 2 minuti e 30 secondi.
- `noregcheck` (senza il ricontrollo del registro, con l'età contata bene): nessuna violazione,
  completa, 4.075.007 stati distinti, profondità 27, 2 minuti e 25 secondi. Vedi regola 5.
- `contagiri-piccolo` (età come "giri di fila", taglia piccola): nessuna violazione, completa,
  7.226.247 stati distinti. Il difetto di quella regola compare solo con un giro in più (`contagiri`):
  un'altra ragione per non fidarsi di una regola che passa solo sui modelli piccoli.
- `catena` (3 database, rami di rami, nessun cameriere): nessuna violazione, completa, 24.113.305
  stati distinti (122.504.112 generati), profondità 32, 14 minuti e 9 secondi.
- `due-raccoglitori` (due raccoglitori insieme, nessun cameriere, con la rinuncia al giro troppo
  lungo): nessuna violazione, completa, 52.565.463 stati distinti (255.275.066 generati), profondità
  46, 31 minuti e 17 secondi.
- `grande` (`Grace = 2`, scritture in volo fino a 1 giro, fogli dei camerieri fino a 2 giri, 4
  giri): nessuna violazione, completa, 10.128.195 stati distinti (57.879.982 generati), profondità 33,
  6 minuti e 1 secondo. Conferma la regola 3c con margini diversi da quelli del modello base.

Controlli negativi, devono fallire (fra parentesi l'invariante violata e la lunghezza della traccia):
tutti e 12 falliscono come previsto. `noregisterfirst` (NoDangling, 10 stati), `splitpin`
(PinsLive, 10), `ownerdeletes` (ReadersOK, 5), `dropdeletesprefix` (PinsLive, 4), `nograce`
(NoDangling, 7), `nograce-reread` (NoDangling, 7), `hold-troppo` (NoDangling, 11), `lettore-vecchio`
(ReadersOK, 13), `agebycreation` (ReadersOK, 13), `contagiri` (ReadersOK, 19),
`noregcheck-contagiri` (PinsLive, 13), `due-raccoglitori-lenti` (NoDangling, 11).

Controlli che il modello non sia vuoto, devono fallire: tutti e 6 falliscono, cioè questi casi
succedono davvero nel modello: il raccoglitore cancella qualcosa (`sanity-delete`, 9 stati); un ramo
vivo legge scatole di un padre uscito (`sanity-orfano`, 6); il raccoglitore recupera le scatole di
un database uscito (`sanity-raccolta`, 8); un cameriere rilegge il foglio dopo un 404
(`sanity-rilettura`, 13); un ramo nasce da un punto di ripristino (`sanity-punto`, 5); un nipote
legge ancora una scatola del nonno dopo che nonno e padre sono usciti (`sanity-catena`, 10).

## 3. I controesempi in parole

Ognuno è la traccia più corta trovata da TLC (ricerca in ampiezza), letta passo per passo
(`python3 tla-rami/trace2.py tla-rami/output/<modello>.txt` la stampa in forma corta). "d1" è il padre, "d2" il ramo, le scatole sono 1, 2, 3.

1. **`noregisterfirst`, registrazione dopo la condivisione.** Il fork legge il foglio di d1 (scatola
   1) senza spilla; d1 viene cancellato; il fork scrive il foglio di d2 con la scatola 1, ma d2 non è
   ancora nel registro; il raccoglitore non vede né d1 né d2 né una spilla, e dopo l'età minima
   cancella la scatola 1. d2 entra nella famiglia con un foglio che punta al vuoto. È la regola 2.
2. **`splitpin`, spilla non atomica.** Il fork legge le scatole di d1, poi si ferma (pausa, rete);
   intanto d1 viene cancellato e il raccoglitore, dopo l'età minima, cancella le scatole; poi il fork
   scrive la spilla con scatole già sparite. Quindi "prima si registra" non basta: **la spilla va
   scritta prima che la sorgente possa perdere le scatole lette: in un passo solo, oppure entro un
   tempo T più piccolo dell'età minima** (correzione del revisore notturno; la seconda strada è
   provata nel giro 2, §6). Il controesempio funziona solo perché il fork resta fermo tra la lettura e
   la spilla più a lungo dell'età minima.
3. **`ownerdeletes`, il lavaggio cancella.** Il lavaggio di d1 fonde la scatola 1 nella 2 e cancella
   subito la 1, che in quel momento non è in nessun foglio e in nessuna spilla. Un cameriere aveva
   preso il foglio vecchio un attimo prima: legge la 1 e trova "scatola non trovata". Anche con una
   vista perfetta di fogli e spille, cancellare subito rompe i lettori. È la regola 1.
4. **`dropdeletesprefix`, la cancellazione del padre cancella il prefisso.** Il fork mette la spilla
   sulla scatola 1 di d1; d1 viene cancellato con tutte le sue scatole (come `Admin::delete_db` di
   SlateDB): la spilla del ramo punta al vuoto. È la regola 4, e il motivo del 409 `has_forks` di oggi.
5. **`nograce` e `nograce-reread`, nessuna età minima.** d1 scrive la scatola 2 su S3; il
   raccoglitore fa il LIST (vede la 2) e legge il foglio (la 2 non c'è ancora); d1 scrive il foglio
   con la 2; il raccoglitore cancella la 2. Il foglio di d1 punta al vuoto. È il caso degli "orphan
   files" di Iceberg. La regola del 404 non aiuta: qui il danno è nel foglio, non in un lettore.
6. **`hold-troppo`, scrittura più lunga dell'età minima.** Come il 5, ma l'età minima c'è (1 giro) e
   la scrittura della scatola 2 dura un giro intero: scatola nata al giro 0, il raccoglitore la vede
   orfana al giro 0 e di nuovo al giro 1, il foglio arriva al giro 1 subito dopo che il raccoglitore
   l'ha letto, e al giro 1 l'età è 1: cancellata. L'età minima deve superare la durata della
   scrittura **più** la durata di un giro del raccoglitore, perché anche la vista del raccoglitore è
   vecchia.
7. **`lettore-vecchio`, cameriere con un foglio più vecchio dell'età minima.** Il cameriere prende il
   foglio di d1 con la scatola 1; il lavaggio la toglie; dopo l'età minima il raccoglitore la
   cancella; il cameriere, che non rilegge il foglio, la cerca: "scatola non trovata". Con la regola
   del 404 (`reread`) lo stesso cameriere rilegge il foglio e trova la scatola nuova.
8. **`agebycreation`, età contata dalla nascita.** La scatola 1 è nata al giro 0. Al giro 1 il
   lavaggio la toglie dal foglio mentre un cameriere tiene il foglio vecchio; la scatola ha già "un
   giro di età" dalla nascita, quindi il raccoglitore la cancella subito; il cameriere fallisce. Una
   scatola vecchia di un anno tolta adesso non ha nessuna protezione: l'età va contata da quando esce
   dall'ultimo foglio.
9. **`contagiri`, età come "vista inutilizzata in N giri di fila".** Sembra giusta, non lo è. La
   scatola 2 resta in volo per un giro (il raccoglitore la vede inutilizzata), entra nel foglio subito
   dopo una lettura del raccoglitore e ne esce (lavaggio) prima della lettura successiva: il
   raccoglitore non l'ha mai vista usata e conta 3 giri di fila. La cancella mentre un cameriere ha
   appena preso il foglio che la contiene. Il raccoglitore da solo non può sapere da quanto una scatola
   è inutilizzata: deve dirglielo chi la toglie.
10. **`noregcheck-contagiri`, raccoglitore che non ricontrolla il registro.** Con l'età contata come
    in 9: il raccoglitore legge il registro (nessuna spilla), il fork mette la spilla sulle scatole 1
    e 2 di d1, d1 viene cancellato, il raccoglitore legge il foglio di d1 (vuoto) e cancella la 2: la
    spilla del ramo punta al vuoto. Con l'età contata bene (`noregcheck`) TLC **non** trova violazioni:
    la cancellazione di d1 fa ripartire l'età e protegge da sola. Il ricontrollo del registro resta
    una seconda barriera economica.
11. **`due-raccoglitori-lenti`, due raccoglitori senza controllo della durata.** Il raccoglitore c1
    legge il foglio di d1 (senza la scatola 2, in volo), poi resta fermo; c2 fa un giro intero; c1
    cancella con la sua vista vecchia di più di un giro. Lo stesso succede con **un solo** raccoglitore
    che si ferma (pausa, rete) tra la lettura dei fogli e la cancellazione: nel modello il tempo avanza
    solo con i giri, quindi serve il secondo raccoglitore per mostrarlo.

## 4. Le regole che escono dal modello, tradotte per il disegno vero

Sono le quattro regole del revisore, con tre precisazioni che il modello ha reso necessarie (la 2b,
la 3b e la 3c sono nuove rispetto al rapporto del revisore).

1. **Cancella solo il raccoglitore.** Il lavaggio toglie scatole dal foglio e basta; la cancellazione
   di un database toglie il database dal registro e basta (controesempi 3 e 4).
2. **Prima la spilla, poi il foglio del ramo**, e il ramo entra nella famiglia con lo stesso PUT del
   registro che toglie la spilla (controesempio 1).
   - **2b. La spilla va scritta prima che la sorgente possa perdere le scatole lette: in un passo
     solo, oppure entro un tempo T più piccolo dell'età minima** (controesempio 2). Tre strade, non
     due (giro 2):
     1. la spilla dentro il foglio della sorgente con PUT condizionale (come il checkpoint di SlateDB,
        che oggi usa `fork.rs`). **Non modellata**: nel modello la spilla sta nel registro; se si
        sceglie questa strada la spilla vive nel foglio del padre e il raccoglitore deve leggerla da
        lì, e serve un modello in più prima del codice;
     2. la sorgente ferma durante il fork (come fa oggi `fork.rs` con `quiesce`): ferma il padre ed è
        da evitare per i database grandi molto scritti (il revisore propone il segno di taglio nel
        quaderno);
     3. **spilla in due passi con un tetto di tempo** (giro 2, `spilla-tetto`): il fork legge il
        foglio della sorgente, poi scrive la spilla nel registro solo se dalla lettura è passato al
        più T; altrimenti annulla e ricomincia. È la stessa regola dello scrittore lento. Nel modello
        regge (§6); la spilla resta nel registro, quindi il modello è più vicino al vero.
     La scelta tra le tre è di disegno: decide il fondatore.
3. **Età minima prima di cancellare.**
   - **3a.** Serve per le scritture in volo anche con la regola del 404 (controesempio 5): una scatola
     appena scritta e non ancora nel foglio sembra orfana.
   - **3b. Si conta da quando la scatola esce dall'ultimo foglio o dall'ultima spilla, e lo scrive chi
     la toglie** (controesempi 8 e 9). Il lavaggio scrive nel foglio nuovo "tolte: 1, 2 alle ore X"; la
     cancellazione del database, la scadenza del punto e l'annullamento del fork scrivono l'ora nel
     registro. Il raccoglitore non può ricostruirlo da solo guardando i fogli di tanto in tanto.
     Attenzione: il raccoglitore di SlateDB 0.17 conta l'età dalla nascita della scatola (l'ora
     nell'identificativo ULID dell'SST: `garbage_collector/compacted_gc.rs`, righe 223-247, letto nel
     sorgente da crates.io). In SlateDB va bene perché un lettore con un foglio vecchio prende un
     checkpoint (una spilla); nel disegno nuovo i camerieri tengono fogli senza spilla, quindi quel
     raccoglitore, riusato così com'è, avrebbe il difetto del controesempio 8.
   - **3c. L'età minima deve superare la durata massima di una scrittura in volo più la durata
     massima di un giro del raccoglitore** (controesempi 6 e 11). Nel disegno vero questo si fa
     rispettare da entrambe le parti con l'orologio: lo scrittore rinuncia a pubblicare il foglio se
     la sua scatola è più vecchia di una soglia T (e la riscrive), il raccoglitore
     rinuncia al giro se dal suo LIST è passata più di una soglia G; poi età minima > T + G con
     margine. **T comprende anche la durata del PUT del foglio** (lo scrittore controlla l'orologio
     prima del PUT, ma il foglio diventa visibile solo alla fine del PUT); **G comprende tutta la fase
     delle cancellazioni del giro**, non solo la lettura (il raccoglitore controlla l'orologio prima di
     ogni gruppo di DELETE, non una volta sola all'inizio: un giro con migliaia di DELETE dura minuti).
     Nel modello "controllo il tempo, poi agisco" è un passo solo (`WriteCommit`, `CSweep`); nel vero
     sono due, ed è per questo che la regola va scritta così. Con l'età di 24 ore proposta dal
     revisore e soglie T e G di qualche minuto il margine è enorme (STIMATO).
   - **3d. I camerieri:** o tengono un foglio più giovane dell'età minima (controesempio 7), o
     applicano "su 404 rileggi il foglio" (modello `reread`: nessuna violazione con fogli vecchi quanto
     si vuole). Meglio tutte e due: la prima rende il 404 raro, la seconda lo rende innocuo.
4. **Il database cancellato esce dalla famiglia e le sue scatole restano finché qualcuno le usa.** Il
   modello lo verifica con rami di rami (`catena`) e mostra che lo spazio poi si recupera davvero
   (`sanity-raccolta`).
5. **Ricontrollo del registro prima di cancellare.** Con l'età contata come in 3b non serve per la
   sicurezza (`noregcheck` senza violazioni); serve se l'età è contata male (controesempio 10). Costa
   una GET: la tengo come seconda barriera. Rischio: in una famiglia dove nascono rami o punti di
   continuo, il raccoglitore potrebbe rinunciare a ogni giro e non recuperare mai spazio (vivacità,
   non verificata). Rimedio possibile, non modellato: invece di rinunciare, leggere i fogli dei membri
   nuovi e aggiungere le spille nuove.
6. **Un solo raccoglitore per famiglia** non è obbligatorio per la sicurezza se ogni raccoglitore
   rinuncia al giro troppo lungo (3c); senza quel controllo due raccoglitori perdono dati
   (controesempio 11). Uno solo resta la scelta più semplice ed economica.

Regge per un database da 1 TB? Sì per la correttezza: nessuna regola dipende dalla taglia. Il costo
cresce con la famiglia: ogni giro legge il registro e il foglio di ogni membro (per 100 rami di un
database da 1 TB circa 200 MB a giro, STIMATO dal revisore), e la durata di quel giro entra nella
regola 3c. Con il foglio a livelli chiesto dal revisore di S1 il giro si accorcia.

## 5. Che cosa il modello NON copre

- Taglie piccole: 2 o 3 database, 3 scatole, 3 o 4 giri. È la verifica "small scope": i difetti di
  questi protocolli si vedono di solito con pochi oggetti (tutti i controesempi trovati usano al
  massimo 3 scatole e 2 database), ma non è una dimostrazione per ogni taglia.
- Il contenuto delle scatole non c'è: il modello guarda solo se una scatola usata esiste. Che il ramo
  veda esattamente lo stato del padre alla posizione P (il "segno di taglio" nel quaderno) non è
  modellato qui; serve un modello a parte sopra `ValidatingLog.tla`.
- La spilla del fork e quella del punto di ripristino (`PointCreate`) sono scritte "insieme" alla
  lettura del foglio sorgente in un passo solo; su S3 registro e foglio sono due oggetti e non c'è
  una scrittura atomica su due oggetti. La strada 3 di 2b (tetto di tempo) toglie questa ipotesi per
  il fork; per il punto di ripristino serve la stessa regola (STIMATO, non modellato). La strada 1
  (spilla dentro il foglio del padre, checkpoint di SlateDB) non è modellata.
- "Controllo l'orologio, poi agisco" è un passo solo (vedi 3c).
- Il tempo vero è sostituito dai giri del raccoglitore, e avanza solo quando un raccoglitore finisce
  un giro: un raccoglitore fermo a metà giro ferma anche l'orologio. Il caso "raccoglitore bloccato
  per ore" è coperto solo con due raccoglitori. Un'azione `Tick` separata per l'orologio sarebbe più
  pulita (correzione facoltativa 5 del revisore, non fatta in questo giro: cambia tutti i conteggi). In produzione l'età minima si misura in ore: la
  regola che il modello dice in giri va tradotta (sezione 5).
- Un solo scrittore per database (lo garantisce il PUT condizionale del foglio): due scrittori sullo
  stesso foglio non ci sono.
- Il ripristino del motore di oggi (`ctl/pitr.rs`) scrive la differenza nel database vivo; qui il
  ripristino è modellato come ramo da un punto, come propone il revisore.
- La cancellazione per GDPR ("cancella in tutta la famiglia") e la cifratura per famiglia non ci sono.
- Vivacità (che il raccoglitore prima o poi recuperi lo spazio) non è verificata: solo sicurezza. Il
  controllo `sanity-raccolta` mostra però che il recupero delle scatole di un padre uscito succede.

## 6. Giro 2 (2 novembre 2026): correzioni del revisore notturno

Correzioni 1–4 e 6 di `fenvix-brain/sessions/2026-11-02-revisore-tla-rami/RAPPORTO.md`. La 5
(`Tick` separato) non è fatta: è facoltativa e cambierebbe tutti i conteggi; il limite è scritto in §5.

### 6.1 Spilla in due passi con tetto di tempo (correzione 1)

Nuove costanti in `Famiglia.tla`: `PinCap` e `FHold`, e la variabile `fcyc` (giro in cui il fork ha
letto la sorgente). Con `SplitPin = TRUE` e `PinCap = TRUE` il fork legge il foglio sorgente, e scrive
la spilla nel registro in un passo successivo **solo se** dalla lettura sono passati al più `FHold`
giri; altrimenti l'unica mossa è `ForkAbort` (annulla e ricomincia). Con `PinCap = FALSE` tutto è come
prima (la `VIEW` aggiunge `fcyc` solo con `PinCap` e fork nello stato "read").

Esiti (MISURATI con la traduzione in Python, vedi 6.3; **non con TLC**):

- `spilla-tetto` (età minima 1 giro, tetto `FHold` = 1): nessuna violazione, modello completo,
  4.596.830 stati distinti.
- `spilla-tetto-grande` (età minima 2, `Hold` 1, `RHold` 2, `FHold` = 2, 4 giri): nessuna violazione,
  modello completo, 11.579.144 stati distinti.
- `spilla-tetto-troppo` (età minima 1, `FHold` = 2): `PinsLive` violata, traccia di 10 stati. Il fork
  legge la scatola 1 del padre al giro 0; il padre viene cancellato (età della scatola riparte da 0);
  il raccoglitore fa un giro vuoto (giro 0 → 1), poi un secondo giro in cui la scatola ha età 1 e la
  cancella (giro 1 → 2); al giro 2 il fork scrive la spilla su una scatola sparita.

Quindi **la strada 3 regge, con la regola: tetto del fork ≤ età minima** (nel modello `FHold ≤
Grace`). È un giro più largo della regola dello scrittore (`Hold ≤ Grace − 1`): il raccoglitore
ricontrolla il registro prima di cancellare, quindi una spilla scritta durante il suo giro lo fa
rinunciare; resta pericolosa solo una spilla scritta **dopo** una cancellazione, e questa arriva al
più presto dopo l'età minima. Nel vero si scrive con margine, come in 3c: **T del fork + durata del PUT
del registro < età minima**. Con 24 ore di età minima e un tetto di qualche minuto il margine è enorme
(STIMATO). La regola vale per qualunque taglia del padre: il fork copia solo il foglio.

### 6.2 Frasi corrette (correzioni 2, 3, 4)

- 2: "la spilla va scritta prima che la sorgente possa perdere le scatole lette: in un passo solo,
  oppure entro un tempo T più piccolo dell'età minima" (§3 punto 2 e §4 punto 2b, con le tre strade).
- 3: nella regola 3c, T comprende il PUT del foglio e G tutte le cancellazioni del giro.
- 4: la spilla dentro il foglio del padre (checkpoint di SlateDB) non è modellata; serve un modello
  in più se si sceglie quella strada (§4 punto 2b e §5).

### 6.3 Come sono stati misurati

L'ambiente di questa sessione ha bloccato l'esecuzione di `tla2tools.jar` scaricato (come per il
revisore notturno). Ho tradotto `Famiglia.tla` a mano in Python (`verifica-python/famiglia.py`,
ricerca in ampiezza con la stessa `VIEW` e impronte a 64 bit; un solo raccoglitore, senza simmetria).
Controllo della traduzione contro TLC:

- gli 8 controlli negativi con un raccoglitore (`splitpin`, `noregisterfirst`, `ownerdeletes`,
  `dropdeletesprefix`, `nograce`, `hold-troppo`, `lettore-vecchio`, `agebycreation`) sono trovati
  con **la stessa invariante** di TLC (MISURATO, `verifica-python/output/`);
- i modelli completi senza violazioni restano senza violazioni: `base` 4.124.169 stati distinti contro
  4.118.114 di TLC, `contagiri-piccolo` 7.215.670 contro 7.226.247 (MISURATO).

Le differenze nei conteggi (0,15%) **non** sono un errore di traduzione: la `VIEW` non è canonica.
Nello stato "listed" il raccoglitore porta ancora `cmem`, `cpins`, `cread`, `cused` del giro prima, che
nello stato "idle" erano nascosti: due stati uguali per la vista possono dare successori diversi per la
vista, e il conteggio dipende dall'ordine di esplorazione. Prova: la stessa traduzione con i successori
in ordine inverso conta 3.900.833 stati distinti su `base` (MISURATO, `base-ordine-inverso.txt`).
Nessun comportamento si perde (quei campi si riscrivono prima di essere letti), quindi le conclusioni
restano valide. Proposta, non fatta per non cambiare i conteggi: nascondere quei quattro campi anche
nello stato "listed", così la vista diventa canonica e il conteggio non dipende dall'ordine.

Da rifare con TLC appena si può: `./run.sh Famiglia-spilla-tetto Famiglia-spilla-tetto-troppo
Famiglia-spilla-tetto-grande` (le configurazioni sono generate da `gen.py`).
