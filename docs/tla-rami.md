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
lettore, `due-raccoglitori`, `grande` con `Grace = 2`, `Hold = 1`, `RHold = 2` e 4 giri. Per stare nella
memoria la configurazione usa una `VIEW`: due stati che differiscono solo in campi che nessuno leggerà
più (i dati di un'operazione già finita, sovrascritti dalla prossima prima di essere letti) contano
come uno. Non toglie comportamenti.

## 2. Esiti

RISULTATI

## 3. I controesempi in parole

Ognuno è la traccia più corta trovata da TLC (ricerca in ampiezza), letta passo per passo
(`tla-rami/trace2.py` la stampa in forma corta). "d1" è il padre, "d2" il ramo, le scatole sono 1, 2, 3.

1. **`noregisterfirst`, registrazione dopo la condivisione.** Il fork legge il foglio di d1 (scatola
   1) senza spilla; d1 viene cancellato; il fork scrive il foglio di d2 con la scatola 1, ma d2 non è
   ancora nel registro; il raccoglitore non vede né d1 né d2 né una spilla, e dopo l'età minima
   cancella la scatola 1. d2 entra nella famiglia con un foglio che punta al vuoto. È la regola 2.
2. **`splitpin`, spilla non atomica.** Il fork legge le scatole di d1, poi si ferma (pausa, rete);
   intanto d1 viene cancellato e il raccoglitore, dopo l'età minima, cancella le scatole; poi il fork
   scrive la spilla con scatole già sparite. Quindi "prima si registra" non basta: la spilla deve
   essere scritta **nello stesso passo** in cui si legge la versione della sorgente.
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
   - **2b. La spilla va scritta nello stesso passo in cui si legge la versione della sorgente**
     (controesempio 2). Due modi che danno questa garanzia: la spilla dentro il foglio della sorgente
     con PUT condizionale (come il checkpoint di SlateDB, che oggi usa `fork.rs`), oppure la sorgente
     ferma durante il fork (come fa oggi `fork.rs` con `quiesce`). Il secondo ferma il padre ed è da
     evitare per i database grandi (il revisore propone il segno di taglio nel quaderno). Un terzo
     modo, per esempio "scrivo la spilla e poi ricontrollo il foglio della sorgente", va aggiunto al
     modello prima di usarlo: non è verificato.
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
     margine. Con l'età di 24 ore proposta dal revisore e soglie T e G di qualche minuto il margine è
     enorme (STIMATO).
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
- Il tempo vero è sostituito dai giri del raccoglitore. In produzione l'età minima si misura in ore: la
  regola che il modello dice in giri va tradotta (sezione 5).
- Un solo scrittore per database (lo garantisce il PUT condizionale del foglio): due scrittori sullo
  stesso foglio non ci sono.
- Il ripristino del motore di oggi (`ctl/pitr.rs`) scrive la differenza nel database vivo; qui il
  ripristino è modellato come ramo da un punto, come propone il revisore.
- La cancellazione per GDPR ("cancella in tutta la famiglia") e la cifratura per famiglia non ci sono.
- Vivacità (che il raccoglitore prima o poi recuperi lo spazio) non è verificata: solo sicurezza. Il
  controllo `sanity-raccolta` mostra però che il recupero delle scatole di un padre uscito succede.
