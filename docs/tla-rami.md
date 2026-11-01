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
`Grace` sono i giri di fila in cui una scatola deve risultare inutilizzata prima di essere cancellata.

Invarianti (nomi nel file):

1. `NoDangling`: nessun foglio vivo (membro, o ramo già scritto e non ancora membro) punta a una
   scatola cancellata. È "il ramo non perde dati".
2. `PinsLive`: ogni spilla (fork in corso, punto di ripristino) punta solo a scatole vive. È "il punto
   di ripristino funziona ancora".
3. `ReadersOK`: nessun lettore riceve "scatola non trovata" su un database che esiste ancora. È il
   "sst not found" già visto nel motore.
4. `InFlightLive` e `TypeOK`: controlli di contorno.

Dimensione dei modelli: database {d1, d2} (d1 radice), 3 scatole, 1 punto di ripristino, 1 lettore,
1 raccoglitore, 3 giri. Varianti: `catena` con 3 database (rami di rami) e nessun lettore;
`due-raccoglitori`; `grande` con `Grace = 3`, `Hold = 1`, `RHold = 1` e 4 giri. Per stare nella
memoria la configurazione usa una `VIEW`: due stati che differiscono solo in campi che nessuno leggerà
più (i dati di un'operazione già finita, sovrascritti dalla prossima prima di essere letti) contano
come uno. Non toglie comportamenti.

## 2. Esiti

RISULTATI

## 3. I controesempi in parole

CONTROESEMPI

## 4. Che cosa il modello NON copre

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
