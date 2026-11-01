# La regola del turno (quaderno a turno su EFS), corretta

Sessione S12, 2026-11-01. Branch `idea-turno`. Parte dall'idea B di Fable
(`fenvix-brain/sessions/2026-11-01-fable-idea-pazza/RAPPORTO.md`, sezione 3) e dalle correzioni A, B, C del
revisore (`fenvix-brain/sessions/2026-11-01-revisore-idea-pazza/RAPPORTO.md`, punto 1). La specifica TLA+ è in
`idea-turno/tla/Turno.tla`; `idea-turno/tla/run.sh` rifà tutti i controlli.

TLC ha trovato **tre buchi in più** oltre a quelli del revisore (regole R3, R4 e R5 sotto). Con tutte le regole la
specifica passa; togliendone una qualsiasi, TLC trova la violazione.

## In cucina

- **Il libro** (file di log di un cameriere su EFS, `libri/<cameriere>/<epoca>.log`): il cameriere di turno ci
  scrive gli ordini di tutti i suoi tavoli.
- **Il turno** (diritto di scrivere un database, `turni/<database>/<n>`): chi crea per primo il cartello numero
  `n` ha il tavolo.
- **L'epoca** (`epoche/<cameriere>/<e>`): un cartello "il tuo libro numero e è chiuso"; lo mette chi porta via un
  tavolo a quel cameriere.
- **Il controllo** (la domanda vera al server): dopo aver salvato il lotto, il cameriere chiede al server "c'è un
  cartello di epoca nuova per me?". Se no, conferma il lotto ai clienti.

## Le regole

**R1. Il cartello del turno si chiama solo `n`** (correzione A). Si crea con `O_CREAT|O_EXCL`; il nome del
cameriere sta dentro il file. Chi riceve `EEXIST` rilegge il contenuto: se dentro c'è il suo nome, aveva vinto
(il client NFS può aver ripetuto la richiesta: EFS non ha la "persistent reply cache", DICHIARATO, pagina Quotas).

**R2. Prima il turno, poi l'epoca** (correzione B). Chi prende il tavolo d crea prima `turni/d/<n+1>`, poi una
epoca nuova (il numero più alto più uno, `O_EXCL`).

**R3. L'epoca nuova va a tutta la catena, non solo all'ultimo titolare** (nuovo, trovato da TLC). La catena sono
tutti i titolari di d dal turno scritto nell'ultimo manifest in poi. Perché: se H ha creato il turno 2 ma è morto
prima di mettere l'epoca a W, e poi K prende il turno 3 mettendo l'epoca solo a H, W non ha mai ricevuto il
cartello e continua a confermare scritture che K non legge (controllo negativo E). Quasi sempre la catena è
lunga uno: lo scarico su S3 la accorcia.

**R4. Dopo le epoche, rileggere i turni dal server; se c'è un turno più alto del proprio, rinunciare** (nuovo,
trovato da TLC con 4 prese di fila). Perché: un subentrante lento che recinta tardi leggerebbe lotti scritti da
un titolare più nuovo e potrebbe scaricarli su S3 con il suo turno vecchio, che poi un terzo titolare
sovrascrive (controllo negativo G).

**R5. Il subentrante legge, per ogni titolare della catena, solo i libri con epoca minore di quella che ha
creato lui**, più i record "presa" che quei titolari avevano scritto (chi avevano letto prima). Trovato da TLC: nella prima
versione il subentrante leggeva tutti i libri, anche quelli aperti dopo dal titolare più nuovo, e con 4 camerieri
e 3 prese un lotto entrato nel manifest ne usciva (`ManifestNonPerde` violata in 23 passi). Insieme a R4 è quello
che tiene pulito il manifest.

**R6. Controllo dopo ogni `fsync`, una domanda vera al server, nessun orologio** (correzione C). Per ogni lotto:
`pwrite` con offset esplicito, `fsync` (o scrittura con `O_DSYNC`), poi `open` per nome di
`epoche/<io>/<e+1>` su un secondo mount con `lookupcache=none,noac,nosharecache`. Se non esiste, il lotto è
confermato. Niente cache degli attributi, niente "controllo buono da meno di 10 ms". Una domanda può valere per
tutti i lotti finiti prima che parta. Controlli negativi C (domanda alla cache) e D (domanda prima dell'`fsync`).

**R7. La reazione: epoca, poi turni, poi libro nuovo.** Chi vede un'epoca nuova smette di confermare, legge
dal server il numero di epoca, rilegge dal server i turni, lascia i tavoli dove c'è un turno più alto del suo e
solo dopo apre il libro della nuova epoca. Un cameriere cambia epoca solo così: nemmeno alla fine di una presa
adotta un'epoca nuova senza rileggere i turni.

**R8. Numero di turno nel manifest.** Si scaricano su S3 solo lotti confermati; il manifest porta il numero di
turno di chi l'ha scritto e il PUT condizionale (`If-Match`) è accettato solo con turno maggiore o uguale a
quello scritto. Controllo negativo F.

**Fuori dal modello, ma obbligatorie** (dal revisore):
- mount `hard` (mai `soft`); `pwrite` con offset calcolato dal titolare, mai `O_APPEND`;
- dopo un errore di `fsync` non si ritenta: si chiude il libro e se ne apre un altro (fsyncgate);
- il nuovo titolare porta il suo orologio ibrido oltre la posizione più alta trovata nei libri letti;
- la regia (routing service) decide gli spostamenti: le letture non prendono mai il turno.

## Perché è sicura senza orologi

Il server EFS mette in fila le operazioni sui file. Se W conferma un lotto, la sua domanda è arrivata al server
dopo la fine dell'`fsync` e ha trovato "nessuna epoca nuova": quindi l'epoca del subentrante H è stata creata
dopo. H legge il libro dopo aver creato l'epoca, quindi trova il lotto. Se invece W aveva già visto un'epoca
nuova e aveva riaperto un libro più nuovo, l'aveva fatto dopo aver riletto i turni (R7), e il turno di H esisteva
già (R2): W ha lasciato d. Le pause della VM o del processo non contano: nessun passo dipende da quanto tempo è
passato.

Ipotesi su EFS che il modello dà per vere e che la micro-prova deve misurare (banchi 9, 10, zombie):
`O_EXCL` atomico anche tra client diversi; dopo `fsync` i byte sono leggibili da un altro client che apre il file
dopo (close-to-open, scritture in coda: AWS garantisce lettura-dopo-scrittura solo per quelle "non in coda",
DICHIARATO, pagina Features); `open` di un nome sul mount senza cache arriva davvero al server.

## La specifica TLA+

`Turno.tla`: un database, camerieri come processi, il file system come insieme di file che il server serializza.
Azioni del protocollo: Scrivi e Fsync, Controlla (dopo l'`fsync`), Prendi il turno (Inizia, CreaTurno, Epoche,
Verifica, Leggi). Più la reazione (Reagisci1, Reagisci2), lo scarico (Scarica) e, per le varianti sbagliate,
ControllaPrima, AggiornaCache e TurnoDopo. Un cameriere che muore o si ferma è un cameriere che non fa più passi:
TLC prova tutti gli ordini possibili, compresi quelli in cui un titolare si ferma a metà e riparte dopo.

Proprietà:
- `NessunaConfermaPersa` (quella chiesta): chi ha finito la presa e ha il turno più alto conosce ogni lotto
  confermato, anche quelli confermati da un vecchio titolare zombie.
- `UnSoloVincitore`: per ogni numero di turno un solo cameriere.
- `ManifestNonPerde`: un lotto entrato nel manifest non ne esce più.
- `SoloConfermatiSuS3`.

Risultati (MISURATO, TLC 1.8.0 in locale, 2 thread):
- `Turno.cfg` (3 camerieri, 3 lotti, 2 prese): nessun errore, 189.234 stati distinti, profondità 27.
- `Turno_medio.cfg` (3 camerieri, 2 lotti, 3 prese): nessun errore, 162.750 stati.
- `Turno_medio3.cfg` (3 camerieri, 3 lotti, 3 prese): nessun errore, 1.485.258 stati, profondità 35.
- `Turno_grande.cfg` (4 camerieri, 2 lotti, 3 prese): nessun errore, 4.405.361 stati distinti, profondità 31, 2 minuti.

Controlli negativi (si spegne una regola, TLC deve trovare la violazione; MISURATO):
- A, nome `<n>-<cameriere>`: `UnSoloVincitore` violata in 6 passi; senza quella proprietà, `NessunaConfermaPersa`
  violata in 14 passi (A2): due titolari, scritture confermate da uno e ignorate dall'altro.
- B, prima l'epoca poi il turno: `NessunaConfermaPersa` violata in 13 passi.
- C, controllo servito dalla cache degli attributi: violata in 10 passi.
- D, controllo prima dell'`fsync`: violata in 9 passi.
- E, epoca solo all'ultimo titolare: violata in 12 passi.
- F, manifest senza numero di turno: `ManifestNonPerde` violata in 13 passi.
- G, senza la rilettura dei turni dopo le epoche: `ManifestNonPerde` violata in 23 passi (3 camerieri, 3 prese).

Limiti del modello: un solo database (l'epoca per cameriere tocca tutti i suoi tavoli: nel modello le epoche
"spurie" vengono dalla catena); spazio degli stati piccolo (fino a 4 camerieri e 3 prese); il server EFS è
corretto per ipotesi; lo scarico è un'azione atomica di sostituzione del manifest (If-Match).
