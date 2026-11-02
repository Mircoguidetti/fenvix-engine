# Rapporto finale, branch tla-registro

Data: 1 novembre 2026. Branch `tla-registro` da `origin/main`; PR in bozza verso `main`
(https://github.com/Mircoguidetti/fenvix-engine/pull/8), nessun merge.
Nessun altro branch toccato (la specifica vecchia e `proto/INTERFACCIA.md` di proto-lettura letti con
`git show`). Nessuna spesa su cloud: tutto in locale, Java 21 del contenitore e `tla2tools.jar` dalla
release ufficiale v1.8.0 di `tlaplus/tlaplus`. Ogni numero è MISURATO (dagli output di TLC salvati nel
repo) o STIMATO.

## Che cosa c'è nel branch

- `docs/tla-registro-decisioni.md`: le scelte A (scritture contese) e B (letture fresche).
- `docs/protocollo.md`: il protocollo nuovo in prosa, base per proto-quaderno, con le revisioni chieste
  dalla sessione proto-lettura.
- `tla-registro/ValidatingLog.tla`: il registro con il leader che valida, 9 configurazioni.
- `tla-registro/LettureScarico.tla`: letture a `S` e scarico su S3 sopra il registro, 9 configurazioni.
- `tla-registro/output/`: l'output di TLC di ogni modello. `tla-registro/run.sh` li riproduce.
- `tla-registro/journal3z-vecchio/`: copia della specifica vecchia con le correzioni per farla girare e
  i 5 output.
- `docs/tla-registro.md`: che cosa è verificato, dimensioni, cosa non è coperto, controesempi in parole.

## Risposte alle due domande (A e B)

A, scritture contese: raccomandata la scelta (c), dipendenze dichiarate. Ogni record porta `S`, le chiavi
scritte (`strutturale` o `delta`) e le chiavi da cui dipende, in modo `esiste` o `valore`. Il leader tiene
due posizioni per chiave e rifiuta solo se la dipendenza è violata nel modo che conta. Un `$inc` per
`_id` dipende solo dall'esistenza: 100 client su un documento caldo passano in un giro senza rifiuti
(STIMATO), contro 100 giri e circa 5.000 tentativi inutili con tutto validato (STIMATO, 3-4 ms a giro).
Comandi per classe e scostamenti accettati nel documento delle decisioni.

B, letture fresche: `S` è il massimo tra il gettone del client e la confermata nota al nodo d'ingresso,
con limite di anzianità oltre il quale si chiede al leader; `linearizable` usa il ReadIndex. La replica
serve `(M, S]` solo se `S` non supera la propria confermata locale, mai in base alla coda. La specifica
mostra con un controesempio che cosa succede altrimenti.

## Passo 1, specifica vecchia

Mai eseguita prima. Quattro correzioni di pura esecuzione per farla partire (ordine delle costanti, due
stringhe mescolate a record, una guardia su un indice), nessuna di protocollo. Esito: nessuna violazione
delle 7 invarianti in 108,4 milioni di stati generati e 13,8 milioni distinti fino a profondità 22 in 30
minuti (MISURATO); esecuzione non completata, coda in crescita, poi uccisa dal sistema per memoria quando
è partito un secondo TLC. Lo spazio degli stati è molto più grande di un'ora. Non altro tempo investito.

## Passo 2, protocollo

`docs/protocollo.md`. Revisioni fatte su segnalazione di proto-lettura (`proto/INTERFACCIA.md` di quel
branch, sezione "Due regole da rivedere" e differenze 1-2):

- §6.2 punto 1: il manifest non si rilegge più ogni 5 s (circa 518.000 GET al mese per database e nodo
  d'ingresso, STIMATO da proto-lettura) ma solo quando la replica risponde `FUORI_FINESTRA(potatoFinoA)`,
  cioè al più una volta per scarico; la replica porta `potatoFinoA` ed `etag`.
- §6.2 punto 3: la replica non risponde mai "fuori finestra" solo perché la base di una chiave non è nella
  finestra: restituisce i delta della finestra e il nodo d'ingresso li applica al valore degli SST
  (differenza 2 di proto-lettura; la specifica compone così da sempre).
- §7.1: lo scarico non è più ogni 5 s per database (circa 1.036.800 PUT al mese, $5,18 al mese per
  database, STIMATO da proto-lettura) ma per dimensione (1 MB) o età (10 minuti), oppure in oggetti
  condivisi e manifest di gruppo per i database piccoli; le repliche espongono `pendenti` con byte e
  record non su S3 e `scaricato` per database.
- §7.1 punto 5: dopo un 412 il worker rilegge il manifest prima di cancellare il proprio SST: il 412 può
  essere la risposta al ritentativo di un PUT già riuscito. Aggiunto il controllo negativo
  `DeleteOrphanBlind` alla specifica: cancellare senza rileggere viola `UploadExact` (MISURATO, 807 stati).
- §2: le posizioni possono essere l'orologio logico ibrido del prototipo (48 bit di ms più 16 di
  contatore): resta un intero crescente assegnato dal leader, tutto il resto vale uguale.

## Passo 3, ValidatingLog.tla

Modello base (3 repliche, 3 chiavi, 3 transazioni una per classe, un cambio di leader, al più 2 voci per
transazione, una perdita di replica): esplorazione COMPLETA senza violazioni delle 8 invarianti e della
proprietà "la confermata non torna indietro": 56.450.952 stati generati, 18.264.273 distinti, profondità
32, 26 minuti e 34 secondi con 4 worker (MISURATO).

Modello con due cambi di leader: NON completato al tetto di 40 minuti, 71.060.112 stati generati,
27.190.939 distinti, 14,1 milioni in coda, profondità 18 raggiunta, nessuna violazione negli stati
esplorati (MISURATO). Evidenza limitata, non una prova.

Controlli negativi, tutti catturati (MISURATO, tempi tra 1 secondo e 3 minuti e mezzo): senza validazione;
`S` sotto la finestra accettata; nuovo leader senza ricostruzione (duplica un id); ritentativo oltre la
finestra (duplica: buco reale del protocollo, chiuso al passo 5); conferma con 1 replica su 3; voto senza
restrizione sul log. Controesempi in parole in `docs/tla-registro.md`.

## Passo 4, LettureScarico.tla

I modelli base delle letture e dello scarico non si esauriscono in tempi utili su questa macchina. Il
modello con 3 transazioni, nessun cambio di leader, 1 lettura, 2 worker e risposta del PUT che può
perdersi è stato fermato al tetto di 60 minuti con 4 worker: 126.050.517 stati generati, 11.014.095
distinti, 4,3 milioni in coda, profondità 22, nessuna violazione (MISURATO). Evidenza limitata.
Il modello ridotto a 2 transazioni è stato fermato al tetto di 40 minuti con 88.932.540 stati generati,
5.801.771 distinti, 299 mila in coda in discesa, profondità 32, nessuna violazione (MISURATO): quasi
completo, un tetto di circa un'ora dovrebbe bastare (STIMATO). Il modello con un cambio di leader e due
letture non è stato portato a termine. Per le letture e lo scarico l'evidenza è quindi: controlli
negativi tutti catturati (ogni regola serve) e nessuna violazione in oltre 11 milioni di stati del
modello base, ma non una esplorazione esaustiva.

Controlli negativi, tutti catturati (MISURATO, 2-18 secondi): replica che serve dalla coda invece che
dalla confermata (restituisce un record non confermato); gettone del client ignorato; manifest senza
condizione (la posizione del manifest torna indietro); oggetto SST chiamato per sola posizione iniziale
(contenuto diverso dal manifest); worker che scarica record non confermati; orfano cancellato senza
rileggere il manifest.

## Passo 5, sessioni

La deduplicazione del leader dura quanto la finestra. Chiusura: ogni scrittura ritentabile scrive anche la
chiave di sessione con il proprio esito, e il nodo d'ingresso la legge a `S` prima di inviare
(`docs/protocollo.md` §4.6, con la dimostrazione in tre casi). Modello `ValidatingLog-sessioni`
(ritentativi anche oltre la finestra, lettura della chiave di sessione): esplorazione COMPLETA senza
violazioni di `NoDuplicateIds` e `OneOutcome`, 61.811.817 stati generati, 17.695.505 distinti, profondità
32, 22 minuti e 52 secondi (MISURATO). Un commit ritentato restituisce il primo esito anche se il leader
che lo aveva confermato è morto.

## Che cosa NON è coperto

Cambio dell'insieme delle repliche e rientro dopo perdita del disco; rete esplicita (la replicazione è
una copia atomica dopo il prefisso comune, come il controllo del prefisso di Raft garantisce); liveness;
semantica degli operatori sui valori (una scrittura è la sua posizione); compattazione, raccolta degli
orfani, più database e manifest di gruppo; tempi e orologi (tutto per posizione); dimensioni oltre quelle
dei modelli. Dettagli in `docs/tla-registro.md`.

## Problemi incontrati

- Prima versione di `ValidatingLog` troppo grande (i rifiuti contavano come passi): ridotta rendendo i
  rifiuti non-passi; il modello base è poi completato in 27 minuti.
- Prima versione di `LettureScarico` troppo grande (ogni lettura lasciava il risultato nello stato):
  la lettura ora registra un booleano. Nel farlo ho introdotto un errore di precedenza (`readOk' = a /\ b`
  letto come `(readOk' = a) /\ b`) che rendeva impossibili le letture sbagliate invece di segnalarle: me
  ne sono accorto perché il controllo `UnsafeServe` non trovava più il controesempio in 2 minuti dove
  prima bastavano 3 secondi. Corretto; tutti i controlli rieseguiti con la specifica definitiva.
- Tre volte ho ucciso la mia stessa shell cercando processi per nome. Tempo perso, nessun danno.
- La specifica vecchia è stata uccisa dal sistema per memoria quando è partito un secondo TLC.

Nessun blocco.
