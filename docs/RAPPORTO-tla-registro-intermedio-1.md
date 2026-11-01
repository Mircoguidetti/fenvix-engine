# Rapporto intermedio 1, branch tla-registro

Data: 1 novembre 2026, circa un'ora e mezza dall'inizio. Branch `tla-registro`, creato da `origin/main`.
Il branch `journal3z` non è stato toccato: la specifica vecchia è stata letta con `git show`.
Nessuna spesa su cloud: tutto in locale (Java 21 già nel contenitore, `tla2tools.jar` dalla release
ufficiale `tlaplus/tlaplus` v1.8.0). Ogni numero qui sotto è MISURATO o STIMATO.

## Consegna intermedia richiesta (A e B)

Il documento è `docs/tla-registro-decisioni.md`. In breve.

A, scritture contese. Raccomando la scelta (c), "dipendenze dichiarate". Ogni record porta la posizione
`S` dell'istantanea, le chiavi che scrive (ciascuna `strutturale` se crea, cancella o sostituisce, oppure
`delta` se modifica campi con operatori) e le chiavi da cui dipende, ciascuna nel modo `esiste` (si è
fidata solo dell'esistenza) o `valore` (si è fidata del contenuto). Il leader tiene per chiave due
posizioni, l'ultima scrittura strutturale e l'ultima scrittura qualsiasi, e rifiuta solo se la
dipendenza è violata nel modo che conta. Conseguenze: un `$inc` per `_id` dipende solo dall'esistenza,
quindi 100 client su un documento caldo passano in un giro con zero rifiuti (STIMATO), contro 100 giri e
circa 5.000 tentativi inutili con "tutto validato" (STIMATO, a 3-4 ms per giro: il più lento aspetta
circa 350 ms). Insert, delete, replace e upsert sono strutturali con dipendenza `esiste`. `findAndModify`,
filtri su campi diversi da `_id`, indici unici, transazioni e collezioni con validatore hanno dipendenza
`valore`, cioè restano validati come oggi; il `findAndModify` in ciclo resta seriale per natura e si
mitiga con una coda per chiave nel nodo d'ingresso. Il documento elenca i comandi MongoDB per classe e
due scostamenti accettati (`modifiedCount` calcolato a `S` per i delta ciechi; limite 16 MB e validatore
controllati a `S` solo lontano dal limite e senza validatore).

B, letture fresche. `S` di default è il massimo tra il gettone del client (posizione più alta vista,
trasportata come `$clusterTime`) e la posizione confermata nota al nodo d'ingresso, con un limite di
anzianità (proposta 1 s, STIMATO) oltre il quale si chiede al leader; `readConcern: linearizable` usa
la confermata del leader dopo un giro di heartbeat (ReadIndex). La replica serve i record `(M, S]` solo
se `S` non supera la propria confermata locale e `M` è dentro la sua finestra, mai in base alla
lunghezza della coda: una replica isolata può avere a una posizione non confermata un record che un
leader più nuovo ha sostituito. La replica isolata rifiuta e il nodo d'ingresso prova un'altra replica.

## Passo 1, specifica vecchia

Non era mai stata eseguita. Per farla partire sono servite quattro correzioni di pura esecuzione, nessuna
di protocollo (ordine delle costanti in `MC.tla`; la stringa `"T"` e la stringa `"none"` mescolate a
record, che TLC non confronta; una guardia `prev >= 1`). Esito con il modello originale (3 nodi, 2
epoche, 2 indici): nessuna violazione delle 7 invarianti in 108,4 milioni di stati generati e 13,8
milioni distinti fino a profondità 22, in 30 minuti (MISURATO); esecuzione NON completata, coda ancora in
crescita, poi fermata dal sistema per memoria quando è partito un secondo TLC. Lo spazio degli stati è
molto più grande di un'ora. Output in `tla-registro/journal3z-vecchio/`. Nessun altro tempo investito.

## Passo 2, protocollo

`docs/protocollo.md` è scritto: parti, posizioni e record, stato per ruolo, scritture con le dipendenze
dichiarate, replicazione e conferma alla Raft, barriera e ricostruzione al cambio di leader, letture con
confermata locale, scarico su S3 con manifest condizionale e nomi di oggetto unici, guasti uno per uno,
e la chiave di sessione per i ritentativi oltre la finestra (passo 5).

## Passo 3 e 4, specifiche

`tla-registro/ValidatingLog.tla` (leader che valida, finestra, cambio di leader con ricostruzione, dedup
per id, perdita di una replica) e `tla-registro/LettureScarico.tla` (letture a `S` da replica qualsiasi,
gettone del client, due worker con manifest condizionale) sono scritte, con 15 configurazioni. Gli 11
controlli negativi sono tutti catturati da TLC (MISURATO, tra 1 secondo e 3 minuti e mezzo ciascuno):
senza validazione, `S` sotto la finestra accettata, nuovo leader senza ricostruzione, ritentativo oltre la
finestra, conferma con 1 replica su 3, voto senza restrizione sul log, replica che serve dalla coda invece
che dalla confermata, gettone ignorato, manifest senza condizione, oggetto SST chiamato per sola posizione
iniziale, worker che scarica record non confermati. Il controllo "ritentativo oltre la finestra" mostra
un buco reale del protocollo come descritto nel brief (dedup lunga quanto la finestra); il passo 5 lo
chiude con la chiave di sessione, già modellato (`SessionLookup`) e in attesa di esecuzione.

I modelli base (tutti i controlli spenti) sono in esecuzione adesso: il base di `ValidatingLog` con un
cambio di leader è a 13 milioni di stati distinti, profondità 24, senza violazioni finora (MISURATO,
alle 13:10 UTC); seguono il modello con due cambi di leader, il modello delle sessioni e il base di
`LettureScarico`. Una prima versione del modello era troppo grande (i rifiuti contavano come passi); è
stata ridotta rendendo i rifiuti non-passi, senza perdere comportamenti.

## Che cosa resta

Attendere la fine dei modelli base, copiare gli output in `tla-registro/output/`, completare
`docs/tla-registro.md` (dimensioni, cosa non è coperto, controesempi in parole), rapporto finale
`docs/RAPPORTO-tla-registro.md`, PR in bozza verso `main`.

Problemi incontrati: due volte ho ucciso la mia stessa shell cercando processi per nome; nessun effetto sul
lavoro, solo tempo perso. Nessun blocco.
