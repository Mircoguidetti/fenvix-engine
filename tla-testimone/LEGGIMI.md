# tla-testimone: due copie con i dati più testimone che tiene le ricevute

Specifica TLA+ piccola della configurazione "due zone più testimone" (replica set PSA con un arbitro
che conserva l'ultima posizione confermata in modo degradato). Solo carta: nessun codice del motore.
Origine: revisore delle idee pazze 3, punto 1 (`fenvix-brain/sessions/2026-11-01-revisore-idee-pazze-3/RAPPORTO.md`).

## File

- `Testimone.tla`: il modello, con le regole R1–R4 e i controlli negativi scritti in testa.
- `MCTestimone.tla`: simmetria tra le due copie.
- `Testimone-*.cfg`: una configurazione per ogni esecuzione.
- `output/`: output completo di TLC di ogni configurazione.
- `run.sh`: riproduce tutto (`./run.sh` oppure `./run.sh Testimone-base`). Serve `tla2tools.jar` v1.8.0
  nella cartella (vedi la testa di `run.sh`).

## Il protocollo nel modello

- R1 elezione: 2 voti su 3 (il candidato più l'altra copia oppure il testimone), un voto per epoca. L'altra
  copia vota solo un candidato con il quaderno almeno aggiornato come il suo; il testimone solo un candidato
  con il quaderno almeno aggiornato come la sua ricevuta.
- R2 modo pieno: si conferma la posizione n solo quando l'altra copia ha dato l'ack fino a n nell'epoca del capo.
- R3 modo degradato: ci si entra solo se il testimone accetta l'epoca del capo; ogni conferma in modo
  degradato scrive prima la ricevuta [epoca, posizione] sul testimone, che la accetta solo se è ancora
  all'epoca del capo; si confermano solo voci dell'epoca del capo (regola di Raft).
- R4 il testimone tiene epoca e ricevuta su disco durevole.

## Invarianti

- `ElectionSafety`: mai due capi nella stessa epoca.
- `NoSplitBrain`: tutte le conferme date ai client stanno su una sola storia (nessun cervello diviso).
- `LeaderCompleteness`: ogni capo ha tutte le voci confermate da capi di epoche non più alte della sua
  (una copia rimasta indietro non può comandare).
- `DurabilityPieno`: ciò che è confermato in modo pieno sta su ogni copia viva.

## Esiti (MISURATO, TLC 1.8.0, 2 core, output in `output/`)

- `base` (epoche fino a 4, quaderno fino a 3 voci, 1 disco perso): nessuna violazione, 1.762.488 stati
  generati, 534.951 distinti, profondità 21, ricerca completa in 13 s.
- `base-grande` (epoche fino a 5, quaderno fino a 4 voci): nessuna violazione, 33.527.119 stati generati,
  9.626.124 distinti, profondità 26, ricerca completa in 3 min 04 s.
- Controlli negativi, tutti trovati da TLC:
  - `ackonone` (conferma con una copia sola in modo pieno): `DurabilityPieno` violata in 3 passi.
  - `nowitnessgrant` (modo degradato deciso dal capo da solo): `LeaderCompleteness` violata;
    `nowitnessgrant-split` (solo `NoSplitBrain`): cervello diviso, le due copie confermano due storie diverse.
  - `noreceipt` (permesso del testimone ma niente ricevuta): `LeaderCompleteness` violata, la copia indietro comanda.
  - `noreceiptcheck` (il testimone vota senza guardare la ricevuta): `LeaderCompleteness` violata.
  - `witnessdoublevote` (il testimone vota due volte nella stessa epoca): `ElectionSafety` violata in 2 passi.
  - `witnessamnesia` (il testimone riparte con il disco vuoto): `ElectionSafety` violata.
  - `noownepochrule` (in degradato si confermano voci di epoche vecchie): `LeaderCompleteness` violata in 8 passi.
- Controlli di raggiungibilità (devono essere violati, e lo sono): `sanity-degradato` (si arriva a una
  conferma in modo degradato) e `sanity-cambio` (dopo il modo degradato l'altra copia prende il comando
  e la copia che confermava da sola perde il disco).

## Cosa NON copre

- Messaggi atomici: ogni azione legge e scrive lo stato dell'altro in un passo. Messaggi persi o ritardati
  sono coperti come "l'azione non avviene" o "avviene più tardi", non come messaggi vecchi riconsegnati.
- Repliche intere del quaderno (come dopo il troncamento di Raft), non voce per voce.
- Nessuna lettura (lease, letture dal capo vecchio), nessuna copia su S3, nessun ripristino da S3.
- Nessuna vitalità: il modello dice che non succede niente di sbagliato, non che il sistema riparta. Il caso
  noto: se la copia che confermava da sola in modo degradato perde il disco, il testimone rifiuta per sempre
  l'altra copia (giusto: le mancano scritture confermate); si riparte solo dalla copia su S3, fuori dal modello.
- Due copie e un testimone soli, epoche fino a 5, quaderno fino a 4 voci.
