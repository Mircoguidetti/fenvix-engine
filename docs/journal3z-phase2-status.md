# journal3z fase 2 – stato (aggiornato a ogni pezzo)

Piano: `docs/journal3z-phase2-plan.md` (criteri del §2 fissati, non modificati). Decisioni del fondatore (30-10-2026):
niente codice del motore (P6 e §2c solo come interfaccia e documento, dopo l'unione di round3), niente TLA+ (§2f solo
strada 1), CI solo corta, valori configurabili (lease 10 s, upload 30 s, limiti 32/128 MB, rilascio lease dopo 5 s
senza giornale), niente AWS, il superamento dei controlli lo dichiara il coordinatore, non la sessione.

Codice della fase 2: `journal3z/p2/` (spazio di lavoro Cargo proprio; il cervello `journal3z/core` è usato, non
copiato né modificato). Strumenti locali: MinIO compilato dai sorgenti con `go install github.com/minio/minio@latest`
(modulo `v0.0.0-20260212201848-7aac2a2c5b7c`, go1.24.7) in `/root/j3z-tools/bin/minio`, fuori dal repository.

| pezzo | stato | stima del piano (giornate di sessione) | tempo usato | prova |
|---|---|---|---|---|
| P1 formato su disco | **fatto** | 0,5 – 1 | ≈ 0,1 (14:24–14:30) | `cargo test -p j3z-codec -p j3z-store`: 13/13 verdi; ogni controllo ha il suo gemello rotto che fallisce (sotto) |
| P2 recupero dopo riavvio | da fare | 0,5 | | |
| P3 programma del nodo | da fare | 1 – 1,5 | | |
| P4 rete + proxy di guasti | da fare | 0,5 – 1 | | |
| P5 client scrittore + MinIO | da fare | 1 – 2 | | |
| P7 banco di prova a 3 processi | da fare | 1 – 2 | | |
| P8 12 versioni rotte sul codice vero | da fare | 0,5 – 1 | | |
| P9 (§2f) controllo esaustivo a 2 scrittori | da fare | 1 – 2 | | |
| CI corta | da fare | — | | |
| P6 integrazione col motore, §2c | **rinviato** (decisione 1): solo interfaccia e documento | 2 – 5 | | |

## Criteri del §2 (la sessione pubblica i dati; il "passato" lo dichiara il coordinatore)

| criterio | stato |
|---|---|
| §2a banco a 3 processi con guasti | non ancora misurato |
| §2b 12 versioni rotte sul codice vero | non ancora misurato |
| §2c test dedicato blocco SlateDB prima del segnalibro | **non eseguibile in questa fase** (richiede il motore, decisione 1) |
| §2d ripetizioni con semi diversi | non ancora misurato |
| §2e revisione indipendente | pacchetto da preparare quando i pezzi sono pronti |
| §2f controllo esaustivo a 2 scrittori | non ancora misurato |

## P1 – formato su disco (fatto)

Codice: `journal3z/p2/codec` (codifica di record e messaggi, cornice `lunghezza | CRC-32 | corpo`, numero di versione
nei messaggi), `journal3z/p2/store` (segmenti, tabella dei recinti, identità del nodo, recupero, strato file system con
guasti iniettabili). Regole: un record strappato in coda all'ultimo segmento è ignorato e tagliato; un record rovinato
**in mezzo** (con record validi dopo) fa rifiutare l'avvio (si tratta il nodo come perso); la tabella dei recinti è
riscritta in modo atomico prima di cancellare qualunque segmento; i segmenti si cancellano solo dal più vecchio, e solo
se tutto quello che contengono è sotto il punto di taglio (in S3) o il database è cancellato; un errore di scrittura o
di fsync "avvelena" lo store (nulla più è accettato: il processo deve uscire e ripartire dal disco); una cartella persa
genera una nuova identità del nodo.

Test (tutti verdi; tra parentesi il gemello rotto e cosa succede):

| test | cosa prova | gemello rotto |
|---|---|---|
| `records_roundtrip`, `batches_roundtrip` | 20.000 record e 5.000 lotti casuali codificati e decodificati identici | — (equivalenza) |
| `frames_detect_flips_and_truncation` | ogni bit invertito e ogni taglio di 300 record casuali è rilevato | il test stesso è il controllo negativo del CRC |
| `decoders_never_panic` | 100.000 sequenze di byte casuali: nessun crash | — |
| `wrong_version_refused` | messaggio con versione 2 rifiutato | — |
| `roundtrip_and_new_identity` | stato riletto = stato scritto; cartella persa → identità nuova | — |
| `segments_without_identity_refused` | segmenti senza `NODE_ID` → avvio rifiutato | — |
| `torn_tail_ignored_and_cut` | coda strappata ignorata, tagliata, secondo riavvio ok | senza taglio: il secondo riavvio rifiuta (rilevato) |
| `corruption_in_the_middle_refused` | byte rovinato con record validi dopo → avvio rifiutato | accettandolo: l'avvio "riesce" perdendo 4 record sincronizzati (rilevato) |
| `fence_survives_segment_deletion` | promessa 42 sopravvive alla cancellazione del suo segmento | senza tabella dei recinti: la promessa è dimenticata (rilevato) |
| `segments_deleted_only_as_a_prefix` | dopo un taglio di coda e un trim, lo stato riletto = stato vivo | cancellando non in ordine: l'indice 10 dell'epoca 1, troncato, risorge (rilevato) |
| `fsync_error_poisons` | fsync fallito → nessuna scrittura o fsync accettati dopo | con la mutazione `ignore_fsync_error`: lo store va avanti (rilevato) |
| `disk_full_poisons` | disco pieno → avvelenato; al riavvio il record scritto a metà è una coda strappata | — |
