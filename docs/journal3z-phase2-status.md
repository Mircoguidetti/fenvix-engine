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
| P1 formato su disco | **fatto** | 0,5 – 1 | ≈ 0,1 (14:24–14:30) | `cargo test -p j3z-codec -p j3z-store`: 14/14 verdi (13 + 1 aggiunto in P2); ogni controllo ha il suo gemello rotto che fallisce (sotto) |
| P2 recupero dopo riavvio | **fatto** | 0,5 | ≈ 0,1 (14:31–14:43) | 2.000 semi a caso + 200 kill -9 di processo vero: 0 fallimenti sul codice corretto; ogni gemello rotto scoperto; **un difetto vero trovato e corretto** (sotto) |
| P3 programma del nodo | **fatto** | 1 – 1,5 | ≈ 0,1 (14:44–14:58, insieme a P4) | 5 test su processi veri (1 e 3 nodi), ognuno col gemello rotto scoperto (sotto) |
| P4 rete + proxy di guasti | **fatto** | 0,5 – 1 | (con P3) | proxy: 7 controlli dei modi tutti verdi; proxy rotto (ignora i modi) fallisce 6 su 7 |
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

## P2 – recupero dopo riavvio (fatto)

Prove (`cargo test -p j3z-store --features testbugs --test recovery -- --nocapture`, 256 s):

| prova | risultato sul codice corretto | gemelli rotti (devono essere scoperti) |
|---|---|---|
| `random_equivalence`: 2.000 semi; un generatore pilota il cervello vero del nodo (recinti con epoche crescenti, scritture, sovrascrittura di code divergenti, trim anche oltre la fine, cancellazioni); segmenti piccoli (200–3.000 byte), pulizia dopo ogni fsync, riavvii puliti a metà; alla fine o chiusura pulita (stato riletto = stato vivo) o crash simulato tagliando l'ultimo segmento in un byte a caso dopo l'ultimo fsync (stato riletto = stato sincronizzato + un prefisso dei record non sincronizzati; un secondo riavvio deve dare lo stesso stato) | **0 fallimenti su 2.000** | senza tabella dei recinti: scoperto in 1.951/2.000; pulizia non in ordine: 686/2.000; coda strappata non tagliata: 253/2.000 |
| `kill9_real_process`: 200 volte un processo vero (`j3z-killchild`) scrive, fa fsync e pulizia, ed è ucciso con SIGKILL dopo 5–150 ms; lo stato riletto deve essere il risultato di un prefisso dei record generati lungo almeno quanto il processo aveva dichiarato sincronizzato | **0 fallimenti su 200**; 142.688 record dichiarati sincronizzati e ritrovati in tutto | senza tabella dei recinti: 189/200; pulizia non in ordine: 27/200 |

Limite dichiarato: un kill -9 uccide il processo, non la macchina: la cache del sistema operativo sopravvive, quindi
questa prova non può vedere un fsync mancante (lo vede il crash simulato di `random_equivalence`, che taglia i dati non
sincronizzati, e la mutazione `ack_before_fsync` del §2b).

**Difetto trovato dalla prova a caso e corretto** (primo giro: 1.392 semi su 2.000 e 82 kill su 200 fallivano sul
codice corretto, tutti con lo stesso messaggio "append of index N after M"): quando un database viene cancellato, i suoi
segmenti più vecchi diventano cancellabili; se un segmento più nuovo restava in vita per colpa di un **altro** database,
al riavvio i record del database cancellato rimasti in quel segmento venivano riapplicati sopra la tabella dei recinti
(che dice già "cancellato") e il nodo rifiutava di ripartire (buco negli indici). Nessuna scrittura confermata sarebbe
stata persa, ma il nodo non sarebbe ripartito. Correzione: al recupero i record di un database che la tabella dei
recinti dà per cancellato si saltano (un database cancellato non accetta più nulla, quindi quei record precedono tutti
la cancellazione). Test fisso aggiunto: `erased_database_not_replayed` (con il gemello `replay_erased`: il recupero
fallisce). Dopo la correzione: secondo giro completo, risultati nella tabella sopra. Tentativi: 2 (il primo fallito,
riportato qui).

## P3 + P4 – programma del nodo, rete, proxy di guasti (fatti)

Codice: `journal3z/p2/net` (connessione scrittore–nodo: stretta di mano con identità, lotti nella cornice CRC con
versione, riconnessione, ping ogni 500 ms e chiusura se non arriva nulla per 2 s; binario `j3z-proxy`),
`journal3z/p2/node` (binario `j3z-node`: un solo thread col cervello `NodeCore` e il disco; per ogni giro: gestisce
tutti i lotti in attesa, scrive tutti i record, **un** fdatasync, **poi** risponde; esce con 75 su errore di scrittura o
di fsync, con 70 su un record che rompe un log, con 3 su un disco non recuperabile). Guasti del disco iniettabili da un
file (`--faults-file`) solo con la funzione di compilazione `faults`; difetti di prova (`--mutation`) solo con
`mutations`: il binario normale rifiuta entrambe le opzioni (codice 2). Lo strato disco con guasti, a un errore di
fsync iniettato, **butta davvero** i dati non sincronizzati (come Linux quando la scrittura su disco fallisce): così un
nodo che ignorasse l'errore confermerebbe dati spariti.

Test `cargo test -p j3z-node --features faults,mutations --test node` (processi veri, rete vera in loopback):

| test | risultato sul codice corretto | gemello rotto |
|---|---|---|
| `replies_only_after_fsync` (fsync rallentato a 300 ms) | conferma dopo **302,5 ms** | `ack_before_fsync`: conferma dopo **1,1 ms** (scoperto) |
| `fsync_error_makes_the_node_exit` | 4ª scrittura **non** confermata; il nodo esce con **75**; al riavvio ci sono barriera + 3 scritture | `ignore_fsync_error`: 4ª scrittura **confermata**, nodo vivo, al riavvio la 4ª non c'è (scoperto) |
| `disk_full_makes_the_node_exit` | nessuna conferma, uscita **75** | — |
| `restart_keeps_identity_and_log` | stessa cartella → stessa identità; cartella persa → identità nuova | — |
| `takeover_on_three_real_nodes`: A (epoca 10) scrive 30 confermate; B (epoca 11) apre vedendo solo i nodi 1 e 2; A riprende e scrive | B recupera **30 su 30**; A **non** ha la scrittura confermata ed è dichiarato "recintato" | `no_fencing` sui nodi: la scrittura tardiva di A è **confermata** (scoperto) |

Test `cargo test -p j3z-net --features testbugs --test proxy`: passa, buco nero (e ritorno a passa sulla stessa
connessione), solo scrittore→nodo perso, solo nodo→scrittore perso, ritardo 200 ms (andata e ritorno ≥ 400 ms), taglio
(connessione aperta chiusa, nuove rifiutate): **7 controlli verdi**; gemello col proxy che ignora i modi: **6 controlli
su 7 falliscono** (solo "passa" resta vero, come deve).

Tentativi: i 3 test a nodo singolo sono falliti al primo giro per un errore del test (lo scrittore di prova con un solo
nodo deve usare un quorum di 1 anche in apertura); corretto il test, non il nodo; secondo giro tutto verde.
