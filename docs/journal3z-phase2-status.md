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
| P3 programma del nodo | **fatto** | 1 – 1,5 | ≈ 0,05 (14:44–14:49, insieme a P4) | 5 test su processi veri (1 e 3 nodi), ognuno col gemello rotto scoperto (sotto) |
| P4 rete + proxy di guasti | **fatto** | 0,5 – 1 | (con P3) | proxy: 7 controlli dei modi tutti verdi; proxy rotto (ignora i modi) fallisce 6 su 7 |
| P5 client scrittore + MinIO | **fatto** | 1 – 2 | ≈ 0,1 (14:50–15:08) | 2 prove sul solo MinIO + 3 scenari su processi veri (3 nodi, 1–2 scrittori, MinIO, proxy), ognuno col gemello rotto scoperto (sotto) |
| P7 banco di prova a 3 processi | **in corso – fermato da un fallimento (vedi sotto)** | 1 – 2 | ≈ 0,2 finora (15:09–15:58) | programma `j3z-soak` scritto; 3 prove brevi di messa a punto: la 3ª ha trovato una **perdita di scritture confermate** causata dal sostituto del motore (P5), corretta; ripetuta: 0 perse; **domanda aperta** sul criterio di ripresa (sotto) |
| P8 12 versioni rotte sul codice vero | da fare | 0,5 – 1 | | |
| P9 (§2f) controllo esaustivo a 2 scrittori | **non riuscito finora – fermo, serve una decisione** | 1 – 2 | ≈ 0,15 | con le riduzioni corrette che ho, il controllo a 2 scrittori non finisce (oltre 45 milioni di stati senza segni di convergenza); dettagli e strade sotto |
| CI corta | **scritta** (primo giro su GitHub dopo questo push) | — | | vedi sezione "CI corta" |
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

## P5 – client scrittore, lease e MinIO (fatto)

Codice: `journal3z/p2/writer` (crate `j3z-writer`):

- `journal.rs` – `ThreeZone`, il backend a 3 zone dell'interfaccia `Journal` (`journal3z/api`): un collegamento per
  nodo, un compito che riceve i messaggi e ritrasmette (ogni 100 ms, configurabile), per ogni database il cervello
  `WriterDb` di `journal3z/core` (usato, non modificato). `append` assegna l'indice subito, sotto un solo lucchetto;
  la conferma arriva quando 2 nodi su 3 hanno il record su disco. Un nodo che risponde con un'epoca più alta fa
  fallire le conferme in sospeso con "recintato". Limite 128 MB di dati non caricati per database (`Quota`).
- `meta.rs` – lease e "database finto" sullo store a oggetti, **solo con scritture condizionali** (crea se assente,
  aggiorna solo se l'ETag è ancora quello letto), come il `lease.rs` di round3. L'epoca dello scrittore è il `seq`
  della scrittura che gli ha dato il lease. Aprire come scrittore alza `writer_epoch` nel manifesto (il recinto dello
  store); un caricamento di un'epoca vecchia viene rifiutato.
- `standin.rs` – il **sostituto del motore** per il banco (il motore vero arriva con P6 dopo l'unione di round3): per
  ogni database prende il lease, recinta lo store e legge il segnalibro, apre il giornale (rilegge i record sopra il
  segnalibro), accetta scritture, carica ogni 30 s (o prima a 32 MB), poi accorcia il giornale; rinnova il lease ogni
  terzo della durata; smette quando il lease scade sul proprio orologio; rilascia il lease dopo 5 s senza maggioranza
  di nodi; sostituisce un nodo con identità nuova (disco perso) a un punto di caricamento, con un'epoca nuova. Tutti i
  valori sono in `ServerCfg` (decisione 4: lease 10 s, upload 30 s, 32/128 MB, rilascio dopo 5 s; più tolleranza
  d'orologio 1,5 s) e si cambiano con `--config <json>`; nessuno è scritto nel codice dei test, che usano valori
  corti (`fast_config`: lease 2 s, upload 0,5 s, rilascio 1 s) per stare in pochi secondi.
- binario `j3z-writer`: protocollo cliente a righe JSON (`write`, `owned`, `flush`), righe di eventi su stdout
  (`OPEN … epoch … barrier`, `UPLOAD`, `LOST`, `RECONFIG`) per il controllore. **Rifiuta uno store che non sia su
  questa macchina** (solo `http://127.0.0.1:…`/`localhost`), e usa solo le credenziali passate sulla riga di comando
  (quelle di MinIO locale): non legge credenziali AWS dall'ambiente.

Banco: `journal3z/p2/bench` (crate `j3z-bench`, sarà esteso in P7): avvia MinIO, nodi, scrittori e proxy come processi
veri, generatore di carico (16 lavoratori, scrittura al proprietario attuale del database), controllore: alla fine
ogni scrittura confermata deve trovarsi nello store **all'indice confermato con lo stesso identificativo**; e nessuna
conferma di un'epoca vecchia a un indice ≥ barriera di un'epoca più nuova ("conferma tardiva").

Test `cargo test -p j3z-bench --test p5_writer -- --nocapture --test-threads 1` (81 s; risultati grezzi in
`journal3z/results/phase2/p5-run2.txt`):

| test | risultato sul codice corretto | gemello rotto |
|---|---|---|
| `lease_is_exclusive`: 3 server si contendono lo stesso lease, 100 giri | 100 giri con **esattamente 1** vincitore | scritture non condizionali: 0 giri su 100 con un solo vincitore (scoperto) |
| `store_fence_rejects_the_old_writer` | il caricamento dello scrittore vecchio è **rifiutato** (indice 11 resta quello nuovo) | `skip_store_fence`: accettato, l'indice 11 viene sovrascritto e il segnalibro torna indietro (scoperto) |
| `writer_kill_takeover`: 2 scrittori, 4 database; il nodo 3 resta indietro (buco nero verso lo scrittore 0); kill -9 dello scrittore 0 sotto carico; per 5 s lo scrittore 1 vede **solo** il nodo indietro, poi tutti | 24.325 conferme, **0 perse, 0 tardive**; lo scrittore 1 apre tutti i database 6,8 s dopo il kill (i 5 s di buco nero sono voluti dal test) | `recovery_single` sullo scrittore 1: **1.625 conferme perse** (scoperto) |
| `clock_jump_old_writer_fenced`: l'orologio dello scrittore 1 salta di +60 s, prende i database mentre lo scrittore 0 crede ancora di averli | 15.719 conferme, **0 perse, 0 tardive**; lo scrittore 0 si accorge dal giornale ("journal_fenced") | `no_fencing` sui nodi: **21 perse e 21 tardive** (lo scrittore 0 se ne accorge solo dallo store, troppo tardi) (scoperto) |
| `lost_disk_reconfig`: 1 scrittore, 3 database; il nodo 3 perde il disco (identità nuova); 4 s dopo il nodo 1 muore per sempre | 3 riconfigurazioni (una per database); **17.863 conferme dopo la morte del nodo 1**; 45.654 conferme, 0 perse | senza riconfigurazione: **0 conferme** dopo la morte del nodo 1 (resta un solo membro vecchio) (scoperto) |

Controllo a mano (binari compilati senza funzioni di prova, cartella separata): `j3z-writer` con uno store non locale →
codice 2; `j3z-writer` e `j3z-node` con `--mutation` o `--faults-file` → codice 2. Andrà nella CI corta.

Tentativi: ogni scenario è stato eseguito due volte (prima uno per uno, poi tutti insieme per il file dei risultati):
entrambe le volte tutto verde sul codice corretto e ogni gemello scoperto; numeri del primo giro: kill 21.766
conferme / gemello 1.217 perse; orologio 19.068 / gemello 11 perse e 11 tardive; disco 44.528 conferme, 16.468 dopo
la morte del nodo 1 / gemello 0. Nessuna correzione al codice è stata necessaria.

Note per P6 (integrazione col motore, rinviata):
- l'interfaccia `Journal::open` non porta i membri: `ThreeZone` aggiunge `open_members` (i membri stanno nel record
  del lease del database); il motore dovrà usare quello;
- `pending()` del backend a 3 zone conosce solo i database aperti dal processo: dopo un riavvio i database da
  recuperare si trovano dai lease, non da un disco locale;
- il sostituto carica solo fino all'indice **confermato** dal giornale; il motore vero dovrà fare lo stesso (o
  dimostrare che caricare record non ancora confermati è sicuro) – da verificare nella revisione §2e.

## P7 – banco di prova a 3 processi (in corso; fermato da un fallimento)

Programma `journal3z/p2/bench/src/bin/j3z-soak.rs` (configurazione del §2a): 3 nodi, 3 scrittori (il sostituto del
motore di P5 con i valori di produzione: lease 10 s, upload 30 s, 32/128 MB, rilascio dopo 5 s), 30 database, MinIO
locale, proxy su ogni collegamento scrittore→nodo, 200 scritture/s da 1 KB a ritmo aperto. Zona z = scrittore z +
nodo z. Un guasto alla volta, poi attesa che il sistema sia di nuovo sano (tutti i processi su, ogni database con un
proprietario e una conferma negli ultimi 5 s, ogni lease con i nodi attuali), poi una pausa di 10–30 s (dentro i
10–60 s del piano: scelti più corti perché l'attesa "sano" allunga il ciclo; serve per arrivare ai numeri minimi di
guasti in circa 12 ore). Guasti estratti da un sacchetto mescolato con le proporzioni dei minimi del §2a (÷10), più
tre tipi in più, segnati: interruzione di corrente di un nodo (i byte non sincronizzati spariscono: serve perché
`ack_before_fsync` sia visibile su processi veri, il kill -9 non lo mostra), interruzione di corrente di tutti e tre
i nodi insieme, store a oggetti fermo 1–4 s. Controlli: ogni 30 minuti (configurabile) e alla fine – ogni scrittura
confermata sotto il segnalibro dello store deve esserci al suo indice; regole sui dischi su copie delle cartelle dei
nodi (stessa coppia indice/epoca ⇒ stessi byte; nessun nodo taglia oltre il segnalibro); alla fine, dopo un caricamento
finale di tutti i database, **ogni** scrittura confermata. Sempre: righe di auto-controllo dei nodi (`VIOLATION` se un
nodo conferma sotto l'epoca promessa), processi che si fermano da soli (attesi solo dopo errore di fsync, disco pieno,
interruzione di corrente), conferme tardive. Ripresa: per ogni guasto e ogni database toccato, tempo dal guasto alla
prima conferma di una scrittura inviata dopo il guasto. Al primo fallimento la prova si ferma (§2d).

Aggiunte al codice dei pezzi precedenti fatte per il banco: interruzione di corrente nello strato disco con guasti
(test `power_cut_drops_only_unsynced` col gemello) e nel nodo (`power_cuts` nel file dei guasti, uscita 99); riga
`VIOLATION ack_below_promise` nel nodo; tempi massimi brevi per le richieste allo store nello scrittore (2 s, 2
tentativi) perché una richiesta appesa non tenga fermo il ciclo del lease.

Prove brevi di messa a punto (non contano per il §2a; risultati grezzi in `journal3z/results/phase2/p7-trials/`):

| prova | esito | causa | cosa è stato fatto |
|---|---|---|---|
| t1 (seme 1, 6 min) | **fallita**: nessuno scrittore prende un database in 120 s | errore mio nello scrittore, non ancora pubblicato: impostando i tempi massimi delle richieste allo store avevo sostituito tutte le opzioni del client, e così spento il permesso di usare `http://` verso MinIO; anche un test di P5 falliva | corretto; test P5 di nuovo verdi |
| t2 (seme 2, 6 min) | 13 guasti, 69.966 conferme, **0 perse, 0 tardive, 0 violazioni sui dischi**; ripresa dopo guasti a un nodo: max 0,95 s; **dopo guasti a uno scrittore: 9 casi su 41 oltre 15 s (max 18,3 s), tutti nello stesso isolamento di zona** | lo scrittore isolato è il "preferito" di un terzo dei database e riprova a prenderli ogni 200 ms: dopo aver rilasciato il lease (5 s senza maggioranza) se lo riprende subito, prova ad aprire il giornale per 5 s, fallisce, rilascia, e così via finché la zona non torna; gli altri scrittori (che provano ogni 1 s) non riescono a entrare | nel sostituto del motore: uno scrittore che non vede 2 nodi su 3 non prova a prendere database. Test P5 rifatti: verdi (`p5-run3.txt`) |
| t3 (seme 3, 10 min, solo guasti che toccano gli scrittori e lo store) | **FALLITA: 7.832 scritture confermate perse** (al primo controllo, 3,2 min; database 6, 9, 15, 18, 30 e altri), 0 violazioni sui dischi | **difetto nel sostituto del motore (P5), non nel giornale.** Dopo un disco perso, al punto di caricamento lo scrittore cambia i membri: carica, poi durante il cambio carica **una seconda volta**; senza nulla di nuovo, il secondo caricamento ha lo stesso indice (971) e la stessa epoca (35) del primo, e il nome dell'oggetto dipendeva solo da (indice, epoca): il secondo caricamento, **vuoto**, ha **sovrascritto** il primo (record 610–971); il giornale era già stato accorciato fino a 971 su conferma del primo caricamento → quelle scritture non erano più da nessuna parte. Prova: nello store il manifesto del database 15 elenca due volte `chunk-…971-…35`, e quell'oggetto pesa 422 byte contro i ~2.800 degli altri (file `t3-failed/db15-*.txt/json`) | nomi degli oggetti unici (suffisso a caso) e scrittura "solo creazione" (un caricamento non può mai sostituirne un altro); il cambio dei membri non ricarica se non c'è nulla di nuovo. Nuovo test `uploads_never_replace_each_other` (gemello col vecchio nome: il record 610 sparisce). Test P5 tutti rifatti: 6 su 6 verdi (`p5-run4.txt`) |
| t3b (stesso seme e stessi tipi di guasto, 15 min, dopo la correzione) | 36 guasti (10 kill di scrittore, 7 "due scrittori", 8 salti d'orologio, 3 isolamenti di zona, 4 errori di fsync, 1 disco pieno, 1 disco perso, 2 pause dello store); 164.544 conferme; 5 controlli: **0 perse, 0 tardive, 0 violazioni sui dischi**; ripresa dopo guasti a un nodo: max 0,69 s; **dopo guasti a uno scrittore: 20 casi su 410 oltre 15 s (max 15,9 s), mediana 8,1 s** | i 20 casi sono **un solo** episodio "due scrittori" (lo scrittore fermato aveva tutti i 30 database): vedi "domanda aperta" sotto | nessuna modifica: serve una decisione |

Cosa insegna t3 per il motore vero (P6, revisione §2e): il giornale si accorcia quando il motore dice "caricato fino a
k"; se il motore poi **sostituisce** ciò che aveva caricato, il giornale non può salvare nulla. SlateDB scrive file
nuovi e aggiorna il manifesto con scrittura condizionale, quindi questo difetto preciso non si trasferisce, ma il punto
"`uploaded(k)` solo quando lo stato fino a k è nello store per intero e non può più essere sostituito" va aggiunto ai
controlli della revisione (§2e punto 6).

## P9 – controllo esaustivo a 2 scrittori (§2f strada 1): iniziato

Nuovo programma `journal3z/mc/src/bin/mc2.rs`: usa **lo stesso modello** della fase 1 (`journal3z/mc/src/main.rs`,
importato, non copiato: lo stesso cervello vero, le stesse azioni, le stesse regole controllate; nel file della fase 1
sono cambiate solo le visibilità). Cambia solo il modo di cercare: ricerca su 4 thread con un insieme comune di stati
visti, e **rimozione dei messaggi "morti"** (messaggi la cui consegna non può più cambiare nulla, né ora né dopo:
ogni regola è scritta nel file con il motivo per cui vale per sempre). Verifica del programma contro la fase 1, uno
scrittore, indice 2: senza rimozione **18.698.439 stati, identico alla fase 1** (stesso numero esatto); con la
rimozione **1.804.416 stati** (10 volte meno), stesso verdetto (nessuna violazione). Prossimi passi (dopo lo sblocco):
la stessa cosa con 2 scrittori; se non basta, la simmetria dei 3 nodi (richiede una funzione in più nel `core`, solo
per il controllo, e quindi la ripetizione della campagna del simulatore, regola del §2d).

### Domanda aperta (P7): il criterio di ripresa del §2a punto 4 e i salti d'orologio

Il §2a chiede: dopo un guasto a uno scrittore, ogni suo database riprende **entro lease + 5 s = 15 s nel 99 % dei
casi**. Il §2a chiede anche, tra i guasti, salti dell'orologio di uno scrittore fino a ±5 s. E il disegno (§5.1, come
il lease di round3) dice: un altro server può prendere il database quando **il suo** orologio supera `expiresMs +
1,5 s`, dove `expiresMs` è scritto con l'orologio **di chi tiene** il lease. Quindi il tempo di subentro è:

  tempo dall'ultimo rinnovo (≤ 10 s) + 1,5 s + (anticipo dell'orologio di chi tiene − anticipo di chi subentra) + fino a 1 s di attesa del giro

Con gli scostamenti ammessi dai guasti (ciascuno scrittore fino a ±5 s, quindi fino a 10 s di differenza) il subentro
può arrivare a circa 22 s **per come è fatto il disegno**, non per un difetto. Lo mostrano i dati di t3b: con
l'orologio dello scrittore fermato in anticipo di 3,9 s e quello di chi subentra in ritardo di 2,0 s, 15,9 s; con
differenza 4 s, 13,1 s; quando l'orologio dello scrittore fermato era giusto, 10,6–11,2 s. In una prova di 12 ore,
con 150 episodi "due scrittori" e 300 kill di scrittore e gli orologi quasi sempre spostati, il 99 % entro 15 s con
buona probabilità **non** sarebbe rispettato. La sicurezza non c'entra (nessuna regola del giornale usa orologi: 0
perdite anche qui).

Due strade, da decidere (non cambio né il criterio né il disegno da solo):

1. **(proposta)** Cambiare la regola di subentro, nel sostituto del motore ora e nel motore vero in P6: chi vuole
   subentrare guarda il lease e lo considera scaduto quando l'ha visto **immutato** (stessa versione) per `lease +
   1,5 s` misurati col **proprio** orologio monotono, invece di confrontare il proprio orologio con un'ora scritta da
   un altro. Così gli scostamenti degli orologi tra macchine non contano più; il subentro diventa ≤ 10 + 1,5 + 1 ≈
   12,5 s (più l'apertura). Chi tiene il lease smette comunque di agire a `ultimo rinnovo + 10 s` sul proprio
   orologio monotono, come oggi. Il salto d'orologio del §2a resta come guasto, applicato all'orologio usato dal
   lease (un salto in avanti del monotono è come una macchina virtuale messa in pausa). È un cambiamento al §5.1 del
   disegno (solo disponibilità, non sicurezza) e al lease del motore (round3 usa l'ora assoluta come oggi).
2. Lasciare il disegno e riscrivere il criterio come "lease + 1,5 s + massimo scostamento d'orologio iniettato + 1 s"
   (con ±5 s: circa 22 s). Questo è ammorbidire un criterio dopo averlo misurato: lo può decidere solo il fondatore.

Finché non c'è una decisione, le prove lunghe del §2a non partono (sarebbero da rifare).

## Decisione del 30-10-2026: nuova regola di subentro (opzione 1)

Regola (disegno §5.1, aggiornato): chi vuole subentrare legge il lease e lo prende solo dopo averlo visto **immutato
nel contenuto** per lease + 1,5 s sul **proprio orologio monotono**, con una scrittura condizionata proprio su quella
versione; chi tiene il lease agisce solo fino a "invio dell'ultimo rinnovo riuscito + lease" sul proprio orologio
monotono. Nessuna ora scritta da un'altra macchina viene confrontata. Il criterio del §2a (15 s nel 99 %, 40 s nel
100 %) è **invariato**. Il lease del motore vero (round3 `lease.rs`) ha il vecchio difetto: segnato nel disegno (§5.1)
come lavoro di P6, il motore non è toccato.

Codice: la regola sta in un solo posto, `journal3z/core/src/lease.rs` (usato dal simulatore e dal sostituto del
motore), con le 3 versioni rotte chieste: `lease_no_margin` (subentro dopo il solo lease, senza 1,5 s),
`lease_wall_clock` (conta con l'orologio a muro, che salta), `lease_holder_only` (guarda solo il nome di chi tiene il
lease, non il contenuto: i rinnovi sembrano "immutato"). Il `core` è cambiato solo per l'aggiunta di questo modulo (il
protocollo del giornale non è toccato); per la regola del §2d tutto il simulatore e i controlli esaustivi sono stati
rifatti.

### Risultati grezzi

**Test del modulo** (`cargo test` in `journal3z/core`): `rule_never_overlaps` (derive ±6 % su entrambi gli orologi,
5 momenti di lettura, salti dell'orologio a muro di ±5 s, chi tiene il lease fermo o che rinnova): sovrapposizione
massima **0 ms**; `each_bug_overlaps`: ognuna delle 3 versioni rotte si sovrappone, il gemello giusto no; 5/5 verdi.

**Controllo esaustivo della regola** (`journal3z/mc/src/bin/lease_mc.rs`, nuovo; `results/phase2/lease/`): tempo
discreto (1 tick = 0,1 s, lease 100 tick, margine 15), ogni combinazione di: deriva di chi tiene e di chi osserva
(−6, 0, +6 %), momento in cui chi tiene si ferma, ogni rinnovo perso o arrivato dopo 0–3 tick, ogni tick di lettura,
un salto di ±5 s dell'orologio a muro, scrittura di subentro arrivata dopo 0–3 tick. Limite: orizzonte 250 tick (25 s:
copre l'arresto di chi tiene fino a ≈ 13 s, quasi 4 periodi di rinnovo; con 400 tick lo spazio cresceva oltre la
memoria e il tempo ragionevoli).

| versione | stati | subentri controllati | esito | tempo |
|---|---|---|---|---|
| regola giusta | 401.195.214 | 3.395.214 | **completo: nessuna sovrapposizione** | 634 s |
| `lease_no_margin` | 52.118.972 | 429.822 | sovrapposizione trovata (deriva −6 % / 0 %) | 82 s |
| `lease_wall_clock` | 75.844 | 6 | sovrapposizione trovata (salto +5 s) | 0,1 s |
| `lease_holder_only` | 362.842 | 72 | sovrapposizione trovata | 0,5 s |

**Simulatore** (`results/phase2/sim/`; campagna completa rifatta, 15 min 54 s): nuova regola controllata **L** ("mai
due scrittori che credono insieme di avere il lease di un database"), orologio monotono con deriva (±3 %, il margine
del simulatore 150 ms su 2 s copre ±3,6 %), salti dell'orologio a muro, e — aggiunto dopo il primo giro, vedi sotto —
**salti in avanti dell'orologio monotono** (lease/2 – 2×lease: l'ipotesi della regola rotta apposta).

| prova | semi | passi | conferme | violazioni |
|---|---|---|---|---|
| principale (guasto ogni 50–400 ms) | 1.000 | 200.000.000 | 4.122.679 | **0** (73.332 subentri; 25.651 salti del monotono; 1.514.390 passi con due scrittori insieme dopo un salto, previsti e coperti dai recinti) |
| moderata (guasto ogni 1–3 s) | 500 | 100.000.000 | 7.493.157 | **0** |
| 10 database | 200 | 40.000.000 | 1.209.782 | **0** |
| calma (senza guasti) | 100 | 20.000.000 | 1.488.374 | **0** |
| doppi guasti (perdita attesa, solo conteggio) | 200 | 40.000.000 | 103.508 | **0** |
| mirata sul lease, regola giusta (scrittore congelato con l'orologio più lento, gli altri più veloci) | 100 | 20.000.000 | 1.386.646 | **0** (1.283 congelamenti) |
| mirata, `lease_no_margin` | 100 | | | **scoperta in 15 semi su 100** (regola L) |
| mirata, `lease_wall_clock` | 100 | | | scoperta in 1 seme su 100 |
| mirata, `lease_holder_only` | 100 | | | scoperta in 100 su 100 |

Ripresa dopo la fine dei guasti (prova principale): massimo 4,8 s, mediana 0,25 s; 0 database fermi.

Controlli negativi (`sim-controls.json`, 100 semi × 200.000 passi, si ferma al primo scoperto): le **12 versioni rotte
del protocollo: tutte scoperte** con tutte le regole (come nella fase 1); con la sola regola di durabilità tutte tranne
`forget_fence` (come nella fase 1). Le 3 della regola di subentro: `lease_holder_only` al seme 1, `lease_wall_clock`
al seme 52, `lease_no_margin` **non** nella prova generale (scoperta nella prova mirata qui sopra, nel controllo
esaustivo e nei test del banco). Con la sola regola di durabilità le 3 del lease non si vedono: è giusto, il lease non
serve alla sicurezza.

**Primo giro della campagna (tentativo 1, `results/phase2/sim-attempt1/`)**: con la nuova regola e senza salti del
monotono, tutte le prove giuste a 0 violazioni, ma **3 versioni rotte non erano più scoperte** (nella fase 1 sì):
`skip_store_fence` (fase 1: seme 19), `forget_fence` con tutte le regole (fase 1: seme 6), `no_fencing` con la sola
durabilità (fase 1: seme 2). Causa: quei difetti si vedono solo quando due scrittori agiscono davvero insieme; prima
succedeva spesso perché gli orologi sfasati facevano subentrare troppo presto, con la regola giusta non succede più.
Il simulatore quindi non metteva più alla prova i recinti. Correzione (nel simulatore, non nel protocollo): il guasto
"salto in avanti dell'orologio monotono", che il disegno già prevede come caso tollerato; dopo un salto la regola L non
si controlla per 3 × (lease + margine) e le sovrapposizioni si contano a parte. Secondo giro: tabella sopra.

**Controlli esaustivi della fase 1 rifatti sul `core` aggiornato** (`results/phase2/mc-*.json`), uno scrittore:
indice 1 con duplicati **21.021 stati**, indice 2 **18.698.439**, indice 2 FIFO **2.765.555**, tutti completi senza
violazioni, **numeri identici alla fase 1**; `mc2` con rimozione dei messaggi morti 1.804.416. Questo modello non
contiene il lease (uno scrittore può aprire in qualunque momento), quindi vale per qualunque regola di subentro; la
regola stessa è controllata da `lease_mc` qui sopra. Due scrittori: P9.

**Sostituto del motore e banco**: il lease usa la regola del `core` (lettura → osservazione → presa condizionata
sulla versione letta; il nome di chi tiene è unico per processo, quindi uno scrittore riavviato aspetta come gli
altri; le scritture sono rifiutate da chi tiene appena il suo orologio monotono supera la scadenza). Righe `TAKEN` e
`HOLD` in tempo reale e controllo "sovrapposizioni di lease" nel banco. Orologi degli scrittori nel banco: salto
dell'orologio a muro (±0,5–5 s, non usato dal lease), **deriva** dell'orologio monotono (±5 %), **salto in avanti del
monotono** (+0,5–5 s: rompe apposta l'ipotesi, perché anche sul codice vero ci siano episodi con due scrittori e le
versioni rotte dei recinti si possano scoprire in P8; le sovrapposizioni subito dopo un salto si contano a parte).

Test P5 (`results/phase2/p5-run6.txt`, 9 su 9 verdi, ognuno col gemello rotto scoperto):

| test | codice giusto | gemello |
|---|---|---|
| `renewing_holder_is_never_taken` | 2 episodi, 0 subentri, 0 sovrapposizioni | `lease_holder_only`: sovrapposizione di 1.492 ms |
| `wall_clock_jump_changes_nothing` (chi tiene congelato, salto +5 s dell'orologio a muro di chi osserva) | 3 subentri, 0 sovrapposizioni | `lease_wall_clock`: 2 sovrapposizioni (prima 1.151 ms) |
| `margin_covers_clock_drift` (deriva −5 % / +5 %) | 4 subentri, 0 sovrapposizioni | `lease_no_margin`: 2 sovrapposizioni (prima 130 ms) |
| `two_writers_fenced` (due scrittori insieme forzati con `lease_holder_only`) | 18.900 conferme, **0 perse, 0 tardive** | nodi `no_fencing`: **241 perse e tardive** |
| `lease_is_exclusive`, `store_fence…`, `uploads_never_replace…`, `writer_kill_takeover`, `lost_disk_reconfig` | come prima, verdi | gemelli scoperti |

Tentativi dei test P5 (riportati tutti, `p5-run5.txt`): primo giro 7 su 9; falliti (a) `lease_is_exclusive`: 3 giri
su 100 con più vincitori — **errore del test**: con la nuova regola `take` non decide più da solo, la decisione
(libero / mio / immutato) la prende chi chiama, e il test chiamava `take` sempre; prova a parte che MinIO rispetta le
scritture condizionali (`bench/tests/minio_cas.rs`: 2.400 giri, 0 errori); la decisione ora è un metodo del lease
usato sia dal sostituto sia dal test; (b) il vecchio test "scrittore congelato" non poteva più far vedere `no_fencing`
(con la regola giusta lo scrittore congelato sa di aver perso il lease quando riparte): sostituito da
`two_writers_fenced`. Secondo giro: 9 su 9.

### Ripetizione di t3b con la nuova regola (t3c)

Stesso seme (3), stessi tipi di guasto di t3b più deriva e salto in avanti dell'orologio monotono, 15 minuti, valori
di produzione (lease 10 s, margine 1,5 s), risultati in `results/phase2/p7-trials/t3c/`:

| | t3b (regola vecchia) | **t3c (regola nuova)** |
|---|---|---|
| guasti | 36 | 36 (9 kill di scrittore, 6 "due scrittori", 7 salti dell'orologio a muro ±0,5–5 s, 4 salti in avanti del monotono, 2 derive ±5 %, 3 isolamenti di zona, 1 disco perso, 1 interruzione di corrente di tutti i nodi, 1 errore di fsync, 2 pause dello store) |
| scritture confermate | 164.544 | 165.523 |
| perse / tardive / violazioni sui dischi | 0 / 0 / 0 | **0 / 0 / 0** |
| sovrapposizioni di lease | (non misurate) | **0** (e 0 anche dopo i salti del monotono) |
| ripresa dopo guasti a uno scrittore: casi oltre 15 s | 20 su 410 (max 15,9 s) | **0 su 390 (max 13,3 s; 99° percentile 12,9 s; mediana 9,1 s)** |
| casi oltre 40 s | 0 | 0 |
| ripresa dopo guasti a un nodo: max | 0,69 s | 0,74 s |

Questa è una prova breve di messa a punto (15 minuti, un seme): **non** è la misura del §2a, che si fa con le prove
lunghe.

## CI corta (decisione 3)

`.github/workflows/journal3z-ci.yml` (solo su push al branch `journal3z` che tocca `journal3z/`, o a mano; niente
segreti, niente cloud): test unitari (core con la regola di subentro, formato su disco, proxy, nodo), simulatore corto
sul codice giusto (20 semi × 100.000 passi), controlli negativi (le 12 versioni rotte del protocollo e le 3 della
regola di subentro devono essere tutte scoperte: `journal3z/ci/check_controls.py`), controllo esaustivo della regola
(giusta: orizzonte 150 tick; versioni rotte: 250) e a uno scrittore (indice 1), build di produzione che rifiuta le
opzioni di prova (`journal3z/ci/check_production_build.sh`: 6 controlli, tutti "codice 2" in locale). Le prove con
MinIO e le prove lunghe restano manuali. Prova locale dello script dei controlli sui risultati della campagna:
"every negative control caught; correct versions clean".

## Prove lunghe del §2a: pezzo 1 FALLITO (fermo)

`results/phase2/2a/chunk-1/` (seme 1001, previsto 115 minuti, fermato da solo al controllo dei 60 minuti, §2d):
139 guasti (26 kill di nodo, 27 kill di scrittore, 3 dischi persi, 16 nodi isolati, 7 tagli in una direzione, 5
zone isolate, 15 salti dell'orologio a muro, 6 derive, 8 salti del monotono, 12 "due scrittori", 3 errori di fsync, 3
dischi pieni, 5 interruzioni di corrente di un nodo, 1 di tutti, 2 pause dello store); 716.363 scritture inviate,
**690.040 confermate, 0 perse, 0 tardive, 0 violazioni sui dischi**; ripresa dopo guasti a uno scrittore: 796 casi,
**0 oltre 15 s**, massimo 13,3 s, 99° percentile 13,0 s; dopo guasti a un nodo: 2.005 casi, massimo 1,1 s.
**Fallito per 14 sovrapposizioni di lease non precedute da un salto del monotono** (al controllo dei 60 minuti): lo
scrittore 0 ha preso 14 database in 0,8 s mentre gli scrittori 1 e 2 li tenevano e rinnovavano regolarmente
(sovrapposizioni di 7,1–7,9 s).

Causa (trovata nei registri): **errore mio nell'orologio simulato degli scrittori** (`writer/src/standin.rs`, solo
codice del banco). Al secondo 2.070,1 il banco ha cambiato la velocità dell'orologio dello scrittore 0 (deriva
+2,6 %); il cambio di velocità ricalcolava la base dell'orologio monotono **contando due volte i salti in avanti
già fatti prima** (lo scrittore 0 ne aveva avuti), quindi l'orologio è saltato in avanti di nascosto: per l'osservatore
era passato più tempo del vero, e ha preso i database (lo stesso effetto di un salto del monotono, ma non registrato
come tale). La regola e il giornale si sono comportati come previsto: i recinti hanno respinto lo scrittore vecchio
("journal_fenced"), 0 perse. Correzione: il cambio di velocità non sposta più l'orologio; test
`rate_change_does_not_move_the_clock` (gemello col vecchio codice: salto di 3.000 ms). Test P5 del lease rifatti dopo
la correzione: 4 su 4 verdi con i gemelli scoperti (`p5-run7-lease-after-clock-fix.txt`). Il simulatore non ha lo
stesso errore (lì i salti stanno già nella base).

Per il §2d il gruppo delle prove lunghe ripartirà da capo (pezzo 1 compreso) dopo il tuo via.

## P9 – controllo esaustivo a 2 scrittori: non riuscito (fermo, serve una decisione)

Obiettivo (§2f strada 1): il controllo con 2 scrittori, 2 epoche, indice 2, messaggi in qualsiasi ordine deve
**finire** senza violazioni e trovare i 4 difetti mancati nella fase 1 più i 3 già trovati.

Fatto (`journal3z/mc/src/bin/mc2.rs`, stesso modello della fase 1):
1. rimozione dei messaggi morti (con uno scrittore: da 18.698.439 a 1.804.416 stati, stesso verdetto);
2. **sottomissione per messaggi in volo** (`--subsume`): uno stato con le stesse parti di uno già visto e un
   sottoinsieme dei suoi messaggi si salta (giustificazione nel file; con uno scrittore 1.297.262 stati, stesso verdetto,
   `ack_on_one` trovato);
3. ricerca parallela.

Risultati con 2 scrittori (`results/phase2/p9/`): solo rimozione → fermato a **45.652.654 stati dopo 31 minuti**
senza violazioni e senza segni di fine (crescita costante, nessuno stato ancora con 2 scritture confermate); con anche
la sottomissione → fermato a ≈ 13 milioni dopo 8 minuti, stessa crescita. Difetti cercati con le riduzioni (trovare
non richiede di finire): `no_fencing` **trovato** (7.033.817 stati; nella fase 1 no), `ack_on_one`,
`trim_before_upload`, `ack_gap` trovati; `recovery_longest` **non trovato** in 17 minuti; `recovery_single` e
`keep_diverged` non eseguiti (serie interrotta da me per fermarmi).

Perché la **simmetria dei 3 nodi** (proposta nel piano) **non si può usare in modo corretto** qui: quando due nodi
hanno il log ugualmente aggiornato, il cervello vero (`core`) sceglie sempre quello col numero più basso; rinominando i
nodi la scelta cambia, e in certi casi (un nodo modificato da uno scrittore vecchio mentre l'altro legge) il
comportamento è davvero diverso. Usarla darebbe un "finito, nessuna violazione" che non si può difendere.

Strade possibili (da decidere; non cambio il criterio da solo):
1. **più tempo macchina** con una ricerca su disco (gli stati visti non stanno più in memoria): stima non nota, la
   crescita non mostra fine; potrebbe servire da molte ore a giorni, senza garanzia;
2. **cambiare il limite "messaggi in qualsiasi ordine"** in "ordine conservato su ogni collegamento
   scrittore–nodo, con perdite" (è come funziona davvero: una connessione TCP per coppia; tra coppie diverse ordine
   qualsiasi). È un ammorbidimento del criterio del §2f: lo può decidere solo il fondatore; non è detto che basti (nella
   fase 1 anche quella versione con 2 scrittori non era finita in 31 minuti, senza le riduzioni di oggi);
3. **rinunciare al "finisce"** e tenere come prova per i 2 scrittori: simulatore (1.000 semi, invariante L, 12 su 12
   difetti), controllo limitato a decine di milioni di stati, e i difetti trovati qui.

## Dopo il via del 30-10-2026 (§2f cambiato; §2a da zero)

### §2a, secondo tentativo: pezzo 1 FALLITO (fermo)

`results/phase2/2a-attempt2/chunk-1/` (seme 1001, fermato da solo al controllo dei 90 minuti): 209 guasti, 1.079.233
scritture inviate, **1.038.239 confermate, 0 perse, 0 tardive, 0 violazioni sui dischi**; ripresa dopo guasti a uno
scrittore: 1.232 casi, **0 oltre 15 s**, massimo 13,6 s, 99° percentile 12,7 s; dopo guasti a un nodo: 3.150 casi,
massimo 1,3 s (misurato). **Fallito per 1 sovrapposizione di lease** (database 17): lo scrittore 0, senza maggioranza
di nodi da 5 s, ha **rilasciato** il lease e solo dopo ha smesso di agire; lo scrittore 2 ha preso il lease libero 2 ms
prima che lo scrittore 0 smettesse (righe `LOST` e `TAKEN` in `events.txt.gz`). **Difetto del sostituto del motore**
(ordine sbagliato: un server deve smettere di accettare scritture **prima** di cedere il lease). Danni: nessuno
(senza maggioranza nessuna scrittura poteva essere confermata; i recinti avrebbero comunque respinto lo scrittore
vecchio). Correzione in `writer/src/standin.rs`: prima smette di agire e scrive `LOST`, poi rilascia. Test P5 rifatti:
9 su 9 verdi con i gemelli (`p5-run8-after-release-order-fix.txt`). **Non verificato con un test dedicato**: la finestra
è di pochi millisecondi e non sono riuscito a farne un test che fallisca in modo affidabile sul codice vecchio; la
verifica sarà la ripetizione delle prove lunghe. Lo stesso ordine va controllato nel motore vero (P6): aggiunto ai
punti della revisione (§2e).

### P9 con il criterio cambiato (ordine per collegamento, con perdite): NON finisce (fermo)

`mc2 --fifo --subsume` (2 thread, mentre giravano le prove del §2a). Verifiche del programma con uno scrittore:
senza riduzioni **2.765.555 stati, identico alla fase 1**; con la rimozione dei messaggi morti 2.070.512; con anche la
sottomissione per sottosequenze **18.077** (il numero varia di poco tra un giro e l'altro, 18.114 nel giro dopo,
perché con la sottomissione dipende dall'ordine di visita dei thread); verdetto sempre "nessuna violazione".

Due scrittori (2 epoche, indice 2): primo giro finito per memoria (limite 7 GB) a 17,2 milioni di stati dopo 17 min;
dopo aver compattato la memoria, secondo giro **fermato da me a 34.502.955 stati dopo 1 h 42 min** (5,6 GB),
**nessuna violazione, non finito**, ancora nessuno stato con 2 scritture confermate; stati nuovi ogni 15 minuti: 7,3 –
5,9 – 5,6 – 3,8 – 4,2 – 3,8 milioni (rallenta piano, nessun segno di fine). Registri in `results/phase2/p9/`. Stima
(**non so**): con questo andamento servirebbero molte ore e più memoria di quella della macchina (15 GB).

### P9: strada 3 (decisione del fondatore del 31-10-2026) – §2f NON RAGGIUNTO, sostituito

Il controllo esaustivo a due scrittori non è stato fatto finire. Come chiesto, ho provato un controllo a due scrittori
con **limiti più piccoli**, al massimo 15 minuti ciascuno, 2 thread (misurato; registri `results/phase2/p9/`):

| limiti (oltre a 2 epoche, indice 2, rimozione dei messaggi morti, sottomissione) | stati a 15 min | finito? |
|---|---|---|
| risposte dei nodi immediate (arrivano nello stesso passo o si perdono), messaggi verso i nodi in qualsiasi ordine | 16.150.744 | no |
| ordine per collegamento + il secondo scrittore non scrive dati suoi (solo la barriera) | 5.386.795 | no |
| qualsiasi ordine + il secondo scrittore non scrive dati suoi | 16.213.018 | no |
| ordine per collegamento + secondo scrittore senza dati + **nessun caricamento nello store** | 1.429.879 | no |

Nessuno è finito; nessuna violazione fino al punto di arresto. **Nessun controllo a due scrittori è stato completato.**
Mi sono fermato qui (niente altro tempo macchina, come deciso). Cosa è provato per intero, cosa no e cosa lo
sostituisce: piano §2f, nota "Esito del §2f".
