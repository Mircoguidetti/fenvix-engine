# journal3z – pacchetto per la revisione indipendente (§2e del piano di fase 2)

Pacchetto preparato dalla sessione che ha scritto il codice (31-10-2026). Contiene **solo** cosa leggere, come
eseguire le prove, dove si trova il codice di ogni punto, dove stanno i risultati grezzi e cosa consegnare. **Non** contiene spiegazioni sul perché il codice sarebbe giusto: il revisore le
deve ricavare da sé (regola del §2e). Il revisore lo sceglie il coordinatore.

Branch: `journal3z`. Il commit da rivedere è quello indicato dal coordinatore quando consegna il pacchetto; tutti i
controlli del §2 devono essere sullo stesso commit.

**Regola del fondatore (31-10-2026):** il giornale a 3 zone non diventa la modalità normale in produzione senza
questa revisione superata. Il superamento lo dichiara il fondatore, non chi ha scritto il codice né il revisore.

## 0. Cosa consegna il revisore

Un file `docs/journal3z-review.md` (sul branch `journal3z`, o consegnato al coordinatore) con:
1. per ognuno dei punti della sezione 4 (i 12 del piano più i 4 aggiunti): "visto, nessun problema" oppure l'elenco dei
   problemi;
2. per ogni problema: gravità (**alta** = può far perdere una scrittura confermata o rompere una regola del protocollo;
   **media** = errore che costa disponibilità o che le prove non vedrebbero; **bassa** = il resto), file e riga, e un
   caso concreto (sequenza di eventi) che lo mostra;
3. per il punto 1, la tabella "regola del disegno §5 → file:riga";
4. cosa non è riuscito a controllare e perché.
Passa (piano §2e) se: 0 problemi di gravità alta aperti; i problemi medi corretti o accettati per scritto dal
fondatore.

## 1. Da leggere

- `docs/journal3z-design.md` – il disegno (protocollo §5; la regola di subentro del lease §5.1, cambiata il 30-10-2026).
- `docs/journal3z-phase2-plan.md` – il piano, con i criteri del §2 (il §2f è stato cambiato dal fondatore il 30-10-2026:
  vedere la nota nel §2f).
- Il codice (sotto). **Non** leggere `docs/journal3z-phase2-status.md` prima di aver scritto il proprio elenco: contiene
  le spiegazioni di chi ha scritto il codice.

## 2. Dove sta il codice

| parte | file |
|---|---|
| protocollo (nodo e scrittore, macchine a stati senza I/O) | `journal3z/core/src/lib.rs` |
| regola di subentro del lease | `journal3z/core/src/lease.rs` |
| interfaccia verso il motore (`Journal`) e backend locale | `journal3z/api/src/lib.rs`, `journal3z/api/src/local.rs` |
| formato su disco, recupero, strato disco con guasti | `journal3z/p2/store/src/lib.rs` (test in `tests.rs`, `tests/recovery.rs`) |
| codifica di record e messaggi, cornici CRC | `journal3z/p2/codec/src/lib.rs` |
| rete scrittore–nodo, proxy di guasti | `journal3z/p2/net/src/lib.rs`, `journal3z/p2/net/src/bin/j3z-proxy.rs` |
| programma del nodo | `journal3z/p2/node/src/main.rs` |
| client scrittore (backend a 3 zone) | `journal3z/p2/writer/src/journal.rs` |
| lease e "database finto" su store a oggetti | `journal3z/p2/writer/src/meta.rs` |
| sostituto del motore (fino a P6) | `journal3z/p2/writer/src/standin.rs`, `journal3z/p2/writer/src/bin/j3z-writer.rs` |
| banco di prova e controllore | `journal3z/p2/bench/src/lib.rs`, `journal3z/p2/bench/src/bin/j3z-soak.rs` |
| simulatore | `journal3z/sim/src/world.rs`, `journal3z/sim/src/main.rs` |
| controlli esaustivi | `journal3z/mc/src/main.rs` (fase 1), `journal3z/mc/src/bin/mc2.rs`, `journal3z/mc/src/bin/lease_mc.rs` |
| CI corta | `.github/workflows/journal3z-ci.yml`, `journal3z/ci/` |

## 3. Come eseguire le prove (tutto in locale)

```bash
cd journal3z/core && cargo test --release
```

```bash
cd journal3z/p2 && cargo test --release -p j3z-codec -p j3z-store --lib
```

```bash
cd journal3z/p2 && cargo test --release -p j3z-store --features testbugs --test recovery -- --nocapture
```

```bash
cd journal3z/p2 && cargo test --release -p j3z-node --features faults,mutations --test node
```

```bash
cd journal3z/p2 && cargo test --release -p j3z-net --features testbugs --test proxy
```

Le prove con MinIO richiedono il binario `minio` (variabile `J3Z_MINIO` o `/root/j3z-tools/bin/minio`):

```bash
cd journal3z/p2 && cargo test --release -p j3z-bench --test p5_writer -- --nocapture --test-threads 1
```

Simulatore e controlli esaustivi:

```bash
cd journal3z/sim && R=/tmp/sim bash campaign.sh
```

```bash
cd journal3z/mc && cargo build --release && ./target/release/mc2 --epochs 2 --idx 2 --fifo --subsume
```

## 4. I 12 punti del §2e e dove guardare

Per ogni punto il revisore scrive i problemi trovati con gravità (alta / media / bassa) e la riga di codice.

| punto (testo nel piano §2e) | dove guardare per primo |
|---|---|
| 1. ogni regola del protocollo ha il suo codice ("regola → file:riga") | `core/src/lib.rs` (`NodeCore::handle`, `WriterDb`), `core/src/lease.rs` |
| 2. "prima fdatasync, poi rispondi" in ogni strada, anche per messaggi duplicati | `p2/node/src/main.rs` (`core_loop`), `p2/store/src/lib.rs` (`write`, `sync`) |
| 3. il recinto non si perde mai (pulizia segmenti, sonno, riavvio); disco perso ⇒ identità nuova | `p2/store/src/lib.rs` (tabella dei recinti, `gc`, `open`, `NODE_ID`) |
| 4. ordine di apertura nel motore: lease → blocco vecchio scrittore → segnalibro → giornale | `p2/writer/src/standin.rs` (`run_db`, `open`) – nel motore vero: P6, non ancora scritto |
| 5. segnalibro nello stesso lotto atomico dei dati; replay idempotente | `p2/writer/src/meta.rs` (`StoreOps::upload`) – nel motore vero: P6 |
| 6. il taglio del giornale usa solo valori confermati da S3; `uploaded(k)` solo quando lo stato fino a k è nello store per intero e non può più essere sostituito | `p2/writer/src/journal.rs` (`uploaded`), `p2/writer/src/standin.rs` (`upload`, `maybe_reconfigure`), `p2/writer/src/meta.rs` (`put_chunk`) |
| 7. il recupero legge qualunque coppia di nodi che risponde | `core/src/lib.rs` (`WriterDb::choose`, `choose_source`) |
| 8. errori: fsync fallito ⇒ il nodo si ferma; disco pieno; tempi massimi; nessun errore ignorato | `p2/node/src/main.rs`, `p2/store/src/lib.rs`, `p2/writer/src/bin/j3z-writer.rs` (tempi massimi verso lo store) |
| 9. i difetti di prova non possono finire nei binari di produzione | funzioni di compilazione `faults`, `mutations`, `testbugs` nei `Cargo.toml`; `journal3z/ci/check_production_build.sh` |
| 10. il controllore del banco controlla davvero quello che dice | `p2/bench/src/lib.rs` (`check`, `disk_violations`, `lease_overlaps_split`), `p2/bench/src/bin/j3z-soak.rs` (`checkpoint`, recupero) |
| 11. l'indice del giornale è assegnato nello stesso ordine in cui le scritture sono applicate | `p2/writer/src/standin.rs` (`write`), `p2/writer/src/journal.rs` (`append`) – nel motore vero: P6 |
| 12. cancellazione, sonno, spostamento, rilascio del lease quando il giornale è irraggiungibile (un server smette di agire **prima** di cedere il lease) | `p2/writer/src/journal.rs` (`erase`, `close`), `p2/writer/src/standin.rs` (`run_db`, `own`: rilascio dopo 5 s) |

Punti aggiunti dopo il piano (decisioni del fondatore e difetti trovati dalle prove):

| punto | dove guardare per primo |
|---|---|
| 13. **regola di subentro** (disegno §5.1, 30-10-2026): chi tiene il lease agisce solo fino a "invio dell'ultimo rinnovo riuscito + lease" sul proprio orologio monotono; chi subentra aspetta di aver visto il lease immutato per lease + margine sul proprio orologio monotono, con scrittura condizionata su quella versione; nessun confronto tra orologi di macchine diverse | `core/src/lease.rs`, `p2/writer/src/meta.rs` (`LeaseOps`), `p2/writer/src/standin.rs` (`run_db`, `own`, `write`); il lease del motore vero (round3 `lease.rs`) **non** segue ancora questa regola (lavoro di P6) |
| 14. **ordine di cessione:** quando si perde la maggioranza dei nodi (o si cede il database per altri motivi), prima si smette di accettare scritture, poi si cede il lease | `p2/writer/src/standin.rs` (`run_db`, `own`); nel motore vero: lista di controllo di P6 nel piano |
| 15. **un caricamento non sostituisce mai uno precedente**; il giornale si accorcia solo fino a uno stato caricato per intero | `p2/writer/src/meta.rs` (`put_chunk`, `StoreOps::upload`), `p2/writer/src/standin.rs` (`upload`, `maybe_reconfigure`) |
| 16. **il controllore del banco**: le sovrapposizioni di lease subito dopo un salto in avanti dell'orologio monotono si contano a parte (ipotesi rotta apposta); verificare che la finestra e il conteggio non nascondano un difetto vero | `p2/bench/src/lib.rs` (`lease_overlaps_split`), `p2/bench/src/bin/j3z-soak.rs` (`MonoJump`, `checkpoint`); nel simulatore la stessa cosa: `sim/src/world.rs` (`check_lease_overlap`) |

## 5. Risultati grezzi (dove sono)

Tutti in `journal3z/results/phase2/` salvo dove detto. Ogni cartella ha i dati come escono dai programmi.

| prova | dove | criterio del piano |
|---|---|---|
| formato su disco e recupero (P1, P2) | `docs/journal3z-phase2-status.md` sezioni P1, P2 (numeri dei test); i test si rifanno con la sezione 3 | — |
| nodo, rete, proxy (P3, P4) | idem, sezione P3 + P4 | — |
| client scrittore su MinIO (P5) | `p5-run*.txt` (l'ultimo giro completo: `p5-run8-after-release-order-fix.txt`) | — |
| prove brevi di messa a punto del banco | `p7-trials/` (t1, t2, t3 fallita, t3b, t3c) | — |
| simulatore, campagna completa | `sim/` (e `sim-attempt1/`, primo giro respinto) | §2d (simulatore) |
| controlli esaustivi a uno scrittore | `mc-e1-*.json`, `mc2-e1-*` | §2f (a) |
| controllo esaustivo della regola di subentro | `lease/` | punto 13 |
| controllo esaustivo a due scrittori (limitato, non finito; e quello a limiti piccoli) | `p9/` | §2f: **non raggiunto**, sostituito (piano §2f) |
| prove lunghe del banco | `2a/` (in corso); tentativi falliti: `2a-attempt1/`, `2a-attempt2/` | §2a, §2d |
| 12 versioni rotte sul codice vero | `p8/` (da fare dopo il §2a) | §2b |
| CI corta | GitHub Actions, workflow `journal3z-ci` | — |

## 6. Limiti noti (dichiarati da chi ha scritto il codice)

- **Motore vero non integrato** (P6 rinviato, decisione 1): i punti 4, 5, 11, 13 (motore), 14 (motore) si vedono solo
  sul sostituto del motore; il test §2c non è eseguibile.
- **Due scrittori non provati per intero** dal controllo esaustivo (§2f non raggiunto); quello che lo sostituisce è
  scritto nel piano, §2f.
- **Il controllo esaustivo usa un hash di 64 bit per stato** (come nella fase 1): due stati diversi con lo stesso hash
  ne nasconderebbero uno (probabilità molto piccola, non zero).
- **Disco:** guasti iniettati da uno strato di prova (errore di fsync, disco pieno, interruzione di corrente), non da
  dischi veri che si guastano; kill -9 non perde la cache del sistema operativo.
- **Store a oggetti:** MinIO locale, non S3; MinIO rispetta le scritture condizionali sotto concorrenza nelle prove
  fatte (`p2/bench/tests/minio_cas.rs`: 2.400 giri, 0 errori), il comportamento di S3 non è provato qui.
- **Una sola macchina:** reti tra zone simulate con il proxy; tempi di rete di AWS non misurati.

## 7. Cosa manca ancora al momento della consegna

- L'integrazione col motore vero (P6) non esiste: i punti 4, 5, 11 si possono rivedere solo sul sostituto del motore.
- Le prove lunghe del §2a (in corso dal 31-10-2026 09:13) e le 12 versioni rotte sul codice vero (§2b, dopo il §2a):
  i loro risultati vanno nelle cartelle della sezione 5; il revisore li legge da lì. Il pacchetto non cambia.
