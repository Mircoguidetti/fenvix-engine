# journal3z – piano della fase 2 (prima tutto in locale, AWS dopo, a gradini)

Stato: **piano, nessun codice scritto, nessuna spesa, nessun lancio.** I criteri di passaggio di questo documento sono
fissati **prima** di costruire (commit di questo file). Se un criterio cambia, il cambio va fatto con un commit
separato, con il motivo, e approvato dal fondatore prima di misurare. Documenti di riferimento della fase 1:
`docs/journal3z-design.md` (protocollo), `docs/journal3z-sim.md` (simulatore), `docs/journal3z-tla.md` (controllo
formale), `docs/journal3z-libraries.md` (misure), `docs/journal3z-soak-plan.md` (prova di 72 ore).

## In breve (per il fondatore)

- Nella fase 1 il protocollo esiste come "cervello" provato al simulatore. Nella fase 2 si costruiscono i pezzi veri
  intorno: il programma del nodo che scrive su disco, la rete, il collegamento col motore del database, il recupero
  dopo un riavvio.
- Tutto si prova **sulla stessa macchina, con 3 processi veri** e guasti veri (processi uccisi, disco cancellato, rete
  tagliata, orologio che salta, due scrittori insieme). Si passa ad AWS solo quando **tutti** i controlli locali di §2
  sono superati.
- AWS in 3 gradini (2 ore, 12 ore, 72 ore), con una cifra massima per ciascuno che tu autorizzi prima di ogni gradino.
- Stima: **10–20 giornate di sessione** per la parte locale (definizione e ipotesi in §3), più il tempo macchina delle
  prove lunghe. Nessuna data.
- Il rischio più grande non è la velocità ma un errore nel collegamento col motore (il punto dove il nuovo scrittore
  deve bloccare il vecchio prima di leggere il segnalibro in S3): ha un test dedicato (§2c).

---

## 1. Cosa si costruisce, a pezzi piccoli e in ordine

Ogni pezzo si prova da solo prima di passare al successivo. "Si prova da solo" vuol dire: test automatici nel suo
crate, che girano con `cargo test`, senza gli altri pezzi. Il "cervello" del protocollo (`journal3z/core`) **non si
riscrive**: è lo stesso codice già provato dal simulatore; i pezzi nuovi lo usano.

| # | pezzo | cosa fa | dipende da | come si prova da solo |
|---|---|---|---|---|
| P1 | **Formato su disco** | file a segmenti con record numerati e somma di controllo (CRC) per ogni record; tabella dei "recinti" (l'epoca promessa per ogni database) salvata a parte e mai persa; cancellazione dei segmenti quando tutto è in S3 o cancellato | `core` (tipi dei record) | scrittura/lettura; record troncato a metà (ignorato); byte alterati (rilevati dal CRC); recinto che sopravvive alla cancellazione dei segmenti; errore di fsync simulato da uno strato di prova → il programma si ferma |
| P2 | **Recupero dopo riavvio** | all'avvio il nodo rilegge disco e tabella dei recinti e ricostruisce lo stato | P1 | prova "a caso": migliaia di sequenze di record casuali; lo stato riletto dal disco deve essere identico allo stato in memoria prima dello spegnimento; poi kill -9 del processo vero durante la scrittura e confronto |
| P3 | **Programma del nodo** | processo che riceve le richieste, le passa al cervello (`NodeCore`), scrive i record, fa **un** fdatasync per lotto e **solo dopo** risponde; si ferma su errore di fsync; disco pieno = smette di confermare | P1, P2 | il nodo avviato da solo con un client di prova: nessuna risposta prima del fsync (verificato con uno strato disco "lento"); ripetizione dei casi del simulatore sui nodi veri dove possibile |
| P4 | **Rete** | una connessione TCP per coppia scrittore–nodo, messaggi impacchettati in lotti, riconnessione, tempi massimi; formato dei messaggi con numero di versione. I nodi non parlano tra loro. Più un **proxy di guasti** (programma di prova tra scrittore e nodo) che ritarda, perde, taglia, o taglia in una sola direzione | P3 | loopback: messaggi interi, in ordine, riconnessione dopo taglio; il proxy verificato da solo (taglia davvero quello che dice) |
| P5 | **Client scrittore** | usa il cervello (`WriterDb`) per ogni database: apertura con recinto, scritture, conferme, riallineamento dei nodi indietro, upload e taglio del giornale; implementa il `Journal` della fase 1 per il backend "3 zone". Lease e oggetti S3 dietro un'interfaccia con due versioni: **MinIO locale** (S3 finto, già usato dai test di round3) e S3 vero (solo per AWS, dopo) | P4, `api` | contro 3 nodi locali: scritture confermate, nuovo scrittore che blocca il vecchio, riallineamento di un nodo fermo |
| P6 | **Integrazione col motore** | nel server MongoDB-compatibile, dietro un'opzione spenta di default (`--journal 3zone`): ordine di apertura **lease → blocco del vecchio scrittore SlateDB → lettura del segnalibro → apertura del giornale**; segnalibro `sys/journal` scritto nello stesso lotto atomico dei dati; replay al riavvio; abbandono del database se "recintato"; rilascio del lease se il giornale è irraggiungibile per 5 s; sonno, spostamento, cancellazione (GDPR); numero di "incarnazione" nel lease. **Modifica codice del motore** → serve il tuo via (§6) | P5, round3 (giornale locale, lease) | prima un'**indagine di sola lettura** del codice del motore (elenco dei punti da toccare, §3); poi i test esistenti del motore devono restare verdi con l'opzione spenta e accesa; più il test dedicato §2c |
| P7 | **Banco di prova locale a 3 processi** | avvia 3 nodi + 3 scrittori (o 3 motori) sulla stessa macchina, inietta guasti a caso, tiene un **registro delle conferme** fuori dai processi sotto prova, e un **controllore** che ad ogni punto di controllo ricostruisce ogni database da ogni coppia di nodi + S3 e confronta | P3–P6 | il controllore deve "vedere" una perdita finta inserita a mano nel registro (prova del controllore) |
| P8 | **Le 12 versioni rotte sul codice vero** | ognuno dei 12 difetti della fase 1 compilabile nei programmi veri con un'opzione di compilazione solo-test (mai nei binari di produzione) | P7 | §2b |
| P9 | **Chiusura del controllo formale a 2 scrittori** | §2f | `core`, `mc`, `tla` | §2f |
| P10 | **Revisione indipendente** | §2e | tutto | §2e |

Ordine: P1 → P2 → P3 → P4 → P5 → (indagine P6) → P6 → P7 → P8. P9 si può fare in parallelo da P1 in poi (non
dipende dai pezzi nuovi). P10 alla fine, prima di AWS.

---

## 2. Controlli locali prima di AWS (criteri fissati ORA)

Regola generale: **tutti** i controlli a–f devono essere superati, sulla stessa versione del codice (stesso commit).
Ogni modifica al codice dopo il superamento obbliga a ripetere a, b, c, d (e, f solo se la modifica tocca il
protocollo o l'integrazione). I risultati grezzi vanno nel repository (`journal3z/results/phase2/`), come nella fase 1.

### a) Programma vero, 3 processi sulla stessa macchina, con guasti

Configurazione: 3 nodi + 3 scrittori (motore vero con l'opzione `--journal 3zone` appena P6 esiste; prima, il client
scrittore di P5), 30 database, MinIO locale come S3, lease di 10 s (proposta della fase 1), upload ogni 30 s, scritture
da 1 KB a ritmo aperto (in tutto 200 al secondo).

Guasti, uno ogni 10–60 s a caso: kill -9 di un nodo; kill -9 di uno scrittore; **disco perso** (cartella del nodo
cancellata, il nodo torna con un'identità nuova); **rete tagliata** (tramite il proxy: un nodo isolato, una zona
isolata, taglio in una sola direzione); **orologio che salta** (spostamento da ±0,5 a ±5 s dell'orologio usato dal lease
di uno scrittore); **due scrittori insieme** (uno scrittore congelato con SIGSTOP per più del lease mentre ha scritture
in volo, poi SIGCONT); errore di fsync e disco pieno iniettati nello strato disco (in questa macchina non si può
montare un disco vero che si guasta: limite dichiarato, vedi §5).

Durata: almeno **12 ore di guasti** in tutto, a pezzi di massimo 2 ore (una sessione può essere interrotta: ogni pezzo
salva e pubblica i risultati), più **50 corse brevi** da 15 minuti con semi diversi.

Numero minimo di guasti in tutto: 300 kill -9 di nodo, 300 kill -9 di scrittore, 50 dischi persi con sostituzione,
300 tagli di rete (di cui almeno 75 in una sola direzione e 50 di zona intera), 150 salti d'orologio, 150 episodi di
due scrittori insieme, 50 errori di fsync, 50 dischi pieni. Almeno 2 milioni di scritture confermate.

Passa se, **tutto insieme**:
1. **0 scritture confermate perse** a ogni punto di controllo (ogni 30 minuti) e alla fine.
2. **0 violazioni** sui dischi dei nodi: stesso indice ed epoca ⇒ stessi byte; nessun nodo ha tagliato oltre il
   segnalibro S3; nessun nodo ha confermato un'epoca più vecchia di una che aveva promesso.
3. Episodi a due scrittori: **0** scritture confermate dal vecchio scrittore dopo la "barriera" del nuovo.
4. **Ripresa**: dopo ogni guasto che colpisce uno scrittore, ogni suo database torna a confermare scritture entro
   **lease + 5 s = 15 s** nel 99 % dei casi ed entro **40 s** nel 100 %; dopo un guasto che colpisce solo un nodo, le
   scritture riprendono entro **5 s** nel 100 % dei casi (servono ancora 2 nodi su 3).
5. **0 errori del banco di prova non spiegati**: ognuno ha una causa scritta (la lezione del ciclo 1 di walbench).
6. Nessun processo si ferma da solo, salvo le fermate volute (errore di fsync, disco pieno, recinto).

### b) Le 12 versioni rotte devono essere scoperte anche sul codice vero

Per ognuno dei 12 difetti (`no_fencing`, `ack_on_one`, `ack_gap`, `keep_diverged`, `forget_fence`,
`recovery_longest`, `recovery_single`, `trim_before_upload`, `ack_before_fsync`, `ignore_fsync_error`,
`skip_store_fence`, `wipe_same_id`): binario compilato col difetto, stesso banco di prova di (a), stessi guasti.

Passa se **12 su 12** sono scoperti dal controllore (perdita o violazione sui dischi) **entro 2 ore** di prova
ciascuno. Un difetto non scoperto ferma tutto: si aggiunge un caso di prova mirato, e quel caso conta solo se gira
**anche** sul codice corretto senza segnalare nulla. In più, almeno **9 su 12** devono essere scoperti come **perdita
vera** di una scrittura confermata (non solo come violazione di una regola interna): nella fase 1 erano 11 su 12.

### c) Test dedicato: il nuovo scrittore blocca il vecchio scrittore SlateDB PRIMA di leggere il segnalibro

Il caso più pericoloso: se il nuovo scrittore legge il segnalibro S3 e solo dopo blocca il vecchio, il vecchio può
ancora caricare in S3 scritture non confermate; il nuovo scrittore riusa quegli indici e una scrittura confermata
sparisce.

Il test (motore vero + MinIO, con "punti di pausa" nel codice, come i `failpoint` già presenti in round3):
1. lo scrittore A ha scritture nel suo database in memoria, alcune non ancora confermate;
2. lo scrittore B prende il lease; il test lo ferma tra due passi dell'apertura;
3. il test fa partire l'upload di A in quel momento esatto;
4. B finisce l'apertura e scrive; poi si controlla ogni scrittura confermata.

Due versioni del codice: quella giusta (blocco, poi lettura) e quella rotta apposta (lettura, poi blocco). Ripetuto
**1.000 volte** con piccoli ritardi a caso.

Passa se: codice giusto **0 perdite su 1.000** e l'upload di A **sempre rifiutato** dopo il blocco; codice rotto
**perdita rilevata in almeno 950 su 1.000** (il test deve saper vedere il difetto). In più lo stesso test con l'ordine
giusto ma con A che carica **dopo** la lettura di B: upload rifiutato 1.000 su 1.000.

### d) Ripetizioni con semi diversi: un solo fallimento ferma tutto

- Simulatore della fase 1 (resta in uso, anche sul `core` modificato): 1.000 semi × 200.000 passi e le 12 versioni
  rotte, a ogni modifica del `core`.
- Banco di prova locale: le 50 corse brevi di (a) e i pezzi da 2 ore, ciascuno con un seme diverso, scritto nei
  risultati.
- **Un solo fallimento ferma tutto**: si riproduce dal suo seme, si trova la causa, si corregge, e poi si ripete
  **tutto il gruppo da capo**, non solo il seme che è fallito. Se la causa era un errore del controllore (come 3 volte
  nella fase 1), la correzione si scrive nel documento e il gruppo si ripete comunque da capo.

### e) Revisione da una sessione che non ha scritto il codice

La sessione revisore riceve `docs/journal3z-design.md`, questo piano e il codice, e **non** riceve le spiegazioni di
chi l'ha scritto. Deve controllare, punto per punto, e scrivere un elenco di problemi con gravità (alta / media /
bassa):

1. Ogni regola del protocollo (design §5) ha il suo pezzo di codice; elenco "regola → file:riga".
2. Ordine "prima scrivi su disco con fdatasync, poi rispondi": in **ogni** strada del codice, anche le risposte a
   messaggi duplicati che non scrivono nulla.
3. Il recinto (epoca promessa) non viene **mai** perso: né alla pulizia dei segmenti, né al sonno del database, né al
   riavvio; un nodo con disco perso torna **sempre** con un'identità nuova.
4. Ordine di apertura nel motore: lease → blocco del vecchio scrittore SlateDB → lettura segnalibro → giornale.
5. Il segnalibro è scritto nello **stesso lotto atomico** dei dati; il replay al riavvio è idempotente (riapplicare due
   volte non cambia nulla).
6. Il taglio del giornale usa **solo** valori confermati da S3.
7. Il recupero legge **qualunque** coppia di nodi che risponde, mai una coppia fissa.
8. Errori: fsync fallito ⇒ il nodo si ferma; disco pieno; tempi massimi; nessun errore ignorato in silenzio.
9. I 12 difetti di prova non possono finire nei binari di produzione (controllo della compilazione).
10. Il controllore del banco di prova controlla davvero quello che dice (leggere il codice del controllore, non solo
    i risultati).
11. Concorrenza nel motore: l'indice del giornale viene assegnato nello stesso ordine in cui le scritture sono applicate.
12. Cancellazione GDPR, sonno, spostamento, rilascio del lease quando il giornale è irraggiungibile.

Passa se **0 problemi di gravità alta aperti**; i problemi medi sono corretti o accettati per scritto da te.

### f) Chiusura del controllo formale a due scrittori

Nella fase 1 il controllo esaustivo era completo con **uno** scrittore, ma con **due** scrittori era solo parziale e non
trovava 4 difetti che esistono. Due strade, si fanno entrambe; per passare basta la **prima**, la seconda è una
conferma indipendente.

**Strada 1 (in sessione, nessuna azione tua).** Rendere il controllo a 2 scrittori esaustivo riducendo i casi
equivalenti (consegne a nodi diversi che non si influenzano si provano in un solo ordine, i 3 nodi sono simmetrici).
Passa se: il controllo a 2 scrittori con i limiti della fase 1 (2 epoche, indice 2, messaggi in qualsiasi ordine)
**finisce** senza violazioni, **e** con la stessa riduzione trova **tutti** e 4 i difetti che non trovava
(`no_fencing`, `recovery_longest`, `recovery_single`, `keep_diverged`), più i 3 che già trovava. Un controllo che non
trova difetti noti non vale come prova.

**Strada 2 (TLA+, serve il tuo computer, oppure un permesso).** In questa sessione eseguire lo strumento TLA+ è stato
bloccato dalle regole di sicurezza. Due modi:
- *(preferito)* il coordinatore aggiunge alle impostazioni delle sessioni un permesso che consenta solo
  `java -cp journal3z/tla/tla2tools.jar ...`; poi la sessione lo esegue da sola;
- *(oppure)* lo esegui tu. Prima la sessione prepara nel repository uno script `journal3z/tla/run-tlc.sh` e i file di
  configurazione per il protocollo giusto e per i 4 difetti. Poi tu, una volta:
  1. installi Java 17 o più recente (per esempio "Eclipse Temurin", dal sito adoptium.net) e controlli in un terminale
     che `java -version` risponda;
  2. scarichi il repository sul branch `journal3z` (`git clone`, poi `git checkout journal3z`, o `git pull` se ce l'hai);
  3. nel terminale: `cd journal3z/tla`, poi
     `curl -sSLO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar`;
  4. controlli che il file sia quello giusto: `shasum -a 256 tla2tools.jar` (su Linux `sha256sum`) deve stampare
     `fc6878c603f0fdc18b9fe3e5bf51860cb90ab5b1154e3c371660c036d6bdd63e` (misurato nella fase 1);
  5. lanci `./run-tlc.sh` e lo lasci girare (durata non nota: da minuti a ore; se dopo 4 ore non ha finito lo fermi con
     Ctrl-C);
  6. mandi la cartella `journal3z/tla/out/` che lo script crea (con un commit, o in allegato).
  Passa se: configurazione giusta "No error has been found", e ognuna delle 4 configurazioni con difetto trova un
  controesempio. Attenzione: la specifica TLA+ non è mai stata eseguita; il primo giro può fallire per errori di
  scrittura della specifica stessa: la sessione li corregge e si ripete.

---

## 3. Cosa non so ancora e come lo scoprirò

**Unità di misura.** "Giornata di sessione" = una sessione di lavoro lunga come quella della fase 1 (circa 3 ore di
lavoro attivo, dalle 10:58 alle 13:29 secondo i commit, più prove in background). Non sono date: dipendono da quando
le sessioni vengono avviate.

| pezzo | stima (giornate di sessione) |
|---|---|
| P1 formato su disco | 0,5 – 1 |
| P2 recupero dopo riavvio | 0,5 |
| P3 programma del nodo | 1 – 1,5 |
| P4 rete + proxy di guasti | 0,5 – 1 |
| P5 client scrittore + MinIO | 1 – 2 |
| P6 integrazione col motore (indagine compresa) | 2 – 5 |
| P7 banco di prova a 3 processi + controllore | 1 – 2 |
| P8 le 12 versioni rotte sul codice vero | 0,5 – 1 |
| §2c test dedicato | 0,5 – 1 |
| P9 controllo formale a 2 scrittori | 1 – 2 |
| P10 revisione + correzioni | 1 – 2,5 |
| **totale** | **10 – 20** |

Più il **tempo macchina** delle prove (12 ore di guasti + 50 × 15 minuti + 12 × 2 ore delle versioni rotte + 2 × 1.000
ripetizioni di §2c): circa 2–3 giorni di calendario di macchina accesa, con poco lavoro di sessione.

Ipotesi della stima: round3 (il giornale locale e il lease) resta com'è o viene unito senza grandi cambi; MinIO gira
nella macchina della sessione come nei test di round3; nessun difetto del protocollo scoperto (se succede, vedi §5);
una sessione alla volta; le tue decisioni (§6) arrivano entro una giornata; la macchina della sessione resta da 4 vCPU.

**Cosa non so e come lo scoprirò:**

1. **Durata vera.** La stima più incerta è P6. Il primo passo di P6 è un'**indagine di sola lettura** del motore che
   produce l'elenco dei punti da toccare e una stima rifatta. Se l'indagine stima più di 5 giornate, ci si ferma e si
   rifà il piano con te.
2. **CPU per scrittura su macchine vere.** Il limite di 50 µs della fase 1 è fallito per **tutti** i candidati, anche
   per il percorso di riferimento: misurava i passaggi tra thread del banco di prova, non il protocollo. Prima lo misuro
   in locale con la rete TCP vera tra 3 processi (solo per sapere, non è un criterio); poi lo misuro sul gradino 1 di
   AWS, dove è un criterio (§4).
3. **Peso dell'integrazione col motore** (quanto codice del motore cambia, quanto rallenta le scritture, quanta memoria
   in più): l'indagine di P6 lo stima; la misura vera viene da (a) in locale e dal gradino 1 di AWS.
4. **Latenza vera tra zone AWS** con questo protocollo: si sa solo per il prototipo walbench (p99 1,98 ms); si misura
   al gradino 1.

---

## 4. Gradini AWS (solo dopo tutto §2; ogni gradino lo autorizzi tu, con la cifra, prima di partire)

Regole comuni: tetto di spesa calcolato in diretta ogni 5 minuti (richieste contate × prezzi scaricati al momento, come
walbench), **arresto automatico all'80 % del tetto**, smontaggio di tutto alla fine, verificato **due volte a 10
minuti di distanza** (in walbench la prima verifica vedeva ancora risorse in cancellazione). Prezzi noti dalla fase 1:
c6i.large 0,0912 USD/ora (scaricato il 30-10-2026); i prezzi delle macchine più piccole non sono ancora stati
scaricati: si scaricano all'avvio e se superano le stime il gradino non parte.

| gradino | cosa | tetto | cosa si misura | criterio per passare al successivo |
|---|---|---|---|---|
| **1** | **1–2 ore**, 3 macchine piccole in 3 zone (eu-west-1), stesso banco di prova di §2a con guasti più rari (uno ogni 2–5 minuti), 30 database | **3 USD** | latenza p50/p99 a 1, 8, 64 scritture in parallelo; scritture/s; **CPU per scrittura**; memoria; tempo di presa in carico dopo kill -9; il test "due scrittori" tra zone; costo reale contato | 0 scritture confermate perse; **p99 ≤ 5 ms** a 8 in parallelo, 1 KB; **CPU ≤ 150 µs per scrittura** a 8 in parallelo e **≤ 50 µs** a 64 (contando tutti e 3 i nodi); ripresa entro 15 s nel 99 % dei casi; smontaggio verificato a zero risorse; spesa stimata ≤ tetto |
| **2** | **12 ore**, stessa configurazione, guasti come in §2a (uno ogni 1–10 minuti) compresi taglio di zona via security group, stop/start delle macchine, disco pieno vero | **8 USD** | tutto il gradino 1 per ora; prese in carico; riallineamenti; errori del banco di prova | 0 perse; 0 violazioni sui dischi; 0 errori non spiegati; ripresa come §2a punto 4; latenza e CPU entro il 20 % del gradino 1; smontaggio verificato |
| **3** | **72 ore**, il piano `docs/journal3z-soak-plan.md` | **25 USD** (arresto a 20) | come il piano | criteri del piano (0 perse a ogni controllo orario, 0 errori non spiegati, ripresa entro lease + 5 s) |

Se un gradino fallisce: niente gradino successivo; si torna in locale, si riproduce il problema col banco di prova
(o si aggiunge un caso), si corregge, si ripetono i controlli di §2 toccati, e poi si chiede di nuovo l'autorizzazione
per lo **stesso** gradino.

---

## 5. Rischi che restano e cosa succede se uno si avvera

| rischio | se si avvera |
|---|---|
| Un difetto del **protocollo** trovato dai test locali, dalla revisione o dal controllo formale | ci si ferma; si corregge il design; si rifanno simulatore della fase 1, controllo formale e tutto §2. Costo stimato: +2 – 5 giornate di sessione |
| L'**integrazione col motore** è più grande del previsto (indagine P6 > 5 giornate) o round3 cambia molto | si rifà il piano con te prima di scrivere codice nel motore; possibile prima versione solo per database nuovi, dietro opzione |
| **CPU per scrittura** troppo alta su macchine vere (gradino 1 fallito su questo punto) | lavoro di ottimizzazione (lotti più grandi, meno copie) e ripetizione del gradino 1; oppure decisione tua di accettare il costo in più per scrittura |
| Alcuni guasti **non si riproducono fedelmente in locale** (errore di fsync vero, disco pieno vero, rete tra zone) | in locale si iniettano nello strato disco o nel proxy; il gradino 2 li fa veri dove AWS lo permette; il rischio residuo resta scritto |
| Il controllo formale a 2 scrittori **non si chiude** (§2f strada 1 non finisce e strada 2 non è possibile) | non si va su AWS senza una tua decisione scritta: o accetti il rischio residuo (resta il simulatore), o si investe ancora (stima +1 – 3 giornate) |
| Le **sessioni vengono interrotte** durante le prove lunghe | prove a pezzi da massimo 2 ore, ogni pezzo pubblica i risultati; un pezzo interrotto si ripete, non conta |
| La macchina della sessione **non permette** strumenti di sistema (tagli di rete del kernel, montare dischi) | si usano il proxy in spazio utente e lo strato disco di prova (già previsti); limite dichiarato |
| **Siamo noi** a mantenere un protocollo di consenso | simulatore, 12 versioni rotte e test §2c restano nei test automatici per sempre; ogni modifica al protocollo li rifà tutti |
| **Due dischi persi** nello stesso intervallo di upload | perdita delle scritture confermate dopo l'ultimo upload: è fuori dalla promessa; va scritto nella promessa del prodotto (§6) |
| **S3 non disponibile** | le conferme continuano fino al limite per database; nessun database può cambiare server finché S3 non torna (già nel design) |
| Spesa AWS oltre il tetto o risorse dimenticate | arresto automatico all'80 %; doppia verifica dello smontaggio; un controllo programmato che smonta tutto se il banco di prova smette di dare segni di vita |

---

## 6. Cosa dobbiamo decidere noi (fondatore e coordinatore) prima di partire

1. **Permesso di toccare il codice del motore** (P6) e su quale base: il branch `journal3z` sopra `round3` (PR #1
   ancora in revisione) o aspettare che round3 sia unito. Il resto della fase 2 non tocca il motore.
2. **TLA+**: permesso alle sessioni di eseguire `java` con `tla2tools.jar` (il coordinatore lo aggiunge alle
   impostazioni) oppure lo esegue il fondatore (§2f strada 2), oppure si rinuncia alla strada 2.
3. **Test automatici su GitHub Actions** per simulatore, 12 versioni rotte e test §2c a ogni modifica: sì o no (usano
   minuti di GitHub Actions, non AWS).
4. **Valori del prodotto** da fissare prima dei test: lease 10 s in modalità giornale (proposta), upload ogni 30 s,
   limiti per database 32 MB (upload anticipato) e 128 MB (stop scritture), rilascio del lease dopo 5 s senza giornale.
5. **La frase sulla durabilità** per i clienti: "nessuna scrittura confermata si perde per un guasto singolo (server,
   disco o zona intera); con due dischi persi entro lo stesso intervallo di upload si possono perdere le scritture
   degli ultimi 30 s".
6. **Chi fa la revisione** di §2e (quale sessione, quale modello) e quando.
7. **Tipo di macchine AWS** per i gradini: c6i.large (come walbench, confrontabile) o più piccole (più economiche,
   meno confrontabili).
8. **Chi dichiara superato un controllo**: proposta: la sessione pubblica i risultati grezzi e un riepilogo; il
   coordinatore li controlla sul repository; il fondatore dà il via ai gradini AWS.
