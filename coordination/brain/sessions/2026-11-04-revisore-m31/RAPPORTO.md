**DA CORREGGERE PRIMA.** Spesa attesa circa 0,45–0,55 $ e tetto circa 1,10 $ (STIMATI, prezzi DICHIARATI ricontrollati oggi): la cifra di M31 regge. Prima di spendere però vanno corretti due buchi nello smontaggio, uno nel recupero dei risultati, la definizione di Q4 e Q1 (oggi copiata dal comando `estese`, che è sbagliato) e i numeri di Atlas nel rapporto. Sono correzioni piccole, circa una sessione di lavoro (STIMATO).

# Revisore M31 · i nostri millisecondi a freddo su S3 vero (preparazione)

Revisore esterno (Opus), lanciato dal coordinatore con scheduled task il 2026-11-04. Non ho scritto M31. Codice letto in sola lettura dal branch `freddo-s3` (commit e843564, PR #53). Le mie prove sono sul branch nuovo `rev-m31` di fenvix-engine, cartella `proto/lavaggio-per-cliente/m31-rev/` (commit 1783563). Spesa 0 $: nessuna chiamata AWS, nessun workflow lanciato, nessun `comando.json`, nessun connettore personale, nessun merge, nessun commit su `freddo-s3`.

## In una riga

Il programma misura quello che dice: le GET contate sono vere, il freddo è freddo dentro il nostro processo, il tempo è dentro il processo, e sulla macchina gira lo stesso programma provato in locale. I soldi sono ben protetti nel caso normale, ma c'è un caso raro in cui la macchina resta accesa senza tetto. Il confronto con Atlas nel rapporto usa i numeri sbagliati e propone di misurare Q4 e Q5 con il comando `estese`, che è sbagliato; e la nostra Q4 copia proprio la definizione sbagliata, quindi oggi non sarebbe confrontabile con l'ultimo giro corretto su Atlas.

## 1. Soldi e account

Ho letto riga per riga il workflow `m31-freddo-s3.yml`, `giornata-ci.sh`, `esegui.sh`, `permessi.sh`, `pulizia.sh` e `PERMESSI.md`.

Cosa funziona (letto nel codice):
- Il cuoco a noleggio (la macchina EC2) parte con lo spegnimento che vale cancellazione (`--instance-initiated-shutdown-behavior terminate`), disco cancellato con la macchina (`DeleteOnTermination=true`), metadati solo con gettone (IMDSv2). `shutdown -h` su Ubuntu (systemd) vuol dire spegnere davvero, non fermare: quindi lo spegnimento a 240 minuti cancella la macchina.
- Il programma si costruisce sul runner prima di creare qualsiasi cosa su AWS: se la costruzione fallisce si spende 0 $.
- Solo un `comando.json` aggiunto o cambiato nell'ultimo commit può far partire una prova; qualsiasi altra cosa diventa "smonta".
- La pulizia gira sempre (`if: always()`, con credenziali nuove), cancella le macchine con il tag e i bucket `fenvix-m31-*`, e scrive "RIMASTO: niente" o "ALLARME".
- La serratura dei permessi (policy IAM) è stretta: `RunInstances` solo `c7i.xlarge` e solo con il tag `progetto=fenvix-m31`; tag solo al lancio; cancellazione solo delle macchine con il tag; `PassRole` solo il ruolo della macchina e solo verso EC2; bucket solo `fenvix-m31-*`; "*" solo sulle letture (Describe e ListAllMyBuckets), che non accettano altro. Il ruolo della macchina tocca solo i bucket `fenvix-m31-*`.
- Il numero dell'account: nel branch c'è solo `<ACCOUNT_ID>` (ho cercato numeri da 12 cifre: solo decimali dei risultati); nei log è mascherato; nei risultati è sostituito prima del commit. Il repo fenvix-engine è privato (MISURATO, ricerca GitHub), quindi anche log e artifact lo sono.
- Nulla nel workflow o nel programma contatta Atlas: "Atlas" compare solo nei commenti (punto 6).

ERRORI PROVATI (dal codice, scenario preciso):
- **E1. Lo spegnimento a 240 minuti si programma solo dopo lo scaricamento del programma.** Lo user-data (giornata-ci.sh righe 36–40) scarica il pacco 5 volte e poi lancia `esegui.sh`; è `esegui.sh` (riga 12) a scrivere `shutdown -h +240`. Se lo scaricamento o il `tar` falliscono (link firmato scaduto, rete, file rotto), nessuno spegnimento è programmato: la macchina resta accesa e ferma. Il workflow non se ne accorge (aspetta `fine.json` o una macchina spenta) e la cancella solo a 250 minuti: circa 0,82 $ buttati (STIMATO: 4,17 ore per 0,1915 più disco e IPv4). Se in più il runner muore, nessuno la cancella: circa 4,6 $ al giorno finché qualcuno se ne accorge (STIMATO: 24 ore per 0,1915 più 0,12 di IPv4).
- **E2. "smonta" non può fermare una giornata in corso.** Il workflow ha `concurrency` con `cancel-in-progress: false`: un nuovo `comando.json` con "smonta" aspetta in coda la fine della prova. PERMESSI.md dice "se qualcosa resta acceso: smonta". Durante la prova il freno d'emergenza vero è il bottone "Cancel workflow" in Actions (la pulizia gira anche dopo l'annullamento).

RISCHI (mia opinione, con la prova o la correzione che li chiude):
- **R1. I risultati si possono perdere.** Il rinnovo delle credenziali ogni 45 minuti (funzione `rinnova`) non ha mai girato davvero. Se fallisce, alla fine la copia dei risultati dal bucket fallisce in silenzio e subito dopo la pulizia cancella il bucket con dentro i risultati: giornata pagata e niente in mano. Correzione semplice nell'elenco sotto (C3).
- **R2. Il tetto vale solo per la macchina, non per le richieste a S3.** Nessun contatore ferma il programma se le GET o i PUT esplodono per un errore. Con il codice di oggi non vedo cicli che lo facciano (le GET sono limitate dalla latenza: circa 1.400 al secondo al massimo nelle prove insieme, MISURATO in locale da M31), quindi è un rischio basso. Un freno da 3 righe in `conta.rs` (fermarsi oltre, per esempio, 1 milione di GET o 200.000 PUT) lo chiude.
- **R3. Due giornate per sbaglio.** Se si fa due volte commit di "prova", la seconda parte in coda dopo la prima: circa 0,5 $ in più (STIMATO). Basta dirlo nei passi.
- **R4. Immagine.** `RunInstances` accetta qualsiasi immagine della regione, anche quelle a pagamento del Marketplace. Il workflow usa sempre Ubuntu di Canonical (parametro SSM pubblico), quindi oggi non succede; si può stringere con la condizione `ec2:Owner` sul proprietario Canonical (numero dell'account di Canonical: NON SO a memoria con certezza, da copiare dalla documentazione di Ubuntu su AWS).
- **R5. Account in verifica.** Il limite di vCPU dell'account e la disponibilità di c7i.xlarge nella prima zona non si vedono con il `--dry-run`. Se mancano, `run-instances` fallisce subito e si spende 0 $; ma la giornata va rifatta.
- **R6. Il link firmato contiene il gettone di sessione del runner** (non la chiave segreta) e sta nello user-data della macchina. Non finisce nel branch né negli artifact; scade con la sessione (1 ora). Rischio basso.

La cifra rifatta, voce per voce (prezzi DICHIARATI, ricontrollati oggi):
- Prezzi: c7i.xlarge on-demand eu-west-1 0,1915 $/ora (DICHIARATO, doit.com e aws-pricing.com, pagine lette oggi); disco gp3 0,088 $ per GB al mese in eu-west-1 (DICHIARATO, aws-pricing.com, letto oggi); IPv4 pubblico 0,005 $/ora (DICHIARATO, blog AWS "New AWS public IPv4 address charge"; le 750 ore gratuite del piano gratuito valgono solo nel primo anno, se si applicano qui NON SO); S3 GET 0,0004 $ e PUT 0,005 $ ogni 1.000 (DICHIARATO, NUMBERS.md, non ricontrollati: pesano centesimi); traffico EC2–S3 nella stessa regione gratis (DICHIARATO AWS).
- Durata della macchina: circa 1,5–1,8 ore (STIMATO: avvio 3 minuti, carico da 10 GB circa 30–35 minuti a 6 MB/s, copia 2–3, tempi circa 15–25, carico e tempi da 2 KB circa 10–15, scritture circa 10). Quindi macchina circa 0,29–0,35 $.
- Disco 60 GB per 1,8 ore: circa 0,013 $. IPv4: circa 0,009 $.
- GET: circa 70.000–90.000 (STIMATO dalle GET per find MISURATE in locale: circa 31.000 per le 7 varianti da 10 GB, 3.500 di riscaldo, circa 2.000 insieme, circa 18.500 per Q5, circa 10.000 per i 2 KB): circa 0,03–0,04 $.
- PUT (e LIST, stesso prezzo): circa 8.000–20.000 (STIMATO: copia circa 600, risultati ogni minuto circa 1.500, scritture il resto; con il WAL ogni 10 ms i PUT vanno uno alla volta, quindi probabilmente meno dei 12.000 di M31): circa 0,04–0,10 $.
- Spazio S3: meno di 0,01 $.
- **Totale atteso circa 0,45–0,55 $ (STIMATO), come M31.** Tetto con lo smontaggio che funziona: macchina a 250 minuti 0,80 $, disco 0,03, IPv4 0,02, S3 fino a circa 0,25: circa 1,10 $ (STIMATO); il tetto 1,50 $ di M31 è prudente e regge. Fuori tetto solo i casi E1 con il runner morto e R2.
- GitHub Actions: repo privato, fino a circa 4,5 ore di runner per giornata dai minuti gratuiti del mese; quanti ne restano NON SO.

## 2. Il confronto con Atlas (sezione 4 del rapporto M31)

ERRORE PROVATO: il rapporto usa RISULTATI.md, non RISULTATI-CORRETTI.md. Rifatto, "freddo meno caldo" a p50, nostro = variante del disegno, clienti dormienti, S3 simulato (MISURATO da M31 in locale, STIMATO per S3 vero):
- **Q2, un documento per `_id`**: noi circa 59 ms; Atlas 2,15–2,92 ms freddo meno caldo, 1,75–2,01 ms freddo meno rete (MISURATO). Atlas circa **20–34 volte** più veloce. Non cambia.
- **Q1, ultimi 20 messaggi**: noi circa 32 ms; Atlas circa 8–13 ms (STIMATO, non 2,2–3,8 che era caldo). Atlas circa **2,5–4 volte** più veloce, non 8–14.
- **Q3, circa 85 eventi**: noi circa 26 ms; Atlas circa 45–65 ms (STIMATO dal revisore B; A2 stima 30–45). Noi circa **1,7–2,5 volte** più veloci con la stima di B, circa 1,1–1,7 con quella di A2. Ma attenzione al gruppo: l'ultimo giro su Atlas (correzione C di B) misura Q3 su clienti **attivi** mai letti; i nostri attivi fanno circa 43 ms (MISURATO in locale), cioè circa pari con Atlas (1,0–1,5 volte), non 1,7–2,5.
- **Byte scritti per byte inserito**: il rapporto dice "Atlas circa 10, circa 3 con 1,9 KB". Corretto: circa 4 a lotti con documenti da 340 byte e circa 3,2 a scritture singole da 1,9 KB (STIMATO dai grafici). Il nostro 5,0 durante il carico (MISURATO in locale, documenti da 310 byte) va confrontato con il 4: noi scriviamo circa 1,25 volte di più, non la metà. In più i denominatori sono diversi (noi chiave più valore, Atlas byte BSON inseriti) e da noi i byte vanno su S3 (si pagano a PUT, non a disco): il confronto vero lo dà la giornata con i documenti da 1,9 KB contro il 3,2.

Cosa cambia nelle conclusioni: la frase "Atlas vince sul singolo, noi su tanti documenti dello stesso cliente" regge, ma il vantaggio di Atlas sulle letture corte è molto più piccolo (circa 3 volte su Q1, non 10), il nostro vantaggio su Q3 vale per i clienti dormienti e si riduce a circa pari sugli attivi, e sui byte scritti non abbiamo un vantaggio. Per la decisione "sempre due numeri" non cambia niente. Ricordo anche che i numeri di Atlas sono visti dal Mac (rete sottratta), i nostri dentro il processo: il confronto giusto è con il tempo lato server del profiler (punto A del revisore B), che arriverà con l'ultimo giro.

## 3. Q4 e Q5 su Atlas: il comando `estese`

Confermo: **non va usato.** Errori provati da A2 (sezione 3) e confermati da B (sezione 5): gli "ultimi 500 messaggi" stanno tutti nello stesso pezzo della collezione, quindi dopo pochi clienti il "freddo" è caldo, e il 96% dei clienti è attivo, non dormiente; la prova insieme apre le connessioni dentro il tempo freddo e scalda il bancone tra un livello e l'altro; nessun tempo lato server; consuma i dormienti rimasti. Il coordinatore lo ha già scritto in NOW.md.

ERRORE PROVATO in M31: la nostra Q4 copia proprio quella definizione ("gli ultimi 500 messaggi, come il comando `estese`", main.rs funzione `find_m31`, ramo Q4). L'ultimo giro corretto su Atlas (p1-1.5) farà invece Q4 con una finestra a caso di 60 giorni e al massimo 500 documenti, su 60 clienti (A2 correzione 6, B punto G). Così com'è, la nostra Q4 misurata su AWS non si potrebbe confrontare.

Cosa serve perché le nostre Q4 e Q5 (e Q1, Q2, Q3) siano confrontabili con p1-1.5:
- **Q4**: stessa definizione di Atlas: finestra a caso di 60 giorni per cliente, `limit 500`, 60 clienti (o più, ma almeno quei 60). La verità del carico deve dare per ogni cliente una finestra di 60 giorni e quanti messaggi ci sono dentro.
- **Q1**: A2 (correzione 5) chiede su Atlas "i 20 messaggi più recenti prima di una data a caso" e scrive "lo stesso per M31". Da noi gli ultimi 20 non hanno il problema delle pagine condivise (il bancone dei dati si svuota prima di ogni find), ma la ricerca deve essere la stessa: aggiungere Q1 con la data a caso, e tenere anche la Q1 di oggi.
- **Q2 e Q3 sullo stesso gruppo di clienti**: p1-1.5 fa Q2 (300) e Q3 (150) su clienti **attivi** mai letti (B passi 4 e 5). Il confronto va fatto con i nostri attivi, che il programma già misura: nel rapporto della giornata scrivere i due gruppi e confrontare attivi con attivi.
- **Q5**: 60 finestre, stesso stato e "i 100 più nuovi" (B punto G): da noi oggi `--q5-n 20`, portarlo a 60 (costa circa 4.700 GET in più, meno di 0,01 $, STIMATO).
- **Il lato Atlas** si confronta con il tempo lato server (profiler: `storage.data.timeReadingMicros`, B punto A) o con il freddo meno caldo accoppiato; mai con il tempo lato client grezzo.

## 4. Le misure, rifatte in locale

Ho costruito il programma di `freddo-s3` così com'è (stesso `Cargo.lock`), fatto un carico piccolo (0,6 GB, 300 clienti, 30% dormienti, stessi parametri di `esegui.sh` tranne taglia e ritmo; 94 secondi, MISURATO), copiato tutto su moto (S3 finto in locale) e lanciato `tempi --magazzino s3 --finto 25:0.6:90` con le varianti `parallelo` e `parallelo+bancone+doppia+ra256`, 30 clienti per gruppo, Q1–Q5.

- **Le GET contate sono vere.** Il contatore del programma dice 2.445 richieste di classe GET (MISURATO); moto ne ha ricevute 2.421 (2.405 GET e 16 HEAD, MISURATO dal suo registro). La differenza (circa 1%) torna con le 32 seconde richieste della richiesta doppia, che a volte vengono annullate prima di partire, meno le poche letture del manifest che il programma non conta.
- **Il freddo è freddo.** Prima di ogni find il bancone dei dati (cache dei blocchi) si svuota; restano solo indici e filtri, come da disegno. Ogni find fredda ha fatto almeno una GET, la find calda subito dopo 0 GET (MISURATO: "get_a_caldo_totali" 0); 0 indici o filtri mancati durante le misure (MISURATO, anche nei risultati di M31). Nessun cliente ripetuto tra le varianti nella mia prova e nel carico da 10 GB di M31 (MISURATO, "clienti_in_piu_varianti" 0). Due eccezioni piccole: nel carico da 2 KB i dormienti sono 219 e le 4 varianti da 150 ne chiedono 600, quindi 179 clienti sono ripetuti (MISURATO dal file di M31); e la prova "insieme" usa clienti già letti nelle varianti (dal codice). Dentro il nostro processo non cambia nulla (il bancone è svuotato); se S3 risponde più in fretta a un oggetto letto pochi minuti prima NON SO: meglio dirlo o usare clienti mai letti.
- **Il tempo è dentro il processo**: `Instant` intorno alla sola find, niente rete del cliente (letto nel codice).
- **I numeri**: con moto i tempi sono più alti di quelli di M31 su disco, perché moto è un server Python lento (circa 15–20 ms in più per GET, STIMATO): dormienti, variante del disegno, Q1 freddo p50 46 ms, Q3 42 ms, Q4 50 ms con 1 GET, Q2 90 ms con 2 GET; attivi Q1 56 ms con 3 GET; caldo 0,3–1,5 ms (MISURATO con moto e ritardo finto). Le GET per find tornano con M31 (1 per i dormienti, 3 per gli attivi con il bancone, 2 per Q2). Con un ritardo fisso di 20 ms su disco locale: dormienti Q1 22,2 ms, attivi Q1 44,4 ms, Q2 43,7 ms (MISURATO), cioè 1 giro, 2 giri, 2 giri in fila: torna con M31.
- **È lo stesso programma che girerà su AWS?** Sì: il workflow costruisce dallo stesso commit con `--locked` sullo stesso Ubuntu 24.04 (glibc 2.39) che girerà sulla macchina. Le uniche differenze sono le credenziali (ruolo della macchina invece di chiavi finte; `object_store` usa IMDSv2, che il lancio richiede) e i parametri di taglia, numero di clienti e ritmo.

## 5. Il secondo giro di GET dei clienti attivi

Spiegato e provato. SlateDB 0.17 apre le scatole (SST) di una ricerca in due gruppi insieme, le L0 e le sorted run, ma in ogni gruppo **al massimo 4 alla volta**: `let max_parallel = total_ssts.clamp(1, 4)` in `reader.rs` riga 305, usato da `build_concurrent` in `segment_iterator.rs` righe 390 e 426. Un cliente attivo con 5 L0 e 3–6 sorted run ha più di 4 scatole in un gruppo: secondo giro. Prova: ho portato il 4 a 64 (patch di una riga, `m31-rev/tetto-4.patch`) e con un ritardo fisso di 20 ms i clienti attivi con 8 GET passano da Q1 44,4 ms a 22,9 ms, Q3 da 44,0 a 22,9, Q4 da 45,5 a 24,3, GET in volo al massimo da 7 a 8 (MISURATO). I dormienti non cambiano.

Cambia i numeri? Per la variante del disegno quasi no: con le pile recenti sul bancone gli attivi fanno circa 3 GET al magazzino, sotto il 4, quindi un giro solo. Cambia invece i numeri della variante "parallelo" sugli attivi (circa un giro in più, circa 25 ms a p50 con S3 simulato, STIMATO). E conta per i database grandi: un database da 1 TB con T = 4 ha più livelli e più di 4 sorted run da toccare per un cliente attivo, quindi con il 4 farebbe due o tre giri anche con il bancone. Nel motore va reso regolabile (o alzato); per PLAN-LAVAGGIO pezzo 2 è una riga da aggiungere. Non serve per la giornata.

## 6. Nomi neutri

La prova non tocca servizi di concorrenti: nessun indirizzo, driver o comando verso Atlas nel workflow, negli script o nel programma (ricerca nel codice: "Atlas" solo nei commenti). I nomi `fenvix-m31-*` sono su AWS nostro, quindi la regola dei nomi neutri non si applica.

## Verdetto

**DA CORREGGERE PRIMA.** Nessun errore che renda la giornata pericolosa nel caso normale, e la cifra regge. Ma ci sono errori provati che si correggono in poco e che, se non corretti, rendono la giornata inutile per il confronto (Q4, Q1) o la espongono a una spesa senza tetto in un caso raro (E1).

Probabilità (STIMATE, mia opinione):
- che la giornata spenda oltre 1,50 $: circa 2–3% oggi, sotto l'1% dopo C1 e C2;
- che la giornata dia risultati inutili o da rifare: circa 30–40% oggi (prima giornata mai girata su AWS vero, quota di vCPU, rinnovo delle credenziali mai provato, Q4 non confrontabile), circa 15–25% dopo le correzioni. Il modo "permessi" a 0 $ toglie una parte dei rischi prima di spendere.

Elenco esatto delle correzioni, in ordine:
- **C1 (soldi)** In `giornata-ci.sh`, nello user-data, come prima riga dopo `#!/bin/bash`: `shutdown -h +240 "M31: tetto" || true`; e se lo scaricamento o il `tar` falliscono: `shutdown -h now`. In `esegui.sh` lo spegnimento a 240 può restare (lo sostituisce, partendo un minuto dopo).
- **C2 (soldi)** In `giornata-ci.sh`, nel ciclo d'attesa: se dopo 15 minuti `risultati/passi.log` non esiste ancora nel bucket, scrivere "MACCHINA MUTA" e uscire dal ciclo (poi la pulizia cancella la macchina), invece di aspettare 250 minuti.
- **C3 (risultati)** Nel workflow, un passo nuovo `if: always()` dopo "Credenziali per la pulizia" e prima della "Pulizia": copia `s3://$BUCKET/risultati/` in `/tmp/m31-out` con le credenziali fresche, e toglie il numero dell'account (la stessa `sed`). Così un rinnovo fallito non fa perdere i risultati.
- **C4 (procedura)** In PERMESSI.md, passo 3: "durante la giornata il freno è il bottone Cancel workflow nella pagina della run (la pulizia gira lo stesso); smonta serve solo dopo"; e "non fare due volte commit di prova".
- **C5 (confronto)** Q4 come p1-1.5: finestra a caso di 60 giorni, `limit 500`, almeno 60 clienti per gruppo. Q1: aggiungere la variante "20 più recenti prima di una data a caso". `--q5-n 60`. Provare in locale con moto come M31 ha già fatto.
- **C6 (rapporto M31)** Sezioni 3 e 4: numeri di Atlas da RISULTATI-CORRETTI (Q1 circa 8–13, Q3 circa 45–65 STIMATI, byte scritti circa 4 e 3,2); confronto Q2 e Q3 anche sui clienti attivi; togliere la proposta di misurare Q4 e Q5 con `estese` e rimandare a p1-1.5.
- **C7 (piccole, consigliate)** Nel carico da 2 KB `--n 50` per non ripetere i dormienti, o scriverlo nel rapporto; nella prova insieme dire che i clienti sono già stati letti; freno sulle richieste in `conta.rs` (R2); condizione sul proprietario dell'immagine (R4).

Cosa mi farebbe cambiare idea verso "PRONTA": C1, C2, C3 e C5 fatti e provati in locale con moto, come M31 ha già fatto per il resto. Le altre si possono fare dopo.

## Cosa ho deciso io

- Ho provato con un carico piccolo (0,6 GB) e non con i 10 GB: le domande erano "il freddo è freddo, le GET sono vere, il programma è lo stesso", e il carico piccolo basta; i numeri da 10 GB di M31 li ho riletti dai suoi file.
- Ho usato moto come M31 (MinIO non si scarica da qui). Il ritardo finto e moto insieme danno tempi più alti di S3 vero, quindi i miei millisecondi servono solo a controllare il programma, non come previsione.
- La patch del tetto di 4 è solo una prova sul mio branch: non tocca il motore né `freddo-s3`.

## Link

- Le mie prove: fenvix-engine, branch `rev-m31`, cartella `proto/lavaggio-per-cliente/m31-rev/` (LEGGIMI.md, `tetto-4.patch`, `risultati/`).
- Codice rivisto: branch `freddo-s3` commit e843564, PR #53; `.github/workflows/m31-freddo-s3.yml`, `proto/lavaggio-per-cliente/m31/giornata/`, `src/main.rs` (`tempi`, `find_m31`, `scrivi`, `copia`), `src/conta.rs`, `src/bancone.rs`.
- SlateDB 0.17.0: `src/reader.rs` riga 305, `src/segment_iterator.rs` righe 63–66, 390, 426.
- Prezzi letti oggi: https://www.doit.com/compute/spot/eu-west-1/c7i.xlarge, https://aws-pricing.com/eu-west-1.html, https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights

## Errori o mancanze trovati in fenvix-brain

- Il rapporto di M31 (sezioni 3 e 4) usa ancora i numeri di RISULTATI.md: va corretto come in C6 (lo dice anche RISULTATI-CORRETTI, ultima riga).
- PLAN-LAVAGGIO, pezzo 2: aggiungere che SlateDB 0.17 apre al massimo 4 scatole insieme per gruppo (L0 e sorted run): due o più giri per i clienti con più di 4 scatole da leggere, da rendere regolabile nel motore prima dei database grandi.
- NOW.md dice che il prompt di M31 deve "valutare CloudShell": è superato, la giornata è già scritta per GitHub Actions.
