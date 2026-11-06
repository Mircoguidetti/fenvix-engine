**PRONTA per la giornata su AWS, da GitHub Actions (non da CloudShell), dopo tre passi IAM del fondatore (`giornata/PERMESSI.md`). Spesa attesa circa 0,50 $, tetto 1,50 $ (STIMATO su prezzi DICHIARATI). Nessuna risorsa AWS creata, 0 $ spesi.**

# M31 · i nostri millisecondi a freddo su S3 vero (preparazione)

Sessione M31 (Opus), lanciata da scheduled task il 2026-11-04, con l'aggiunta del fondatore delle 15:15 (punti A–F) e il cambio successivo del fondatore (niente CloudShell: giornata da GitHub Actions). Branch `freddo-s3` di `fenvix-engine` (da `clienti-grandi`), cartella `proto/lavaggio-per-cliente/m31/`, più il workflow `.github/workflows/m31-freddo-s3.yml`. PR in bozza: https://github.com/Mircoguidetti/fenvix-engine/pull/53. Motore non toccato, nessun merge, nessun connettore personale, nessun contatto.

## In una riga

Il programma è pronto e provato in locale, anche contro un finto S3 vero e proprio (server con le stesse API di S3). Con un magazzino lento come S3 (ritardo finto) le nostre prime letture di un cliente dormiente costano circa 27–35 ms in più del caldo con 1 GET (Q1, Q3, Q4) e circa 58–65 ms con 2 GET in fila (Q2). Atlas fa meglio di noi di circa 8–28 volte sul documento singolo e sugli ultimi 20 messaggi, noi facciamo meglio di Atlas su circa 85 eventi sparsi (circa 1,3–1,7 volte). Tutto questo è STIMATO: il ritardo di S3 è un modello, non una misura. La giornata serve proprio a sostituirlo con la misura.

## 1. Cosa ho fatto, un passo alla volta

1. **Il magazzino si sceglie** (S3 o disco locale): `--magazzino locale` (com'era in M29) oppure `--magazzino s3`, cioè `object_store::aws::AmazonS3Builder` con le credenziali dall'ambiente, regione eu-west-1, bucket e cartella dati da riga di comando. Nessuna chiave nel codice o nei file. Con `AWS_ENDPOINT` lo stesso codice parla con un S3 finto in locale.
2. **Lo stesso carico di Atlas P1 (punto A)**: 10 GB di documenti misti come in `prova_infinite.py` (50% messaggi 150–450 byte, 35% eventi 120–280, 15% ordini 400–800, circa 310 byte medi, testo zipf), 5.120 clienti da circa 2 MB, il 30% dormienti che scrivono solo nel primo 60% del carico, circa un anno di tempo simulato. In locale: 29,7 milioni di documenti (P1: 31,6 milioni), 47 minuti (MISURATO). Più un carico da 2 GB con documenti da 2 KB (1.024 clienti da 2 MB). Disposizione di M29: pile a gradini (sorted run a livelli, T = 4), blocchi 64 KB, lz4, cliente in testa alla chiave, filtri per prefisso del cliente, lettore regolato.
3. **`copia`** porta la fotografia del database su S3 (stessi file, stessi percorsi).
4. **`tempi`** misura dentro il processo (niente rete del cliente), come P1: freddo = cliente mai letto prima, con sul bancone (cache) solo gli indici dei blocchi e i filtri; caldo = la stessa ricerca subito dopo, con i blocchi del cliente sul bancone. Per ogni ricerca: p50, p95, p99, quota sotto 5 ms e sopra 20 ms, "freddo meno caldo", GET per ricerca, byte per GET, GET in più della richiesta doppia. Due gruppi: dormienti (come P1) e attivi (il caso peggiore: il cliente è in tutte le pile).
5. **Le ricerche**: Q1 ultimi 20 messaggi; Q2 un documento per `_id` (biglietto `_id` → chiave, poi il documento); Q3 eventi di un cliente in una finestra con esattamente 85 eventi; **Q4** (punto B) gli ultimi 500 messaggi del cliente, come il comando `estese` di `prova_infinite.py`; **Q5** (punto C) gli ordini di tutti i clienti con uno stato negli ultimi 7 giorni, i 100 più nuovi, come in `estese`.
6. **Le varianti** (punto 4 del prompt): serie (una GET alla volta), parallelo (SlateDB 0.17 com'è), con le pile recenti sul bancone (servite da una copia sul disco locale), con la richiesta doppia (seconda GET uguale se la prima supera il p95 delle ultime 1.000), con la lettura in anticipo da 256 KB per pila.
7. **Letture fredde insieme** (punto E): 10, 50 e 100 clienti freddi diversi che partono nello stesso istante, 3 giri per ricerca.
8. **Scritture** (punto D): `scrivi`, documenti da 1.922 byte, 32 fili, singole e a lotti da 100, ognuna aspetta di essere durevole su S3.
9. **La giornata su AWS da GitHub Actions** (sezione 6).

## 2. Due fatti trovati per strada

- **SlateDB 0.17 legge già le pile in parallelo.** Il prompt dava per possibile la lettura una dopo l'altra. Con un ritardo fisso di 20 ms per GET (MISURATO, 0,25 GB): Q1 di un cliente attivo con 7 GET dura 44 ms (2 giri da 20 ms) con SlateDB com'è e 176 ms con le GET forzate una alla volta; un cliente dormiente 22 ms (1 giro). Ho provato una modifica alla libreria per aprire tutte le pile insieme: nessun cambio, quindi l'ho tolta. Resta un secondo giro per i clienti attivi che non ho spiegato (probabilmente la pila più nuova aperta da sola prima delle altre): da guardare nel pezzo 2.
- **I clienti dormienti costano una GET sola.** I loro dati stanno tutti nella pila più vecchia, perché il lavaggio (compattazione) li ha già portati giù; i filtri per prefisso fanno saltare tutte le altre pile. A 10 GB con 11 pile: Q1 dormiente p50 1 GET, Q1 attivo p50 10 GET (MISURATO). È la cucina che lavora per noi: chi non scrive da tempo sta in un ripiano solo.

## 3. I numeri in locale con S3 simulato (la mia previsione per S3 vero)

Come ho simulato S3: ogni GET aspetta un tempo a caso con mediana 25 ms e p99 circa 100 ms (lognormale, sigma 0,6), più i byte divisi per 90 MB/s. È il modello già usato dai revisori di M15, M24 e M25. Dataset da 10 GB su disco locale, 11 pile (5 L0 e 6 sorted run: su 2 vCPU il lavaggio resta un po' indietro durante il carico). 150 clienti per gruppo e per variante, nessun cliente ripetuto tra le varianti nel carico da 10 GB (2.100 clienti misurati). Tutti i numeri di questa sezione sono MISURATI con S3 simulato, quindi per S3 vero sono STIMATI.

**La variante che userebbe il disegno** (parallelo, pile recenti sul bancone, richiesta doppia, lettura in anticipo da 256 KB), clienti dormienti:
- Q1 ultimi 20: freddo p50 32,0 ms, p95 64,1, p99 82,3; caldo p50 0,36; 1 GET (p99 2); 0% sotto 5 ms, 81% sopra 20 ms.
- Q2 un documento: freddo p50 59,9, p95 104,4, p99 124,0; caldo 0,46; 2 GET in fila (biglietto, poi documento).
- Q3 85 eventi: freddo p50 26,9, p95 68,0, p99 88,6; caldo 0,47; 1 GET.
- Q4 500 messaggi: freddo p50 30,8, p95 73,2, p99 87,4; caldo 1,46; 1 GET (con 64 KB di lettura in anticipo servono 2 GET in fila e il p50 sale a circa 46–57 ms).
- Clienti attivi, stessa variante: Q1 p50 43,7 / p99 96,4 con 3 GET; Q3 42,9 / 106,3; Q4 45,8 / 113,7. Senza il bancone gli attivi fanno 10 GET e p50 circa 73–76 ms, p99 circa 140–190.

**Quanto aiuta ogni pezzo** (attivi, Q1, freddo p50 / p99): una GET alla volta 322 / 511 ms; SlateDB com'è 75 / 208; più bancone 45 / 122; più richiesta doppia 43 / 100. La richiesta doppia costa il 4,4–6,8% di GET in più e taglia soprattutto il p99 (da 208 a 119 ms sugli attivi senza bancone).

**Documenti da 2 KB** (2 GB, 5 pile, dormienti, parallelo + bancone + doppia, lettura in anticipo 256 KB): Q1 p50 32,1 / p99 90,4; Q2 62,7 / 121,6; Q3 28,6 / 81,9; Q4 65,6 / 133,1 con 2 GET (500 messaggi da 2 KB sono 1 MB: servirebbe una lettura in anticipo da 1 MB per stare a una GET). Il campione di Q4 dormienti qui è piccolo: pochi clienti hanno 500 messaggi.

**100 clienti freddi insieme** (dormienti, SlateDB com'è): Q1 p50 30,7 / p99 128,7; Q2 59,4 / 151,5; Q3 32,0 / 131,9; fino a 1.433 GET al secondo e 114 GET in volo. Con 10 clienti Q1 29,9 / 134,2: in locale nessun peggioramento con il numero. Su S3 vero il limite DICHIARATO è 5.500 GET al secondo per prefisso (NUMBERS.md): 100 clienti insieme sono circa un quarto. Che S3 tenga la stessa coda con 100 in volo: NON SO, lo misura la giornata.

**Q5, "di tutti"** (punto C). Il prototipo ha un solo indice di tutti i clienti: le voci `stato | data | _id` degli ordini, scritte da M26, come l'indice `stato_ts` di P1. Non ha un indice che contenga i documenti. Freddo, 20 finestre da 7 giorni:
- i 100 ordini più nuovi con l'indice: p50 487 ms, p99 591; caldo 18 ms; 118 GET e 3,1 MB per ricerca. Il costo vero sono i 100 documenti sparsi tra i clienti: per ognuno biglietto più documento, 32 alla volta;
- tutti gli ordini della finestra (circa 37.900): p50 47 s, 5.387 GET, 141 MB;
- senza indice (scansione di tutto): 72 s, 254 GET da 16 MB, 3,8 GB letti.
Come lo farebbe il motore: un indice secondario che porta con sé i campi della risposta (indice coprente) o i documenti degli ultimi giorni tenuti sul bancone. Con l'indice coprente i 100 ordini sono una scansione di circa 10–20 KB per pila: circa 1–3 GET, circa 30–60 ms freddo (STIMATO dal costo per pila misurato sopra). "Quanto siamo scarsi" oggi: circa 490 ms contro i 30–60 di un buon indice, cioè circa 8–16 volte peggio del necessario (STIMATO). Contro Atlas: NON SO (sezione 4).

**Scritture** (punto D). Il prototipo non ha il quaderno (registro replicato a 3 zone): qui la scrittura è durevole quando SlateDB ha messo il suo WAL su S3. In locale ho solo controllato che il programma funzioni (5 secondi per caso, S3 finto senza ritardo sui PUT): singole con WAL ogni 100 ms, 316 documenti al secondo e p50 100,6 ms (il tempo è quasi tutto l'attesa del giro del WAL); WAL ogni 10 ms, 2.706 al secondo e p50 11,1 ms; lotti da 100, 5.545 documenti al secondo (MISURATO, ma senza la latenza vera dei PUT: non va usato per confronti). Byte scritti per byte inserito durante il carico grande da 10 GB, con documenti da 310 byte: circa 5,0 sul totale dei byte della chiave e del valore (59,2 GB scritti nelle scatole per 11,8 GB), circa 2.390 PUT per GB (MISURATO in locale, i PUT sono quelli che andrebbero su S3; WAL spento come in M29). Atlas durante il carico di P1 scriveva circa 10 volte i byte in ingresso, circa 3 con documenti da 1,9 KB (STIMATO dai grafici, RISULTATI.md). La latenza fino a S3 e i byte per i documenti da 1,9 KB li misura la giornata. Il mio STIMATO per S3 vero: WAL ogni 100 ms circa 120–160 ms a p50, ogni 10 ms circa 30–70 ms (mediana dei PUT di S3 circa 20–60 ms, non misurata da noi). Nel disegno la scrittura va invece nel quaderno: p99 1,79–3,40 ms su 3 zone (MISURATO walbench, NUMBERS.md).

## 4. Noi contro Atlas, ricerca per ricerca (punto F)

Il confronto è "freddo meno caldo" a p50 sui clienti dormienti, come la revisione indipendente di P1 chiede. Nostro: variante del disegno in locale con S3 simulato (STIMATO per S3 vero). Atlas: RISULTATI.md, giri 0h e 2h (MISURATO).
- **Q2, un documento per `_id`**: noi circa 59 ms, Atlas 2,15–2,92 ms: Atlas circa **20–28 volte** più veloce. Due GET in fila contro una pagina da un disco veloce in zona.
- **Q1, ultimi 20 messaggi**: noi circa 32 ms, Atlas 2,2–3,8 ms: Atlas circa **8–14 volte** più veloce.
- **Q3, circa 85 eventi**: noi circa 26 ms, Atlas 33,8–46,7 ms: noi circa **1,3–1,7 volte** più veloci. Qui vince il lavaggio per cliente: una GET contro circa 85 pagine lette una dopo l'altra.
- **Q4, 500 messaggi**: noi circa 29 ms (1 GET, lettura in anticipo 256 KB). Atlas: NON SO. Come misurarlo: il comando `estese` di `prova_infinite.py` lo fa già (ultimi 500 messaggi di 200 clienti dormienti, freddo e caldo); basta lanciarlo sul cluster P1 dopo un passaggio M30 → M10 → M30 che svuota la memoria, come per il giro 2h. Mi aspetto che Atlas cresca con i documenti (circa 0,4–0,55 ms per documento sparso come in Q3, STIMATO dal revisore di P1): forse 100–250 ms, ma è una stima, non un dato.
- **Q5, 100 ordini di tutti in 7 giorni**: noi circa 490 ms con il prototipo, circa 30–60 ms STIMATI con un indice coprente. Atlas: NON SO, con lo stesso comando `estese` (150 finestre). Qui Atlas parte avvantaggiato: il suo indice `stato_ts` è ordinato proprio per questa domanda.
- **Quota sotto 5 ms e sopra 20 ms**: noi a freddo 0% sotto 5 ms e 73–100% sopra 20 ms (S3 simulato); a caldo 100% sotto 5 ms (p99 0,5–5,7 ms). Atlas: NON SO, in RISULTATI.md non c'è; il riassunto di `prova_infinite.py` lo può calcolare dai tempi salvati.

Detto in cucina: Atlas tiene ogni pagina su una dispensa veloce accanto al cuoco (Page Service su NVMe in ogni zona), noi andiamo al magazzino (S3), che risponde in circa 25 ms. Quando serve una cosa sola, Atlas vince sempre. Quando servono tante cose dello stesso cliente, noi le prendiamo in un viaggio solo (lavaggio per cliente) e Atlas fa tanti viaggi brevi: lì pareggiamo o vinciamo. È coerente con la decisione "sempre due numeri" (DECISIONS 2026-11-04).

## 5. La previsione per S3 vero (punto 7, STIMATO)

Le GET di S3 vero dalla stessa regione con 30–150 KB: NON SO per misura nostra (sezione 8). Uso il modello: mediana circa 20–30 ms, p99 circa 80–130 ms (STIMATO, M15/M25). Allora, clienti dormienti, variante del disegno: Q1 e Q3 p50 circa 25–40 ms, p99 circa 80–130; Q2 p50 circa 45–70, p99 circa 120–200; Q4 come Q1; 100 clienti insieme come uno solo se S3 tiene la coda. Clienti attivi: circa +10–15 ms a p50. Tutto dentro l'obiettivo freddo (p50 30–60 ms, p99 sotto 150 ms) tranne il p99 di Q2, che sta al bordo. Se la giornata misura GET da 10–15 ms, tutti i numeri scendono di circa la metà; se misura 40–50 ms, Q2 esce dall'obiettivo.

## 6. La giornata su AWS (da GitHub Actions, come walbench e S13)

Chi fa cosa, un passo alla volta:
1. **Il fondatore, nella console IAM** (una volta sola, testo pronto in `proto/lavaggio-per-cliente/m31/giornata/PERMESSI.md`):
   - aggiunge alla serratura (trust policy) del ruolo GitHub il branch `repo:Mircoguidetti/fenvix-engine:ref:refs/heads/freddo-s3`;
   - crea il ruolo della macchina `fenvix-m31-macchina` (servizio EC2) con la policy inline `solo-bucket-m31`: legge e scrive solo nei bucket `fenvix-m31-*`;
   - aggiunge al ruolo GitHub la policy inline `fenvix-m31`: letture (DescribeVpcs, DescribeSubnets, DescribeInstances, ListAllMyBuckets); `ssm:GetParameter` solo sull'immagine Ubuntu pubblica; `ec2:RunInstances` solo `c7i.xlarge` e solo con il tag `progetto=fenvix-m31`; `ec2:CreateTags` solo al lancio; `ec2:TerminateInstances` solo con il tag; `iam:PassRole` solo per `fenvix-m31-macchina` verso EC2; `iam:GetInstanceProfile` su quel profilo; sui bucket `fenvix-m31-*` crea, cancella, tag, scadenza, blocco pubblico, lista, GET, PUT, DELETE, annulla multiparte;
   - attenzione: se c'è ancora il `Deny` di walbench "tutto tranne c6i.large", va tolto (lo diceva già S13).
2. **Il fondatore o il coordinatore, dopo il sì sulla cifra**: crea dal sito di GitHub il file `proto/lavaggio-per-cliente/m31/giornata/comando.json` sul branch `freddo-s3` con `{"modo": "permessi"}` (link in PERMESSI.md). Costo 0 $: il workflow controlla ogni permesso (dry-run, bucket vuoto creato e cancellato) e scrive "PERMESSI: tutti presenti" o cosa manca.
3. Poi lo stesso file con `{"modo": "prova"}`. Il workflow, da solo: costruisce il programma sul runner; crea il bucket `fenvix-m31-<run>` (privato, oggetti cancellati da soli dopo 1 giorno); ci mette il programma; lancia una `c7i.xlarge` (4 vCPU, Ubuntu 24.04, disco 60 GB gp3) nella prima zona della VPC di serie di eu-west-1, con il ruolo della macchina. La macchina scarica il programma, fa il carico da 10 GB sul suo disco (circa 35 minuti), lo copia su S3, misura i tempi su S3, fa il carico da 2 KB, le scritture da 1,9 KB, e mette i risultati nel bucket ogni minuto. Il workflow scrive ogni minuto il passo e la riga "SPESA", rinnova da solo le credenziali OIDC ogni 45 minuti, e alla fine copia i risultati sul branch in `m31/risultati/aws-<run>/` (togliendo il numero dell'account) e come artifact.
4. **Smontaggio automatico**: la macchina si spegne e si cancella da sola alla fine (spegnimento = cancellazione), e comunque dopo 240 minuti; il workflow smette di aspettare a 250 minuti; la pulizia gira sempre, anche se qualcosa fallisce: cancella le macchine con il tag e tutti i bucket `fenvix-m31-*` e scrive "RIMASTO: niente" o "ALLARME". Se resta qualcosa: `{"modo": "smonta"}`.

Durata attesa: circa 1 ora e 45 minuti (STIMATO: carico 35 min, copia 2, tempi 10 GB circa 25, carico e tempi 2 KB circa 12, scritture 9, preparazione 10).

## 7. La cifra, voce per voce

Prezzi DICHIARATI: c7i.xlarge on-demand eu-west-1 0,1915 $/ora (doit.com, pagina c7i.xlarge eu-west-1, letta oggi); S3 GET 0,0004 $ ogni 1.000, PUT 0,005 $ ogni 1.000, spazio 0,023 $ per GB al mese (NUMBERS.md); trasferimento tra EC2 e S3 nella stessa regione gratis (DICHIARATO AWS, letto oggi su una guida ai prezzi S3 2026); IPv4 pubblico 0,005 $/ora e disco gp3 circa 0,088 $ per GB al mese (DICHIARATO dal listino AWS, ma non l'ho riaperto oggi: da ricontrollare, pesano pochi centesimi).
- Macchina: 1,75 ore × 0,1915 = circa 0,34 $; al tetto (4 ore) 0,77 $.
- Disco 60 GB: circa 0,01 $; al tetto 0,03 $.
- IPv4 pubblico: circa 0,01 $; al tetto 0,02 $.
- GET su S3: circa 80.000 (tempi circa 35.000, Q5 circa 19.000, carico da 2 KB circa 15.000, il resto riscaldo e insieme) = circa 0,03 $; al tetto (3 volte) 0,10 $.
- PUT su S3: circa 18.000 (scritture con WAL ogni 10 ms circa 12.000, le altre scritture circa 3.000, risultati ogni minuto circa 2.000, copia circa 300) = circa 0,09 $; al tetto 0,25 $.
- Spazio su S3: circa 7 GB per meno di un giorno = meno di 0,01 $.
- GitHub Actions: circa 2 ore del runner, dentro i minuti gratuiti del piano (DICHIARATO GitHub, non ricontrollato oggi): 0 $ se non sono già finiti.
- **Totale atteso circa 0,50 $; tetto 1,50 $** (somma dei tetti, arrotondata). Credito AWS circa 133 $ (NOW.md).

## 8. Cosa ho deciso io

- Il carico si fa sul disco della macchina e poi si copia su S3, invece di scrivere su S3 durante il carico: le pile sono quelle già controllate in locale e la giornata dura meno. Il costo in byte delle scritture su S3 lo misura a parte `scrivi`.
- Q4 sono gli ultimi 500 messaggi (come `estese` di P1), non 500 messaggi in un mese.
- Q5 usa l'indice `stato | data` già nel prototipo, con le date dalla più nuova (così "i 100 più nuovi" è una scansione in avanti).
- "Serie" vuol dire una GET alla volta, perché SlateDB 0.17 apre già le pile insieme (sezione 2). Tolta la modifica alla libreria che avevo scritto.
- MinIO non si scarica da questa macchina (il proxy blocca dl.min.io e Docker Hub). Al suo posto il server `moto` (pip), che parla la stessa lingua di S3: carico, copia e tempi provati attraverso le API S3, firma e PUT condizionali compresi (MISURATO). Il ritardo finto è nel contatore (lognormale per GET), non in toxiproxy.
- Macchina c7i.xlarge (4 vCPU) e non c7i.large: in locale con 2 vCPU il lavaggio resta indietro durante il carico (11 pile). Ritmo del carico 6 MB/s sulla macchina.
- Ubuntu 24.04 sulla macchina: il programma costruito sul runner usa glibc 2.39, che Amazon Linux 2023 non ha.

## 9. Cosa manca o blocca

- Blocca la giornata: i tre passi IAM (sezione 6) e il sì del fondatore sulla cifra. CloudShell non serve più.
- `prova` non ha mai girato su AWS vero (non poteva): il controllo dei permessi e la prima parte del log servono a fermarsi presto. Lo script di prova completo ha girato in locale contro `moto`, in piccolo, dall'inizio alla fine (MISURATO).
- Atlas Q4, Q5 e le quote sotto 5 ms e sopra 20 ms: NON SO (sezione 4, comando `estese` già pronto).
- Il secondo giro di GET per i clienti attivi (sezione 2): non spiegato.
- Nel carico da 2 KB i clienti dormienti con 500 messaggi sono pochi: Q4 da 2 KB ha un campione piccolo.

## 10. Link

- Branch `freddo-s3`, PR in bozza: https://github.com/Mircoguidetti/fenvix-engine/pull/53
- Codice: `proto/lavaggio-per-cliente/m31/src/main.rs` (`carica`, `copia`, `tempi`, `scrivi`), `src/conta.rs` (contatore, ritardo finto, bancone, richiesta doppia, una GET alla volta)
- Giornata: `.github/workflows/m31-freddo-s3.yml`, `m31/giornata/PERMESSI.md`, `permessi.sh`, `giornata-ci.sh`, `esegui.sh`, `pulizia.sh`
- Risultati locali: `m31/risultati/riassunto-locale.txt` (tutto leggibile), `tempi-locale.jsonl`, `carichi.jsonl`, `scritture-moto-mini.jsonl`; riassunto con `scripts/riassunto.py`

## 11. Errori o mancanze trovati in fenvix-brain

- Il prompt dice "latenze S3 misurate: M6, M21, M25" e PLAN-LAVAGGIO dice "p99 126–138 ms, MISURATO M25". Non sono misure di S3: M6 usa un ritardo finto di 30 ms, M21 un finto S3, M25 e il suo revisore un modello (mediana 25 ms, p99 100 ms). Nessuna sessione ha mai misurato una GET su S3 vero; in NUMBERS.md "20–40 ms per GET" è segnato STIMATO, giusto. La giornata M31 sarebbe la prima misura.
- PLAN-LAVAGGIO, pezzo 2: aggiungere che SlateDB 0.17 legge già le pile di una scansione in parallelo (2 giri per i clienti attivi, 1 per i dormienti, MISURATO) e che i dormienti costano una GET sola.
- RULES.md, "Spese AWS di prova": CloudShell non funziona sull'account (account in verifica, fondatore 2026-11-04): le giornate vanno da GitHub Actions; la trust policy va estesa a ogni branch nuovo (stessa trappola di S13). Forse conviene una regola fissa, per esempio un solo branch "giornate" autorizzato.
