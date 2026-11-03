# M24 · Soglie e metodo, scritti prima di misurare

Scritto il 2026-11-03, prima di aprire qualsiasi traccia per misurare. Le tracce le ho solo elencate
(nomi dei file, campi, durata). Nessun numero di questa misura è stato guardato prima di questo file.

## La domanda

Quando arriva una lettura fredda (dato non sul bancone), lo stesso utente ha già fatto una richiesta
poco prima? Se sì, il cameriere poteva portare sul bancone tutta la sua storia (dati raggruppati per
utente, M21: 3–7 GET) prima che la chiedesse.

## Soglie (dal prompt del coordinatore, fissate prima)

- **LUCE**: almeno il 70% delle letture fredde è preceduto da un segnale dello stesso utente entro 5 s,
  su almeno due fonti; p99 di lettura con il riscaldamento sotto 10–15 ms (decido io: passa se sotto
  15 ms, pieno se sotto 10); prezzo al cliente sul profilo B ancora sotto il 47% di Infinite.
- **MUORE**: meno del 30% preceduto (sulle fonti che decidono), oppure il riscaldamento costa tanto da
  portare il prezzo sopra il 47%.
- **In mezzo**: FERITA, con la prova che deciderebbe.

## Fonti e chi è "l'utente"

- **tau2-bench** (Sierra Research, repo pubblico, `data/tau2/results/final`): conversazioni vere di
  agenti di servizio clienti con l'ora di ogni chiamata. Utente = il cliente della conversazione.
  Proprietà dei dati da `domains/*/db.json` (ordini e prenotazioni hanno il loro `user_id`).
  Solo il primo tentativo di ogni compito (come l'ambito "clienti" di M18), così lo stesso cliente non
  arriva quattro volte insieme.
- **GH Archive** (eventi pubblici di GitHub, ora al secondo): utente = chi fa l'azione (`actor`);
  proprietario del dato = il padrone del repository (parte prima della `/`). Le azioni sono quasi
  tutte scritture: ogni evento conta come una lettura dello stato del pezzo di repository che tocca.
  Scaricata da un workflow GitHub Actions (rete bloccata qui), campione per utente (hash del nome).
- **Twitter** (`twitter/cache-trace`, campioni 2020Mar, come M9): il campo `client id` è "il servizio
  frontend che manda la richiesta" (DICHIARATO, README del repository), non una persona. Lo misuro come
  controllo, **non conta per il verdetto**.
- **Meta KV**: i campi sono `usecase` e `sub_usecase` (visto nelle sbirciate di M15): nessun
  identificativo di utente. Non usata.
- **E-commerce** (REES46 o Retailrocket, se il workflow trova una copia pubblica senza credenziali):
  utente = `user_id`/`visitorid`; dato proprio = carrello e ordini; il catalogo è condiviso.
  Se non si trova, NON SO e si va avanti con le altre.

## Che cosa è "freddo"

- **A tempo**: oggetto non letto né scritto da più di X ore, X = 1, 6, 24. Si misurano solo le letture
  dopo le prime X ore della traccia (prima non so la storia). Dove la traccia è più corta di 2X, quel X
  non si riporta.
- **A bancone**: oggetto fuori da un bancone LRU grande l'1% o il 10% degli oggetti toccati nella
  traccia. Il primo 20% delle letture riempie il bancone e non si misura (come M15).
- Le scritture mettono l'oggetto sul bancone (il cameriere che scrive ha i byte), come M15.
- tau2: il database c'era prima della traccia e i suoi clienti non erano attivi, quindi la prima
  lettura di un dato del cliente nella traccia è fredda. Gli indici di ricerca del cliente
  (`find_user_id_by_*`, `get_customer_by_*`) li considero sempre caldi: sono piccoli (pochi byte per
  utente) e si tengono fissi sul bancone.
- Caso che decide: **a tempo, X = 1 ora** dove la traccia lo permette (GH, e-commerce); **a bancone 10%**
  per tau2 e Twitter (tracce corte).

## Il segnale

- Per ogni lettura fredda: c'è una richiesta dello **stesso utente** in [t − w, t), w = 1, 5, 30, 300 s?
  Conta solo una richiesta che viene prima nell'ordine della traccia.
- Con l'ora al secondo (GH, e-commerce) due eventi nello stesso secondo hanno una distanza vera che non
  so: la estraggo a caso, uniforme tra 0 e 1 s.
- Due misure:
  - **stretta** (decide): il segnale viene dal proprietario del dato letto. In GH: chi legge un dato
    di un altro (un issue sul repository di un altro) non riscalda quel dato con il suo segnale.
    In tau2: il cliente è noto solo dopo la chiamata che lo identifica.
  - **larga** (solo riportata): GH, una richiesta qualunque sui dati dello stesso proprietario
    (anche di altri: il "condominio" di un'organizzazione); tau2, il cliente è noto dall'inizio della
    conversazione (app con login).

## Il riscaldamento simulato

- Al primo segnale di un utente il cui gruppo non è sul bancone parte il riscaldamento: tutti i suoi
  oggetti già esistenti arrivano sul bancone dopo una latenza di caricamento L.
- Il gruppo resta sul bancone fino a 30 minuti dopo l'ultima richiesta dell'utente (provo anche 5 minuti
  e 2 ore).
- Una lettura fredda di un gruppo caldo è servita dal bancone; se il caricamento è in corso aspetta il
  resto (o la sua GET, se è prima); altrimenti va su S3.
- **Inutile**: riscaldamento durante il quale non arriva nessuna lettura fredda di quel gruppo.
- Confronto senza utente (come M15): alla lettura dell'oggetto o, si anticipa l'oggetto che l'ultima
  volta è venuto dopo o (tabella dei successori), 1 GET, tenuto 30 minuti. Stessa misura.

## Latenze (scenario medio di M15, DICHIARATO dalle fonti di M15 o STIMATO)

- Bancone in RAM: 0,05 ms. Variante su NVMe: lognormale, mediana 0,4 ms, p99 1,5 ms.
- Una GET su S3 Standard: lognormale con mediana 25 ms e p99 100 ms, più i byte a 85 MB/s.
- Find fredda con i dati raggruppati (M21): k GET in parallelo, una per pila, k = 4 (chat) o 7
  (eventi); il tempo è quello della più lenta; nel 10% dei casi un secondo giro di una GET (M21:
  1,1 giri).
- Caricamento della storia di un utente: k GET a intervallo in parallelo, ognuna con H/k byte.
  H = 2 MB (profilo B con 10 milioni di utenti) o 20 MB (1 milione di utenti), STIMATO.
- p99 su due miscele: quella della traccia e quella del **profilo B** (10% delle letture fredde, B13).
  **Decide il profilo B**, perché le tracce hanno miscele lontane da un database grande.

## Costi sul profilo B (B13 e M21)

- 20 TB, 500 operazioni al secondo, 35 letture fredde al secondo (B13, STIMATO).
- Dalla traccia misuro: quota di letture fredde servite S, riscaldamenti per lettura fredda R, tempo
  medio di permanenza sul bancone T.
- GET risparmiate: 35 × S × (3,6–6,8). GET in più: 35 × R × (4–14) (k pile × 1–2 collezioni).
- Byte caricati: 35 × R × H al secondo. Spazio sul bancone (legge di Little): 35 × R × T × H.
- NVMe circa $0,17 per GB al mese (M21, STIMATO). GET $0,0004 ogni 1.000 (DICHIARATO, NUMBERS.md).
- Prezzo = costo / 0,55. Base M21 con i dati raggruppati: costo $780–910 (chat) e $900–1.050 (eventi),
  Infinite centro $7.440 (forchetta $6.500–8.900).
