# M25 · Soglie e metodo, scritti prima di misurare

Scritto il 2026-11-03, prima di misurare. Delle tracce ho guardato solo formato e campi (nomi delle chiamate,
presenza dell'ora), nessun numero di questa misura. Il codice di M24 (`proto/riscaldamento-utente/sim.py`)
lo riuso per le latenze e i conti.

## La domanda

Con gli agenti, tra una chiamata e l'altra il modello pensa per secondi. Se appena si sa su quale cliente
(o entità) lavora l'agente portiamo sul bancone la sua storia, le letture fredde arrivano quando il piatto è
già pronto. Tre politiche, da sole e insieme:

- **(a) primo segnale**: come M24, si scalda il gruppo di un cliente alla prima chiamata che lo rivela
  (ricerca per nome o email, chiamata con il suo `user_id` negli argomenti, risultato che lo contiene).
  Qui ogni cliente rivelato si scalda, non solo il primo della conversazione.
- **(b) condivisi sempre sul bancone**: le collezioni non legate a un cliente (catalogo, voli, piani,
  documenti della base di conoscenza) stanno fisse sul bancone. Riporto quanto pesano sul totale.
- **(c) segnale esplicito**: l'agente o il suo strumento (per esempio un server MCP) dice all'inizio del
  compito "lavoro sul cliente X". Simulato all'inizio della conversazione, con il cliente del compito.
- Combinazioni: a+b, c+b, a+b+c (quest'ultima è "le politiche insieme" della soglia).

## Soglie (dal prompt del coordinatore, fissate prima)

- **LUCE**: con le politiche insieme (a+b+c), almeno il 90% delle letture fredde servito dal bancone su
  almeno **due fonti di agenti**; p99 di lettura sotto 15 ms **oppure** sotto il 5% del tempo mediano di
  un passo del modello; prezzo al cliente sotto il 47% di Infinite.
- **MUORE**: sotto il 60% su **tutte** le fonti anche con il segnale esplicito (c, e c+b).
- **In mezzo**: FERITA, con il rimedio e la prova successiva.
- Decido io: "due fonti" vuol dire due famiglie diverse di agenti. tau2 del 2025 (articolo) e tau2 della
  classifica 2026 contano come **una** famiglia (stessi database). tau-bench originale conta come famiglia
  a sé solo nella lettura larga, perché i database di negozio e compagnia aerea sono gli stessi di tau2:
  nella lettura stretta lo conto insieme a tau2.

## Fonti (cosa cerco, in ordine)

1. **tau2-bench, risultati dell'articolo** (repo pubblico, `data/tau2/results/final`): retail, airline,
   telecom, 4 modelli, **tutti e 4 i tentativi**. Ora vera di ogni chiamata.
2. **tau2-bench, classifica 2026** (traiettorie pubblicate da Sierra su S3 pubblico, scaricate da un
   workflow GitHub Actions senza segreti): modelli del 2026, domini retail, airline, telecom e
   banking_knowledge, 4 tentativi. Ora vera.
3. **tau-bench originale** (repo pubblico, `historical_trajectories`): retail e airline, gpt-4o e
   Claude 3.5 Sonnet. **Senza ora**: i tempi tra le chiamate li prendo dalla distribuzione misurata su
   tau2 (STIMATO).
4. **DAB** (traiettorie già scaricate da M18): agenti sui dati, senza ora; tempi come sopra (STIMATO).
   L'entità è il database del compito (il dataset), non un cliente: il "gruppo" da scaldare sono le sue
   tabelle e collezioni.
5. **Agenti di codice**: traiettorie di SWE-bench Verified pubblicate su GitHub (mini-swe-agent, con l'ora
   di ogni comando). Entità = il repository del compito; letture = file aperti. Somiglianza con un
   database: bassa (sono file di un progetto, non documenti); la riporto per il tipo di app.
6. AppWorld, WorkArena, BIRD-Interact: le cerco; se non trovo traiettorie pubbliche con accessi ai dati
   e tempi, NON SO e vado avanti.

Nessuna traccia generata con un LLM.

## Che cosa è una lettura fredda

Pensato per il profilo B (database grande e freddo, B13): la storia di un cliente non è sul bancone quando
inizia la sua conversazione.

- Ogni file di risultati è un database a sé, riprodotto con l'ora vera (le conversazioni dello stesso file
  si sovrappongono come nel banco di prova).
- **Dati di un cliente**: ogni tentativo è un cliente diverso (chiavi separate per tentativo), così la
  prima lettura di un suo dato nella conversazione è fredda. Le letture successive dello stesso oggetto
  nella stessa conversazione sono calde (sono sul bancone dopo la prima).
- **Dati condivisi**: freddi se nessuno li ha letti o scritti nell'ultima ora in quel database (freddo a
  tempo, X = 1 ora). Le prime letture della traccia contano come fredde: è pessimista per la politica
  senza (b), indifferente con (b).
- Gli indici di ricerca del cliente (`find_user_id_by_*`, `get_customer_by_*`) sono sempre caldi (pochi
  byte per cliente), come in M24: sono segnali, non letture fredde.
- DAB: fredda = prima query su una tabella o collezione nella conversazione.
- SWE: fredda = prima lettura di un file nella conversazione; i file fuori dal repository (librerie
  installate) sono "condivisi".

## Il riscaldamento simulato

- Al segnale parte il caricamento del gruppo: k GET a intervallo in parallelo (sim.Lat.warm, scenario
  medio di M15: GET mediana 25 ms, p99 100 ms, 85 MB/s), storia H = 2 MB (caso base) o 20 MB.
- Una lettura fredda di un gruppo il cui caricamento è finito è servita dal bancone; se il caricamento è
  in corso aspetta il minimo tra il resto del caricamento e la sua GET; altrimenti va su S3 (find fredda
  di M21, 4 GET in parallelo).
- Il gruppo resta sul bancone 30 minuti dopo l'ultima richiesta.
- **Inutile**: riscaldamento senza nessuna lettura fredda del gruppo dopo.
- (b): ogni lettura di un oggetto condiviso è servita (0,05 ms in RAM; variante NVMe).

## Cosa riporto per ogni politica e fonte

- Quota di letture fredde servite dal bancone (MISURATO sulla traccia).
- p50 e p99 di lettura: miscela del **profilo B** (10% delle letture fredde, B13) e miscela della traccia
  (STIMATO dal modello di latenza). Il p99 lo confronto con il tempo di un passo del modello, MISURATO
  sulle tracce con l'ora: dalla risposta di uno strumento alla chiamata successiva dell'agente.
- Riscaldamenti inutili, riscaldamenti per lettura fredda, byte e GET in più.
- Spazio del bancone: storie calde (legge di Little) più la parte condivisa. Per (b) la quota condivisa
  sul totale la misuro sui database dei banchi (byte JSON per collezione) e la dichiaro come forchetta per
  il profilo B (STIMATO).
- Costo in più al mese sul profilo B (20 TB, 35 letture fredde al secondo, B13), prezzo = costo / 0,55,
  contro Infinite centro $7.440 (forchetta $6.500–8.900), base M21 con dati raggruppati $780–1.050.
  NVMe $0,17 per GB al mese (M21), GET $0,0004 ogni 1.000 (DICHIARATO, NUMBERS.md).

## Tipi di app

Per servizio clienti (tau2, tau-bench), agenti sui dati (DAB), agenti di codice (SWE) e agenti di vendita
(retail di tau2 come approssimazione, o NON SO): quanto pesa la parte condivisa nelle letture fredde e nei
byte, e se le politiche bastano.
