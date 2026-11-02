# M15 · Soglie e latenze, scritte prima di simulare

Scritto il 2026-11-02 prima della prima corsa del simulatore. Le tracce le ho solo elencate (`risultati/esplora*`), non simulate.

## Riferimento "velocità Atlas"

Ho cercato numeri pubblicati di p50/p99 delle letture puntuali su Atlas (ricerca web: solo pagine di metriche, confronti di concorrenti come ScyllaDB e Couchbase/Altoros, nessun numero ufficiale MongoDB). NON SO un numero DICHIARATO da MongoDB. Quindi vale il riferimento del prompt: **p50 ≤ 2 ms e p99 ≤ 10 ms lato server**.

## Latenze (DICHIARATE o STIMATE, prima di simulare)

Bancone (cache del cameriere):
- RAM: 0,05 ms fisso (dal prompt).
- Variante su disco di rete EBS gp3: AWS dichiara "single-digit millisecond latency" (docs EBS, General Purpose SSD). Nostra misura vicina: fdatasync su gp3 p99 0,75 ms con 1 scrittore (MISURATO, walbench, NUMBERS.md; sono scritture, non letture). Modello: lognormale mediana 0,4 ms, p99 1,5 ms (STIMATO). Solo come variante nello scenario medio.

S3 Standard, una GET: attesa del primo byte lognormale + byte / 85 MB/s (stesso modello di M10, così i numeri si confrontano).
- ottimista: mediana 20 ms, p99 50 ms;
- medio: mediana 25 ms, p99 100 ms (lo "centrale" di M10);
- pessimista: mediana 30 ms, p99 200 ms (il "pesante" di M10), più 1 GET su 1.000 che aspetta 1 s in più.
Fonti (DICHIARATO): GreptimeDB 2024 (oggetti < 1 KiB: p50 circa 30 ms, p99 circa 60 ms); Quickwit (primo byte fino a circa 30 ms, code di circa 80 ms "comuni", p90 circa 50 ms); Durner, Leis, Neumann, VLDB 2023 (mediana circa 30 ms per oggetti piccoli); nixiesearch, dic. 2025 ("100+ ms p99" su S3 Standard, letture da 4 KB).

S3 Express One Zone, una GET nella stessa zona: stesso modello.
- ottimista: mediana 2 ms, p99 5 ms;
- medio: mediana 4 ms, p99 10 ms;
- pessimista: mediana 6 ms, p99 25 ms.
Fonti (DICHIARATO): AWS "consistent single-digit millisecond" (pagina del prodotto e guida alle prestazioni); Quickwit p90 circa 5 ms (segnato con "?" dall'autore); nixiesearch "5–10 volte più lento di EBS". Da un'altra zona: +0,5 ms o più (DICHIARATO, Momento, "cross-AZ hops typically add 500µs or more"). Simulo un ripiano per zona (nessun viaggio tra zone); il costo conta 3 ripiani.

Banda per GET: 85 MB/s (come M10; DICHIARATO a memoria dalla guida AWS, non riverificato).

## Prezzi (DICHIARATO)

- S3 Standard GET $0,0004 ogni 1.000 (NUMBERS.md, eu-west-1).
- S3 Express One Zone dal 10 aprile 2025, prezzi us-east-1 (blog AWS "up to 85% price reductions"): $0,11 per GB al mese, PUT $0,00113 ogni 1.000, GET $0,00003 ogni 1.000, caricamento $0,0032 per GB, lettura $0,0006 per GB. Prezzo esatto in eu-west-1: NON SO (la pagina dei prezzi non si apre da qui); Express esiste in eu-west-1 nelle zone euw1-az1 ed euw1-az3 (DICHIARATO, docs AWS), quindi **solo 2 zone su 3**.
- Accesso da un'altra zona: nessun costo per scrivere in un bucket di un'altra zona (DICHIARATO, Jack Vanlightly 2024); per le letture NON SO con certezza.

## Cosa conta come "letture lente"

Con il riferimento p99 ≤ 10 ms, una lettura che va su S3 Standard (mediana 25 ms nello scenario medio) è quasi sempre sopra 10 ms. Quindi il p99 sta sotto 10 ms solo se meno di circa l'1% delle letture va su S3 Standard. La richiesta doppia non può portare una GET Standard sotto 10 ms: serve sulla coda (p999) e sul ripiano Express.

## Soglie (dal prompt)

- **VINCE**: con bancone ≤ 10% dei dati toccati, p99 ≤ 10 ms nello scenario medio, su almeno due famiglie di tracce, con una combinazione il cui costo in più (Express, GET doppi, byte anticipati) per un database da 1 TB a 20.000 letture al secondo resta sotto il 10% della bolletta B1 profilo (a): 10% di $2.086 = **$209 al mese** (STIMATO; B1 (a) compresso, revisore B1).
- **MUORE**: anche con tutti gli attrezzi serve un bancone ≥ 50% dei dati per stare sotto la soglia nello scenario medio.
- In mezzo: **FERITA**.

## Scelte del simulatore (decise ora)

- "Dati toccati": pagine da 4 KB (o chiavi) toccate da qualunque operazione nella finestra simulata. Bancone 2%, 5%, 10%, 20%, 50% di questo.
- Scritture: entrano nel bancone (il cameriere che scrive ha i byte), non contano come letture.
- Riscaldamento: il primo 20% delle richieste riempie il bancone e non si misura.
- Una lettura di disco che tocca più pagine conta una volta; le pagine mancanti contigue arrivano con una GET a intervallo; il tempo è quello della GET più lenta.
- Tracce chiave-valore (Meta, Twitter): prima una cache dell'app che tiene ogni risposta 60 s (come M9); al database arrivano le mancate.
