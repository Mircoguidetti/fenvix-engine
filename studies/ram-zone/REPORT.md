# B9b · Una dispensa in RAM al posto delle dispense su NVMe di Infinite?

Studio sulla carta, $0, 2026-11-03. Nessun codice cambiato, nessuna chiamata AWS. Prezzi AWS on-demand us-east-1, mese di 730 ore. Etichette: DICHIARATO (con link), MISURATO (con file del repo), STIMATO (con il perché).

Glossario: la **dispensa di zona** è il livello delle pagine (Page Service di Atlas Infinite: server di pagine su NVMe in ogni zona). La **mensola calda** è la parte calda dei dati (hot set) tenuta in RAM. Il **cuoco di turno** è il proprietario del database (lease owner).

## Verdetto (prima di tutto)

**Sì, ma solo se la parte calda è piccola, e il disegno che vince è B, non A.**

1. **B (RAM solo sul cuoco di turno, 1 copia) vive fino al 10% di parte calda**, sia a 1 TB sia a 5 TB: costa il 5%, il 25% e il 51% della proxy di Infinite all'1%, al 5% e al 10% (STIMATO). Al 30% costa 1,5 volte C: da killare. Il suo costo vero non è nei soldi ma nel tempo: dopo un cambio di turno la mensola è vuota. Riempirla da S3 costa da $1 a $146 una volta sola (STIMATO), ma il tempo per riempirla NON SO (non si misura sulla carta).
2. **A (RAM in tre zone, una copia piena per zona) vive solo all'1%.** Costa il 21% di C a 1 TB e il 17% a 5 TB. Al 5% sta in mezzo (83% e 77%). Dal 10% in su è da killare: 1,5 volte C al 10%, 4,6 volte al 30% (STIMATO).
3. **Il pareggio** (dettagli più sotto): A costa come C quando la parte calda è il 6,3% dei dati; B costa come C al 18,8% (STIMATO, dai prezzi DICHIARATI). Per "vivere" (60% di C o meno) servono una parte calda fino al 3,8% per A e fino all'11,3% per B.
4. **Attenzione, la proxy C è cara.** Tre copie piene su i4i costano a noi circa $1,13 al mese per GB logico (STIMATO), cioè 3,8 volte quello che Infinite fa pagare al cliente per lo spazio ($0,30 per GB non compresso, DICHIARATO). Quindi Infinite quasi certamente non fa la C ingenua: comprime e usa dischi più densi (come già detto da B2). Battere C non vuol dire battere Infinite. Contro i $0,30 di Infinite: B costa $0,06, $0,28 e $0,57 per GB logico all'1%, al 5% e al 10%; A costa $0,24, $0,94 e $1,76 (STIMATO). **Solo B fino a circa il 5% e A all'1% stanno sotto il prezzo dello spazio di Infinite.**
5. **Scala (principio "deve reggere 1 TB e oltre").** B non regge 5 TB con il 30% caldo: servono circa 1.875 GB di RAM su una sola macchina, più della r8g più grande (1.536 GiB, cioè circa 1.650 GB, DICHIARATO). B è un disegno che vale finché la parte calda sta in una macchina sola. A invece scala in orizzontale, ma costa troppo sopra il 5%.

| Disegno | 1 TB, 1% | 1 TB, 5% | 1 TB, 10% | 1 TB, 30% | 5 TB, 1% | 5 TB, 5% | 5 TB, 10% | 5 TB, 30% |
|---|---|---|---|---|---|---|---|---|
| A, RAM 3 copie | vive (21%) | in mezzo (83%) | da killare (156%) | da killare (458%) | vive (17%) | in mezzo (77%) | da killare (152%) | da killare (456%) |
| B, RAM solo sul cuoco | vive (5%) | vive (25%) | vive (51%) | da killare (152%) | vive (5%) | vive (25%) | vive (51%) | da killare (152%), e non sta in una macchina |
| C, proxy Infinite | riferimento, $1.127 al mese | uguale | uguale | uguale | riferimento, $5.634 al mese | uguale | uguale | uguale |

Le percentuali sono il costo diviso per C allo stesso scenario (STIMATO). "In mezzo" vuol dire tra il 60% e il 100% di C.

## I prezzi e i conti

**Prezzo per GB al mese (prima di tutto il resto).**
- RAM: r7g.medium, 1 vCPU e 8 GiB, $0,0536 all'ora, cioè $39,13 al mese e **$4,56 per GB** (DICHIARATO, [holori r7g.medium](https://calculator.holori.com/aws/ec2/r7g.medium/us-east-1)). La r8g.medium costa $0,0589 all'ora, **$5,01 per GB** (DICHIARATO, [holori r8g.medium](https://calculator.holori.com/aws/ec2/r8g.medium/us-east-1); coincide con un quarto della r8g.xlarge, $0,23564, letta sul file prezzi di AWS [awsstatic](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/US%20East%20(N.%20Virginia)/Linux/index.json)). Uso la r7g, la più economica.
- NVMe: i4i.large, 468 GB, $0,1715 all'ora (un quarto della i4i.2xlarge, $0,686, DICHIARATO, file prezzi AWS sopra), cioè **$0,27 per GB**. La i7i.large costa $0,1888 all'ora per 468 GB, **$0,29 per GB** (DICHIARATO, stesso file). La i7ie.xlarge, più densa, costa $0,5198 per 2.500 GB, **$0,15 per GB** (DICHIARATO, stesso file).
- gp3: **$0,08 per GB** (DICHIARATO, [pagina EBS](https://aws.amazon.com/ebs/pricing/), esempio che non nomina la regione; in eu-west-1 $0,088, MISURATO dal listino in `results-walbench/prices.json` sul branch `walbench`).
- S3 Standard: **$0,023 per GB**, GET $0,0004 ogni 1.000 (MISURATO dal listino in `results-walbench/prices.json`, eu-west-1, stesso prezzo di us-east-1 in `results-round3.md`; GET DICHIARATO anche su [pagina S3](https://aws.amazon.com/s3/pricing/)).

**I rapporti** (STIMATO, divisioni dei prezzi sopra): la RAM costa 17 volte l'NVMe, 57 volte gp3, 198 volte S3. L'NVMe costa 3,3 volte gp3 e 11,6 volte S3.

**Il pareggio.** Riempio la RAM all'80% e l'NVMe al 75% (STIMATO, sotto). Allora un GB utile di RAM costa $5,69 e uno di NVMe $0,357. A paga 3 copie della parte calda, C 3 copie di tutto: pareggio quando la parte calda è $0,357 / $5,69 = **6,3%**. B paga 1 copia della parte calda: pareggio a 3 × 6,3% = **18,8%**. La soglia "vive" (60% di C) cade al **3,8%** per A e all'**11,3%** per B. Questi pareggi non dipendono dalla taglia del database né dalla compressione, purché RAM e NVMe tengano i dati nello stesso formato.

**C, la proxy di Infinite** (STIMATO). 1 TB: 3 i4i.large per zona, 9 in tutto, $1.127 al mese. 5 TB: 15 per zona, 45 in tutto, $5.634 al mese. Con la i7ie densa costerebbe poco più della metà (57%, STIMATO dal prezzo per GB): i pareggi salirebbero a circa il 3,5% per A e il 10,7% per B, e B al 10% passerebbe da "vive" a "in mezzo".

**A** (STIMATO). Macchine r7g.medium per zona: 2, 8, 15 e 44 a 1 TB; 8, 37, 73 e 219 a 5 TB (all'1%, 5%, 10%, 30%). Costo totale per 3 zone: $235, $939, $1.761, $5.165 a 1 TB; $939, $4.343, $8.569, $25.707 a 5 TB. Il traffico tra macchine della stessa zona è gratis: "Data transfer within the same Availability Zone (uniquely identified using the AZ-ID) is free irrespective of account ownership" (DICHIARATO, [docs VPC](https://docs.aws.amazon.com/en_cn/vpc/latest/userguide/vpc-share-billing.html)); anche la [pagina prezzi VPC](https://aws.amazon.com/en/vpc/pricing/) non fa pagare il traffico "since the traffic stays in the same Availability Zone using private IP addresses". Tra zone diverse: $0,01 per GB per verso (DICHIARATO, stessa pagina; $0,010 anche in `results-walbench/prices.json`).

**B** (STIMATO). Costo della RAM in più sul cuoco di turno: $57, $285, $569, $1.708 a 1 TB; $285, $1.423, $2.847, $8.541 a 5 TB. Se per le taglie grandi serve la r8g, si aggiunge il 10%: i verdetti non cambiano. **Mensola vuota dopo il cambio di turno, una volta sola**, contando un GET per pagina da 4 KiB: $0,98, $4,88, $9,77, $29,30 a 1 TB; $4,88, $24,41, $48,83, $146,48 a 5 TB. Con letture da 1 MiB il costo scende sotto $0,60 in ogni scenario.

**Atlas Infinite, pagina pubblica** (DICHIARATO, [prezzi MongoDB](https://www.mongodb.com/en-us/pricing), [costi Infinite](https://www.mongodb.com/docs/atlas/billing/atlas-infinite-costs.md)). Spazio $0,30 per GB al mese di dati non compressi, misurato a ore, oplog compreso. Calcolo per coppia primario più standby ("HA shard"): M40 Gen2 $1,354 all'ora, M50 Gen2 $2,709, M60 Gen2 $5,417, prezzi us-east-1. Fino a 128 TB per HA shard. 24 ore di backup comprese. La replica dentro lo strato di storage non si paga. È in anteprima pubblica. Il costo delle dispense non è pubblico: per questo C è una proxy.

## Velocità: solo la classe, senza numeri inventati

- **RAM locale** (B): il nostro lettore fa una lettura puntuale in circa 4 µs (MISURATO, `fenvix-brain/sessions/2026-11-02-misura-lettore/`).
- **RAM nella stessa zona, via rete** (A): "microsecond read latency" (DICHIARATO, [FAQ ElastiCache](https://aws.amazon.com/elasticache/faqs/)).
- **NVMe locale** (C, dal punto di vista del server di pagine; dal cuoco si aggiunge un salto di rete nella zona): ElastiCache dice che leggere dal suo SSD locale aggiunge "300µs latency on average" rispetto alla RAM (DICHIARATO, stessa FAQ).
- **S3** (mensola vuota di B, o pagine fredde in A e B): "roughly 100–200 milliseconds" (DICHIARATO, [guida S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)); nel repo un PUT su S3 Standard ha p50 di 23 ms (MISURATO, `results-walbench.md` sul branch `walbench`).

Quindi A e B sono più veloci di C sulle letture calde. Sulle letture fredde (la parte che non sta in RAM) vanno a S3, cioè circa cento volte più lenti di C. **A e B sono confrontabili con C solo se le letture fuori dalla parte calda sono rare**: quanto siano rare NON SO per un cliente da 1 TB.

## Ipotesi

1. Dati: 1 TB e 5 TB **logici**, la stessa unità che fattura Infinite. RAM e NVMe tengono i dati nello stesso formato; se la RAM tenesse pagine decompresse e l'NVMe pagine compresse, A e B andrebbero peggio (STIMATO: circa 1,4 volte con il rapporto 1 TB → 700 GB di B1).
2. RAM usabile all'80% (STIMATO: sistema operativo, programma, frammentazione; la r7g.medium ha solo 8 GiB). NVMe riempito al 75% (STIMATO, nel mezzo del 60–75% di B2). Macchine arrotondate per eccesso.
3. Contato solo il livello delle pagine. Escluse in tutti e tre: calcolo dei cuochi, quaderno, S3 come magazzino. Anche il viaggio dei byte che tiene aggiornate le copie nelle altre zone (A e C) è escluso: STIMATO simile tra A e C, perché entrambi leggono lo stesso log.
4. C: una copia piena per zona, 3 in tutto. B2 stimava da 3 a 6 copie: con 6 copie C raddoppia e i pareggi raddoppiano (A al 12,5%, B al 37,6%).
5. B: tutto il prezzo della r7g conta come RAM, anche le vCPU (STIMATO, per eccesso). Una sola copia: se il cuoco di turno cade, la mensola si riempie da S3.
6. Pagina da 4 KiB: dimensione di blocco predefinita di SlateDB (STIMATO, `src/lifecycle.rs` non la imposta). La cache di oggi è 256 MiB condivisa (MISURATO in codice, `mongo-server/src/main.rs`, `block_cache_mb`).
7. Prezzi on-demand, senza sconti. Gli sconti 1–3 anni (34–55%, DICHIARATO holori) valgono su tutte e tre le famiglie e non spostano i pareggi.
8. Fonti non lette: la pagina on-demand di AWS non mostra le tabelle (scritte via JavaScript); il file prezzi di AWS si legge solo in parte. Per r7g.medium, r8g.medium e i4i.large ho usato holori. Per r8g e i4i i prezzi di holori coincidono con quelli di AWS (r8g.xlarge diviso 4, i4i.2xlarge diviso 4).
