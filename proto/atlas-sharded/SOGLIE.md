# M19 · Atlas sharded contro i quaderni al minuto · soglie e modello, scritti PRIMA di simulare

Prompt: `fenvix-brain/prompts/M19-atlas-sharded-sim.md`. Solo simulazione, $0. Riusa `proto/elastica/sim.py` (M17):
le curve, `sim.fenvix` (camerieri) e `sim._prefix`. Il motore non è toccato.

## Soglia (dal coordinatore, DECISIONS 2026-11-03)

- Rapporto = prezzo al cliente da noi (costo / 0,55) diviso bolletta Atlas Core sharded.
- PASSA: rapporto sotto il 47% in variante (i) (autoscaling acceso) su almeno un profilo realistico.
- MUORE: sopra il 47% in variante (i) su tutti i profili, anche con mongod a 600 µs per scrittura.
- FERITA: sotto il 47% solo in (ii) o (iii).
- Scelta mia, fissata ora: il verdetto si legge sul valore centrale (mediana delle curve Meta + Twitter, che sono le
  famiglie "da database" di M17; Alibaba e Tencent riportati a parte), con le ipotesi centrali sotto. Do sempre anche
  la forbice (nostro caso migliore contro Atlas più caro, nostro peggiore contro Atlas più economico).
- "Profilo realistico": A, B o C con la forma delle curve Meta/Twitter. Il profilo C con 600 µs conta.

## Profili (scritture al secondo; letture = scritture, stessa curva, come B6)

- Curve: le 16 di M17 (12 Twitter, 2 Meta, somma Alibaba, somma Tencent), ripetute fino a 28 giorni (A e C) o a
  365 giorni (B). Il primo giorno non si conta (avvio).
- "Picco" = minuto al 99,9 percentile delle scritture (come il quaderno di M17).
- A: 2 TB logici. Forma: curva^k, con k scelto perché picco/media = 5 (la punta giornaliera di B6); poi scala con
  picco 40.000 scritture/s (media circa 8.000). Variante "forma vera" (k = 1, picco 40.000, la media viene dalla curva).
- B: 2 TB. Base = curva^k con picco/media della base = 1,5 ("stabile"); 4 eventi l'anno (giorni 45, 136, 227, 318)
  di 3 giorni a 10 volte, con salita e discesa lineari di 6 ore; scala con picco 40.000.
- C: 5 TB, come A con picco 80.000.
- Scala: la serie A moltiplicata per 0,25 / 0,5 / 1 / 2 / 4 / 8 (picco da 10.000 a 320.000), con 2 TB fissi e con TB
  proporzionali (2,5 TB ogni 40.000 di picco, come C), per dire da che grandezza il rapporto scende sotto il 47%.

## Atlas Core sharded (simulato minuto per minuto, shard per shard)

- Listino DICHIARATO (mongodb.com/pricing, aggiornato 29/09/2026, us-east-1, cluster da 3 nodi, spazio di base
  compreso): M30 2 vCPU $0,54/h, 512 GB max; M40 4 vCPU $1,04, 1 TB max; M50 8 vCPU $2,00, 4 TB; M60 16 vCPU $3,95,
  4 TB; M80 32 vCPU $7,30, 4 TB. Spazio di base compreso: 40 / 80 / 160 / 320 / 750 GB (docs "Amazon Web Services").
- CPU del primario: 55 µs per lettura; scrittura 300 µs (B1) o 600 µs (M16), come B6. Le letture vanno al primario.
- Numero di shard S fisso sul picco: il più piccolo S per cui lo shard più carico al picco sta sotto il 65% di una
  M60 (B6). Varianti: shard dimensionati su M50 (più shard piccoli) e su M80 (meno shard grandi). Più il vincolo di
  spazio: al massimo 4 TB di disco per shard.
- Carico per shard: quote fisse da 0,9 a 1,1 (squilibrio del 10%, scelta mia); variante perfettamente in pari.
- Spazio su disco per nodo: 0,8 GB fisici per GB logico (B6: circa 800 GB per nodo con 2 TB su 2 shard), diviso per
  S, più 25% di margine (l'autoscaling dello spazio sale al 90% pieno). Pavimento della taglia: la più piccola taglia
  il cui spazio massimo contiene il disco dello shard (DICHIARATO, docs: non si scende sotto la taglia che regge il
  disco), e almeno M30 (la predittiva vuole M30 o più).
- Spazio oltre il compreso: $0,10–0,25 per GB per nodo (STIMATO, B1/B6), calcolato ora per ora sulla taglia.
  Backup $100–160 per TB logico (B6: $200–320 per 2 TB). Server di configurazione $0 (config shard, B6).
- (i) Autoscaling acceso (DICHIARATO, "Compute Auto-Scaling on Atlas Core"):
  - sale di una taglia se la CPU media supera il 90% negli ultimi 10 minuti o il 75% nell'ultima ora; non due salite
    in 10 minuti (un'ora per la regola dell'ora);
  - scende di una taglia se la CPU è sotto il 45% sia negli ultimi 10 minuti sia nelle ultime 4 ore, e nessuna
    discesa nelle 24 ore prima; memoria non simulata (favorevole ad Atlas: la memoria può solo bloccare discese);
  - predittiva perfetta come M17 (sale un minuto prima che scatti una regola reattiva), al massimo due taglie per
    volta, nessuna discesa se una salita è prevista entro un'ora. Su B la predittiva vede anche gli eventi: è il
    "pre-scale" manuale che MongoDB consiglia per gli eventi (la predittiva vera non vede cicli oltre 4 settimane);
  - per shard, indipendenti; lo shard più piccolo al massimo due taglie sotto il più grande;
  - taglia massima M80; taglia minima = pavimento dello spazio (il cliente ha allargato i limiti: favorevole ad
    Atlas, perché di serie il minimo è la taglia di creazione, DICHIARATO).
  - Sotto-variante: solo reattiva (senza predittiva).
- (ii) Discesa spenta: come (i) ma senza discese; parte dal pavimento.
- (iii) Tutto sul picco: ogni shard sulla taglia del dimensionamento (M60) tutto il tempo.
- Fatturazione proporzionale ai minuti (favorevole ad Atlas, M17). Cambio di taglia istantaneo (favorevole ad Atlas).
- Mese di 730 ore.

## Noi (pesi di B6 e M17, tutto STIMATO)

- Camerieri: `sim.fenvix` di M17 (c7g.large, margine 25%, avvio 60 s, calma 10 minuti, almeno 3). CPU per lettura
  59 µs; per scrittura 833 µs (0,36 volte mongod) o 370 µs (0,81), raddoppiata nel caso 600 µs.
- Quaderni accesi al minuto: ogni minuto ceil(scritture / (tetto × 0,8)) quaderni, almeno uno, $451 al mese l'uno
  (M17); tetto 20.000 (M17) o 42.000 (M16) record al secondo.
- Dispensa di zona: $185 per TB (con compressione delle scatole) o $317 per TB (senza) (B6: $370/$634 per 2 TB).
- S3: $24,7 per TB + $0,0121 per scrittura/s di media; lavaggio $45,8 per TB + $0,0052 per scrittura/s (ricavati
  dai valori di B6 per A e B). Tra zone $39 ogni 1.000 scritture/s di media (B6). Resto $80.
- Prezzo = costo / 0,55.
- Ipotesi centrale: media dei due estremi. Nostro caso migliore: 370 µs, tetto 42.000, compressione. Peggiore:
  833 µs, tetto 20.000, senza compressione. Atlas: spazio $0,10 o $0,25, backup $100 o $160 per TB; centro a metà.
- I nostri prezzi sono eu-west-1, quelli di Atlas us-east-1 (sfavorevole a noi di circa 5–10%, B5/B6).

## Cosa riporto

Per profilo, variante, CPU: ore al mese per taglia per shard, bolletta Atlas (calcolo, spazio, backup), nostra
bolletta per voce, prezzo, rapporto; quanto tempo la variante (i) resta sulla taglia del picco; da che grandezza
(shard, scritture al secondo) il rapporto scende sotto il 47%. Ogni cambio fatto dopo il primo giro è scritto qui
sotto, con il verso.

## Cambi fatti dopo il primo giro

(nessuno per ora)
