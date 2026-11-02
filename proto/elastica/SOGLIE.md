# M17 · bolletta elastica · soglie e modello, scritti PRIMA di simulare

Prompt: `fenvix-brain/prompts/M17-bolletta-elastica.md`. Solo simulazione, $0.

## Soglie (dal prompt, invariate)

- VINCE: bolletta totale Fenvix almeno 2 volte sotto Infinite (predittiva perfetta) sul profilo (a), su almeno 2
  famiglie di curve, con avvio 60 s e margine 25%.
- MUORE: meno di 1,3 volte sotto Infinite su tutte le famiglie.
- In mezzo: FERITA.

Scelta mia, fissata ora: il verdetto si legge con la CPU per scrittura del caso base (sotto) e si ripete con 0,81;
se i due danno verdetti diversi lo scrivo.

## Curve

- Conteggi per minuto da tracce pubbliche cacheMon (CC BY 4.0): Twitter (grappoli, oracleGeneral 10% chiavi),
  Alibaba e Tencent (dischi, lcs, somma di molti dischi scelti a caso), Meta KV (lcs, 1% chiavi).
- Scala: ogni curva moltiplicata per una costante in modo che la media sia 20.000 operazioni al secondo (a) o
  50.000 (b). Miscela fissa del profilo B1: (a) 80% letture, (b) 50%. La forma (rapporto picco/media) resta.
- Si scartano i minuti prima del primo e dopo l'ultimo minuto pieno di ogni curva.

## Infinite (simulato minuto per minuto)

- CPU del primario: 55 µs per lettura, 300 µs per scrittura con indici (B1, STIMATO). Lo standby non conta CPU.
- Taglie (DICHIARATO, pagina prezzi MongoDB del 29 settembre 2026, us-east-1, per HA shard):
  M10 2 vCPU burst $0,088/h; M20 2 burst $0,227; M30 Gen2 2 $0,677; M40 Gen2 4 $1,354; M50 Gen2 8 $2,709;
  M60 Gen2 16 $5,417. Massimo M60 Gen2: sopra, il carico non è servito (lo scrivo).
- Minimo M30 (predittiva solo da M30 in su; M10/M20 burst non reggono un database da 1 TB). Variante minimo M10.
- Reattiva (DICHIARATO, pagina "Compute Auto-Scaling on Atlas Infinite"): su di una taglia se la media CPU degli
  ultimi 10 minuti > 90% o dell'ultima ora > 75%, non più di una salita ogni 10 minuti (ogni ora per la regola
  dell'ora); giù di una taglia solo se nessuna discesa nelle ultime 24 ore e media CPU < 45% sia negli ultimi 10
  minuti sia nelle ultime 4 ore (CPU della taglia attuale). Memoria non simulata (favorevole a Infinite).
- Predittiva perfetta: a ogni minuto la taglia è almeno la più piccola con CPU ≤ 75% per il carico dei prossimi
  60 minuti (sale prima, fino a due taglie per volta come da documentazione, qui senza limite: favorevole);
  la discesa resta reattiva; nessuna discesa se una salita è prevista entro un'ora (DICHIARATO).
- Fatturazione: "Atlas meters compute hourly, based on the configured cluster tier" (DICHIARATO). Come si conta un
  cambio a metà ora: NON SO. Base: proporzionale ai minuti (favorevole); variante: la taglia più alta dell'ora.
- Spazio: 1 TB con indici a $0,30 per GB non compresso al mese, $345 (revisore B1), più l'oplog (B1: fino a $64
  in (a), $398 in (b)); uso $345 (favorevole). Mese di 730 ore.

## Fenvix (simulato a passi di 10 secondi, carico costante dentro il minuto)

- CPU del cameriere (motore di oggi, S8 MISURATO): lettura 0,93 volte mongod → 55/0,93 = 59 µs; scrittura base
  0,36 volte mongod → 300/0,36 = 833 µs (il peggiore misurato: inserimenti su un database); sensibilità 0,81 →
  370 µs.
- Camerieri c7g.large (2 vCPU, $0,0775/h eu-west-1, DICHIARATO DoiT in NUMBERS.md) e variante c7g.xlarge
  ($0,155/h). Si pagano al secondo con un minuto minimo (DICHIARATO AWS).
- Margine 25% sopra il carico: capacità voluta = carico osservato × 1,25. Carico osservato = media degli ultimi
  60 s. Un cameriere nuovo serve dopo 60 s (STIMATO: dispensa di zona su gp3, nessun riscaldamento da S3), e si
  paga da quando si accende. Si spegne un cameriere quando la capacità voluta resta sotto quella accesa per 10
  minuti (uno alla volta). Minimo 3 camerieri (uno per zona).
- Qualità: minuti in cui il carico supera la capacità servibile (sovraccarico), per Fenvix e per Infinite.
- Voci fisse (uguali per tutte le curve, STIMATO da B1 e revisori):
  (a) quaderno $226, dispensa di zona su gp3 $317, S3 $82, lavaggio $56, tra zone con la dieta (M13) $155,
  portiere $0 (M14 viva per il revisore), resto $65.
  (b) quaderno $451, dispensa $317, S3 $336, lavaggio $112, tra zone con la dieta $970, portiere $0, resto $65.
  Il quaderno però va dimensionato sul picco di scritture: un quaderno regge circa 20.000 record al secondo
  (STIMATO da B1/revisore 18.000–28.000); se il picco lo supera, un quaderno in più ($451) e lo scrivo.
- Sensibilità: avvio 10/60/300 s; margine 10/25/50%; scrittura 0,36/0,81; portiere come B1 ($500 a, $970 b).

## Cambi fatti DOPO il giro di prova (dichiarati, con il verso)

1. Predittiva perfetta: prima "taglia con CPU ≤ 75% minuto per minuto"; ora "sale prima che scatti una delle due
   regole reattive" (media dei prossimi 10 minuti ≤ 90% e dei prossimi 60 ≤ 75%). Motivo: la prima versione
   costava più della reattiva, quindi non era "favorevole a Infinite". Verso: abbassa la bolletta di Infinite.
2. Quaderno: dimensionato sul minuto di scritture al 99,9 percentile invece che sul minuto massimo (un minuto di
   punta su 5 giorni non deve comprare un quaderno per un mese; i minuti oltre si contano). Verso: favorevole a noi.
3. Aggiunte varianti: Infinite con minimo M50 (RAM per la parte calda, ragionamento di B1); "a parità di
   servizio" (si toglie, per tutti e due, il carico che nemmeno una M60 Gen2 serve); quaderno da 28.000 al secondo.
