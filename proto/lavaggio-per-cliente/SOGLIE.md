# M26 · Soglie e metodo (scritte prima di misurare, 2026-11-03)

## Domanda

Quanto costa il lavaggio (compattazione) che raggruppa i dati per cliente in tutte le collezioni e gli indici, con un tetto alle pile (sorted run), su un database grande (20 TB, 4.000 scritture al secondo): amplificazione di scrittura, CPU, richieste S3, e come restano le letture fredde.

## Soglie (del coordinatore, fissate prima)

- REGGE: una strategia con find fredda tipica sotto 8 GET e "scaldare un cliente" sotto 25 GET a 20 TB proiettati, con un costo del lavaggio che lascia il prezzo al cliente sotto il 47% di Infinite.
- MUORE: nessuna strategia sotto 15 GET per find fredda senza far salire il prezzo sopra il 47%.
- In mezzo: FERITA, con il rimedio e la prova successiva.
- "Find fredda tipica" = come M21: 60% ultimi 20 messaggi di un cliente (Q1), 20% un documento per _id (Q2), 20% intervallo di date di un cliente su un'altra collezione (Q3, al massimo 100 documenti). Bancone S1 (indici dei blocchi e filtri caldi, blocchi di dati vuoti a ogni query).
- "Scaldare un cliente" = leggere tutti i dati di un cliente in tutte le sue collezioni (documenti, non gli indici globali), bancone S1, come il revisore di M25. Riporto media e p90 sui clienti con almeno 20 documenti.

## Metodo

1. Niente motore toccato. Prototipo in `proto/lavaggio-per-cliente/`, con la stessa libreria del motore (SlateDB 0.17.0) e il contatore e il bancone di M21.
2. Carico realistico "misto", con scritture continue: clienti con attività zipf (s = 0,7), 3 collezioni per cliente: storico chat (inserimenti, indice {cliente, ts}), eventi per dispositivo (inserimenti, indice {cliente, ts}), ordini (inserimenti e aggiornamenti dello stato, indice {cliente, ts} e indice globale {stato, ts}). Prima si riempie fino alla taglia, poi si continua a scrivere con una ritenzione (i documenti più vecchi si cancellano) così la taglia resta ferma: questo è il "regime". La misura del lavaggio si prende sul regime, non sul riempimento.
3. Tre disposizioni delle chiavi:
   - "oggi": come il motore (`doc/{coll}/{_id}` più gli indici);
   - "collezione": il rimedio di M21 (documento sotto `{coll}/{cliente}{ts}{_id}`, biglietto `_id` → chiave);
   - "cliente": cliente in testa a tutto (`{cliente}/{coll}/{ts}{_id}`), così tutte le collezioni di un cliente stanno vicine; biglietto `_id` → chiave; l'indice globale {stato, ts} resta a parte.
4. Strategie di lavaggio:
   - (a) quella di oggi: SlateDB size-tiered di default, disposizione "oggi";
   - (b) raggruppamento per cliente senza tetto;
   - (c) raggruppamento con tetto a 2, 4, 6 pile (`sorted_run_consolidation_threshold`);
   - (d) partizioni per intervalli di clienti con lavaggio indipendente (segmenti di SlateDB 0.17, RFC-0024), con tetto; e sulla carta/simulatore: livelli per età e lavaggio a livelli (leveled).
5. Misure per strategia e taglia, dal contatore del finto S3 e da getrusage:
   - amplificazione di scrittura = byte scritti dal lavaggio (scatole in `compacted/` meno lo scarico delle L0) / byte dell'utente (chiavi + valori scritti, compresi gli indici); riporto anche il totale con lo scarico (L0). Il WAL di SlateDB non conta: nel disegno lo sostituisce il quaderno;
   - GET e PUT del lavaggio (ogni parte di un caricamento multiparte è un PUT);
   - CPU del lavaggio per GB = (CPU del processo con il lavaggio − CPU dello stesso carico con il lavaggio spento) / GB scritti dal lavaggio;
   - pile a regime: media e massimo di (sorted run + L0) campionati durante il regime;
   - GET per find fredda tipica e per scaldare un cliente, alla fine.
6. Taglie: quanto la macchina regge (2 vCPU, 7 GB di RAM, circa 30 GB di disco libero). Previsto 0,5, 1, 2 e 4 GB di dati vivi; dichiaro nel rapporto quelle fatte davvero.
7. Proiezione a 20 TB: simulatore del lavaggio sui soli metadati (taglie delle pile), con la stessa logica dello schedulatore size-tiered di SlateDB 0.17, tarato sulle misure vere (pile e amplificazione alle taglie piccole). Lo stesso simulatore per leveled e livelli per età. GET per find a 20 TB = regola misurata (una GET per pila dove il cliente c'è) × pile simulate, con i filtri per prefisso che saltano le pile dove il cliente non c'è.
8. Soldi a 20 TB con 4.000 scritture al secondo: PUT $0,005 ogni 1.000, GET $0,0004 ogni 1.000 (DICHIARATO, NUMBERS.md), parti multiparte da 16 MB come B1, CPU su c7g spot (prezzo da cercare o STIMATO), costo di base da B13 (cucina condivisa) più le voci di scrittura di B1 profilo (a). Prezzo = costo / 0,55. Infinite: spazio 20 TB a $0,30/GB più calcolo per 4.000 scritture al secondo (prezzi DICHIARATI in B13).

## Cosa NON misura

- Non misura su S3 vero né latenze vere (solo i giri in fila, come M21).
- Le taglie misurate sono di GB, non di TB: la proiezione a 20 TB viene dal simulatore e lo dico ogni volta (STIMATO).
- Non costruisce il lavaggio nel motore: dice cosa manca e quanto costa farlo.
