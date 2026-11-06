**REGGE, di misura e con due condizioni: la strategia migliore sono le "pile a gradini" (livelli geometrici, T = 4), con un'amplificazione di scrittura di circa 29 a 20 TB (STIMATO; 14,1 MISURATO a 4 GB) e una find fredda tipica di circa 7,6 GET nel caso peggiore (cliente presente in tutte le pile) e circa 4 per un cliente qualsiasi (STIMATO a 20 TB; 1–4 GET MISURATE alla fine delle prove a gradini da 0,25 a 4 GB).** Il prezzo al cliente resta sotto il 47% di Infinite (circa 43%, forchetta 33–45%, STIMATO) solo se valgono due condizioni: le scritture viaggiano come nel disegno completo (dieta M13 e driver diretti M14) e il lavaggio usa richieste S3 grandi (16 MB in lettura, almeno 10 MB per parte in scrittura). Con le scritture di oggi o con le richieste di oggi si va al 50–53%. Il tetto alle pile che SlateDB ha già (2, 4 o 6 pile) non serve: a 20 TB o riscrive tutto il database a ogni lavaggio piccolo, o sotto carico non tiene il tetto (già a 1 GB scrive più delle pile a gradini e tiene più pile, MISURATO).

# M26 · Quanto costa il lavaggio che ordina i dati per cliente

Sessione M26 (Opus), lanciata da scheduled task il 2026-11-03. Tutto in locale, $0, nessun connettore personale. Branch `lavaggio-per-cliente` di `fenvix-engine` (da `granularita-freddi`), cartella `proto/lavaggio-per-cliente/`. Il motore non è stato toccato. Soglie e metodo scritti prima di misurare in `proto/lavaggio-per-cliente/SOGLIE.md` (commit 1ba4dad, il primo del branch).

Letti: `CLAUDE.md`, `RULES.md`, `DESIGN.md`, `DECISIONS.md` (ultima voce), `NUMBERS.md`, M21, revisore M25, revisore B13, B1 e il suo revisore, B13.

## In una riga

**Fatto.** Raggruppare i dati per cliente non costa lavaggio in più (MISURATO). Quello che costa è tenere poche pile su un database grande. Il tetto di SlateDB lo fa nel modo peggiore. Ho scritto un lavaggio nuovo, le "pile a gradini", che SlateDB accetta come schedulatore esterno senza toccare la libreria: a 20 TB tiene circa 9 pile con un'amplificazione di circa 29 e costa circa $460 al mese (STIMATO), cioè circa 10 punti del prezzo contro Infinite.

## Com'è fatta la prova (in cucina)

- **Il magazzino** (S3) è finto: una cartella sul disco con il contatore di M21, che conta ogni richiesta. In più conta ogni parte dei caricamenti multiparte come un PUT (S3 la fa pagare così) e separa le richieste sulle scatole (SST) da quelle sul foglio indice.
- **Le scatole e il lavaggio** sono quelli veri: SlateDB 0.17.0 come il motore, memtable e L0 da 8 MB, blocchi da 4 KB, nessuna compressione, filtri per prefisso sul cliente (l'etichetta sul coperchio della scatola che dice quali clienti ci sono dentro). Il WAL di SlateDB è spento: nel disegno lo sostituisce il quaderno (registro replicato a 3 zone).
- **Il carico "misto"**: clienti con attività zipf (s = 0,7), circa 50 documenti vivi per cliente in media, tre collezioni per cliente: storico chat (50% delle scritture, inserimenti), eventi per dispositivo (35%, inserimenti), ordini (15%, di cui un terzo aggiornamenti dello stato). Indice {cliente, ts} su ogni collezione e un indice globale {stato, ts} sugli ordini, che non si può raggruppare per cliente. Prima si riempie fino alla taglia, poi si continua a scrivere il doppio della taglia con una ritenzione: i documenti più vecchi si cancellano, così la taglia resta ferma (il "regime"). Le misure del lavaggio sono prese sul regime.
- **Tre disposizioni delle chiavi**: "oggi" (come il motore: documento sotto `_id`, indici a parte); "collezione" (il rimedio di M21: collezione, cliente, data); "cliente" (cliente in testa a tutto: cliente, collezione, data; così tutte le collezioni di un cliente stanno una dopo l'altra). Nelle ultime due un biglietto `_id` → chiave serve la lettura per `_id`.
- **Lo scrittore a ritmo fisso**: 2 MB/s di byte dell'utente (3 MB/s a 4 GB), così il lavaggio sta al passo come in produzione. Con lo scrittore a tutta velocità (prima serie, a 0,25 GB) la macchina finisce la CPU e le pile salgono per mancanza di lavaggio, non per la strategia: quelle misure le uso solo per la CPU.
- **La macchina**: 2 vCPU Intel a 2,1 GHz, 7 GB di RAM, circa 30 GB di disco libero. **Taglie fatte davvero: 0,25, 0,5, 1, 2 e 4 GB di dati vivi per le pile a gradini T = 4; 0,25–2 GB senza tetto; 0,25–1 GB per le altre** (la 4 GB con un regime di 1 volta la taglia invece di 2). Oltre non ci sta nel tempo di una sessione: a 2 MB/s la prova da 4 GB dura 75 minuti. 10–200 GB non li ho fatti: li proietto con il simulatore.
- **Il simulatore**: rifà sui soli metadati (taglie e finestre di tempo delle pile) la logica dello schedulatore size-tiered di SlateDB 0.17, riga per riga, e le pile a gradini. L'ho tarato sulle misure vere (punto 3) e lo uso per 20 TB.
- **Le GET**: alla fine di ogni prova misuro find fredde come M21 (60% ultimi 20 messaggi, 20% un documento per `_id`, 20% un intervallo di date sugli eventi) e "scaldare un cliente" (tutti i documenti delle sue tre collezioni), con il bancone S1 (indici dei blocchi e filtri caldi, blocchi di dati vuoti). Ogni risposta è controllata: utente, collezione, date e numero esatto di documenti. Zero errori.

## 1. Cosa ho misurato (in locale, carico misto, scrittore a 2 MB/s)

**(a) Il lavaggio di oggi con le chiavi di oggi.** Amplificazione del lavaggio (byte scritti dal lavaggio diviso byte dell'utente) 3,95 a 0,25 GB, uguale alla disposizione "cliente" (3,91) e "collezione" (3,91) (MISURATO). **Raggruppare per cliente non fa scrivere di più**: il lavaggio di SlateDB unisce sempre pile intere, qualunque sia l'ordine delle chiavi. Le letture invece sì: con le chiavi di oggi la find tipica costa 16,9 GET con 3 pile (Q1 = 23,2: 20 documenti più 3 pile) e scaldare un cliente **162 GET** in media, p90 241 (MISURATO). Con "collezione" scaldare un cliente costa 9,1 GET (tre collezioni per 3 pile); con "cliente" una GET per pila (MISURATO).

**(b) Raggruppato per cliente, senza tetto.** Amplificazione del lavaggio 3,9 a 0,25 GB, 4,2 a 0,5, 5,1 a 1, 5,8 a 2 GB (MISURATO). Cresce con la taglia, piano. Pile a regime (scatole L0 più sorted run): in media 6,9, 7,9, 8,6 e 9,3; p90 9, 10, 11 e 11; massimo 11, 12, 13 e 15 (MISURATO). Circa 3 di queste sono L0.

**(c) Raggruppato con il tetto di SlateDB.** A 0,25 GB: tetto a 2 pile amplificazione 14,0 e pile medie 5,6; tetto a 4: 11,1 e 7,5; tetto a 6: 5,0 e 7,9 (MISURATO). Già a 0,25 GB il tetto a 2 scrive 3,6 volte più di (b). A 1 GB il tetto a 4 scrive 13,4 e tiene in media 8,1 pile (MISURATO): più delle pile a gradini (11,0 e 6,3) su tutte e due le cose. Sotto carico SlateDB non rispetta il tetto (in media 4,5 sorted run con tetto 4): consolida solo quando non ha altro lavoro.

**(d) Le pile a gradini (livelli geometrici), il disegno nuovo.** In cucina: le scatole stanno in pile di taglia crescente con l'età, come cassetti di misure diverse: in alto le ultime ore, sotto la settimana, sotto il mese, in fondo tutto il resto. Quando una pila diventa grande almeno un quarto (T = 4) di quella sotto, si lavano insieme. Così le pile restano circa "logaritmo in base 4 della taglia" e il lavaggio non riscrive il fondo a ogni piccola novità. L'ho scritto come schedulatore esterno di SlateDB (`src/livelli.rs`, circa 120 righe): SlateDB 0.17 lo accetta senza modifiche alla libreria.
- T = 4: amplificazione 7,3 a 0,25 GB, 10,4 a 0,5, 11,0 a 1, 12,8 a 2, **14,1 a 4 GB** (MISURATO). Pile medie 5,2, 5,7, 6,3, 6,7 e **6,9**; p90 7, 7, 8, 8 e 8; massimo 8, 9, 9, 10 e 10 (MISURATO). Le pile crescono di circa mezza pila ogni volta che la taglia raddoppia. Una prima prova a 4 GB fatta insieme ad altre, con lo scrittore a 3 MB/s, ha dato 9,7 pile medie (massimo 16) con la stessa amplificazione (13,9): quando al lavaggio manca CPU le pile salgono subito.
- T = 8: amplificazione 8,6 a 0,25 GB e 14,6 a 1 GB; pile medie 5,4 e 5,8 (MISURATO).
- **Con partizioni per intervalli di clienti** (16 segmenti di SlateDB, lavaggio indipendente per segmento) e T = 8, a 0,25 GB: stessa amplificazione (8,8), ma 6 volte i PUT e 8 volte le GET del lavaggio, e lo scrittore non regge 2 MB/s (0,6 MB/s, MISURATO). Il motivo: la memtable da 8 MB si divide in 16 L0 da 0,5 MB. Le partizioni servono solo se ogni segmento scarica L0 grandi (punto 6).

**La regola delle GET regge anche con tre collezioni** (MISURATO in 21 prove su 21): con la disposizione "cliente" Q1, Q3 e "scaldare un cliente" costano una GET per pila dove il cliente c'è, Q2 costa 2 GET (biglietto e documento). Fino a 2 GB il cliente c'è in tutte le pile; a 4 GB, con 6–7 pile, Q1 costa 3,6–4,2 GET e scaldare 3,6–4,2, perché i filtri per prefisso saltano le pile piccole dove il cliente non ha scritto (MISURATO). Con "collezione", scaldare costa una GET per pila per collezione. Con le chiavi di oggi, una GET per documento.

**La CPU del lavaggio**: 18–28 secondi di CPU per GB scritto dal lavaggio, media 24,9 su 18 prove (MISURATO: CPU del processo con il lavaggio meno CPU dello stesso carico con il lavaggio spento). Nei conti uso 24,5. Vuol dire circa 40 MB/s scritti per vCPU, su questa macchina, senza compressione.

**Le richieste S3 del lavaggio**: con le impostazioni di oggi di SlateDB un PUT porta circa 7 MB e una GET circa 1,2 MB (MISURATO). Per GB dell'utente: (b) 770–970 PUT e 4.400–5.100 GET; (d) T = 4 1.440–2.020 PUT e 6.600–9.350 GET (MISURATO). Le letture del lavaggio si possono allargare (`bytes_to_fetch` di SlateDB, oggi 2 MB) e i PUT anche (taglia delle parti: oggi circa 10 MB nella libreria `object_store`, più grande con una piccola modifica).

## 2. Il tetto di SlateDB a 20 TB: non serve

Il tetto (`sorted_run_consolidation_threshold`) unisce le pile più nuove con quelle sotto finché non sono K. Su un database grande, a ogni nuova pila piccola rifà anche la pila del fondo. Nel simulatore, con L0 da 256 MB e 7,6 MB/s di scritture: tetto a 2 pile amplificazione circa 39.000, a 4 circa 19.500, a 6 circa 9.700 (STIMATO). Il simulatore fa rispettare il tetto sempre; SlateDB vero lo rispetta solo quando il lavaggio non ha altro da fare (a 1 GB, tetto 4: 4,5 sorted run in media e amplificazione 13,4, MISURATO, contro 33 simulato). Su 20 TB restano due possibilità, tutte e due cattive: o il tetto si rispetta e il fondo si riscrive di continuo, o non si rispetta e le pile non scendono. È il "tetto a 2 pile scrive 2,2 volte" di M21 visto a 20 TB. **Non va usato nel motore per i database grandi.**

Ho provato anche un tetto solo sulle pile grandi (le piccole libere): a 20 TB amplificazione 45 con circa 10 pile (STIMATO). Peggio delle pile a gradini su tutte e due le cose.

## 3. Quanto torna il simulatore con le misure

- **Amplificazione**: il simulatore dà (vero / simulato) 3,9 / 3,6, 5,1 / 5,9 e 5,8 / 6,2 per (b); 7,3 / 8,5, 11,0 / 12,1 e 12,8 / 14,1 per T = 4, più 10,4 / 10,7 a 0,5 GB e 14,1 / 16,1 a 4 GB; 8,6 / 10,2 e 14,6 / 16,7 per T = 8 (MISURATO e simulato). Per le pile a gradini il simulatore scrive il 3–17% in più del vero: è prudente. Per il tetto di SlateDB sbaglia di più (da −29% a +22%).
- **Pile**: le pile vere sono 2,1–2,8 in più di quelle simulate in 13 prove su 14 (MISURATO, da 0,25 a 4 GB, senza crescere con la taglia); 5,3 in più nella prova a 4 GB fatta con poca CPU. Circa 1,6 sono L0 che aspettano il lavaggio (il simulatore le lava subito), circa 1 è una sorted run in più. Nei conti a 20 TB aggiungo quindi **+2,6 pile** a quelle simulate. È prudente: a 20 TB una L0 arriva ogni 34 secondi, non ogni 3, e l'attesa pesa meno.

## 4. Proiezione a 20 TB, 4.000 scritture al secondo

Ipotesi (STIMATO): 20 TB logici, 4.000 scritture al secondo da 1,9 KB (B1), cioè 7,6 MB/s e circa 20 TB al mese dell'utente; ritenzione che tiene ferma la taglia (ogni dato vive circa 32 giorni); L0 da 256 MB (scarico ogni 34 secondi; con 64 MB o 1 GB i risultati sono vicini e sono in `risultati/proiezione-20tb.jsonl`); 10 milioni di clienti con attività zipf come nel carico misurato; lavaggio con capacità sufficiente (con meno capacità le pile salgono: punto 6).

GET per find: "caso peggiore" = cliente presente in tutte le pile (come nelle prove, dove ogni cliente scritto scrive ogni giorno); "cliente qualsiasi" = media sulla popolazione: un cliente poco attivo non c'è nelle pile piccole e recenti, e il filtro per prefisso le salta senza GET; "dormiente" = cliente che non scrive da 7 giorni.

**(a) Oggi** (lavaggio di oggi, chiavi di oggi): amplificazione circa 12,6, pile circa 14 (STIMATO); find tipica circa 25–30 GET (20 documenti più le pile, STIMATO dalla regola misurata); scaldare un cliente centinaia di GET.

**(b) Raggruppato, senza tetto**: amplificazione 12,6; pile circa 14 (massimo simulato 23, più le 2,6); find peggiore **11,8 GET**, cliente qualsiasi **6,5**, dormiente circa 2; scaldare un cliente p90 **circa 18** (tutto STIMATO). Scrive circa 250 TB al mese. Costo del lavaggio circa **$280 al mese** con richieste grandi, $420 con quelle di oggi (STIMATO).

**(c) Tetto a 2, 4, 6 pile**: non serve (punto 2).

**(d) Pile a gradini**:
- **T = 4**: amplificazione **29,4**; pile circa 9 (massimo simulato 11, più le 2,6); find peggiore **7,6 GET**, cliente qualsiasi **circa 4**, dormiente circa 1,2; scaldare un cliente p90 **circa 12** (tutto STIMATO). Scrive circa 590 TB al mese. Lavaggio: CPU media 5,5 vCPU (8 con il margine), circa $115 al mese su macchine spot; PUT circa $300 (parti da 10 MB) o $430 (richieste di oggi); GET $16 (letture da 16 MB) o $210 (di oggi); spazio in più su S3 circa $33. **Totale circa $460 al mese, $790 con le richieste di oggi** (STIMATO).
- T = 6: amplificazione 36,6, find peggiore 7,1, qualsiasi 3,7, lavaggio circa $560 (STIMATO).
- T = 8: amplificazione 42,2, find peggiore 6,8, qualsiasi 3,4, lavaggio circa $630 (STIMATO).
- T = 3: amplificazione 26,5 ma find peggiore 8,2: sopra la soglia (STIMATO).

Quanto è sensibile al numero di clienti (STIMATO): con 1 milione di clienti invece di 10, il cliente qualsiasi costa circa 4,9 GET con T = 4 e 8,3 con (b); con 100 milioni circa 3,3 e 4,8. Il caso peggiore non cambia.

## 5. I soldi: prezzo al cliente contro Infinite

Il conto (tutto STIMATO su prezzi DICHIARATI in `NUMBERS.md`, B1 e B13, script `scripts/prezzo_finale.py`):
- Base: cucina condivisa di B13 sul profilo da 20 TB, $630 meno le sue GET ($48).
- Scritture a 4.000 al secondo, voci del revisore di B1 (profilo a, stesse scritture): quaderno $226, CPU dei camerieri per le scritture circa $120, scarico $18, punto nel tempo $29; traffico tra zone $412 e portiere circa $195 con il disegno di oggi, oppure $165 e $0 con la dieta (M13, circa −60% di byte tra zone, MISURATO) e i driver diretti (M14).
- Bancone per indici dei blocchi e filtri di tutto il magazzino: fino a $105 (M21).
- GET delle find fredde: 35 al secondo (profilo B), $36 al mese per ogni GET per find.
- Il lavaggio del punto 4.
- Prezzo = costo / 0,55. Infinite: 20–23 TB a $0,30 più M50 Gen2 ($1.978, basta per 4.000 scritture al secondo secondo B1) o M60 Gen2 ($3.954): **$7.980–10.850, centro $8.430**.

Risultati con le pile a gradini T = 4, find nel caso peggiore:
- **Disegno completo (dieta e driver diretti), richieste grandi: costo circa $1.990, prezzo circa $3.620, cioè il 43% di Infinite al centro (33–45%).** Sotto il 47%.
- Disegno completo con le richieste di oggi: prezzo circa $4.200, **50%** (39–53%).
- Scritture come oggi, richieste grandi: circa $4.420, **52%** (41–55%).
- Senza contare il lavaggio, il disegno completo sarebbe al 33%: **il lavaggio a gradini vale circa 10 punti di Infinite.**
- Con il cliente qualsiasi invece del caso peggiore: 40% (31–42%).

Per confronto: (b) senza tetto, disegno completo e richieste grandi, sta al 42% ma con 11,8 GET nel caso peggiore; T = 8 sta al 46% (36–49%).

Il prezzo di questo profilo lo fanno soprattutto le scritture (quaderno, tra zone, portiere), non il lavaggio: con 4.000 scritture al secondo il disegno di oggi è già al 42% senza lavaggio.

## 6. Cosa propongo per il motore

**Le pile a gradini, T = 4 come partenza (manopola per database tra 4 e 6), con questi pezzi:**
1. **Cliente in testa alla chiave** in tutte le collezioni e negli indici che cominciano con il cliente: `{cliente}/{collezione}/{chiave}`; biglietto `_id` → chiave; gli indici globali (che non cominciano con il cliente) restano a parte e costano ancora una GET per documento. Scaldare un cliente diventa una scansione sola per pila.
2. **Filtri per prefisso sul cliente** (già in SlateDB 0.17, usati qui): un cliente fermo da giorni non paga le pile piccole e recenti.
3. **Lo schedulatore a gradini** di questa prova, in un compattatore a parte (SlateDB lo permette), su macchine spot fuori dai camerieri.
4. **L0 grandi** (256 MB o più): meno gradini, meno riscritture. Per un database grande l'aiutante scarica dal quaderno a blocchi grandi.
5. **Partizioni per intervalli di clienti** (segmenti di SlateDB) solo per i database grandi, e solo con L0 grandi per segmento: servono a spezzare il lavaggio del fondo (20 TB in un pezzo solo vogliono ore) in tanti pezzi paralleli. Nel simulatore, con una capacità di lavaggio di 300 MB/s invece di "abbastanza", le pile medie salgono da 6,4 a 8,7 (STIMATO): serve una flotta che si allarga quando lava il fondo.
6. **Richieste grandi**: letture del lavaggio da 16 MB (`bytes_to_fetch`, impostazione) e parti dei PUT da almeno 10 MB, meglio 64 MB (piccola modifica). Valgono circa $320 al mese su 20 TB (STIMATO).
7. **Mai il tetto di SlateDB** sui database grandi.

**Cosa manca per costruirla bene (STIMATO in sessioni Claude Code):**
- Cliente in testa nel motore (`keyenc`/`layout`, come si dichiara il campo cliente per collezione, biglietto, indici per cliente dentro il prefisso, aggiornamenti che cambiano il cliente come cancella più inserisci): **3–5 sessioni**.
- Lavaggio a gradini nel motore, con compattatore separato, regole di sicurezza (solo pile vicine, mai una pila in uso), segmenti con L0 per segmento: **3–4 sessioni**.
- **Indici coerenti durante il lavaggio**: documento, voci d'indice e biglietto vanno nello stesso lotto di scrittura (stesso numero di sequenza), il lavaggio unisce solo pile vicine e intere, il lettore legge a una fotografia (snapshot). Così un indice non può puntare a un documento che "non c'è ancora" o "non c'è più" in quella fotografia. Va provato con un test a proprietà (in ogni momento, durante e dopo ogni lavaggio, ogni voce d'indice punta a un documento vivo e viceversa) e con test di crash a metà lavaggio: **2–3 sessioni**.
- **Cancellazione mirata per legge** (un cliente chiede di sparire): con il cliente in testa tutti i suoi dati sono un intervallo di chiavi. SlateDB 0.17 non ha la cancellazione per intervallo, ma ha i filtri del lavaggio (`CompactionFilter`): un registro dei clienti cancellati, letto anche dal lettore (che li nasconde subito), e il lavaggio che li butta via quando riscrive le pile. Con i gradini il fondo si rilava ogni circa 1–2 settimane a 4.000 scritture al secondo (STIMATO); se serve prima, si rilava solo il segmento del cliente. Più la regola per le copie del punto nel tempo: **2–3 sessioni**.
- Prova grande: 50–200 GB su una macchina più grossa o una giornata di prova su AWS (serve il sì del fondatore), e la CPU su Graviton con compressione: **1–2 sessioni**.
- Revisione indipendente: **1 sessione**.
- **Totale circa 12–18 sessioni** (STIMATO).

## 7. Contro la soglia

- REGGE chiede: find fredda tipica sotto 8 GET e scaldare un cliente sotto 25 a 20 TB proiettati, con il prezzo sotto il 47% di Infinite. Pile a gradini T = 4: **7,6 GET nel caso peggiore, circa 4 per il cliente qualsiasi; scaldare p90 circa 12; prezzo 43% (33–45%)** con il disegno completo e le richieste grandi (STIMATO). → **REGGE**, di misura sul caso peggiore (7,6 contro 8) e sul prezzo (43 contro 47).
- Se le due condizioni non valgono (scritture come oggi o richieste di oggi): prezzo 50–52% → FERITA. Rimedio: la dieta e i driver diretti sono già nel disegno; le richieste grandi sono un'impostazione e una piccola modifica. Prova successiva: punto 9.
- MUORE chiede: nessuna strategia sotto 15 GET senza superare il 47%. No: anche (b) senza tetto sta a 11,8 nel caso peggiore e al 42%.
- Probabilità che il verdetto sia sbagliato: **circa 35–45%** (STIMATO). I rischi: le misure arrivano a 4 GB e 20 TB è una proiezione; la CPU è misurata su Intel senza compressione; il prezzo dipende dalle voci delle scritture di B1, che non ho rifatto; il margine sul caso peggiore è piccolo.

## 8. Cosa ho deciso io

- Ho usato la ritenzione (i più vecchi si cancellano) per tenere ferma la taglia: senza, a 4.000 scritture al secondo un database da 20 TB raddoppia in un mese. Con la ritenzione nessun cliente è davvero dormiente nelle prove: ogni cliente scelto è presente in tutte le pile. È il caso peggiore per le GET; il cliente qualsiasi e il dormiente vengono solo dal simulatore.
- Ho spento il WAL di SlateDB (nel disegno c'è il quaderno) e contato a parte lo scarico delle L0: l'amplificazione del lavaggio non lo comprende; con lo scarico si aggiunge circa 1,3.
- Lo scrittore a ritmo fisso: senza, la macchina da 2 vCPU non tiene il passo e le pile misurano la CPU che manca. Le prove a tutta velocità (prima serie, a 0,25 GB) restano in `risultati/` e le uso solo per la CPU. Le prime due prove a ritmo fisso le ho rifatte dopo aver portato il controllo del lavaggio da 5 a 0,5 secondi (le vecchie sono in `risultati/*-poll5s.jsonl`).
- Lo schedulatore a gradini l'ho scritto io: non è nel motore e non tocca SlateDB.
- Una prova da 2 GB è finita in errore dopo 50 minuti (un file tolto dal raccoglitore mentre lo ripulivo); l'ho corretta e rifatta.
- Ho fatto alcune prove in parallelo (scrittori a ritmo fisso, CPU libera): possono aver aggiunto qualche L0 in attesa. La prova a 4 GB l'ho rifatta da sola dopo aver visto l'effetto (punto 1). La 4 GB senza tetto l'ho fermata per fare posto a questa: non c'è.

## 9. Cosa manca o blocca

- Taglie tra 10 GB e 20 TB: solo simulatore. La prova più economica che deciderebbe: 50–200 GB di carico misto con le pile a gradini, su una macchina con più disco e CPU (in locale, $0, se c'è; altrimenti una giornata di prova AWS, qualche dollaro, serve il sì del fondatore). Mi fa cambiare idea: pile medie sopra 9,5 o amplificazione sopra 40 a 200 GB.
- La CPU su Graviton, con compressione dei blocchi: NON SO. La compressione dimezza i byte (e i PUT) ma costa CPU.
- Il lavaggio del fondo a 20 TB in tempo vero (ore) con una flotta spot: solo simulato.
- Le voci delle scritture (quaderno, tra zone, portiere) a 4.000 scritture al secondo le ho prese da B1 e dal suo revisore senza rifarle: sono loro a decidere tra 43% e 52%.

## 10. Link

- Branch: `lavaggio-per-cliente` di `fenvix-engine`, PR in bozza: https://github.com/Mircoguidetti/fenvix-engine/pull/48
- Soglie e metodo: `proto/lavaggio-per-cliente/SOGLIE.md`
- Codice: `src/main.rs` (carico, misura), `src/livelli.rs` (pile a gradini), `src/conta.rs` (contatore con le parti multiparte), `src/bancone.rs` (di M21)
- Simulatore: `scripts/sim.py`; proiezione delle GET: `scripts/proietta.py`; soldi: `scripts/costi.py`, `scripts/prezzo_finale.py`
- Risultati: `risultati/carichi.jsonl`, `risultati/misure.jsonl`, `risultati/riassunto.json` (da `scripts/riassunto.py`), `risultati/sim-piccole.jsonl`, `risultati/proiezione-20tb.jsonl`, `risultati/prezzo-20tb.jsonl`, `risultati/sensibilita-clienti.txt`
- Come si rifà: `proto/lavaggio-per-cliente/README.md`

## 11. Errori o mancanze trovati in `fenvix-brain`

- `DECISIONS.md`, ultima voce: "il tetto alle pile (opzione che SlateDB ha già)" non è la strada per i database grandi: a 20 TB riscrive tutto a ogni lavaggio. Va scritto "lavaggio a gradini (schedulatore nostro)".
- `NUMBERS.md`: aggiungere "raggruppare per cliente non cambia l'amplificazione del lavaggio (MISURATO M26)", "CPU del lavaggio circa 24,5 s per GB scritto, Intel 2,1 GHz, senza compressione (MISURATO M26)", "richieste del lavaggio con le impostazioni di oggi: PUT circa 7 MB, GET circa 1,2 MB (MISURATO M26)", "20 TB, 4.000 scritture/s: lavaggio a gradini circa $460 al mese, circa 10 punti del prezzo contro Infinite (STIMATO M26)".
- `NUMBERS.md` e B1: il lavaggio di B1 ($56 di CPU più $14 di richieste a 1 TB con amplificazione 4) è troppo basso per un database grande che tiene poche pile: a 20 TB l'amplificazione è 13 senza tetto e circa 29 con i gradini.
- `DESIGN.md`, tappa 8: aggiungere che il lavaggio dei database grandi tiene le pile a gradini, gira su un compattatore a parte e usa richieste grandi.
