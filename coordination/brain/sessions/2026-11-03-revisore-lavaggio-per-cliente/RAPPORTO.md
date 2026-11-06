**Find fredda a 20 TB, caso peggiore: 7,6 GET solo con il lettore regolato (12,7 GET con le impostazioni del lettore usate da M26, quando i clienti sono grandi come a 20 TB); prezzo 43% di Infinite (47% con il lettore di M26; circa 37–40% con le pile recenti sul bancone e la compressione).** Tutti numeri STIMATI su misure locali. Verdetto: **VIVA, con correzioni.** Probabilità che sia sbagliata: circa 25–35% (STIMATO).

# Revisore di M26 · Il lavaggio che ordina i dati per cliente

Sessione revisore (Opus), lanciata da scheduled task il 2026-11-03. Non ho scritto M26. Tutto in locale, $0, nessun connettore personale, nessun merge. Branch mio: `rev-lavaggio-per-cliente` di `fenvix-engine` (nato da `lavaggio-per-cliente`, che non ho toccato). Le previsioni le ho scritte prima delle misure in `proto/lavaggio-per-cliente/rev/PREVISIONI.md` (primo commit del branch).

Letti: `CLAUDE.md`, `RULES.md`, `PLAN-LAVAGGIO.md`, `DECISIONS.md` (voci del 2026-11-03), il rapporto M26 intero, M21, B1 e il suo revisore, B13. Codice: tutto il prototipo M26 (`main.rs`, `livelli.rs`, `sim.py`, `proietta.py`, `costi.py`, `prezzo_finale.py`) e, di SlateDB 0.17.0, lo schedulatore size-tiered, i filtri, la validazione del lavaggio, i lavoratori distribuiti e la ripresa dopo un crash.

## In una riga

**Fatto.** Le misure di M26 tornano (rifatte a 1 GB) e il simulatore regge anche a 8 GB, il doppio della prova più grande di M26. Ho trovato un errore provato: la regola "una GET per pila" vale solo per clienti piccoli. Con clienti da 2 MB, che è la taglia di un cliente medio a 20 TB con 10 milioni di clienti, il lettore di M26 fa il 60% di GET in più nelle find e circa 8 volte di più per scaldare un cliente. Si corregge con due impostazioni del lettore (MISURATO). Ho trovato anche una strada migliore: tenere sul bancone le pile piccole e recenti, che costano poco spazio.

## 1. Le prove rifatte (stessa macchina, stesso codice, stessi parametri)

In cucina: ho rifatto due giornate di lavoro della cucina con le stesse ricette e ho contato di nuovo quanto lavaggio serve e quante pile restano.

- **1 GB, pile a gradini T = 4, scrittore a 2 MB/s.** Amplificazione del lavaggio (byte riscritti dal lavaggio diviso byte dell'utente) **11,1** contro 11,0 di M26. Pile medie **6,1** contro 6,3; p90 8 e massimo 9, uguali. PUT 1.857 per GB dell'utente contro 1.843, GET del lavaggio 8.543 contro 8.485. Alla fine, con 2 pile, la find tipica costa 2,02 GET e scaldare un cliente 2,24 (tutto MISURATO).
- **1 GB senza tetto.** Amplificazione **5,07**, uguale a M26. Pile medie **8,3** contro 8,6; p90 10 contro 11; massimo 13, uguale. Alla fine, con 5 pile, find 4,46 GET e scaldare 5,6 (MISURATO).
- Con i clienti delle prove di M26 (circa 22 KB di dati ciascuno) la regola "una GET per pila dove il cliente c'è" torna anche nelle mie prove: Q1 5,07 GET con 5 pile, Q2 2,09 (biglietto più documento) (MISURATO).

Le misure di M26 sono giuste. Nota: delle 6,1 pile a 1 GB, 3,0 sono L0 (scatole appena scaricate che aspettano il lavaggio), non 1,6 come scrive M26 (MISURATO). Non cambia la correzione totale.

## 2. Il simulatore e la prova più grande (8 GB)

**Il codice.** Ho letto lo schedulatore size-tiered di SlateDB 0.17 (`size_tiered_compaction.rs`) e il simulatore `sim.py` riga per riga. Il simulatore rifà le stesse regole: almeno 4 L0 per fare una pila nuova (al massimo 8, si tengono le più vecchie), pile vicine di taglia simile (soglia 4 volte, da 4 a 8 sorgenti), il controllo di contropressione, il tetto, al massimo 4 lavaggi insieme. Una differenza piccola: il simulatore ricalcola la contropressione a ogni passo, SlateDB una volta per giro. Le pile a gradini del simulatore sono le stesse di `livelli.rs`. Rifatta la proiezione a 20 TB: amplificazione 29,4 e 6,4 pile simulate, come M26.

**La prova da 8 GB** (8 GB di dati vivi, T = 4, scrittore a 2 MB/s, regime di 1 volta la taglia, 2 ore e 25 minuti, circa 10 GB sul disco). Prima di misurare ho scritto la previsione: 7,2 pile se la correzione di M26 (+2,6) non cresce con la taglia.
- Pile medie **7,20**, p90 9, massimo 11 (MISURATO). Il simulatore dava 4,58: la differenza è **2,62**, come la correzione di M26.
- Amplificazione **16,0** (MISURATO) contro 18,2 simulata: il simulatore resta prudente (scrive il 12% in più del vero).
- Alla fine, con 4 pile: find tipica 2,21 GET, scaldare 2,42 (MISURATO).

Quindi la correzione +2,6 regge da 0,25 a 8 GB, cioè su 5 raddoppi. Da 0,25 a 2 GB la differenza era salita da 2,0 a 2,7; da 2 a 8 GB è ferma tra 2,6 e 2,7 (MISURATO). Restano 11 raddoppi fino a 20 TB: lì è solo simulatore (STIMATO).

**Il lavaggio del fondo a 20 TB.** Il simulatore di M26 dà al lavaggio "capacità infinita". Con un tetto di lettura del lavaggio: 300 MB/s danno 8,7 pile simulate (11,3 con la correzione, find peggiore 9,4), 1.000 MB/s danno 6,8 (find 7,9), 3.000 MB/s 6,5 (STIMATO, `sim.py --cap-mbs`). Serve quindi circa 1 GB/s di lettura quando si lava il fondo, cioè 4 volte la media (233 MB/s). Buona notizia che M26 non dice: **SlateDB 0.17 ha già i lavoratori del lavaggio distribuiti** (RFC-0025, `compaction_worker.rs`: più macchine prendono i lavori, battito ogni pochi secondi, un lavoro abbandonato viene ripreso da un altro) **e le sottoparti di un lavaggio in parallelo** (RFC-0028, `max_subcompactions`, di serie 4). Per spezzare il lavaggio del fondo in pezzi paralleli non servono le partizioni per intervalli di clienti, che nella prova di M26 moltiplicavano i PUT per 6 e frenavano lo scrittore.

## 3. Le GET a 20 TB: l'errore di granularità

**L'errore provato.** Nelle prove di M26 un cliente ha in media circa 50 documenti, circa 22 KB: in ogni pila i suoi dati stanno in un solo pezzo letto in anticipo (read-ahead) da 64 KB. A 20 TB con 10 milioni di clienti, un cliente medio ha circa 2 MB di dati (20 TB diviso 10 milioni). La regola "una GET per pila" non era mai stata provata con clienti così.

In cucina: se il cliente ha un cassetto piccolo, basta aprirlo una volta per pila. Se ha un armadio intero, il cameriere di M26 apre quattro ante insieme anche quando gli servono solo le prime 20 cose, e per prendere tutto apre un'anta piccola alla volta.

La prova: 0,5 GB con 250 clienti, cioè circa 2 MB per cliente, pile a gradini T = 4, stesso carico e stesso scrittore (MISURATO, due prove uguali):
- **Con il lettore di M26** (read-ahead 64 KB, 4 letture in anticipo per pila): con 5 pile la find tipica costa **7,0–7,2 GET** invece di 4,4. Q1 (ultimi 20 messaggi) 8,8–9,2 GET invece di 5. Q3 6,4. **Scaldare un cliente 42–43 GET, p90 85–89**, invece di 5.
- **Con una lettura in anticipo per pila** (`max_fetch_tasks` = 1) le find tornano alla regola: Q1 **5,0**, Q3 **5,0**, find **4,41** (MISURATO).
- **Per scaldare** serve leggere grande: read-ahead 1 MB 6,7 GET, 4 MB 5,2, 16 MB **5,0**, cioè una per pila (MISURATO). Costa solo byte letti, che su S3 nella stessa regione non si pagano.

Proiezione a 20 TB, caso peggiore (9 pile), STIMATO dalle misure per pila: con il lettore di M26 Q1 circa 16,6, Q3 circa 11,6, **find circa 12,7 GET**, sopra la soglia di 8; scaldare un cliente medio circa 40–80 GET, sopra la soglia di 25. Con il lettore regolato: **find 7,6 e scaldare p90 circa 12**, i numeri di M26.

Quindi i numeri finali di M26 sono giusti solo se il lettore usa una lettura in anticipo per pila nelle find e letture grandi (4–16 MB) quando scalda un cliente. È un'impostazione, ma va scritta nel piano. Va controllato anche il motore vero (`coll.rs`): M21 dice read-ahead 64 KB; quante letture in anticipo fa: NON SO, non l'ho guardato.

**Il resto della regola delle GET:**
- **Filtri per prefisso** (l'etichetta sul coperchio con i clienti dentro): in SlateDB 0.17 il filtro tiene sia il cliente sia la chiave intera (`whole_key_filtering` acceso di serie, `filter.rs`). Quindi il biglietto `_id` costa una GET anche con `_id` non in ordine di tempo (UUID, stringhe), non solo con gli ObjectId. Q2 = 2 GET regge (MISURATO 2,04–2,09 in tutte le mie prove).
- **Il filtro è sul cliente, non su cliente e collezione.** Una find sui messaggi paga anche le pile dove il cliente ha scritto solo eventi. Il conto di M26 lo fa già così (presenza del cliente in qualsiasi collezione): è prudente.
- **Il caso peggiore 7,6 contro 8** è la media nel tempo. Nel momento peggiore (pile al p90 nel simulatore, 9 più 2,6) il cliente presente in tutte le pile paga circa 9,7 GET (STIMATO). La soglia parla di find tipica, quindi non è un errore, ma va detto.
- **Il cliente qualsiasi.** M26 fa la media uniforme sui clienti. Se un cliente legge in proporzione a quanto scrive, la media pesata sulle letture è un po' più alta: 4,45 GET invece di 4,05 con 10 milioni di clienti, 5,46 invece di 4,9 con 1 milione, 3,62 invece di 3,25 con 100 milioni (STIMATO, `rev/qualsiasi_pesato.py`). Lontano dalla soglia in tutti e tre i casi.
- **Indici globali** (quelli che non cominciano con il cliente, come {stato, ts} sugli ordini): una GET per documento, come oggi. Non sono nella find tipica di M26. Se il 10% delle find usa un indice globale con 20 risultati, la find tipica sale di circa 2 GET (STIMATO). È un rischio da misurare sulle app vere, non un errore.
- **Indici per cliente non in ordine di tempo** (per esempio {cliente, stato}): con un cliente da 2 MB, 20 documenti presi da un indice non in ordine di data sono sparsi nel suo armadio: fino a una GET per documento per pila, o una lettura grande. Non provato da nessuno (rischio).
- **Bancone per gli indici dei blocchi e i filtri:** M26 mette $105, che in M21 è il caso con blocchi da 64 KB; le GET però sono misurate con blocchi da 4 KB, dove M21 dà $65–230. Con blocchi da 64 KB le GET per pila non cambiano, quindi va bene, ma il motore deve usare blocchi da 64 KB (STIMATO: fino a +$125, cioè 2,7 punti, se restano 4 KB).

## 4. I soldi

Ho rifatto il conto di M26 a mano, con le stesse voci (STIMATO su prezzi DICHIARATI in `NUMBERS.md`, B1 e B13):
- Lavaggio: 29,4 volte 20 TB al mese, cioè circa 590 TB scritti. PUT con parti da 10 MB circa $300; GET con letture da 16 MB circa $16; CPU 5,5 vCPU di media con margine 1,5 su spot a $0,0192 per vCPU-ora circa $115; spazio in più circa $33. **Totale $464**, come M26.
- Base B13 $582, scritture con dieta e driver diretti $558, bancone $105, GET delle find 7,6 per 35 al secondo circa $281, lavaggio $464: **costo $1.990, prezzo $3.620, 43% di Infinite al centro ($8.430)**. Torna con M26.
- Con il lettore di M26 (12,7 GET): costo circa $2.175, **prezzo circa 47%** (STIMATO).
- Una piccola doppia voce: la base B13 contiene già $40 di traffico tra zone, e M26 aggiunge quello delle scritture. Vale meno di un punto.
- Il profilo è un ibrido: spazio e letture del profilo B di B13 (500 operazioni al secondo), scritture del profilo (a) di B1 (4.000 scritture al secondo). Va bene come caso di prova, ma non è un cliente misurato.

**Le due condizioni.**
- **Richieste grandi:** realistiche. `bytes_to_fetch` è un'opzione del compattatore di SlateDB 0.17 (`compactor_executor.rs`); le parti dei PUT sono 10 MB di serie in `object_store` 0.14 (`BufWriter`, `buffered.rs`), più grandi con una modifica piccola. Con parti da 64 MB i PUT del lavaggio scendono da circa $300 a circa $47 (STIMATO).
- **Dieta M13 e driver diretti M14:** non le ho rifatte. Sono loro a decidere tra 43% e 52%, come dice M26. Da M26 a oggi niente le smentisce; restano condizioni di disegno, non misure su questo profilo.

**La compressione dei blocchi.** I valori del prototipo sono byte casuali: non si comprimono, quindi questa prova non può misurarla. Con documenti veri compressi 2 volte (l'ipotesi di B13) il lavaggio scrive e legge la metà dei byte: PUT da $300 a circa $150, GET da $16 a $8. La CPU per comprimere e decomprimere aggiunge circa il 6–11% (STIMATO da velocità DICHIARATE nel README di zstd: lz4 e zstd livello 1 comprimono a circa 500–700 MB/s per core e decomprimono a 1,5–4 GB/s, contro i circa 40 MB/s per vCPU del lavaggio). Netto circa −$145 al mese, cioè circa −3 punti (STIMATO). Con blocchi da 4 KB lz4 comprime peggio: un motivo in più per blocchi da 64 KB.

## 5. La sicurezza del lavaggio a gradini

- **Lava solo pile vicine e intere?** Sì nel codice (`livelli.rs`): unisce due pile una accanto all'altra, intere, nella più vecchia; le L0 tutte insieme in una pila nuova. Ma **lo schedulatore non ha il controllo `validate`**: in SlateDB 0.17 il controllo "sorgenti consecutive, destinazione la pila più vecchia" sta nello schedulatore size-tiered, non nel compattatore (`compactor.rs` controlla solo che le sorgenti esistano e l'ordine delle L0). Se un giorno arriva un lavoro sbagliato, nessuno lo ferma. Rimedio: copiare `validate` da `size_tiered_compaction.rs` (righe 297–360). Rischio, non errore: con il codice di oggi non succede.
- **Letture in corso:** il lavaggio non cancella mai una scatola; le toglie il raccoglitore (GC) solo dopo che nessun lettore la tiene (checkpoint del lettore, età minima). È il meccanismo normale di SlateDB; né M26 né io l'abbiamo provato sotto lavaggio a gradini.
- **Rami (fork):** un ramo tiene un checkpoint delle scatole del padre. Il lavaggio del padre non rompe il ramo, ma le scatole vecchie restano finché il ramo vive: con 20 TB e rami lunghi, lo spazio su S3 può raddoppiare (STIMATO). Da mettere nel modello TLA+ (M28) e nel prezzo dei rami.
- **Il compattatore separato muore a metà:** SlateDB 0.17 salva i pezzi già scritti di un lavaggio e un lavoratore nuovo riparte da lì (`ResumingIterator`, battito con scadenza di serie 30 s). Il rischio vero è un altro: se il coordinatore del lavaggio resta fermo, le L0 si accumulano e a 8 L0 (`l0_max_ssts` di serie) **lo scrittore si ferma**. Con L0 da 256 MB ogni 34 secondi sono circa 3–5 minuti (STIMATO). Serve un compattatore di riserva pronto e un allarme. Non provato né da M26 né da me: è la prova di crash del piano.

## 6. Una strategia migliore: le pile recenti sul bancone

In cucina: le pile a gradini sono piccole in alto e grandi in fondo. Le pile in alto, tutte insieme, pesano poco. Se le teniamo sul bancone (NVMe dei camerieri), una find fredda va sul magazzino solo per le 2–3 pile grandi in fondo.

Si può fare con quello che è già deciso: il lavaggio porta le scatole nuove nei banconi prima di pubblicarle (DECISIONS 2026-11-02). Basta dire "le pile sotto una certa taglia restano sul bancone".

Conto sulle fotografie del simulatore a 20 TB (STIMATO, `rev/pile_sul_bancone.py`; aggiungo 1 pila di correzione, le L0 sono già sul bancone):
- **T = 4, 200 GB sul bancone:** restano su S3 2,7 pile (p90 3), più 1: **find peggiore circa 3,4 GET invece di 7,6.** Risparmio sulle GET circa $155 al mese; NVMe circa $34 per copia ($0,17 per GB, M21). Netto circa −$120 al mese con una copia: **prezzo circa 40%** (41,7% con tre copie, una per zona).
- **Con anche la compressione:** circa **37%**.
- **Senza tetto (amplificazione 12,6) e 800 GB sul bancone:** restano 3,9 pile (p90 6), find peggiore circa 4,3 GET, lavaggio circa $280 invece di $464. Prezzo simile al caso T = 4 con 200 GB, ma con più bancone e più pile nel momento peggiore. Tengo T = 4 come partenza.

Gli indici dei blocchi per cliente, cioè una lettura sola che va dall'inizio alla fine del cliente nella scatola usando l'indice dei blocchi già sul bancone, sono la versione pulita del read-ahead grande: una GET per pila a ogni taglia di cliente, senza leggere byte inutili. Costa una modifica al lettore, non al lavaggio (circa 1 sessione, STIMATO).

## Verdetto

**VIVA, con correzioni.** Probabilità che non funzioni: circa 25–35% (STIMATO).

Errori provati:
1. **La regola "una GET per pila" non regge con clienti grandi** con il lettore usato da M26: find +60%, scaldare circa 8 volte (MISURATO a 0,5 GB con clienti da 2 MB). Proiezione di M26 a 20 TB senza correzione: 12,7 GET invece di 7,6, scaldare sopra 25. Si corregge con le impostazioni del lettore (MISURATO).
2. **Piccole imprecisioni nel rapporto:** le L0 in attesa sono circa 3 e non 1,6 (MISURATO); il bancone da $105 vale con blocchi da 64 KB, non da 4 KB; nella base ci sono $40 contati due volte. Nessuna cambia il verdetto.

Cosa regge (MISURATO): amplificazione e pile di M26 a 1 GB; la correzione +2,6 fino a 8 GB; il simulatore prudente sull'amplificazione (12% in più del vero a 8 GB); il conto dei soldi.

Rischi (non errori): 11 raddoppi di taglia tra 8 GB e 20 TB solo simulati; le condizioni di scrittura (M13, M14) non rifatte; lo scrittore che si ferma se il compattatore muore; indici globali e indici per cliente non in ordine di data; rami che tengono lo spazio.

Correzioni da mettere nel piano:
1. Lettore: una lettura in anticipo per pila nelle find; letture da 4–16 MB, o meglio dall'indice dei blocchi, quando si scalda un cliente.
2. Pile piccole e recenti fissate sul bancone (circa 200 GB per 20 TB).
3. Blocchi da 64 KB e compressione.
4. `validate` nello schedulatore a gradini.
5. Lavoratori distribuiti e sottoparti di SlateDB 0.17 per il fondo, al posto delle partizioni per intervalli di clienti; compattatore di riserva con allarme sulle L0.

**La prova più economica che deciderebbe** (in locale, $0, circa una sessione): 4–8 GB di carico misto con circa 2 MB per cliente (pochi clienti grandi, come 20 TB con 10 milioni), pile a gradini T = 4, lettore regolato e pile piccole sul bancone. Misurare find, scaldare e pile. Mi fa cambiare idea (verso FERITA): find peggiore sopra 1,2 GET per pila con il lettore regolato, o pile medie sopra 7,8 a 8 GB. Per i 11 raddoppi mancanti resta la prova grande di M26 (50–200 GB), che chiede una macchina più grossa o il sì del fondatore per AWS.

## Cosa ho deciso io

- Ho scelto la prova da 8 GB con lo scrittore a 2 MB/s e il regime di 1 volta, come la 4 GB di M26, per poterle confrontare e stare nel disco (30 GB liberi). Nessun'altra prova girava insieme a lei.
- Ho aggiunto la prova con pochi clienti grandi perché la proiezione di M26 cambia la taglia del cliente (10 milioni di clienti su 20 TB) ma le prove no.
- Per provare il rimedio ho reso regolabili read-ahead e letture in anticipo nel lettore del prototipo (variabili `REV_READ_AHEAD`, `REV_FETCH_TASKS`; di serie come M26), solo sul mio branch.
- Non ho rifatto le voci delle scritture di B1 e del revisore: le ho prese come M26.

## Cosa manca o blocca

- Niente blocca. Non ho provato crash, letture in corso e rami sotto il lavaggio a gradini: li lascio al TLA+ (M28) e alla prova di crash del piano.
- La CPU con la compressione e su Graviton: NON SO (i dati del prototipo non si comprimono).

## Link

- Branch: `rev-lavaggio-per-cliente` di `fenvix-engine`, cartella `proto/lavaggio-per-cliente/rev/` e `risultati-rev/` (nessuna PR, nessun merge).
- Previsioni ed esito: `rev/PREVISIONI.md`. Prove: `rev/matrice-rev.sh`, `rev/pochi2.sh`. Risultati: `risultati-rev/carichi.jsonl`, `misure.jsonl`, `riassunto.jsonl`, `pile-sul-bancone.jsonl`, `qualsiasi-pesato.jsonl`.
- Lavoro di M26: PR #48, https://github.com/Mircoguidetti/fenvix-engine/pull/48
- Fonte delle velocità di compressione: README di zstd, https://github.com/facebook/zstd (DICHIARATO).

## Errori o mancanze in `fenvix-brain`

- `PLAN-LAVAGGIO.md`, pezzo 1 e pezzo 2: aggiungere le impostazioni del lettore (una lettura in anticipo per pila nelle find, letture grandi per scaldare), le pile recenti fissate sul bancone e il rischio "lo scrittore si ferma se il compattatore muore". Il punto 3 parla ancora di "tetto alle pile per cliente": con M26 è il lavaggio a gradini.
- `PLAN-LAVAGGIO.md`, punto 5 delle proposte di M26: le partizioni per intervalli di clienti non servono per il fondo; SlateDB 0.17 ha già lavoratori distribuiti (RFC-0025) e sottoparti (RFC-0028).
- `NUMBERS.md`: aggiungere "a 8 GB pile a gradini T = 4: 7,2 pile medie, amplificazione 16,0 (MISURATO, revisore M26)" e "con clienti da 2 MB il lettore di M26 fa 1,8 GET per pila in Q1 e circa 8 per scaldare; con una lettura in anticipo e read-ahead da 16 MB una (MISURATO, revisore M26)".
