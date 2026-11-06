**REGGE. Find peggiore 0,92–1,00 GET per pila con il lettore regolato (soglia 1,2) e pile medie 7,2 a 8 GB (soglia 7,8), MISURATO; a 20 TB find peggiore circa 7,6 GET senza bancone e circa 3,4 con le pile recenti sul bancone; prezzo circa 40% di Infinite senza compressione, circa 37% con la compressione (forchetta 29–39%), circa 32–35% se valgono anche il rapporto di compressione misurato o le parti dei PUT da 64 MB (tutto STIMATO).** Al 30–35% ci si arriva solo con le leve in più, non con il disegno di oggi.

# M29 · Il lavaggio a gradini con clienti grandi, lettore regolato e pile recenti sul bancone

Sessione M29 (Opus), lanciata da scheduled task il 2026-11-04. Tutto in locale, $0, nessun connettore personale, nessun contatto, nessun merge. Branch `clienti-grandi` di `fenvix-engine` (da `rev-lavaggio-per-cliente`), cartella `proto/lavaggio-per-cliente/m29/`. Il motore non è stato toccato. Soglie e metodo scritti prima di misurare in `m29/SOGLIE.md` (commit 64bf507, il primo del branch).

Letti: `CLAUDE.md`, `RULES.md`, `PLAN-LAVAGGIO.md`, `DECISIONS.md` (voci del 2026-11-03 sul lavaggio, il 47% e il 30–35%), il rapporto di M26 e quello del suo revisore. Codice: il prototipo di M26 con le variabili del revisore, e di SlateDB 0.17 lo schedulatore size-tiered, il compattatore, il costruttore del compattatore e l'iteratore delle scatole.

## In una riga

**Fatto.** Con clienti grandi la regola "una GET per pila" torna vera con il lettore regolato, anche a 8 GB (MISURATO). Le pile medie a 8 GB sono 7,2, come il revisore. Le pile recenti sul bancone portano una find da 6–7 GET a 2 GET in tutte le prove. La compressione dimezza i PUT del lavaggio e costa il 18–25% di CPU in più. Il prezzo scende sotto il 40%; il 30–35% chiede ancora due leve.

## Com'è fatta la prova (in cucina)

- **Il magazzino** (S3) è finto: una cartella sul disco con il contatore di M26, che conta ogni richiesta. In più ora conta le GET per scatola (SST), così so su quale pila è andata ogni GET.
- **Il carico** è quello misto di M26 (chat, eventi, ordini; attività zipf 0,7; riempimento poi regime con ritenzione; scrittore a 2 MB/s; L0 da 8 MB; cliente in testa alla chiave; filtri per prefisso sul cliente).
- **Clienti grandi**: numero di clienti = taglia / 2 MB. 4.096 clienti a 8 GB, 2.048 a 4 GB, 512 a 1 GB. Con così pochi clienti ognuno scrive di continuo ed è in tutte le pile: è il caso peggiore per le GET.
- **Documenti realistici** che si comprimono: messaggi di chat con testo da un vocabolario di 4.000 parole con frequenze zipf, eventi di dispositivi con campi ripetuti e numeri, ordini con righe e indirizzi, più un identificativo casuale per documento (che non si comprime). Sono sintetici: il loro rapporto di compressione è una stima di quello vero, non una misura su dati di clienti.
- **Blocchi da 64 KB** (e 4 KB per confronto) anche nelle scatole riscritte dal lavaggio. Qui ho trovato un fatto (punto 6): il compattatore separato di SlateDB 0.17 scrive sempre blocchi da 4 KB senza compressione. Per questo il lavaggio a gradini gira dentro il processo del database.
- **La fotografia per le GET** si prende appena finiscono le scritture, fermando il lavaggio senza aspettarlo. Così le pile sono quelle del regime (7 a 8 GB) e non le 2–4 che restano dopo un lavaggio finale, come nelle misure di M26 e del revisore.
- **Le GET** si misurano come in M26: 200 find per tipo (Q1 ultimi 20 messaggi, Q2 un documento per `_id`, Q3 intervallo di date sugli eventi, "scaldare un cliente" = tutti i suoi documenti), bancone S1 (indici dei blocchi e filtri caldi, blocchi di dati vuoti a ogni query), ogni risposta controllata (cliente, collezione, date, numero esatto di documenti). Zero errori in tutte le prove.
- **La macchina**: 2 vCPU Intel, 7 GB di RAM, circa 30 GB di disco. Una prova alla volta.

Prove fatte: 8 GB (2 ore e 23 minuti), 4 GB, e la serie a 1 GB: blocchi 64 KB senza compressione, con lz4, con zstd livello 1; blocchi 4 KB senza compressione e con lz4; 64 KB senza lavaggio (per misurare la CPU del solo lavaggio). Più un banco della sola compressione sugli stessi documenti.

## 1. Il lettore regolato con clienti grandi (soglia: al massimo 1,2 GET per pila)

In cucina: il cameriere di M26 apriva quattro ante insieme per ogni pila anche quando gli servivano le prime 20 cose. Il cameriere regolato apre un'anta per pila nelle find; per portare sul bancone tutto il cliente apre l'armadio intero in un colpo solo.

**A 8 GB, 4.096 clienti da 2 MB, fotografia con 7 pile (3 L0)** (tutto MISURATO):
- Lettore di M26 (read-ahead 64 KB, 4 letture in anticipo per pila): Q1 **9,56 GET (1,37 per pila)**, Q3 7,21 (1,03), find tipica 7,6, scaldare un cliente **22,3 GET (3,2 per pila)**, p90 34.
- Lettore regolato (una lettura in anticipo da 64 KB per pila nelle find): Q1 **6,46 GET (0,92 per pila)**, Q3 **6,47 (0,92)**, Q2 2,1, find tipica **5,6**. Con read-ahead 128 KB uguale.
- Per scaldare: letture da 4 MB 6,6 GET (0,94 per pila), da 16 MB 6,46, tutto il cliente in un colpo 6,45 (0,92 per pila).
- Meno di 1 per pila perché la L0 più nuova era appena nata (1 MB) e non conteneva tutti i clienti: il filtro per prefisso la salta.

**A 1 GB, 512 clienti da 2 MB** (MISURATO, cinque prove): con il lettore regolato Q1 e Q3 costano **esattamente 1,00 GET per pila** in tutte e cinque (9 pile, 7, 6, 6, 7), scaldare 1,00–1,01. Con il lettore di M26 Q1 1,38–1,62 per pila e scaldare 2,1–5,4 per pila (il peggio con i blocchi da 4 KB).

**La lettura di tutto il cliente "usando l'indice dei blocchi" c'è già in SlateDB 0.17.** L'ho letto nel codice (`sst_iter.rs`, `spawn_fetches`): una scansione calcola dall'indice dei blocchi (che sta sul bancone) il primo e l'ultimo blocco dell'intervallo cercato, e una lettura in anticipo non va mai oltre l'ultimo blocco. Quindi un read-ahead più grande del cliente legge proprio "dall'inizio alla fine del cliente" con una GET per pila, senza byte inutili dopo la fine. Basta l'impostazione; non serve la modifica al lettore che il revisore stimava in una sessione. Byte letti per scaldare un cliente a 8 GB: circa 2,4 MB in tutte le configurazioni (MISURATO), cioè il cliente e poco più.

**Contro la soglia**: find peggiore **0,92 GET per pila a 8 GB, 0,99 a 4 GB e 1,00 a 1 GB** (MISURATO), sotto 1,2. Regge.

Il lettore di M26 con blocchi da 64 KB fa meno danno di quanto aveva misurato il revisore con blocchi da 4 KB (Q1 1,37 per pila invece di 1,8): parte dell'errore di granularità veniva dai blocchi piccoli.

## 2. Le pile a 8 GB (soglia: al massimo 7,8 pile medie)

**8 GB, T = 4, blocchi 64 KB, 4.096 clienti** (MISURATO): pile medie **7,20**, p90 9, massimo 11; di queste 3,0 sono L0 che aspettano il lavaggio e 4,2 sorted run. Amplificazione (byte scritti nelle scatole, scarico delle L0 compreso, diviso byte dell'utente) 16,4. Il revisore, con clienti piccoli e blocchi da 4 KB, aveva misurato 7,20 pile e 17,3 con lo scarico: la taglia dei clienti e dei blocchi non cambia le pile.

**4 GB, 2.048 clienti** (MISURATO): pile medie **6,95**, p90 8, massimo 10; amplificazione con lo scarico 14,2. Fotografia con 7 pile (2 L0): lettore regolato Q1 6,95 GET (**0,99 per pila**), Q3 0,99, scaldare 0,99–1,01 per pila; lettore di M26 Q1 1,60 per pila, scaldare 3,3.

**A 1 GB, 512 clienti** (MISURATO): pile medie 6,05–6,25 in tutte e cinque le prove con il lavaggio, con o senza compressione, con blocchi da 64 o 4 KB.

La correzione del simulatore (+2,6 pile tra vere e simulate) torna anche qui: 7,20 vere contro 4,58 simulate a 8 GB, +2,62. **Contro la soglia: 7,20, sotto 7,8.** Regge.

## 3. Le pile recenti sul bancone

In cucina: le pile in alto sono piccole e nuove. Le teniamo sul bancone (NVMe dei camerieri, dove il lavaggio già le porta prima di pubblicarle, DECISIONS 2026-11-02). Il cameriere va al magazzino solo per le pile grandi in fondo.

Come l'ho misurato: il contatore sa su quale scatola è andata ogni GET; "con il bancone" vuol dire che le GET sulle L0 e sulle sorted run sotto la soglia non vanno su S3. Soglia: l'1% dei dati, come i 200 GB su 20 TB del revisore (e 2% e 5% per vedere la curva).

**A 8 GB** (MISURATO): con l'1% stanno sul bancone 5 pile su 7, cioè 76 MB (0,9% dei dati); restano su S3 le due pile grandi (653 MB e 8,9 GB). Con il lettore regolato: Q1 da 6,46 a **2,0 GET**, Q3 da 6,47 a **2,0**, Q2 da 2,1 a 2,0 (il biglietto e il documento stanno quasi sempre nelle pile grandi), scaldare da 6,45 a **2,0**. Il 2% e il 5% danno lo stesso: tra 28 MB e 653 MB non c'è nessuna pila. Con il lettore di M26: Q1 da 9,6 a 5,1 e scaldare da 22 a 18, perché le pile grandi sono quelle dove il lettore di M26 sbaglia di più.

**A 4 GB** (MISURATO): l'1% (41 MB) tiene 3 pile e ne lascia 4 su S3, perché una pila da 55 MB sta appena sopra: Q1 4,0 GET; con il 2% (98 MB) ne restano 3 e Q1 costa 3,0. Ogni pila su S3 costa 1,0 GET anche qui. Con una cucina piccola la soglia giusta dipende da dove cadono i gradini; a 20 TB il revisore la calcola sulle pile simulate.

**A 1 GB** (MISURATO): in tutte e cinque le prove restano su S3 due pile e Q1, Q3 e scaldare costano 2,0 GET. (Con la compressione la soglia in byte va letta sui byte compressi: con lz4 l'1% tiene solo le L0 e Q1 costa 4,0; con il 2% di nuovo 2,0.)

Ogni pila rimasta su S3 costa 1,0 GET per find (MISURATO). Quindi la proiezione del revisore a 20 TB si può usare con il fattore misurato: con 200 GB sul bancone restano su S3 circa 2,7 pile (simulatore del revisore), più 1 di correzione, e la **find peggiore viene circa 3,4 GET** (STIMATO). Nel momento peggiore (pile al p90) circa 3,6. Scaldare un cliente p90 circa 4 GET (STIMATO).

Senza bancone, a 20 TB: pile medie circa 9,0 (6,4 simulate più la correzione), find peggiore **circa 7,6 GET** con il fattore 1,0 per pila (7,1 con il fattore 0,92 di 8 GB), circa 9,7 nel momento peggiore; scaldare p90 circa 12 (STIMATO). Con il lettore di M26 e blocchi da 64 KB circa 9,7 (STIMATO dai fattori misurati a 8 GB).

Quanto costa tenerle: 200 GB di NVMe a circa $0,17 per GB al mese (M21) sono circa $34 al mese per copia, $102 con una copia per zona (STIMATO). Con la compressione la metà o meno.

## 4. La compressione dei blocchi

**Il banco della sola compressione** (MISURATO, stessi documenti messi come in una scatola, un core Intel a 2,1 GHz):
- Blocchi da 64 KB: lz4 rapporto 2,77, comprime a 483 MB/s, decomprime a 2.222 MB/s; zstd livello 1 rapporto 3,89, 362 MB/s e 880 MB/s; zstd livello 3 (quello che SlateDB 0.17 usa sempre) 3,93, 269 e 671 MB/s.
- Blocchi da 4 KB: lz4 2,22, zstd 1 3,03; zstd sui blocchi piccoli decomprime solo a 130–190 MB/s.

**Le prove vere a 1 GB, 512 clienti, stessi dati** (tutto MISURATO; "per GB" = per GB dell'utente nel regime):
- **Rapporto di compressione** (byte scritti nelle scatole senza compressione diviso con compressione): lz4 **2,41**, zstd 1 **3,40** con blocchi da 64 KB; lz4 1,99 con blocchi da 4 KB. Meno del banco perché chiavi, biglietti, indici e filtri si comprimono meno.
- **Byte scritti nelle scatole per GB dell'utente**: 11,66 GB senza compressione, 4,83 con lz4, 3,43 con zstd 1.
- **PUT** (parti comprese) per GB: 1.802 senza, 999 lz4, 840 zstd 1. Togliendo i 179 PUT dello scarico delle L0, quelli del lavaggio scendono di **1,98 volte con lz4 e 2,46 con zstd 1**.
- **GET del lavaggio** per GB: 8.056 senza, 5.058 lz4, 4.409 zstd 1 (1,59 e 1,83 volte meno).
- **CPU**: il processo intero usa 290,8 s per GB senza compressione, 333,3 con lz4, 349,6 con zstd 1, e 56,3 senza lavaggio. Quindi il lavaggio costa **22,5 s di CPU per GB scritto** con documenti veri e blocchi da 64 KB (M26: 24,5 con byte casuali e blocchi da 4 KB). La compressione aggiunge **4,1 s per GB scritto con lz4 (+18%) e 5,7 con zstd 1 (+25%)**. Il revisore aveva stimato +6–11% dalle velocità dichiarate: il costo vero è circa il doppio, ma resta piccolo nei soldi (punto 7).
- **Blocchi da 4 KB contro 64 KB**: 4 KB costano più CPU (351,5 s per GB contro 290,8, +21%), comprimono peggio (1,99 contro 2,41 con lz4) e vogliono 17 volte più bancone per gli indici dei blocchi (13,5 MB per GB di dati contro 0,8). Un motivo in più per i 64 KB.
- Le pile non cambiano con la compressione (6,05–6,25 in tutte le prove) e le GET per pila nemmeno (1,00).

Il livello 1 di zstd: SlateDB 0.17 usa sempre il livello 3 (`format/sst.rs`, riga 590). Per la prova ho reso regolabile il livello con una riga in una copia locale della libreria (`m29/scripts_vendor.sh`, solo per il prototipo). Nel motore servirebbe la stessa piccola modifica, o restare al livello 3: dal banco, il 3 comprime quasi uguale e costa circa il 35% di CPU in più in compressione.

## 5. Validate nello schedulatore a gradini

Ho aggiunto `validate` a `livelli.rs`, copiato da `size_tiered_compaction.rs` di SlateDB 0.17 (righe 297–360): un lavoro passa solo se le sue sorgenti sono pile consecutive dell'albero giusto (L0 dalla più nuova, poi le sorted run dalla più nuova alla più vecchia) e la destinazione è la pila più vecchia tra le sorgenti. SlateDB chiama `validate` sia quando un lavoro viene proposto sia prima di pubblicarlo; se rifiuta, il lavoro è segnato fallito e non tocca il foglio indice.

Test (tutti verdi, 3 giri di fila, MISURATO):
- `valida_accetta_e_rifiuta`: accetta "tutte le L0 in una pila nuova" e "due pile vicine nella più vecchia"; rifiuta pile non consecutive, l'ordine rovesciato, la destinazione sbagliata, le L0 nell'ordine sbagliato, il segmento sconosciuto.
- `senza_validate_si_legge_un_valore_vecchio`: un database vero (SlateDB in memoria) con tre pile che hanno la stessa chiave a v1, v2, v3, e uno schedulatore sbagliato che lava la pila più nuova con la più vecchia saltando quella in mezzo. I controlli generali di SlateDB lo lasciano passare (le sorgenti esistono, la destinazione è tra le sorgenti) e dopo il lavaggio **la lettura restituisce v2 invece di v3**: un dato vecchio torna a galla.
- `con_validate_il_lavoro_sbagliato_e_rifiutato`: stesso caso con il nostro `validate`: il lavoro è rifiutato, restano tre pile e la lettura restituisce v3.

Quindi il rischio del revisore era vero: senza `validate` un lavoro sbagliato corrompe le letture senza che SlateDB se ne accorga. Ora è chiuso nel prototipo; nel motore va portato con lo schedulatore.

## 6. Un fatto nuovo su SlateDB 0.17: il compattatore separato ignora blocchi e compressione

`CompactorBuilder::build` (`db/builder.rs`, righe 1388–1392) crea il suo formato delle scatole con `SsTableFormat::default()`: blocchi da 4 KB, nessuna compressione, qualunque cosa sia impostata nel database. Il compattatore dentro il processo del database (`DbBuilder::with_compactor_builder`) invece usa il formato del database, e i lavoratori distribuiti (`CompactionWorkerBuilder`) hanno le loro opzioni per blocchi e compressione. Conseguenze:
- M26 e il revisore, che usavano il compattatore separato, hanno misurato scatole del lavaggio con blocchi da 4 KB. Le loro pile e la loro amplificazione restano valide (qui tornano uguali con 64 KB), ma i loro numeri sulle GET con il lettore di M26 sono quelli dei blocchi da 4 KB.
- Nel motore il "compattatore a parte su macchine spot" del piano va fatto con i lavoratori distribuiti (`CompactionWorkerBuilder`, che il revisore consigliava già per il fondo) oppure con una piccola modifica a `CompactorBuilder`. Con il compattatore separato com'è oggi, blocchi da 64 KB e compressione andrebbero persi alla prima riscrittura.

## 7. Il prezzo a 20 TB (contro il 47% e l'obiettivo 30–35%)

Il conto rifà quello di M26 e del revisore con i numeri nuovi (tutto STIMATO, su prezzi DICHIARATI in `NUMBERS.md`, B1 e B13; script `m29/scripts/prezzo_m29.py`, ingressi con la fonte in `m29/risultati/ingressi-prezzo.json`). Voci fisse come M26: base di B13 $582, scritture con la dieta (M13) e i driver diretti (M14) $558, bancone per indici e filtri $105 (i filtri misurati qui sono lo 0,83–0,93% dei dati, come M21), find fredde 35 al secondo ($36,8 al mese per ogni GET per find), lavaggio a gradini T = 4 con amplificazione 29,4 e richieste grandi (letture da 16 MB, parti dei PUT da 10 MB), CPU su spot con margine 1,5. Prezzo = costo / 0,55. Infinite al centro $8.430 (forchetta $7.980–10.850).

Un passo alla volta:
1. **Lettore di M26, niente bancone, niente compressione**: find peggiore circa 9,7 GET, prezzo circa **44,5%** (35–47%). Con i blocchi da 4 KB il revisore aveva 12,7 GET e 47%.
2. **Lettore regolato**: find circa 7,6 GET, costo circa $1.990, prezzo circa **43%** (33–45%). È il numero di M26, ora con le GET per pila misurate con clienti grandi.
3. **Più le pile recenti sul bancone** (200 GB, una copia, $34): find circa 3,4 GET (le GET delle find scendono da $281 a $124), costo circa $1.870, prezzo circa **40%** (31–43%). Con tre copie (una per zona) circa 42%.
4. **Più la compressione lz4**, con PUT, GET e CPU del lavaggio misurati e lo spazio su S3 ancora al rapporto 2 di B13 (prudente): il lavaggio scende da $464 a $329 (PUT da $300 a $152, CPU da $115 a $134), costo circa $1.730, prezzo circa **37,4%** (29–39,5%). Con zstd 1 circa **36,9%** (29–39%).
5. **Le leve che portano nel 30–35%** (STIMATO, ognuna da provare):
   - lo spazio su S3 al rapporto misurato qui (2,41 lz4, 3,40 zstd 1, invece di 2): circa 35,9% con lz4 e **34,0%** con zstd 1. Dipende da quanto si comprimono i dati veri dei clienti: i miei documenti sono sintetici;
   - le parti dei PUT da 64 MB invece di 10 MB (piccola modifica a `object_store`, già vista dal revisore): circa **34,6%**, con lz4 o zstd;
   - tutte e due insieme: circa **33,2% con lz4 e 31,7% con zstd 1** (forchetta 25–35%).

Dove va ancora il prezzo: con la compressione il lavaggio vale circa 7 punti di Infinite (4 con le parti da 64 MB; erano circa 10 in M26), le find circa 2,7 (erano circa 6). Il resto sono le scritture (quaderno, tra zone, CPU) e la base di B13, che questa prova non tocca. Con la dieta e i driver diretti (le due condizioni di M26, non rifatte qui) il prezzo dipende da loro più che dal lavaggio.

## 8. Contro la soglia

- REGGE chiede find peggiore al massimo 1,2 GET per pila con il lettore regolato, pile medie al massimo 7,8 a 8 GB, prezzo sotto il 47%. Misurato: **0,92 GET per pila a 8 GB, 0,99 a 4 GB e 1,00 a 1 GB; pile medie 7,20 a 8 GB (6,95 a 4 GB)**; prezzo proiettato **circa 40% senza compressione, circa 37% con, circa 32–35% con le leve in più** (STIMATO). → **REGGE**.
- Il 30–35%: ci si arriva solo con la compressione più una delle due leve (spazio al rapporto vero dei dati dei clienti, parti dei PUT da 64 MB). Con quello che è già deciso si sta al 37% circa.
- MUORE chiede find sopra 15 GET a 20 TB anche con il bancone, o prezzo sopra il 47% senza rimedio: no (circa 3,4 GET con il bancone, 7,6 senza).
- Vale per i database grandi: è la prova fatta per loro (clienti da 2 MB come a 20 TB). Per i database piccoli non cambia niente: le pile sono poche e stanno tutte sul bancone.
- Probabilità che il verdetto sia sbagliato: **circa 20–30%** (STIMATO). I rischi: 20 TB è ancora una proiezione (11 raddoppi oltre 8 GB, solo simulatore); i documenti sono sintetici (il rapporto di compressione vero può essere più basso); la CPU è su Intel a 2,1 GHz, non su Graviton; le voci delle scritture vengono da B1 e non sono rifatte; il bancone delle pile recenti è misurato contando le GET, non con un bancone vero che si riempie e si svuota.

## 9. Cosa ho deciso io

- Il lavaggio a gradini gira dentro il processo del database (`with_compactor_builder`) invece che nel compattatore separato, perché quello separato ignora blocchi e compressione (punto 6). Lo schedulatore è lo stesso di M26, più `validate`.
- La fotografia per le GET la prendo a fine scrittura senza aspettare il lavaggio: le pile sono quelle del regime. M26 e il revisore misuravano dopo il lavaggio finale, con 2–5 pile.
- Prove principali senza compressione, per confrontarle con il revisore (che non aveva compressione); la compressione l'ho misurata nella serie a 1 GB con gli stessi dati.
- Regime di 1 volta la taglia a 8 e 4 GB (come il revisore), e anche a 1 GB per stare nel tempo: la serie a 1 GB è servita a confrontare i formati tra loro, non a misurare l'amplificazione assoluta.
- Livello di zstd: una riga in una copia locale di SlateDB (non nel motore, non nel repository: lo script la rifà).
- Nel prezzo uso 1,0 GET per pila (misurato a 1 GB) e non 0,92 (8 GB): prudente.
- Nel prezzo con la compressione tengo lo spazio su S3 al rapporto 2 di B13 come caso centrale, perché i miei documenti sono sintetici.

## 10. Cosa manca o blocca

- Niente blocca.
- Da 8 GB a 20 TB resta il simulatore. La prova grande di M26 (50–200 GB) chiede una macchina più grande o una giornata di prova AWS (serve il sì del fondatore).
- Il rapporto di compressione sui dati veri dei clienti: NON SO. Prova economica: comprimere con lz4 e zstd 1 blocchi da 64 KB fatti con un dump pubblico di documenti JSON (per esempio le tracce di agenti già usate in M24/M25).
- La CPU su Graviton: NON SO.
- Un bancone vero per le pile recenti (spazio, cosa succede quando il lavaggio le sostituisce): non misurato, qui conto solo le GET.

## 11. Link

- Branch: `clienti-grandi` di `fenvix-engine`, PR in bozza: https://github.com/Mircoguidetti/fenvix-engine/pull/51
- Soglie e metodo: `proto/lavaggio-per-cliente/m29/SOGLIE.md`
- Codice: `m29/src/main.rs` (carico, misura con le GET per pila, banco della compressione), `m29/src/livelli.rs` (pile a gradini con `validate` e i test), `m29/src/documenti.rs` (documenti realistici), `m29/src/conta.rs` (contatore con le GET per scatola), `m29/scripts_vendor.sh` (livello di zstd), `m29/prove.sh` (le prove fatte)
- Risultati: `m29/risultati/carichi.jsonl`, `misure.jsonl`, `riassunto.jsonl` (da `scripts/riassunto.py`), `banco-compressione.jsonl`, `ingressi-prezzo.json`, `prezzo-20tb.json`; log della prova da 8 GB in `log-m29-8gb.txt`
- Intermedio: `sessions/2026-11-04-clienti-grandi/INTERMEDIO-1.md`

## 12. Errori o mancanze trovati in `fenvix-brain`

- `PLAN-LAVAGGIO.md`, pezzo 1, "compattatore separato su macchine spot": con SlateDB 0.17 va fatto con i lavoratori distribuiti (`CompactionWorkerBuilder`) o con una modifica a `CompactorBuilder`, perché il compattatore separato riscrive le scatole con blocchi da 4 KB e senza compressione (punto 6).
- `PLAN-LAVAGGIO.md`, correzioni del revisore: "letture dall'indice dei blocchi" non costa una sessione: SlateDB 0.17 lo fa già con un read-ahead più grande del cliente (punto 1).
- `NUMBERS.md`: aggiungere "con clienti da 2 MB e il lettore regolato una GET per pila (0,92 a 8 GB, 1,00 a 1 GB, MISURATO M29)", "pile a gradini T = 4 a 8 GB con clienti da 2 MB e blocchi 64 KB: 7,20 pile medie (MISURATO M29)", "lavaggio: 22,5 s di CPU per GB scritto con blocchi 64 KB; lz4 +18%, zstd 1 +25% (MISURATO M29, Intel 2,1 GHz)", "compressione su documenti sintetici: lz4 2,41, zstd 1 3,40 con blocchi 64 KB; PUT del lavaggio 1,98 e 2,46 volte meno (MISURATO M29)", "20 TB: circa 40% di Infinite con pile recenti sul bancone, circa 37% con la compressione (STIMATO M29)".
- `DECISIONS.md`, voce del 30–35%: oggi "siamo a circa 43%"; con M29 circa 37% con quello che è già nel piano, se il revisore conferma.
