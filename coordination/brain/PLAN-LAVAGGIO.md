# Il piano del lavaggio per cliente

Coordinatore, 2026-11-03. Aggiornato con M26 (REGGE di misura, in attesa del revisore). Decisione di partenza: DECISIONS.md, voce "Direzione confermata e il pezzo difficile".

## Cosa vendiamo

"Il MongoDB senza dischi": stessi driver di MongoDB, l'unica copia dei dati su S3, per database grandi che si leggono un cliente alla volta. Il cliente naturale sono le app con agenti: mentre il modello pensa (2,3–8 s a passo, MISURATO M24/M25) noi portiamo i dati di quel cliente sul bancone.

Il prezzo basso viene da S3 e dalle letture raggruppate. Il riscaldamento serve alla velocità, non al prezzo.

## Il pezzo che ci difende

Atlas Infinite tiene le pagine del B-tree ordinate per chiave, su dischi NVMe in ogni zona (DICHIARATO, blog MongoDB 8 ottobre 2026). Anche se le spostasse su S3, una lettura fredda di un cliente gli costerebbe circa 20–30 GET, perché i documenti di un cliente sono sparsi in pagine diverse (STIMATO da M21). Per arrivare dove saremo noi deve rifare come salva i dati: un lavaggio (compattazione) che ordina per cliente. È il lavoro di mesi che ci stiamo prendendo per primi.

## Gli obiettivi (confermati dal fondatore il 2026-11-03, DECISIONS.md)

- Letture calde (dal bancone): p50 sotto 2 ms, p99 sotto 10 ms.
- Letture fredde di un cliente (da S3): p50 circa 30–60 ms, p99 sotto 150 ms.
- Scritture: p99 sotto 10 ms.
- Almeno 10.000 scritture al secondo per quaderno (oggi 10.500–10.800, MISURATO M16).
- Le scansioni grandi su tutto il database sono fuori obiettivo: lì vince chi ha i dati su disco.

## Prima di tutto: le correzioni di oggi

Nessun pezzo nuovo parte prima di queste, perché toccano la durata dei dati.
- La perdita di dati su main: un commit ripetuto risponde "ok" prima che il PUT del WAL sia durevole (MISURATO 3 su 3, C7). Correzione in C8, poi PR #34.
- Il lavaggio che raffredda il bancone: corretto in PR #47 (letture calde servite da 0% a 100%, MISURATO M27).
- Gli errori di isolamento S4, sul branch isolamento (PR #27).

## Pezzo 1 · Il lavaggio per cliente (compattazione raggruppata per chiave cliente)

Cosa fa, un passo alla volta:
1. Ogni collezione dichiara la sua chiave cliente, come una chiave di shard (per esempio userId). Senza chiave, la collezione resta "condivisa".
2. Il lavaggio riscrive le scatole (SST) in modo che i documenti e le voci di indice dello stesso cliente stiano vicini, in tutte le collezioni e in tutti gli indici di quel database. Le scatole si tagliano ai confini tra clienti, così un cliente sta in poche scatole.
3. Tetto alle pile (sorted runs) per cliente: oltre il tetto, il lavaggio unisce. M21 dice che il tetto a 2 pile porta una lettura fredda a 2,6 GET ma fa scrivere 2,2 volte più byte (MISURATO). M26 sceglie il tetto giusto.
4. Indici coerenti: documento e voci di indice di uno stesso cliente cambiano scatola nello stesso giro, con lo stesso numero di versione. Un lettore non vede mai una voce di indice che punta a un documento sparito.
5. Cancellazione mirata (GDPR): una riga nella lista dei cancellati dentro il foglio indice (il lettore nasconde subito il cliente, i rami la ereditano), poi la riscrittura delle sole scatole dove sta lui; la riga si toglie quando non ne resta nessuna (M28). Aperto, decide il fondatore: propagare la cancellazione ai rami e ai punti di ripristino nati prima.

Dove si costruisce (M26): dentro SlateDB, con uno schedulatore nostro che SlateDB 0.17 accetta senza toccare la libreria. La strategia sono le "pile a gradini" (livelli geometrici, T = 4, manopola tra 4 e 6): pile di taglia crescente con l'età, si lavano insieme quando una è almeno un quarto di quella sotto. Il tetto alle pile di SlateDB non va usato sui database grandi: a 20 TB riscrive tutto a ogni lavaggio piccolo (STIMATO M26).
Numeri di M26: raggruppare per cliente non costa lavaggio in più (3,91 contro 3,95, MISURATO); a 20 TB e 4.000 scritture/s amplificazione circa 29, circa 9 pile, find fredda 7,6 GET nel caso peggiore e circa 4 per un cliente qualsiasi, scaldare un cliente p90 circa 12 GET, lavaggio circa $460 al mese, prezzo circa 43% di Infinite (33–45%) (tutto STIMATO). Due condizioni: le scritture del disegno completo (dieta M13, driver diretti M14) e richieste S3 grandi nel lavaggio (letture 16 MB, parti dei PUT almeno 10 MB); senza, 50–53%.
Pezzi concreti: cliente in testa alla chiave ({cliente}/{collezione}/{chiave}) con biglietto _id → chiave; filtri per prefisso sul cliente; compattatore separato su macchine spot; L0 grandi (256 MB); partizioni per intervalli di clienti solo per i database grandi; cancellazione mirata con un registro dei clienti cancellati letto anche dal lettore e il filtro del lavaggio (CompactionFilter).

Test a ogni passo:
- Carta: modello TLA+ dello scambio delle scatole nel foglio indice, con letture in corso, rami (fork) e cancellazioni. Proprietà: nessun dato perso, nessuna voce di indice senza documento, una lettura vede sempre una sola versione, un cliente cancellato non riappare.
- Prova a confronto con mongod: stesse operazioni casuali sui due, stessi risultati, anche durante il lavaggio.
- Prova di crash: SIGKILL a metà lavaggio, poi riavvio: nessun dato perso, nessuna scatola orfana per sempre.
- Misure: GET per find fredda, GET per scaldare un cliente, byte scritti dal lavaggio, CPU per GB, tempo per cancellare un cliente.

Cancello per passare al pezzo 2 (soglia di M26): find fredda tipica sotto 8 GET, scaldare un cliente sotto 25 GET a 20 TB proiettati, prezzo al cliente sotto il 47% di Infinite.

Lavoro: circa 12–18 sessioni Claude Code (STIMATO M26), più il TLA+ (M28). Prova grande prima del motore: 50–200 GB di carico misto; cambia idea se pile medie sopra 9,5 o amplificazione sopra 40 a 200 GB.

## Pezzo 2 · Il cameriere che legge raggruppato

Cosa fa:
1. Tiene sul bancone gli indici dei blocchi e i filtri (bloom) di tutto il magazzino, così una lettura fredda va dritta alle scatole giuste senza GET in più. Quanto bancone serve per 20 TB: NON SO, lo stima M26.
2. Scalda un cliente al primo segnale: la prima richiesta di quel cliente porta sul bancone tutte le sue scatole. Questa regola da sola serve l'85,6–97,7% delle letture fredde degli agenti (32,4% sulla classifica tau2 2026) (MISURATO, revisore M25).
3. Segnale esplicito dell'agente: un comando "sto per lavorare su questo cliente", per chi vuole il 100%.
4. Le collezioni condivise (senza chiave cliente) restano fissate sul bancone. Funziona se sono sotto circa l'8–10% dei dati (STIMATO, revisore M25).
5. Per la coda lenta di S3 (p99 126–138 ms, MISURATO M25), una seconda richiesta se la prima tarda.

Test: tracce vere di agenti (tau2, DAB, SWE-bench) e di persone (GitHub, REES46) contro il motore; quota di letture servite dal bancone; p50 e p99; prova con 100 clienti scaldati insieme.

Cancello: letture fredde p99 sotto 150 ms e calde p99 sotto 10 ms in locale con il finto S3 e le latenze vere di S3.

Lavoro: circa 8–12 sessioni (STIMATO).

## Pezzo 3 · Le fondamenta per i database grandi

1. Il quaderno veloce nel motore al posto del WAL di SlateDB: 10.500–10.800 scritture al secondo a 3 copie, contro 820–990 di mongod (MISURATO M16). Circa 10–15 sessioni (STIMATO).
2. I camerieri senza stato (M8), così un database grande si serve da più macchine: circa 36–52 sessioni (STIMATO). Può arrivare dopo i primi clienti: per partire basta un cameriere per database con un sostituto pronto.

Test: Jepsen in casa (partizioni di rete, SIGKILL, orologi sfasati), TLA+ già fatto per il quaderno.

## Ordine e tempi

Correzioni di oggi, poi pezzo 1, poi pezzo 2, poi il quaderno nel motore. In 1–2 mesi arriviamo a un motore con lavaggio per cliente, bancone che scalda e quaderno veloce: circa 35–55 sessioni (STIMATO). I camerieri senza stato vengono dopo.

Dopo il pezzo 2 si può fare la prova vera su AWS (con il sì del fondatore e la cifra) e il test su Infinite ($30–80, carta del fondatore).

## Cosa può andare storto

- Il lavaggio scrive troppi byte e il prezzo sale sopra il 47% (lo misura M26).
- Gli indici dei blocchi di 20 TB non entrano in un bancone economico.
- Le app vere hanno troppi dati condivisi tra clienti.
- Atlas mette le pagine fredde su S3 con un lavaggio simile: secondo noi servono mesi di lavoro anche a loro.

## Verifica sulla carta (M28, 2026-11-03)
REGGE: 23,9 milioni di stati controllati da TLC, vivezza compresa, 10 controlli negativi su 10 (MISURATO). 24 regole per il codice in fenvix-engine docs/tla-lavaggio/REGOLE-PER-IL-CODICE.md (PR #49). Le più importanti: scambio del foglio solo con PUT condizionale e solo se tutte le scatole di partenza ci sono ancora; lista dei cancellati nel foglio ed ereditata dai rami; il lettore tiene la versione con la posizione più alta (da verificare in SlateDB 0.17; se no, lavare solo pile vicine); il lavaggio per cliente parte anche con 2 scatole; la pulizia toglie anche gli orfani mai entrati in un foglio. Nessuna regola ferma le scritture.

## Correzioni del revisore di M26 (2026-11-04, VIVA con correzioni, 25–35% di probabilità di errore)
- Errore provato: con clienti grandi (circa 2 MB, come 20 TB con 10 milioni di clienti) la regola "una GET per pila" regge solo con il lettore regolato: una lettura in anticipo per pila nelle find, letture grandi (4–16 MB) o dall'indice dei blocchi per scaldare (MISURATO). Senza: 12,7 GET e prezzo circa 47%.
- Strada migliore: pile piccole e recenti fissate sul bancone (circa 200 GB per 20 TB): find peggiore circa 3,4 GET, prezzo circa 40%; con la compressione circa 37% (STIMATO).
- Da fare nel pezzo 1: blocchi da 64 KB, compressione, validate nello schedulatore a gradini, lavoratori distribuiti di SlateDB per il fondo, compattatore di riserva con allarme sulle L0 (se il compattatore muore, a 8 L0 lo scrittore si ferma: circa 3–5 minuti con L0 da 256 MB, STIMATO).
- Prova che decide: M29 (4–8 GB, clienti da 2 MB, lettore regolato, pile sul bancone, compressione).

## Idea del fondatore: il login come segnale (2026-11-04)
Per le app usate da persone il segnale migliore è il login: la ricerca dell'utente per email è la prima lettura, e il controllo della password (bcrypt e simili, circa 100–300 ms, STIMATO) nasconde il riscaldamento di tutti i suoi dati. Con "ricordami" il segnale arriva già all'apertura della pagina. Il comando esplicito scalda(utente) resta per chi vuole di più. Un riscaldamento sprecato costa circa 3–12 GET, cioè circa $5 per milione (S3 $0,0004 per 1.000 GET, DICHIARATO): il costo vero è lo spazio sul bancone. Da provare nel pezzo 2 con tracce vere di login; riscaldare in base agli orari abituali resta un'idea di riserva.

## Da aggiungere fuori dal lavaggio: backup fuori dal bucket (2026-11-04)
Snapshot e ripristino da noi sono istantanei (foglio indice più spilla, nessuna copia). Manca una copia fuori dal bucket: replica S3 verso un altro account e un'altra regione, con regole di conservazione (30 giorni per il GDPR) e il costo dello spazio tenuto dalle spille. Senza, un bucket cancellato o un account compromesso porta via anche gli snapshot. Prima domanda di sicurezza di un cliente grande.

## Idea del fondatore: lo scaffale caldo regolabile (2026-11-04, dopo P1)
P1 ha mostrato che Atlas Infinite legge dallo storage in meno di 2 ms perché tiene tutte le pagine su dischi veloci in ogni zona (page server su NVMe, R-P1). Per avvicinarci sulle letture fredde: un ripiano veloce (disco locale dei camerieri o S3 Express) solo per i clienti attivi, come allungamento del bancone, una copia sola e perdibile (la verità resta su S3), condiviso tra i clienti, con l'uscita del cliente non visto da più tempo. Idea del fondatore: farne un'opzione per il cliente, regolabile nel tempo (più caldo nei periodi di picco, meno in quelli calmi). Parere del coordinatore: di serie automatico (grandezza dello scaffale seguita dai clienti attivi, con la quota di letture servite mostrata nel pannello); il cliente sceglie un tetto di spesa o una preferenza "più veloce / più risparmio", non una percentuale secca, perché pochi conoscono la propria quota di dati attivi e una scelta sbagliata diventa lentezza data a noi. Costo d'esempio: 1 TB logico, circa 330 GB compressi, scaffale al 10% su NVMe circa $3,6 al mese in più contro circa $7,6 di S3 (STIMATO). Da misurare nel pezzo 2: quota di letture servite dallo scaffale per grandezza, con tracce vere. Non deciso.
Come si configura (discussione con il fondatore, 2026-11-04): la scelta si fa in giorni ("tieni pronti gli utenti visti negli ultimi N giorni"), con accanto l'effetto calcolato sul traffico vero del cliente (quota di letture calde e costo in più al mese), programmabile per periodi, oppure Automatico dentro un tetto di spesa; avviso se le letture calde scendono sotto una soglia. Due rischi da studiare indicati dal fondatore: (1) rallentare utenti che il cliente non vuole rallentare: servono utenti o gruppi fissati sempre caldi (per esempio le org a pagamento); (2) troppa frizione: il cliente non sa cosa vuol dire 85% o 94% e non tocca niente per paura. Di serie deve funzionare senza toccare niente, e i numeri vanno detti in tempi (prima lettura tipica in ms), non in percentuali. Idea buona di fondo, da studiare prima di decidere.

## Correzione da M29 (2026-11-04)
SlateDB 0.17: il compattatore separato (`CompactorBuilder::build`) riscrive le scatole con blocchi da 4 KB e senza compressione, qualunque sia l'impostazione del database. Il compattatore su macchine spot del pezzo 1 va fatto con i lavoratori distribuiti (`CompactionWorkerBuilder`) o con una piccola modifica a `CompactorBuilder`. M26 e il suo revisore hanno misurato con blocchi da 4 KB per questo motivo.
