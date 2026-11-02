# Rapporto proto-quaderno, giro 2

Branch `proto-quaderno`. Tutto in locale, nessuna spesa cloud: S3 simulato con MinIO compilato in locale. La macchina
è la stessa del giro 1: 4 vCPU, 16 GB, un disco cloud. Ogni numero è MISURATO su questa macchina o STIMATO. Risultati
grezzi in `proto/quaderno/results/`; quelli del giro 1 sono stati spostati in `proto/quaderno/results/giro1/`. Il
rapporto del giro 1 resta in `docs/RAPPORTO-proto-quaderno.md`.

## 1. In breve

- **Fatto quanto chiesto ai punti 1–3.**
  - Letture, `scaricabili`, `pendenti`, `stato_scarico` e `status` non passano più dal thread principale della
    replica: le servono i thread di rete leggendo la finestra, che ha un lock per database.
  - `pendenti` è aggiornato man mano.
  - Potatura, conferme di scarico e sfratto lavorano a passi da 0,2 ms al massimo.
  - Due thread di rete per replica.
  - Cronometro per tipo di lavoro del thread principale.
  - Latenze misurate anche dall'invio vero.
- **Banco 2, soglia del 25% non superata.** Letture dalla finestra al 25% del carico, p99 misurata dall'invio:
  9,6 ms (MISURATO), soglia 1 ms. Dal tick pianificato del generatore: 22,2 ms.
  - Le letture da sole passano: 10.000 letture/s, p99 0,97 ms dall'invio (MISURATO, con 2.000 scritture/s di
    sottofondo).
  - Al 25% la macchina è quasi satura e il tempo va nel kernel: risvegli e cambi di contesto tra thread.
  - Al 50% e al 100% il sistema crolla. Al 50% il crollo dipende dal secondo thread di rete: con un solo thread il
    50% regge, 14.736 commit/s (MISURATO).
- **Banco 1b.**
  - 1 mutazione da 200 byte: soglia superata. 32 client: 17.294–17.986 record/s, p99 3,5–3,7 ms (MISURATO).
  - 5 mutazioni da 200 byte (il caso realistico): soglia non superata. Al massimo 11.893–12.695 record/s a p99
    4,6–8,4 ms (MISURATO).
- **Banco 3 (chiave calda), modalità per S1:**
  - cieca: 32.312–33.316 aggiornamenti/s, p99 4,1–4,2 ms, 0 abort;
  - "delta + dipendenza esiste" (la scelta del protocollo): 32.401–33.633/s, p99 3,9–4,0 ms, 0 abort;
  - validata: 510/s, p99 55 ms, 69% di abort;
  - validata con lettura speculativa: 2.387–2.484/s, p99 64–68 ms, 7,9% di abort.

  Tutto MISURATO. Le due modalità senza lettura superano la soglia; le due con lettura e validazione no.
- **Banco 4 superato.** Replica isolata per 600,8 s sotto carico: 2.430.676 letture lineari, 0 letture vecchie,
  porcupine "Ok" (MISURATO).
- **Banco 6 non superato.**
  - Con lo scarico fermo 10 minuti i commit sono continuati (~15.000/s) e i valori in RAM sono rimasti al tetto
    (805 MB).
  - Ma i metadati per record non hanno tetto, circa 300 byte per record non scaricato. Il kernel ha terminato per
    memoria esaurita due repliche su tre: a 4,4 e 5,6 GB, la seconda durante il recupero. Rimasta una sola replica, i
    commit si sono fermati e il recupero non è finito (MISURATO).
- **Criterio del cronometro.** "Nessun blocco sopra 1 ms" non è soddisfatto in tempo reale: ci sono blocchi fino a
  25 ms. Su CPU propria del thread invece quasi sempre: solo lo 0,01–0,02% dei giri supera 1 ms (MISURATO). Il resto
  sono attese: il thread principale viene sospeso dal sistema operativo 3.000–4.000 volte al secondo.

## 2. Cosa è cambiato nel prototipo

1. **Letture fuori dal thread principale.** La finestra ha un lock lettore/scrittore per database (in 32 gruppi).
   - Il thread principale scrive: aggiunta, troncamento, potatura, sfratto, scarichi applicati.
   - I thread di rete servono direttamente `get`, `scan`, `attendi`, `scaricabili`, `pendenti`, `stato_scarico` e
     `status`. Per `scaricabili` copiano sotto lock solo i riferimenti alle voci e costruiscono la risposta fuori dal
     lock. `status` viene da una fotografia aggiornata ogni 100 ms.
   - La "posizione leggibile" (confermata e già indicizzata) è pubblicata su un canale `watch`: chi chiede una S più
     nuova aspetta lì, sul thread di rete.
   - I valori sfrattati portano la loro collocazione nel registro su disco, così anche un thread di rete li rilegge.
2. **`pendenti` aggiornato man mano.** Per ogni database: record e byte non su S3 e posizione del più vecchio,
   aggiornati a ogni aggiunta, scarico applicato e troncamento. La richiesta legge una mappa.
3. **Lavori periodici a passi:**
   - potatura: al massimo 0,2 ms per giro, cursore sui database, un giro completo ogni 200 ms;
   - conferme di scarico: accodate al commit e applicate a passi da 0,2 ms;
   - sfratto: 0,2 ms per giro;
   - indicizzazione sui follower: 0,5 ms per giro (prima il disco, poi l'indice);
   - checkpoint e cancellazione dei segmenti: spostati sul thread del disco;
   - messaggi di replica: al massimo 128 KB.
4. **Thread di rete:** `PQ_IO_THREADS`, default 2.
5. **Priorità dei thread.** Thread principale, disco e rete delle repliche a nice −10 (`PQ_IO_NICE` per quelli di
   rete). Sulla macchina condivisa passano davanti a generatore, proxy e MinIO.
6. **Cronometro del thread principale**, 16 tipi di lavoro: numero, tempo totale, blocco più lungo, blocchi sopra
   1 ms (campo `diag` di `status`). In più, per ogni giro: tempo reale e tempo CPU del thread.
7. **Banco: latenze in due modi** (dal tick pianificato e dall'invio), più CPU per processo e cambi di contesto
   forzati del thread principale durante ogni giro (`scripts/cpu.py`, `scripts/ctx.py`).

Dopo le modifiche la correttezza è stata ricontrollata (MISURATO):
- SIGKILL del leader con 2.000 database: 201.900 commit, 0 persi, 0 duplicati; 29.017 commit ambigui rinviati con
  la stessa posizione;
- letture lineari: 0 vecchie, porcupine "Ok";
- banco 4 completo (§3).

## 3. Risultati

### Banco 2: carico misto (soglia: letture dalla finestra p99 sotto 1 ms dall'invio, al 25%)

Carico al 100%: 10.000 database piccoli a 1 scrittura/s, un database caldo a 20.000 scritture/s, 100.000 letture/s
(metà su chiavi della finestra), 1.000 "leggi la tua scrittura"/s. Scaricatore su MinIO attivo. Ogni giro dura 120 s,
su un gruppo appena creato.

- **RAM (solo i 10.000 database piccoli, 4 minuti):** RSS 858–867 MB per replica contro 10 MB a vuoto, cioè 87–88 KB
  per database piccolo (MISURATO). Nel giro 1 erano 82 KB: ogni record ora tiene anche la collocazione su disco e un
  riferimento alla voce. Sotto i 256 KB.
- **25% del carico** (7.500 scritture/s, 25.000 letture/s, 250 "leggi la tua scrittura"/s), tutto MISURATO:
  - commit confermati: 7.376/s;
  - letture dalla finestra: dall'invio p50 0,38 ms e **p99 9,6 ms**; dal tick p50 1,28 ms e p99 22,2 ms;
  - "leggi la tua scrittura" (dalla conferma del commit alla lettura su un follower): p50 0,60 ms, p99 10,6 ms;
  - commit dall'invio: p50 2,6 ms, p99 24 ms;
  - CPU in core: repliche 0,50, 0,68 e 0,50; generatore 0,61; proxy 0,21; MinIO 0,15; scaricatore 0,05;
  - il thread principale di ogni replica viene sospeso dal sistema operativo 3.100–4.000 volte al secondo.

  Andamento: nei primi 10 s, finestra quasi vuota, le letture hanno p99 3,6 ms dall'invio; a finestra piena
  (60 s, 466.000 versioni) salgono a 7–15 ms.
- **50%:** crollo. Nei primi 45 s regge circa 14.500 commit/s, con p99 in crescita fino a 2 s. Poi i timeout dei
  client (1,5 s per tentativo) generano rinvii e rotazioni tra repliche, e il carico rinviato si somma a quello
  nuovo. In media 7.403 commit/s, p99 nell'ordine dei secondi (MISURATO).
- **100%:** crollo come nel giro 1. In media 1.816 commit/s (MISURATO).

Prove diagnostiche (in `results/banco2-diagnosi/`, tutto MISURATO):
- **Solo letture,** con 2.000 scritture/s per riempire la finestra, nessuno scaricatore:
  - 25.000 letture/s: p99 1,9 ms dall'invio, 4,2 ms dal tick;
  - 10.000 letture/s: **p99 0,97 ms** dall'invio, 2,2 ms dal tick.

  In una prova esplorativa con 500 scritture/s erano 0,90 ms a 25.000 letture/s e 0,61 ms a 10.000.
- **25% senza potatura** (finestra di 600 s): letture p99 7,7 ms dall'invio. La potatura pesa poco sulla coda.
- **25% con un solo thread di rete per replica:** letture p99 7,9 ms dall'invio. Uguale.
- **50% con i thread di rete a priorità normale:** crolla lo stesso (8.172 commit/s).
- **50% con un solo thread di rete per replica:** **regge**. 14.736 commit/s, letture p99 53 ms dall'invio, commit p99
  114 ms dall'invio. Il secondo thread di rete (6 thread in più su 4 vCPU) sposta il punto di crollo sotto il 50%.

Lettura dei risultati (STIMATO dalle misure sopra e dal profilo di sistema al 25%):
- Le letture costano poco (lookup nella finestra, pochi microsecondi). La coda delle latenze viene dalla CPU condivisa:
  il 57% dei campioni è nei thread di rete, gran parte nel kernel (risvegli 12%, cambi di contesto 6%), e la
  macchina è inattiva solo al 14%.
- Ogni richiesta attraversa più thread (generatore, rete, risposta) che si contendono 4 vCPU con 3 repliche, proxy,
  MinIO e scaricatore.

### Banco 1: commit completi (soglia: almeno 15.000 record/s confermati, p99 sotto 5 ms)

Tre ripetizioni per punto, traffico tra repliche attraverso il proxy, tutto MISURATO.

- **1a, validazione isolata:** 6,9–7,1 µs per record da 5 chiavi; con codifica, indicizzazione e potatura 21,5–22,0 µs,
  cioè 45.400–46.600 record/s per core. Sotto 30 µs: superata.
- **1b, 1 mutazione da 200 B:**
  - 16 client: 9.978–10.489 record/s, p99 2,9–3,2 ms;
  - 32 client: 17.294–17.986 record/s, p99 3,5–3,7 ms;
  - 48 client: 23.270–23.751 record/s, p99 4,2–5,5 ms;
  - 64 client: 20.069–27.369 record/s, p99 4,9–7,8 ms.

  Superata a 32 client.
- **1b, 5 mutazioni da 200 B:**
  - 16 client: 8.088–9.108 record/s, p99 3,3–3,8 ms;
  - 32 client: 11.893–12.695 record/s, p99 4,6–8,4 ms;
  - 48 client: 10.422–13.667 record/s, p99 16–22 ms.

  **Non superata**: oltre i 12.000–13.000 record/s la p99 esce dai 5 ms.
- **1c, 5 mutazioni da 1 KB (dato, non soglia):**
  - 16 client: 28,5–32,9 MB/s, p99 8–19 ms;
  - 64 client: 47–55 MB/s, p99 34–37 ms.

  Il disco da solo regge 466 MB/s (MISURATO nel giro 1): il limite è la CPU, non il disco.

### Banco 3: chiave calda (soglia: almeno 2.000 aggiornamenti/s, p99 sotto 20 ms, abort sotto il 5%)

64 client fanno `$inc` sullo stesso documento per 60 s; due ripetizioni; al massimo 50 tentativi per aggiornamento.
In tutti i giri il contatore finale coincide col numero di aggiornamenti riusciti. Tutto MISURATO.

- **cieca** (delta applicato in ordine di registro, nessun controllo): 32.312 e 33.316 aggiornamenti/s, p50 1,8 ms,
  p99 4,1–4,2 ms, 0 abort. **Superata.**
- **delta + dipendenza esiste** (`docs/protocollo.md`: `$inc` su `_id` di un documento esistente): 32.401 e
  33.633/s, p99 3,9–4,0 ms, 0 abort. **Superata.** Il controllo "esiste" costa quasi niente.
- **validata** (leggi alla posizione confermata, scrivi valore+1 con dipendenza `valore`): 510/s, p50 18 ms, p99
  55 ms, 69% di abort dopo 50 tentativi. **Non superata.**
- **validata con lettura speculativa** (lettura dell'ultima versione accettata sul leader, anche non confermata):
  2.387 e 2.484/s, p99 64–68 ms, 7,9% di abort. **Non superata** (p99 e abort).

Per S1: sulla chiave calda funziona solo un aggiornamento che non legge prima di scrivere (delta, con o senza
dipendenza `esiste`). Leggi-modifica-scrivi con validazione, anche speculativa, serializza 64 client su un
documento e non regge. Per `findAndModify` e simili sulla chiave calda serve la coda per chiave nel nodo d'ingresso
(§4.4 del protocollo), non misurata qui.

### Banco 4: letture fresche sotto partizione (soglia: zero letture più vecchie della posizione confermata)

- **Carico:** 8 scrittori e 16 lettori su 1.000 chiavi, client attraverso il proxy. Ogni lettura: posizione_confermata
  dal leader (ReadIndex), poi `get(S)` su una replica a caso, anche quella isolata.
- **Isolamento:** dopo 30 s il proxy ha isolato la replica follower 2 per 600,8 s (viva, raggiungibile dai client,
  tagliata fuori dalle altre repliche); poi altri 60 s di carico. In tutto 690 s.

Risultati (MISURATO):
- 1.117.287 scritture confermate, 0 dall'esito ignoto; 2.430.676 letture.
- **Controllo diretto:** 0 letture più vecchie della posizione confermata, 0 valori inventati o dal futuro.
- **Porcupine** (linearizzabilità completa, registro per chiave): **"Ok"**, in 6,2 s.
- La replica isolata ha risposto "indietro" 9.391 volte. I lettori l'hanno evitata per 1 s ogni volta e hanno letto
  dalle altre. Durante l'isolamento ha servito 86 letture, tutte nei primi 65 ms dopo il taglio, a posizioni che
  conosceva già.
- La replica isolata è rimasta al mandato 1, senza elezioni, grazie al pre-voto. Al rientro ha recuperato circa
  1,07 milioni di voci senza disturbare il leader: a fine banco tutte e tre erano a 1.117.288.
- Lo storico (62 MB compressi) non è nel repository; si rigenera con `scripts/banco4.sh`.

**Superata.**

### Banco 6: scarico fermo 10 minuti (soglia: commit continuano, RAM lineare entro un tetto, recupero entro 20 minuti)

- **Carico:** le scritture del banco 2 al 50% (10.000 database a 0,5 scritture/s, database caldo a 10.000/s, 15.000
  scritture/s in tutto), senza letture.
- **Blocco:** MinIO sospeso (SIGSTOP) a t=120 s per 600 s.
- **Tetto di RAM per i valori:** 768 MB per replica.

Tutto MISURATO:
- **Commit durante il blocco:** continuati a circa 15.000/s (14.725–15.001/s per intervallo), p99 dall'invio tra 12
  e 211 ms.
- **RAM:** da 1,1 GB (t=120 s) a 3,8–3,9 GB per replica (t=601 s), in modo circa lineare.
  - I valori in RAM sono arrivati al tetto (805 MB) a t≈330 s; da lì sono stati sfrattati su disco, 4,4 milioni di
    record per replica a t=601 s.
  - I metadati per record (posizione nel registro, versione nella finestra, chiave, id per la deduplica) non hanno
    tetto: circa 300 byte per record non scaricato dopo il tetto dei valori, 380 byte includendo i valori prima del
    tetto.
- **Disco:** il registro è cresciuto fino a 3,1 GB per replica.
- **Memoria esaurita:**
  - a circa 480 s di blocco il kernel ha terminato la replica 0, a 4,4 GB di RSS; il gruppo ha continuato con due
    repliche;
  - a t=721 s MinIO è ripartito e lo scarico ha recuperato a circa 19.600 record/s, da 9,04 a 5,49 milioni di record
    non scaricati in 180 s;
  - a t≈915 s il kernel ha terminato anche la replica 2, a 5,6 GB. La RSS cresceva ancora: i record escono dalla
    finestra solo 60 s dopo lo scarico, e l'allocatore non restituisce subito la memoria;
  - rimasta una sola replica, i commit si sono fermati (nessuna maggioranza) e il recupero non è finito.
- **Senza l'esaurimento della memoria** il recupero avrebbe richiesto circa 460 s più 60 s di potatura, entro i
  1.200 s concessi (STIMATO dalla velocità misurata).

**Non superata.** Il tetto va esteso ai metadati: per i record non scaricati più vecchi di 60 s si tiene una sola
versione per chiave, e i record si rileggono dal registro su disco al momento dello scarico. Lo scarico può avvenire
in ordine di registro, per tutti i database insieme. Proposta non implementata.

### Cronometro del thread principale (criterio: nessun blocco sopra 1 ms)

Dal giro ufficiale al 25% (`results/banco2/2b-misto-x0.25-status-fine.jsonl`), tutto MISURATO:
- **Giri del ciclo sopra 1 ms in tempo reale:**
  - leader: 3.030 su 499.120 (0,6%);
  - follower: circa 2.190 su 285.000 (0,8%).
- **Giri sopra 1 ms di CPU propria del thread:** 41 sul leader e 49–50 sui follower (0,01–0,02%). Tutti gli altri
  sono attese: il thread viene sospeso dal sistema operativo 3.100–4.000 volte al secondo (cambi di contesto forzati),
  e a volte aspetta un lock di un database.
- **Blocco più lungo per tipo, in tempo reale:**
  - indicizzazione e aggiunta sul follower: 16–25 ms;
  - potatura: 5–14 ms, 264–401 blocchi sopra 1 ms;
  - invio alle repliche: 8–15 ms;
  - commit sul leader: 7,6 ms.
- **Tempo totale per tipo in 130 s:** potatura circa 8,4 s per replica; indicizzazione sui follower circa 16 s;
  commit sul leader 11 s.

Il criterio è soddisfatto sul lavoro vero del thread (CPU) quasi sempre, non in tempo reale. Con un thread per replica
su una macchina sua, le sospensioni sparirebbero quasi del tutto (STIMATO).

## 4. Cosa resta e cosa cambierà con 3 macchine

- **Banco 2.** Le letture sono già fuori dal thread principale e da sole stanno sotto 1 ms fino a ~10.000/s su questa
  macchina. Per il carico misto servono CPU dedicate: oggi tre repliche, il generatore (0,6–1,1 core da solo), proxy e
  MinIO si contendono 4 vCPU. Con una macchina per replica e il generatore altrove mi aspetto la p99 delle letture
  sotto 1 ms al 25% (STIMATO). La prova va rifatta su 3 macchine, che però costano: qui non è stata fatta.
- **Più thread di rete** aiutano solo con CPU libera: qui il secondo thread ha peggiorato il 50%.
- **Banco 1b con 5 mutazioni:** stesso limite di CPU. Da solo il banco 1a dice 46.000 record da 5 chiavi per core
  del leader (STIMATO come tetto su una macchina dedicata).
- **Banco 6:** serve il tetto anche sui metadati (§3, proposta). È un limite del prototipo, non della macchina.
- **Banco 5:** non rifatto qui, l'ha rifatto il revisore.

## 5. Come rifare tutto

Da `proto/quaderno` (servono Rust, Go e MinIO compilato in locale in `$MINIO`):

    cargo build --release && cargo test --release
    scripts/banco2.sh results/banco2 120         # RAM, 25/50/100% (~15 min)
    scripts/banco1.sh results/banco1 a b c       # ~20 min
    scripts/banco3.sh results/banco3 60          # ~10 min
    scripts/banco4.sh results/banco4 600         # ~13 min + porcupine
    scripts/banco6.sh results/banco6 600 768     # ~35 min; attenzione: su 16 GB esaurisce la memoria
    scripts/coda-giro2.sh                         # 1, 3, 4, 6 in fila dopo il banco 2

Riassunti: `scripts/riassunto2.py` (banchi 2 e 6), `scripts/analizza6.py`. Cronometro: campo `diag` di
`pq status`.
