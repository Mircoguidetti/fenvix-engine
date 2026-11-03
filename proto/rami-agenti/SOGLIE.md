# M22 · Soglie e metodo (scritti PRIMA di misurare, 2026-11-03)

Domanda: un ramo usa e getta di un agente costa davvero a Xata e Neon "un Postgres con 1 GB di RAM", o un Postgres fermo
tiene poca RAM e si risveglia in fretta, così il loro costo vero è vicino al nostro?

## Soglie (dal prompt M22, di B15, confermate dal coordinatore)

- MUORE: un Postgres fermo tiene meno di circa 30 MB e si risveglia in meno di 300 ms, oppure i tentativi veri durano
  meno di circa 1,5 minuti.
- VIVA: un Postgres fermo resta sopra circa 300 MB, o il risveglio è sopra 1 secondo, e i sistemi degli agenti tengono
  il ramo aperto per minuti; prezzo sotto il 47% del concorrente migliore.
- In mezzo: FERITA.

## Come leggo le soglie (fissato prima)

- "RAM di un Postgres fermo" = somma del PSS di tutti i processi di un'istanza (postmaster, processi di servizio e la
  connessione aperta dell'agente), dopo il lavoro tipico di B15 (100 letture e 20 scritture) e 30 secondi di pausa.
  Riporto anche l'RSS e la caduta di MemAvailable del sistema con 50 istanze (la verità a terra, compresi kernel e
  tabelle delle pagine).
- "Risveglio" = tempo dalla richiesta dell'agente alla prima risposta di una lettura per chiave, dopo 30 s di pausa.
  Tre stati per Postgres:
  1. acceso e fermo (nessuna sospensione): RAM piena, risveglio = latenza di una query;
  2. sospeso con SIGSTOP (o freezer): la RAM resta tutta (non c'è swap), risveglio = SIGCONT + query;
  3. spento (CHECKPOINT e pg_ctl stop): RAM zero, risveglio = avvio di Postgres + connessione + prima lettura, con la
     cache del sistema calda e, a parte, fredda (drop_caches).
  La soglia si giudica sullo stato che dà RAM bassa: se l'unico modo di stare sotto 30 MB è lo stato 3, il risveglio che
  conta è quello dello stato 3.
- Configurazione "realistica tipo Neon/Xata": PostgreSQL 16 (quello disponibile qui, lo dichiaro), shared_buffers 128 MB,
  max_connections 112 (valori di Neon per 0,25 CU, da verificare nella ricerca), autovacuum acceso. In più una
  configurazione "stretta" (shared_buffers 16 MB, max_connections 20) per dare a Postgres la sua occasione migliore.
- Ramo di Postgres = overlayfs sopra la cartella dati del padre spento (copy-on-write, come ZFS di Xata), più un
  postmaster suo. Tempo per creare il ramo = mount + avvio + prima connessione.
- Ramo nostro = fork `clone` di `ctl/fork.rs` (POST /v1/databases/{db}/fork) su un solo mongo-server con
  `--store local`. RAM per ramo = crescita del PSS del processo con 50 rami aperti, divisa per 50. Risveglio: ramo
  ancora aperto, e ramo chiuso dal timer di inattività (riapertura). Con `--store local` non c'è la latenza di S3: la
  aggiungo come STIMA (numero di GET in sequenza × 30 ms) e lo scrivo.
- Database di prova: 1 GB (pgbench scala 68 per Postgres; circa 1 milione di documenti da circa 1 KB per noi) e il più
  grande che entra nei circa 30 GB liberi del disco per Postgres (lo dichiaro). Per noi il costo del ramo non dipende
  dalla taglia (fork a tempo costante, già MISURATO dal revisore dei rami): lo controllo con un secondo padre più grande
  se c'è spazio.
- Durata dei tentativi: dalle tracce di M18 con l'ora (tau2-bench): durata di ogni conversazione dalla prima all'ultima
  chiamata, mediana e percentili. DAB non ha l'ora: non la uso per la durata.
- Conto del prezzo: metodo di B15 (prezzo = costo / 0,55; passa sotto il 47% del concorrente migliore, sul suo costo
  vero), rifatto con la RAM e i tempi misurati qui.

Niente spesa cloud, il motore non si tocca.
