# proto/senza-cuoco (prova M8)

Due `mongo-server` servono lo stesso database sopra `proto/quaderno`, senza proprietario con lease.

- Codice nel motore: `mongo-server/src/qmode.rs` (acceso con `--quaderno host:porta,...`), più piccoli agganci in
  `dbs.rs` (letture a S, commit validato), `cmd.rs` (il comando si rifà sul conflitto), `coll.rs` (chiave di guardia
  degli indici unici), `natural.rs` (blocchi dell'ordine naturale presi da più processi), `retry.rs` (scritture
  ritentabili), `error.rs`, `main.rs`.
- Prove: `python3 prova.py tutto` (serve `cargo build --release -p mongo-server` e `proto/quaderno` compilato,
  `pip install pymongo`). `python3 prova.py correzioni` lancia solo le prove aggiunte da M8-correzioni. I server
  partono con `--replica-set` e i client con `retryWrites=true`; `SC_RS=0` torna al modo della prima prova.
  `SC_MS=<binario>` usa un altro mongo-server, `SC_RES=<cartella>` scrive i risultati altrove.
- Risultati in `results/`. `results/prima-correzione-*` sono le corse fallite che hanno mostrato i difetti:
  ordine naturale e repliche morte (prima prova), e i tre errori del revisore (`prima-correzione-revisore/`, fatte
  con il binario del commit 0cb05a4).
- Rapporti: `fenvix-brain/sessions/2026-11-02-senza-cuoco/RAPPORTO.md` e `RAPPORTO-2.md`.

## Le prove

Prima prova (M8): `fumo`, `stesso_id`, `stessa_chiave`, `inc`, `cambia_chiave`, `morte_server`, `morte_capo`,
`senza_guardia` (negativa: deve mostrare doppioni).

Aggiunte da M8-correzioni:
- del revisore, in gara: `cancella_reinserisci`, `toglie_rimette`, `upsert`, `coda`, `letture_lineari`;
- errore 1 (ritentativo su un altro cameriere): `ritentativo` (i tre casi a mano) e `ritentativi_in_gara` (lo stesso
  comando mandato insieme ad A e a B, migliaia di volte);
- errore 2 (insert di più di 1.000 documenti in un comando): `insert_mille`;
- errore 3 (comando più grande di un record del quaderno): `troppo_grande`.

## Prove da fare (rischi, non corretti qui)

1. **Documento caldo.** Due camerieri, `$inc` su 1 e su 10 documenti, a 2.000 e a 10.000 operazioni al secondo
   offerte. Misurare: aggiornamenti confermati al secondo per documento, quota di comandi rifatti, p99. Soglia
   proposta dal revisore: almeno 2.000 aggiornamenti al secondo su un documento solo. Oggi il tetto STIMATO è circa
   1/W (W = tempo tra "guarda l'ora" e "consegna"), circa 500 al secondo in locale. Prima idea da provare: mettere in
   fila nel cameriere i comandi sullo stesso documento e consegnarli in un biglietto solo.
2. **Latenza tra zone.** Il proxy a ritardo di `proto/quaderno` (`proxy.rs`) con 0,25 e 0,5 ms per verso (oggi il
   minimo è 1 ms: va abbassato), il capo in un'altra zona. Misurare p50 e p99 di insert, find per `_id`, update.
   Soglie proposte: scritture p99 sotto 10 ms, lettura per `_id` p99 sotto 3 ms. Se le scritture restano sopra circa
   15 ms dopo il lavoro sui giri di rete, ci si ferma e si ripensa.
3. **Più comandi in volo per cameriere** sullo stesso database (oggi uno alla volta): 64 in volo su chiavi sparse,
   rifiuti sotto l'1% e almeno 5 volte i comandi al secondo di oggi.
4. **Tempo dei driver tra camerieri**: `afterClusterTime` letto su B dopo una scrittura su A (oggi l'orologio è per
   processo, non la posizione del quaderno).
