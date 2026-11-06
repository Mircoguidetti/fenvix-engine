# NOW

Aggiornato 2026-11-04 16:45 dal coordinatore (sessione nuova: brain riletto per intero).

## Direzione
"Il MongoDB senza dischi": dati solo su S3, per database grandi letti un cliente alla volta (app nuove, soprattutto con agenti, che crescono in fretta). Pezzo difficile: il lavaggio per cliente (PLAN-LAVAGGIO.md). Prezzo: minimo sotto il 47% di Infinite, obiettivo 30–35%.

Cosa abbiamo capito oggi da P1 e R-P1: Atlas Infinite tiene tutte le pagine su dispense veloci in ogni zona (Page Service su NVMe), legge dallo storage in meno di 2 ms. Non vendiamo velocità a freddo (DECISIONS: sempre due numeri). Il nostro vantaggio è il rapporto tra i pavimenti di costo (il loro "HBM": pagine di tutti i dati su dischi veloci, cuochi sempre accesi, fattorini): lo misura B23.

### Irlanda, caricamento (2026-11-05, pomeriggio)
- Run 42053678455 (carica): INCOMPLETO. Atlas ha bloccato le scritture ("InsufficientDiskSpace"): il tetto dello spazio di Infinite era 60 GB, scelto alla creazione del cluster. Errore del coordinatore: non ha confrontato 16 GB + 53 GB con il tetto prima del lancio. AWS 0,31 $, RIMASTO niente. File di stato a metà nel commit c9ed9fa (li rimpiazza il nuovo caricamento).
- MISURATO dalla console: lo spazio usato da Atlas cresce circa 3 volte i GB che scriviamo (+50 GiB per circa 17 GB scritti). "Data Size" si aggiorna in ritardo. Perché: NON SO (storia delle scritture?). Da capire e da scrivere nei numeri di Atlas.
- Il fondatore ha alzato il tetto a 256 GB e spento "Additional Backup Retention". Caricamento 2: run 42060218873, IP 108.130.140.117/32 aggiunto, 54.246.145.174/32 da togliere. Controllo: foto di Data Size a metà; fermare se si avvicina a 230 GB.

- Richiesta del fondatore per l'analisi finale (2026-11-05): il confronto con Atlas non è "battiamo Atlas" da solo. Per ogni taglia vanno messi i suoi tetti e cosa succede quando li tocca: IOPS e banda (M30 5.000 IOPS e 80 MB/s, M10 625 e 10 MB/s, DICHIARATO), connessioni (3.000 su M30), vCPU (2 su M30 Gen2), lo spazio che cresce circa 2,7 volte i dati scritti, il traffico (stesso posto 0–0,01 $/GB in uscita, entrata gratis). Poi: dove si piega M30 (dal carico massimo dall'Irlanda), e quanto costa da noi la stessa capacità. Il paragone giusto è il costo per la stessa capacità, al limite di Atlas e oltre.

### Da discutere con il fondatore dopo i giri (lui ha detto "dopo")
- Picchi di clienti: un cameriere nuovo si accende in decine di secondi (STIMATO) e parte con il bancone vuoto. Servono camerieri di scorta? Va deciso insieme alla scelta "senza cuoco fisso" oppure primario e secondario come Atlas, senza Page Service e con lavaggio.

### Piano deciso con il fondatore (2026-11-05, sera) — sostituisce le catene di revisori dal mattino
Metodo per ogni compito:
1. Prima di partire: l'elenco dei "gravi" del compito (3–5 cose che non possono essere sbagliate), scritto dal coordinatore e approvato dal fondatore in una riga.
2. La sessione di lavoro porta una prova per ogni grave.
3. Revisione: un solo revisore Opus per compito (i token non bastano per due). Controlla l'elenco dei gravi e in più cerca qualunque altro buco grave che l'elenco ha dimenticato; segnala solo buchi gravi (che perdono dati o soldi o cambiano una decisione), ognuno con la prova. Le cose piccole in una nota che non blocca. Niente Fable come revisore di serie.
4. Chiusura: le correzioni dei buchi gravi, poi un solo controllo delle sole correzioni. Fine. Quello che resta aperto lo decide il fondatore.
5. Le correzioni sono sullo stesso branch della sessione di lavoro. Ogni buco grave trovato diventerà un test quando si costruisce la batteria di test (dopo il prototipo, non ora).
6. Le dieci garanzie di un database (elenco chiuso, preso da ACID e dalle prove tipo Jepsen; una garanzia nuova entra solo se viene da lì), gravi fissi per ogni compito che tocca scritture o dati, provate rompendo le cose apposta (eseguite dalla sessione e rifatte dal revisore, $0 in locale): (1) durabilità: nessuna scrittura confermata persa, kill -9 in punti a caso almeno 100 volte anche con una copia del quaderno spenta; (2) atomicità: una scrittura o una transazione avviene tutta o per niente, mai a metà visibile; (3) leggi quello che hai scritto: dopo la conferma ogni lettura la vede; (4) indici sempre coerenti con i dati; (5) isolamento tra clienti; (6) cancellazioni complete e precise, anche dopo il lavaggio e dai pezzi su S3; (7) risposte giuste: migliaia di ricerche a caso confrontate con un mongod vero; (8) un solo scrittore per database, anche durante guasti e cambi (turno numerato nel quaderno, due camerieri che si contendono lo stesso database); (9) nessun dato rovinato quando S3 o la rete sbagliano (errori, lentezze, scritture a metà, riavvii nel mezzo); (10) sicurezza e ripristino: accesso solo con le credenziali giuste, dati cifrati, backup ripristinato davvero. Ogni errore trovato dopo diventa una prova in più.
Stanotte: finiscono le catene già lanciate, nessuna sessione nuova. Niente verifica separata del rapporto di Fable: il tetto di 26.000 documenti/s con indici resta il riferimento finché non lo misuriamo noi.
Al mattino, in ordine: (a) riassunto della notte; (b) il fondatore approva gli elenchi dei gravi di quaderno, lavaggio, attrezzi del cameriere; (c) revisione con il metodo nuovo su quello che è arrivato; (d) decisioni A2 e A6 della cucina e quota AWS; (e) la cucina contro M30 si lancia insieme al fondatore, solo con il quaderno dentro il motore (DECISIONS 2026-11-05): prima Q8d approvato e il workflow della cucina con le 3 copie del quaderno.

### Notte del 2026-11-05 (piano del fondatore)
- In corso: giro M30 corto dall'Irlanda (run 42068989463). Alla fine: analisi A3 (trig_014Rf2JzDzhZBgyudmCj1mfv) lanciata dal controllo del coordinatore; il suo revisore R-A3 (trig_01Ey2UXnu58PU93iLA9yaRof) parte da solo. Chiusura di Atlas con il fondatore.
- Partite subito, $0, ognuna con il revisore in catena (RULES "Il revisore parte da solo, in catena"): Q8c quaderno condiviso come Atlas (revisore trig_01WG8VBU9CZiqmtTcgxRebXh), L1 lavaggio che non finisce e correzioni di K1 (revisore trig_01KasRKiF97wJyxCNvpGs7Pc), T1 attrezzi del cameriere (revisore trig_01DjZpgQWFpVNYLwuW5EBFed). Revisore K2b in corso.
- Catene della notte: le sessioni NON possono fare fire_trigger (rifiutato dal controllo dei permessi, rapporto A3). I passi della catena li lancia il coordinatore con controlli ogni 30 minuti nella sua chat (trig_01NKeZPwuuqY3kAPxEdQEN5P e successivi). Partiti: Q8c, L1, T1, K2c, A3, R-A3 (Fable, con le metriche). partito: R-K2c. partito: R-T1. partito: R-L1 (solo al mattino). partito: R-Q8c (solo al mattino). ERRORE del coordinatore: la sua chat si è sospesa dopo il controllo di mezzanotte e le catene si sono fermate per la notte; Q8d e L1b non sono stati fatti. Finito: R-K2c (APPROVATO CON CORREZIONI).
- Atlas chiuso dal fondatore (Terminate) dopo il giro M30 corto, lo scarico delle metriche e le foto (sessions/2026-11-05-atlas-irlanda-metriche/). Fattura del mese a quel momento: 2,98 $ (DICHIARATO, la console aggiorna in ritardo; la fattura resta visibile dopo il Terminate). Da confermare con lui: chiave API, utente del database, variabili sul Mac, segreto DEMO_DB_URI cancellati.
- Ultimo passo, solo con il fondatore: la nostra cucina contro M30 su AWS. Serve la quota ad almeno 12 vCPU (oggi 8) e le decisioni A2 e A6 di K2b.

## Obiettivi di oggi 2026-11-05 (dal fondatore)
1. Non dimenticare niente su Atlas: oggi il cluster si elimina.
2. Costruire la nostra cucina che attacca Atlas su M10 e su M30, e vedere cosa ci manca per arrivare ai loro numeri.

### Osservazioni di oggi su Atlas Infinite (MISURATO dalla console)
- 11:50: "Search & Vector Search: this feature is not yet available for Atlas Infinite Dedicated clusters". Il fondatore: per gli agenti noi dovremmo averla, ed è già nel piano (sua decisione del 2026-11-05; nel brain finora c'era solo come ricerca: sessions/2026-11-03-database-nuovo, candidato 2, e B8 "vettori circa 10–14 sessioni"). Da aggiungere a DECISIONS e al piano dal coordinatore.
- 11:50: il cambio M10 → M30 dice "0 of 3 servers complete": Infinite cambia 3 server, non 2 (ieri l'API vedeva 5 nomi di nodo).

### Atlas: tutto quello che serve prima del Terminate (il coordinatore spunta ogni voce)
- [x] Scarico iniziale dall'Admin API su M10 (148 su 214: metriche complete dei nodi di oggi; 404 = nodi spenti ieri; 401 = log e fatture).
- [x] Documenti da 2 KB caricati su M10; collezione fredda rifiutata (risposta salvata).
- [ ] Cambio a M30 dopo le 11:49 con foto del prezzo, cache sotto 0,2 GiB, Data Size prima.
- [ ] Giro M30 completo (comando con --tieni-grandi e 102 dormienti), fino a "GIRO FINITO", con il suo atlas_fine.
- [ ] Consegna dei file di M30 nel brain.
- [ ] Cambio a M10 e giro M10 (riga approvata dal revisore di P1-m10), con il suo atlas_fine; consegna.
- [ ] Log dei nodi scaricati a mano dalla console (Download Logs, ogni nodo, periodo più lungo): l'API ha risposto 401.
- [ ] Foto: Data Size dopo, Billing (fatture: l'API ha risposto 401), Namespace Insights per collezione.
- [ ] Controllo del coordinatore: ogni casella della matrice e ogni file nel brain, poi "tutto salvato".
- [ ] Terminate, cancellazione della chiave API, dell'utente del database e dell'IP nella lista di accesso.

## Sessioni di oggi e da quale account (per non lanciare doppioni)
- Da questo account (coordinatore): P1 giro M10 (trig_01Sv3jGjMtx3UdazrUd4ZuXS) e il suo revisore (trig_019qi6esjKG7tDwWoN6F2H5a); M31 metriche CloudWatch (trig_01Mn3fxGrkGGn5nDQhetCe4d) e il suo revisore (trig_01B4WPCCLnupisVCg6BRAjvB).
- Da questo account, 12:19: P1 code dall'Irlanda (giro corto Q1, Q2, Q3 da una EC2 in eu-west-1 su 150 attivi mai letti, dopo M30 e prima di M10) e il suo revisore. Il giro M10 scende a circa 334 attivi.
- Dall'altro account (fondatore): I1 inventario (FINITO, rapporto alle 11:51), R-I1, Q8 e R-Q8 FERMATI alle 12:29 (crediti finiti); Q8 aveva già lavoro sul branch quaderno-corsie (ultimo commit 12:03).
- Da questo account: R-I1, Q8 (riprende dal branch quaderno-corsie) e R-Q8 creati alle 12:30 ma partiti davvero solo alle 12:52 (errore del coordinatore: non li avevo avviati).
- Da questo account, 13:26: P1 Irlanda v2 (parallelo 10 e 50, carico massimo a più copie; scelta A del fondatore alle 13:24, M10 avrà circa 150 attivi) e il suo revisore. Vale solo l'approvazione della versione 2: il revisore della v1 (12:19) controlla un piano superato.
- 13:31, decisione del fondatore: il giro M10 si fa tutto dalla macchina in Irlanda, non dal Mac (code pulite, parallelo e carico massimo validi). Restano sul Mac solo atlas_inizio e atlas_fine (chiave API solo sul Mac). Da verificare sul rapporto v2: se il workflow accetta il comando dello script come parametro basta quello, altrimenti piccola modifica.
- 14:02, decisione del fondatore (DECISIONS.md): dopo il giro M30 dal Mac, UNA sessione con revisore legge tutti i rapporti e i dati e decide cosa misurare dall'Irlanda su M30 e M10; si misura tutto, con clienti freddi e dormienti per ogni prova. Irlanda v2 resta ferma fino a quel verdetto.
- Da questo account, 14:36: Analisi Irlanda e direzione HBM/luce (prompts/A-analisi-irlanda-direzione.md) e il suo revisore. Giro M30 dal Mac finito alle 14:16, consegna completa nel brain (470 file controllati, foto Data Size, cluster, Query Insights, Billing). Cluster acceso su M30 ad aspettare.
- 14:42–14:47: configurazione AWS per l'Irlanda fatta dal fondatore (trust policy con code-irlanda, fenvix-coldread-test con c7i.4xlarge/8xlarge, CloudWatch e quota, fenvix-walbench con i due tipi, segreto DEMO_DB_URI). Controllo permessi run 42038100140: tutti OK tranne la quota vCPU on-demand standard = 8. Richiesta di aumento a 16 inviata, AWS ha aperto un ticket (tempi: NON SO). Con 16 si usa c7i.4xlarge con 10 copie (già previsto dal workflow). Se al lancio la quota è ancora 8: piano B c7i.2xlarge, serve una riga nel workflow e nelle due policy, con revisore.
- 14:53: piano B pronto. Workflow con c7i.2xlarge (commit 026ae9e, revisore lanciato: prompts/R-piano-b-2xlarge.md), c7i.2xlarge aggiunta dal fondatore a fenvix-coldread-test e fenvix-walbench. Controllo permessi run 42039703042: PERMESSI tutti presenti, quota 8 su 8, RIMASTO: niente. Si può partire con 8 vCPU (5 copie) appena l'analisi dice cosa misurare; con la quota a 16, c7i.4xlarge.
- 14:59: fermate dal coordinatore (task cancellati, con le loro sessioni) "P1 · giro su M10 dopo M30" e "Revisore · P1 giro M10": piano superato dalla decisione delle 13:31. Il rapporto e PASSO-7b restano nel brain.
- 15:02: pulizia. Cancellati 23 task finiti (sessioni di lavoro e revisori di oggi e del 2026-11-04; i rapporti restano nel brain). Restano in corso: Analisi Irlanda e direzione, il suo revisore, Q8 (nessun commit dalle 12:52; se alle 15:30 niente, si ferma). Il revisore di Q8 rilanciato alle 12:52 non risulta più tra i task: se Q8 consegna, va rilanciato. Lasciati i promemoria legati a sessioni del coordinatore e quello di trusio (altro progetto).
- 15:04: Q8 (ripresa) fermata dal coordinatore su richiesta del fondatore (più di 2 ore senza commit). Lanciate da questo account Q8b (prompts/Q8b-quaderno-corsie-prove.md: stesso compito, riparte dal branch quaderno-corsie, salva dopo ogni livello in PROGRESSI.md) e il suo revisore (prompts/R-Q8b-quaderno-corsie.md).
- 15:08: log di mongod NON scaricabili su Infinite: la console dice "MongoDB process log download for Atlas Infinite clusters is not yet available" (DICHIARATO, foto 2026-11-05_1508_log_non_disponibili_Infinite.png). La voce della lista di chiusura "scaricare i log a mano" è chiusa: non esistono.
- 15:12: obiettivo 2, banco di prova della nostra cucina (DECISIONS 15:10). Da questo account: K1 server pronto per lo script (branch server-per-script) e K2 workflow cucina minima su AWS (branch cucina-minima), ognuno con il suo revisore (prompts/K1-*, K2-*, R-K1, R-K2). Nessun lancio AWS senza il sì del fondatore.
- 15:15: il fondatore riporta il cluster a M10 (decisione: M30 acceso il meno possibile). Ordine dall'Irlanda: prima M10, poi cambio a M30 solo per la sua prova (circa 1 ora), poi scarica_atlas dal Mac e Terminate. Il cambio svuota il bancone: stessa condizione del giro dal Mac (partito subito dopo il cambio M10→M30 delle 11:50). L'analisi Irlanda è partita sapendo M30 acceso: il coordinatore adatta l'ordine al lancio e lo segnala al suo revisore.
- 15:33: fermati i 4 revisori in attesa (analisi, Q8b, K1, K2) per non consumare token (RULES: il revisore parte quando il lavoro è consegnato). Prompt pronti: R-A-analisi-irlanda-direzione.md, R-Q8b-quaderno-corsie.md, R-K1.md, R-K2.md; il coordinatore li lancia appena arriva ogni rapporto.
- 15:42, domanda aperta del fondatore (da discutere quando arriva l'analisi Irlanda, non prima): stesso disegno di Atlas (primario + secondario, quaderno, S3) togliendo solo il Page Service e aggiungendo il lavaggio per cliente, invece del disegno senza cuoco fisso. Il coordinatore la ritiene la strada più probabile per battere M30 presto (STIMATO); da valutare con i numeri di oggi.
- 15:40 analisi Irlanda consegnata (PRONTO, piano v3: carica su M30, giro M10, giro M30). 15:56 revisore: APPROVATO CON CORREZIONI (carico 1,7–2,5 h; cambio a M10 subito dopo il carico; un programma solo per taglia m10-tutto/m30-tutto; pause 70 s da decidere). 16:06 lanciata A2 (prompts/A2-irlanda-correzioni.md) per le correzioni; il programma carica non cambia e può partire subito.
- Da questo account, 11:25: M31 rapporto giornata S3 vero (trig_01Jt9cFEXBYPXA7VgJ5Cz9SY) e il suo revisore (trig_01UfVZh9f7GrEYeuv2ufxU5t).
- Regola: prima di lanciare una sessione il coordinatore chiede al fondatore da quale account, e la scrive qui.

## Oggi 2026-11-05, decisioni del fondatore sul giro Atlas
- Dopo M30 si fa anche un giro su M10 con lo stesso script (riga di comando preparata dalla sessione P1-m10, trig_01Sv3jGjMtx3UdazrUd4ZuXS); lo script scaricato sul Mac non si tocca.
- Giornata M31 su S3 vero partita alle 10:05 (run 41999425773, istanza EC2 c7i.xlarge tag project=fenvix-coldread). Detailed monitoring CloudWatch acceso dal fondatore alle 10:56. Oggi, a fine giornata M31: scaricare le metriche CloudWatch dell'istanza (CSV dalla console o API con cloudwatch:GetMetricData e ListMetrics in sola lettura) e metterle nel brain.
- Dormienti: 34 tenuti da parte per M10. Comando M30 con `--dorm-q2 30 --dorm-q1 18 --dorm-q1d 18 --dorm-q3 18 --dorm-q4 18` (102 su 136), controllato prima con `--a-secco`. Il comando M10 usa i 34 restanti (il coordinatore adatta la riga della sessione e la controlla con `--a-secco`).

## Piano di domani 2026-11-05 (deciso con il fondatore il 2026-11-04 sera)
1. Catena della notte per lo script Atlas (solo M30, tutte le combinazioni, raccolta completa): (1) revisore p1-1.5 partito 20:16; (2) p1-1.6 matrice partita 20:46, aspetta il revisore; (3) revisori A e B indipendenti alle 23:00; (4) correzioni alle 01:30; (5) revisore finale alle 03:30. Ognuno aspetta il rapporto del passo prima fino alle 08:00 del 2026-11-05, poi scrive BLOCCATO. Al mattino il coordinatore legge tutti i rapporti: il giro parte solo se il revisore finale scrive PRONTO PER IL GIRO.
2. Domattina il fondatore gira lo script su Atlas seguendo GIRO-FINALE.md. Tutti i dati grezzi nel brain (sessions/2026-11-04-protocollo-infinite/). Il cluster si spegne SOLO dopo che il coordinatore ha controllato che c'è tutto (fasi, profiler, metriche e log dall'Admin API, fattura).
3. Tutti i dati di Atlas a Fable.
4. Giornata M31 su AWS dopo la modifica alla policy fenvix-coldread-test (Letture: DescribeVolumes, DescribeAddresses, testo in RAPPORTO-4 e PERMESSI.md) e il sì sulla cifra (attesa circa $0,50, blocco a $5).
5. Poi il piano del prototipo della cucina intera su AWS a tre taglie (piccola, grande, enorme), da discutere con il fondatore prima di toccare l'architettura.
6. Con i numeri misurati delle due parti: dove migliorare, dov'è l'HBM, dove battiamo Atlas. Niente nuove analisi sulla carta prima di questi numeri.

## In corso
- **P1 · prova su Atlas Infinite.** Cluster0 (Project 0, Irlanda) è su **M10**, dati caricati (10 GiB). Risultati validi: `sessions/2026-11-04-protocollo-infinite/RISULTATI-CORRETTI.md` (revisione a tre in `revisione/`: A1, A2, B tutti d'accordo). RISULTATI.md ha errori, non usarlo. Prossimo passo da decidere con il fondatore: un ultimo giro su M30 con uno script nuovo p1-1.5 (correzioni in `revisione/REVISORE-A2.md` e `REVISORE-B.md` sezione 5), scritto da una sessione di lavoro e controllato da un revisore, non dal coordinatore. Da dove lanciarlo: CloudShell NON funziona (account in verifica, solo CloudShell); le prove AWS si fanno da GitHub Actions in eu-west-1 (ruolo OIDC, come walbench e S13: ogni prova nuova vuole il branch nella trust policy e i suoi permessi IAM, dal fondatore). Proposta del coordinatore: una prova "macchina in Irlanda" riusabile sia per l'ultimo giro Atlas sia per la giornata di M31; alternativa: dal Mac con il profiler. Data Size da riguardare dopo le 11:49 ora italiana del 2026-11-05 (oplog 14,8 GiB, finestra minima N/A, ipotesi 24 ore). Il fondatore spegne il cluster Atlas domani 2026-11-05, dopo l'ultimo giro. Dati grezzi: `sessions/2026-11-04-protocollo-infinite/risultati_p1.json` (copia del 2026-11-04 15:38, senza segreti); cronologia, script del mattino v1.0, log parziale del caricamento ed elenco degli screenshot copiati nel brain alle 16:53 (controllati: niente segreti).
- **Attenzione allo script di P1:** `prova_infinite.py` versione p1-1.4 contiene il comando `estese` che i revisori hanno trovato SBAGLIATO (letture "fredde" in realtà calde, connessioni non aperte prima). Non lanciarlo. Gli screenshot del giorno (98, ora italiana nel nome) sono in `sessions/2026-11-04-protocollo-infinite/screenshot/`.
- **M31 · i nostri millisecondi a freddo su S3 vero (preparazione, $0)**: branch `freddo-s3`, ultimo commit 5414921 alle 15:55 (giornata AWS ancora scritta per CloudShell). Sbloccata: alle 16:52 commit 1c62c91 (giornata AWS da GitHub Actions, OIDC, bucket, c7i.xlarge, permessi IAM) ed e843564 (riassunto dei risultati locali). Rapporto consegnato alle 16:54 (sessions/2026-11-04-m31-freddo-s3/, PR #53): dice PRONTA, spesa attesa circa $0,50, tetto $1,50, ma NON ancora revisionata, e il confronto con Atlas usa i numeri sbagliati di RISULTATI.md. Revisore M31 (Opus) avviato alle 17:05 (prompts/R-M31-freddo-s3.md). Revisore M31: DA CORREGGERE PRIMA (smontaggio, recupero risultati, Q4/Q1 come p1-1.5, numeri Atlas). Correzione M31 lanciata alle 17:51 (prompts/M31-correzioni-revisore.md). IAM fatto dal fondatore alle 18:15 con nomi inglesi: branch freddo-s3 nella trust di fenvix-gha-tests (forma normale e con id numerici), ruolo fenvix-coldread-machine con policy coldread-buckets-only, policy fenvix-coldread-test sul ruolo GitHub, Deny di walbench allargato a c6i.large e c7i.xlarge. Dopo la correzione: sessione dei nomi (prompts/M31-nomi-coldread.md), poi modo permessi ($0), poi giornata con il sì sulla cifra. Il resto:, lanciata dal fondatore alle 15:02, con l'aggiunta del fondatore (confronto completo con Atlas). Il prompt dice di valutare CloudShell: non funziona, va usata la strada GitHub Actions. Poi revisore prima di spendere.
- **C12 · scrittura ritentabile applicata due volte quando il database cambia cuoco** (bug grave): FATTO (`sessions/2026-11-04-c12-sessioni-cambio-cuoco/`, PR #52 in bozza, commit f4e1046). Trovato anche l'aggiornamento perso con transazione aperta durante il cambio di cuoco (1 invece di 2 su main), corretto. **Revisore C12**: primo giro BLOCCATO (la sessione non vedeva fenvix-engine: lanciata da un coordinatore che aveva solo il brain). Giro 2 lanciato alle 16:47 con fenvix-engine nella sessione del coordinatore (`prompts/R-C12-sessioni-cambio-cuoco.md`); se APPROVATA, merge di #52.
- **B23 · pavimento contro pavimento: FATTO**, corretto con la prova delle scritture (rapporto circa 3–4, non 6): vantaggio di costo minimo circa 3,1–4,6 volte sui database grandi (STIMATO), 1,4–1,6 su 1 TB con poche scritture.

## Lezione del 2026-11-04 (dal fondatore)
Il coordinatore ha lavorato da junior: risposte veloci a memoria dopo la compressione della conversazione, senza rileggere il brain, "tutto a posto" senza controllo, revisori usati solo dopo. Regole che il fondatore ha chiesto: dati delle prove sui concorrenti perfetti; revisori a catena (2–3) sui piani e sui risultati prima di chiedere al fondatore di agire; il coordinatore non scrive codice; dopo una compressione si rilegge il brain prima di rispondere. Da scrivere in RULES.md con il sì del fondatore. In più, proposta del coordinatore dopo l'errore sul revisore C12: prima di lanciare una sessione che legge il codice, il coordinatore aggiunge il repo del codice alla sua sessione (le sessioni lanciate vedono solo i repo del coordinatore).

## In attesa del fondatore
- Promemoria e attività programmate della sessione del 2026-11-04: tutti spenti o finiti (il promemoria P1 delle 9:31 del 2026-11-05 è disattivato). Il nuovo coordinatore crea i suoi.
- Artifact "Architettura Fenvix" (https://claude.ai/artifact/QXvDGmnwvgMHffMnud3GuQ, pubblicare sempre con questo url): da correggere il paragone "Atlas: il checkpoint di WiredTiger" e da aggiornare con quanto visto in P1 (Page Service su NVMe in ogni zona).
- Ultimo giro Atlas: da dove (GitHub Actions in Irlanda o Mac con profiler) e quando.
- Prova Base44 ($0). Emergent ($20) più avanti.

## Fatto oggi (2026-11-04)
Merge #50 (esiti del commit ripetuto) e #47 (il lavaggio scalda il bancone). M29 REGGE (una GET per pila con clienti grandi; 20 TB circa 37% di Infinite con compressione; il compattatore separato di SlateDB ignora blocchi e compressione). R-P1 (Fable): storage di Infinite come Socrates. Decisioni: pannello al livello di Atlas (dettagli), promesse di velocità con due numeri, gruppi sempre caldi o sempre freddi. Idea del fondatore in PLAN-LAVAGGIO: scaffale caldo regolabile.

## Storico
- **2026-11-03 FATTO:** Merge #33 e #34 (perdita di dati corretta). M16, M26 (REGGE di misura), M27, M28 (TLA+ REGGE, 23,9 milioni di stati), C8, B21 (INCERTA verso VALE: Base44), B22 (nessun settore VALE; regola dei 5 TB per 1.000 scritture/s). Decisioni: obiettivi di velocità, 47% come obiettivo di costruzione, prezzo 30–35%, GDPR 30 giorni. STRATEGY-TRUST: scala dei 4 gradini e prove prima della vendita.
- **17:35 STATO PRIMA DELLA PAUSA**
  - Finite: M15 (bancone piccolo: FERITA vicino a MUORE), M17 (bolletta elastica: FERITA, 1,23–1,34 volte sotto Infinite; i camerieri battono il calcolo di Infinite 5–7 volte ma le nostre voci fisse, $901 in (a), valgono quasi quanto tutto il calcolo di Infinite; "il quaderno è il nostro cuoco fisso"), C6 (PR #33 hello streaming, PR #34 commit ripetuto: da rivedere), revisore M4 (Viva con correzioni, un errore da correggere prima del merge), revisore luce letture (non è una luce).
  - In corso: M16 capo veloce (intermedio: mongod 3 membri w:majority su 2 vCPU 800–1.000 scritture/s con 7 indici).
  - In attesa del fondatore: Fable luce cuochi (prompt pronto, da riorientare dopo M17 sulle nostre voci fisse: quaderno e dispensa di zona), prova gp3 su AWS ($5–15).
  - Dopo la pausa (routine): revisore di C6, correzioni di M4.

- **DA FARE, OBBLIGATORIO (non perdere):** il lavaggio porta le scatole nuove nei banconi prima di pubblicarle (DECISIONS 2026-11-02). Primo passo: verificare se il motore di oggi ha già il problema; poi la correzione con un test. Va anche nella prova P1.

- **15:30** Merge fatti: PR #23 (fix-piccoli), #22 (fix-cs-token), #26 (porta-ingresso). Revisori M12 (Ferita: un quaderno probabilmente non regge il profilo b con la finestra ad albero) e M14 (Viva con correzioni) consegnati. Lanciata C6 (fix hello streaming Node + commit ripetuto di transazione). In corso: M15, revisore M4.

- **M15 · bancone piccolo su tracce vere** (lanciata 2026-11-02 14:13, Opus, $0): simulazione su tracce Alibaba/Tencent/Meta/Twitter, prompts/M15-bancone-piccolo.md. Poi revisore.

Aggiornato: 2026-11-02 07:40, dal coordinatore (dopo la notte).

## In corso

- **S7 · blocco delle scritture**: FATTO (`sessions/2026-11-01-s7-fix-stall/RAPPORTO.md`, PR #11 in bozza). 80 su 80 senza blocchi, test_28 passa, ctl-tests verdi (MISURATO). Inserimenti contro mongod 0,12–0,22 per core. Per il merge manca la revisione indipendente.
- **S1 · quaderno sulla carta**: FATTO (`sessions/2026-11-01-s1-tla-registro/RAPPORTO.md`, PR #8 in bozza). Sessione da tenere aperta: riceverà il bollettino corretto e le correzioni del revisore. Revisore della specifica FATTO (`sessions/2026-11-01-revisore-s1/`): affidabile con correzioni, 2 errori veri nel protocollo in prosa. Messaggio giro 2 pronto: `prompts/S1-giro2-correzioni.md`.
- **S2 · quaderno di prova**: giro 2 consegnato (`sessions/2026-11-01-s2-proto-quaderno/RAPPORTO-2.md`). Superati: banco 4 (0 letture vecchie, porcupine Ok), banco 3 in modalità senza lettura (circa 33.000/s, 0 abort). Non superati: banco 2 al 25% (p99 9,6 ms contro 1 ms; letture da sole 0,97 ms), banco 1b con 5 mutazioni (circa 12.000/s contro 15.000), banco 6 (metadati senza tetto: 2 repliche su 3 morte per memoria). Revisore giro 2 FATTO (`sessions/2026-11-01-revisore-s2-giro2/`): opzione (c), prima banco 6 col freno in locale, poi una sola giornata C. Decisione del fondatore in attesa.
- **S8 · cameriere più veloce**: FATTO il giro 1 (`sessions/2026-11-01-s8-perf-letture/RAPPORTO.md`, PR #10 in bozza). Letture +48% sul grande e +65% sui piccoli; contro mongod 0,31 e 0,67 per core (soglie 0,5 e 0,8 non raggiunte). Resto: 3 letture di metadati per comando (59% della CPU sul grande), cache in `dbs.rs` dopo il merge di S7 (STIMATO 0,77 e 1,0).
- **Sessione Lettura (S3)**: FATTO (`sessions/2026-11-01-sessione-lettura/RAPPORTO.md`, copia dal branch `proto-lettura`, PR #9 in bozza). Passi 1–4 superati; passo 5 superato con la variante a2 (database piccolo riscritto intero ogni 10 minuti): S3 $0,048 al mese per database (MISURATO in locale con MinIO), ma RAM del registro fino a 690 MB ogni 1.000 database per replica (MISURATO). Stesso punto del revisore S2: serve il tetto di memoria.
- **S9 · impianto, il codice**: UNITO a main di `fenvix-infra` il 2026-11-01 (PR #1, commit 37ac5a6). 129 test su 129 (MISURATO dal coordinatore). Punti rimandati in `fenvix-infra/docs/da-fare.md`. Prossimo: giornata A, con il sì del fondatore (spesa attesa circa $0,70, tetto nel codice $2,05).

- **S10 · compatibilità MongoDB**: FATTO (`sessions/2026-11-01-s10-compatibilita/`). 89,8% dei test ufficiali eseguiti (626 su 697), 2 bug che toccano i dati, 10 buchi. PR in bozza su `compat-misura`.
- **Fable · letture e affinità**: FATTO, rivisto ("approvabile con correzioni": buco nel bollettino, record DNS, rete di sicurezza per i getMore). Decisioni del fondatore in attesa.
- **Fable · contro Firestore**: FATTO. Revisore lanciato (prompt `prompts/R-contro-firestore.md`); poi decisioni del fondatore.

- **S11 · bug di compatibilità**: FATTO (`sessions/2026-11-01-s11-bug-compatibilita/RAPPORTO.md`, PR #12 in bozza). I 2 bug che perdono dati corretti (più un terzo caso trovato), 10 test di regressione, ctl-tests verdi (MISURATO). Per il merge manca la revisione indipendente.
- **Opzione da decidere dopo la prova EFS**: quaderno ibrido per fascia (3 server per latenza e più regioni, EFS per costo e scala), stessa interfaccia; ha senso solo se EFS fa risparmiare molto.
- **Fable · database grande**: FATTO (`sessions/2026-11-01-fable-database-grande/`). Regge con 4 aggiunte; sui grandi con molte scritture vantaggio 1,5–2 volte, non 3–5. Decisioni del fondatore in attesa.
- **Fable · catalogo del ristorante pieno**: FATTO (`sessions/2026-11-01-fable-catalogo-ristorante/`): 86 app, metodo di confronto, piano. Decisioni del fondatore in attesa (manca mongod nell'ambiente).

- **Fable · idea pazza**: FATTO (`sessions/2026-11-01-fable-idea-pazza/`). Sopravvive l'idea B: quaderno come file su Amazon EFS, scritto dal cameriere di turno, senza repliche né Raft. Revisore FATTO (`sessions/2026-11-01-revisore-idea-pazza/`): vale con correzioni; EFS Elastic fattura almeno 32 KiB per scrittura (costo circa 20 volte quello di Fable), latenza p99 probabile 8–15 ms, 3 buchi nella regola del turno. Proposta: correggere la regola ($0), poi micro-prova su AWS circa $7, tetto $15, con il sì del fondatore.
- **S12 · micro-prova EFS (preparazione, $0)**: lanciata alle 19:37 (`prompts/S12-micro-prova-efs.md`). Regola del turno corretta in TLA+, programma di prova, stack CDK non acceso, scheda. Poi sì del fondatore sulla cifra (circa $7, tetto $15).
- **Fable · idea pazza giro 2**: FATTO (`RAPPORTO-2.md`). Accetta quasi tutte le correzioni del revisore; l'idea si salva solo come EFS Provisioned. Utile subito: comprimere il flusso di replica del quaderno (N2), $2.000–3.000 al mese in meno per database grande caldo (STIMATO), da passare a S2 dopo il giro 3. Tre misure in più per S12 (`prompts/S12-aggiunta-fable.md`).
- **Revisore S7**: FATTO, approvata con 2 correzioni piccole (`prompts/S7-correzioni-revisore.md`). Aperti a parte: motore piantato dopo la perdita del lease con store fermo oltre 30 s; solo 2 lavaggi per processo (scala Atlas).
- **Revisore S11**: FATTO, approvata con 2 correzioni piccole (`prompts/S11-correzioni-revisore.md`). Aperti a parte: collation ignorata dentro le transazioni (cambia dati); sort top-1 senza tenere tutto in memoria; togliere lo schema take/put_back.
- **S12 · micro-prova EFS**: preparazione FATTA (`sessions/2026-11-01-s12-micro-efs/`). Regola del turno chiusa in TLA+ (3 buchi nuovi trovati da TLC e corretti). Stack non acceso (fenvix-infra PR #2, 149 test). Spesa attesa circa $3, tetto $15: serve il sì del fondatore e qualcuno con accesso all'account (CloudShell).
- **Fable · scommesse**: FATTO (`sessions/2026-11-01-fable-scommesse/`). Spot muore come scommessa (0 sulla media, 10–24% sulla grande; resta miglioria più avanti). Sopravvive sulla carta il database a rami (fork istantaneo anche da 1 TB copiando solo il manifest); prova in locale a $0.
- **S11 · PR #12 UNITA a main** il 2026-11-01 (commit 42505e9), dopo le correzioni del revisore.
- **Revisore database a rami**: FATTO (`sessions/2026-11-01-revisore-database-rami/`). Vale con correzioni. Il fork esiste già nel motore (`ctl/fork.rs`, 25 test verdi). Atlas Infinite fa cloni a costo costante (blog MongoDB 8 ottobre 2026): niente fossato, il vantaggio è il prezzo di mille rami che dormono. Difetto di sicurezza nel motore di oggi: il ramo non eredita la lista di IP ammessi. Proposta: TLA+ della raccolta di famiglia + F1 rivista sul motore ($0, circa una settimana).
- **S13 · prova EFS da GitHub**: ferma a $0: il ruolo AWS di GitHub non accetta il branch idea-turno. Aspetta il suo rapporto con il permesso preciso da aggiungere.
- **S14 · bug di sicurezza e dati**: lanciata (`prompts/S14-bug-sicurezza-dati.md`): ramo senza lista IP del padre, collation ignorata nelle transazioni.
- **In coda, sì del fondatore**: lavoro sui rami (TLA+ della raccolta di famiglia, F1 rivista, circa una settimana, $0) dopo S7, S8 e il quaderno.
- **Fable · idee pazze giro 3**: FATTO (`sessions/2026-11-01-fable-idee-pazze-3/`). Sopravvive "zone a manopola" (database grande caldo da circa $2.200 a $1.400–1.000 al mese, STIMATO; cucina di lancio a una macchina circa $90). Strato in casa: motore di storage nostro al posto di SlateDB, 6 tappe, 5–7 mesi. Quasi gratis: affinità DNS per zona del bilanciatore, lotti nel quaderno. Revisore lanciato (`prompts/R-idee-pazze-3.md`). "Zone a manopola": idea favorita del fondatore (2026-11-01 22:42), decisione finale dopo il revisore.
- **S7 · PR #11 UNITA a main** il 2026-11-01 (commit 3c13077), dopo le correzioni del revisore.
- **S2 giro 3**: FATTO (`RAPPORTO-3.md`): banco 6 chiuso in locale (nessuna replica morta, RAM sotto il tetto, zero perse, recupero 126 s, MISURATO). Difetto: 63.095 commit con esito ignoto invece che rifiutati puliti. Lotti non fatti.
- **S14**: FATTO (PR #14). Revisore lanciato.
- **Revisore idee pazze 3**: FATTO. A (zone a manopola) vale con correzioni: risparmio circa 25% non 35%; due zone più testimone costa quasi come una zona; cucina a una macchina $115–130 con quaderno su EBS, accesa solo col primo cliente. B (strato in casa) vale con correzioni: 8–11 mesi; prima una prova "senza cuoco fisso". C: spegnere il bilanciamento tra zone (S15 lanciata); lotti nel quaderno 1,5–2 volte.
- **S15 · PR #3 di fenvix-infra UNITA** il 2026-11-02 00:05 (commit 14b9d95): bilanciamento tra zone spento, NLB su 3 zone, affinità DNS come manopola spenta. Correzione di documentazione del revisore fatta dal coordinatore (giornata A: circa 0,75 $, tetto 2,05 $). 138 test su 138 (MISURATO).
- **N2 · TLA+ del testimone**: FATTO (`sessions/2026-11-02-tla-testimone/`). Due copie più testimone con ricevute: nessuna violazione fino a 9,6 milioni di stati, 7 controlli negativi tutti trovati (MISURATO, TLC).
- **S14 · PR #14 UNITA a main** il 2026-11-02 01:05 (commit 0a12e39): ramo con la lista IP del padre, collation nelle transazioni, suggerimenti TTL, token del padre rifiutato. Correzione di testo del revisore fatta dal coordinatore nella descrizione della PR. Da fare (mattino): `drop_auth_outside` dopo `set_cached` in fork.rs (una riga, consigliata dal revisore), chiave del ramo durante il fork, nome del database nel token di change stream (N7).
- **N3 · misura del lettore**: FATTO. Il nostro lettore fa una lettura puntuale in circa 4 µs contro circa 20 µs di SlateDB 0.17, stessi dati e macchina (MISURATO): 5 volte più veloce, soglia della tappa T1 superata.
- **Revisore idee pazze 4**: FATTO. Vale con correzioni; NON è un fossato strutturale contro Infinite (il loro fondo non sa niente di collezioni: MongoDB potrebbe arrivarci con anni di lavoro). Supabase ha comprato Turso il 2 ottobre 2026 per "un database per ogni agente" (DICHIARATO). Vantaggio circa 10 volte contro Atlas, solo 2–3 volte contro Firestore (STIMATO). Dove muore davvero: la pulizia delle scatole orfane fa un LIST per database ogni 10 minuti in proto-lettura; si risolve con l'inventario di S3.
- **Fable · caccia alla luce**: FATTO (`sessions/2026-11-02-fable-caccia-alla-luce/`). Nessuna luce tecnica; la più vicina è il quaderno senza capo ordinato dagli orologi di AWS. Revisore lanciato.
- **S16 · quaderno giro 4**: FATTO (`sessions/2026-11-01-s2-proto-quaderno/RAPPORTO-4.md`). Esiti ignoti sotto il freno: causa nel cameriere, da 3.062 a 0, zero perse (MISURATO). Lotti: VINCONO, CPU del capo per record −63/64% con lotti da 16, −74% con 32 (MISURATO, soglia 40%). Compressione zstd 1: byte sul filo −51% (singole) e −66% (lotti da 16), circa 7–10 µs di CPU del capo per record (MISURATO). Macchina piccola (2 vCPU).
- **N5 · sort senza tenere tutto in memoria**: FATTO, PR #17 in bozza. Aspetta il revisore notturno.
- **N6 · TLA+ dei rami**: PR #18 aperta, rapporto in arrivo.
- **N5 · PR #17 UNITA a main** il 2026-11-02 03:10 (commit 451caec): sort top-k in streaming, sort su `_id` con l'indice `_id`. Correzioni del revisore notturno fatte dal coordinatore (test rinominato in test_32, limite di memoria scritto nella PR). Compiti a parte: strada "indice che non dà l'ordine" a blocchi; tetto di memoria del sort (errore 292 oltre 100 MB).
- **N1 · prova locale della manopola delle zone**: FATTO, tutte le soglie superate (PR #19, base proto-quaderno). 4 morti di "zona" con cartelle cancellate: cella ricostruita da S3, zero scritture confermate perse oltre la finestra, ripristino fermo al buco, ritardo della copia visibile (MISURATO, macchina da 2 vCPU, finto S3 moto).
- **Revisori notturni**: N2 testimone (approvata con correzioni: 3 controlli negativi in più; il testimone è un componente nuovo, decide il fondatore), N6 rami (approvata con correzioni; le scelte su spilla ed età minima sono di disegno, decide il fondatore). Specifiche sulla carta: restano sui loro branch, nessun merge.
- **Numeri dei test ctl**: main usa test_29 e test_30; perf-letture usa test_30 (collisione da risolvere al merge di S8); fix-lease-stall usa test_31; sort usa test_32.
- **S8 · giro 2**: FATTO (`sessions/2026-11-01-s8-perf-letture/RAPPORTO-2.md`, PR #10 aggiornata). Cache dei metadati: letture puntuali 0,93 volte mongod sul database grande e 1,16 sui 100 piccoli (MISURATO): il traguardo di 1 volta è quasi raggiunto sulle letture. Scritture ancora 0,36–0,81. Piano fino a 1 volta scritto. Decisione del fondatore: accendere di default il group commit adattivo per i database molto scritti (+41% inserimenti, MISURATO, cambia la latenza)? Per il merge: revisore (notturno 2) e collisione del numero test_30 da risolvere.
- **S1 · giro 2**: FATTO (`sessions/2026-11-01-s1-tla-registro/RAPPORTO-2.md`). I 2 errori veri del protocollo corretti e verificati; buco tra 60 s e scarico chiuso; modello con 3 cambi di capo esplorato tutto; memoria e scarico scritti come nel prototipo; bollettino regge in un modello piccolo, 9 controlli negativi trovati. Due modelli larghi non finiscono sulla macchina piccola.
- **Revisori notturni**: approvate con correzioni anche N1 (prova zone, numeri rifatti e confermati), N3 (lettore) e S16 (quaderno giro 4, 0 esiti ignoti confermato).
- **N4 · motore piantato dopo la perdita del lease**: FATTO (PR #20). Revisore notturno: approvata con correzioni; prima del merge va chiuso un caso nuovo nell'erase (non cancellare gli oggetti se l'abbandono non è finito entro il tetto) più due ritocchi. Da lanciare al mattino una sessione di correzione.
- **Revisore notturno S1 giro 2**: approvata con correzioni (query per indice e disco condiviso).
- **S8 · PR #10 UNITA a main** il 2026-11-02 07:35 (commit 71a435e): letture e cache dei metadati. Letture 0,93 volte mongod sul database grande e 1,16 sui piccoli (MISURATO da S8). Correzione obbligatoria del revisore fatta dal coordinatore (test rinominato in test_33); l'albero unito con main compila (cargo check, MISURATO dal coordinatore). Compiti a parte: tetto in byte della cache e contatore in tenantStats; prova di lettori in parallelo con creazione e cancellazione di indici.
- **Lanciate alle 08:53 del 2026-11-02 (coda prompts/CODA-MATTINO-2026-11-02.md, tutte a $0)**: M1 correzioni N4 (fix-lease-stall, poi merge #20); M2 token di change stream (fix-cs-token); M3 piccole correzioni (fork.rs, tetto sort 100 MB, indice a blocchi); M4 S4 tessera a punti (isolamento); M5 P3 porta d'ingresso e limite per IP (porta-ingresso); M6 P1 un milione di database (prova-milione); M7 correzioni TLA+ (tla-registro, tla-testimone, tla-rami). Poi, dopo le decisioni del mattino: M8 prova senza cuoco fisso (senza-cuoco), M9 misura del maître (misura-maitre), M10 letture k su n (prova-kn). Ordine dei merge sul motore: M1 (#20) → M3 → M2 → M5 → M4 (S4) → seguito S8 (tetto cache, group commit con tetto). M6–M10 sono prove: nessun merge su main. Dopo S4: tetto in byte della cache di S8 e prova dei lettori in parallelo. In ufficio: parte AWS (permessi EFS, giornata A, giornata C).
- **Ordine dei merge sul motore (aggiornato)**: S11 #12 (fatto) → S7 #11 (fatto) → S14 #14 (fatto) → N5 #17 (fatto) → S8 #10 (fatto) → N4 #20 (dopo le correzioni) → S4 → S8 #10 (dopo la cache) → S4.

## Pomeriggio 2026-11-02 (12:45)
- **N4 · PR #20 UNITA a main** (8b14f9b): correzioni del revisore fatte da M1, suite intera 513 passati, 0 falliti (MISURATO).
- Correzioni dei revisori in corso: C2 (fix-cs-token), C3 (fix-piccoli), C5 (porta-ingresso). Revisore di M4 (isolamento) in corso. M8 corretto (RAPPORTO-2).
- Prove finite: M10 letture k su n MUORE (la richiesta doppia fa lo stesso); M11 cucitura vince solo con due cambi al formato, guadagno in dollari piccolo; M13 record a differenza VINCE (circa −60% di byte tra zone, MISURATO).
- Luce: revisore di Fable = "due corsie" Ferita (60% che non valga la pena), "la dieta" (record a differenza) Viva e porta quasi tutto il guadagno; non è stile Olix. Strada nuova del revisore: MSK (replica tra broker gratis). Da discutere con il fondatore.
- In attesa: M12 (tetto del quaderno), M14 (driver senza bilanciatore).

## In attesa

- **S4 · tessera a punti**: si lancia dopo S8 (che dopo il merge di S7 fa la cache in `dbs.rs`).
- **Prova del database grande** (per esempio 1 TB, decine di migliaia di operazioni al secondo, database diviso in pezzi): da progettare; serve a non costruire un prodotto solo per i piccoli.
- **Il ristorante pieno** (50–100 app realistiche, confronto con MongoDB 8.0): dopo S10. Da decidere chi costruisce il catalogo.
- **Prova per confronto con l'AI** (query generate, risposte confrontate con mongod): dopo S10, con i crediti API del fondatore (verificare la loro scadenza).

## Da mandare a S1 dopo il suo revisore

- Correzioni del revisore della specifica; il bollettino con le correzioni del revisore di letture e affinità; le due regole di scarico (età/dimensione/RAM e scarico d'emergenza); la cadenza di scarico da decidere.

## Ordine dei merge sul motore

S7 → S8 (cache dei metadati, messaggio pronto in `prompts/S8-cache-metadati.md`) → S4. I prototipi (`proto-*`, `tla-registro`) restano in PR in bozza finché non si decide come collegarli.

## Prossimi passi del coordinatore

- Controllare ogni rapporto contro le soglie del suo prompt.
- Calcolare il costo fisso della flotta per database quando arrivano i numeri di S2 e della Sessione Lettura.

## Prima della giornata A

- Delega di `db.fenvix.dev` a Route 53: probabilmente già fatta in Cloudflare con lo stack vecchio (il fondatore ricorda record `db`); da verificare. Se c'è, la base nuova riusa la zona con `FENVIX_HOSTED_ZONE_ID`.
- `cdk bootstrap`, base accesa almeno un'ora prima, tag di costo attivati il giorno prima, controllo dei nomi già usati dallo stack vecchio (revisore S9, D8).
- Spesa attesa circa $0,70; tetto vero = somma dei tetti delle celle, circa $2 dopo le correzioni. Sì del fondatore sulla cifra.
- Prima delle giornate con il registro vero: rimontaggio del disco dopo un riavvio (D1) e aggiornamenti un nodo alla volta.

## Regola sui connettori

- Le sessioni lanciate dal coordinatore vedono i connettori personali del fondatore (Gmail, Drive, Calendar). Le sessioni di lavoro non li usano mai.

## Infrastruttura AWS oggi

- eu-west-1: una EC2 t3.medium (stack CDK in `fenvix-app/infra`), ferma dal 2026-11-01. I bucket dati e artefatti restano. Il budget di quello stack non scatta con il credito (`includeCredit`).
- Credito AWS: circa $133 rimasti, scadenza ottobre 2027.
