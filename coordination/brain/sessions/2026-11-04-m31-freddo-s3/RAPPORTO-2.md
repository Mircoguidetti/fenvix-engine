**PRONTA per la giornata (dopo i passi IAM del fondatore e il suo sì sulla cifra). C1 fatto, C2 fatto, C3 fatto, C4 fatto, C5 fatto, C6 fatto (in questo rapporto), C7 fatto (tutte e quattro le piccole). Spesa attesa circa 0,40–0,50 $, tetto 1,50 $ (somma dei tetti 1,49 $), STIMATI su prezzi DICHIARATI. 0 $ spesi.**

# M31 · correzioni del revisore prima della giornata AWS (RAPPORTO-2)

Sessione Opus lanciata da scheduled task il 2026-11-04, prompt `prompts/M31-correzioni-revisore.md`. Branch `freddo-s3` di fenvix-engine (PR #53), commit 4b2d011, 485b378, 70ee878. Nessuna risorsa AWS, nessun workflow lanciato, nessun `comando.json`, nessun connettore personale, nessun merge. Questo rapporto sostituisce le sezioni 3 (definizione di Q4) e 4 (confronto con Atlas) di RAPPORTO.md.

## In una riga

Le sette correzioni sono nel branch. I tre guasti che il revisore temeva (programma non scaricato, macchina muta, credenziali scadute) li ho simulati in locale: la macchina si spegne da sola, il workflow smette di aspettare, i risultati restano. Tutta la giornata, in piccolo, gira dall'inizio alla fine contro un finto AWS e finisce con "RIMASTO: niente".

## 1. Le correzioni, una per una

**C1 · il cuoco a noleggio si spegne anche se non riesce a entrare in cucina (spegnimento programmato nello user-data).** Lo user-data adesso sta in `giornata/user-data.sh`. La prima riga dopo `#!/bin/bash` è `shutdown -h +240 "M31: tetto" || true`, prima di scaricare qualsiasi cosa. Se le 5 prove di scaricamento falliscono, o se il `tar` fallisce: `shutdown -h now` (e la macchina, che parte con "spegnimento = cancellazione", si cancella). Lo spegnimento a 240 minuti in `esegui.sh` resta, come diceva il revisore.

**C2 · la macchina muta.** Nel ciclo d'attesa di `giornata-ci.sh`: se dopo 15 minuti nel bucket non è mai comparso `risultati/passi.log`, il workflow scrive "MACCHINA MUTA", smette di aspettare, e la pulizia cancella la macchina. Prima aspettava 250 minuti.

**C3 · i risultati non si perdono se il rinnovo delle credenziali fallisce.** Passo nuovo nel workflow, `if: always()`, dopo "Credenziali per la pulizia" e prima della "Pulizia": `giornata/recupera.sh` copia `s3://$BUCKET/risultati/` in `/tmp/m31-out` con le credenziali fresche e toglie il numero dell'account (stessa `sed`). Non fa mai fallire il job.

**C4 · il freno a mano (procedura).** In `PERMESSI.md`: durante la giornata il freno è il bottone "Cancel workflow" nella pagina della run (la pulizia gira lo stesso); `smonta` serve solo dopo, se la pulizia ha scritto "ALLARME"; non fare due volte il commit di `prova` (partirebbe una seconda giornata in coda, circa 0,5 $ in più, STIMATO dal revisore). L'ho scritto anche in testa al workflow.

**C5 · le stesse domande del giro p1-1.5 su Atlas.**
- Q4 (`find_m31`): non più "gli ultimi 500" del comando `estese` (sbagliato). Adesso: per ogni cliente una finestra a caso di 60 giorni dentro la storia dei suoi messaggi, dal più nuovo, `limit 500`. Il carico scrive nella verità del cliente due finestre e quanti documenti deve trovare (min(500, messaggi nella finestra)); la find controlla che siano esattamente quelli e tutti dentro la finestra. Q4 si misura su tutti i clienti di ogni variante: 150 per gruppo sul carico P1 (almeno 60, come chiede p1-1.5); se fossero meno di 60 il programma scrive "AVVISO" e nel risultato c'è `q4_almeno_60`.
- Q1: aggiunta `Q1d`, "i 20 messaggi più recenti prima di una data a caso" (`ts < data`, dal più nuovo, `limit 20`), con la data scelta a caso tra il ventesimo messaggio più vecchio e il più nuovo del cliente. La Q1 di prima (ultimi 20) resta accanto.
- Q5: `--q5-n 60` (prima 20). Siccome 60 finestre da 7 giorni sono 420 giorni e la storia è circa un anno, il passo tra una finestra e l'altra si accorcia da solo (circa 6 giorni a 10 GB, STIMATO dal tempo simulato), così tutte le finestre stanno dentro la storia. Prima, con 60, le ultime sarebbero state vuote.
- Q2 e Q3 si misurano già sui due gruppi (dormienti e attivi): il confronto con gli attivi è nella sezione 4.
- Attenzione per chi scrive p1-1.5: la finestra di Q4 la scelgo **dentro la storia dei messaggi del cliente** (per i dormienti questo vuol dire nei loro primi mesi). Lo script di Atlas deve fare lo stesso, altrimenti per un dormiente una finestra negli ultimi mesi trova 0 messaggi. Lo segnalo nella sezione 7.

**C6 · il confronto con Atlas** è rifatto nella sezione 4, con RISULTATI-CORRETTI e con gli attivi; tolta la proposta di misurare Q4 e Q5 con `estese`.

**C7 · le piccole, tutte.**
- Carico da 2 KB: `--n 50` per gruppo (prima 150), così 4 varianti per 50 = 200 dormienti, sotto i circa 219 disponibili (MISURATO dal revisore): nessun dormiente ripetuto. Conseguenza: lì Q4 ha 50 clienti per gruppo, meno di 60; va bene perché il confronto con Atlas si fa sul carico P1, non su quello da 2 KB.
- Prova "insieme": ogni riga del risultato dice quante find sono su clienti già letti nelle varianti (`find_su_clienti_gia_letti_nelle_varianti`). Nella prova locale erano tutte (30 su 30, MISURATO): dentro il nostro processo sono fredde (bancone svuotato), se S3 risponde più in fretta a un oggetto letto da poco NON SO.
- Freno sulle richieste (`conta.rs`): contatori di tutto il processo, mai azzerati, accesi solo quando il magazzino è S3. Oltre il tetto il programma scrive "FRENO" ed esce con il codice 97; `esegui.sh` lo vede, scrive "FRENO" nei passi, salta alla fine e la macchina si spegne. Tetti per passo, circa 3–4 volte il previsto: tempi P1 250.000 GET (previste circa 70.000, STIMATO), tempi 2 KB 100.000 GET, ogni `scrivi` 100.000 GET e 20.000 PUT (previsti al massimo circa 12.000, STIMATO), 5.000 PUT/LIST per i tempi.
- Immagine solo di Canonical: in `PERMESSI.md` l'immagine ha ora una regola a parte con `ec2:Owner = 099720109477`. Numero dell'account di Canonical DICHIARATO dalla documentazione Ubuntu "Find Ubuntu images on AWS" (https://ubuntu.com/aws/docs/aws-how-to/instances/find-ubuntu-images/, letta oggi: "099720109477 (in the default partition)"). Così il cuoco non può partire con un'immagine a pagamento del Marketplace.

## 2. Le prove dei guasti (C1, C2, C3), in locale

Come le ho fatte: moto (un finto AWS in locale: S3, EC2, IAM, SSM) al posto di AWS; un `aws` finto in testa al PATH che passa tutto a moto e in più fa da "macchina": quando il workflow chiede `run-instances`, esegue lo user-data vero in una cartella a parte, con uno `shutdown` finto che scrive cosa gli chiedono e, se è "now", cancella la macchina in moto e ferma i suoi processi. Gli script girano così come sono nel workflow: `giornata-ci.sh`, poi `recupera.sh`, poi `pulizia.sh`. Un "minuto" d'attesa dura 2 secondi. Script: `giornata/prova-locale/prova-guasti.sh`. Risultato: 0 controlli falliti su 24 (MISURATO, `m31/risultati/prova-locale-correzioni/guasti-c1-c2-c3.txt`).

- **Scaricamento fallito** (link firmato rotto): la macchina chiede prima `shutdown -h +240 M31: tetto`, poi, dopo le 5 prove, `shutdown -h now M31: scaricamento fallito`; risulta cancellata; il workflow lo vede ("MACCHINA_SPENTA") in circa 14 secondi; la pulizia scrive "RIMASTO: niente" (MISURATO).
- **Tar fallito** (pacco rotto): stessa cosa, con "tar fallito" (MISURATO).
- **Macchina muta** (parte ma non scrive mai): "MACCHINA MUTA" dopo 16 minuti finti (33 s), non a 40 (il tetto della prova); fino a lì la macchina è accesa; dopo la pulizia è cancellata e i suoi processi sono finiti; "RIMASTO: niente" (MISURATO).
- **Rinnovo delle credenziali fallito**: le credenziali del workflow scadono a metà giornata; il rinnovo fallisce; la copia di fine giornata fallisce e in mano non c'è niente (è proprio il caso del revisore). Poi `recupera.sh` con le credenziali fresche copia 5 file (passi, tempi, fine), la pulizia cancella il bucket, e i risultati restano (MISURATO).
- **Freno** (`prova-freno.sh`): con il tetto delle GET abbassato a 300, `tempi` si ferma a 301 GET con il codice 97, `esegui.sh` scrive "FRENO … salto alla fine", niente carico da 2 KB né scritture, e `fine.json` arriva nel bucket (MISURATO, `freno-c7.txt`).

Quello che una prova locale non può provare: il rinnovo vero con il gettone OIDC di GitHub (qui fallisce sempre, per costruzione) e lo `shutdown` vero di Ubuntu (qui è finto; che `shutdown -h` su Ubuntu con systemd voglia dire spegnere, e quindi cancellare, l'ha letto il revisore).

## 3. Tutta la giornata in locale, in piccolo

`giornata/prova-locale/prova-giornata-locale.sh`, stessi passi del workflow nello stesso ordine: `permessi.sh` ("PERMESSI: tutti presenti" su moto), pacco del programma (costruito con `--locked`, come il workflow), `giornata-ci.sh` (bucket, macchina, attesa con la riga SPESA, esito "FINITA" in 292 s), `recupera.sh` (19 file), `pulizia.sh` ("bucket cancellato", "RIMASTO: niente"), risultati senza il numero dell'account (MISURATO, `giornata-locale.txt`). Sulla "macchina": carico P1 da 0,3 GB con 300 clienti, copia su moto, tempi con il ritardo finto (mediana 25 ms, sigma 0,6, 90 MB/s), carico da 2 KB da 0,1 GB, tempi, tre `scrivi` da 5 secondi, fine, spegnimento.

Cosa dicono (MISURATO con moto e ritardo finto, 20 clienti per gruppo: servono solo a controllare il programma, non come previsione; moto aggiunge circa 15–20 ms per GET, STIMATO dal revisore). Variante del disegno (parallelo, pile recenti sul bancone, richiesta doppia, lettura in anticipo 256 KB):
- dormienti: Q1 freddo p50 41,8 ms, Q1d 48,9, Q2 90,7, Q3 42,5, Q4 46,8; caldo Q1 0,30 ms; circa 2 GET per find;
- attivi: Q1 54,5, Q1d 54,8, Q2 58,3, Q3 60,8, Q4 48,9; circa 3 GET per find.
- Q4 legge in media 433 messaggi per i dormienti e 500 per gli attivi (MISURATO): la finestra di 60 giorni a volte ne ha meno di 500, come deve.
- Q5: 60 finestre, 100 documenti ciascuna (MISURATO).
- Ogni find ha trovato esattamente i documenti attesi (il programma si ferma se no).

## 4. Noi contro Atlas, ricorretto (C6)

"Freddo meno caldo" a p50. Nostro: variante del disegno, carico da 10 GB in locale con S3 simulato, 150 clienti per gruppo (MISURATO da M31 in locale, STIMATO per S3 vero). Atlas: RISULTATI-CORRETTI.md (M30, tempo visto dal Mac).
- **Q2, un documento per `_id`**: noi 59,5 ms dormienti e 57,9 attivi (2 GET in fila); Atlas 2,15–2,92 freddo meno caldo, 1,75–2,01 freddo meno rete (MISURATO). Atlas circa **20–34 volte** più veloce, sugli attivi come sui dormienti.
- **Q1, ultimi 20 messaggi**: noi 31,6 dormienti, 43,2 attivi; Atlas circa 8–13 (STIMATO, non più 2,2–3,8 che era caldo). Atlas circa **2,5–4 volte** più veloce sui dormienti, circa **3,5–5,5** sugli attivi. Q1d (data a caso) a 10 GB non l'ho ancora misurata: la misura la giornata; mi aspetto lo stesso di Q1 (STIMATO: stesse GET nella prova piccola).
- **Q3, circa 85 eventi**: noi 26,4 dormienti, 42,3 attivi; Atlas circa 45–65 (STIMATO dal revisore B; A2 stima 30–45). Il giro p1-1.5 misura Q3 sugli **attivi** mai letti, quindi il confronto giusto è con i nostri attivi: con la stima di B noi circa **1,1–1,5 volte** più veloci, con quella di A2 tra circa 1,4 volte più lenti e 1,1 volte più veloci. Detto semplice: sugli attivi siamo **circa pari**; il vantaggio di 1,7–2,5 volte vale solo per i dormienti.
- **Q4 e Q5**: Atlas NON SO. Si misurano nel giro p1-1.5 (60 clienti con finestra a caso di 60 giorni, `limit 500`; 60 finestre di Q5), **non** con il comando `estese`, che è sbagliato. La nostra Q4 nuova a 10 GB non è ancora misurata: STIMATO circa come la Q4 vecchia (dormienti circa 29 ms, attivi circa 44), perché 500 messaggi da 310 byte sono circa 155 KB e stanno nella lettura in anticipo da 256 KB (una GET per pila).
- **Byte scritti per byte inserito**: Atlas circa 4 a lotti con documenti da 340 byte, circa 3,2 a scritture singole da 1,9 KB (STIMATO dai grafici). Noi 5,0 durante il carico con documenti da 310 byte (MISURATO in locale): circa **1,25 volte di più**, non la metà come diceva RAPPORTO.md. I due numeri non hanno lo stesso denominatore (noi chiave più valore, Atlas byte BSON) e da noi i byte vanno su S3 (si pagano a PUT, non a disco). Il confronto con il 3,2 lo dà la giornata (`scrivi`, documenti da 1,9 KB).
- Ricordo: i tempi di Atlas sono visti dal Mac (rete sottratta), i nostri dentro il processo. Il confronto giusto è con il tempo lato server del profiler (`storage.data.timeReadingMicros`, punto A del revisore B), che arriva con p1-1.5.

In cucina: quando serve una cosa sola (un documento), la dispensa veloce accanto al cuoco di Atlas (Page Service su NVMe) vince di circa 20–30 volte; sugli ultimi 20 messaggi vince di circa 3–5 volte, non 10; sui tanti documenti dello stesso cliente noi vinciamo con chi non scrive da tempo e pareggiamo con chi scrive ancora. "Sempre due numeri" (DECISIONS 2026-11-04) non cambia.

## 5. La cifra, ricalcolata con le correzioni

Prezzi DICHIARATI (ricontrollati oggi dal revisore M31): c7i.xlarge eu-west-1 0,1915 $/ora; disco gp3 0,088 $ per GB al mese; IPv4 0,005 $/ora; S3 GET 0,0004 $ e PUT/LIST 0,005 $ ogni 1.000 (NUMBERS.md); traffico EC2–S3 nella stessa regione gratis.

Cosa cambia rispetto a prima (STIMATO): Q1d aggiunge circa 2.100 find sul carico P1 (circa 8.000 GET e circa 2 minuti); Q5 da 20 a 60 finestre circa 4.700 GET e 20 secondi; il carico da 2 KB con 50 clienti invece di 150 toglie circa 6.500 GET e qualche minuto. Quasi pari.

Attesa (STIMATO):
- macchina circa 1,5–1,8 ore: 0,29–0,35 $;
- disco 60 GB: circa 0,01 $; IPv4: circa 0,01 $;
- GET circa 75.000–100.000: 0,03–0,04 $;
- PUT e LIST circa 8.000–20.000: 0,04–0,10 $;
- spazio S3: meno di 0,01 $.
- **Totale circa 0,40–0,50 $.**

Tetto, cioè il peggio possibile con tutti i freni (STIMATO):
- macchina: si spegne da sola 240 minuti dopo l'avvio (C1, anche se il runner muore), il workflow la cancella a 250: al massimo circa 4,2 ore, 0,80 $;
- disco 0,03 $, IPv4 0,02 $;
- GET: somma dei tetti del freno 650.000 = 0,26 $;
- PUT e LIST: somma dei tetti 70.000 = 0,35 $;
- copie, risultati ogni minuto e attesa del workflow (non passano dal freno, ma sono legate al numero dei file e dei minuti): circa 0,03 $.
- **Somma 1,49 $: il tetto resta 1,50 $.** Con la macchina muta (C2) si spende circa 0,05 $; con lo scaricamento fallito (C1) pochi centesimi. Il caso "senza tetto" del revisore (runner morto e macchina accesa) non c'è più: lo spegnimento a 240 minuti parte prima di tutto.
- GitHub Actions: repo privato, fino a circa 4,5 ore di runner dai minuti gratuiti del mese; quanti ne restano NON SO.

## 6. Cosa ho deciso io

- Lo user-data in un file a parte (`user-data.sh`), così la prova locale esegue esattamente lo stesso testo che riceve la macchina.
- Tempi del workflow regolabili solo per la prova locale (`MINUTO_S`, `MUTA_MIN`, `RINNOVO_S`, cartelle `M31_OUT` e `M31_PACCO`): sul runner valgono i valori di serie (60 s, 15 minuti, 45 minuti, `/tmp`).
- Q4: finestra dentro la storia del cliente (sezione 1). Q1d: data uniforme nel tempo tra il ventesimo messaggio più vecchio e il più nuovo. Due finestre e due date per cliente, come per Q3.
- Q5: passo accorciato quando 60 finestre non stanno nella storia.
- Freno per passo e non per giornata intera: ogni passo è un processo diverso; con i tetti per passo la somma resta sotto 1,50 $. Ho scelto tetti più stretti di quelli "per esempio" del revisore (1 milione GET, 200.000 PUT), che da soli avrebbero portato il tetto oltre 1,50 $.
- Carico da 2 KB con `--n 50` (prima opzione del revisore) invece di dirlo soltanto.

## 7. Cosa manca o blocca

- Blocca la giornata: i passi IAM del fondatore (`PERMESSI.md`, con la regola nuova `SoloImmaginiDiCanonical` nella policy `fenvix-m31`) e il suo sì sulla cifra.
- Rischio che resta (mio, non provato): se il rinnovo delle credenziali fallisce davvero, il workflow non vede più niente e aspetta fino a 250 minuti, anche se la macchina ha finito prima. I soldi non cambiano (la macchina si spegne da sola alla fine) e i risultati li salva C3; si perdono solo minuti di GitHub Actions. Correzione possibile: uscire dall'attesa dopo qualche minuto di errori di credenziali. Non l'ho fatta perché il prompt chiede C1–C7 come scritte.
- Rischio R5 del revisore (quota di vCPU dell'account, c7i.xlarge nella prima zona): resta; se manca, `run-instances` fallisce subito e si spendono 0 $.
- Lo script p1-1.5 per Atlas non esiste ancora: deve usare per Q4 la finestra di 60 giorni dentro la storia del cliente e per Q1 la data a caso come qui (sezione 1).
- Q1d e la nuova Q4 a 10 GB: le misura la giornata (in locale solo in piccolo).

## 8. Link

- Branch `freddo-s3`, PR #53: https://github.com/Mircoguidetti/fenvix-engine/pull/53 (commit 4b2d011, 485b378, 70ee878).
- Codice: `proto/lavaggio-per-cliente/m31/src/main.rs` (`verita_m31`, `find_m31` Q1d e Q4, `tempi`, Q5), `src/conta.rs` (freno).
- Giornata: `.github/workflows/m31-freddo-s3.yml` (passo "Recupero"), `giornata/user-data.sh` (C1), `giornata-ci.sh` (C2), `recupera.sh` (C3), `esegui.sh` (freno, `--n 50`, `--q5-n 60`), `PERMESSI.md` (C4, Canonical).
- Prove locali: `giornata/prova-locale/` (`prova-guasti.sh`, `prova-freno.sh`, `prova-giornata-locale.sh`, `aws` e `shutdown` finti); risultati in `m31/risultati/prova-locale-correzioni/`.
- Fonte del numero di Canonical: https://ubuntu.com/aws/docs/aws-how-to/instances/find-ubuntu-images/

## 9. Errori o mancanze trovati in fenvix-brain

- Il prompt dice "rapporto con i numeri di RISULTATI-CORRETTI": ho scritto il confronto corretto qui e non ho riscritto RAPPORTO.md (regola "solo aggiunte"); il coordinatore dovrebbe segnare in testa a RAPPORTO.md che le sezioni 3 (Q4) e 4 sono sostituite da RAPPORTO-2.
- PROTOCOLLO o il prompt di p1-1.5 dovrebbero dire dove sta la finestra di Q4 (dentro la storia del cliente), altrimenti i due lati non misurano la stessa cosa.
