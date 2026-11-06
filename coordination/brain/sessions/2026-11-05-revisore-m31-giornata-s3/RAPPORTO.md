**CONFERMATO CON CORREZIONI: 4 errori provati, nessuno cambia la conclusione principale (la GET su S3 vero sta intorno ai 25 ms a p50; il freddo con il disegno completo resta dentro l'obiettivo, peggio Q2 attivi 50,7 / 105,7 ms, MISURATO dentro il programma di prova).**

# Revisore · rapporto della giornata M31 su S3 vero (2026-11-05)

Sessione revisore (Opus), lanciata da scheduled task, prompt `prompts/R-M31-rapporto-giornata.md`. Non ho scritto il rapporto. Ingresso: `sessions/2026-11-05-m31-giornata-s3/RAPPORTO.md`, commit 73bed90 delle 10:31 UTC. L'ho controllato fermo da almeno 30 minuti prima di consegnare. Dati grezzi: fenvix-engine, branch `freddo-s3`, commit d988e6a, cartella `proto/lavaggio-per-cliente/m31/risultati/aws-41999425773/`, in sola lettura. 0 $, nessun account toccato, nessun connettore personale, nessun merge, nessun commit sui repo di codice.

## 1. Cosa ho rifatto

Ho riletto con `python3 -I` `tempi.jsonl` (35 righe), `scritture.jsonl`, `carichi.jsonl`, `copie.jsonl`, `spesa.txt`, `passi.log`, `giornata.json`. Ho letto anche il codice che misura (`src/conta.rs`, funzione `get_opts`; `src/main.rs`, funzione `tempi` e la prova "insieme"), per capire cosa vuol dire ogni numero.

Come il rapporto dice già, nei file non ci sono le singole GET ma solo i riassunti per gruppo (p50, p95, p99, media, massimo, quote). Quindi "rifare ogni numero" qui vuol dire rileggerlo dal riassunto giusto e rifare le somme e i rapporti. Fatto per tutti.

Tornano tutti (MISURATO, riletti uno per uno):
- le GET: 36.504 sul carico da 10 GB e 6.237 su quello da 2 KB, p50 24,3–26,2 ms, media pesata 26,3 ms, 82,9% sopra 20 ms; 2 KB media 25,7 ms, 84,9% sopra 20 ms;
- ogni ricerca di ogni variante, a freddo e a caldo, sia dormienti sia attivi (sezione 1.3 del rapporto), e il "freddo meno caldo" (il rapporto usa la differenza dei due p50, campo `p50_freddo_meno_p50_caldo`: va bene, scarto sotto 0,2 ms);
- le GET per find, il 4,0–7,0% di GET in più della richiesta doppia sul carico da 10 GB, i 289–357 messaggi medi di Q4;
- la prova "insieme" (1.440 find, 115 GET in volo, 3.023 GET al secondo in 37,7 ms);
- i 2 KB, Q5 (268 / 402 ms, 116 GET, 3,1 MB; tutte 26,3 s su 3 finestre; senza indice 45,1 s, 3,94 GB);
- le scritture (316, 886 e 4.264 documenti al secondo; 1,98, 2,70 e 4,44 byte per byte; 18.459, 24.153 e 1.576 PUT per GB; p99 7,1 s con i lotti);
- il carico (5,02–5,03 byte per byte, 2.024 PUT per GB, 1,32 core in media, 10,74 GB che diventano 4,92 GB) e la copia (circa 190 MB/s);
- le GET della spesa (73.051 = 36.504 + 6.237 + 23.351 di Q5 + 4.860 di riscaldo + 2.099 insieme) e i PUT (6.614 della copia, 7.362 delle scritture).

## 2. Errori provati (da correggere)

**Errore 1 · La "GET su S3 vero" mescola GET semplici e GET con la richiesta doppia.** In `conta.rs` il tempo di una GET si misura da quando parte la prima richiesta a quando arriva la risposta che vince: con la richiesta doppia (seconda GET uguale se la prima supera il p95) quella è la latenza dopo il rimedio, non la latenza di S3. Sei dei 14 gruppi da 10 GB e quattro degli 8 da 2 KB hanno la doppia. Il p99 più basso della forchetta (57,8 ms) e quello dei 2 KB (42,3) vengono proprio da gruppi con la doppia. Solo i gruppi senza doppia (MISURATO, rifatto da me):
- 10 GB, 8 gruppi, 23.581 GET: p50 24,3–26,2 ms, p95 34,5–78,9 (mediana dei gruppi 41,2), p99 68,7–104,7 (mediana 78,1), la più lenta 199 ms, media 26,4, 82,6% sopra 20 ms;
- 2 KB, 4 gruppi, 3.131 GET: p99 44,9–63,6 (mediana 55,9).
- Con la doppia, per confronto: 10 GB p99 57,8–80,0 (mediana 67,2).
Da correggere: la prima riga ("p99 tra 58 e 105, mediana 71" diventa "p99 tra 69 e 105, mediana dei gruppi 78, senza la richiesta doppia"), la sezione 1.1, la previsione 1 della sezione 2 (resta CONFERMA: il modello diceva p99 80–130, la misura sta al bordo basso, non "un po' più corta") e la riga proposta per NUMBERS.md. Detto in cucina: quando il cameriere manda un secondo fattorino se il primo tarda, il tempo che misuriamo è quello del più veloce dei due, non quello del magazzino.

**Errore 2 · Previsione 7: "su S3 vero tutti più bassi o uguali" non è vero.** Dormienti Q1 p99: 87,8 ms su S3 vero contro 82,3 in locale con S3 simulato (MISURATO, tutti e due). In più nella stessa lista c'è Q4 (locale 30,8 / 87,4 e 45,8 / 113,7) confrontata con la Q4 della giornata, che è un'altra domanda (finestra di 60 giorni invece degli ultimi 500): è lo stesso confronto che il rapporto stesso, al punto 12, chiama NON VALIDO per i 2 KB. Da correggere: "tutti più bassi o uguali tranne il p99 di Q1 dormienti (87,8 contro 82,3); Q4 non confrontabile".

**Errore 3 · Manca un limite: nel carico da 2 KB 29 clienti sono stati misurati in più di una variante.** La riga `tempi-clienti` di `aws-d2k-2gb` dice `clienti_in_piu_varianti: 29` su 371 (MISURATO). Il motivo, dal codice: il riscaldo prende 200 dei 963 clienti, e tra quelli che restano i dormienti sono meno dei 200 che servono (4 varianti per 50). RAPPORTO-2 di M31 (C7) prometteva "nessun dormiente ripetuto": non è andata così. Dentro il processo erano freddi (bancone svuotato prima di ogni find), ma è lo stesso dubbio dei clienti di "insieme" (S3 risponde più in fretta a un oggetto letto da poco? NON SO). Il rapporto lo dice per "insieme", non per i 2 KB. Sul carico da 10 GB nessun cliente ripetuto (0 su 2.100, MISURATO): lì va bene.

**Errore 4 · Un numero dedotto segnato MISURATO.** "Il WAL pesa 0,53 byte per byte inserito: i documenti sintetici si comprimono circa 1,9 volte (MISURATO)". Lo 0,53 è MISURATO; l'1,9 è una deduzione (1 / 0,53), e nel WAL ci sono anche chiavi e intestazioni: va segnato STIMATO.

## 3. Conclusioni più forti dei dati (correzioni di parole, non errori)

1. **Prima riga, "RISPETTATO".** L'obiettivo di DECISIONS (2026-11-03) riguarda le letture fredde di un cliente nel prodotto. La misura è dentro il programma di prova: niente driver MongoDB, niente rete del cliente, niente cameriere separato, niente quaderno, 10 GB e non 1 TB. Il limite è scritto in sezione 4, ma la prima riga deve dirlo: "rispettato dentro il programma di prova (senza driver né rete, 10 GB)".
2. **"Su tutte le ricerche".** Va scritto "su tutte le ricerche di un cliente (Q1–Q4)". Q5 "di tutti" non c'entra con l'obiettivo, ma resta a 268 / 402 ms (MISURATO) e chi legge solo la prima riga non lo sa.
3. **"Con 100 clienti insieme la coda non peggiora rispetto a uno solo."** Rispetto a un cliente solo nella stessa variante (parallelo, dormienti) Q1 e Q2 vanno meglio, ma Q3 va un po' peggio: 28,7 / 44,6 con 100 insieme contro 25,2 / 37,3 da solo (MISURATO). Sono 3 raffiche per ricerca. Con 10 clienti il p99 è la più lenta di 30 find. Meglio: "con 100 clienti insieme nessun peggioramento grande (al massimo circa 7 ms), su 3 raffiche".
4. **Il peso dei pezzi piccoli.** Le varianti girano una dopo l'altra, una volta sola, e S3 ha momenti lenti: la stranezza della variante "parallelo" dormienti (Q1 61,9 ms con 1 GET, sezione 4 punto 5 del rapporto) lo mostra. Quindi le differenze di pochi ms tra varianti vicine (per esempio con o senza doppia: attivi Q1 31,9 / 101,9 contro 32,7 / 89,6) non sono distinguibili dal rumore. Reggono le differenze grandi: una GET alla volta contro in parallelo (270 contro 68 ms) e il bancone sugli attivi (68 contro 32 ms). "La doppia aiuta meno del previsto" va scritto come impressione da un giro solo.
5. **La spesa.** Le voci del rapporto sommano 0,32–0,33 $ (0,207 + 0,01 + 0,029 + 0,075 + meno di 0,01), non "circa 0,30"; la forchetta 0,28–0,35 resta giusta (STIMATO). La riga SPESA del workflow contava 1,2 ore-macchina, il rapporto 63–65 minuti: la differenza pesa circa 0,02 $. Da `passi.log` la macchina ha lavorato dalle 09:10:16 alle 10:11:52 UTC (MISURATO), più l'avvio.
6. **CloudWatch: non era del tutto NON SO.** Nel brain, in `sessions/2026-11-04-m31-freddo-s3/cloudwatch/`, ci sono due grafici della console messi alle 10:48 ora italiana, prima del rapporto. Coprono solo fino a circa le 09:45 UTC, cioè il carico e l'inizio dei tempi: CPU dell'istanza tra circa 20% e 43% durante il carico, a punti di 5 minuti (MISURATO dal grafico, lettura a occhio), coerente con l'1,3 core su 4 del programma. Per i tempi su S3 (09:43–09:55 UTC) resta NON SO. Il rapporto doveva citarli.

## 4. Controlli che tornano

- **Le previsioni di M31** (sezione 2 del rapporto): tranne il punto 7 (errore 2) e la parola sul punto 1 (errore 1), i giudizi CONFERMA o SMENTISCE IN MEGLIO sono giusti; ho controllato ogni numero previsto in RAPPORTO.md e RAPPORTO-2 di M31.
- **Atlas di ieri** (sezione 3): i rapporti tornano. Q2: Atlas circa 17–29 volte più veloce (49,3–50,2 contro 1,75–2,92). Q1: 2,2–3,9 volte. Q3: le due forchette, con la stima di B e con quella di A2. Le etichette sono giuste (Atlas Q1 e Q3 STIMATO, Q2 MISURATO). È giusto che non si faccia il confronto finale. Un dettaglio: se il "primo tocco" (limite 4 del rapporto) è vero, anche la nostra Q3 andrebbe letta più vicina a Q1 (circa 28 ms invece di 24, MISURATO su Q1): il verso del confronto con Atlas non cambia.
- **I limiti** (sezione 4): ci sono quasi tutti; mancano solo l'errore 3 e la mescolanza della doppia nelle GET (errore 1).
- **Dalla regola dei database grandi** (CLAUDE.md): il rapporto dice in chiaro che è una prova a 10 GB e cosa non sa per 1 TB. Aggiungo un numero di oggi: indici dei blocchi e filtri tenuti sul bancone dopo il riscaldo, 83,7 MB per 10,74 GB vivi, circa 0,78% (MISURATO, `tempi-p1.log`). Portato a 1 TB fa circa 8 GB di RAM (STIMATO), dentro la forchetta 0,7–9 GB del rapporto.
- **I NaN di Q5 sui 2 KB** sono voluti (`--q5-n 0` in `esegui.sh`), come dice il rapporto.
- **"ra_find 65536"** nelle righe delle varianti "+ra256": è il valore di partenza; il codice usa 256 KB in quelle varianti (`main.rs` riga 1426), e si vede nei byte per GET (circa 134 KB contro 80). I numeri sono giusti.

## 5. Elenco esatto delle correzioni da fare nel rapporto M31

1. Prima riga: GET "p50 circa 25 ms, p99 tra 69 e 105 (mediana dei gruppi 78) senza la richiesta doppia"; "rispettato su tutte le ricerche di un cliente (Q1–Q4), dentro il programma di prova senza driver né rete, a 10 GB".
2. Sezione 1.1: dare a parte i gruppi senza doppia (numeri dell'errore 1) e dire che con la doppia il tempo è quello della risposta che vince.
3. Sezione 1.5: aggiungere "29 clienti su 371 misurati in più di una variante".
4. Sezione 1.7: "si comprimono circa 1,9 volte" da MISURATO a STIMATO.
5. Sezione 1.4: "nessun peggioramento grande (Q3 circa +7 ms), su 3 raffiche" al posto di "non peggiora".
6. Sezione 1.9: totale "circa 0,32 $ (0,28–0,35)".
7. Sezione 2, punto 1: "CONFERMA, al bordo basso del modello" e i numeri senza doppia. Punto 7: togliere "tutti", dire l'eccezione di Q1 dormienti p99 e che Q4 non è confrontabile. Punto 8: la doppia "aiuta meno del previsto" come impressione da un giro solo.
8. Sezione 4: aggiungere il limite dei 29 clienti ripetuti nei 2 KB e quello delle varianti girate una volta sola in ore diverse; al punto 11 citare i grafici CloudWatch del brain (CPU circa 20–43% nel carico; tempi NON SO).
9. Sezione 5, riga per NUMBERS.md: "GET su S3 vero, senza richiesta doppia: p50 24–26 ms, p95 35–79 (mediana dei gruppi 41), p99 69–105 (mediana 78), media 26 ms (MISURATO, M31, 23.581 GET, 8 gruppi)"; la riga dei 2 KB con il limite dei clienti ripetuti; "si comprimono 1,9 volte" STIMATO.

## 6. Rischi (opinione mia, non errori)

- Una giornata sola, un'ora, una zona: la coda di S3 in altre ore NON SO. La prova più economica che deciderebbe: ripetere solo `tempi` sulle stesse pile già su S3 (circa 15 minuti di c7i.xlarge, pochi centesimi, STIMATO), con le varianti in ordine diverso e le GET salvate una per una.
- Il "primo tocco" (limite 4 del rapporto) si decide con la stessa prova, alternando l'ordine delle ricerche per cliente.
- La coda di 5–7 secondi delle scritture a lotti: finché non si sa perché, nessun numero di scrittura a lotti va usato fuori da M31.

## 7. Errori o mancanze nel brain

- Il prompt di M31 indica le metriche CloudWatch solo in fenvix-engine (`m31/risultati/aws-41999425773/cloudwatch/`), ma i grafici di oggi sono nel brain, in `sessions/2026-11-04-m31-freddo-s3/cloudwatch/`: vanno uniti in un posto solo, o citati nei due prompt.
- RAPPORTO-2 di M31 (C7) dice "nessun dormiente ripetuto" nei 2 KB: smentito dalla giornata (29 clienti ripetuti).
