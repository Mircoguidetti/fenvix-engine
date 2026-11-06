**GET su S3 vero: p50 circa 25 ms (24,3–26,2 nei 14 gruppi misurati), p99 tra 58 e 105 ms (mediana dei gruppi 71) (MISURATO). Freddo con il disegno completo, peggio tra le ricerche: p50 50,7 ms e p99 105,7 ms, tutti e due su Q2 (un documento per `_id`) dei clienti attivi (MISURATO). Obiettivo freddo (p50 30–60 ms, p99 sotto 150 ms): RISPETTATO su tutte le ricerche, dormienti e attivi, anche con documenti da 2 KB.**

# M31 · rapporto della giornata su S3 vero (2026-11-05)

Sessione di lavoro Opus, lanciata da scheduled task, prompt `prompts/M31-rapporto-giornata.md`. Solo lettura: fenvix-engine branch `freddo-s3` (commit d988e6a), cartella `proto/lavaggio-per-cliente/m31/risultati/aws-41999425773/`. 0 $ spesi, nessun account toccato, nessun connettore personale, nessun merge, nessun commit sui repo di codice.

Come ho ricavato i numeri: ho riletto `tempi.jsonl`, `scritture.jsonl`, `carichi.jsonl`, `copie.jsonl` con `python3 -I` e li ho confrontati con `tempi-p1.log` e `tempi-d2k.log`: coincidono. Attenzione: `tempi.jsonl` non contiene ogni singola GET, ma per ogni gruppo (variante × dormienti/attivi) già p50, p95, p99, media, massimo e quote. Quindi un p50 o un p99 "di tutta la giornata" non si può ricalcolare: do la forchetta tra i gruppi e la mediana dei gruppi. Medie e quote invece le ho sommate pesando per numero di GET.

Il giorno in breve (MISURATO, `passi.log` e `giornata.json`): macchina c7i.xlarge (4 vCPU Intel Xeon 8488C), eu-west-1. Passi in ora italiana: carico 10 GB dalle 10:10 alle 10:42, copia su S3, tempi 10 GB dalle 10:43 alle 10:55, carico 2 KB fino alle 11:01, tempi 2 KB, tre prove di scrittura fino alle 11:12. Workflow 62 minuti, esito FINITA, "RIMASTO: niente".

## 1. I numeri della giornata

### 1.1 Una GET al magazzino (S3 vero, dalla stessa regione)

- Quante: 36.504 GET misurate nelle ricerche sul carico da 10 GB, 6.237 su quello da 2 KB (MISURATO). Oggetti letti da 26 a 211 KB per GET (p50 per ricerca, MISURATO). Il tempo è dalla richiesta all'ultimo byte ricevuto, dentro il processo.
- 10 GB: p50 24,3–26,2 ms nei 14 gruppi, p95 33,9–78,9 (mediana dei gruppi 39,9), p99 57,8–104,7 (mediana 71,4), la più lenta 199 ms. Media pesata 26,3 ms. Sopra 20 ms: 82,9% delle GET. Sotto 5 ms: nessuna (MISURATO).
- 2 KB: p50 24,5–25,9, p95 33,5–42,7, p99 42,3–63,6, la più lenta 114 ms, media 25,7, sopra 20 ms 84,9% (MISURATO).
- In cucina: il magazzino (S3) risponde quasi sempre in circa 25 ms, qualunque cosa gli chiedi tra 26 e 211 KB; la coda lenta (p99) è circa 2–4 volte la mediana.

### 1.2 GET per ricerca (quante volte il cameriere va al magazzino)

Disegno completo (in parallelo, pile recenti sul bancone, richiesta doppia, lettura in anticipo da 256 KB), 10 GB (MISURATO):
- dormienti: Q1 1,21, Q1d 1,08, Q2 2,05, Q3 1,04, Q4 1,04 GET per find in media (p50 1 per tutte tranne Q2 con 2);
- attivi: Q1 3,25, Q1d 3,11, Q2 2,17, Q3 3,12, Q4 3,04 (p50 3; Q2 2);
- attivi senza bancone: circa 9,1–9,5 GET per find su Q1, Q1d, Q3, Q4 (p50 9).
Detto in cucina: chi non scrive da tempo sta in un ripiano solo (una pila, una GET); chi scrive ancora è sparso su più ripiani, e il bancone (cache delle pile recenti) taglia i viaggi da 9 a 3.

### 1.3 Ogni ricerca, a freddo e a caldo, 10 GB, 150 clienti per gruppo

Freddo = cliente mai letto, bancone vuoto tranne indici e filtri; caldo = la stessa ricerca subito dopo. Tutto MISURATO. Formato: freddo p50 / p99, caldo p50.

**Disegno completo (parallelo + bancone + doppia + ra256)**
- Dormienti: Q1 28,4 / 87,8, caldo 0,28; Q1d 26,4 / 62,1, caldo 0,29; Q2 49,7 / 88,8, caldo 0,47; Q3 24,0 / 37,1, caldo 0,49; Q4 26,2 / 38,8, caldo 0,68.
- Attivi: Q1 31,4 / 77,7, caldo 0,49; Q1d 32,1 / 56,1, caldo 0,49; Q2 50,7 / 105,7, caldo 0,48; Q3 31,3 / 73,6, caldo 0,59; Q4 29,5 / 43,5, caldo 0,95.
- Sopra 20 ms a freddo: 74–100% a seconda della ricerca; sotto 5 ms a freddo: 0–1%. A caldo: 100% sotto 5 ms, p99 caldo 0,5–1,6 ms.

**Le altre varianti** (stesso formato, dormienti poi attivi):
- Una GET alla volta (serie): Q1 28,3 / 103,0 e 269,9 / 352,5; Q1d 27,0 / 67,6 e 226,1 / 294,5; Q2 49,4 / 79,8 e 85,0 / 164,1; Q3 25,5 / 65,9 e 222,1 / 315,3; Q4 27,5 / 84,1 e 230,8 / 299,8.
- Parallelo (SlateDB 0.17 com'è): Q1 61,9 / 131,3 e 67,5 / 123,2; Q1d 26,8 / 49,1 e 47,4 / 87,5; Q2 57,4 / 166,7 e 68,1 / 139,1; Q3 25,2 / 37,3 e 45,4 / 82,5; Q4 26,9 / 63,1 e 46,9 / 86,3.
- Parallelo + ra256: Q1 33,3 / 106,2 e 55,5 / 111,0; Q1d 26,4 / 54,1 e 48,2 / 88,5; Q2 50,7 / 113,8 e 57,2 / 138,0; Q3 24,7 / 33,6 e 45,8 / 83,2; Q4 26,1 / 34,3 e 48,0 / 70,4.
- Parallelo + bancone: Q1 32,4 / 104,5 e 31,9 / 101,9; Q1d 26,0 / 47,7 e 29,0 / 73,1; Q2 50,2 / 100,4 e 52,8 / 115,4; Q3 24,0 / 60,2 e 28,4 / 51,1; Q4 26,9 / 62,2 e 30,1 / 59,5.
- Parallelo + doppia: Q1 30,6 / 92,9 e 50,3 / 113,7; Q1d 26,0 / 40,3 e 47,4 / 58,0; Q2 49,5 / 113,2 e 54,8 / 128,7; Q3 24,6 / 35,1 e 45,9 / 69,0; Q4 27,7 / 64,9 e 48,5 / 82,1.
- Parallelo + bancone + doppia: Q1 27,9 / 90,3 e 32,7 / 89,6; Q1d 26,8 / 63,8 e 31,8 / 56,0; Q2 48,8 / 75,5 e 50,7 / 110,1; Q3 25,1 / 58,7 e 30,2 / 62,5; Q4 27,7 / 60,9 e 30,3 / 59,2.
- La richiesta doppia (seconda GET uguale se la prima supera il p95) costa il 4,0–7,0% di GET in più (MISURATO).
- Q4 ha trovato in media 289–357 messaggi per find (finestra di 60 giorni, `limit 500`); 150 clienti per gruppo, sopra i 60 di p1-1.5 (MISURATO).

**"Freddo meno caldo" a p50, disegno completo** (il numero da mettere accanto ad Atlas): dormienti Q1 28,2, Q1d 26,1, Q2 49,3, Q3 23,6, Q4 25,5 ms; attivi Q1 30,9, Q1d 31,6, Q2 50,2, Q3 30,7, Q4 28,6 ms (MISURATO).

### 1.4 10, 50 e 100 clienti freddi insieme

Variante "parallelo com'è" (senza bancone e senza doppia), clienti dormienti, 3 giri per ricerca (MISURATO). Formato: p50 / p99 della find.
- 10 insieme: Q1 30,0 / 100,1; Q2 48,2 / 77,2; Q3 26,6 / 46,0.
- 50 insieme: Q1 29,1 / 104,9; Q2 50,2 / 87,8; Q3 26,3 / 48,2.
- 100 insieme: Q1 28,1 / 73,0; Q2 49,2 / 92,3; Q3 28,7 / 44,6. Fino a 115 GET in volo nello stesso istante e fino a circa 3.000 GET al secondo in una raffica di 38 ms.
- Con 100 clienti insieme la coda non peggiora rispetto a uno solo (MISURATO). Ma tutte le 1.440 find erano su clienti già letti prima nelle varianti (sezione 4).

### 1.5 Documenti da 2 KB (carico da 2 GB, 50 clienti per gruppo)

Disegno (parallelo + bancone + doppia, lettura in anticipo 256 KB per tutte le varianti), freddo p50 / p99 (MISURATO):
- dormienti: Q1 27,9 / 53,4; Q1d 27,3 / 42,8; Q2 48,9 / 77,6; Q3 25,4 / 34,0; Q4 29,4 / 62,6 (1,32 GET per find);
- attivi: Q1 29,7 / 67,7; Q1d 29,9 / 43,0; Q2 50,1 / 74,5; Q3 28,3 / 44,8; Q4 37,8 / 86,9 (2,60 GET).
- Senza bancone gli attivi fanno circa 9 GET e p50 47–57 ms (MISURATO). Con 50 clienti il p99 è la find più lenta: numero fragile.

### 1.6 Q5, "di tutti" (ordini con uno stato negli ultimi 7 giorni)

Solo sul carico da 10 GB (sul 2 KB lo script la salta apposta: `--q5-n 0`, quindi i NaN del log sono attesi). MISURATO:
- i 100 più nuovi con l'indice: freddo p50 267,8 ms, p99 402,4; caldo 9,6 ms; 116 GET e 3,1 MB per ricerca, 60 finestre;
- tutti quelli della finestra (circa 38.000): freddo p50 26,3 s, 5.526 GET, 150 MB (solo 3 finestre);
- senza indice (scansione di tutto): 45,1 s, 251 GET, 3,9 GB letti.
In cucina: per "di tutti" il cameriere deve andare a prendere 100 piatti sparsi in 100 ripiani diversi; senza un indice che porti con sé la risposta (indice coprente) restano circa 270 ms.

### 1.7 Scritture fino a S3 (documenti da 1.922 byte, 32 fili, 2 minuti per prova)

Qui la scrittura è confermata quando SlateDB ha messo il suo WAL su S3 (nel disegno vero va invece nel quaderno, il registro replicato a 3 zone). MISURATO:
- singole, WAL ogni 100 ms: 316 documenti al secondo; latenza fino a S3 p50 101,1 ms, p95 110,0, p99 122,8; 1,98 byte scritti su S3 per byte inserito; 18.459 PUT per GB inserito;
- singole, WAL ogni 10 ms: 886 al secondo; p50 29,6, p95 57,8, p99 77,4 ms; 2,70 byte per byte; 24.153 PUT per GB;
- lotti da 100, WAL ogni 100 ms: 4.264 al secondo (7,8 MB/s); p50 175,5 ms ma p95 4,9 s e p99 7,1 s; 4,44 byte per byte (3,59 prima che il lavaggio finisse); 1.576 PUT per GB.
- In tutte e tre il WAL pesa 0,53 byte per byte inserito: i documenti sintetici si comprimono circa 1,9 volte (MISURATO).
- Il tempo di un PUT del WAL, ricavato dalla prova a 10 ms (29,6 ms di p50 con al massimo 10 ms di attesa del giro): circa 20–30 ms (STIMATO).
- La coda di 5–7 secondi con i lotti vuol dire che il programma ha frenato lo scrittore (probabilmente troppe pile nuove in attesa del lavaggio): il motivo esatto NON SO.
- Prezzo dei PUT per GB inserito (prezzi DICHIARATI di NUMBERS.md): circa 0,09 $ con WAL ogni 100 ms, 0,12 $ ogni 10 ms, 0,008 $ a lotti (STIMATO).

### 1.8 Carico e copia

- Carico da 10 GB sul disco della macchina: 29.717.432 documenti in 32,3 minuti, 2.562 s di CPU (in media 1,3 core su 4); 10 pile alla fine (5 in L0), la più grande 3,9 GB; 5,03 byte scritti per byte inserito (chiave più valore) e 2.025 PUT per GB inserito, cioè quelli che sarebbero andati su S3 (MISURATO). Spazio su S3 dopo lz4: 4,92 GB per 10,74 GB di dati vivi (MISURATO).
- Carico da 2 KB: 1.094.996 documenti in 6,3 minuti; 3,34 byte per byte; 1.676 PUT per GB; 9 pile (MISURATO).
- Copia su S3: 4,92 GB in 5.570 file in 25,8 s (circa 190 MB/s); 0,94 GB in 1.044 file in 2,5 s (MISURATO).

### 1.9 La spesa vera (STIMATO)

Contata dalle richieste nei file, con i prezzi DICHIARATI in NUMBERS.md e RAPPORTO-2:
- macchina circa 63–65 minuti a 0,1915 $/ora: circa 0,20–0,21 $;
- disco 60 GB e IPv4 per circa un'ora: circa 0,01 $;
- GET: circa 73.000 nei tempi (36.504 + 6.237 nelle varianti, 23.351 in Q5, 4.860 di riscaldo, 2.099 insieme) più quelle del lavaggio nelle scritture, NON SO quante ma poche: circa 0,03 $;
- PUT: circa 6.600 della copia, 7.362 delle scritture, circa 1.000 dei risultati ogni minuto: circa 15.000, 0,075 $;
- spazio S3 per poche ore: meno di 0,01 $.
- **Totale circa 0,30 $ (forchetta 0,28–0,35).** La riga SPESA del workflow diceva 0,89 $: è una stima per eccesso voluta (conta tutto il tetto dei freni di S3, 0,64 $, dal primo minuto). La fattura vera: NON SO finché AWS non la mostra.

## 2. Le previsioni scritte prima, una per una

Da RAPPORTO.md (sezione 5 "La previsione per S3 vero" e sezione 3, modello: mediana 25 ms, p99 circa 100 ms) e RAPPORTO-2.

1. **GET su S3 vero: mediana 20–30 ms, p99 80–130.** Misurato p50 24–26, p99 58–105 (mediana 71). CONFERMA; la coda è un po' più corta del modello.
2. **Obiettivo freddo p50 30–60 ms, p99 sotto 150 ms, con il dubbio sul p99 di Q2.** CONFERMA: il p50 peggiore è 50,7, il p99 peggiore 105,7, e Q2 è lontano dal bordo. Molti p50 stanno anche sotto 30 (meglio dell'obiettivo).
3. **Dormienti Q1 e Q3: p50 25–40, p99 80–130.** Q1 28,4 / 87,8: CONFERMA. Q3 24,0 / 37,1: p50 al bordo basso, p99 molto meglio: SMENTISCE IN MEGLIO.
4. **Q2: p50 45–70, p99 120–200.** 49,7 / 88,8 (dormienti), 50,7 / 105,7 (attivi): il p50 CONFERMA, il p99 SMENTISCE IN MEGLIO.
5. **Q4 come Q1.** 26,2 / 38,8 e 29,5 / 43,5: CONFERMA sul p50, p99 meglio di Q1.
6. **Attivi circa 10–15 ms più lenti dei dormienti a p50.** Misurato da 1 a 7 ms in più (Q2 +1,0, Q1 +3,0, Q4 +3,3, Q1d +5,7, Q3 +7,3): SMENTISCE IN MEGLIO. Con il bancone le 3 GET degli attivi partono quasi tutte insieme.
7. **Numeri della variante del disegno in locale con S3 simulato** (dormienti Q1 32,0 / 82,3, Q2 59,9 / 124,0, Q3 26,9 / 88,6, Q4 30,8 / 87,4; attivi Q1 43,7 / 96,4, Q3 42,9 / 106,3, Q4 45,8 / 113,7). Su S3 vero tutti più bassi o uguali: CONFERMA il modello, con un margine a favore.
8. **Quanto aiuta ogni pezzo, attivi Q1** (previsto: una alla volta 322 / 511, com'è 75 / 208, più bancone 45 / 122, più doppia 43 / 100). Misurato: 269,9 / 352,5; 67,5 / 123,2; 31,9 / 101,9; 32,7 / 89,6. CONFERMA l'ordine e il peso di ogni pezzo; la richiesta doppia aiuta meno del previsto, perché la coda vera di S3 è già più corta di quella del modello.
9. **Richiesta doppia: 4,4–6,8% di GET in più.** Misurato 4,0–7,0%: CONFERMA.
10. **GET per find: dormienti 1, attivi 3 con bancone e circa 10 senza, Q2 2 in fila.** Misurato 1,04–1,21; 3,04–3,25; 9,1–9,5; 2,05–2,26: CONFERMA. La regola "una GET per pila, pile lette insieme" passa a MISURATO su S3 vero.
11. **100 clienti freddi insieme come uno solo, se S3 tiene la coda.** Q1 28,1 / 73,0, Q2 49,2 / 92,3, Q3 28,7 / 44,6: CONFERMA, con la riserva dei clienti già letti (sezione 4).
12. **2 KB, Q4 circa 66 / 133 ms con 2 GET.** Misurato 29,4 / 62,6 con 1,32 GET: meglio, ma non è lo stesso conto: la previsione era sulla vecchia Q4 (ultimi 500, 1 MB), la giornata misura la Q4 nuova (finestra di 60 giorni, in media 254 messaggi). Confronto NON VALIDO.
13. **Q5 con l'indice: p50 487, p99 591, 118 GET; tutte 47 s; senza indice 72 s.** Misurato 267,8 / 402,4 con 116 GET; 26,3 s; 45,1 s. Le GET CONFERMANO, i tempi SMENTISCONO IN MEGLIO (la macchina è più veloce di quella locale a 2 vCPU).
14. **Scritture: WAL ogni 100 ms p50 120–160 ms; ogni 10 ms 30–70 ms; PUT di S3 20–60 ms.** Misurato 101,1 (SMENTISCE IN MEGLIO), 29,6 (CONFERMA, al bordo basso), PUT circa 20–30 (STIMATO, dentro la forchetta).
15. **Byte scritti per byte inserito nel carico: 5,0 in locale.** 5,03 su AWS: CONFERMA (stesso programma, stesse pile). PUT per GB 2.390 previsti, 2.025 misurati.
16. **Durata circa 1 ora e 45 minuti; spesa 0,40–0,50 $.** 62 minuti; circa 0,30 $ (STIMATO): tutti e due meglio del previsto.

## 3. Accanto ai numeri di Atlas di ieri (RISULTATI-CORRETTI, M30)

Non è il confronto finale: i numeri di Atlas di oggi (giro su M30 e su M10, con il tempo lato server) arrivano stasera. Ricordo che i tempi di Atlas di ieri sono visti dal Mac con la rete tolta, i nostri sono dentro il processo. Nostro = "freddo meno caldo" a p50, disegno completo (MISURATO oggi).

- **Q2, un documento per `_id`.** Noi 49,3 dormienti e 50,2 attivi. Atlas 2,15–2,92 freddo meno caldo e 1,75–2,01 freddo meno rete (MISURATO ieri). Atlas circa **17–29 volte** più veloce. **Il confronto di ieri REGGE**: è il numero più pulito dei due lati. In cucina: per un piatto solo, la dispensa veloce accanto al cuoco (Page Service su NVMe in ogni zona) batte sempre due viaggi al magazzino (biglietto `_id` → chiave, poi il documento).
- **Q1, ultimi 20 messaggi.** Noi 28,2 dormienti, 30,9 attivi. Atlas circa 8–13 (STIMATO ieri, da circa 50 clienti). Atlas circa **2,2–3,9 volte** più veloce. **Regge a metà**: il nostro lato è misurato, quello di Atlas è una stima.
- **Q1d, 20 messaggi prima di una data a caso.** Noi 26,1 e 31,6. Atlas NON SO (arriva stasera).
- **Q3, circa 85 eventi.** Noi 23,6 dormienti, 30,7 attivi. Atlas circa 45–65 (STIMATO dal revisore B; A2 stimava 30–45); misurato ieri solo come miscela (33,8 e 46,7; 23,7 e 41,8 a parità di 100 documenti). Con 45–65: noi circa 1,9–2,8 volte più veloci sui dormienti, 1,5–2,1 sugli attivi. Con 30–45: 1,3–1,9 sui dormienti, da pari a 1,5 sugli attivi. **Non regge ancora**: il lato di Atlas è stimato e largo. Il giro di oggi misura Q3 sugli attivi mai letti, il confronto giusto sarà con i nostri attivi (30,7).
- **Q4, finestra di 60 giorni, `limit 500`.** Noi 25,5 e 28,6. Atlas NON SO (ieri non misurata).
- **Q5, i 100 ordini più nuovi di tutti.** Noi 267,8 ms freddo (non freddo meno caldo: caldo 9,6). Atlas NON SO. Qui Atlas parte avvantaggiato: il suo indice `stato_ts` è fatto per questa domanda, il nostro prototipo non ha un indice coprente.
- **Quote sotto 5 ms e sopra 20 ms a freddo.** Noi 0–1% sotto 5 ms, 74–100% sopra 20 ms. Atlas NON SO (non c'è in RISULTATI-CORRETTI).
- **Caldo.** Noi 0,2–1,0 ms dentro il processo; Atlas caldo visto dal Mac comprende la rete: non confrontabili.
- **Byte scritti per byte inserito.** Atlas circa 3,2 a scritture singole da 1,9 KB (forchetta 2,7–3,6) e circa 4 a lotti con documenti da 340 byte (STIMATO dai grafici). Noi con 1,9 KB: 1,98 (WAL ogni 100 ms) e 2,70 (ogni 10 ms) a scritture singole, 4,44 a lotti (MISURATO, 2 minuti). **Non regge come confronto**: denominatori diversi (noi chiave più valore, Atlas byte BSON), prove da 2 minuti con il lavaggio non a regime, noi scriviamo su S3 e non su disco, e nel disegno la scrittura passa dal quaderno. Dice solo che sulle scritture siamo nello stesso ordine di grandezza.

## 4. I limiti, detti chiari

1. **È un programma di prova su una macchina, non la cucina.** Niente quaderno (registro replicato a 3 zone), niente camerieri separati, niente driver MongoDB e niente rete del cliente: i tempi sono dentro il processo. Una richiesta vera aggiunge il viaggio del driver e del cameriere: NON SO quanto.
2. **10 GB, non 1 TB.** Il principio di disegno chiede di dirlo: questa prova vale per un database da 10 GB con 10 pile. La GET di S3 non dipende dalla taglia del database; il numero di pile sì, cresce piano (pile a gradini T = 4). Un dormiente dovrebbe restare a 1 GET (pila più vecchia più filtri) e un attivo dovrebbe avere qualche GET in più nello stesso giro (STIMATO, non misurato). Gli indici dei blocchi e i filtri sul bancone a 1 TB pesano circa 0,7–9 GB di RAM (STIMATO dallo 0,07–0,9% di M21). Decine di migliaia di operazioni al secondo: non provate; il picco di oggi è circa 3.000 GET al secondo per 38 ms, contro il limite di 5.500 GET al secondo per prefisso (DICHIARATO AWS).
3. **I clienti delle prove "insieme" erano già stati letti nelle varianti** (1.440 find su 1.440, MISURATO). Dentro il processo erano freddi (bancone svuotato); se S3 risponde più in fretta a un oggetto letto da poco: NON SO.
4. **Possibile "primo tocco" di S3.** In quasi ogni gruppo Q1 (la prima ricerca su un cliente nuovo) ha la coda più lunga di Q1d, che legge le stesse pile con le stesse GET e gli stessi byte: dormienti del disegno p95 61,3 contro 40,6 (MISURATO). Ipotesi: Q1d, Q2, Q3, Q4 vengono dopo Q1 sullo stesso cliente e S3 risponde più in fretta a una zona letta da poco. Non provato: NON SO. Se è così, il numero freddo onesto per un cliente toccato la prima volta è quello di Q1 (28,4 / 87,8) e Q2 (49,7 / 88,8), non quello di Q3 e Q4. L'obiettivo resta rispettato anche così.
5. **Una stranezza non spiegata.** Nella variante "parallelo" dei dormienti Q1 ha p50 61,9 ms con 1 GET, contro circa 28–33 in tutte le altre varianti; anche le GET di quel gruppo hanno p95 78,9 contro 34–61. Non tocca la variante del disegno. Causa: NON SO (un momento lento di S3 o del bucket appena scritto?).
6. **p99 su pochi campioni.** Con 150 find per ricerca il p99 è circa la seconda più lenta; con 50 (carico 2 KB) è la più lenta. Sono numeri fragili, di qualche decina di ms in su o in giù.
7. **Niente GET una per una nei file.** `tempi.jsonl` tiene solo i riassunti per gruppo: un p50 o p99 unico della giornata non si può ricalcolare. Proposta per la prossima giornata: salvare ogni GET (ms, byte, ora).
8. **Q5**: il prototipo non ha un indice coprente; "tutte" misurata su 3 finestre sole; sul carico da 2 KB non misurata (voluto).
9. **Scritture**: 2 minuti per prova, lavaggio non a regime; WAL di SlateDB su S3 invece del quaderno; con i lotti lo scrittore viene frenato (p99 7 s) per un motivo che NON SO.
10. **Un'ora sola, una zona, un giovedì mattina.** Come cambia S3 nelle altre ore e nelle altre zone: NON SO.
11. **Metriche CloudWatch della macchina**: la cartella `cloudwatch/` non c'era quando ho finito (controllato alle 10:29 UTC). CPU e rete della macchina durante i tempi: NON SO. In particolare NON SO se il programma era limitato dalla CPU in qualche momento (nel carico usava in media 1,3 core su 4, MISURATO dal suo contatore).
12. **Il carico è stato fatto sul disco e poi copiato su S3**, come deciso in RAPPORTO.md: i byte e i PUT del carico sono "quelli che sarebbero andati su S3", non PUT veri.
13. **Dati sintetici** (testo zipf, lz4): compressione e taglia dei blocchi su dati veri NON SO.

## 5. Righe proposte per NUMBERS.md

Da aggiungere in fondo, sotto una voce "2026-11-05 · M31, giornata su S3 vero (sessions/2026-11-05-m31-giornata-s3/RAPPORTO.md)":

- **GET su S3 vero, eu-west-1, dalla stessa regione, oggetti da 26 a 211 KB:** p50 24–26 ms, p95 34–79 (mediana dei gruppi 40), p99 58–105 (mediana 71), media 26 ms, 83% sopra 20 ms, 0% sotto 5 ms (MISURATO, M31, 42.741 GET, c7i.xlarge, run 41999425773). Sostituisce "Lettura da S3 Standard: circa 20–40 ms per GET (STIMATO)".
- **Freddo con il disegno completo** (parallelo, pile recenti sul bancone, richiesta doppia, lettura in anticipo 256 KB), 10 GB, 150 clienti per gruppo: p50 24–51 ms, p99 37–106 ms; peggio Q2 attivi 50,7 / 105,7 (MISURATO). Freddo meno caldo p50: Q1 28 / 31, Q2 49 / 50, Q3 24 / 31, Q4 26 / 29 ms (dormienti / attivi). Obiettivo p50 30–60, p99 sotto 150: rispettato.
- **GET per find su S3 vero:** dormienti 1,0–1,2, attivi 3,0–3,3 con il bancone e 9,1–9,5 senza, Q2 2 in fila (MISURATO).
- **Una GET alla volta contro in parallelo, attivi Q1:** 270 / 353 ms contro 68 / 123 (com'è) e 33 / 90 (con bancone e richiesta doppia) (MISURATO). Richiesta doppia: 4–7% di GET in più (MISURATO).
- **100 clienti freddi insieme:** Q1 28 / 73, Q2 49 / 92, Q3 29 / 45 ms p50 / p99, fino a 115 GET in volo (MISURATO, clienti già letti prima).
- **Documenti da 2 KB, disegno:** p50 25–50 ms, p99 34–87 ms (MISURATO, 50 clienti per gruppo).
- **Q5, i 100 ordini più nuovi di tutti, senza indice coprente:** 268 ms p50, 402 p99, 116 GET (MISURATO). Con indice coprente circa 30–60 ms (STIMATO, M31 RAPPORTO).
- **Scritture fino a S3 con documenti da 1,9 KB** (WAL di SlateDB, senza quaderno): WAL ogni 10 ms p50 29,6, p99 77,4 ms, 886 documenti al secondo con 32 fili; ogni 100 ms p50 101, p99 123; byte su S3 per byte inserito 2,0–2,7 a scritture singole e 4,4 a lotti in 2 minuti (MISURATO). PUT di S3 circa 20–30 ms (STIMATO).
- **Carico da 10 GB su c7i.xlarge:** 32 minuti, 1,3 core in media, 5,0 byte scritti per byte inserito, 2.025 PUT per GB, 10,74 GB diventano 4,92 GB con lz4 (MISURATO).
- **Costo della giornata:** circa 0,30 $ (STIMATO dalle richieste e dalle ore-macchina).

## 6. Errori o mancanze trovati in fenvix-brain

- NUMBERS.md, "Lettura da S3 Standard: circa 20–40 ms per GET (STIMATO)": da sostituire con la riga misurata qui sopra.
- Fable hbm-luce, sezione 5 ("mai misurato: una GET su S3 vero dalla nostra regione") e sezione 8 punti 1, 2 e 4 della giornata: ora misurati; il punto 3 (byte per byte contro il 3,2 di Atlas) è misurato solo in parte (prove da 2 minuti, denominatori diversi).
- Il prompt cita "RAPPORTO-1": nel brain il file si chiama `RAPPORTO.md`.
- RAPPORTO.md di M31, sezione 5, scrive "Q2 p99 al bordo" dell'obiettivo: con S3 vero Q2 è lontano dal bordo (105,7 contro 150).
- Per le prossime giornate: salvare le GET una per una (vedi limite 7) e alternare l'ordine delle ricerche per cliente, così il "primo tocco" (limite 4) si misura invece di restare un'ipotesi.
