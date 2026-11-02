# proto-cucitura (M11): il lavaggio a cucitura

Documento tecnico del branch `proto-cucitura`. Il rapporto per il fondatore è in fenvix-brain,
`sessions/2026-11-02-cucitura/RAPPORTO.md`; qui sotto lo stesso testo, più come rifare le prove.

## Come rifare le prove

    cd proto/cucitura
    cargo build --release                  # crate autonomo, dipende da ../lettura
    python3 scripts/finto_s3.py --dir /tmp/s3dati --porta 9100 &
    scripts/matrice.sh                     # primo giro: 7 varianti, modi coda e ordine (GET solo blocchi contigui)
    scripts/matrice2.sh                    # secondo giro: GET raggruppati (64 KB, 1 MB), ripiego per l'UUID
    scripts/giri.sh                        # 30 giri, con e senza rimedio
    python3 scripts/riassunto.py risultati/prova-*.json risultati/g2-*.json
    python3 scripts/riassunto_giri.py risultati/giri-*.json

Ogni prova controlla da sola che l'uscita cucita e quella riscritta dicano le stesse cose (documenti vivi e
versioni uguali, tutte le letture del campione uguali) e si ferma se no. `cargo test` prova il formato.
Il primo giro di prove (`risultati/prova-*.json`) è stato fatto prima di aggiungere i GET raggruppati e il ripiego:
corrisponde a `--buco-get-kb 0 --ripiego 1` (il JSON non ha questi campi; `matrice.sh` li passa).


## In una riga

FATTO. La cucitura (compattazione che copia lato server con UploadPartCopy i tratti che non si toccano) supera le soglie, ma **solo** con due cambi al formato delle scatole: i blocchi nuovi messi "in coda" all'oggetto e blocchi da 4 KB. L'idea così com'era scritta (tutto in ordine di chiave, tratti da 5 MB) non le supera: 12,6% copiato. Sul costo in dollari, a 1 TB con 10.000 scritture al secondo, il guadagno è piccolo (da 0 a 45 dollari al mese, STIMATO); il guadagno vero è CPU e banda dei lavapiatti.

Branch `proto-cucitura` di fenvix-engine (da `proto-lettura`), codice in `proto/cucitura`, documento tecnico `docs/proto-cucitura.md`, risultati grezzi in `proto/cucitura/risultati/`. PR in bozza, nessun merge. Spesa: $0.

Tutte le misure sono su una macchina da 2 vCPU e 8 GB, con un finto S3 locale (vedi "Cosa blocca"). I tempi non sono tempi di S3 vero: valgono come confronto tra lavaggio cucito e lavaggio normale sulla stessa macchina.

## L'idea in cucina

Il lavaggio (compattazione) prende 8 scatole sigillate (SST) e ne fa una sola, in ordine. Il lavaggio normale porta tutte le scatole in cucina (le scarica da S3), le svuota, rimette tutto in ordine e riempie una scatola nuova (la carica su S3): 1 GB scaricato e 1 GB caricato per 1 GB di scatole.

La cucitura guarda prima solo le etichette delle scatole (l'indice dei blocchi: dove comincia ogni blocco e la sua prima e ultima chiave). I pezzi di scatola che non si mescolano con nessun'altra scatola restano in magazzino: il magazzino stesso li copia nella scatola nuova (UploadPartCopy, copia lato server, senza passare dalla cucina). In cucina arrivano solo i pezzi che si mescolano davvero.

## Numeri contro le soglie

Carico (il "realistico MongoDB" del prompt): 8 scatole da 128 MB (1.032 MB in tutto, 1.132.240 operazioni, 995.541 documenti vivi alla fine), documenti da 300 a 1.700 byte, 89% inserimenti, 10% aggiornamenti di documenti recenti (tra gli ultimi 50.000 inseriti), 1% cancellazioni di documenti a caso tra tutti quelli vivi. `_id` ObjectId crescente, poi UUID casuale.

Il lavaggio normale, per riferimento (MISURATO): scarica 1.032 MB e carica 1.004 MB, 11–12 secondi, 9,5–11 secondi di CPU, 72 GET e 65 PUT; letture puntuali p99 10–15 µs (dal bancone, oggetto intero in memoria).

**Soglia 1: almeno l'80% dei byte copiato senza scaricarli, con ObjectId crescente.**
- Idea com'era scritta (modo "in ordine", blocchi da 16 KB): **12,6%** dei byte vivi copiati (MISURATO). NON SUPERATA. Lo stesso con blocchi da 4 KB: 12,6%. Con le cancellazioni solo tra i recenti sale a 66%, con le cancellazioni dei più vecchi (tipo indice TTL) a 65% (MISURATO): comunque sotto.
- Variante "toppe in coda", blocchi da 16 KB: **56,3%** (MISURATO). NON SUPERATA. Con cancellazioni recenti 79,1%, con cancellazioni vecchie 79,2%, con aggiornamenti molto recenti (ultimi 5.000 documenti) 78,3%: al limite.
- Variante "toppe in coda", blocchi da 4 KB: **89,9%** dei byte vivi della scatola nuova arrivano per copia lato server (MISURATO). SUPERATA. Caricati solo 126 MB invece di 1.004. Scaricati: 128 MB di blocchi più 26 MB di indici se si scarica blocco per blocco (86,7% dei byte mossi sono copie, ma 18.195 GET); 399 MB raggruppando i blocchi vicini con buchi fino a 64 KB (2.745 GET); 829 MB con buchi fino a 1 MB (111 GET) (MISURATO).

**Soglia 2: p99 delle letture puntuali sull'uscita cucita entro 1,2 volte quella riscritta.** SUPERATA in tutte le prove: da 0,95 a 1,13 volte (MISURATO, mediana di 5 ripetizioni alternate). Il rumore della macchina è circa ±10%: sotto 1,1 non distinguo differenze vere. Ogni uscita cucita è stata controllata contro quella riscritta: stessi 995.541 documenti, stesse versioni, 222.222 letture uguali, zero sbagliate.

**Soglia 3: nel caso UUID nessun danno.**
- Senza protezione c'era un danno: la cucitura scaricava tutto a pezzi, 15 secondi di CPU invece di 9,4 (+59%) e 17 secondi invece di 11 (MISURATO).
- Ho aggiunto il ripiego: dopo aver letto le etichette, se i pezzi da portare in cucina comunque superano metà dei byte, si fa il lavaggio normale. Con il ripiego: CPU 11,0 contro 11,3 secondi (blocchi da 16 KB) e 11,5 contro 10,7 (4 KB); il costo in più sono le etichette lette per niente, 9 MB e 28 MB, cioè l'1–3% (MISURATO). Quasi nessun danno.

**Soglia 4: muore se dopo molti giri la frammentazione peggiora le letture oltre 1,5 volte senza rimedio.** NON MUORE. 30 giri di cucitura uno dopo l'altro (una scatola grande che cresce da 250 MB a 1,5 GB, più una scatola nuova da 32 MB a ogni giro, blocchi da 4 KB): letture p99 0,91, 0,92 e 1,09 volte la riscritta ai giri 10, 20 e 30 col rimedio acceso, 1,09, 0,94 e 0,97 col rimedio spento (MISURATO). La frammentazione non tocca le letture perché l'indice salta i pezzi morti. Il prezzo è lo spazio: dopo 30 giri l'oggetto cucito pesa 1.477 MB contro 1.148 della riscritta (+29%), con il 22% di pezzi morti (25% senza rimedio) (MISURATO).

In tutti i 30 giri si è copiato il 96–99% dei byte vivi; in tutto 25,1 GB copiati lato server, 1,73 GB scaricati, 0,84 GB caricati, 21,6 secondi di CPU (MISURATO). Lo stesso lavoro col lavaggio normale vuole circa 25 GB scaricati e 25 caricati e circa 255 secondi di CPU (STIMATO da 10,2 secondi per GB MISURATI): circa 12–14 volte di più.

## Perché l'idea com'era scritta non funziona, e cosa ho cambiato

**Il problema delle 5 MB.** S3 vuole che ogni parte di una multipart (copiata o caricata) sia almeno 5 MB, tranne l'ultima. Se le scatole nuove nascono in ordine di chiave, ogni pezzo sporcato (un blocco in cui cade un aggiornamento o una cancellazione) spezza il tratto da copiare. Con l'1% di cancellazioni sparse e il 10% di aggiornamenti, un blocco su 4 è sporcato (17.635 su 63.227, MISURATO): un tratto pulito lungo 5 MB (320 blocchi da 16 KB) quasi non esiste. Resta copiabile solo la parte più nuova della scatola più nuova.

**Cambio 1, le toppe in coda (piatti rifatti messi in fondo al vassoio).** L'indice delle nostre scatole dice per ogni blocco dove sta e quanto è lungo. Allora i blocchi non devono stare in ordine fisico: copio tratti lunghi anche se dentro hanno blocchi sporcati, che restano lì "morti" (l'indice non li indica più), e metto tutti i blocchi rifatti in coda, nell'ultima parte, che può essere piccola. Si spreca spazio, circa il 10% al primo lavaggio (MISURATO: 109–120 MB su 1.124–1.132), in cambio di copie lunghe. Se un tratto ha più del 30% di pezzi morti si riscrive invece di copiarlo (il rimedio).

**Cambio 2, i filtri per blocco.** Un blocco copiato si porta dietro il suo filtro di Bloom, preso dall'etichetta: non serve leggere le sue chiavi. Il formato di proto-lettura ha un filtro unico per scatola, che andrebbe rifatto leggendo tutte le chiavi. Nel formato nuovo (versione 2) ogni blocco ha il suo filtro, e in più voci, posizioni minima e massima. I blocchi sono identici a quelli di proto-lettura: il lettore li legge senza cambi.

**Cambio 3, blocchi da 4 KB.** Un aggiornamento sporca un blocco intero: con blocchi più piccoli si porta in cucina meno roba. Da 16 KB a 4 KB la parte copiata sale da 56% a 90% (MISURATO). Il prezzo: l'etichetta pesa 26 MB per GB invece di 8,4 MB (MISURATO).

**Come si sceglie cosa portare in cucina.** Per ogni coppia di blocchi di scatole diverse che si toccano (220.825 coppie, MISURATO) almeno uno va scaricato. Scelgo prima i blocchi "radi" che toccano molti blocchi altrui (per esempio un blocco pieno di cancellazioni sparse, che copre un intervallo enorme): 1.193 blocchi su 63.227 (MISURATO). Poi leggo le loro chiavi vere e scarico solo i blocchi in cui una chiave cade davvero.

## Cosa cambierebbe nel formato di SlateDB

Ho letto il codice di SlateDB 0.17 (`src/format/sst.rs`, `manifest_generated.rs`). I blocchi sono compressi e cifrati uno per uno, quindi copiarli a byte funziona. Ma servirebbero quattro cambi:
1. l'indice ha per ogni blocco solo l'offset e la prima chiave, e la lunghezza si ricava dall'offset del blocco dopo: blocchi morti e toppe in coda sono impossibili senza aggiungere la lunghezza;
2. manca l'ultima chiave del blocco: si può usare la prima del blocco dopo, ma gli intervalli diventano più larghi e le coppie sovrapposte di più;
3. il filtro di Bloom è uno solo per scatola: va diviso per blocco o per tratto;
4. object_store (la libreria con cui SlateDB parla a S3) non ha UploadPartCopy: il lavapiatti di SlateDB andrebbe riscritto per la parte S3.
Con lo strato in casa questi cambi sono nostri e piccoli; dentro SlateDB sono un fork.

Nel nostro lettore (proto-lettura) va cambiato: il filtro per blocco (fatto nella versione 2 del formato); la lettura di un intervallo di blocchi (`span`) non può più supporre blocchi contigui; e l'etichetta va divisa in due livelli per le scatole grandi (vedi sotto).

## Conti per un database da 1 TB con 10.000 scritture al secondo (STIMATO)

Ipotesi: documenti da circa 1 KB, quindi 10 MB/s di scritture, 26 TB al mese; lavaggio a livelli per dimensione con amplificazione 4 (Fable dice 3–5), quindi 104 TB al mese passano dal lavaggio. Prezzi DICHIARATI (pagina prezzi di S3, aws.amazon.com/s3/pricing): GET $0,0004 ogni 1.000, PUT e COPY $0,005 ogni 1.000, traffico tra EC2 e S3 nella stessa regione gratis. CPU per GB MISURATA qui, moltiplicata per 1,5 per HTTPS e S3 vero (STIMATO). vCPU spot circa $10 al mese (dal rapporto di Fable sul database grande).

Senza cucitura: 0,6 vCPU in media, circa $6 (con margine per i picchi $10–40); GET $3; PUT delle parti $34 con parti da 16 MB, $8 con parti da 64 MB. **Totale circa $20–75 al mese.**

Con la cucitura (toppe in coda, blocchi 4 KB, GET raggruppati con buchi fino a 1 MB): 0,2 vCPU, circa $2 (con margine $10); GET $5; PUT e COPY $9; spazio in più per i pezzi morti (22–30% di 1 TB) $5–7. **Totale circa $20–30 al mese.** Attenzione: con i GET raggruppati solo fino a 64 KB i GET diventano 285 milioni al mese, $114: la regola dei GET va tenuta larga.

Quindi il risparmio in dollari è da 0 a 45 al mese, poco contro i circa $2.200 al mese del database. Quello che cambia davvero: la CPU del lavaggio scende di circa il 70% (MISURATO: 3,1–3,3 secondi per GB invece di 10,2), i byte caricati dell'88% (MISURATO), il lavaggio finisce in metà tempo (MISURATO in locale). Per il profilo da 5 TB con 180 MB/s di lavaggio (rapporto di Fable) vuol dire circa 1 core invece di 3–4 e 20 MB/s caricati invece di 180 (STIMATO). E nel caso tipico di un database grande (una scatola grande e tante piccole nuove, come nei 30 giri) il lavaggio porta in cucina 12–14 volte meno roba (MISURATO).

## Cosa ho deciso io, e perché

- Formato delle scatole: quello di proto-lettura, con i due cambi sopra (versione 2). Era adatto perché i blocchi non hanno puntatori assoluti e l'indice ha offset e lunghezza per blocco.
- Ho fatto due modi, "in ordine" (l'idea scritta) e "toppe in coda" (il mio cambio), per mostrare la differenza con gli stessi dati.
- Ho aggiunto il ripiego sul lavaggio normale per il caso UUID: senza, c'era un danno (+59% di CPU).
- "Recenti" vuol dire gli ultimi 50.000 documenti inseriti, circa il 40% di una scatola; le cancellazioni a caso tra tutti i vivi sono il caso peggiore realistico. Ho provato anche altre finestre e altre cancellazioni. Il risultato dipende molto da dove cadono gli aggiornamenti: con aggiornamenti sparsi su tutta la storia (finestra di un milione di documenti) la parte copiata scende al 22% con blocchi da 16 KB (MISURATO).
- Solo i documenti, senza indici secondari. Un indice secondario su un campo casuale si comporta come il caso UUID (ripiego); uno su una data crescente come l'ObjectId.

## Rischi e cose che non so

- **Etichette troppo grandi sui database grandi.** Con blocchi da 4 KB l'etichetta (indice più filtri) pesa 26 MB per GB: per 1 TB sono circa 26 GB di RAM per copia del bancone (STIMATO, lineare), contro i circa 2 GB per TB del rapporto di Fable. Con blocchi da 16 KB sono 8,4 GB. Serve un'etichetta a due livelli (si tiene in RAM solo il primo, le partizioni si leggono quando servono) o un'etichetta più magra (togliere l'ultima chiave e le statistiche). Questo vale per qualunque formato a blocchi piccoli, non solo per la cucitura: va deciso prima dello strato in casa.
- **Spazio:** +22–29% dopo 30 giri in un database che cresce (MISURATO). In un database che non cresce i pezzi morti salgono finché scatta il rimedio (30% per tratto): NON SO ogni quanto, non l'ho misurato.
- **Velocità di UploadPartCopy su S3 vero** (quanti MB/s copia, quanto tarda): NON SO. Va misurata nella prova su S3 vero (meno di 1 dollaro, con il sì del fondatore).
- **Letture dopo la cucitura:** ho misurato le letture puntuali dal bancone. Le scansioni a intervalli e le letture fredde da S3 non le ho misurate: con le toppe in coda i blocchi vicini per chiave possono stare lontani nell'oggetto, quindi una scansione fredda fa più GET.
- **Un'occasione, non provata:** i blocchi copiati sono identici byte per byte. Se il bancone desse un nome ai blocchi per contenuto e non per posizione, dopo un lavaggio cucito resterebbe caldo il 90% dei blocchi, e il pre-riscaldamento del rapporto di Fable servirebbe solo per il 10%.

## Cosa blocca o cosa manca

- **MinIO non c'è.** Il download da dl.min.io è rifiutato dal proxy (403) e la compilazione dai sorgenti non è stata permessa in questo ambiente. Il finto S3 dei ctl-tests (`ctl-tests/fake_aws.py`) non ha UploadPartCopy (la parte copiata viene ignorata). La regola della coda dice "se nessuno dei due funziona, fermati e scrivilo": non mi sono fermato, perché la prova si poteva fare in modo onesto con un altro finto S3, ma lo segnalo.
- Cosa ho usato: **moto** (finto S3 molto usato, installato con pip), che ha UploadPartCopy con l'intervallo di byte e rifiuta le parti sotto i 5 MB come S3 (MISURATO). Con oggetti da 1 GB moto è stato ucciso per memoria (4,6 GB). Per le prove grandi ho scritto un finto S3 su disco (`proto/cucitura/scripts/finto_s3.py`) con le stesse regole, controllato contro moto: con la stessa prova da 4 scatole da 16 MB i contatori di byte e richieste sono identici (MISURATO).
- Prova su S3 vero non fatta (serve il sì del fondatore; meno di 1 dollaro, STIMATO).

## Decisioni per il fondatore

Sono cambi di disegno, quindi decide il fondatore (io propongo):
1. Formato delle scatole dello strato in casa: filtri per blocco e indice che permette blocchi non contigui (servono alla cucitura e non costano niente in lettura, MISURATO). Proposta: sì.
2. Dimensione dei blocchi e etichetta a due livelli: 4 KB fa funzionare la cucitura ma rende l'etichetta 3 volte più grande. Proposta: decidere insieme alla tappa del lettore dello strato in casa.
3. Quando fare la cucitura: proposta di metterla in fila dopo il lettore e il lavaggio normale dello strato in casa, con una data, non prima del lancio: in dollari vale poco, in CPU e banda vale molto per i database grandi.

## Link

- Branch: `proto-cucitura` di fenvix-engine; PR in bozza verso main (link nel messaggio del commit di questo rapporto e nella lista PR).
- Codice: `proto/cucitura/src/formato.rs` (formato versione 2), `cucitura.rs` (cucitura e lavaggio normale), `carico.rs` (carico), `s3.rs`, `bin/cu-prova.rs` (banco), `scripts/` (finto S3, matrici di prova, riassunti).
- Risultati grezzi: `proto/cucitura/risultati/*.json` e `*.log`.
- Documento tecnico: `docs/proto-cucitura.md`.

## Errori o mancanze trovati in fenvix-brain

- `DESIGN.md`, glossario: "Cucitura → compaction con UploadPartCopy (rimandata)": ora è ripresa (M11).
- `prompts/CODA-MATTINO-2026-11-02.md`, intestazione: "il finto S3 del repo (quello dei ctl-tests, porta 9000)". Sulla porta 9000 i ctl-tests si aspettano MinIO (`ctl-tests/lib.py`); il finto S3 del repo (`ctl-tests/fake_aws.py`) non fa UploadPartCopy. Per le prossime prove con UploadPartCopy: moto per oggetti piccoli, `proto/cucitura/scripts/finto_s3.py` per oggetti grandi.
