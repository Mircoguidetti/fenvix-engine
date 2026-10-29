# aws-test3 — database dormienti del motore finale su S3 vero (eu-central-1)

- **Run:** GitHub Actions 40846615265, 29/10/2026, dalle 09:03 alle 13:07 UTC (driver 09:08–12:59).
- **Branch:** `aws-test3`. Dati grezzi in `results-aws-test3/`.
- **Motore:** commit `41ed169` più una riga dichiarata (`aws-test3/tenantstats-auth.patch`, sha256 `4cf868d2…c396`). Binario compilato nel runner, sha256 `28495466b791d073b9eff63183efd08ce4c7ff2d5b966e9efb5395c2f70ab18f`.
- **Metodo:** ruolo OIDC del Test 1, credenziali solo dall'ambiente di Actions, nessuna chiave, nessuna EC2, nessun altro servizio oltre S3 (e STS per rinnovare la sessione del ruolo).
- **Bucket:** nuovo, `fenvix-test-t3-<run>`, con tag e scadenza a 1 giorno.

**Avvertenza sulle latenze:** il runner di GitHub non è a Francoforte. Tutte le latenze qui sotto sono più alte di quelle di un server in eu-central-1 e **non sono le latenze finali**.

## Esito in breve

| | Esito |
|---|---|
| Parte A (credenziali, lease e fencing, ciclo completo) | **passata** |
| Parte B, N = 5 → 2.000 | **tutti i gradini completati** |
| Richieste S3 a riposo (regime, 30 minuti) | **0 a ogni N** (dettagli §3) |
| Spesa stimata totale | **0,849 USD** (tetto 5 USD) |
| Bucket e oggetti | **eliminati, verificato** |
| Confronto con Atlas | **non fatto**: pagina ufficiale non raggiungibile (§5) |

## 1. Parte A — controlli su S3 vero

Risultati misurati, file `partA.json`.

- **A1, credenziali:** motore avviato con `--region eu-central-1`, senza `--endpoint`, credenziali solo dall'ambiente di Actions. Ha scritto e riletto. Passata.
- **A3, ciclo completo:** creato un database, scritti 200 documenti, atteso il riposo (62 s con `--idle-secs 60`), riletto tutto. Poi motore riavviato e riletto di nuovo. **200 documenti su 200, identici al byte** (impronta sha256 dei documenti) in entrambe le riletture. Zero dati persi.
- **A2, lease e fencing:** due processi del motore sullo stesso bucket e sullo stesso database, con holder diversi e lease da 10 s. Cosa è successo davvero, in ordine:

| t (s) | Chi | Azione | Risultato |
|---|---|---|---|
| 0,0 | P1 | scrive `p1-1` | ok, P1 prende il lease |
| 5,2 | P2 | scrive `p2-1` | **rifiutato**, codice 134 `TenantLeaseHeld` ("lease valid for another 6829 ms") |
| 5,9 | P2 | legge | **rifiutato**, 134 (anche le letture richiedono il lease) |
| 5,9 | P1 | congelato con SIGSTOP (niente rinnovi) | |
| 8,9 / 12,1 | P2 | riprova | rifiutato, il lease scade (3652 ms, poi 476 ms) |
| 15,3 | P2 | scrive `p2-2` | **ok: subentra dopo la scadenza** |
| 20,7 | P1 | ripreso con SIGCONT, crede di avere ancora il database | |
| 20,7 | P1 | scrive `p1-2` | **rifiutato**, 134 ("owned by … holder-2") |
| 23,9 | P1 | scrive `p1-3` | **rifiutato**, 134 |
| 24,1 | P2 | legge tutto | `["p1-1", "p2-2"]` |

Le scritture di P1 dopo la perdita del lease non sono finite nel database: il fencing funziona. Contatori dei lease: P1 1 lease perso e 2 rifiuti; P2 1 subentro, 2 rinnovi, 0 rinnovi falliti.

Su S3 le scritture condizionali (If-None-Match, If-Match) sono quelle usate dal codice dei lease (`lease.rs`). Il test verifica il comportamento risultante, non i singoli header HTTP.

## 2. Parte B — tabella per N

Ogni database ha 200 documenti (circa 1 KB ciascuno), 2 collezioni e 1 indice secondario, scritti con pymongo, `--auth` e TLS `requireTLS`. Il motore gira con `--idle-secs 60 --retention-window-secs 604800 --wal-gc-mode bulk --auth --tls-mode requireTLS`. Ogni gradino aggiunge database ai precedenti.

- **Come sono misurati byte e oggetti:** elenco completo del bucket (`list-objects-v2`) a database fermi.
- **Come sono misurate le richieste:** contatori del motore (`tenantStats`). Le richieste ritentate non sono contate (vedi §8).

| N | Oggetti totali | Byte totali | Byte per db (mediana) | Oggetti per db (mediana / max) | Riposo 30 min, PUT-tier / GET-tier | Risveglio 1ª richiesta p50 / p95 / max (ms) | 2ª richiesta p50 (ms) | Richieste per risveglio (mediana) |
|---|---|---|---|---|---|---|---|---|
| 5 | 98 | 2.252.693 | 374.320 | 13 / 13 | 40 / 110 (tutte nel 1° minuto) | 1.883 / 2.031 / 2.031 | 170 | 11 GET, 1 LIST, 1 HEAD, 1 PUT |
| 10 | 153 | 4.121.763 | 373.589 | 12 / 13 | 40 / 110 (1° minuto) | 1.911 / 2.124 / 2.124 | 175 | 11 GET, 1 LIST, 1 HEAD, 2 PUT |
| 50 | 664 | 19.067.762 | 373.822 | 13 / 14 | 488 / 1.348 (1° minuto) | 1.933 / 2.076 / 2.162 | 174 | 11 GET, 1 LIST, 1 HEAD, 5 PUT |
| 100 | 1.219 | 37.730.770 | 373.579 | 12 / 13 | 464 / 1.260 (1° minuto) | 1.932 / 2.061 / 2.097 | 175 | 11 GET, 1 LIST, 1 HEAD, 5 PUT |
| 1.000 | 14.561 | 375.149.285 | 374.762 | 16 / 17 | **0 / 0** | 2.252 / 2.320 / 2.359 | 174 | 13 GET, 1 LIST, 1 HEAD, 7 PUT |
| 2.000 | 28.502 | 749.430.906 | 374.531 | 13 / 17 | **0 / 0** | 2.226 / 2.310 / 2.316 | 172 | 13 GET, 1 LIST, 1 HEAD, 6 PUT |

- **Campione di risveglio:** tutti i database per N ≤ 50, 50 a caso per N > 50. Una richiesta alla volta, su database a riposo.
- **Prefissi diversi dai database** (utenti `admin`, Parte A, controllo): 33 oggetti, 381.516 byte, costanti per ogni N.
- **Creazione di un database** (crea, scrive 200 documenti, chiude), misurata come media sui gradini 1.000 e 2.000: **circa 69 richieste PUT-tier e 115 GET-tier per database**, cioè circa 0,00042 USD per database.
- **Per fase:** `partB_phases.csv`. Per i gradini piccoli (5 e 10) la divisione tra "create" e "write" non è affidabile: i contatori vengono letti ogni 10 secondi, e fasi più brevi finiscono attribuite alla fase dopo. I totali per gradino invece sono corretti.

## 3. Riposo: cosa succede davvero

- **A regime, zero richieste a ogni N.** Lettura minuto per minuto dei contatori (`partB_rest_minutes.csv`): dal secondo minuto al trentesimo le richieste non aumentano mai, né con 5 né con 2.000 database.
- **N ≤ 100:** le richieste segnate nel "riposo" (per esempio 40 + 110 con 5 database) arrivano tutte **nel primo minuto**. Sono la coda della chiusura (LIST, GET e DELETE del garbage collector dopo che i database risultano chiusi), non un costo che si ripete.
- **N = 1.000 e 2.000:** zero anche nel primo minuto, perché prima del riposo lo script ha riavviato il motore per rinnovare le credenziali, e la coda della chiusura è avvenuta prima della finestra.
- **Limite:** ho osservato 30 minuti, cioè 3 cicli della pulizia dei punti di ripristino (`--retention-prune-secs` 600 di default). Con la finestra a 7 giorni nessun punto è scaduto durante il test. Il costo della pulizia quando i punti iniziano a scadere, dopo 7 giorni, **non è misurato**.

## 4. Il costo dipende da N?

- **Avvio del motore:** **non dipende da N.** A ogni gradino, nei 60 secondi dopo l'avvio senza client: 7 PUT, 9 GET, 1 LIST, 1 HEAD, sia con 5 sia con 2.000 database. Nessun LIST dell'intero bucket.
- **Risveglio:** sempre **1 solo LIST** per risveglio, da 5 a 2.000. Quindi nessun LIST dell'intero bucket.
- **Da N=1.000 in su:** le GET per risveglio passano da 11 a 13 e la latenza sale di circa 300 ms. La causa non è dimostrata. È coerente con il fatto che quei database hanno più oggetti (mediana 16 contro 12–13 a N=1.000): sono stati creati in un gradino molto più carico. Non è un effetto di N in sé, ma non l'ho isolato.

## 5. Costo mensile calcolato

**Prezzi** (S3 Standard, eu-central-1), letti dal listino ufficiale AWS https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-central-1/index.json (pubblicato il 15/10/2026, letto il 29/10/2026 alle 09:03 UTC):
- PUT, COPY, POST, LIST: 0,0054 USD ogni 1.000;
- GET e altre: 0,0043 USD ogni 10.000;
- storage: 0,0245 USD per GB-mese, primi 50 TB.

**Formula:**
- storage = N × 374.530 byte / 2^30 × 0,0245
- richieste = N × M × 30 giorni × K × costo di un risveglio
- costo di un risveglio = 11 PUT-tier × 0,0000054 + 21 GET-tier × 0,00000043 = 0,0000688 USD

Il risveglio misurato vale 7 PUT-tier e 14 GET-tier. Ci aggiungo la chiusura che segue, che in un risveglio di sola lettura **non è misurata**: uso come limite superiore la chiusura dopo le scritture del gradino 2.000 (4 PUT-tier e 7 GET-tier).

**Ipotesi:** M% dei database attivi in un giorno (M = 1 e 10), K risvegli al giorno per ogni database attivo (K = 1 e 10). Riposo a regime = 0 richieste (§3).

| N | M=1%, K=1 | M=1%, K=10 | M=10%, K=1 | M=10%, K=10 | di cui storage |
|---|---|---|---|---|---|
| 5 | 0,0001 | 0,0011 | 0,0011 | 0,0104 | 0,00004 |
| 10 | 0,0003 | 0,0022 | 0,0022 | 0,0207 | 0,00009 |
| 50 | 0,0015 | 0,0108 | 0,0108 | 0,1036 | 0,00043 |
| 100 | 0,0029 | 0,0215 | 0,0215 | 0,2073 | 0,00085 |
| 1.000 | 0,0292 | 0,2150 | 0,2150 | 2,0728 | 0,0086 |
| 2.000 | 0,0584 | 0,4299 | 0,4299 | 4,1455 | 0,0171 |

Valori in USD al mese (`results-aws-test3/monthly_cost.csv`).

**Esclusi da questo costo:**
- la macchina che fa girare il motore (EC2 o altro);
- le richieste ritentate;
- la creazione iniziale (circa 0,00042 USD per database, una volta sola);
- la pulizia dei punti di ripristino dopo 7 giorni (non misurata, §3);
- le tasse.

**Confronto con MongoDB Atlas M10: non fatto.** Non ho potuto leggere il prezzo dalla pagina ufficiale: da questa sessione `www.mongodb.com` non è raggiungibile (rifiutato dalla rete della sessione). Come da tua regola, non lo stimo a memoria. Servono una di queste due cose:
- abilitare `www.mongodb.com` nelle impostazioni di rete dell'ambiente;
- mandarmi il prezzo con data e link.

Poi aggiungo il confronto.

## 6. Spesa stimata totale

**0,849 USD**, calcolata durante il run dai conteggi reali (`run.json`, `cost_live.csv`):
- 138.445 richieste PUT-tier, di cui 49 LIST del runner;
- 233.788 richieste GET-tier;
- lo storage, con 24 ore in più di margine.

Il tetto di 5 USD non è mai stato vicino. Le richieste ritentate non sono contate. La fattura reale la confronti tu dopo 24 ore.

## 7. Pulizia

`bucket_delete.json`: bucket svuotato, eliminato, e verificato con `head-bucket` che non esiste più. Prima c'era la regola di scadenza a 1 giorno come rete di sicurezza.

**Log pubblici controllati dopo il run:** 0 occorrenze di ID account, nome del ruolo, ARN o chiavi. I file pubblicati sono passati dalla scansione (0 file da oscurare).

## 8. Problemi trovati nel motore

1. **Con `--auth` nessun utente può leggere i contatori (`tenantStats`).** Il comando richiede `anyAction`, che nessun utente può avere, nemmeno root. Non si può nemmeno creare un ruolo che lo dia. Per il test ho aggiunto una riga dichiarata: `tenantStats` con lo stesso privilegio di `serverStatus`.
2. **Il motore legge le credenziali una volta sola, all'avvio.** Con sessioni brevi (il ruolo OIDC dura 1 ora) va riavviato prima della scadenza: il test l'ha fatto 9 volte. Il codice supporta anche il file di token web identity, che si rinnoverebbe da solo, ma non l'ho provato.
3. **Il contatore delle richieste non vede i retry**, perché sta sopra lo strato dei retry di `object_store`. La spesa reale può essere un po' più alta di quella contata.
4. **Il risveglio fa circa 11–13 GET una dopo l'altra.** Dal runner costa circa 2 secondi. Da Francoforte sarà meno, ma resta il numero di richieste in fila che lo determina.

## 9. Decisioni che servono a te

1. **Atlas:** abilitare `www.mongodb.com` nella rete della sessione, oppure darmi il prezzo M10 con data e link.
2. **`tenantStats` con `--auth`:** portare nel motore la riga del test, o un privilegio dedicato, così un operatore può misurare un motore in produzione.
3. **Credenziali che scadono:** usare in produzione il file di token web identity (rinnovo automatico), oppure accettare riavvii periodici.
4. **Latenze reali:** decidere se misurare da una macchina in eu-central-1. Servirebbe EC2, escluso da questo test.

## 10. Cosa NON ho potuto verificare

- Le latenze da un server in eu-central-1: il runner non è lì.
- Il riposo oltre 30 minuti, e la pulizia dei punti di ripristino quando scadono, dopo 7 giorni.
- Il costo della chiusura dopo un risveglio di sola lettura (nel calcolo uso un limite superiore).
- Le richieste ritentate da S3.
- Il prezzo di Atlas.
- La fattura reale.
- Gli header HTTP condizionali uno per uno: ho verificato il comportamento dei lease, non il traffico.
- Perché da N=1.000 il risveglio fa 2 GET in più (§4).
