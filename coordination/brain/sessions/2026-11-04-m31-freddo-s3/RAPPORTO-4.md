**PRONTA. Una modifica alla policy `fenvix-coldread-test`: sostituire il blocco `"Sid": "Letture"` con questo (testo e passi anche in cima a `PERMESSI.md`):**

```json
    {
      "Sid": "Letture",
      "Effect": "Allow",
      "Action": ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeInstances", "ec2:DescribeVolumes", "ec2:DescribeAddresses", "s3:ListAllMyBuckets"],
      "Resource": "*"
    },
```

# M31 · blocco a 5 $ e controllo finale della pulizia (RAPPORTO-4)

Sessione Opus lanciata da scheduled task il 2026-11-04, prompt `prompts/M31-tetto-5-e-pulizia.md`. Branch `freddo-s3` di fenvix-engine, partito da fc6510d, commit **b6d9d41** (PR #53). 0 $ spesi: nessuna chiamata ad AWS vero, nessun workflow lanciato (nel branch non c'è `comando.json`, quindi il push non fa partire niente), nessun connettore personale, nessun merge.

## In una riga

La riga SPESA adesso è un freno vero: a 5 $ stimati il workflow scrive "BLOCCO", cancella subito la macchina (il cuoco a noleggio, cioè l'istanza EC2) e parte la pulizia. Dopo la pulizia c'è un ultimo controllo che guarda in tutta la regione se è rimasto qualcosa che si paga: scrive "RIMASTO: niente" oppure "ALLARME" e fa diventare rossa la run. Tutte le prove in locale passano.

## 1. Il blocco a 5 $ (`giornata-ci.sh`)

- La riga SPESA resta ogni minuto e dice anche il tetto: "SPESA: 0,89 $ finora a 63 minuti (stima per eccesso: …), tetto 5,00 $". Non c'è più "1,50".
- Il tetto è la variabile `TETTO_DOLLARI`, di serie 5 (nel workflow `TETTO_DOLLARI: "5"`).
- Quando la stima arriva a 5 $: riga "BLOCCO: spesa stimata 6,64 $ ≥ 5,00 $: …", la macchina si cancella subito (`terminate-instances`, permesso che c'è già), esito `BLOCCO_SPESA`, poi recupero dei risultati, pulizia e controllo finale come sempre (tutti con `if: always()`). In cima al riassunto della giornata c'è "# BLOCCO" e un avviso giallo nella pagina della run.

Perché la stima resta per eccesso (in cucina: contiamo più cuochi e più ore di quelli veri, mai meno):
- conto i minuti-macchina di **tutte** le macchine con l'etichetta `project=fenvix-coldread` ancora vive, non solo della nostra: se ne è rimasta una da prima, o ne parte una in più, la stima la paga. Se la lettura non riesce, conto almeno la nostra;
- i minuti partono dall'inizio dello script, prima che la macchina esista, e si arrotondano per eccesso;
- le richieste a S3 non si vedono minuto per minuto, quindi le metto tutte subito: 0,64 $, cioè la somma dei tetti del freno di `esegui.sh` (0,61 $) più 0,03 $ di copie (STIMATO in RAPPORTO-2). Per questo la riga SPESA parte già da circa 0,65 $;
- la riga finale aggiunge 10 minuti per recupero, pulizia e controllo.

Cosa vuol dire in pratica (STIMATO): una giornata normale finisce intorno a 1 $ stimato (0,64 $ di S3 più circa 0,35 $ di macchina), mentre la spesa vera attesa resta 0,40–0,50 $. Il blocco a 5 $ con **una** macchina arriverebbe solo dopo circa 22 ore, ma la macchina si spegne da sola a 240 minuti e il workflow smette di aspettare a 250. Quindi oggi il blocco è una rete di sicurezza: scatta davvero solo se succede qualcosa che i freni di prima non vedono (per esempio più macchine con l'etichetta, o un prezzo diverso).

## 2. Il controllo finale (`controllo-finale.sh`)

È l'ultimo passo del job, sempre (`if: always()`), dopo la pulizia e dopo la pubblicazione dei risultati, con credenziali fresche (un passo di credenziali subito prima). Non cancella niente: guarda soltanto, in eu-west-1:
- le macchine con l'etichetta `project=fenvix-coldread` in qualsiasi stato tranne terminated;
- i dischi (volumi EBS) con quell'etichetta in qualsiasi stato, e quelli **non attaccati** creati dall'inizio della giornata, anche senza etichetta. L'inizio lo scrive il primo passo del workflow (`M31_INIZIO`, 2 minuti prima, per eccesso);
- gli indirizzi Elastic IP con quell'etichetta;
- i bucket (le dispense) che cominciano con `fenvix-coldread-`.

Se non c'è niente: "RIMASTO: niente" e il job passa. Se c'è qualcosa: "ALLARME: rimasto macchina i-… (running, c7i.xlarge); disco vol-…; indirizzo …; bucket …", il job fallisce (rosso), il riassunto del passo comincia con "# ALLARME" e l'elenco, e c'è un errore rosso `::error::` in cima alla pagina della run.

Se una lettura non riesce (permesso mancante, credenziali scadute) o AWS risponde con qualcosa che non si legge, il controllo **non** dice "niente": scrive "ALLARME: rimasto NON SO, lettura non riuscita: …" e il job fallisce. Meglio un allarme di troppo che un "niente" falso.

Permessi: servono solo due letture nuove, `ec2:DescribeVolumes` e `ec2:DescribeAddresses`. `DescribeInstances` e `ListAllMyBuckets` c'erano già. Sono solo Describe, quindi stanno nello statement delle letture con `Resource` `"*"`, come chiede il prompt. Il blocco da incollare è in cima a questo rapporto e in cima a `PERMESSI.md`, con il percorso nella console (Roles → `fenvix-gha-tests` → policy inline `fenvix-coldread-test` → Edit → JSON). `permessi.sh` adesso controlla anche queste due letture: con la policy di oggi il comando `{"modo": "permessi"}` scriverà "MANCA ec2:DescribeVolumes" e "MANCA ec2:DescribeAddresses".

## 3. Le prove in locale

Come sempre: moto (un finto AWS in locale) al posto di AWS, e gli `aws` e `shutdown` finti che c'erano già. All'`aws` finto ho aggiunto tre guasti a comando: una pulizia che dice "fatto" ma non cancella macchine e bucket, un waiter che fallisce subito, e una risposta JSON rotta. Script nuovo: `giornata/prova-locale/prova-blocco-e-controllo.sh`. Risultati in `m31/risultati/prova-locale-tetto5/`.

**(a) spesa finta oltre 5 $** (prezzo finto di 60 $/ora, cioè 1 $ al minuto-macchina; tetto di serie). SPESA 3,64 → 4,64 → 6,64 $, poi "BLOCCO: spesa stimata 6,64 $ ≥ 5,00 $" dopo 5 minuti finti, non al tetto di tempo (40). La macchina è cancellata dal blocco prima della pulizia. La stima della macchina al blocco (6,00 $) è sopra il vero (circa 1,5 $ di vita della macchina): per eccesso. Poi recupero (i passi della macchina sono in mano), pulizia "RIMASTO: niente", controllo finale "RIMASTO: niente" con codice 0. 13 controlli su 13 (MISURATO).

**(a2) come (a), più una seconda macchina con l'etichetta rimasta da prima.** La parte macchina della stima è esattamente il doppio (2 × (minuti + 1)): la seconda si paga. La pulizia cancella tutte e due, controllo finale "RIMASTO: niente". 5 su 5 (MISURATO).

**(b) pulizia che lascia di proposito una macchina e un bucket**, più un Elastic IP con l'etichetta e un disco da 60 GB non attaccato senza etichetta. Il controllo finale scrive "ALLARME: rimasto macchina … (running, c7i.xlarge); disco … (in-use, 60 GB, tag); disco … (available, 60 GB, non attaccato, creato nella giornata); indirizzo …; bucket fenvix-coldread-…", con `::error::`, il riassunto che comincia con "# ALLARME" e codice 1. Un disco non attaccato creato **prima** della giornata non compare, come deve. Controllo del workflow: il controllo finale è l'ultimo passo, ha `if: always()`, non ha `continue-on-error` e ha le credenziali fresche subito prima, quindi il codice 1 fa il job rosso. Dopo la pulizia vera e a mano torna "RIMASTO: niente". 14 su 14 (MISURATO).

**(b2) credenziali scadute** e **(b3) risposta JSON rotta con codice 0**: tutte e due "ALLARME … NON SO, lettura non riuscita", codice 1. 2 su 2 (MISURATO).

In tutto `prova-blocco-e-controllo.sh`: 34 controlli, 0 falliti (MISURATO).

**(c) giornata normale in piccolo** (`prova-giornata-locale.sh`, con il programma vero costruito con `--locked`): permessi "tutti presenti", esito FINITA in 318 s, recupero 19 file, pulizia "RIMASTO: niente", controllo finale "RIMASTO: niente" con codice 0, nessun numero dell'account nei risultati. Nessun BLOCCO, ultima SPESA 0,89 $ a 63 minuti finti (MISURATO con moto).

**prova-guasti.sh rifatta**, con il controllo finale aggiunto a ogni scenario: 28 controlli, 0 falliti (prima 24; i 4 nuovi sono "controllo finale: RIMASTO: niente" nei quattro guasti) (MISURATO).

**Policy:** `controlla-policy.py` adesso riconosce i blocchi di PERMESSI.md dal contenuto, non dall'ordine. Controlla che la policy di oggi abbia tutto tranne le due letture nuove, che il blocco nuovo abbia solo letture su "*", e che dopo la modifica (con e senza il restringimento di Canonical) ogni chiamata degli script sia permessa e i freni rifiutino ancora quello che devono. 137 controlli, 0 falliti (MISURATO). In più, con moto e IAM acceso (`prova-permessi-iam.sh`, che adesso accetta `SCRIPT=controllo-finale.sh`): con la policy di oggi il controllo finale scrive ALLARME "NON SO" per dischi e indirizzi; con la policy aggiornata "RIMASTO: niente". Quelli di `permessi.sh` sono MANCA con la policy di oggi e OK con quella aggiornata (MISURATO). Gli altri due MANCA che restano lì (RunInstances, GetInstanceProfile) sono il limite di moto già spiegato in RAPPORTO-3.

Il workflow passa `actionlint` (MISURATO).

## 4. Cosa ho deciso io

- Al BLOCCO cancello la macchina subito, dentro `giornata-ci.sh`, invece di aspettare la pulizia: si risparmiano i minuti del recupero, e quello che la macchina aveva già caricato nel bucket resta.
- S3 nella stima conta tutto il tetto dei freni fin dal primo minuto. È l'unico modo di restare per eccesso senza contare le richieste vere minuto per minuto.
- Una run finita con BLOCCO_SPESA non diventa rossa da sola: il passo della giornata ha già `continue-on-error`, e il prompt chiede il rosso solo per l'ALLARME. Si vede dall'avviso giallo e da "# BLOCCO" nel riassunto. Se il fondatore vuole il rosso anche lì, basta una riga.
- "In cima al riassunto della run": GitHub mette i riassunti dei passi uno dopo l'altro, e l'ultimo passo non può scrivere sopra quelli di prima. Per questo l'ALLARME va anche in un'annotazione `::error::`, che GitHub mostra in cima alla pagina della run, sopra i riassunti. Il riassunto del passo di controllo comincia comunque con "# ALLARME".
- Una lettura che non riesce vale come ALLARME (NON SO), non come "niente".
- Il controllo finale non cancella dischi e indirizzi: servirebbero `ec2:DeleteVolume` e `ec2:ReleaseAddress`, cioè permessi di scrittura, e il prompt chiede di aggiungere solo letture. Oggi la giornata non crea Elastic IP e il disco si cancella con la macchina (`DeleteOnTermination`). Se l'ALLARME li trova, si cancellano a mano dalla console.

## 5. Cosa manca o blocca

- Blocca la giornata: la modifica della policy qui sopra (senza, il job finisce rosso con "NON SO" anche se tutto è pulito), poi il comando `{"modo": "permessi"}`, poi il sì del fondatore sulla cifra (attesa 0,40–0,50 $, STIMATO in RAPPORTO-2).
- Il tetto calcolato in RAPPORTO-2 (1,49 $) resta il peggio con una macchina. Il blocco a 5 $ non lo cambia: si aggiunge sopra.
- Le prove locali non provano GitHub vero (annotazioni, colore del job) né IAM vero. Il primo comando "permessi" e la prima giornata lo diranno.
- La prova del freno (`prova-freno.sh`) non l'ho rifatta: non tocca niente di quello che ho cambiato.

## 6. Link

- Branch `freddo-s3`, PR #53: https://github.com/Mircoguidetti/fenvix-engine/pull/53, commit b6d9d41.
- File cambiati: `.github/workflows/m31-freddo-s3.yml` (`TETTO_DOLLARI`, `M31_INIZIO`, credenziali fresche e controllo finale come ultimi passi), `proto/lavaggio-per-cliente/m31/giornata/giornata-ci.sh`, `controllo-finale.sh` (nuovo), `permessi.sh`, `PERMESSI.md`.
- Prove: `giornata/prova-locale/prova-blocco-e-controllo.sh` (nuovo), `prova-guasti.sh`, `prova-giornata-locale.sh`, `prova-permessi-iam.sh`, `controlla-policy.py`, `bin/aws`. Risultati: `m31/risultati/prova-locale-tetto5/`.

## 7. Errori o mancanze trovati in fenvix-brain

- RULES.md dice che la pulizia "cancella tutto ciò che ha creato (… volumi EBS, indirizzi IP …)". Oggi la pulizia cancella macchine e bucket. Dischi e IP pubblico se ne vanno con la macchina, ma un disco o un Elastic IP rimasti da soli il controllo finale li **trova** e non li **cancella** (servirebbero permessi di scrittura). Se il coordinatore vuole anche la cancellazione, serve un prompt con `ec2:DeleteVolume` e `ec2:ReleaseAddress` sotto condizione di etichetta.
- RAPPORTO-2 (e un commento in `esegui.sh`) parlano ancora di "tetto 1,50 $" come cifra della giornata. Resta giusto come peggio calcolato, ma il tetto del blocco adesso è 5 $: conviene dirlo così in NOW.md.
