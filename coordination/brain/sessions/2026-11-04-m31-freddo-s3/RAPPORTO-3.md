**NOMI ALLINEATI.** File cambiati in fenvix-engine, branch `freddo-s3`, commit fc6510d: `.github/workflows/m31-freddo-s3.yml`, `giornata/PERMESSI.md`, `giornata/permessi.sh`, `giornata/giornata-ci.sh`, `giornata/pulizia.sh`, `giornata/prova-locale/comune.sh`; nuovi `giornata/prova-locale/controlla-policy.py`, `giornata/prova-locale/prova-permessi-iam.sh` e la cartella `risultati/prova-locale-nomi/` (tutti sotto `proto/lavaggio-per-cliente/m31/`). 0 $ spesi.

# M31 · nomi inglesi per le risorse AWS (RAPPORTO-3)

Sessione Opus lanciata da scheduled task il 2026-11-04, prompt `prompts/M31-nomi-coldread.md`. Nessuna chiamata ad AWS vero, nessun workflow lanciato (il push tocca il workflow ma non `comando.json`, quindi non parte niente), nessun `comando.json`, nessun connettore personale, nessun merge.

## In una riga

Il codice usa adesso esattamente i nomi che il fondatore ha creato in AWS. La policy già creata (quella senza la regola di Canonical) basta per la giornata: `permessi.sh` passa. La regola di Canonical è solo un restringimento facoltativo, e per funzionare va aggiunta **togliendo anche una riga** (sezione 4).

## 1. I nomi cambiati

- ruolo e profilo della macchina (il cuoco a noleggio): `fenvix-m31-macchina` → `fenvix-coldread-machine` (workflow, `comune.sh`, PERMESSI.md);
- policy della macchina: `solo-bucket-m31` → `coldread-buckets-only` (solo in PERMESSI.md: nessuno script la nomina);
- bucket (la dispensa della prova): `fenvix-m31-<run>` → `fenvix-coldread-<run>`, e il bucket vuoto di `permessi.sh` `fenvix-coldread-permessi-<run>`; la pulizia cerca `^fenvix-coldread-`;
- etichetta: `progetto=fenvix-m31` → `project=fenvix-coldread` su macchina e disco al lancio, sul bucket, nei filtri di `pulizia.sh` e `permessi.sh`; anche l'etichetta `Name` della macchina è ora `fenvix-coldread`;
- policy sul ruolo GitHub `fenvix-gha-tests`: `fenvix-m31` → `fenvix-coldread-test` (solo PERMESSI.md).

Altri resti di "m31" che finivano su AWS, cambiati anche loro: il nome della sessione nel rinnovo delle credenziali (`m31-giornata` → `coldread-giornata`, si vede in CloudTrail) e il nome dell'oggetto del programma nel bucket (`programma/coldread-giornata.tar.gz`).

Resti lasciati apposta, perché non arrivano in AWS: cartelle e file del repo (`m31/`, `m31-freddo-s3.yml`), le variabili `M31_*`, il gruppo di concorrenza di GitHub `fenvix-m31` e il nome dell'artifact di GitHub, i messaggi di `shutdown` sulla macchina ("M31: tetto"), il percorso locale `/tmp/m31-giornata.tar.gz`. I `Sid` della policy GitHub (per esempio `CancellareSoloLeMacchineM31`) li ho lasciati come nel testo che il fondatore ha copiato: sono solo etichette, nessuno script li usa.

## 2. PERMESSI.md

In cima: "Già fatto dal fondatore il 2026-11-04", con l'elenco dei nomi. Poi il testo esatto delle due policy create (con `<ACCOUNT_ID>`): `coldread-buckets-only` e `fenvix-coldread-test` nella versione di prima delle correzioni (l'immagine `image/*` sta dentro `MacchinaRisorseDiServizio`). Il restringimento di Canonical è in un paragrafo a parte, segnato "facoltativo". Il paragrafo su walbench ora dice che il suo divieto ammette c6i.large e c7i.xlarge. I passi "Come si lancia" restano uguali (freno = "Cancel workflow", mai due `prova`).

## 3. Le prove in locale (moto, il finto AWS)

**Tutta la giornata, in piccolo** (`prova-giornata-locale.sh`, MISURATO, `risultati/prova-locale-nomi/giornata-locale.txt`): `permessi.sh` "PERMESSI: tutti presenti"; `giornata-ci.sh` esito FINITA in 355 s; `recupera.sh` 19 file; `pulizia.sh` "bucket cancellato: fenvix-coldread-…" e "RIMASTO: niente"; nessun numero dell'account nei risultati. Nel registro delle chiamate AWS della prova compaiono solo `fenvix-coldread-<run>` (101 volte), `fenvix-coldread-permessi-locale`, `fenvix-coldread-machine`, `Key=project,Value=fenvix-coldread` e `tag:project`; nessun nome vecchio (MISURATO, ricerca nel registro).

**Ricerca dei nomi vecchi nel branch**, fuori dai risultati vecchi: nessun `fenvix-m31-`, `solo-bucket-m31`, `progetto`, `m31-macchina` negli script, nel workflow e in PERMESSI.md. Restano solo il gruppo di concorrenza di GitHub (sezione 1) e i casi "nome vecchio deve essere rifiutato" dentro `controlla-policy.py`.

**Il modo "permessi" con la policy del fondatore** (il punto 5 del prompt). Ho fatto due controlli, perché uno solo non basta:

- `controlla-policy.py`: legge le policy da PERMESSI.md e le confronta con ogni chiamata che fanno `permessi.sh`, `giornata-ci.sh`, `recupera.sh`, `pulizia.sh` (ruolo GitHub) ed `esegui.sh` (ruolo della macchina), condizioni comprese (etichetta `project`, tipo c7i.xlarge, `CreateAction`, `PassedToService`, `ec2:Owner`). Con la policy creata dal fondatore: tutte le chiamate permesse; rifiutate come devono una macchina più grande, una macchina senza etichetta o con l'etichetta vecchia, la cancellazione di una macchina non nostra, il ruolo, il profilo e i bucket con i nomi vecchi. Con la policy più il restringimento: stesso risultato, e in più rifiutata un'immagine non di Canonical. Policy della macchina: lettura e scrittura solo nei bucket `fenvix-coldread-*`. 96 controlli, 0 falliti (MISURATO, `controlla-policy.txt`). È una mia imitazione delle regole di IAM, ristretta agli operatori usati in queste policy: non è IAM vero.
- `prova-permessi-iam.sh`: `permessi.sh` contro moto con IAM acceso, come un utente che ha solo la policy `fenvix-coldread-test`. Tutta la parte S3 e le letture passano con la policy nuova, e con i nomi vecchi falliscono 9 permessi S3 su 9 (MISURATO, `permessi-iam-moto.txt`). Limite di moto: per EC2 e IAM controlla la policy solo contro la risorsa "*", quindi RunInstances e GetInstanceProfile risultano sempre "MANCA" anche quando la policy è giusta. Per questo quelle due le copre il primo controllo.

Il verdetto vero lo dà solo il primo comando `{"modo": "permessi"}` su AWS (0 $). Da questi controlli mi aspetto "PERMESSI: tutti presenti" (STIMATO). Il test con IAM in moto usa l'account finto di moto (123456789012), che compare nei file di risultato: non è il nostro.

## 4. La regola di Canonical: pronta da incollare, facoltativa

**Non è obbligatoria.** È solo un restringimento (il cuoco non può partire con un'immagine a pagamento del Marketplace). Gli script usano sempre l'immagine Ubuntu ufficiale letta da SSM, quindi la giornata funziona anche senza.

Attenzione: aggiungere il blocco **da solo non cambia niente**. Le Allow si sommano, e `image/*` è ancora permessa da `MacchinaRisorseDiServizio`. Servono due modifiche nella policy `fenvix-coldread-test`:

1. togliere da `MacchinaRisorseDiServizio` la riga `"arn:aws:ec2:eu-west-1::image/*",`
2. aggiungere nella lista `Statement` questo blocco:

```json
    {
      "Sid": "SoloImmaginiDiCanonical",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": "arn:aws:ec2:eu-west-1::image/*",
      "Condition": { "StringEquals": { "ec2:Owner": "099720109477" } }
    },
```

`099720109477` è l'account di Canonical (DICHIARATO, documentazione Ubuntu "Find Ubuntu images on AWS", https://ubuntu.com/aws/docs/aws-how-to/instances/find-ubuntu-images/). Con le due modifiche `permessi.sh` passa lo stesso (controllo `controlla-policy.py`, policy "ristretta"), perché l'immagine letta da SSM è di Canonical.

## 5. Cosa ho deciso io

- Ho cambiato anche il nome della sessione STS e quello dell'oggetto del programma: tutti e due finiscono in AWS (CloudTrail e bucket).
- Ho lasciato il gruppo di concorrenza `fenvix-m31` del workflow: è di GitHub, non di AWS, e cambiarlo non serve.
- Ho lasciato i `Sid` vecchi in PERMESSI.md, perché il testo deve essere quello creato davvero.
- Ho aggiunto due script di prova (`controlla-policy.py`, `prova-permessi-iam.sh`): moto da solo non controlla le condizioni di IAM, e senza questi non potevo verificare il punto 5.

## 6. Cosa manca o blocca

- Il testo esatto delle policy in AWS non l'ho visto: ho scritto in PERMESSI.md quello descritto nel prompt (versione di prima delle correzioni più i nomi nuovi). Se il fondatore ha cambiato altro mentre copiava, lo dirà il comando `permessi`.
- La trust policy del ruolo `fenvix-gha-tests`: il prompt dice che `freddo-s3` resta com'è. Ho dato per scontato che il branch sia già nella condizione `sub`. Se non c'è, il workflow si ferma al primo passo delle credenziali, a 0 $.
- Resta valido tutto il resto di RAPPORTO-2: per la giornata serve il sì del fondatore sulla cifra (attesa circa 0,40–0,50 $, tetto 1,50 $, STIMATO).
- La prova dei guasti (`prova-guasti.sh`) e quella del freno non le ho rifatte. Usano gli stessi script con i nomi cambiati, e la giornata intera è passata.

## 7. Link

- Branch `freddo-s3`, PR #53: https://github.com/Mircoguidetti/fenvix-engine/pull/53 (commit fc6510d).
- `proto/lavaggio-per-cliente/m31/giornata/PERMESSI.md`
- Prove: `giornata/prova-locale/controlla-policy.py`, `giornata/prova-locale/prova-permessi-iam.sh`, `giornata/prova-locale/prova-giornata-locale.sh`; risultati in `m31/risultati/prova-locale-nomi/`.

## 8. Errori o mancanze trovati in fenvix-brain

- Il prompt chiede di aggiungere la regola di Canonical, ma non dice che va tolta anche `image/*` da `MacchinaRisorseDiServizio`: senza questo passo la regola non restringe niente. L'ho scritto qui e in PERMESSI.md.
