# M31 · i permessi in AWS per la giornata (nomi "coldread")

## DA FARE PRIMA DELLA GIORNATA (M31 blocco a 5 $ e controllo finale, 2026-11-04): una modifica

Il controllo finale (ultimo passo del job) deve leggere anche i dischi EBS e gli indirizzi Elastic IP. Servono due
letture in più, `ec2:DescribeVolumes` e `ec2:DescribeAddresses` (solo Describe, quindi `Resource` `"*"`). Nient'altro.

Dove: console IAM → Roles → `fenvix-gha-tests` → Permissions → policy inline `fenvix-coldread-test` → Edit → JSON.
Cosa: **sostituisci** il blocco con `"Sid": "Letture"` (il primo della lista `Statement`) con questo, virgola finale
compresa:

```json
    {
      "Sid": "Letture",
      "Effect": "Allow",
      "Action": ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeInstances", "ec2:DescribeVolumes", "ec2:DescribeAddresses", "s3:ListAllMyBuckets"],
      "Resource": "*"
    },
```

Poi "Next" e "Save changes". Il resto della policy non cambia. Il comando `{"modo": "permessi"}` lo controlla (0 $):
senza questa modifica scrive "MANCA ec2:DescribeVolumes" e "MANCA ec2:DescribeAddresses". Senza la modifica una
giornata partirebbe lo stesso, ma il controllo finale non potrebbe leggere dischi e indirizzi: scriverebbe ALLARME
("NON SO, lettura non riuscita") e il job finirebbe rosso anche con tutto pulito.

**Già fatto dal fondatore il 2026-11-04** (alle 18:15, dalla console IAM). Questo file dice cosa c'è, con il testo
esatto delle policy create; non c'è niente da rifare tranne la modifica "DA FARE" qui sopra. Regione: eu-west-1. `<ACCOUNT_ID>` sta al posto del numero
dell'account (12 cifre).

I nomi in AWS:
- ruolo e profilo della macchina: `fenvix-coldread-machine`;
- policy inline della macchina: `coldread-buckets-only`;
- bucket: prefisso `fenvix-coldread-` (per esempio `fenvix-coldread-<run>`);
- etichetta su macchina e disco (messa al lancio, letta dalla pulizia e da TerminateInstances): `project` = `fenvix-coldread`;
- ruolo GitHub: `fenvix-gha-tests` (il suo ARN sta nel secret `AWS_ROLE_ARN`); policy inline su quel ruolo: `fenvix-coldread-test`.

## 1. Serratura del ruolo GitHub (trust policy)

Nella condizione su `token.actions.githubusercontent.com:sub` del ruolo `fenvix-gha-tests` c'è il branch `freddo-s3`:

```
repo:Mircoguidetti/fenvix-engine:ref:refs/heads/freddo-s3
```

## 2. Ruolo della macchina: `fenvix-coldread-machine`

Ruolo per il servizio EC2, con il profilo dell'istanza dello stesso nome. Una sola policy inline,
`coldread-buckets-only`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::fenvix-coldread-*", "arn:aws:s3:::fenvix-coldread-*/*"]
    }
  ]
}
```

La macchina legge e scrive solo nei bucket `fenvix-coldread-*`. Nient'altro.

## 3. Policy inline `fenvix-coldread-test` sul ruolo GitHub `fenvix-gha-tests`

Testo creato (quello di prima delle correzioni del revisore, con i nomi nuovi; i `Sid` sono solo etichette e non li
usa nessuno script):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Letture",
      "Effect": "Allow",
      "Action": ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeInstances", "s3:ListAllMyBuckets"],
      "Resource": "*"
    },
    {
      "Sid": "ImmagineUbuntuPubblica",
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:eu-west-1::parameter/aws/service/canonical/*"
    },
    {
      "Sid": "MacchinaSoloC7iXlargeConIlTag",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:volume/*"],
      "Condition": {
        "StringEquals": { "aws:RequestTag/project": "fenvix-coldread" },
        "StringEqualsIfExists": { "ec2:InstanceType": "c7i.xlarge" }
      }
    },
    {
      "Sid": "MacchinaRisorseDiServizio",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": [
        "arn:aws:ec2:eu-west-1::image/*",
        "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:subnet/*",
        "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:network-interface/*",
        "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*"
      ]
    },
    {
      "Sid": "TagSoloAlLancio",
      "Effect": "Allow",
      "Action": "ec2:CreateTags",
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:volume/*"],
      "Condition": { "StringEquals": { "ec2:CreateAction": "RunInstances" } }
    },
    {
      "Sid": "CancellareSoloLeMacchineM31",
      "Effect": "Allow",
      "Action": "ec2:TerminateInstances",
      "Resource": "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*",
      "Condition": { "StringEquals": { "ec2:ResourceTag/project": "fenvix-coldread" } }
    },
    {
      "Sid": "PassareSoloIlRuoloDellaMacchina",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/fenvix-coldread-machine",
      "Condition": { "StringEquals": { "iam:PassedToService": "ec2.amazonaws.com" } }
    },
    {
      "Sid": "LeggereIlProfiloDellaMacchina",
      "Effect": "Allow",
      "Action": "iam:GetInstanceProfile",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:instance-profile/fenvix-coldread-machine"
    },
    {
      "Sid": "BucketDellaProva",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketTagging", "s3:PutLifecycleConfiguration",
        "s3:PutBucketPublicAccessBlock", "s3:ListBucket", "s3:ListBucketMultipartUploads",
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"
      ],
      "Resource": ["arn:aws:s3:::fenvix-coldread-*", "arn:aws:s3:::fenvix-coldread-*/*"]
    }
  ]
}
```

Con questa policy la giornata funziona: `permessi.sh` la controlla al primo comando "permessi".

### Restringimento facoltativo: solo immagini di Canonical (non obbligatorio)

Oggi l'immagine può essere di chiunque (`image/*` in `MacchinaRisorseDiServizio`). Gli script usano sempre l'immagine
Ubuntu ufficiale letta da SSM, quindi la giornata va bene così. Per chiudere anche la porta alle immagini a pagamento
del Marketplace (revisore M31, R4) servono **due** modifiche insieme, altrimenti non cambia niente (le Allow si
sommano): togliere la riga `"arn:aws:ec2:eu-west-1::image/*",` da `MacchinaRisorseDiServizio` e aggiungere questo
blocco nella lista `Statement`:

```json
    {
      "Sid": "SoloImmaginiDiCanonical",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": "arn:aws:ec2:eu-west-1::image/*",
      "Condition": { "StringEquals": { "ec2:Owner": "099720109477" } }
    },
```

`099720109477` è l'account di Canonical che pubblica le immagini Ubuntu ufficiali (DICHIARATO, documentazione Ubuntu
"Find Ubuntu images on AWS", https://ubuntu.com/aws/docs/aws-how-to/instances/find-ubuntu-images/).

**walbench**: il divieto (`Deny`) di walbench sul ruolo GitHub ora ammette c6i.large e c7i.xlarge (fondatore,
2026-11-04), quindi non blocca la macchina della giornata.

Non servono: security group nuovi (si usa quello di serie della VPC di serie), chiavi SSH, SSM sulla macchina,
CloudWatch.

## Come si lancia (link e passi, uno per volta)

1. I permessi ci sono già (sotto), più la modifica "DA FARE" in cima a questo file. Crea il file di comando dal sito di GitHub:
   https://github.com/Mircoguidetti/fenvix-engine/new/freddo-s3/proto/lavaggio-per-cliente/m31/giornata
   nome `comando.json`, contenuto `{"modo": "permessi"}`, "Commit directly to the freddo-s3 branch".
   Questo costa 0 $ e dice in Actions (https://github.com/Mircoguidetti/fenvix-engine/actions/workflows/m31-freddo-s3.yml)
   "PERMESSI: tutti presenti" oppure l'elenco di quello che manca.
2. Se tutti presenti: modifica lo stesso file in `{"modo": "prova"}` e fai commit. Parte la giornata (circa
   1,5–2 ore). Il log scrive ogni minuto la riga "SPESA". Alla fine i risultati arrivano da soli sul branch in
   `proto/lavaggio-per-cliente/m31/risultati/aws-<numero>/` e la pulizia scrive "RIMASTO: niente".
3. Freno automatico: quando la spesa stimata (per eccesso) arriva a 5 $ il log scrive "BLOCCO: spesa stimata … ≥ 5,00 $",
   la macchina si cancella e parte la pulizia (esito BLOCCO_SPESA). Con i freni di oggi la giornata non dovrebbe
   arrivarci (tetto calcolato circa 1,5 $, STIMATO). L'ultimo passo del job, sempre, è il controllo finale: "RIMASTO:
   niente" oppure "ALLARME: rimasto …" con l'elenco, in rosso in cima alla pagina della run e job fallito.
   **Durante la giornata il freno a mano è il bottone "Cancel workflow"** nella pagina della run in Actions (in alto a
   destra): la run si ferma e la pulizia gira lo stesso (cancella macchina e bucket, scrive "RIMASTO: niente").
   `{"modo": "smonta"}` serve solo **dopo**, se la pulizia ha scritto "ALLARME": durante una giornata in corso
   aspetterebbe in coda la sua fine e non fermerebbe niente.
4. **Non fare due volte il commit di `{"modo": "prova"}`**: il secondo non annulla il primo, parte una seconda
   giornata in coda dopo la prima (circa 0,5 $ in più). Per rifare una giornata, aspetta che la prima sia finita e
   che la pulizia abbia scritto "RIMASTO: niente".
5. Da sola la giornata si ferma anche così: la macchina si spegne (e si cancella) subito se non riesce a scaricare o
   aprire il programma, e comunque dopo 240 minuti; il workflow smette di aspettare se dopo 15 minuti la macchina non
   ha scritto niente ("MACCHINA MUTA"); il programma si ferma se le richieste a S3 passano i tetti di esegui.sh
   in un passo ("FRENO").

Il coordinatore può fare i passi 1–3 al posto del fondatore (sono commit sul branch), ma solo dopo il sì del
fondatore sulla cifra.
