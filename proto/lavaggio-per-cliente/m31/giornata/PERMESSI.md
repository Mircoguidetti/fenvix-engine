# M31 · cosa aggiungere in AWS prima della giornata (il fondatore, dalla console IAM)

Sostituisci `<ACCOUNT_ID>` con il numero dell'account (12 cifre). Regione: eu-west-1. Tre passi.

## 1. Serratura del ruolo GitHub (trust policy)

Ruolo: quello il cui ARN sta nel secret `AWS_ROLE_ARN` di `fenvix-engine`. IAM → Ruoli → quel ruolo → scheda
"Relazioni di attendibilità" → Modifica. Nella condizione su `token.actions.githubusercontent.com:sub` aggiungi
alla lista questo valore (lascia quelli che ci sono):

```
repo:Mircoguidetti/fenvix-engine:ref:refs/heads/freddo-s3
```

Se oggi la condizione è un solo valore con `StringEquals`, diventa una lista, per esempio:

```json
"Condition": {
  "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
  "StringLike": { "token.actions.githubusercontent.com:sub": [
    "repo:Mircoguidetti/fenvix-engine:ref:refs/heads/round3",
    "repo:Mircoguidetti/fenvix-engine:ref:refs/heads/freddo-s3"
  ] }
}
```

## 2. Ruolo nuovo per la macchina: `fenvix-m31-macchina`

IAM → Ruoli → Crea ruolo → "Servizio AWS" → "EC2" → Avanti → nessuna policy → nome `fenvix-m31-macchina` → Crea
(la console crea anche il profilo dell'istanza con lo stesso nome). Poi nel ruolo: Aggiungi autorizzazioni →
Crea policy inline → JSON → incolla → nome `solo-bucket-m31`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::fenvix-m31-*", "arn:aws:s3:::fenvix-m31-*/*"]
    }
  ]
}
```

La macchina legge e scrive solo nei bucket `fenvix-m31-*`. Nient'altro.

## 3. Policy inline `fenvix-m31` sul ruolo GitHub

Nel ruolo GitHub: Aggiungi autorizzazioni → Crea policy inline → JSON → incolla → nome `fenvix-m31`:

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
        "StringEquals": { "aws:RequestTag/progetto": "fenvix-m31" },
        "StringEqualsIfExists": { "ec2:InstanceType": "c7i.xlarge" }
      }
    },
    {
      "Sid": "SoloImmaginiDiCanonical",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": "arn:aws:ec2:eu-west-1::image/*",
      "Condition": { "StringEquals": { "ec2:Owner": "099720109477" } }
    },
    {
      "Sid": "MacchinaRisorseDiServizio",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": [
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
      "Condition": { "StringEquals": { "ec2:ResourceTag/progetto": "fenvix-m31" } }
    },
    {
      "Sid": "PassareSoloIlRuoloDellaMacchina",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/fenvix-m31-macchina",
      "Condition": { "StringEquals": { "iam:PassedToService": "ec2.amazonaws.com" } }
    },
    {
      "Sid": "LeggereIlProfiloDellaMacchina",
      "Effect": "Allow",
      "Action": "iam:GetInstanceProfile",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:instance-profile/fenvix-m31-macchina"
    },
    {
      "Sid": "BucketDellaProva",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketTagging", "s3:PutLifecycleConfiguration",
        "s3:PutBucketPublicAccessBlock", "s3:ListBucket", "s3:ListBucketMultipartUploads",
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"
      ],
      "Resource": ["arn:aws:s3:::fenvix-m31-*", "arn:aws:s3:::fenvix-m31-*/*"]
    }
  ]
}
```

`099720109477` non è un numero nostro: è l'account di Canonical che pubblica le immagini Ubuntu ufficiali (DICHIARATO,
documentazione Ubuntu "Find Ubuntu images on AWS", https://ubuntu.com/aws/docs/aws-how-to/instances/find-ubuntu-images/).
Con questa condizione la macchina può partire solo da un'immagine di Canonical, mai da una del Marketplace a pagamento
(revisore M31, R4).

**Attenzione** (da S13): se sul ruolo GitHub c'è ancora la policy di walbench con un `Deny` su "tutto tranne
c6i.large", quel divieto blocca anche la c7i.xlarge (un Deny vince sempre). Va tolta quella policy (walbench ha
finito) o almeno il suo `Deny`.

Non servono: security group nuovi (si usa quello di serie della VPC di serie), chiavi SSH, SSM sulla macchina,
CloudWatch.

## Come si lancia (link e passi, uno per volta)

1. Dopo i tre passi sopra, crea il file di comando dal sito di GitHub:
   https://github.com/Mircoguidetti/fenvix-engine/new/freddo-s3/proto/lavaggio-per-cliente/m31/giornata
   nome `comando.json`, contenuto `{"modo": "permessi"}`, "Commit directly to the freddo-s3 branch".
   Questo costa 0 $ e dice in Actions (https://github.com/Mircoguidetti/fenvix-engine/actions/workflows/m31-freddo-s3.yml)
   "PERMESSI: tutti presenti" oppure l'elenco di quello che manca.
2. Se tutti presenti: modifica lo stesso file in `{"modo": "prova"}` e fai commit. Parte la giornata (circa
   1,5–2 ore). Il log scrive ogni minuto la riga "SPESA". Alla fine i risultati arrivano da soli sul branch in
   `proto/lavaggio-per-cliente/m31/risultati/aws-<numero>/` e la pulizia scrive "RIMASTO: niente".
3. **Durante la giornata il freno è il bottone "Cancel workflow"** nella pagina della run in Actions (in alto a
   destra): la run si ferma e la pulizia gira lo stesso (cancella macchina e bucket, scrive "RIMASTO: niente").
   `{"modo": "smonta"}` serve solo **dopo**, se la pulizia ha scritto "ALLARME": durante una giornata in corso
   aspetterebbe in coda la sua fine e non fermerebbe niente.
4. **Non fare due volte il commit di `{"modo": "prova"}`**: il secondo non annulla il primo, parte una seconda
   giornata in coda dopo la prima (circa 0,5 $ in più). Per rifare una giornata, aspetta che la prima sia finita e
   che la pulizia abbia scritto "RIMASTO: niente".
5. Da sola la giornata si ferma anche così: la macchina si spegne (e si cancella) subito se non riesce a scaricare o
   aprire il programma, e comunque dopo 240 minuti; il workflow smette di aspettare se dopo 15 minuti la macchina non
   ha scritto niente ("MACCHINA MUTA"); il programma si ferma se le richieste a S3 passano 500.000 GET o 100.000 PUT
   in un passo ("FRENO").

Il coordinatore può fare i passi 1–3 al posto del fondatore (sono commit sul branch), ma solo dopo il sì del
fondatore sulla cifra.
