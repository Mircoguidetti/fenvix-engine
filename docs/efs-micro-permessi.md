# Micro-prova EFS (S13): permessi AWS da aggiungere

Il giro 1 (run 41345130300, 2026-11-01, 0 $) si è fermato al primo passo: il ruolo GitHub (secret `AWS_ROLE_ARN`)
risponde "Not authorized to perform sts:AssumeRoleWithWebIdentity" al branch `idea-turno`. Nessuna risorsa creata.

Servono tre modifiche, tutte nella console IAM o con la CLI. `<ACCOUNT_ID>` = l'id dell'account (non è scritto in
questo repo). Tutto ciò che la prova crea si chiama `fenvix-prova-efs-*` e ha il tag `project=fenvix-prova-efs`.

## 1. Fiducia OIDC: aggiungere il branch idea-turno

Nella trust policy del ruolo, alla condizione `token.actions.githubusercontent.com:sub`, aggiungere (lasciando gli
altri valori come sono):

```
repo:Mircoguidetti/fenvix-engine:ref:refs/heads/idea-turno
```

## 2. Nuovo ruolo e profilo di istanza per i nodi: `fenvix-prova-efs-ec2`

I nodi leggono i comandi e scrivono i risultati in un bucket della prova. Niente altro.

Trust policy:

```json
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow", "Principal": { "Service": "ec2.amazonaws.com" }, "Action": "sts:AssumeRole" }] }
```

Policy inline `fenvix-prova-efs-ec2`:

```json
{ "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::fenvix-prova-efs-*", "arn:aws:s3:::fenvix-prova-efs-*/*"] } ] }
```

```bash
aws iam create-role --role-name fenvix-prova-efs-ec2 --assume-role-policy-document file://trust-ec2.json
aws iam put-role-policy --role-name fenvix-prova-efs-ec2 --policy-name fenvix-prova-efs-ec2 --policy-document file://ec2.json
aws iam create-instance-profile --instance-profile-name fenvix-prova-efs-ec2
aws iam add-role-to-instance-profile --instance-profile-name fenvix-prova-efs-ec2 --role-name fenvix-prova-efs-ec2
```

## 3. Policy inline sul ruolo GitHub: `fenvix-prova-efs`

```json
{ "Version": "2012-10-17",
  "Statement": [
    { "Sid": "Leggi", "Effect": "Allow",
      "Action": ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeImages", "ec2:DescribeInstances",
                 "ec2:DescribeSecurityGroups", "ec2:DescribeNetworkInterfaces", "ec2:DescribeInstanceTypeOfferings",
                 "elasticfilesystem:DescribeFileSystems", "elasticfilesystem:DescribeMountTargets",
                 "cloudwatch:GetMetricStatistics", "s3:ListAllMyBuckets", "servicequotas:GetServiceQuota",
                 "pricing:GetProducts"],
      "Resource": "*" },

    { "Sid": "EfsCreaSoloConTag", "Effect": "Allow",
      "Action": ["elasticfilesystem:CreateFileSystem", "elasticfilesystem:TagResource"],
      "Resource": "*",
      "Condition": { "StringEquals": { "aws:RequestTag/project": "fenvix-prova-efs" } } },
    { "Sid": "EfsAgisciSoloSuiNostri", "Effect": "Allow",
      "Action": ["elasticfilesystem:CreateMountTarget", "elasticfilesystem:DeleteMountTarget",
                 "elasticfilesystem:UpdateFileSystem", "elasticfilesystem:DeleteFileSystem",
                 "elasticfilesystem:PutLifecycleConfiguration"],
      "Resource": "arn:aws:elasticfilesystem:eu-west-1:<ACCOUNT_ID>:file-system/*",
      "Condition": { "StringEquals": { "aws:ResourceTag/project": "fenvix-prova-efs" } } },
    { "Sid": "EfsMountTargetInterfacce", "Effect": "Allow",
      "Action": ["ec2:CreateNetworkInterface", "ec2:DeleteNetworkInterface"],
      "Resource": "*" },

    { "Sid": "IstanzeSoloC7gLargeConTag", "Effect": "Allow", "Action": "ec2:RunInstances",
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:volume/*",
                   "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:network-interface/*"],
      "Condition": { "StringEquals": { "aws:RequestTag/project": "fenvix-prova-efs" } } },
    { "Sid": "SoloC7gLarge", "Effect": "Deny", "Action": "ec2:RunInstances",
      "Resource": "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*",
      "Condition": { "StringNotEquals": { "ec2:InstanceType": "c7g.large" } } },
    { "Sid": "IstanzeUsa", "Effect": "Allow", "Action": "ec2:RunInstances",
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:subnet/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*",
                   "arn:aws:ec2:eu-west-1::image/*", "arn:aws:ec2:eu-west-1::snapshot/*"] },
    { "Sid": "SecurityGroupConTag", "Effect": "Allow", "Action": "ec2:CreateSecurityGroup",
      "Resource": "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*",
      "Condition": { "StringEquals": { "aws:RequestTag/project": "fenvix-prova-efs" } } },
    { "Sid": "SecurityGroupNelVpc", "Effect": "Allow", "Action": "ec2:CreateSecurityGroup",
      "Resource": "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:vpc/*" },
    { "Sid": "TagSoloAllaCreazione", "Effect": "Allow", "Action": "ec2:CreateTags", "Resource": "*",
      "Condition": { "StringEquals": { "ec2:CreateAction": ["RunInstances", "CreateSecurityGroup"] } } },
    { "Sid": "AgisciSoloSuiNostri", "Effect": "Allow",
      "Action": ["ec2:TerminateInstances", "ec2:AuthorizeSecurityGroupIngress", "ec2:DeleteSecurityGroup"],
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*"],
      "Condition": { "StringEquals": { "aws:ResourceTag/project": "fenvix-prova-efs" } } },
    { "Sid": "PassaSoloIlRuoloDeiNodi", "Effect": "Allow", "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/fenvix-prova-efs-ec2",
      "Condition": { "StringEquals": { "iam:PassedToService": "ec2.amazonaws.com" } } },

    { "Sid": "BucketDellaProva", "Effect": "Allow",
      "Action": ["s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketTagging", "s3:ListBucket",
                 "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::fenvix-prova-efs-*", "arn:aws:s3:::fenvix-prova-efs-*/*"] }
  ] }
```

Note:
- `ec2:CreateNetworkInterface` e `ec2:DeleteNetworkInterface` servono a EFS per i mount target (pagina API
  CreateMountTarget di AWS: richiede anche `ec2:DescribeSubnets`, `ec2:DescribeNetworkInterfaces`,
  `ec2:CreateNetworkInterface`). Non accettano il filtro per tag del file system; se si vuole stringere, limitarli
  alle subnet di default di eu-west-1.
- Il `Deny` tiene il ruolo sul solo `c7g.large` per quello che lancia con questa policy. Attenzione: se il ruolo ha
  ancora la policy `fenvix-walbench` con il suo `Deny` "tutto tranne c6i.large", quel `Deny` blocca anche i
  c7g.large. Va tolta la policy walbench (la prova walbench è finita) o tolto il suo `Deny`.
- `elasticfilesystem:UpdateFileSystem` serve al passaggio Elastic → Provisioned 64 MiB/s.
- Facoltativo, per un controllo più completo al prossimo giro: `iam:SimulatePrincipalPolicy` sul ruolo stesso.

## Come ripartire

Dopo le tre modifiche, il coordinatore cambia `idea-turno/efs-ci/comando.json` in `{"modo": "permessi"}` (di
nuovo 0 $). Se il giro dice "PERMESSI: tutti quelli provati ci sono", la prova vera parte con `{"modo": "prova"}`:
il workflow rifà il controllo dei permessi e, solo se passa, compila e lancia `idea-turno/efs-ci/prova.py`.
