PRONTO, ma solo dopo che il fondatore ha incollato la policy qui sotto (senza la policy il modo "metriche" scrive "MANCA ListMetrics" e non scarica niente).

# M31 · scarico delle metriche CloudWatch della giornata (modo "metriche")

Sessione di lavoro, 2026-11-05, consegnata alle 10:10Z circa (11:10 ora italiana). Commit `dd8ffff` sul branch `freddo-s3` di fenvix-engine. Nessuna chiamata AWS vera, nessuna spesa, nessun merge. La run 41999425773 non è stata toccata: il mio commit cambia il workflow e la cartella `giornata/`, ma non `comando.json`, quindi non fa partire niente; la run in corso usa la sua copia del workflow.

## 1. Il comando.json da committare (dopo la policy e dopo la fine della run)

```json
{"modo": "metriche", "run": "41999425773"}
```

Facoltativi, solo se serve forzare: `"istanze": ["i-..."]`, `"volumi": ["vol-..."]`, `"inizio"` e `"fine"` in UTC (`AAAA-MM-GGTHH:MM:SSZ`). Se la run è ancora in corso quando si committa, il nuovo job aspetta in coda la sua fine (stesso gruppo di concorrenza `fenvix-m31`), quindi committarlo presto non disturba la giornata.

Attenzione all'ora: una macchina cancellata resta visibile in DescribeInstances per circa un'ora (DICHIARATO da AWS, documentazione EC2 sulle istanze terminate). Se si committa entro un'ora dalla fine, la finestra si calcola da sola dall'ora di lancio e dall'ora di cancellazione. Dopo, va ancora bene (l'ID della macchina si prende da `giornata.json` dei risultati), ma la finestra diventa "ultime 6 ore fino ad adesso": in quel caso conviene mettere `"inizio"` e `"fine"` nel comando.json.

## 2. La policy completa da incollare

Dove: console IAM, Roles, `fenvix-gha-tests`, Permissions, policy inline `fenvix-coldread-test`, Edit, JSON. È il testo attuale (quello salvato dal fondatore alle 10:01) con un solo blocco nuovo in fondo, `ReadCloudWatchMetrics`, di sola lettura. Niente scrittura su CloudWatch (PutMetricData, allarmi, dashboard): resta tutto rifiutato.

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{"Sid": "ReadOnly", "Effect": "Allow", "Action": ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeInstances", "ec2:DescribeVolumes", "ec2:DescribeAddresses", "s3:ListAllMyBuckets"], "Resource": "*"},
		{"Sid": "PublicUbuntuImage", "Effect": "Allow", "Action": "ssm:GetParameter", "Resource": "arn:aws:ssm:eu-west-1::parameter/aws/service/canonical/*"},
		{"Sid": "LaunchOnlyC7iXlargeWithTag", "Effect": "Allow", "Action": "ec2:RunInstances", "Resource": ["arn:aws:ec2:eu-west-1:865859654353:instance/*", "arn:aws:ec2:eu-west-1:865859654353:volume/*"], "Condition": {"StringEquals": {"aws:RequestTag/project": "fenvix-coldread"}, "StringEqualsIfExists": {"ec2:InstanceType": "c7i.xlarge"}}},
		{"Sid": "LaunchSupportResources", "Effect": "Allow", "Action": "ec2:RunInstances", "Resource": ["arn:aws:ec2:eu-west-1::image/*", "arn:aws:ec2:eu-west-1:865859654353:subnet/*", "arn:aws:ec2:eu-west-1:865859654353:network-interface/*", "arn:aws:ec2:eu-west-1:865859654353:security-group/*"]},
		{"Sid": "TagOnlyAtLaunch", "Effect": "Allow", "Action": "ec2:CreateTags", "Resource": ["arn:aws:ec2:eu-west-1:865859654353:instance/*", "arn:aws:ec2:eu-west-1:865859654353:volume/*"], "Condition": {"StringEquals": {"ec2:CreateAction": "RunInstances"}}},
		{"Sid": "TerminateOnlyColdreadMachines", "Effect": "Allow", "Action": "ec2:TerminateInstances", "Resource": "arn:aws:ec2:eu-west-1:865859654353:instance/*", "Condition": {"StringEquals": {"ec2:ResourceTag/project": "fenvix-coldread"}}},
		{"Sid": "PassOnlyTheMachineRole", "Effect": "Allow", "Action": "iam:PassRole", "Resource": "arn:aws:iam::865859654353:role/fenvix-coldread-machine", "Condition": {"StringEquals": {"iam:PassedToService": "ec2.amazonaws.com"}}},
		{"Sid": "ReadTheMachineProfile", "Effect": "Allow", "Action": "iam:GetInstanceProfile", "Resource": "arn:aws:iam::865859654353:instance-profile/fenvix-coldread-machine"},
		{"Sid": "TestBuckets", "Effect": "Allow", "Action": ["s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketTagging", "s3:PutLifecycleConfiguration", "s3:PutBucketPublicAccessBlock", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"], "Resource": ["arn:aws:s3:::fenvix-coldread-*", "arn:aws:s3:::fenvix-coldread-*/*"]},
		{"Sid": "ReadCloudWatchMetrics", "Effect": "Allow", "Action": ["cloudwatch:GetMetricData", "cloudwatch:ListMetrics"], "Resource": "*"}
	]
}
```

Ho controllato che il testo sia JSON valido e che con questo testo le chiamate del modo metriche (DescribeInstances, DescribeVolumes, ListMetrics, GetMetricData) siano permesse e PutMetricData e DeleteAlarms no (controllo locale, non il simulatore di IAM).

Effetto collaterale da sapere: da questo commit `permessi.sh` controlla anche i due permessi CloudWatch. Quindi, finché la policy non è incollata, anche i modi `permessi` e `prova` si fermano con "MANCA cloudwatch:ListMetrics". È quello che chiede il prompt; lo scrivo perché non sorprenda.

## 3. Come lavora il modo "metriche", un passo alla volta

1. Il primo passo del workflow riconosce `"modo": "metriche"` e legge il numero della run. Un modo sconosciuto continua a diventare "smonta" come prima.
2. Le credenziali sono le stesse di sempre (ruolo OIDC). Per il modo metriche NON girano `permessi.sh` (crea un bucket di prova) e NON gira la pulizia (cancella): il modo fa solo letture. Il controllo finale invece gira lo stesso, perché è di sola lettura: se fosse rimasto qualcosa della giornata, il job diventa rosso con l'ALLARME, anche se le metriche sono state scaricate bene.
3. Trovare la macchina (la scelta più robusta, in quest'ordine): gli `istanze` del comando.json, se ci sono; poi il campo `id` di `risultati/aws-41999425773/giornata.json`, che la giornata scrive e pubblica sul branch da sola (questo non scade); solo se manca, DescribeInstances con il tag `project=fenvix-coldread` sulle macchine lanciate nelle ultime 24 ore.
4. La finestra: dall'ora di lancio della macchina meno 10 minuti fino all'ora di cancellazione più 15 minuti (AWS la scrive nel campo StateTransitionReason, per esempio "User initiated (2026-11-05 09:44:10 GMT)"), mai oltre "adesso". Se la macchina non si vede più: ultime 6 ore fino ad adesso. Tutto viene scritto in `note.txt` e `finestra.json`.
5. I dischi (volumi EBS): dalla descrizione della macchina se c'è; altrimenti dalle metriche AWS/EBS attive nelle ultime 3 ore, tenendo solo i dischi che non esistono più (il nostro si cancella con la macchina) o che hanno il nostro tag. Un disco vivo senza tag è di un'altra macchina e si scarta. Per una macchina cancellata AWS di solito non mostra più i dischi nella sua descrizione (NON SO con certezza: non provato su AWS vero), per questo c'è la seconda strada. Anche senza dischi trovati, le metriche dei dischi viste dalla macchina (EBSReadOps, EBSWriteBytes e simili) sono comunque nel namespace AWS/EC2 e vengono scaricate.
6. Per la macchina e per ogni disco: ListMetrics (tutte le metriche che esistono, nessuna lista scritta a mano), poi GetMetricData con Period 60 e Period 300, statistiche Average, Maximum, Minimum, Sum, SampleCount, a blocchi di al massimo 500 domande per chiamata (limite di GetMetricData).
7. Si salva in `proto/lavaggio-per-cliente/m31/risultati/aws-41999425773/cloudwatch/`: i JSON grezzi di AWS (describe-instances, list-metrics-*, get-metric-data-*-p60-*, -p300-*), `finestra.json`, `note.txt`, e `INDICE.txt` con dimensione in byte e SHA-256 di ogni file. Il numero dell'account diventa `<ACCOUNT_ID>`; se ne restasse uno, non si pubblica niente.
8. Commit e push sul branch `freddo-s3` con `git pull --rebase` prima, tre tentativi, come fa la pubblicazione dei risultati della giornata.

Da sapere sui punti al minuto: il detailed monitoring è acceso dalle 09:56Z, quindi prima di quell'ora i punti con Period 60 saranno uno ogni 5 minuti (DICHIARATO da AWS: monitoring di base a 5 minuti).

## 4. Quanto costa

Non è proprio 0 $. GetMetricData costa 0,01 $ ogni 1.000 metriche chieste e non rientra nel livello gratuito (DICHIARATO, pagina prezzi CloudWatch, https://aws.amazon.com/cloudwatch/pricing). Ogni coppia metrica-statistica-periodo conta come una metrica chiesta. Con circa 20 metriche della macchina e circa 12 del disco (STIMATO), cinque statistiche e due periodi fanno circa 320 metriche chieste: circa 0,003 $ (STIMATO). Il controllo in `permessi.sh` chiede una metrica sola: circa 0,00001 $ (STIMATO). ListMetrics e DescribeInstances: 0 $ (DICHIARATO: ListMetrics nel milione di chiamate gratuite al mese; Describe di EC2 gratis).

## 5. Le prove fatte (tutte senza AWS)

- actionlint 1.7 sul workflow: nessun errore di sintassi; solo i soliti avvisi di stile di shellcheck (virgolette) che c'erano già negli altri passi, più uno uguale a quello della pubblicazione esistente (variabile `i` non usata nel ciclo dei tre tentativi). MISURATO.
- shellcheck (livello warning) su `metriche.sh`, `permessi.sh`, `prova-metriche.sh`: pulito. MISURATO.
- `prova-locale/prova-metriche.sh` con un `aws` finto nel PATH (`prova-locale/bin-metriche/aws`): 8 scenari, 66 controlli, 0 falliti (MISURATO; uscita in `prova-metriche-locale.txt` in questa cartella). Gli scenari: ID da giornata.json; ID solo dal tag; dischi dalla descrizione della macchina; macchina non più visibile con ID nel comando; finestra data nel comando; 120 metriche (600 domande, divise in due chiamate); macchina introvabile (esce con errore); CloudWatch negato (scrive "MANCA ListMetrics", esce con errore, nessun account nei file). In ogni scenario riuscito si controlla: macchina trovata, numero di serie uguale a metriche per 5 statistiche per 2 periodi, disco altrui scartato, nessun numero di account nei file, INDICE.txt con dimensione e SHA-256 giusti per ogni file, solo chiamate di lettura.
- `permessi.sh` con un `aws` finto: le due righe nuove dicono OK, e con ListMetrics negato finisce in "PERMESSI MANCANTI: cloudwatch:ListMetrics". MISURATO.
- `controlla-policy.py` aggiornato (il blocco nuovo è in PERMESSI.md, in cima, come "DA FARE"): 0 controlli falliti, uscita in `controlla-policy.txt`. MISURATO.
- Il passo "modo" del workflow, provato a mano in bash con quattro comando.json (metriche con run, metriche senza run, prova, modo sconosciuto): risultato giusto in tutti e quattro. MISURATO.

## 6. Cosa NON è stato provato

- Nessuna chiamata AWS vera: il formato vero delle risposte di CloudWatch e di DescribeInstances per una macchina cancellata (in particolare se StateTransitionReason porta l'ora quando la macchina si spegne da sola con shutdown, e se i dischi compaiono ancora) è NON SO. Se l'ora non c'è, la finestra finisce "adesso", che va bene lo stesso se si committa poco dopo la fine.
- La paginazione di GetMetricData: la AWS CLI unisce da sola le pagine; con molti punti la stessa serie può comparire in due pezzi nello stesso file. Il dato c'è tutto, ma chi lo legge deve unire le serie con lo stesso Id. Con circa 2 ore a 1 minuto (circa 120 punti per serie, STIMATO) non dovrebbe succedere.
- Il passo del workflow intero (git pull, commit, push) su GitHub Actions vero: provato solo il pezzo che sceglie il modo.
- La policy nel simulatore di IAM: provata solo con il controllo locale.

## 7. Cose che ho deciso io

- Niente `permessi.sh` e niente pulizia nel modo metriche, perché il prompt chiede solo letture e la pulizia cancella. Se il coordinatore vuole anche la pulizia di sicurezza, c'è già il modo "smonta".
- Se il numero di run nel comando.json manca o non è valido, il modo resta "metriche" e fallisce con un messaggio, invece di diventare "smonta" (che cancellerebbe).
- Ho aggiunto il blocco DA FARE in PERMESSI.md e il controllo in `controlla-policy.py` perché le prove locali esistenti leggono la policy da lì.

## 8. Errori o mancanze nel brain

- `PERMESSI.md` sul branch descrive la policy con i vecchi nomi dei blocchi (per esempio "Letture"), mentre la policy vera salvata dal fondatore usa nomi inglesi ("ReadOnly", "TestBuckets"...). Non l'ho riscritto (non è il mio compito): il JSON da incollare è quello del punto 2, costruito dal testo vero.

## Link

- fenvix-engine, branch `freddo-s3`, commit `dd8ffff`: `.github/workflows/m31-freddo-s3.yml`, `proto/lavaggio-per-cliente/m31/giornata/metriche.sh`, `permessi.sh`, `PERMESSI.md`, `prova-locale/prova-metriche.sh`, `prova-locale/bin-metriche/aws`, `prova-locale/controlla-policy.py`.
