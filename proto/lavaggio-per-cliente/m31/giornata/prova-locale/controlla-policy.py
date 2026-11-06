#!/usr/bin/env python3
"""Prova locale M31 (nomi coldread) · le policy di PERMESSI.md contro ogni chiamata AWS degli script, 0 $.

Legge da PERMESSI.md i blocchi JSON delle policy (macchina `coldread-buckets-only`, GitHub `fenvix-coldread-test`,
regola facoltativa `SoloImmaginiDiCanonical`) e valuta, con le regole di IAM per le Allow (azione, risorsa con *,
StringEquals / StringEqualsIfExists / StringLike; una chiave mancante non soddisfa StringEquals), ogni richiesta che
fanno permessi.sh, giornata-ci.sh, recupera.sh, pulizia.sh (ruolo GitHub) ed esegui.sh (ruolo della macchina).
Non sostituisce IAM vero: lo approssima per le sole regole usate in queste policy. Nessun contatto con AWS.
Uso: python3 controlla-policy.py [PERMESSI.md]
"""
import fnmatch, json, re, sys, pathlib

ACCT = "123456789012"
P = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent.parent / "PERMESSI.md")
testo = P.read_text().replace("<ACCOUNT_ID>", ACCT)
blocchi = [json.loads(b.strip().rstrip(",")) for b in re.findall(r"```json\n(.*?)```", testo, re.S)]
# i blocchi si riconoscono dal contenuto, non dall'ordine: policy intere (macchina: 1 regola; GitHub: tante) e pezzi
macchina = next(b for b in blocchi if "Statement" in b and len(b["Statement"]) == 1)
creata = next(b for b in blocchi if "Statement" in b and len(b["Statement"]) > 1)
canonical = next(b for b in blocchi if b.get("Sid") == "SoloImmaginiDiCanonical")
letture_nuove = next(b for b in blocchi if b.get("Sid") == "Letture")  # il blocco "DA FARE" in cima (M31 tetto 5 $)
# la policy dopo la modifica "DA FARE": il blocco Letture sostituito da quello nuovo
github = json.loads(json.dumps(creata))
github["Statement"] = [letture_nuove if s.get("Sid") == "Letture" else s for s in github["Statement"]]
assert sum(s.get("Sid") == "Letture" for s in github["Statement"]) == 1
# il blocco "DA FARE" del modo metriche (M31 metriche CloudWatch, 2026-11-05): si aggiunge in fondo
cloudwatch = next(b for b in blocchi if b.get("Sid") == "ReadCloudWatchMetrics")
github["Statement"].append(cloudwatch)

# la policy GitHub col restringimento: senza image/* nei servizi, più la regola di Canonical
ristretta = json.loads(json.dumps(github))
for s in ristretta["Statement"]:
    if s.get("Sid") == "MacchinaRisorseDiServizio":
        s["Resource"] = [r for r in s["Resource"] if ":image/" not in r]
ristretta["Statement"].append(canonical)


def lista(x):
    return x if isinstance(x, list) else [x]


def cond_ok(cond, ctx):
    for op, coppie in (cond or {}).items():
        for k, v in coppie.items():
            vals = lista(v)
            if k not in ctx:
                if op.endswith("IfExists"):
                    continue
                return False
            got = ctx[k]
            if op.startswith("StringEquals") and got not in vals:
                return False
            if op.startswith("StringLike") and not any(fnmatch.fnmatchcase(got, p) for p in vals):
                return False
            if not (op.startswith("StringEquals") or op.startswith("StringLike")):
                raise ValueError("operatore non gestito: " + op)
    return True


def permesso(pol, azione, risorsa, ctx=None):
    ctx = ctx or {}
    for s in pol["Statement"]:
        if s["Effect"] != "Allow":
            continue
        if not any(fnmatch.fnmatchcase(azione, a) for a in lista(s["Action"])):
            continue
        if not any(fnmatch.fnmatchcase(risorsa, r) for r in lista(s["Resource"])):
            continue
        if cond_ok(s.get("Condition"), ctx):
            return True
    return False


E = f"arn:aws:ec2:eu-west-1:{ACCT}"
B = "fenvix-coldread-12345"           # bucket della giornata (fenvix-coldread-<run>)
BP = "fenvix-coldread-permessi-12345"  # bucket vuoto di permessi.sh
TAG = {"aws:RequestTag/project": "fenvix-coldread"}
CANON = "099720109477"


def run_instances(tipo="c7i.xlarge", tag=TAG, owner=CANON):
    """Le risorse che EC2 autorizza per una RunInstances degli script (immagine, rete, macchina, disco)."""
    return [
        ("ec2:RunInstances", f"{E}:instance/*", {**tag, "ec2:InstanceType": tipo}),
        ("ec2:RunInstances", f"{E}:volume/*", dict(tag)),
        ("ec2:RunInstances", "arn:aws:ec2:eu-west-1::image/ami-0123456789abcdef0", {"ec2:Owner": owner}),
        ("ec2:RunInstances", f"{E}:subnet/subnet-0123", {}),
        ("ec2:RunInstances", f"{E}:network-interface/*", {}),
        ("ec2:RunInstances", f"{E}:security-group/sg-0123", {}),
        ("ec2:CreateTags", f"{E}:instance/*", {"ec2:CreateAction": "RunInstances"}),
        ("ec2:CreateTags", f"{E}:volume/*", {"ec2:CreateAction": "RunInstances"}),
        ("iam:PassRole", f"arn:aws:iam::{ACCT}:role/fenvix-coldread-machine", {"iam:PassedToService": "ec2.amazonaws.com"}),
    ]


def bucket(b):
    a = f"arn:aws:s3:::{b}"
    return [
        ("s3:CreateBucket", a, {}), ("s3:PutBucketPublicAccessBlock", a, {}), ("s3:PutBucketTagging", a, {}),
        ("s3:PutLifecycleConfiguration", a, {}), ("s3:PutObject", a + "/programma/coldread-giornata.tar.gz", {}),
        ("s3:GetObject", a + "/risultati/passi.log", {}), ("s3:ListBucket", a, {}),
        ("s3:DeleteObject", a + "/risultati/passi.log", {}), ("s3:DeleteBucket", a, {}),
    ]


richieste_github = (
    [("ssm:GetParameter", "arn:aws:ssm:eu-west-1::parameter/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id", {}),
     ("ec2:DescribeVpcs", "*", {}), ("ec2:DescribeSubnets", "*", {}), ("ec2:DescribeInstances", "*", {}),
     ("ec2:DescribeVolumes", "*", {}), ("ec2:DescribeAddresses", "*", {}),  # controllo finale (M31 tetto 5 $)
     ("iam:GetInstanceProfile", f"arn:aws:iam::{ACCT}:instance-profile/fenvix-coldread-machine", {}),
     ("s3:ListAllMyBuckets", "*", {}),
     ("cloudwatch:ListMetrics", "*", {}), ("cloudwatch:GetMetricData", "*", {}),  # modo metriche e permessi.sh
     ("ec2:TerminateInstances", f"{E}:instance/i-0123", {"ec2:ResourceTag/project": "fenvix-coldread"})]
    + run_instances() + bucket(BP) + bucket(B)
)
# cose che DEVONO essere rifiutate (i freni della policy, e i nomi vecchi)
rifiuti_github = [
    ("macchina più grande (c7i.2xlarge)", run_instances(tipo="c7i.2xlarge")[0]),
    ("macchina senza etichetta project", run_instances(tag={})[0]),
    ("macchina con l'etichetta vecchia progetto=fenvix-m31", run_instances(tag={"aws:RequestTag/progetto": "fenvix-m31"})[0]),
    ("cancellare una macchina senza etichetta", ("ec2:TerminateInstances", f"{E}:instance/i-0999", {})),
    ("cancellare una macchina con l'etichetta vecchia", ("ec2:TerminateInstances", f"{E}:instance/i-0999", {"ec2:ResourceTag/progetto": "fenvix-m31"})),
    ("passare il ruolo vecchio fenvix-m31-macchina", ("iam:PassRole", f"arn:aws:iam::{ACCT}:role/fenvix-m31-macchina", {"iam:PassedToService": "ec2.amazonaws.com"})),
    ("leggere il profilo vecchio", ("iam:GetInstanceProfile", f"arn:aws:iam::{ACCT}:instance-profile/fenvix-m31-macchina", {})),
    ("bucket col prefisso vecchio fenvix-m31-", ("s3:CreateBucket", "arn:aws:s3:::fenvix-m31-12345", {})),
    ("bucket di un altro progetto", ("s3:GetObject", "arn:aws:s3:::fenvix-prod/x", {})),
    ("scrivere metriche CloudWatch", ("cloudwatch:PutMetricData", "*", {})),
    ("cancellare allarmi CloudWatch", ("cloudwatch:DeleteAlarms", "*", {})),
]
richieste_macchina = [
    ("s3:GetObject", f"arn:aws:s3:::{B}/p1/x.sst", {}), ("s3:PutObject", f"arn:aws:s3:::{B}/risultati/fine.json", {}),
    ("s3:ListBucket", f"arn:aws:s3:::{B}", {}), ("s3:DeleteObject", f"arn:aws:s3:::{B}/scrivi-l1-f5/x", {}),
    ("s3:AbortMultipartUpload", f"arn:aws:s3:::{B}/p1/grande", {}),
]
rifiuti_macchina = [
    ("bucket col prefisso vecchio", ("s3:GetObject", "arn:aws:s3:::fenvix-m31-12345/x", {})),
    ("creare bucket", ("s3:CreateBucket", f"arn:aws:s3:::{B}", {})),
    ("macchine", ("ec2:RunInstances", f"{E}:instance/*", {})),
]

falliti = 0


def conta(ok, riga):
    global falliti
    print(("OK      " if ok else "FALLITO ") + riga)
    falliti += 0 if ok else 1


print("== fenvix-coldread-test com'è oggi (prima della modifica DA FARE): le due letture nuove mancano, tutto il resto c'è")
for a, r, c in richieste_github:
    nuova = a in ("ec2:DescribeVolumes", "ec2:DescribeAddresses", "cloudwatch:ListMetrics", "cloudwatch:GetMetricData")
    conta(permesso(creata, a, r, c) != nuova, ("manca      " if nuova else "permesso   ") + f"{a} {r}")
conta(not any(s.get("Effect") == "Allow" and any(x for x in lista(s["Action"]) if not x.startswith(("ec2:Describe", "s3:ListAllMyBuckets")))
              for s in [letture_nuove]) and letture_nuove["Resource"] == "*", "il blocco nuovo ha solo letture (Describe e ListAllMyBuckets) su \"*\"")
for nome, pol in (("fenvix-coldread-test dopo la modifica DA FARE", github), ("fenvix-coldread-test dopo la modifica + SoloImmaginiDiCanonical", ristretta)):
    print(f"== {nome}")
    for a, r, c in richieste_github:
        conta(permesso(pol, a, r, c), f"permesso   {a} {r}")
    for desc, (a, r, c) in rifiuti_github:
        conta(not permesso(pol, a, r, c), f"rifiutato  {desc}")
    altra = run_instances(owner="679593333241")[2]  # un'immagine del Marketplace (account di esempio, non Canonical)
    atteso = pol is github  # la policy senza restringimento la permette; il restringimento la rifiuta
    conta(permesso(pol, *altra) == atteso, ("permessa" if atteso else "rifiutata") + "  immagine non di Canonical")
print("== coldread-buckets-only (macchina)")
for a, r, c in richieste_macchina:
    conta(permesso(macchina, a, r, c), f"permesso   {a} {r}")
for desc, (a, r, c) in rifiuti_macchina:
    conta(not permesso(macchina, a, r, c), f"rifiutato  {desc}")
print(f"POLICY: {falliti} controlli falliti")
sys.exit(1 if falliti else 0)
