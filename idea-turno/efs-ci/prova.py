#!/usr/bin/env python3
"""Micro-prova EFS (S13), la prova vera, lanciata dal workflow efs-micro in modo "prova".

Fa i passi della scheda S12 (fenvix-brain/sessions/2026-11-01-s12-micro-efs/SCHEDA.md) con la AWS CLI, senza CDK:
  montaggio (bucket, security group, file system Elastic, 3 mount target, 3 c7g.large in 3 zone)
  passo 0 (3 cicli di presa sul vero EFS) -> fase 1 Elastic (<= 15 min) -> passaggio a Provisioned 64 MiB/s ->
  fase 2 (<= 3 h: KiB per operazione, soglia 30 min da 3 zone, matrice, banchi 9/10/zombie, letture dalla coda) ->
  risultati sul runner -> metriche CloudWatch -> smontaggio (anche se qualcosa va storto: finally + passo di pulizia
  del workflow).
Le istanze non hanno SSH né SSM: un piccolo agente legge i comandi da s3://<bucket>/cmd/<nodo>/ e scrive l'uscita in
s3://<bucket>/out/<nodo>/. Il ruolo delle istanze (profilo fenvix-prova-efs-ec2) può solo leggere e scrivere i bucket
fenvix-prova-efs-*.

Spesa: ogni 60 s una riga "SPESA: $x,xx finora (stima), tetto $15" (log e $OUT/spesa.log); LIVE.md spinto sul branch
idea-turno ogni ~10 min. Sopra 12 $ (stima) smonta tutto e si ferma."""
import json, os, re, subprocess, sys, threading, time, traceback, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live  # noqa: E402

OUT = os.environ.get("OUT", "risultati-efs")
RUN = os.environ.get("GITHUB_RUN_ID", str(int(time.time())))
R = "eu-west-1"
NOME = f"fenvix-prova-efs-{RUN}"
BUCKET = f"fenvix-prova-efs-{RUN}"
TIPO = "c7g.large"
ZONE = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
NODI = ["n1", "n2", "n3"]
PROFILO = "fenvix-prova-efs-ec2"
TETTO, STOP = 15.0, 12.0
TTL_MIN = 300                       # le istanze si spengono (e terminano) da sole dopo 5 ore
PROV_MAX_S = 3 * 3600 - 10 * 60     # fase 2 al massimo 3 ore (10 minuti di margine per lo smontaggio)
F1_MAX_S = 15 * 60
# prezzi (eu-west-1): c7g.large 0,0775 $/h e IPv4 0,005 $/h DICHIARATO (NUMBERS.md); gp3 0,088 $/GB-mese STIMATO;
# EFS Elastic 0,07 $/GB scritto e 0,03 $/GB letto DICHIARATO (re:Post); Provisioned 6,60 $/MiBps-mese STIMATO
# (us-east-1 6,00 + 10%); traffico tra zone 0,01 $/GB per verso, STIMATO per prudenza sul 100% dei byte di dati.
P_IST_H = 0.0775 + 0.005 + 20 * 0.088 / 730
P_EL_GB, P_PROV_MIBS_H, P_AZ_GB = 0.07, 6.60 / 730, 0.02
TAGS = [{"Key": "project", "Value": "fenvix-prova-efs"}, {"Key": "fenvix:prova", "Value": "efs"},
        {"Key": "fenvix:run", "Value": RUN}]

ST = {"istanze": {}, "fs": None, "sg": None, "mt": [], "t0": time.time(), "prov_da": None, "fase": "avvio",
      "spesa": 0.0, "stop": False, "seq": 0, "esiti": {}}
os.makedirs(OUT, exist_ok=True)


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


class Stop(Exception):
    pass


# --------------------------------------------------------------------------------------------- AWS CLI e credenziali
_cred_at = time.time()


def rinnova(forza=False):
    """Nuova sessione di 1 h dal token OIDC del runner (valori mascherati, mai scritti)."""
    global _cred_at
    if not forza and time.time() - _cred_at < 25 * 60:
        return
    if "ACTIONS_ID_TOKEN_REQUEST_URL" not in os.environ:
        return
    for t in range(3):
        try:
            req = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
                                         headers={"Authorization": "bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
            tok = json.load(urllib.request.urlopen(req, timeout=30))["value"]
            print(f"::add-mask::{tok}", flush=True)
            env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_") or k in ("AWS_DEFAULT_REGION", "AWS_PAGER")}
            r = subprocess.run(["aws", "sts", "assume-role-with-web-identity", "--role-arn", os.environ["WB_ROLE"],
                                "--role-session-name", f"efs-{RUN}-{int(time.time())}", "--web-identity-token", tok,
                                "--duration-seconds", "3600", "--output", "json"], env=env, capture_output=True, text=True, timeout=60)
            if r.returncode:
                raise RuntimeError(r.stderr[-200:])
            c = json.loads(r.stdout)["Credentials"]
            for v in (c["AccessKeyId"], c["SecretAccessKey"], c["SessionToken"]):
                print(f"::add-mask::{v}", flush=True)
            os.environ.update(AWS_ACCESS_KEY_ID=c["AccessKeyId"], AWS_SECRET_ACCESS_KEY=c["SecretAccessKey"],
                              AWS_SESSION_TOKEN=c["SessionToken"])
            _cred_at = time.time()
            log("credenziali rinnovate")
            return
        except Exception as e:
            log(f"rinnovo credenziali, tentativo {t + 1} fallito: {type(e).__name__}")
            time.sleep(5)


_aws_lock = threading.Lock()


def aws(*args, ok_err=(), js=True, timeout=120):
    with _aws_lock:
        rinnova()
    cmd = ["aws", *args, "--region", R] + (["--output", "json"] if js else [])
    for t in range(4):
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if r.returncode == 0:
            return json.loads(r.stdout) if js and r.stdout.strip() else r.stdout
        err = r.stderr.strip()
        if any(e in err for e in ok_err):
            return None
        if "ExpiredToken" in err:
            rinnova(forza=True)
        elif not re.search(r"Throttl|RequestLimitExceeded|InternalError|ServiceUnavailable|timed out", err):
            raise RuntimeError(f"aws {args[0]} {args[1]}: {err[-300:]}")
        time.sleep(3 * (t + 1))
    raise RuntimeError(f"aws {args[0]} {args[1]}: troppi tentativi")


def tagspec(tipo, nome):
    t = TAGS + [{"Key": "Name", "Value": nome}]
    return f"ResourceType={tipo},Tags=[" + ",".join(f"{{Key={x['Key']},Value={x['Value']}}}" for x in t) + "]"


# ------------------------------------------------------------------------------------------------------- la spesa
def metrica_somma(nome, da, a):
    if not ST["fs"]:
        return 0.0
    r = aws("cloudwatch", "get-metric-statistics", "--namespace", "AWS/EFS", "--metric-name", nome, "--dimensions",
            f"Name=FileSystemId,Value={ST['fs']}", "--statistics", "Sum", "--period", "60",
            "--start-time", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(da - 60)),
            "--end-time", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(a + 60)))
    return sum(p["Sum"] for p in (r or {}).get("Datapoints", []))


def spesa():
    """Stima: istanze accese x minuti x prezzo; EFS Elastic dai byte fatturati (TotalIOBytes, con i minimi 32/4 KiB,
    tutti al prezzo di scrittura: per eccesso) fino al passaggio; Provisioned 64 MiB/s x ore da allora; traffico tra
    zone stimato sui byte di dati. CloudWatch arriva con qualche minuto di ritardo: per prudenza si aggiunge il
    consumo massimo del minuto corrente."""
    now = time.time()
    ist = sum((min(now, v.get("fine") or now) - v["avvio"]) / 3600 * P_IST_H for v in ST["istanze"].values())
    el = prov = az = 0.0
    if ST["fs"]:
        fine_el = ST["prov_da"] or now
        tot = metrica_somma("TotalIOBytes", ST["t0"], fine_el)
        el = tot / 1e9 * P_EL_GB
        if ST["prov_da"] is None:
            el += 0.05  # ritardo di CloudWatch: un margine (STIMATO) per i minuti non ancora contati
        dati = metrica_somma("DataWriteIOBytes", ST["t0"], now) + metrica_somma("DataReadIOBytes", ST["t0"], now)
        az = dati / 1e9 * P_AZ_GB
        if ST["prov_da"]:
            prov = (min(now, ST.get("prov_fine") or now) - ST["prov_da"]) / 3600 * 64 * P_PROV_MIBS_H
    tot = ist + el + prov + az + 0.05  # +0,05: S3, CloudWatch, chiamate (STIMATO)
    return tot, f"istanze {ist:.2f}, EFS Elastic {el:.2f}, Provisioned {prov:.2f}, tra zone {az:.2f}"


def eur(x):
    return f"${x:.2f}".replace(".", ",")


def riga_spesa():
    try:
        tot, det = spesa()
    except Exception as e:
        log(f"spesa: lettura fallita ({type(e).__name__}); tengo l'ultima stima più 0,10 $")
        tot, det = ST["spesa"] + 0.10, "stima precedente + 0,10 $ (lettura fallita)"
    ST["spesa"] = max(ST["spesa"], tot)
    r = f"SPESA: {eur(ST['spesa'])} finora (stima), tetto $15 ({det}; fase: {ST['fase']})"
    print(r, flush=True)
    open(f"{OUT}/spesa.log", "a").write(r + "\n")
    return r


def push_live(r, allarme=""):
    try:
        live.scrivi_live(r, ST["fase"], "Esiti finora: " + json.dumps(ST["esiti"], ensure_ascii=False), allarme)
        subprocess.run(["python3", "idea-turno/efs-ci/scan.py", live.LIVE], capture_output=True, timeout=30)
        g = lambda *a: subprocess.run(["git", *a], capture_output=True, text=True, timeout=60)
        g("add", "-f", live.LIVE)
        g("commit", "-qm", f"efs-micro run {RUN}: live")
        for _ in range(3):
            if g("pull", "--rebase", "-q", "origin", "idea-turno").returncode == 0 and g("push", "-q", "origin", "HEAD:idea-turno").returncode == 0:
                return
            g("rebase", "--abort")
            time.sleep(3)
    except Exception as e:
        log(f"LIVE.md non spinto ({type(e).__name__}); riprovo al prossimo giro")


def guardiano():
    ultimo_push = 0
    while not ST.get("finito"):
        r = riga_spesa()
        if ST["spesa"] > STOP and not ST["stop"]:
            ST["stop"] = True
            log(f"STOP: la stima supera {STOP} $: smonto tutto")
        if time.time() - ultimo_push > 600 or ST["stop"]:
            push_live(r, "spesa oltre 12 $, smontaggio in corso" if ST["stop"] else "")
            ultimo_push = time.time()
        for _ in range(60):
            if ST.get("finito"):
                return
            time.sleep(1)


def controlla():
    if ST["stop"]:
        raise Stop("spesa stimata oltre 12 $")
    if ST["prov_da"] and time.time() - ST["prov_da"] > PROV_MAX_S:
        raise Stop("fase 2 oltre 3 ore")


# ---------------------------------------------------------------------------------------------------- montaggio
AGENTE = r"""#!/bin/bash
# agente della prova: esegue i comandi che trova in s3://$B/cmd/$N/ e rimette l'uscita in s3://$B/out/$N/
set -a; source /etc/fenvix-prova.env; set +a; mkdir -p /var/fenvix/done; cd /var/fenvix
while true; do
  for k in $(aws s3 ls s3://$B/cmd/$N/ 2>/dev/null | awk '{print $4}'); do
    id=${k%.sh}; [ -e done/$id ] && continue; touch done/$id
    aws s3 cp --quiet s3://$B/cmd/$N/$k ./$k || { rm -f done/$id; continue; }
    ( bash ./$k > $id.out 2>&1; echo $? > $id.rc; aws s3 cp --quiet $id.out s3://$B/out/$N/$id.out; aws s3 cp --quiet $id.rc s3://$B/out/$N/$id.rc ) &
  done
  sleep 3
done
"""

MOUNT = "nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport"


def user_data(nodo, zona):
    dns = f"{ST['fs']}.efs.{R}.amazonaws.com"
    return f"""#!/bin/bash
set -x
shutdown -h +{TTL_MIN} "fenvix-prova-efs: TTL"
printf 'B=%s\\nN=%s\\nFS_ID=%s\\nZONA=%s\\nNODO=%s\\nAWS_DEFAULT_REGION={R}\\n' {BUCKET} {nodo} {ST['fs']} {zona} {nodo} > /etc/fenvix-prova.env
dnf install -y nfs-utils iproute-tc kernel-modules-extra || dnf install -y nfs-utils iproute-tc
mkdir -p /mnt/efs /mnt/efs-ctl /mnt/efs-noac
for t in $(seq 1 30); do mount -t nfs4 -o {MOUNT} {dns}:/ /mnt/efs && break; sleep 10; done
mount -t nfs4 -o {MOUNT},lookupcache=none,noac,nosharecache {dns}:/ /mnt/efs-ctl
mount -t nfs4 -o {MOUNT},noac,nosharecache {dns}:/ /mnt/efs-noac
aws s3 cp s3://{BUCKET}/bin/micro-efs /usr/local/bin/micro-efs && chmod 755 /usr/local/bin/micro-efs
aws s3 cp s3://{BUCKET}/bin/agente.sh /usr/local/bin/agente.sh && chmod 755 /usr/local/bin/agente.sh
mkdir -p /mnt/efs/risultati
(mount | grep -c nfs4; chronyc tracking | grep -E 'System time|Reference'; micro-efs aiuto | head -1) > /tmp/boot.txt 2>&1
aws s3 cp /tmp/boot.txt s3://{BUCKET}/out/{nodo}/boot.txt
nohup /usr/local/bin/agente.sh > /var/log/agente.log 2>&1 &
"""


def monta():
    ST["fase"] = "montaggio"
    aws("s3api", "create-bucket", "--bucket", BUCKET, "--create-bucket-configuration", f"LocationConstraint={R}")
    aws("s3api", "put-bucket-tagging", "--bucket", BUCKET, "--tagging",
        json.dumps({"TagSet": TAGS}), ok_err=("AccessDenied",))
    open(f"{OUT}/agente.sh", "w").write(AGENTE)
    aws("s3", "cp", f"{OUT}/micro-efs-arm64", f"s3://{BUCKET}/bin/micro-efs", js=False)
    aws("s3", "cp", f"{OUT}/agente.sh", f"s3://{BUCKET}/bin/agente.sh", js=False)
    vpc = aws("ec2", "describe-vpcs", "--filters", "Name=isDefault,Values=true")["Vpcs"][0]["VpcId"]
    sub = {}
    for z in ZONE:
        sub[z] = aws("ec2", "describe-subnets", "--filters", "Name=default-for-az,Values=true",
                     f"Name=availability-zone,Values={z}")["Subnets"][0]["SubnetId"]
    sg = aws("ec2", "create-security-group", "--group-name", NOME, "--description", "micro-prova EFS: NFS tra i nodi",
             "--vpc-id", vpc, "--tag-specifications", tagspec("security-group", NOME))["GroupId"]
    ST["sg"] = sg
    aws("ec2", "authorize-security-group-ingress", "--group-id", sg, "--ip-permissions",
        json.dumps([{"IpProtocol": "tcp", "FromPort": 2049, "ToPort": 2049, "UserIdGroupPairs": [{"GroupId": sg}]}]))
    fs = aws("efs", "create-file-system", "--creation-token", NOME, "--performance-mode", "generalPurpose",
             "--throughput-mode", "elastic", "--encrypted", "--no-backup", "--tags",
             *[f"Key={t['Key']},Value={t['Value']}" for t in TAGS + [{"Key": "Name", "Value": NOME}]])["FileSystemId"]
    ST["fs"] = fs
    log(f"file system {fs} creato (Elastic)")
    aspetta(lambda: aws("efs", "describe-file-systems", "--file-system-id", fs)["FileSystems"][0]["LifeCycleState"] == "available", 300, "file system")
    for z in ZONE:
        ST["mt"].append(aws("efs", "create-mount-target", "--file-system-id", fs, "--subnet-id", sub[z],
                            "--security-groups", sg)["MountTargetId"])
    aspetta(lambda: all(m["LifeCycleState"] == "available" for m in aws("efs", "describe-mount-targets", "--file-system-id", fs)["MountTargets"]), 600, "mount target")
    ami = sorted(aws("ec2", "describe-images", "--owners", "amazon", "--filters",
                     "Name=name,Values=al2023-ami-2023.*-kernel-*-arm64", "Name=state,Values=available")["Images"],
                 key=lambda i: i["CreationDate"])[-1]["ImageId"]
    for n, z in zip(NODI, ZONE):
        open(f"{OUT}/ud-{n}.sh", "w").write(user_data(n, z))
        r = aws("ec2", "run-instances", "--image-id", ami, "--instance-type", TIPO, "--subnet-id", sub[z],
                "--security-group-ids", sg, "--iam-instance-profile", f"Name={PROFILO}",
                "--instance-initiated-shutdown-behavior", "terminate", "--associate-public-ip-address",
                "--metadata-options", "HttpTokens=required",
                "--block-device-mappings", json.dumps([{"DeviceName": "/dev/xvda", "Ebs": {"VolumeSize": 20, "VolumeType": "gp3", "DeleteOnTermination": True, "Encrypted": True}}]),
                "--user-data", f"file://{OUT}/ud-{n}.sh",
                "--tag-specifications", tagspec("instance", f"{NOME}-{n}"), tagspec("volume", f"{NOME}-{n}"),
                tagspec("network-interface", f"{NOME}-{n}"))
        ST["istanze"][n] = {"id": r["Instances"][0]["InstanceId"], "avvio": time.time(), "fine": None}
        os.remove(f"{OUT}/ud-{n}.sh")
        log(f"istanza {n} in {z} avviata")
    for n in NODI:
        aspetta(lambda n=n: esiste(f"out/{n}/boot.txt"), 900, f"avvio di {n}")
        txt = s3get(f"out/{n}/boot.txt")
        open(f"{OUT}/boot-{n}.txt", "w").write(txt)
        log(f"{n} pronto: {txt.strip().splitlines()[0] if txt.strip() else '?'} mount nfs4")
        if not txt.strip().startswith("3"):
            raise Stop(f"{n}: i tre mount EFS non ci sono")


def aspetta(cond, timeout, cosa):
    t = time.time()
    while time.time() - t < timeout:
        controlla()
        try:
            if cond():
                return
        except Stop:
            raise
        except Exception as e:
            log(f"attesa {cosa}: {str(e)[:120]}")
        time.sleep(10)
    raise Stop(f"{cosa}: non pronto dopo {timeout} s")


def esiste(chiave):
    r = aws("s3api", "list-objects-v2", "--bucket", BUCKET, "--prefix", chiave)
    return bool(r and r.get("KeyCount"))


def s3get(chiave):
    p = f"{OUT}/.tmp"
    aws("s3", "cp", f"s3://{BUCKET}/{chiave}", p, js=False)
    t = open(p).read()
    os.remove(p)
    return t


# ---------------------------------------------------------------------------------------------- comandi ai nodi
def manda(nodo, script):
    ST["seq"] += 1
    cid = f"c{ST['seq']:03d}"
    p = f"{OUT}/.cmd.sh"
    open(p, "w").write("set -o pipefail; set -a; source /etc/fenvix-prova.env; set +a; cd /mnt/efs\n" + script + "\n")
    aws("s3", "cp", p, f"s3://{BUCKET}/cmd/{nodo}/{cid}.sh", js=False)
    return nodo, cid


def attendi(cmds, timeout):
    """Aspetta la fine dei comandi; restituisce {nodo: (rc, uscita)}. Rispetta lo stop della spesa."""
    t = time.time()
    res = {}
    while len(res) < len(cmds):
        controlla()
        if time.time() - t > timeout:
            raise Stop(f"comandi {cmds} non finiti dopo {timeout} s")
        for nodo, cid in cmds:
            if (nodo, cid) in res:
                continue
            if esiste(f"out/{nodo}/{cid}.rc"):
                rc = int(s3get(f"out/{nodo}/{cid}.rc").strip() or 1)
                out = s3get(f"out/{nodo}/{cid}.out")
                res[(nodo, cid)] = (rc, out)
                open(f"{OUT}/log-{nodo}.txt", "a").write(f"== {cid} rc={rc}\n{out}\n")
        time.sleep(15)
    return {k[0]: v for k, v in res.items()}


def esegui(nodo, script, timeout):
    return attendi([manda(nodo, script)], timeout)[nodo]


def esito(nome, testo):
    m = re.findall(r'"esito":"(\w+)"', testo)
    ST["esiti"][nome] = m[-1] if m else "NON SO"
    log(f"{nome}: {ST['esiti'][nome]}")


CTL = "--dir /mnt/efs --controllo-dir /mnt/efs-ctl"


def fasi_file(n):
    return f"/mnt/efs/risultati/fasi-{n}.jsonl"


# ------------------------------------------------------------------------------------------------------ le fasi
def presa(run, tit, sub, extra_tit="", modo="uccidi", cicli=20, lotti=100):
    a = manda(sub, f"micro-efs presa-subentra {CTL} --run {run} --cicli {cicli} --id {sub}")
    time.sleep(20)
    b = manda(tit, f"micro-efs presa-titolare {CTL} --run {run} --cicli {cicli} --lotti {lotti} --modo {modo} {extra_tit}")
    attendi([a, b], 1500)
    rc, out = esegui("n1", f"micro-efs presa-verifica --dir /mnt/efs --run {run} --cicli {cicli} | tee /mnt/efs/risultati/presa-{run}.json", 600)
    esito(f"presa-{run}", out)


def gara(run, perdita=False):
    inizio = int((time.time() + 150) * 1000)
    pre = post = ""
    if perdita:
        pre = "IF=$(ip -br link | awk '$1!=\"lo\"{print $1; exit}'); modprobe sch_netem 2>/dev/null; tc qdisc add dev $IF root netem loss 1% || { echo 'NETEM NON DISPONIBILE'; exit 3; }\n"
        post = "\nrc=$?; tc qdisc del dev $IF root; exit $rc"
    cmds = [manda(n, f"{pre}micro-efs gara --dir /mnt/efs --run {run} --id {n} --concorrenti 8 --round 1000 --periodo-ms 50 --inizio-ms {inizio}{post}") for n in NODI]
    r = attendi(cmds, 900)
    if any("NETEM NON DISPONIBILE" in o for _, o in r.values()):
        ST["esiti"][f"gara-{run}"] = "NON SO (netem non disponibile sul kernel)"
        return
    rc, out = esegui("n1", f"micro-efs gara-verifica --dir /mnt/efs --run {run} --round 1000 | tee /mnt/efs/risultati/gara-{run}.json", 600)
    esito(f"gara-{run}", out)


def prova():
    monta()
    # ---- passo 0: il programma funziona sul vero EFS?
    ST["fase"] = "passo 0 (3 prese sul vero EFS)"
    presa("p0", "n1", "n2", modo="vivo", cicli=3, lotti=20)
    if ST["esiti"].get("presa-p0") != "PASSA":
        raise Stop("passo 0 non passa: mi fermo prima di misurare")
    # ---- fase 1: Elastic, al massimo 15 minuti, solo n1
    ST["fase"] = "fase 1 Elastic (KiB per operazione)"
    t1 = time.time()
    esegui("n1", f"micro-efs matrice {CTL} --lotti-kib 1,4,32 --files 1 --modi fsync,dsync --controlli si --durata-s 30 --pausa-s 60 "
                 f"--anche-solo-controllo si --controllo-modi open,stat,odirect --intervallo-ms 1 --fase f1 --fasi {fasi_file('n1')} "
                 f"> /mnt/efs/risultati/f1-n1.jsonl", F1_MAX_S)
    log(f"fase 1 finita in {(time.time() - t1) / 60:.1f} min")
    # ---- passaggio a Provisioned 64 MiB/s (non si torna indietro per 24 ore: il file system si cancella alla fine)
    ST["fase"] = "passaggio a Provisioned 64 MiB/s"
    aws("efs", "update-file-system", "--file-system-id", ST["fs"], "--throughput-mode", "provisioned",
        "--provisioned-throughput-in-mibps", "64")
    ST["prov_da"] = time.time()
    aspetta(lambda: (lambda f: f["ThroughputMode"] == "provisioned" and f["LifeCycleState"] == "available")(
        aws("efs", "describe-file-systems", "--file-system-id", ST["fs"])["FileSystems"][0]), 600, "Provisioned")
    # ---- 2.1 KiB per operazione in Provisioned
    ST["fase"] = "fase 2.1 KiB per operazione (Provisioned)"
    esegui("n1", f"micro-efs matrice {CTL} --lotti-kib 1,4,32 --files 1 --modi fsync,dsync --controlli si --durata-s 30 --pausa-s 60 "
                 f"--anche-solo-controllo si --controllo-modi open,stat,odirect --intervallo-ms 1 --fase f2kib --fasi {fasi_file('n1')} "
                 f"> /mnt/efs/risultati/f2kib-n1.jsonl", 20 * 60)
    # ---- 2.2 la soglia: 30 minuti da tre zone insieme
    ST["fase"] = "fase 2.2 soglia (30 min, 3 zone, 4 KiB, 8 file, controllo)"
    r = attendi([manda(n, f"micro-efs scrivi {CTL} --modo fsync --lotto-kib 4 --file 8 --controllo si --durata-s 1800 --fase f2-soglia "
                          f"--fasi {fasi_file(n)} | tee /mnt/efs/risultati/f2-soglia-{n}.json") for n in NODI], 40 * 60)
    for n, (rc, out) in r.items():
        m = re.search(r'"p99_ms":([\d.]+).*"p999_ms":([\d.]+)', out)
        ST["esiti"][f"soglia-p99-{n}"] = f"{m.group(1)} ms (p99,9 {m.group(2)})" if m else "NON SO"
    log(f"soglia: {ST['esiti']}")
    # ---- 2.3 matrice della latenza da tre zone
    ST["fase"] = "fase 2.3 matrice (3 zone)"
    attendi([manda(n, f"micro-efs matrice {CTL} --lotti-kib 4,64 --files 1,8,64 --modi fsync,dsync --controlli no,si,pipeline --durata-s 45 "
                      f"--pausa-s 15 --fase f2m --fasi {fasi_file(n)} > /mnt/efs/risultati/f2m-{n}.jsonl") for n in NODI], 65 * 60)
    attendi([manda(n, f"micro-efs matrice {CTL} --lotti-kib 256 --files 1,8 --modi fsync,dsync --controlli no,si,pipeline --durata-s 45 "
                      f"--pausa-s 15 --fase f2g --fasi {fasi_file(n)} > /mnt/efs/risultati/f2g-{n}.jsonl") for n in NODI], 20 * 60)
    # ---- 2.4 banchi di correttezza
    ST["fase"] = "fase 2.4 banchi (gara, presa, zombie)"
    gara("g1")
    gara("g2", perdita=True)
    presa("b10", "n1", "n2")
    presa("b10p", "n1", "n2", extra_tit="--prealloca-mib 64")
    presa("zf", "n1", "n2", modo="ferma")
    presa("zv", "n1", "n2", modo="vivo")
    presa("b10x", "n2", "n3")
    # ---- 2.5 letture dalla coda da un'altra zona
    ST["fase"] = "fase 2.5 letture dalla coda (n1 scrive, n2 legge)"
    for giro, libro, od in (("c1", "/mnt/efs/coda/c1-0.log", "si"), ("c2", "/mnt/efs-noac/coda/c2-0.log", "no"), ("c3", "/mnt/efs/coda/c3-0.log", "no")):
        controlla()
        a = manda("n1", f"micro-efs scrivi {CTL} --file 1 --lotto-kib 4 --intervallo-ms 2 --controllo si --durata-s 300 --nome-libro {giro} "
                        f"--fase f2{giro} --fasi {fasi_file('n1')}")
        time.sleep(8)
        b = manda("n2", f"micro-efs leggi-coda --libro {libro} --durata-s 290 --letture-s 500 --odirect {od} --fase f2{giro} "
                        f"--fasi {fasi_file('n2')} | tee /mnt/efs/risultati/coda-{giro}.json")
        attendi([a, b], 15 * 60)


def raccogli():
    """Copia i risultati da EFS al bucket (da n1) e dal bucket al runner. Non fallisce mai."""
    ST["fase"] = "raccolta dei risultati"
    try:
        esegui("n1", f"aws s3 sync /mnt/efs/risultati s3://{BUCKET}/risultati/ && aws s3 sync /mnt/efs/gara s3://{BUCKET}/gara/ --exclude '*.log' "
                     f"&& aws s3 sync /mnt/efs/presa s3://{BUCKET}/presa/ --exclude '*.log'; true", 900)
    except Exception as e:
        log(f"raccolta da EFS: {e}")
    try:
        aws("s3", "sync", f"s3://{BUCKET}/", f"{OUT}/prova/", "--exclude", "bin/*", "--exclude", "cmd/*", js=False, timeout=900)
    except Exception as e:
        log(f"copia sul runner: {e}")


def metriche():
    """KiB per operazione e conto al mese dalle metriche CloudWatch (dopo almeno 10 minuti dall'ultima fase)."""
    ST["fase"] = "metriche CloudWatch"
    rinnova(forza=True)
    try:
        fasi = "".join(open(os.path.join(d, f)).read() for d, _, fs in os.walk(f"{OUT}/prova/risultati") for f in fs if f.startswith("fasi-"))
        open(f"{OUT}/fasi.jsonl", "w").write(fasi)
        r = subprocess.run([os.environ.get("MICRO_EFS_X86", "micro-efs"), "metriche", "--fs-id", ST["fs"], "--regione", R,
                            "--fasi", f"{OUT}/fasi.jsonl"], capture_output=True, text=True, timeout=1800)
        open(f"{OUT}/metriche.jsonl", "w").write(r.stdout)
        open(f"{OUT}/metriche.err", "w").write(r.stderr[-5000:])
        for riga in r.stdout.splitlines():
            for k in ("soglia_db_piccolo", "soglia_cella_1000_elastic", "soglia_cella_1000_provisioned"):
                m = re.search(rf'"fase":"([^"]+)".*"{k}":"([^"]+)"', riga)
                if m and m.group(2) != "passa":
                    ST["esiti"][f"{k}-{m.group(1)}"] = m.group(2)
    except Exception as e:
        log(f"metriche: {e}")


def smonta():
    ST["fase"] = "smontaggio"
    ids = [v["id"] for v in ST["istanze"].values()]
    if ids:
        try:
            aws("ec2", "terminate-instances", "--instance-ids", *ids)
        except Exception as e:
            log(f"terminate: {e}")
        for v in ST["istanze"].values():
            v["fine"] = v["fine"] or time.time()
    if ST["fs"]:
        for m in ST["mt"]:
            try:
                aws("efs", "delete-mount-target", "--mount-target-id", m, ok_err=("MountTargetNotFound",))
            except Exception as e:
                log(f"mount target: {e}")
        for _ in range(40):
            try:
                if not aws("efs", "describe-mount-targets", "--file-system-id", ST["fs"])["MountTargets"]:
                    break
            except Exception:
                pass
            time.sleep(10)
        try:
            aws("efs", "delete-file-system", "--file-system-id", ST["fs"], ok_err=("FileSystemNotFound",))
            ST["prov_fine"] = time.time()
            log("file system cancellato")
        except Exception as e:
            log(f"ALLARME file system non cancellato: {e}")
    # security group e bucket: li toglie il passo di pulizia del workflow (dopo che le istanze sono terminate)


def main():
    g = threading.Thread(target=guardiano, daemon=True)
    g.start()
    errore = None
    try:
        prova()
    except Stop as e:
        errore = f"fermata: {e}"
    except Exception as e:
        errore = f"errore: {e}"
        traceback.print_exc()
    if errore:
        log(errore)
        ST["esiti"]["fermata"] = errore
    try:
        if ST["istanze"] and not ST["stop"]:
            raccogli()
    finally:
        fine_misure = time.time()
        if ST["fs"] and not ST["stop"]:
            # le metriche di CloudWatch arrivano in ritardo: aspetto 10 minuti con le istanze già spente
            smonta_ist = [v["id"] for v in ST["istanze"].values()]
            if smonta_ist:
                aws("ec2", "terminate-instances", "--instance-ids", *smonta_ist, ok_err=("InvalidInstanceID",))
                for v in ST["istanze"].values():
                    v["fine"] = time.time()
            ST["fase"] = "attesa delle metriche (istanze spente)"
            while time.time() - fine_misure < 600 and not ST["stop"]:
                time.sleep(15)
            metriche()
        smonta()
        riga_spesa()
        ST["finito"] = True
        json.dump({k: v for k, v in ST.items() if k not in ("finito",)}, open(f"{OUT}/stato.json", "w"), indent=1, default=str)
        open(f"{OUT}/esito.txt", "a").write("\nPROVA: " + json.dumps(ST["esiti"], ensure_ascii=False) + "\n")
        log("esiti: " + json.dumps(ST["esiti"], ensure_ascii=False))


if __name__ == "__main__":
    main()
