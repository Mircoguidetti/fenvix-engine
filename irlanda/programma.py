"""P1 Irlanda v3 (analisi del 2026-11-05) · il programma di un giro, SULLA MACCHINA in eu-west-1. Lo script del fondatore
(prova_infinite.py p1-1.6.2) NON si tocca: qui si decide solo quale comando dargli, su quale file di stato, in che
ordine, e si misura la macchina che manda il carico.

Il programma e' un file di testo (programmi/NOME/passi.txt), un passo per riga ('#' = commento):
  carica    STATO OPZIONI...   lo script 'carica' su un database nuovo, con il SUO file di stato nuovo
  giro      STATO OPZIONI...   lo script 'giro' sul file di stato STATO (prima si sposta il giro di prima, come
                               prepara_stato.py, sotto 'giro_finale_prima_passo_K': se no il parallelo salta i livelli)
  dormienti STATO OPZIONI...   lo script 'giro' su una COPIA del file di stato in cui i clienti dormienti mai letti
                               figurano come "attivi" (e gli attivi come gia' letti): lo script mette i clienti
                               dormienti solo nelle ricerche Q1..Q4 una alla volta, mai nel parallelo, nelle condizioni
                               e mentre si scrive. Solo Q1 e Q2 (sono le uniche ricerche che non dipendono dall'eta'
                               del cliente: Q1 = ultimi 20, Q2 = un _id). Dopo, i clienti letti tornano nel file vero.
  massimo   STATO COPIE OPZIONI...  COPIE copie dello script insieme, ognuna con una COPIA del file di stato
  aspetta                      aspetta i passi partiti in sottofondo
Un passo che comincia con '&' parte in sottofondo; i passi in sottofondo sullo stesso STATO vanno in fila tra loro,
quelli su STATI diversi vanno insieme (serve ai caricamenti: un processo Python carica circa un core).
Ogni passo riceve da qui --file, --cartella-dati (e per massimo --giro); --db va scritto nel passo.

Misura la macchina: ogni 5 s la CPU di ogni processo dello script (da /proc/PID/stat, % di un core) in
out/cpu-processi.log; la CPU di tutta la macchina la scrive cpu_macchina.py (out/cpu-macchina.log). Alla fine
out/validita-macchina.txt: per ogni fase di ogni passo il p95 e il massimo della CPU del processo e il massimo della
macchina; VALIDA se il p95 del processo e' sotto l'80% di un core e la macchina sotto l'80% (RULES.md, 2026-11-05).
Uso: programma.py PASSI.txt   (dentro la cartella del pacco; le variabili FENVIX_P1_URI e URI_GIRO le mette esegui.sh)
Codice di uscita: 0 = tutti i passi finiti e tutte le fasi chieste 'fatta'."""
import datetime as dt
import glob
import json
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time

PY = sys.executable
QUI = os.path.dirname(os.path.abspath(__file__))
LAV = os.path.join(QUI, "lavoro")
OUT = os.path.join(QUI, "out")
SOGLIA = 80.0
SCRIPT = os.path.join(QUI, "prova_infinite.py")
DORM_QUERY_OK = {"Q1", "Q2"}
DORM_FASI_OK = {"condizioni", "mentre_scrivi", "parallelo"}
NON_MISURE = {"grandi_carica", "grandi_lavaggio", "inizio", "fine"}  # niente tempi da confrontare

_lock = threading.Lock()
_vivi = {}  # pid -> etichetta (per la CPU)


def ora():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def passo_log(msg):
    riga = f"{ora()[:19]}Z {msg}"
    print(riga, flush=True)
    with _lock:
        with open(os.path.join(OUT, "passi.log"), "a") as f:
            f.write(riga + "\n")


def leggi_passi(path):
    passi = []
    for k, riga in enumerate(open(path), 1):
        riga = riga.split("#", 1)[0].strip()
        if not riga:
            continue
        sotto = riga.startswith("&")
        p = shlex.split(riga.lstrip("&").strip())
        passi.append({"riga": k, "sotto": sotto, "tipo": p[0], "arg": p[1:]})
    return passi


def opzione(arg, nome):
    for i, x in enumerate(arg):
        if x == nome and i + 1 < len(arg):
            return arg[i + 1]
        if x.startswith(nome + "="):
            return x.split("=", 1)[1]
    return None


def controlla_passi(passi):
    """Errori di forma del programma (prima di spendere: lo usa anche simula_programma.py)."""
    err = []
    for p in passi:
        t, a = p["tipo"], p["arg"]
        if t == "aspetta":
            continue
        if t not in ("carica", "giro", "dormienti", "massimo"):
            err.append(f"riga {p['riga']}: passo sconosciuto '{t}'")
            continue
        if not a:
            err.append(f"riga {p['riga']}: manca lo STATO")
            continue
        resto = a[2:] if t == "massimo" else a[1:]
        for vietata in ("--file", "--cartella-dati", "--forza", "--anche-se-pochi", "--da-fase"):
            if vietata in resto:
                err.append(f"riga {p['riga']}: {vietata} non si scrive nel programma")
        if not opzione(resto, "--db"):
            err.append(f"riga {p['riga']}: manca --db (ogni passo dice su quale database lavora)")
        if t in ("giro", "dormienti") and "--nuova-cartella" not in resto:
            err.append(f"riga {p['riga']}: 'giro' senza --nuova-cartella")
        if t == "dormienti":
            fasi = set((opzione(resto, "--solo-fasi") or "").split(","))
            if not fasi or not fasi <= DORM_FASI_OK:
                err.append(f"riga {p['riga']}: dormienti solo con --solo-fasi tra {sorted(DORM_FASI_OK)}")
            if "parallelo" in fasi and not set((opzione(resto, "--par-query") or "Q2,Q3,Q1").split(",")) <= DORM_QUERY_OK:
                err.append(f"riga {p['riga']}: dormienti nel parallelo solo con --par-query Q2,Q1 (Q3 dipende dall'eta')")
        if t == "massimo":
            if len(a) < 2 or not a[1].isdigit():
                err.append(f"riga {p['riga']}: massimo STATO COPIE ...")
            if opzione(resto, "--solo-fasi") != "carico_massimo":
                err.append(f"riga {p['riga']}: massimo solo con --solo-fasi carico_massimo")
    return err


def file_stato(stato):
    return os.path.join(LAV, "stati", stato, "risultati_p1.json")


def prepara(stato, k):
    """Come prepara_stato.py: il giro di prima sotto un'altra chiave, gli 'usati' non si toccano."""
    f = file_stato(stato)
    d = json.load(open(f))
    gf = d.pop("giro_finale", None)
    if gf is None:
        return "nessun giro di prima"
    chiave = f"giro_finale_prima_passo_{k}"
    while chiave in d:
        chiave += "_"
    d[chiave] = gf
    with open(f + ".tmp", "w") as o:
        json.dump(d, o, indent=1)
    os.replace(f + ".tmp", f)
    return f"giro di prima ('{gf.get('nome')}') spostato sotto '{chiave}'"


def lancia(cmd, cwd, terminale, etichetta, env):
    with open(terminale, "a") as t:
        t.write(f"===== {ora()} {' '.join(shlex.quote(x) for x in cmd[1:])}\n")
        t.flush()
        pr = subprocess.Popen(cmd, cwd=cwd, stdout=t, stderr=subprocess.STDOUT, env=env)
    with _lock:
        _vivi[pr.pid] = etichetta
    return pr


def aspetta_proc(pr):
    rc = pr.wait()
    with _lock:
        _vivi.pop(pr.pid, None)
    return rc


def env_per(uri_giro):
    e = dict(os.environ)
    if uri_giro and os.environ.get("URI_GIRO"):
        e["FENVIX_P1_URI"] = os.environ["URI_GIRO"]
    e.pop("URI_GIRO", None)
    return e


def esegui_script(k, stato, args, uri_giro, etichetta, cartella_stato=None):
    cs = cartella_stato or os.path.join(LAV, "stati", stato)
    os.makedirs(cs, exist_ok=True)
    dati = os.path.join(LAV, "dati", stato)
    os.makedirs(dati, exist_ok=True)
    term = os.path.join(OUT, "terminali", f"passo_{k:02d}_{etichetta}.txt")
    os.makedirs(os.path.dirname(term), exist_ok=True)
    shutil.copy(SCRIPT, os.path.join(cs, "prova_infinite.py"))
    cmd = [PY, "prova_infinite.py"] + args[:1] + ["--file", "risultati_p1.json", "--cartella-dati", dati] + args[1:]
    t0 = ora()
    rc = aspetta_proc(lancia(cmd, cs, term, f"passo_{k:02d}_{etichetta}", env_per(uri_giro)))
    return rc, t0, ora(), term


def passo_dormienti(k, stato, opz):
    vero = file_stato(stato)
    d = json.load(open(vero))
    cl = d["clienti"]
    ris = set(cl["riservati"])
    copia = os.path.join(LAV, "dormienti", f"passo_{k:02d}_{stato}")
    os.makedirs(copia, exist_ok=True)
    usati0 = set(d.get("usati", []))
    # anche quelli del registro (lo script li rilegge all'avvio)
    log_vero = vero + ".usati.log"
    righe = open(log_vero).read().split("\n") if os.path.exists(log_vero) else []
    for r in righe:
        p = r.split()
        if len(p) >= 2 and p[0] == "a" and p[1].lstrip("-").isdigit():
            usati0.add(int(p[1]))
    attivi = {i for i in range(cl["n"]) if i not in ris}
    dc = json.loads(json.dumps(d))
    dc["clienti"]["riservati"] = []
    dc["clienti"]["riservati_veri_nascosti_dal_programma"] = sorted(ris)
    dc["usati"] = sorted(usati0 | attivi)
    dc.pop("giro_finale", None)
    iniziali = set(dc["usati"])
    with open(os.path.join(copia, "risultati_p1.json"), "w") as o:
        json.dump(dc, o, indent=1)
    shutil.copy(log_vero, os.path.join(copia, "risultati_p1.json.usati.log")) if os.path.exists(log_vero) else \
        open(os.path.join(copia, "risultati_p1.json.usati.log"), "w").close()
    rc, t0, t1, term = esegui_script(k, stato, ["giro"] + opz, True, f"{stato}_dormienti", cartella_stato=copia)
    # i clienti letti dalla copia (file di stato + registro della copia) tornano nel file vero
    dfin = json.load(open(os.path.join(copia, "risultati_p1.json")))
    letti = set(dfin.get("usati", [])) - iniziali
    for r in open(os.path.join(copia, "risultati_p1.json.usati.log")).read().split("\n"):
        p = r.split()
        if len(p) >= 2 and p[0] == "a" and p[1].lstrip("-").isdigit() and int(p[1]) not in iniziali:
            letti.add(int(p[1]))
    fuori = letti - ris
    if fuori:
        passo_log(f"ERRORE: il passo dormienti ha letto {len(fuori)} clienti che non sono dormienti (non dovrebbe)")
    d = json.load(open(vero))
    with open(log_vero, "a") as f:
        f.write("".join(f"a {i}\n" for i in sorted(letti)))
        f.flush()
        os.fsync(f.fileno())
    d["usati"] = sorted(set(d.get("usati", [])) | letti)
    gfc = dfin.get("giro_finale")
    if gfc:
        d[f"giro_finale_dormienti_passo_{k}"] = gfc
    with open(vero + ".tmp", "w") as o:
        json.dump(d, o, indent=1)
    os.replace(vero + ".tmp", vero)
    passo_log(f"passo {k} dormienti: {len(letti)} dormienti letti, tornati nel file di stato vero ({len(fuori)} non "
              f"dormienti)")
    return rc, t0, t1, term, gfc, (1 if fuori else 0)


def passo_massimo(k, stato, copie, opz):
    base = os.path.join(LAV, f"massimo_{k:02d}")
    vero = file_stato(stato)
    prima = open(vero, "rb").read()
    procs = []
    t0 = ora()
    for n in range(1, copie + 1):
        c = os.path.join(base, f"{n:02d}")
        os.makedirs(os.path.join(c, "dati"), exist_ok=True)
        shutil.copy(vero, os.path.join(c, "stato.json"))
        open(os.path.join(c, "stato.json.usati.log"), "w").close()
        shutil.copy(SCRIPT, c)
        term = os.path.join(c, "terminale.txt")
        cmd = [PY, "prova_infinite.py", "giro"] + opz + ["--file", "stato.json", "--cartella-dati", "dati",
                                                         "--giro", f"irlanda_massimo_{k:02d}_{n:02d}"]
        procs.append(lancia(cmd, c, term, f"passo_{k:02d}_massimo_{n:02d}", env_per(False)))
    rcs = [aspetta_proc(p) for p in procs]
    t1 = ora()
    if open(vero, "rb").read() != prima:
        passo_log("ATTENZIONE: il file di stato vero e' cambiato durante il carico massimo (non dovrebbe)")
    somma = os.path.join(OUT, f"massimo-somma-passo-{k:02d}.txt")
    with open(somma, "w") as f:
        subprocess.run([PY, os.path.join(QUI, "somma_massimo.py"), base, os.path.join(OUT, "cpu-macchina.log")],
                       stdout=f, stderr=subprocess.STDOUT)
    if os.path.exists(os.path.join(base, "somma.json")):
        shutil.copy(os.path.join(base, "somma.json"), os.path.join(OUT, f"massimo-somma-passo-{k:02d}.json"))
    dest = os.path.join(OUT, f"massimo_{k:02d}")
    for n in range(1, copie + 1):
        c = os.path.join(base, f"{n:02d}")
        os.makedirs(os.path.join(dest, f"{n:02d}"), exist_ok=True)
        shutil.copy(os.path.join(c, "terminale.txt"), os.path.join(dest, f"{n:02d}"))
        if os.path.isdir(os.path.join(c, "dati")):
            shutil.copytree(os.path.join(c, "dati"), os.path.join(dest, f"{n:02d}", "dati"), dirs_exist_ok=True)
    nonzero = sum(1 for r in rcs if r != 0)
    passo_log(f"passo {k} massimo: {copie} copie, codice diverso da 0: {nonzero}; somma in {os.path.basename(somma)}")
    return nonzero, t0, t1


def campiona_cpu(ferma):
    """CPU di ogni processo dello script vivo, ogni 5 s (% di un core: utime+stime da /proc/PID/stat)."""
    tick = os.sysconf("SC_CLK_TCK")
    prima = {}
    f = open(os.path.join(OUT, "cpu-processi.log"), "a")
    while not ferma.wait(5.0):
        adesso = time.time()
        with _lock:
            vivi = dict(_vivi)
        for pid, et in vivi.items():
            try:
                campi = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
                cpu = (int(campi[11]) + int(campi[12])) / tick
            except (OSError, IndexError, ValueError):
                continue
            if pid in prima:
                t_p, c_p = prima[pid]
                pct = 100.0 * (cpu - c_p) / max(adesso - t_p, 1e-6)
                f.write(json.dumps({"t": ora(), "passo": et, "pid": pid, "cpu_di_un_core": round(pct, 1)}) + "\n")
                f.flush()
            prima[pid] = (adesso, cpu)
    f.close()


def _ts(s):
    return dt.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc).timestamp()


def p95(xs):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(0.95 * len(xs)))] if xs else None


def validita(esiti):
    """Per ogni fase di ogni passo: CPU del processo (p95, max) e della macchina (max) nella finestra della fase."""
    proc, mac = [], []
    for r in open(os.path.join(OUT, "cpu-processi.log")) if os.path.exists(os.path.join(OUT, "cpu-processi.log")) else []:
        try:
            x = json.loads(r)
            proc.append((_ts(x["t"]), x["passo"], x["cpu_di_un_core"]))
        except Exception:
            pass
    for r in open(os.path.join(OUT, "cpu-macchina.log")) if os.path.exists(os.path.join(OUT, "cpu-macchina.log")) else []:
        try:
            x = json.loads(r)
            mac.append((_ts(x["t"]), x["cpu"]))
        except Exception:
            pass
    righe = ["Chi manda il carico non deve essere il limite (RULES.md 2026-11-05): per ogni fase, CPU del processo dello",
             "script (% di un core, ogni 5 s) e di tutta la macchina (% di tutte le vCPU). VALIDA se p95 del processo < 80 e",
             "massimo della macchina < 80. Le fasi lunghe meno di 10 s hanno pochi campioni: guardare il massimo.", ""]
    tutte_ok = True
    for e in esiti:
        if not e.get("fasi"):
            continue
        for fase, (a, b) in e["fasi"].items():
            if fase in NON_MISURE:
                righe.append(f"non misura passo {e['k']:2d} {e['etichetta']:28s} {fase:16s} (caricamento o controllo: "
                             f"non conta)")
                continue
            ta, tb = _ts(a), _ts(b)
            pc = [c for (t, et, c) in proc if et.startswith(e["etichetta"]) and ta <= t <= tb + 5]
            mc = [c for (t, c) in mac if ta <= t <= tb + 5]
            pp, pm, mm = p95(pc), (max(pc) if pc else None), (max(mc) if mc else None)
            ok = (pp is None or pp < SOGLIA) and (mm is None or mm < SOGLIA)
            if e["tipo"] == "massimo":
                ok = mm is None or mm < SOGLIA
            tutte_ok &= ok
            righe.append(f"{'VALIDA    ' if ok else 'NON VALIDA'} passo {e['k']:2d} {e['etichetta']:28s} {fase:16s} "
                         f"processo p95 {pp if pp is not None else 'NON SO'} max {pm if pm is not None else 'NON SO'} "
                         f"(campioni {len(pc)}), macchina max {mm if mm is not None else 'NON SO'}")
    righe.append("")
    righe.append("MACCHINA: " + ("mai il limite in nessuna fase" if tutte_ok else "ATTENZIONE: almeno una fase NON VALIDA"))
    open(os.path.join(OUT, "validita-macchina.txt"), "w").write("\n".join(righe) + "\n")
    return tutte_ok


def fasi_del_passo(stato_file, t0, chiave="giro_finale"):
    try:
        d = json.load(open(stato_file))
    except Exception:
        return {}, {}
    gf = d.get(chiave) or {}
    fasi, esiti = {}, {}
    for f, lista in (gf.get("fasi") or {}).items():
        if f.startswith("pausa"):
            continue
        for e in lista:
            if e.get("inizio", "") >= t0[:19]:
                esiti[f] = e.get("stato")
                if e.get("fine"):
                    fasi[f] = (e["inizio"], e["fine"])
    return fasi, esiti


def main():
    passi = leggi_passi(sys.argv[1])
    err = controlla_passi(passi)
    if err:
        for x in err:
            passo_log("ERRORE nel programma: " + x)
        sys.exit(2)
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(os.path.join(LAV, "stati"), exist_ok=True)
    ferma = threading.Event()
    th = threading.Thread(target=campiona_cpu, args=(ferma,), daemon=True)
    th.start()
    esiti, errori = [], [0]
    catene = {}  # stato -> thread in sottofondo

    def esegui(k, p):
        t, a = p["tipo"], p["arg"]
        stato = a[0]
        try:
            if t == "carica":
                os.makedirs(os.path.join(LAV, "stati", stato), exist_ok=True)
                if os.path.exists(file_stato(stato)):
                    car = json.load(open(file_stato(stato))).get("caricamento") or {}
                    if car.get("finito"):
                        passo_log(f"passo {k} carica {stato}: gia' caricato, non lo rifaccio")
                        return
                    if car:  # lo script non riprende un caricamento: ricominciare scriverebbe di nuovo sui dormienti
                        errori[0] += 1
                        passo_log(f"FERMO passo {k} carica {stato}: caricamento cominciato e non finito; non lo "
                                  f"rifaccio da solo (decide il coordinatore: database nuovo o --forza a mano)")
                        return
                passo_log(f"passo {k} carica {stato}: {' '.join(a[1:])}")
                rc, t0, t1, term = esegui_script(k, stato, ["carica"] + a[1:], False, f"{stato}_carica")
                if not os.path.exists(file_stato(stato) + ".usati.log"):
                    open(file_stato(stato) + ".usati.log", "w").close()
                ok = rc == 0 and json.load(open(file_stato(stato))).get("caricamento", {}).get("finito")
                motivo = json.load(open(file_stato(stato))).get("caricamento", {}).get("motivo_fine") \
                    if os.path.exists(file_stato(stato)) else None
                if not ok or motivo != "obiettivo raggiunto":
                    errori[0] += 1
                passo_log(f"passo {k} carica {stato}: codice {rc}, motivo '{motivo}'")
                esiti.append({"k": k, "tipo": t, "etichetta": f"passo_{k:02d}_{stato}_carica", "rc": rc, "fasi": {}})
            elif t == "giro":
                passo_log(f"passo {k} giro {stato}: {prepara(stato, k)}; {' '.join(a[1:])}")
                rc, t0, t1, term = esegui_script(k, stato, ["giro"] + a[1:], True, stato)
                fasi, st = fasi_del_passo(file_stato(stato), t0)
                chieste = [x for x in (opzione(a[1:], "--solo-fasi") or "").split(",") if x]
                mancano = [f for f in chieste if st.get(f) != "fatta"]
                if rc != 0 or mancano:
                    errori[0] += 1
                passo_log(f"passo {k} giro {stato}: codice {rc}; fasi non 'fatta': {mancano or 'nessuna'}")
                esiti.append({"k": k, "tipo": t, "etichetta": f"passo_{k:02d}_{stato}", "rc": rc, "fasi": fasi})
                if "parallelo" in chieste:
                    controlla_par(k, stato)
            elif t == "dormienti":
                passo_log(f"passo {k} dormienti {stato}: {' '.join(a[1:])}")
                rc, t0, t1, term, gfc, fuori = passo_dormienti(k, stato, a[1:])
                copia = os.path.join(LAV, "dormienti", f"passo_{k:02d}_{stato}", "risultati_p1.json")
                fasi, st = fasi_del_passo(copia, t0)
                chieste = [x for x in (opzione(a[1:], "--solo-fasi") or "").split(",") if x]
                mancano = [f for f in chieste if st.get(f) != "fatta"]
                if rc != 0 or mancano or fuori:
                    errori[0] += 1
                passo_log(f"passo {k} dormienti {stato}: codice {rc}; fasi non 'fatta': {mancano or 'nessuna'}")
                esiti.append({"k": k, "tipo": t, "etichetta": f"passo_{k:02d}_{stato}_dormienti", "rc": rc, "fasi": fasi})
                if "parallelo" in chieste:
                    controlla_par(k, stato)
            elif t == "massimo":
                copie = int(a[1])
                passo_log(f"passo {k} massimo {stato}: {copies_txt(copie)}; {' '.join(a[2:])}")
                nonzero, t0, t1 = passo_massimo(k, stato, copie, a[2:])
                if nonzero:
                    errori[0] += 1
                esiti.append({"k": k, "tipo": t, "etichetta": f"passo_{k:02d}_massimo", "rc": nonzero,
                              "fasi": {"carico_massimo": (t0, t1)}})
        except Exception as e:  # un passo rotto non ferma i dati gia' presi: si scrive e si va avanti
            errori[0] += 1
            passo_log(f"ERRORE nel passo {k} ({t} {stato}): {type(e).__name__}: {e}")

    for k, p in enumerate(passi, 1):
        if p["tipo"] == "aspetta":
            for s, ths in list(catene.items()):
                for x in ths:
                    x.join()
            catene.clear()
            passo_log(f"passo {k}: i passi in sottofondo sono finiti")
            continue
        if p["sotto"]:
            stato = p["arg"][0]
            prima = list(catene.get(stato, []))

            def fila(k=k, p=p, prima=prima):
                for x in prima:
                    x.join()
                esegui(k, p)
            x = threading.Thread(target=fila)
            x.start()
            catene.setdefault(stato, []).append(x)
        else:
            esegui(k, p)
    for ths in catene.values():
        for x in ths:
            x.join()
    ferma.set()
    th.join(timeout=10)
    macchina_ok = validita(sorted(esiti, key=lambda e: e["k"]))
    json.dump({"passi": len(passi), "errori": errori[0], "macchina_mai_limite": macchina_ok,
               "esiti": sorted(esiti, key=lambda e: e["k"])}, open(os.path.join(OUT, "programma-esito.json"), "w"),
              indent=1)
    passo_log(f"programma finito: errori {errori[0]}; macchina " + ("mai il limite" if macchina_ok else "LIMITE in "
                                                                     "almeno una fase (validita-macchina.txt)"))
    sys.exit(0 if errori[0] == 0 else 1)


def copies_txt(n):
    return f"{n} copie insieme ({os.cpu_count()} vCPU)"


def controlla_par(k, stato):
    carts = sorted(glob.glob(os.path.join(LAV, "dati", stato, "giro_finale_*")), key=os.path.getmtime)
    if not carts:
        return
    out = os.path.join(OUT, f"parallelo-connessioni-passo-{k:02d}.txt")
    with open(out, "w") as f:
        subprocess.run([PY, os.path.join(QUI, "controlla_parallelo.py"), carts[-1]], stdout=f, stderr=subprocess.STDOUT)
    try:
        passo_log(f"passo {k} connessioni del parallelo: {open(out).read().strip().splitlines()[-1]}")
    except IndexError:
        passo_log(f"passo {k} connessioni del parallelo: NON SO")


if __name__ == "__main__":
    main()
