"""M22: N rami Postgres (istantanea btrfs copy-on-write a blocchi di una cartella dati spenta, come i cloni ZFS di
Xata), tutti fermi ad aspettare.

Per ogni ramo: tempo di creazione (mount + avvio + prima connessione + prima lettura), RAM (RSS e PSS di tutti i
processi dell'istanza) dopo il lavoro tipico di B15 (100 letture, 20 scritture) e 30 s di pausa, prima lettura dopo la
pausa in tre stati: acceso, sospeso (SIGSTOP/SIGCONT), spento (CHECKPOINT + arresto, poi riavvio).

Uso (root): python3 pg_rami.py --base /mnt/rami/base1 --n 50 --conf neon --out risultati/pg-1g-neon.json
"""
import argparse
import json
import os
import pwd
import random
import signal
import statistics
import subprocess
import time

import psycopg2

PG = "/usr/lib/postgresql/16/bin"
CONFS = {
    # valori tipo Neon 0,25 CU (shared_buffers 128 MB, max_connections 112), il resto di serie
    "neon": {"shared_buffers": "128MB", "max_connections": "112"},
    # la migliore occasione per Postgres: buffer e connessioni al minimo ragionevole
    "stretta": {"shared_buffers": "16MB", "max_connections": "20", "autovacuum_max_workers": "1"},
}
PGUSER = pwd.getpwnam("postgres")
SOCK = "/tmp/pgrami"
LOGS = "/home/claude/pgrami-log"


def as_postgres():
    os.setgid(PGUSER.pw_gid)
    os.setuid(PGUSER.pw_uid)


def mem_avail_mb():
    for line in open("/proc/meminfo"):
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024


def rollup(pid):
    out = {}
    try:
        for line in open(f"/proc/{pid}/smaps_rollup"):
            p = line.split()
            if len(p) >= 3 and p[0].endswith(":") and p[2] == "kB":
                out[p[0][:-1]] = int(p[1])
    except FileNotFoundError:
        pass
    return out


def children(pid):
    try:
        with open(f"/proc/{pid}/task/{pid}/children") as f:
            return [int(x) for x in f.read().split()]
    except FileNotFoundError:
        return []


class Ramo:
    def __init__(self, i, base, root, conf):
        self.i, self.base, self.conf = i, base, CONFS[conf]
        self.mnt = os.path.join(root, f"r{i:03d}")           # sottovolume btrfs: la cartella dati del ramo
        self.dir = os.path.join(LOGS, f"r{i:03d}")
        self.port = 6100 + i
        self.proc = self.conn = None

    def create(self):
        t0 = time.perf_counter()
        subprocess.run(["btrfs", "subvolume", "snapshot", self.base, self.mnt], check=True, stdout=subprocess.DEVNULL)
        t_snap = time.perf_counter() - t0
        os.makedirs(self.dir, exist_ok=True)
        t_ready, t_first = self.start(t0)
        return {"istantanea_ms": t_snap * 1e3, "pronto_ms": t_ready * 1e3, "prima_lettura_ms": t_first * 1e3}

    def exclusive_mb(self):
        out = subprocess.run(["btrfs", "filesystem", "du", "-s", "--raw", self.mnt], capture_output=True, text=True).stdout.split("\n")[1].split()
        return int(out[1]) / 2**20

    def start(self, t0=None):
        """Avvia il postmaster e aspetta la prima connessione; ritorna (secondi alla connessione, secondi alla prima lettura)."""
        t0 = t0 or time.perf_counter()
        args = [f"{PG}/postgres", "-D", self.mnt, "-p", str(self.port), "-k", SOCK, "-c", "listen_addresses="]
        for k, v in self.conf.items():
            args += ["-c", f"{k}={v}"]
        self.log = open(os.path.join(self.dir, "pg.log"), "ab")
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=self.log, preexec_fn=as_postgres)
        while True:
            try:
                self.conn = psycopg2.connect(host=SOCK, port=self.port, user="postgres", dbname="postgres")
                break
            except psycopg2.OperationalError:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"postgres {self.i} uscito, vedi {self.dir}/pg.log")
                time.sleep(0.002)
        t_ready = time.perf_counter() - t0
        self.conn.autocommit = True
        self.read()
        return t_ready, time.perf_counter() - t0

    def read(self):
        with self.conn.cursor() as c:
            c.execute("select abalance from pgbench_accounts where aid = %s", (random.randint(1, self.naccounts),))
            return c.fetchone()

    naccounts = 6_800_000

    letture, scritture = 100, 20

    def work(self):
        """Il lavoro tipico di un tentativo in B15: 100 letture e 20 scritture da una riga (cambiabile con --letture/--scritture)."""
        for _ in range(self.letture):
            self.read()
        with self.conn.cursor() as c:
            for _ in range(self.scritture):
                c.execute("update pgbench_accounts set abalance = abalance + 1 where aid = %s", (random.randint(1, self.naccounts),))

    def pids(self):
        if not self.proc:
            return []
        return [self.proc.pid] + children(self.proc.pid)

    def memory(self):
        tot = {"rss_kb": 0, "pss_kb": 0, "pss_anon_kb": 0, "pss_shmem_kb": 0, "pss_file_kb": 0, "processi": 0}
        for p in self.pids():
            r = rollup(p)
            tot["rss_kb"] += r.get("Rss", 0)
            tot["pss_kb"] += r.get("Pss", 0)
            tot["pss_anon_kb"] += r.get("Pss_Anon", 0)
            tot["pss_shmem_kb"] += r.get("Pss_Shmem", 0)
            tot["pss_file_kb"] += r.get("Pss_File", 0)
            tot["processi"] += 1
        return tot

    def signal_all(self, sig):
        for p in self.pids():
            try:
                os.kill(p, sig)
            except ProcessLookupError:
                pass

    def timed_read(self):
        t0 = time.perf_counter()
        self.read()
        return (time.perf_counter() - t0) * 1e3

    def stop(self, checkpoint=True):
        if self.conn:
            if checkpoint:
                with self.conn.cursor() as c:
                    c.execute("checkpoint")
            self.conn.close()
            self.conn = None
        if self.proc:
            self.proc.send_signal(signal.SIGINT)   # arresto "fast"
            self.proc.wait(60)
            self.proc = None

    def destroy(self):
        self.stop(checkpoint=False)
        subprocess.run(["btrfs", "subvolume", "delete", self.mnt], stdout=subprocess.DEVNULL)


def stats(xs):
    xs = sorted(xs)
    n = len(xs)
    return {"n": n, "min": xs[0], "mediana": statistics.median(xs), "p90": xs[min(n - 1, int(n * 0.9))], "max": xs[-1],
            "media": statistics.fmean(xs)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--conf", default="neon", choices=CONFS)
    ap.add_argument("--pausa", type=float, default=30)
    ap.add_argument("--accounts", type=int, default=6_800_000)
    ap.add_argument("--root", default="/mnt/rami/rami")
    ap.add_argument("--freddi", type=int, default=10, help="quanti riavvii misurare con la cache del sistema svuotata")
    ap.add_argument("--letture", type=int, default=100)
    ap.add_argument("--scritture", type=int, default=20)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    Ramo.naccounts, Ramo.letture, Ramo.scritture = a.accounts, a.letture, a.scritture
    os.makedirs(SOCK, exist_ok=True)
    os.makedirs(a.root, exist_ok=True)
    os.chown(SOCK, PGUSER.pw_uid, PGUSER.pw_gid)
    random.seed(22)
    res = {"base": a.base, "n": a.n, "letture": a.letture, "scritture": a.scritture, "conf": a.conf, "config": CONFS[a.conf], "pausa_s": a.pausa,
           "postgres": subprocess.run([f"{PG}/postgres", "--version"], capture_output=True, text=True).stdout.strip()}
    subprocess.run(["sync"])
    open("/proc/sys/vm/drop_caches", "w").write("3\n")
    time.sleep(1)
    avail0 = mem_avail_mb()
    rami = [Ramo(i, a.base, a.root, a.conf) for i in range(a.n)]
    try:
        crea = []
        for r in rami:
            crea.append(r.create())
        res["creazione"] = {k: stats([c[k] for c in crea]) for k in crea[0]}
        res["creazione_grezzi"] = crea
        for r in rami:
            r.work()
        time.sleep(a.pausa)
        # 1. acceso e fermo
        mems = [r.memory() for r in rami]
        res["fermo_acceso_mem"] = {k: stats([m[k] / (1024 if k.endswith("_kb") else 1) for m in mems]) for k in mems[0]}
        res["fermo_acceso_mem"]["unita"] = "MB per istanza (processi: numero)"
        res["sistema_memavailable_calo_mb"] = avail0 - mem_avail_mb()
        res["sistema_calo_per_istanza_mb"] = res["sistema_memavailable_calo_mb"] / a.n
        res["fermo_acceso_prima_lettura_ms"] = stats([r.timed_read() for r in rami])
        # 2. sospeso con SIGSTOP, poi SIGCONT
        for r in rami:
            r.signal_all(signal.SIGSTOP)
        time.sleep(a.pausa)
        mems = [r.memory() for r in rami]
        res["sospeso_mem_pss_mb"] = stats([m["pss_kb"] / 1024 for m in mems])
        sv = []
        for r in rami:
            t0 = time.perf_counter()
            r.signal_all(signal.SIGCONT)
            r.read()
            sv.append((time.perf_counter() - t0) * 1e3)
        res["sospeso_risveglio_ms"] = stats(sv)
        # 3. spento: CHECKPOINT e arresto, poi riavvio con la cache del sistema calda (e fredda per i primi `freddi`)
        for r in rami:
            r.stop()
        res["spento_memavailable_calo_mb"] = avail0 - mem_avail_mb()
        time.sleep(a.pausa)
        caldi = []
        for r in rami[a.freddi:]:
            caldi.append(r.start()[1] * 1e3)
        res["spento_risveglio_cache_calda_ms"] = stats(caldi) if caldi else None
        freddi = []
        for r in rami[:a.freddi]:
            subprocess.run(["sync"])
            open("/proc/sys/vm/drop_caches", "w").write("3\n")
            freddi.append(r.start()[1] * 1e3)
        res["spento_risveglio_cache_fredda_ms"] = stats(freddi) if freddi else None
        # memoria subito dopo un riavvio (prima del lavoro)
        mems = [r.memory() for r in rami]
        res["dopo_riavvio_mem_pss_mb"] = stats([m["pss_kb"] / 1024 for m in mems])
        # spazio del copy-on-write di ogni ramo
        subprocess.run(["sync"])
        res["spazio_ramo_esclusivo_mb"] = stats([r.exclusive_mb() for r in rami])
    finally:
        for r in rami:
            try:
                r.destroy()
            except Exception as e:
                print("pulizia", r.i, e)
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    json.dump(res, open(a.out, "w"), indent=1)
    show = {k: v for k, v in res.items() if k != "creazione_grezzi"}
    print(json.dumps(show, indent=1))


if __name__ == "__main__":
    main()
