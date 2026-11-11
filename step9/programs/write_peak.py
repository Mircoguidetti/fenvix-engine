"""Step 9 · program "write-peak" (aws-fixes, B4 of the total review): peak writes WITH THE REAL INDEXES through the
MEASURED cook (the round's cook type, m7i.large, the 3-zone log on, the round's server_options), like for like with the
Atlas number it is compared with.

The Atlas number (MEASURED, 2026-11-05, Admin API metrics of the Ireland load run 42053678455, fenvix-brain
sessions/2026-11-05-revisore-analisi-atlas-m30/RAPPORTO.md C2): the founder's script loading 6 databases at the same
time, 16 threads each, batches of 1,000 documents of about 341 bytes, into collections with their real secondary
indexes (cliente_ts; cliente_ts and stato_ts on ordini); after a burst of about 4 minutes, 25,800 documents per second for
24 minutes in a row (p50 26,000, max 30,800), the M30 primary at 181% of 200% CPU, the client machine at 10%.
The m30 program's maximum-load step does NOT measure this: it writes into a scratch collection with only `_id`
(prova_infinite.py, phase carico_massimo); and our load job goes through cook 0 (another machine type, log off).

What this program does, on a client of the m30 client's type (c7i.xlarge), after the extra programs:
  - DBS processes of the founder's script, UNCHANGED (md5 checked), each `carica --db app_peak_N --gb GB
    --quota-riservati 0.4 --seme 7000+N --thread THREADS` (the Atlas load's options; GB large enough never to finish),
    started together against the measured cook;
  - every SAMPLE_S seconds, from the cook's serverStatus: documents inserted (`metrics.document.inserted`, the counter
    mongod also has; `opcounters.insert` kept too) and the cook process's CPU (`extra_info.user_time_us` +
    `system_time_us`), plus this machine's CPU (/proc/stat): OUT/samples.jsonl;
  - after MINUTES, SIGTERM to every script (its own handler stops cleanly and saves its state file), SIGKILL after 180 s;
  - the counts per database (dbStats), then the databases are DROPPED (so later jobs on the kept pantry are unchanged;
    --keep keeps them);
  - OUT/write-peak-summary.json: documents per second per minute, the steady part (minutes after the first BURST_MIN,
    as Atlas's 24 minutes came after its first 4), p50 / mean / min / max, the cook's CPU in % of its vCPU, the client's
    CPU; VALID only if the client machine stayed under 80% CPU (RULES: the sender must not be the limit).
Last line: "WRITE-PEAK: p50 N documents/s ..." or "WRITE-PEAK: NOT VALID ..." or "WRITE-PEAK: FAILED ...".
Usage: write_peak.py OUT --uri URI [--dbs 6] [--threads 16] [--minutes 25] [--gb 10] [--sample-s 5] [--burst-min 4]
       [--cook-vcpu 2] [--keep]"""
import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time

SCRIPT_MD5 = "8396ca455557baaa4e3a9b1f881451f9"
ATLAS = {"docs_per_s": 25800, "p50": 26000, "max": 30800, "minutes": 24, "cook_cpu_pct_of_machine": 90.5,
         "source": "Atlas M30 Ireland load, run 42053678455, 2026-11-05 (MEASURED, Admin API metrics)"}


def cpu_times():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    return sum(v), v[3] + (v[4] if len(v) > 4 else 0)


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def server_sample(client):
    s = client.admin.command("serverStatus")
    ei = s.get("extra_info") or {}
    return {"inserted": int(((s.get("metrics") or {}).get("document") or {}).get("inserted", 0) or 0),
            "op_insert": int((s.get("opcounters") or {}).get("insert", 0) or 0),
            "cpu_us": int(ei.get("user_time_us", 0) or 0) + int(ei.get("system_time_us", 0) or 0),
            "s3_put": ((s.get("fenvix") or {}).get("s3") or {}).get("put")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--dbs", type=int, default=6)
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--minutes", type=float, default=25)
    ap.add_argument("--gb", type=float, default=10)
    ap.add_argument("--sample-s", type=float, default=5)
    ap.add_argument("--burst-min", type=float, default=4)
    ap.add_argument("--cook-vcpu", type=int, default=2)
    ap.add_argument("--script", default="prova_infinite.py")
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    script = os.path.abspath(a.script)
    if hashlib.md5(open(script, "rb").read()).hexdigest() != SCRIPT_MD5:
        print("WRITE-PEAK: FAILED: the script is not the founder's (md5)")
        sys.exit(2)
    from pymongo import MongoClient
    client = MongoClient(a.uri, appname="app-demo", serverSelectionTimeoutMS=20000)
    names = [f"app_peak_{i}" for i in range(1, a.dbs + 1)]
    existing = set(client.list_database_names()) & set(names)
    if existing:
        print(f"WRITE-PEAK: FAILED: databases already there: {sorted(existing)}")
        sys.exit(2)
    samples = open(os.path.join(a.out, "samples.jsonl"), "w")
    stop = threading.Event()
    rows = []

    def sampler():
        t_prev, idle_prev = cpu_times()
        while not stop.is_set():
            try:
                r = server_sample(client)
            except Exception as e:  # noqa: BLE001 - a missed sample is recorded, the load goes on
                r = {"error": f"{type(e).__name__}: {str(e)[:120]}"}
            t, idle = cpu_times()
            r["t"] = time.time()
            r["client_cpu_pct"] = round(100 * (1 - (idle - idle_prev) / max(1, t - t_prev)), 1)
            t_prev, idle_prev = t, idle
            rows.append(r)
            samples.write(json.dumps(r) + "\n")
            samples.flush()
            stop.wait(a.sample_s)

    procs = []
    env = dict(os.environ, FENVIX_P1_URI=a.uri, PYTHONUNBUFFERED="1")
    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    time.sleep(a.sample_s)
    t_start = time.time()
    for i, n in enumerate(names, 1):
        d = os.path.join(a.out, n)
        os.makedirs(d, exist_ok=True)
        shutil.copy(script, os.path.join(d, "prova_infinite.py"))
        cmd = [sys.executable, "prova_infinite.py", "carica", "--db", n, "--gb", str(a.gb), "--quota-riservati", "0.4",
               "--seme", str(7000 + i), "--thread", str(a.threads), "--max-ore-carico", str(max(1.0, a.minutes / 60 + 0.5)),
               "--tetto-logico-gb", "60"]
        with open(os.path.join(d, "command.txt"), "w") as f:
            f.write(" ".join(cmd) + "\n")
        procs.append(subprocess.Popen(cmd, cwd=d, env=env, stdout=open(os.path.join(d, "terminal.txt"), "w"),
                                      stderr=subprocess.STDOUT, start_new_session=True))
    print(f"write-peak: {a.dbs} script processes x {a.threads} threads started for {a.minutes:g} minutes", flush=True)
    end = t_start + a.minutes * 60
    while time.time() < end and any(p.poll() is None for p in procs):
        time.sleep(1)
    ended_early = [names[i] for i, p in enumerate(procs) if p.poll() is not None]
    t_stop = time.time()
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    deadline = time.time() + 180
    while time.time() < deadline and any(p.poll() is None for p in procs):
        time.sleep(1)
    killed = []
    for i, p in enumerate(procs):
        if p.poll() is None:
            p.kill()
            killed.append(names[i])
    time.sleep(a.sample_s)
    stop.set()
    th.join(timeout=30)
    samples.close()
    counts = {}
    for n in names:
        try:
            counts[n] = client[n].command("dbStats").get("objects")
        except Exception as e:  # noqa: BLE001
            counts[n] = f"{type(e).__name__}"
    dropped = []
    if not a.keep:
        for n in names:
            try:
                client.drop_database(n)
                dropped.append(n)
            except Exception as e:  # noqa: BLE001
                print(f"drop {n}: {type(e).__name__}", flush=True)
    # documents per second per minute, from the cook's own counter, over the load window
    ok_rows = [r for r in rows if "inserted" in r and t_start - a.sample_s <= r["t"] <= t_stop + a.sample_s]
    per_min, cpu_min = [], []
    if ok_rows:
        k0 = ok_rows[0]
        m = 1
        for r in ok_rows[1:]:
            if r["t"] - k0["t"] >= 60 or r is ok_rows[-1]:
                dt = r["t"] - k0["t"]
                if dt >= 30:
                    per_min.append({"minute": m, "docs_per_s": round((r["inserted"] - k0["inserted"]) / dt),
                                    "cook_cpu_pct": round(100 * (r["cpu_us"] - k0["cpu_us"]) / 1e6 / dt / a.cook_vcpu, 1),
                                    "t_from_start_s": round(r["t"] - t_start)})
                k0 = r
                m += 1
    steady = [x for x in per_min if x["t_from_start_s"] > a.burst_min * 60 + 30]
    rates = [x["docs_per_s"] for x in steady]
    ccpu = [x["cook_cpu_pct"] for x in steady]
    client_cpu = [r["client_cpu_pct"] for r in rows if t_start <= r["t"] <= t_stop]
    errors = [r["error"] for r in rows if "error" in r]
    valid = bool(rates) and (max(client_cpu) if client_cpu else 100) < 80
    s = {"dbs": a.dbs, "threads": a.threads, "minutes": a.minutes, "gb_per_db": a.gb, "burst_min": a.burst_min,
         "per_minute": per_min, "steady_minutes": len(steady),
         "steady_docs_per_s": {"p50": pct(rates, 0.5), "mean": round(sum(rates) / len(rates)) if rates else None,
                               "min": min(rates) if rates else None, "max": max(rates) if rates else None},
         "steady_cook_cpu_pct_of_machine": {"p50": pct(ccpu, 0.5), "max": max(ccpu) if ccpu else None},
         "client_cpu_pct": {"p50": pct(client_cpu, 0.5), "max": max(client_cpu) if client_cpu else None},
         "valid": valid, "documents_per_db": counts, "dropped": dropped, "ended_before_time": ended_early,
         "killed_after_sigterm": killed, "sample_errors": len(errors), "first_sample_errors": errors[:5],
         "script_exit_codes": [p.returncode for p in procs], "atlas": ATLAS}
    json.dump(s, open(os.path.join(a.out, "write-peak-summary.json"), "w"), indent=1)
    st = s["steady_docs_per_s"]
    if not rates or ended_early:
        print(f"WRITE-PEAK: FAILED: {len(rates)} steady minutes; scripts that ended before the time: {ended_early}; "
              f"sample errors {len(errors)}")
        sys.exit(1)
    line = (f"p50 {st['p50']} documents/s (mean {st['mean']}, min {st['min']}, max {st['max']}) over {len(steady)} steady "
            f"minutes, cook CPU p50 {s['steady_cook_cpu_pct_of_machine']['p50']}% of its {a.cook_vcpu} vCPU, client CPU max "
            f"{s['client_cpu_pct']['max']}%; Atlas M30: {ATLAS['docs_per_s']} (p50 {ATLAS['p50']}) at {ATLAS['cook_cpu_pct_of_machine']}%")
    print(f"WRITE-PEAK: {line}" if valid else f"WRITE-PEAK: NOT VALID (the client machine reached 80% CPU): {line}")
    sys.exit(0 if valid else 1)


if __name__ == "__main__":
    main()
