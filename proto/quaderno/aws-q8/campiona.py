#!/usr/bin/env python3
"""Q8 su AWS: campionatore sulla macchina di una replica. Ogni 5 s una riga JSON con l'ora, la CPU (processo e
thread principale) e la memoria della replica, e i contatori del disco (tutti i dischi "nvme*n1" e "xvd*").
Uso: campiona.py PID USCITA"""
import json, os, sys, time
pid, out = int(sys.argv[1]), sys.argv[2]
HZ = os.sysconf('SC_CLK_TCK')
def cpu(p):
    try:
        f = open(p).read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / HZ
    except Exception:
        return None
def rss():
    try:
        for l in open(f'/proc/{pid}/status'):
            if l.startswith('VmRSS'):
                return int(l.split()[1])
    except Exception:
        return None
def disks():
    d = {}
    for l in open('/proc/diskstats'):
        f = l.split()
        if (f[2].startswith('nvme') and f[2].endswith('n1')) or f[2].startswith('xvd'):
            d[f[2]] = {'writes': int(f[7]), 'sectors_w': int(f[9]), 'ms_w': int(f[10]), 'io_ms': int(f[12]), 'flush': int(f[18]) if len(f) > 18 else 0}
    return d
with open(out, 'a') as o:
    while os.path.exists(f'/proc/{pid}'):
        o.write(json.dumps({'t': time.time(), 'cpu': cpu(f'/proc/{pid}/stat'), 'main': cpu(f'/proc/{pid}/task/{pid}/stat'), 'rss_kb': rss(), 'disk': disks()}) + '\n')
        o.flush()
        time.sleep(5)
