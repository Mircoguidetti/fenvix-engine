#!/usr/bin/env python3
"""Q8c on AWS: sampler on a replica machine. Every second one JSON line with the time, the busy % of EVERY core
(Q8c rule: no core of the system under test above 90%), CPU and memory of each replica process (one per log), and
the disk counters (all "nvme*n1" and "xvd*" disks).
Usage: campiona.py OUT PID [PID ...]"""
import json, os, sys, time
out, pids = sys.argv[1], [int(x) for x in sys.argv[2:]]
HZ = os.sysconf('SC_CLK_TCK')
def cpu(p):
    try:
        f = open(p).read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / HZ
    except Exception:
        return None
def rss(pid):
    try:
        for l in open(f'/proc/{pid}/status'):
            if l.startswith('VmRSS'):
                return int(l.split()[1])
    except Exception:
        return None
def cores():
    o = {}
    for l in open('/proc/stat'):
        if l.startswith('cpu') and l[3].isdigit():
            f = l.split(); v = [int(x) for x in f[1:]]
            o[f[0]] = (v[3] + v[4], sum(v))
    return o
def disks():
    d = {}
    for l in open('/proc/diskstats'):
        f = l.split()
        if (f[2].startswith('nvme') and f[2].endswith('n1')) or f[2].startswith('xvd'):
            d[f[2]] = {'writes': int(f[7]), 'sectors_w': int(f[9]), 'ms_w': int(f[10]), 'io_ms': int(f[12]), 'flush': int(f[18]) if len(f) > 18 else 0}
    return d
a = cores()
with open(out, 'a') as o:
    while any(os.path.exists(f'/proc/{p}') for p in pids):
        time.sleep(1)
        b = cores()
        pc = {k: round(100.0 * (1 - (b[k][0] - a[k][0]) / max(1, b[k][1] - a[k][1])), 1) for k in b}
        a = b
        o.write(json.dumps({'t': time.time(), 'percore': pc, 'procs': {str(p): {'cpu': cpu(f'/proc/{p}/stat'), 'main': cpu(f'/proc/{p}/task/{p}/stat'), 'rss_kb': rss(p)} for p in pids}, 'disk': disks()}) + '\n')
        o.flush()
