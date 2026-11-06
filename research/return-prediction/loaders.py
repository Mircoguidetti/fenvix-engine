"""Loaders: each returns a list of (group, user_string, unix_seconds) as three numpy arrays.

Raw data stays outside git (default /home/user/data); see README.md for where each file comes from.
"""
import calendar
import glob
import gzip
import os
import re
import time

import numpy as np

DATA = os.environ.get("RP_DATA", "/home/user/data")


def _arrays(g, u, t):
    return np.array(g, dtype=object), np.array(u, dtype=object), np.array(t, dtype=np.int64)


def lsapp(path=None):
    """LSApp (Aliannejadi et al., TOIS 2021): Android app usage events, 292 users, 2017-09 to 2018-05.
    group = app name, user = user_id. Timestamps are naive local times, read as UTC (only the hour of the night
    boundary depends on it, and that is chosen from the data)."""
    path = path or os.path.join(DATA, "lsapp", "lsapp.tsv")
    g, u, t = [], [], []
    with open(path) as f:
        f.readline()
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 5:
                continue
            uid, _sid, ts, app, _ev = p
            t.append(calendar.timegm(time.strptime(ts, "%Y-%m-%d %H:%M:%S")))
            u.append(uid)
            g.append(app)
    return _arrays(g, u, t)


LINE = re.compile(r"^\[(\d\d):(\d\d)\] (?:<([^>]+)>|\s\*\s(\S+))")
BOT = re.compile(r"bot|^meetingology$|^lubotu|^ubot|^ubottu")


def norm_nick(n):
    n = n.lower().lstrip("@+")
    n = re.sub(r"[|`^].*$", "", n)
    return n.rstrip("_-0123456789") or n


def irc(root=None):
    """Ubuntu IRC logs (irclogs.ubuntu.com): one file per channel per UTC day, one line per message.
    group = channel, user = normalised nick. Only messages and /me actions count as accesses (joins, quits and nick
    changes do not: a bouncer joins without a person). Nicks that look like bots are dropped."""
    root = root or os.path.join(DATA, "irc")
    g, u, t = [], [], []
    for ch_dir in sorted(glob.glob(os.path.join(root, "*"))):
        ch = os.path.basename(ch_dir)
        for fp in sorted(glob.glob(os.path.join(ch_dir, "*.txt"))):
            day = calendar.timegm(time.strptime(os.path.basename(fp)[:10], "%Y-%m-%d"))
            with open(fp, errors="replace") as f:
                for line in f:
                    m = LINE.match(line)
                    if not m:
                        continue
                    nick = norm_nick(m.group(3) or m.group(4))
                    if not nick or BOT.search(nick):
                        continue
                    g.append(ch)
                    u.append(nick)
                    t.append(day + int(m.group(1)) * 3600 + int(m.group(2)) * 60)
    return _arrays(g, u, t)


def cdnow(path=None):
    """CDNOW (Fader and Hardie; shipped in the `lifetimes` PyPI package): 23,570 customers who first bought in
    Q1 1997, every purchase until June 1998, day granularity. One purchase day = one access at 12:00."""
    if path is None:
        import lifetimes
        path = os.path.join(os.path.dirname(lifetimes.__file__), "datasets", "CDNOW_master.txt")
    g, u, t = [], [], []
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 4 or not p[1].isdigit():
                continue
            g.append("cdnow")
            u.append(p[0])
            t.append(calendar.timegm(time.strptime(p[1], "%Y%m%d")) + 12 * 3600)
    return _arrays(g, u, t)


def rees46_m24(path=None):
    """REES46 subset already used by M24 (fenvix-engine branch riscaldamento-utente, tracce/ecom/ecom-eventi.tsv.gz):
    first 12.7 days of October 2019, 10% of users. Shorter than 4 weeks: side check only."""
    path = path or os.path.join(DATA, "rees46", "ecom-eventi.tsv.gz")
    g, u, t = [], [], []
    with gzip.open(path, "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 3:
                continue
            g.append("rees46")
            u.append(p[1])
            t.append(calendar.timegm(time.strptime(p[0][:19], "%Y-%m-%d %H:%M:%S")))
    return _arrays(g, u, t)


def encode(users):
    _, codes = np.unique(users.astype(str), return_inverse=True)
    return codes.astype(np.int64)
