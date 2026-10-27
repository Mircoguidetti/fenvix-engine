#!/usr/bin/env python3
"""Fetch a docs page (cached on disk) and append to compat/fetch_log.csv: url, http status, bytes, sha256, fetched_at.
Usage: fetch.py URL [URL...]   (module: fetch(url) -> text or None)"""
import csv, hashlib, os, ssl, sys, threading, time, urllib.request, urllib.error
_lock = threading.Lock()
HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(HERE, "..", "fetch_log.csv")
CACHE = os.environ.get("COMPAT_CACHE", "/tmp/compat-cache")
os.makedirs(CACHE, exist_ok=True)
_logged = set()
if os.path.exists(LOG):
    _logged = {r[0] for r in csv.reader(open(LOG))}

def fetch(url, retries=3):
    key = hashlib.sha1(url.encode()).hexdigest()
    p = os.path.join(CACHE, key)
    if os.path.exists(p + ".txt"):
        return open(p + ".txt", encoding="utf-8").read()
    if os.path.exists(p + ".404"):
        return None
    status, body = None, b""
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "fenvix-compat-audit/1.0"})
            with urllib.request.urlopen(req, timeout=40) as r:
                status, body = r.status, r.read()
            break
        except urllib.error.HTTPError as e:
            status, body = e.code, b""
            if e.code in (404, 403, 410):
                break
            time.sleep(2 ** i)
        except Exception as e:
            status = "ERR:" + type(e).__name__
            time.sleep(2 ** i)
    with _lock:
        if url not in _logged:
            with open(LOG, "a", newline="") as f:
                csv.writer(f).writerow([url, status, len(body), hashlib.sha256(body).hexdigest()[:16], time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())])
            _logged.add(url)
    if status == 200:
        t = body.decode("utf-8", "replace")
        open(p + ".txt", "w", encoding="utf-8").write(t)
        return t
    if status == 404:
        open(p + ".404", "w").close()
    return None

if __name__ == "__main__":
    for u in sys.argv[1:]:
        t = fetch(u)
        print(u, "OK" if t is not None else "FAIL", len(t or ""))
