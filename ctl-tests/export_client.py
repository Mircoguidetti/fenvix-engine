"""Download an export from the admin interface into a mongodump directory, resuming broken transfers with Range
requests, and verify the SHA-256 recorded by the server. Usage as a library (download) or from the command line:
    python export_client.py <admin-base-url> <token> <export-id> <dest-dir>"""
import hashlib
import http.client
import json
import os
import sys
import time
import urllib.parse


def _conn(base):
    u = urllib.parse.urlsplit(base)
    return http.client.HTTPConnection(u.hostname, u.port, timeout=60)


def get_json(base, token, path):
    c = _conn(base)
    c.request("GET", path, headers={"Authorization": f"Bearer {token}"})
    r = c.getresponse()
    body = r.read()
    c.close()
    return r.status, json.loads(body)


def fetch(base, token, export_id, path, dest, stop_after=None, retry=True, attempts=200):
    """Append the file to `dest`, continuing from its current size. `stop_after`: abort the connection after that many
    bytes of this call (to simulate a broken transfer). Returns True when the server ended the stream normally."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    got_this_call = 0
    for attempt in range(attempts):
        have = os.path.getsize(dest) if os.path.exists(dest) else 0
        c = _conn(base)
        headers = {"Authorization": f"Bearer {token}"}
        if have:
            headers["Range"] = f"bytes={have}-"
        try:
            c.request("GET", f"/v1/exports/{export_id}/files/{urllib.parse.quote(path)}", headers=headers)
            r = c.getresponse()
            if r.status not in (200, 206):
                raise RuntimeError(f"download of {path}: HTTP {r.status} {r.read()[:200]!r}")
            if have and r.status == 200:  # the server ignored the range: start over
                open(dest, "wb").close()
            with open(dest, "ab") as f:
                while True:
                    chunk = r.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
                    got_this_call += len(chunk)
                    if stop_after is not None and got_this_call >= stop_after:
                        c.close()
                        return False
            c.close()
            return True
        except (http.client.IncompleteRead, http.client.RemoteDisconnected, ConnectionError, OSError, http.client.HTTPException):
            c.close()
            if not retry:
                return False
            time.sleep(0.3)
    raise RuntimeError(f"download of {path} did not finish")


def download(base, token, export_id, dest, stop_after=None):
    st, rec = get_json(base, token, f"/v1/exports/{export_id}")
    assert st == 200, rec
    for f in rec["files"]:
        fetch(base, token, export_id, f["path"], os.path.join(dest, f["path"]), stop_after=None)
    return verify(base, token, export_id, dest)


def verify(base, token, export_id, dest):
    st, rec = get_json(base, token, f"/v1/exports/{export_id}")
    assert st == 200, rec
    out = {}
    for f in rec["files"]:
        p = os.path.join(dest, f["path"])
        data = open(p, "rb").read()
        want = f.get("complete") if f["kind"] == "bson" else {"bytes": f["bytes"], "sha256": f["sha256"]}
        if want is None:
            out[f["path"]] = "unverified"
            continue
        ok = len(data) == want["bytes"] and hashlib.sha256(data).hexdigest() == want["sha256"]
        out[f["path"]] = "ok" if ok else f"MISMATCH (have {len(data)} bytes, server says {want['bytes']})"
    return out


if __name__ == "__main__":
    base, token, eid, dest = sys.argv[1:5]
    print(json.dumps(download(base, token, eid, dest), indent=1))
