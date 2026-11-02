"""Finto S3 su disco, solo per le prove della cucitura (M11).

Perché esiste: MinIO non si scarica da questa macchina (il proxy risponde 403 a dl.min.io) e non si può compilare;
il finto S3 dei ctl-tests (ctl-tests/fake_aws.py) non ha UploadPartCopy; moto (pip) ha UploadPartCopy con
x-amz-copy-source-range e rifiuta le parti sotto i 5 MB come S3, ma tiene in RAM l'oggetto intero quando chiude una
multipart: con oggetti da 1 GB è stato ucciso per memoria (4,6 GB, MISURATO). Questo server tiene tutto su file.

Fa solo le chiamate del banco, in stile path (/bucket/chiave), senza controllare le firme:
PUT bucket; PUT, GET (anche Range "a-b" e "-n"), HEAD, DELETE di un oggetto; CreateMultipartUpload, UploadPart,
UploadPartCopy (x-amz-copy-source + x-amz-copy-source-range), CompleteMultipartUpload (rifiuta con EntityTooSmall le
parti sotto i 5 MiB che non sono l'ultima, come S3), AbortMultipartUpload. GET /__stats dà i contatori lato server.

Uso: python3 finto_s3.py --dir /percorso/dati --porta 9000
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import threading
import time
import urllib.parse
import uuid
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MIN_PARTE = 5 << 20
BUF = 4 << 20
STATS = {}
LOCK = threading.Lock()


def conta(k, n=1):
    with LOCK:
        STATS[k] = STATS.get(k, 0) + n


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    root = "."

    def log_message(self, *a):
        pass

    # --- percorsi
    def _parse(self):
        u = urllib.parse.urlsplit(self.path)
        q = dict(urllib.parse.parse_qsl(u.query, keep_blank_values=True))
        parts = urllib.parse.unquote(u.path).lstrip("/").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key, q

    def _obj(self, bucket, key):
        return os.path.join(self.root, bucket, urllib.parse.quote(key, safe=""))

    def _mp(self, uid):
        return os.path.join(self.root, ".mp", uid)

    # --- risposte
    def _send(self, code, body=b"", headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _err(self, code, name, msg=""):
        x = f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{name}</Code><Message>{msg}</Message></Error>".encode()
        self._send(code, x, {"Content-Type": "application/xml"})

    def _body_to(self, f):
        n = int(self.headers.get("Content-Length", "0"))
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            raise RuntimeError("chunked non supportato")
        h = hashlib.md5()
        left = n
        while left > 0:
            b = self.rfile.read(min(BUF, left))
            if not b:
                raise RuntimeError("corpo corto")
            f.write(b)
            h.update(b)
            left -= len(b)
        return n, h.hexdigest()

    def _body(self):
        n = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(n) if n else b""

    # --- metodi
    def do_PUT(self):
        bucket, key, q = self._parse()
        if not key:
            os.makedirs(os.path.join(self.root, bucket), exist_ok=True)
            self._body()
            return self._send(200)
        if "uploadId" in q:
            d = self._mp(q["uploadId"])
            if not os.path.isdir(d):
                self._body()
                return self._err(404, "NoSuchUpload")
            pn = int(q["partNumber"])
            dst = os.path.join(d, f"{pn:05d}")
            src = self.headers.get("x-amz-copy-source")
            if src:
                self._body()
                sb, sk = urllib.parse.unquote(src).lstrip("/").split("/", 1)
                sp = self._obj(sb, sk)
                if not os.path.exists(sp):
                    return self._err(404, "NoSuchKey")
                tot = os.path.getsize(sp)
                r = self.headers.get("x-amz-copy-source-range")
                a, z = 0, tot - 1
                if r:
                    m = re.match(r"bytes=(\d+)-(\d+)$", r)
                    a, z = int(m.group(1)), int(m.group(2))
                    if z >= tot or a > z:
                        return self._err(416, "InvalidRange")
                h = hashlib.md5()
                with open(sp, "rb") as fi, open(dst + ".tmp", "wb") as fo:
                    fi.seek(a)
                    left = z - a + 1
                    while left > 0:
                        b = fi.read(min(BUF, left))
                        fo.write(b)
                        h.update(b)
                        left -= len(b)
                os.replace(dst + ".tmp", dst)
                conta("upload_part_copy")
                conta("byte_copiati", z - a + 1)
                et = '"' + h.hexdigest() + '"'
                with open(dst + ".etag", "w") as f:
                    f.write(et)
                x = f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyPartResult><ETag>{et}</ETag><LastModified>{time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())}</LastModified></CopyPartResult>"
                return self._send(200, x.encode(), {"Content-Type": "application/xml"})
            with open(dst + ".tmp", "wb") as f:
                n, md5 = self._body_to(f)
            os.replace(dst + ".tmp", dst)
            et = '"' + md5 + '"'
            with open(dst + ".etag", "w") as f:
                f.write(et)
            conta("upload_part")
            conta("byte_caricati", n)
            return self._send(200, b"", {"ETag": et})
        p = self._obj(bucket, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + "." + uuid.uuid4().hex + ".tmp"
        with open(tmp, "wb") as f:
            n, md5 = self._body_to(f)
        os.replace(tmp, p)
        conta("put")
        conta("byte_caricati", n)
        self._send(200, b"", {"ETag": '"' + md5 + '"'})

    def do_POST(self):
        bucket, key, q = self._parse()
        if "uploads" in q:
            self._body()
            uid = uuid.uuid4().hex
            os.makedirs(self._mp(uid))
            with open(os.path.join(self._mp(uid), "chiave"), "w") as f:
                f.write(json.dumps([bucket, key]))
            conta("create_multipart")
            x = f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><UploadId>{uid}</UploadId></InitiateMultipartUploadResult>"
            return self._send(200, x.encode(), {"Content-Type": "application/xml"})
        if "uploadId" in q:
            body = self._body().decode()
            d = self._mp(q["uploadId"])
            if not os.path.isdir(d):
                return self._err(404, "NoSuchUpload")
            nums = [int(x) for x in re.findall(r"<PartNumber>(\d+)</PartNumber>", body)]
            files = [os.path.join(d, f"{n:05d}") for n in nums]
            for i, f in enumerate(files):
                if not os.path.exists(f):
                    return self._err(400, "InvalidPart")
                if i + 1 < len(files) and os.path.getsize(f) < MIN_PARTE:
                    conta("entity_too_small")
                    return self._err(400, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.")
            p = self._obj(bucket, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            tmp = p + "." + uuid.uuid4().hex + ".tmp"
            with open(tmp, "wb") as fo:
                for f in files:
                    with open(f, "rb") as fi:
                        shutil.copyfileobj(fi, fo, BUF)
            os.replace(tmp, p)
            shutil.rmtree(d, ignore_errors=True)
            conta("complete_multipart")
            et = '"' + hashlib.md5(q["uploadId"].encode()).hexdigest() + f'-{len(files)}"'
            x = f"<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>{et}</ETag></CompleteMultipartUploadResult>"
            return self._send(200, x.encode(), {"Content-Type": "application/xml"})
        self._body()
        self._err(400, "NonSupportato")

    def do_GET(self, head=False):
        bucket, key, q = self._parse()
        if bucket == "__stats":
            with LOCK:
                b = json.dumps(STATS).encode()
            return self._send(200, b, {"Content-Type": "application/json"})
        p = self._obj(bucket, key)
        if not key or not os.path.exists(p):
            return self._err(404, "NoSuchKey")
        tot = os.path.getsize(p)
        hdr = {"Last-Modified": formatdate(os.path.getmtime(p), usegmt=True), "ETag": '"%x"' % int(os.path.getmtime(p) * 1e6), "Accept-Ranges": "bytes"}
        r = self.headers.get("Range")
        a, z, code = 0, tot - 1, 200
        if r:
            m = re.match(r"bytes=(\d*)-(\d*)$", r)
            if m.group(1) == "":
                n = int(m.group(2))
                a, z = max(0, tot - n), tot - 1
            else:
                a = int(m.group(1))
                z = min(int(m.group(2)), tot - 1) if m.group(2) else tot - 1
            if a > z:
                return self._err(416, "InvalidRange")
            code = 206
            hdr["Content-Range"] = f"bytes {a}-{z}/{tot}"
        n = z - a + 1
        self.send_response(code)
        for k, v in hdr.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(n))
        self.end_headers()
        if head:
            conta("head")
            return
        conta("get")
        conta("byte_scaricati", n)
        with open(p, "rb") as f:
            f.seek(a)
            left = n
            while left > 0:
                b = f.read(min(BUF, left))
                self.wfile.write(b)
                left -= len(b)

    def do_HEAD(self):
        self.do_GET(head=True)

    def do_DELETE(self):
        bucket, key, q = self._parse()
        if "uploadId" in q:
            shutil.rmtree(self._mp(q["uploadId"]), ignore_errors=True)
            return self._send(204)
        try:
            os.remove(self._obj(bucket, key))
        except FileNotFoundError:
            pass
        conta("delete")
        self._send(204)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--porta", type=int, default=9000)
    a = ap.parse_args()
    os.makedirs(os.path.join(a.dir, ".mp"), exist_ok=True)
    H.root = a.dir
    s = ThreadingHTTPServer(("127.0.0.1", a.porta), H)
    s.daemon_threads = True
    s.serve_forever()


if __name__ == "__main__":
    main()
