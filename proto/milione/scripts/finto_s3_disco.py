"""Il finto S3 del repo (ctl-tests/fake_aws.py, FakeS3) con i corpi degli oggetti su disco invece che in RAM.

Perché: un milione di database da circa 10 KB sono circa 10 GB, la macchina ha 7 GB di RAM. Tutto il resto è il FakeS3 dei
ctl-tests così com'è (firma SigV4 controllata, PUT condizionali If-None-Match / If-Match, GET a intervallo, DELETE, LIST v2).
Cambia solo:
  * `objects`: una mappa su disco, root/<bucket>/<primi 2 caratteri>/<resto della chiave con "/" scritto "~">; l'etag è (mtime in ns, dimensione) del file, quindi non serve
    nessuna tabella in RAM e i file scritti direttamente sul disco (precarico) hanno un etag coerente;
  * il registro delle richieste (`log`) non tiene le righe (un milione di righe in RAM), conta solo per metodo;
  * TCP_NODELAY sulle connessioni (senza, ogni risposta aspetta circa 40 ms);
  * LIST: legge solo la cartella del prefisso invece di ordinare tutte le chiavi del bucket.
Uso: python3 finto_s3_disco.py <cartella> <porta>   (credenziali minioadmin/minioadmin, come il MinIO dei test)
"""
import os
import sys
import threading
import time
from http.server import ThreadingHTTPServer

QUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(QUI, "..", "..", "..", "ctl-tests"))
import fake_aws  # noqa: E402


def etag_di(st):
    return '"%x%08x"' % (st.st_mtime_ns, st.st_size)


class Disco:
    def __init__(self, root):
        self.root = root

    def _p(self, k):
        # chiave "ab/resto/della/chiave" -> root/bucket/ab/resto~della~chiave: una cartella per i primi 2 caratteri
        # (256 cartelle) e nessuna cartella per database, per stare nel disco (ogni cartella occupa 4 KB)
        b, key = k
        testa, _, resto = key.partition("/")
        return os.path.join(self.root, b, testa, resto.replace("/", "~")) if resto else os.path.join(self.root, b, "_", testa)

    def get(self, k, default=None):
        p = self._p(k)
        try:
            with open(p, "rb") as f:
                data = f.read()
                st = os.fstat(f.fileno())
        except (FileNotFoundError, NotADirectoryError, IsADirectoryError):
            return default
        return (data, etag_di(st), st.st_mtime)

    def __getitem__(self, k):
        v = self.get(k)
        if v is None:
            raise KeyError(k)
        return v

    def __contains__(self, k):
        return os.path.isfile(self._p(k))

    def pop(self, k, default=None):
        try:
            os.remove(self._p(k))
            return True
        except (FileNotFoundError, NotADirectoryError, IsADirectoryError):
            return default

    def scrivi(self, k, data):
        p = self._p(k)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = "%s.tmp%d" % (p, threading.get_ident())
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, p)
        return etag_di(os.stat(p))

    def __setitem__(self, k, v):
        self.scrivi(k, v[0])

    def chiavi(self, bucket, prefix):
        """Chiavi sotto `prefix`: si cammina solo dalla cartella più lunga contenuta nel prefisso."""
        base = os.path.join(self.root, bucket)
        testa, sl, resto = prefix.partition("/")
        teste = [testa] if sl else [d for d in os.listdir(base) if d.startswith(testa)]
        out = []
        for t in teste:
            try:
                fs = os.listdir(os.path.join(base, t))
            except FileNotFoundError:
                continue
            for f in fs:
                if ".tmp" in f:
                    continue
                k = f if t == "_" else t + "/" + f.replace("~", "/")
                if k.startswith(prefix):
                    out.append(k)
        return out


class Contalog(list):
    def __init__(self):
        super().__init__()
        self.per = {}

    def append(self, e):
        k = e["method"]
        self.per[k] = self.per.get(k, 0) + 1


class Handler(fake_aws._S3Handler):
    def setup(self):
        # TCP_NODELAY: il FakeS3 scrive intestazioni e corpo in due write; senza, Nagle più l'ACK ritardato aggiungono
        # circa 40 ms a ogni risposta (misurato: risvegli a 48-93 ms in locale)
        super().setup()
        import socket
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    @staticmethod
    def _store(o, bucket, key, data):
        return o.objects.scrivi((bucket, key), data)

    def _list(self, o, bucket, q):
        # come FakeS3._list, ma le chiavi vengono da una sola cartella
        orig = o.objects
        prefix = q.get("prefix", "")
        keys = orig.chiavi(bucket, prefix)
        class Vista(dict):
            pass
        v = Vista()
        for k in keys:
            v[(bucket, k)] = orig.get((bucket, k))
        o.objects = v
        try:
            return fake_aws._S3Handler._list(self, o, bucket, q)
        finally:
            o.objects = orig


class FakeS3Disco(fake_aws.FakeS3):
    def __init__(self, root, port):
        self.registry = fake_aws.Registry()
        self.registry.add("minioadmin", "minioadmin", None, time.time(), time.time() + 10 * 365 * 86400)
        self.region = "us-east-1"
        self.lock = threading.RLock()
        self.objects = Disco(root)
        self.uploads = {}
        self.log = Contalog()
        self.unsupported = []
        srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        srv.daemon_threads = True
        srv.owner = self
        self.srv = srv


if __name__ == "__main__":
    root, port = sys.argv[1], int(sys.argv[2])
    os.makedirs(os.path.join(root, "milione"), exist_ok=True)
    s = FakeS3Disco(root, port)
    def stampa():
        while True:
            time.sleep(10)
            print(time.strftime("%H:%M:%S"), dict(s.log.per), "non supportate:", len(s.unsupported), flush=True)
    threading.Thread(target=stampa, daemon=True).start()
    print("finto S3 su disco pronto", root, port, flush=True)
    s.srv.serve_forever()
