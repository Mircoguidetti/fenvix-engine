"""A TCP forwarder in front of the object store that can be frozen: while frozen it accepts connections but forwards no
byte in either direction (the store "hangs", it does not refuse), as a store that stopped answering. Used by
tools/lease-stall/repro.py and ctl-tests/test_31_lease_lost_store_down.py.

    p = BlockProxy(("127.0.0.1", 9000)); p.endpoint -> "http://127.0.0.1:<port>"; p.freeze(); p.thaw()
"""
import socket
import threading
import time


class BlockProxy:
    def __init__(self, dst, port=0):
        self.dst = dst
        self.frozen = threading.Event()
        self.ls = socket.socket()
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", port))
        self.ls.listen(512)
        self.port = self.ls.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def freeze(self):
        self.frozen.set()

    def thaw(self):
        self.frozen.clear()

    def _wait_thawed(self):
        while self.frozen.is_set():
            time.sleep(0.05)

    def _accept(self):
        while True:
            try:
                c, _ = self.ls.accept()
            except OSError:
                return
            threading.Thread(target=self._conn, args=(c,), daemon=True).start()

    def _conn(self, c):
        self._wait_thawed()
        try:
            u = socket.create_connection(self.dst)
        except OSError:
            c.close()
            return
        threading.Thread(target=self._pipe, args=(c, u), daemon=True).start()
        threading.Thread(target=self._pipe, args=(u, c), daemon=True).start()

    def _pipe(self, a, b):
        try:
            while True:
                d = a.recv(65536)
                if not d:
                    break
                self._wait_thawed()
                b.sendall(d)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
