#!/usr/bin/env python3
"""M17: conteggi per minuto (letture, scritture) da una traccia decompressa letta su stdin.

Uso: zstd -dc file | python3 conta.py <formato> <nome> <out.npz>
formato: og (oracleGeneral, record da 24 byte, senza tipo di operazione) oppure lcs (intestazione 8192 byte,
versione all'offset 8, record v1..v8 come in libCacheSim traceReader/customizedReader/lcs.h).
Salva: m0 (primo minuto, secondi//60 grezzi), r (letture per minuto), w (scritture per minuto), t_first, t_last, n.
Per og tutte le richieste finiscono in r (il formato non dice il tipo).
"""
import sys
import numpy as np

fmt, name, out = sys.argv[1], sys.argv[2], sys.argv[3]
inp = sys.stdin.buffer
LCS_ITEM = {1: 24, 2: 28, 3: 36, 4: 40, 5: 44, 6: 52, 7: 68, 8: 100}

if fmt == "lcs":
    h = inp.read(8192)
    assert len(h) == 8192 and int.from_bytes(h[0:8], "little") == 0x123456789ABCDEF0, "non lcs"
    ver = int.from_bytes(h[8:16], "little")
    item = LCS_ITEM[ver]
    op_off = None if ver == 1 else (16 if ver == 2 else 20)
else:
    ver, item, op_off = 0, 24, None

CH = item * (1 << 20)  # circa 1 milione di record per blocco
counts_r = {}
counts_w = {}
n = 0
t_first = None
t_last = 0
rest = b""


def add(d, keys, cnt):
    for k, c in zip(keys.tolist(), cnt.tolist()):
        d[k] = d.get(k, 0) + c


while True:
    buf = inp.read(CH)
    if not buf:
        break
    buf = rest + buf
    m = len(buf) // item
    rest = buf[m * item:]
    if m == 0:
        continue
    a = np.frombuffer(buf, dtype=np.uint8, count=m * item).reshape(m, item)
    t = a[:, 0:4].copy().view(np.uint32).ravel()
    if t_first is None:
        t_first = int(t[0])
    t_last = max(t_last, int(t.max()))
    minute = (t // 60).astype(np.int64)
    if op_off is not None:
        op = a[:, op_off]  # byte basso del campo op
        isr = (op == 1) | (op == 2) | (op == 12)
    else:
        isr = np.ones(m, dtype=bool)
    for d, mask in ((counts_r, isr), (counts_w, ~isr)):
        if mask.any():
            k, c = np.unique(minute[mask], return_counts=True)
            add(d, k, c)
    n += m

keys = sorted(set(counts_r) | set(counts_w))
if keys:
    m0, m1 = keys[0], keys[-1]
    r = np.zeros(m1 - m0 + 1, dtype=np.int64)
    w = np.zeros(m1 - m0 + 1, dtype=np.int64)
    for k, c in counts_r.items():
        r[k - m0] += c
    for k, c in counts_w.items():
        w[k - m0] += c
else:
    m0, r, w = 0, np.zeros(0, np.int64), np.zeros(0, np.int64)
np.savez_compressed(out, m0=m0, r=r, w=w, t_first=t_first or 0, t_last=t_last, n=n, ver=ver)
print(f"{name} fmt={fmt} v{ver} n={n} minuti={len(r)} t_first={t_first} t_last={t_last} letture={int(r.sum())} scritture={int(w.sum())}")
