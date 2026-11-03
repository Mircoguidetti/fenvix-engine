#!/usr/bin/env python3
"""M26 · simulatore del lavaggio sui soli metadati (taglie e finestre di tempo delle pile).

Riproduce la logica dello schedulatore size-tiered di SlateDB 0.17 (size_tiered_compaction.rs): L0 -> nuovo
sorted run quando ci sono almeno 4 L0 (al massimo 8), unione di run consecutivi di taglia simile (soglia 4x, da 4 a 8
sorgenti), controllo di contropressione, consolidamento oltre `consolida` run, al massimo 4 lavaggi insieme.

Ogni pila ricorda la finestra di tempo [a, c] delle scritture che contiene. La sua taglia viene da lì: con la
ritenzione FIFO (D = taglia / velocità) i dati più vecchi di D sono cancellati, ma le lapidi tolgono i dati solo
quando la pila si unisce con la più vecchia. Questo dà anche l'amplificazione dello spazio.

Varianti oltre a size-tiered:
- "grandi": tetto solo sulle pile grandi (quelle più grandi di taglia/`soglia_grandi`); le piccole restano libere.
- "leveled": lavaggio a livelli (fanout T), chiavi uniformi (dati raggruppati per cliente).

Tempo: il lavaggio ha una capacità di `cap_bps` byte letti al secondo, divisa tra i lavaggi in corso. Lo scrittore si
ferma se le L0 sono 8 o più (come l0_max_ssts di SlateDB).
"""
import argparse, bisect, heapq, json, math, random, sys


class Pila:
    __slots__ = ("id", "a", "c", "size", "tomb")

    def __init__(self, id, a, c, size, tomb=0.0):
        self.id, self.a, self.c, self.size, self.tomb = id, a, c, size, tomb


class Sim:
    def __init__(self, S, w, l0, ovh=1.45, D=None, consolida=0, cap_bps=1e18, conc=4, min_src=4, max_src=8,
                 thr=4.0, l0_max=8, tomb_frac=0.10, modo="tiered", soglia_grandi=64, T=10):
        self.S, self.w, self.l0b, self.ovh = S, w, l0, ovh
        self.D = D if D is not None else S / w  # tempo di vita di un dato (ritenzione FIFO)
        self.consolida, self.cap, self.conc = consolida, cap_bps, conc
        self.min_src, self.max_src, self.thr, self.l0_max = min_src, max_src, thr, l0_max
        self.tomb_frac, self.modo, self.soglia_grandi, self.T = tomb_frac, modo, soglia_grandi, T
        self.t = 0.0
        self.next_id = 0
        self.l0 = []  # dal più nuovo
        self.srs = []  # dal più nuovo
        self.active = []  # [rem_work, set(ids), kind, sources, dst_pos_info]
        self.bytes_written = 0.0
        self.bytes_read = 0.0
        self.n_comp = 0
        self.flush_bytes = 0.0
        self.user_bytes = 0.0

    # taglia di una pila che contiene tutte le scritture tra a e c
    def taglia(self, a, c, include_bottom):
        live = self.w * (c - max(a, c - self.D))
        if include_bottom:
            return self.ovh * live
        # i dati cancellati da lapidi più nuove non spariscono, le lapidi nemmeno: restano finché non si tocca il fondo
        dead_inside = 0.0
        tomb = self.tomb_frac * self.w * max(0.0, c - max(a, self.D))  # lapidi scritte nella finestra
        return self.ovh * (live + dead_inside) + tomb

    def nid(self):
        self.next_id += 1
        return self.next_id

    def busy(self):
        s = set()
        for x in self.active:
            s |= x[1]
        return s

    # ---- size-tiered di SlateDB
    def _compactable(self, srs, i, thr, busy, max_sources=None, check=True):
        out = []
        min_sz = None
        est = 0.0
        for j in range(i, len(srs)):
            if max_sources is not None and len(out) >= max_sources:
                break
            s = srs[j]
            if thr is not None:
                if min_sz is not None:
                    if s.size > min_sz * thr:
                        break
                    min_sz = min(min_sz, s.size)
                else:
                    min_sz = s.size
            if check:
                if s.id in busy:
                    break
                est += s.size
                if not self._bp_ok(est, srs[j + 1] if j + 1 < len(srs) else None, srs):
                    break
            out.append(j)
        return out

    def _bp_ok(self, est, nxt, srs):
        if nxt is None:
            return True
        if nxt.size <= est * self.thr:
            k = srs.index(nxt)
            if len(self._compactable(srs, k, self.thr, set(), check=False)) >= self.max_src:
                return False
        return True

    def propose(self):
        props = []
        while len(self.active) + len(props) < self.conc:
            busy = self.busy()
            for p in props:
                busy |= p[1]
            p = self._pick(busy)
            if p is None:
                break
            props.append(p)
        for p in props:
            self._start(p)

    def _pick(self, busy):
        # L0
        l0free = [x for x in self.l0 if x.id not in busy]
        if len(l0free) == len(self.l0) and len(self.l0) >= self.min_src:
            cand = self.l0[-self.max_src:]  # i più vecchi (clamp_max toglie dal davanti)
            est = sum(x.size for x in cand)
            nxt = self.srs[0] if self.srs else None
            if self._bp_ok(est, nxt, self.srs):
                return ("l0", set(x.id for x in cand), cand)
        if self.modo == "livelli":
            # livelli geometrici: ogni pila si unisce alla successiva (più vecchia) quando è grande almeno 1/T di lei
            for i in range(len(self.srs) - 1):
                a_, b_ = self.srs[i], self.srs[i + 1]
                if a_.id in busy or b_.id in busy:
                    continue
                if a_.size * self.T >= b_.size:
                    return ("sr", {a_.id, b_.id}, [a_, b_])
            return None
        for i in range(len(self.srs)):
            idx = self._compactable(self.srs, i, self.thr, busy)
            if len(idx) >= self.min_src:
                idx = idx[-self.max_src:]
                c = [self.srs[j] for j in idx]
                return ("sr", set(x.id for x in c), c)
        # consolidamento
        projected = len(self.srs) + sum(1 for x in self.active if x[2] == "l0")
        if self.modo == "grandi":
            big = [s for s in self.srs if s.size >= self.S * self.ovh / self.soglia_grandi]
            projected = len(big) + 0
            limit_pool = big
        else:
            limit_pool = self.srs
        if self.consolida > 0 and projected > self.consolida and self.max_src >= 2:
            lim = min(self.max_src, projected - self.consolida + 1)
            for i in range(len(self.srs)):
                if self.modo == "grandi" and self.srs[i] not in limit_pool:
                    continue
                idx = self._compactable(self.srs, i, None, busy, max_sources=lim)
                if len(idx) >= 2:
                    c = [self.srs[j] for j in idx]
                    return ("sr", set(x.id for x in c), c)
        return None

    def _start(self, p):
        kind, ids, srcs = p
        work = sum(x.size for x in srcs)
        self.active.append([work, ids, kind, srcs])

    def _finish(self, act):
        _, ids, kind, srcs = act
        a = min(x.a for x in srcs)
        c = max(x.c for x in srcs)
        if kind == "l0":
            bottom = False
            new = Pila(self.nid(), a, c, 0)
            new.size = self.taglia(a, c, False) if self.srs or True else 0
            # le L0 tolte, nuova pila in testa
            self.l0 = [x for x in self.l0 if x.id not in ids]
            # inserita al posto giusto: le L0 sono più nuove di ogni sorted run
            self.srs.insert(0, new)
        else:
            pos = [i for i, s in enumerate(self.srs) if s.id in ids]
            bottom = pos[-1] == len(self.srs) - 1
            new = Pila(self.nid(), a, c, self.taglia(a, c, bottom))
            self.srs = self.srs[: pos[0]] + [new] + self.srs[pos[-1] + 1:]
        self.bytes_read += sum(x.size for x in srcs)
        self.bytes_written += new.size
        self.n_comp += 1

    # ---- leveled (fanout T, chiavi uniformi): livelli come pile; ogni byte che scende di livello riscrive ~T byte
    def leveled_step(self):
        pass


def run(args):
    S = args.taglia_gb * 2**30
    w = args.w_mbs * 2**20
    l0 = args.l0_mb * 2**20
    sim = Sim(S, w, l0, ovh=args.ovh, consolida=args.consolida, cap_bps=args.cap_mbs * 2**20, conc=args.conc,
              tomb_frac=args.tomb, modo=args.modo, soglia_grandi=args.soglia_grandi, T=args.T)
    dt_flush = l0 / (w * args.ovh)  # tempo-dati per riempire una L0
    n_fill = int(math.ceil(S * args.ovh / l0))
    n_tot = int(n_fill * (1 + args.regime))
    k = 0  # L0 scaricate; la L0 k contiene le scritture del tempo-dati [k, k+1) * dt_flush
    next_flush_wall = dt_flush
    pile_tw = pile_t = spazio_tw = 0.0
    pile_max = 0
    reg = None
    snaps = []
    snap_every = max(1, (n_tot - n_fill) // 200)
    while k < n_tot:
        n = len(sim.active)
        rate = sim.cap / n if n else 0
        t_comp = min((x[0] / rate for x in sim.active), default=math.inf) if n else math.inf
        t_fl = (next_flush_wall - sim.t) if len(sim.l0) < sim.l0_max else math.inf
        dt = max(0.0, min(t_comp, t_fl))
        if dt == math.inf:
            raise RuntimeError("bloccato")
        if reg is not None:
            cnt = len(sim.l0) + len(sim.srs)
            pile_tw += cnt * dt
            pile_t += dt
            pile_max = max(pile_max, cnt)
            # spazio: pile vive più i sorgenti dei lavaggi in corso (le uscite parziali non contano)
            spazio_tw += (sum(x.size for x in sim.srs) + sum(x.size for x in sim.l0)) * dt
        for x in sim.active:
            x[0] -= rate * dt
        sim.t += dt
        if t_comp <= t_fl:
            done = [x for x in sim.active if x[0] <= 1e-3]
            sim.active = [x for x in sim.active if x[0] > 1e-3]
            for x in done:
                sim._finish(x)
        else:
            sim.l0.insert(0, Pila(sim.nid(), k * dt_flush, (k + 1) * dt_flush, l0))
            sim.flush_bytes += l0
            k += 1
            next_flush_wall = max(next_flush_wall + dt_flush, sim.t)
            if k == n_fill:
                reg = (sim.bytes_written, sim.bytes_read, sim.n_comp, sim.t, sim.flush_bytes)
            if reg is not None and (k - n_fill) % snap_every == 0:
                snaps.append([(s.a, s.c, s.size) for s in sim.srs] + [(s.a, s.c, s.size) for s in sim.l0])
        if len(sim.l0) < sim.l0_max and next_flush_wall < sim.t:
            next_flush_wall = sim.t
        sim.propose()
    reg_user = (sim.flush_bytes - reg[4]) / args.ovh
    out = {
        "taglia_gb": args.taglia_gb, "l0_mb": args.l0_mb, "w_mbs": args.w_mbs, "consolida": args.consolida, "modo": args.modo,
        "cap_mbs": args.cap_mbs, "soglia_grandi": args.soglia_grandi, "T": args.T,
        "wa_lavaggio": (sim.bytes_written - reg[0]) / reg_user,
        "wa_lettura": (sim.bytes_read - reg[1]) / reg_user,
        "lavaggi_regime": sim.n_comp - reg[2],
        "pile_media": pile_tw / max(pile_t, 1e-9), "pile_max": pile_max,
        "rallentamento": (sim.t - reg[3]) / ((n_tot - n_fill) * dt_flush),
        "spazio_medio": spazio_tw / max(pile_t, 1e-9) / (S * args.ovh),
        "spazio_fine": (sum(s.size for s in sim.srs) + sum(s.size for s in sim.l0)) / (S * args.ovh),
    }
    if args.snaps:
        out["snaps"] = snaps
        out["dt_flush"] = dt_flush
        out["D"] = sim.D
        out["t_dati_fine"] = k * dt_flush
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--taglia-gb", type=float, default=1)
    ap.add_argument("--w-mbs", type=float, default=5)
    ap.add_argument("--l0-mb", type=float, default=8)
    ap.add_argument("--ovh", type=float, default=1.45)
    ap.add_argument("--consolida", type=int, default=0)
    ap.add_argument("--cap-mbs", type=float, default=1e9)
    ap.add_argument("--conc", type=int, default=4)
    ap.add_argument("--regime", type=float, default=2.0)
    ap.add_argument("--tomb", type=float, default=0.10)
    ap.add_argument("--modo", default="tiered")
    ap.add_argument("--soglia-grandi", type=float, default=64)
    ap.add_argument("--T", type=float, default=10)
    ap.add_argument("--snaps", action="store_true")
    args = ap.parse_args()
    print(json.dumps(run(args)))


if __name__ == "__main__":
    main()
