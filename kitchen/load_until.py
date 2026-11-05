"""K2b · run the founder's script `carica` UNCHANGED, but stop it once a given fraction of its target is written.

Why: on Atlas the load of the new databases stopped early (InsufficientDiskSpace) at 29.5-43.9% of the target. The
script gives each document a date that grows with the load (date = start of the fake year + fraction loaded x one
year), so the Atlas databases hold every client's documents only up to a given day of the fake year. To compare our
kitchen one to one, our databases must stop at the same fraction: same clients (same seed), same part of the fake
year, so the empty windows fall in the same places (reviewer of the Ireland corrections, sections 1-3).

How (the script file is not touched; md5 checked): this wrapper imports the script as a module and replaces two of its
globals before calling its own main():
  - RawBSONDocument: same class, but every document the loader builds adds its BSON size to a counter (the script
    builds each document with RawBSONDocument(raw) right before inserting it, in lotto());
  - STOP: the script's stop event; is_set() also becomes true once the counter reaches FRACTION x target. The workers
    check STOP before each batch, so the load stops within one batch per thread (at most THREADS x LOTTO documents,
    about 0.2% of 6.6 GB with 16 threads and 1000 documents: about 0.6 days of the fake year).
The script then saves its state file as usual, with motivo_fine "fermato a mano" (stopped by hand). The fraction
really reached is in the state file (caricamento.byte_bson / target) and is printed at the end.
FRACTION >= 1 runs the script with no stop at all (a full load, like the old database fenvix_p1 on Atlas).

Usage: load_until.py FRACTION carica --db ... --gb ... [the script's own options]
       (in the folder that holds prova_infinite.py and the state file; FENVIX_P1_URI in the environment)"""
import hashlib
import importlib.util
import os
import sys
import threading

SCRIPT_MD5 = "8396ca455557baaa4e3a9b1f881451f9"


def main():
    fraction = float(sys.argv[1])
    args = sys.argv[2:]
    if not args or args[0] != "carica":
        print("load_until.py: only the 'carica' command", file=sys.stderr)
        sys.exit(2)
    path = os.path.join(os.getcwd(), "prova_infinite.py")
    if hashlib.md5(open(path, "rb").read()).hexdigest() != SCRIPT_MD5:
        print("load_until.py: the script is not the founder's (md5)", file=sys.stderr)
        sys.exit(2)
    spec = importlib.util.spec_from_file_location("prova_infinite", path)
    s = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(s)
    gb = None
    for i, x in enumerate(args):
        if x == "--gb" and i + 1 < len(args):
            gb = float(args[i + 1])
    if gb is None:
        print("load_until.py: --gb is required (the fraction is a fraction of it)", file=sys.stderr)
        sys.exit(2)
    target = int(gb * s.GB)
    limit = fraction * target
    built = [0]
    lock = threading.Lock()
    real_raw = s.RawBSONDocument
    real_stop = s.STOP

    def counting_raw(raw, *a, **k):
        with lock:
            built[0] += len(raw)
        return real_raw(raw, *a, **k)

    class FractionStop(threading.Event):
        def is_set(self):
            if real_stop.is_set() or super().is_set():
                return True
            with lock:
                return fraction < 1.0 and built[0] >= limit

    stop = FractionStop()
    # keep the script's own signal handlers working: ferma() calls STOP.set() on this object
    s.STOP = stop
    if fraction < 1.0:
        s.RawBSONDocument = counting_raw
        print(f"load_until: stop at {100 * fraction:.3f}% of {gb:g} GB ({limit / s.GB:.3f} GB of BSON)", flush=True)
    else:
        print(f"load_until: full load of {gb:g} GB (no stop)", flush=True)
    sys.argv = ["prova_infinite.py"] + args
    s.main()
    st_file = None
    for i, x in enumerate(args):
        if x == "--file" and i + 1 < len(args):
            st_file = args[i + 1]
    if st_file and os.path.exists(st_file):
        import json
        car = json.load(open(st_file)).get("caricamento") or {}
        reached = (car.get("byte_bson") or 0) / target
        print(f"load_until: reached {100 * reached:.3f}% (asked {100 * min(fraction, 1.0):.3f}%), "
              f"reason '{car.get('motivo_fine')}'", flush=True)


if __name__ == "__main__":
    main()
