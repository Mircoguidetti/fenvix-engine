"""Prova locale v3 · i programmi veri rimpiccioliti: stessi passi, stesse fasi, stessi tipi di passo, numeri piccoli e
pause corte (il server finto sta sulla stessa macchina da 2 vCPU). Uso: rimpicciolisci.py PROGRAMMI_VERI DESTINAZIONE"""
import os
import re
import sys

src, dst = sys.argv[1], sys.argv[2]
SOST = [(r"--gb \S+", "--gb 0.03 --mb-cliente 0.03"), (r"--thread 16", "--thread 2"),
        (r"--grandi-gib \S+", "--grandi-gib 0.004"), (r"--grandi-clienti 1320", "--grandi-clienti 80"),
        (r"--grandi-thread 8", "--grandi-thread 2"),
        (r"(--n-q\w+) 300", r"\1 6"), (r"(--dorm-q\w+) 300", r"\1 6"), (r"(--n-q\w+) 46", r"\1 3"),
        (r"(--dorm-q\w+) [34]\b", r"\1 1"), (r"(--grandi-n-q\w) 300", r"\1 5"), (r"(--grandi-n-q\w) 88", r"\1 3"),
        (r"--n-condizione 300", "--n-condizione 4"), (r"--livelli 10,50,100", "--livelli 2,4,6"),
        (r"--par-lotti 30,6,3", "--par-lotti 2,1,1"), (r"--n-cancella 20", "--n-cancella 2"),
        (r"^massimo (\S+) \d+ ", r"massimo \1 3 ")]
CORTE = (" --pausa-s 1 --min-secondi-fase 2 --min-secondi-condizione 2 --par-secondi 2 --n-q5 6 --n-spiega 2"
         " --minuti-volume 0.05 --pausa-volume-s 1 --n-scritture 10 --n-lotti-scritture 3 --sfondo-riscaldamento-s 1")
MASSIMO = " --minuti-massimo 0.1 --pausa-volume-s 1 --thread-massimo 4 --pausa-s 1"
for prog in sorted(os.listdir(src)):
    os.makedirs(os.path.join(dst, prog), exist_ok=True)
    out = []
    for r in open(os.path.join(src, prog, "passi.txt")):
        r = r.rstrip("\n")
        if r.strip() and not r.lstrip().startswith("#"):
            for a, b in SOST:
                r = re.sub(a, b, r)
            t = r.lstrip("& ").split()[0]
            if t in ("giro", "dormienti"):
                r += CORTE
            elif t == "massimo":
                r += MASSIMO
        out.append(r)
    open(os.path.join(dst, prog, "passi.txt"), "w").write("\n".join(out) + "\n")
    open(os.path.join(dst, prog, "macchina.txt"), "w").write(open(os.path.join(src, prog, "macchina.txt")).read())
print("programmi rimpiccioliti:", ", ".join(sorted(os.listdir(dst))))
