# Samples every thread of a process every 2 ms (from the host): per thread, CPU ticks used and the syscall it sits in.
# Writes one line per sample where the server's runtime worker thread(s) used CPU or sat in a syscall other than
# epoll_wait/futex. Usage: thread_sampler.py PID OUT SECONDS
import os, sys, time
pid, out, secs = int(sys.argv[1]), open(sys.argv[2], "w"), float(sys.argv[3])
def threads():
    r = {}
    for t in os.listdir(f"/proc/{pid}/task"):
        try:
            name = open(f"/proc/{pid}/task/{t}/comm").read().strip()
            st = open(f"/proc/{pid}/task/{t}/stat").read().rsplit(")", 1)[1].split()
            sc = open(f"/proc/{pid}/task/{t}/syscall").read().split()[0]
            r[t] = (name, int(st[11]) + int(st[12]), st[0], sc)
        except Exception:
            pass
    return r
prev = threads(); t0 = time.time()
while time.time() - t0 < secs:
    time.sleep(0.002)
    cur = threads(); now = time.time()
    for t, (name, cpu, state, sc) in cur.items():
        d = cpu - prev.get(t, (name, cpu))[1]
        out.write(f"{now:.3f} {t} {name} {state} {sc} {d}\n")
    prev = cur
