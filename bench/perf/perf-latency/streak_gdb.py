# Watches the server's runtime worker thread from the host; when it has been running (state R) for >= 30 ms in a row,
# takes its stack with gdb. Usage: streak_gdb.py PID BINARY OUT SECONDS
import os, sys, time, subprocess
pid, binary, out, secs = int(sys.argv[1]), sys.argv[2], open(sys.argv[3], "w", buffering=1), float(sys.argv[4])
tid = next(t for t in os.listdir(f"/proc/{pid}/task") if open(f"/proc/{pid}/task/{t}/comm").read().strip() == "tokio-rt-worker")
gdbn = None
r = subprocess.run(["gdb", binary, "-p", str(pid), "-batch", "-ex", "info threads"], capture_output=True, text=True)
for l in r.stdout.splitlines():
    if f"LWP {tid})" in l:
        gdbn = l.strip().lstrip("*").split()[0]
t0 = time.time(); start = None; n = 0
while time.time() - t0 < secs and n < 400:
    st = open(f"/proc/{pid}/task/{tid}/stat").read().rsplit(")", 1)[1].split()[0]
    now = time.time()
    if st == "R":
        start = start or now
        if now - start >= 0.03:
            r = subprocess.run(["gdb", binary, "-p", str(pid), "-batch", "-ex", f"thread apply {gdbn} bt 60"], capture_output=True, text=True)
            fr = [l for l in r.stdout.splitlines() if l.startswith("#")]
            out.write(f"=== t={start:.3f} running_for_ms={(now-start)*1000:.0f}\n" + "\n".join(fr) + "\n")
            n += 1; start = None
    else:
        start = None
    time.sleep(0.002)
