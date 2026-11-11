# Writes 4 KiB + fdatasync every 10 ms on the host disk; prints each fdatasync slower than 20 ms (time base: unix s).
import os, sys, time
p = sys.argv[1]; out = open(sys.argv[2], "a", buffering=1)
fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
buf = b"x" * 4096; i = 0; n = 0; tot = 0.0
while True:
    os.pwrite(fd, buf, (i % 256) * 4096); i += 1
    t = time.time(); os.fdatasync(fd); d = (time.time() - t) * 1000
    n += 1; tot += d
    if d >= 20: out.write(f"{t:.3f} {d:.1f}\n")
    if n % 6000 == 0: out.write(f"# {t:.3f} n={n} mean_ms={tot/n:.2f}\n")
    time.sleep(0.01)
