"""One customer with ~MB MiB of documents moved into our runs; then a cold server in a 1 GiB cgroup reads one document of
that customer. mode=load|read."""
import os, sys, time, subprocess, threading
sys.path.insert(0, "/home/user/bs/ctl-tests")
from lib import Server, make_bucket
mode, bucket, mb, box = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
X = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000",
     "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--memory-limit-mb", "1024"]
if mode == "load":
    s = Server(bucket, "rvbigL", idle=600, extra=X, env={"FENVIX_TEST_DROP": "1"}).start()
    db = s.client().app
    blob = "y" * (256 * 1024)
    n = mb * 4
    for k in range(0, n, 16):
        db.checkpoints.insert_many([{"_id": f"big-{j:06d}", "thread_id": "big", "v": j, "b": blob + str(j)} for j in range(k, min(n, k + 16))])
    db.checkpoints.insert_many([{"_id": f"small-{j}", "thread_id": "small", "v": j} for j in range(10)])
    st, r = s.admin.post("/v1/test/databases/app/publish-drop", {})
    print("load done", st, r, flush=True)
    s.term()
else:
    CG = "/sys/fs/cgroup/memory/process_api/01a1d26d-94b2-743d-9610-bceb6f35d0dc/claude-code-bash/rv1g"
    open(CG + "/memory.max_usage_in_bytes", "w").write("0")
    x = X + ([] if box == "on" else ["--customer-box", "false"])
    s = Server(bucket, "rvbigR", idle=600, extra=x, env={"FENVIX_TEST_DROP": "1"})
    s.start(wait=False)
    open(CG + "/cgroup.procs", "w").write(str(s.proc.pid))
    s.wait_ready()
    db = s.client().app
    pid = s.proc.pid
    peak = [0]
    stop = [False]
    def watch():
        while not stop[0]:
            try:
                r = int(open(f"/proc/{pid}/status").read().split("VmRSS:")[1].split()[0]) // 1024
                peak[0] = max(peak[0], r)
            except Exception:
                return
            time.sleep(0.05)
    threading.Thread(target=watch, daemon=True).start()
    t0 = time.time()
    try:
        d = db.checkpoints.find_one({"thread_id": "big", "_id": "big-000005"})
        res = "ok" if d and d["v"] == 5 else f"wrong {d and d.get('v')}"
    except Exception as e:
        res = f"error {type(e).__name__}: {str(e)[:120]}"
    time.sleep(1)
    stop[0] = True
    alive = s.proc.poll() is None
    oom = [l for l in open(CG + "/memory.oom_control").read().splitlines() if l.startswith("oom_kill ")]
    print(f"READ box={box}: {res} in {time.time()-t0:.1f}s, server alive {alive} (exit {s.proc.returncode}), RSS peak {peak[0]} MiB, cgroup max usage {int(open(CG + '/memory.max_usage_in_bytes').read()) >> 20} MiB, {oom}", flush=True)
    try:
        s.term()
    except Exception:
        pass
