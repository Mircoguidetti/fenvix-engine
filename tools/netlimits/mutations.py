#!/usr/bin/env python3
"""Negative controls of the network limits: each control is removed on purpose (a one-line source change), the engine is
rebuilt, the new test files run against that binary (they must fail), and the source is restored with `git checkout`.
The mutated binaries are kept in /tmp/claude-0/nl/bin/mut_<name> only while their tests run.
usage: mutations.py [name ...]   (default: all)"""
import os, subprocess, sys, shutil
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "results-network-limits", "mutations"); os.makedirs(OUT, exist_ok=True)
PY = "/tmp/claude-0/venv/bin/python"
NG, AUTH = "mongo-server/src/netguard.rs", "mongo-server/src/auth.rs"
MUT = {
    # the address check against the database's list: every address passes
    "ip_check": (NG, "if ip.map(|ip| l.iter().any(|n| n.contains(ip))).unwrap_or(false) {", "if true {"),
    # the blocks: no address is ever blocked
    "block": (NG, "        if self.is_trusted(ip) {\n            return false;\n        }\n        let now = Instant::now();\n        let m = self.clients.lock().unwrap();",
              "        if true {\n            return false;\n        }\n        let now = Instant::now();\n        let m = self.clients.lock().unwrap();"),
    # the per-address connection limit
    "conn_limit": (NG, "if limited && c.conns >= self.cfg.max_conns {", "if false && limited && c.conns >= self.cfg.max_conns {"),
    # the handshake timeout
    "handshake_timeout": (NG, "    pub fn handshake_timeout(&self) -> Option<Duration> {\n        self.cfg.handshake", "    pub fn handshake_timeout(&self) -> Option<Duration> {\n        None"),
    # the message cap before authentication
    "pre_auth_size": (NG, "    pub fn pre_auth_max(&self) -> usize {\n        self.cfg.pre_auth_max", "    pub fn pre_auth_max(&self) -> usize {\n        usize::MAX"),
    # a refusal that says why (instead of looking like a wrong password)
    "distinct_refusal": (AUTH, "        let fail = |this: &Self|",
                         "        if refused == Some(crate::netguard::Verdict::NotInList) {\n            return Err(MongoError::new(18, \"AuthenticationFailed\", \"address not allowed\".into()));\n        }\n        let fail = |this: &Self|"),
}
names = sys.argv[1:] or list(MUT)
for name in names:
    f, old, new = MUT[name]
    p = os.path.join(ROOT, f)
    src = open(p).read()
    assert src.count(old) == 1, (name, src.count(old))
    open(p, "w").write(src.replace(old, new))
    try:
        b = subprocess.run("cargo build --release -p mongo-server 2>&1 | tail -3", shell=True, cwd=ROOT, capture_output=True, text=True)
        diff = subprocess.run(["git", "diff", "--", f], cwd=ROOT, capture_output=True, text=True).stdout
    finally:
        subprocess.run(["git", "checkout", "--", f], cwd=ROOT, check=True)
    binp = f"/tmp/claude-0/nl/bin/mut_{name}"
    shutil.copy(os.path.join(ROOT, "target/release/mongo-server"), binp)
    open(os.path.join(OUT, f"{name}.diff"), "w").write(diff)
    r = subprocess.run(f"{PY} -m pytest -q -p no:cacheprovider --tb=line test_13_ip_allowlist.py test_14_auth_limits.py", shell=True,
                       cwd=os.path.join(ROOT, "ctl-tests"), env=dict(os.environ, MONGO_SERVER_BIN=binp), capture_output=True, text=True, timeout=3000)
    open(os.path.join(OUT, f"{name}.txt"), "w").write(r.stdout[-20000:] + r.stderr[-3000:])
    last = [l for l in r.stdout.splitlines() if " in " in l and ("passed" in l or "failed" in l)]
    print(name, "->", last[-1] if last else "NO SUMMARY", flush=True)
    os.remove(binp)
    subprocess.run("find /tmp/minio-data -maxdepth 1 -name 'ctl-*' -mmin +1 -exec rm -rf {} + 2>/dev/null", shell=True)
# the source is back as committed: rebuild the real binary
subprocess.run("cargo build --release -p mongo-server 2>&1 | tail -1", shell=True, cwd=ROOT)
print("source restored:", subprocess.run(["git", "status", "--porcelain", "mongo-server"], cwd=ROOT, capture_output=True, text=True).stdout or "clean")
