#!/usr/bin/env python3
"""Negative controls of engine-round2: each new check is removed on purpose (a small source change), the engine is rebuilt,
the test file of that check runs against the mutated binary (it must fail), and the source is restored with `git checkout`.
Diffs and outputs go to results-engine-round2/mutations/. usage: mutations.py [name ...]   (default: all)"""
import os, subprocess, sys, shutil
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "results-engine-round2", "mutations"); os.makedirs(OUT, exist_ok=True)
PY = "/tmp/claude-0/venv/bin/python"
BINDIR = "/tmp/claude-0/r2/bin"; os.makedirs(BINDIR, exist_ok=True)
AZ, AUTH, NG = "mongo-server/src/authz.rs", "mongo-server/src/auth.rs", "mongo-server/src/netguard.rs"
T15, T16, T17 = "test_15_admin_cmd_privileges.py", "test_16_block_defaults.py", "test_17_grantable_roles.py"
MUT = {
    # rotateCertificates back to "unknown command" (needs anyAction: nobody but __system)
    "rotate_privilege": (AZ, '        "rotateCertificates" => vec![(cl, "rotateCertificates")],\n', "", T15),
    # tenantStats for any authenticated user
    "tenant_stats_root_only": (AUTH, 'if name == "tenantStats" && !holds_admin_role(', 'if false && name == "tenantStats" && !holds_admin_role(', T15),
    # the per-address threshold back to 10
    "fail_max_100": (NG, "    #[arg(long, default_value_t = 100)]\n    pub auth_fail_max: u32,", "    #[arg(long, default_value_t = 10)]\n    pub auth_fail_max: u32,", T16),
    # the per-(address, database) threshold off
    "fail_max_per_db": (NG, "        if let Some((d, strike)) = t.fail(now, self.cfg.fail_max_db, &self.cfg) {", "        if let Some((d, strike)) = t.fail(now, 0, &self.cfg) {", T16),
    # --grantable-roles: any role passes the list
    "grantable_list": (AUTH, " || !list.iter().any(|x| *x == r) ", " ", T17),
    # --grantable-roles: users may be managed on admin, local, config, $external
    "protected_dbs": (AUTH, "        if PROTECTED_DBS.contains(&db) {\n            return Ok(false);", "        if false && PROTECTED_DBS.contains(&db) {\n            return Ok(false);", T17),
    # --grantable-roles: the caller may hand out its own roles
    "own_role": (AUTH, " || mine.iter().any(|(m, mdb)| *m == r && *mdb == rdb)", "", T17),
    # --grantable-roles: roles of another database may be handed out
    "same_database": (AUTH, "if rdb != db || !list", "if !list", T17),
    # --grantable-roles: roles may be created and changed
    "role_commands": (AUTH, "        if role_cmd {\n            return Ok(false);", "        if false && role_cmd {\n            return Ok(false);", T17),
    # --grantable-roles at startup: any built-in role is accepted in the list (dbOwner, userAdmin, admin roles...)
    "startup_check": (AZ, '        let ok = builtin(x, "grantable_check").map(|(p, _)| confined_without_user_admin(&p, "grantable_check")).unwrap_or(false);',
                      '        let ok = builtin(x, "grantable_check").is_some();', T17),
    # --grantable-roles at run time: a listed role is not checked again for user administration (redundant with startup_check)
    "runtime_power_check": (AUTH, "                Some((privs, _)) if authz::confined_without_user_admin(&privs, &rdb) => {}", "                Some(_) => {}", T17),
    # point 4: the database parameter of DELETE /v1/network/blocks/{ip} is ignored (every block of the address goes)
    "unblock_one_database": ("mongo-server/src/ctl/allowlist.rs", 'netguard::get().unblock_db(addr, db)', 'netguard::get().unblock(addr)', "test_18_unblock_per_database.py"),
}
names = sys.argv[1:] or list(MUT)
summary = []
for name in names:
    f, old, new, tests = MUT[name]
    p = os.path.join(ROOT, f)
    src = open(p).read()
    assert src.count(old) == 1, (name, src.count(old))
    open(p, "w").write(src.replace(old, new))
    try:
        b = subprocess.run("cargo build --release -p mongo-server 2>&1 | tail -3", shell=True, cwd=ROOT, capture_output=True, text=True)
        diff = subprocess.run(["git", "diff", "--", f], cwd=ROOT, capture_output=True, text=True).stdout
    finally:
        subprocess.run(["git", "checkout", "--", f], cwd=ROOT, check=True)
    binp = f"{BINDIR}/mut_{name}"
    shutil.copy(os.path.join(ROOT, "target/release/mongo-server"), binp)
    open(os.path.join(OUT, f"{name}.diff"), "w").write(diff)
    r = subprocess.run(f"{PY} -m pytest -q -p no:cacheprovider --tb=line {tests}", shell=True,
                       cwd=os.path.join(ROOT, "ctl-tests"), env=dict(os.environ, MONGO_SERVER_BIN=binp), capture_output=True, text=True, timeout=3000)
    open(os.path.join(OUT, f"{name}.txt"), "w").write(f"build: {b.stdout}\n" + r.stdout[-30000:] + r.stderr[-3000:])
    last = [l for l in r.stdout.splitlines() if " in " in l and ("passed" in l or "failed" in l or "error" in l)]
    line = f"{name} ({tests}) -> {last[-1] if last else 'NO SUMMARY'}"
    print(line, flush=True); summary.append(line)
    os.remove(binp)
    subprocess.run("find /tmp/minio-data -maxdepth 1 -name 'ctl-*' -mmin +1 -exec rm -rf {} + 2>/dev/null", shell=True)
open(os.path.join(OUT, "summary.txt"), "a").write("\n".join(summary) + "\n")
subprocess.run("cargo build --release -p mongo-server 2>&1 | tail -1", shell=True, cwd=ROOT)
print("source restored:", subprocess.run(["git", "status", "--porcelain", "mongo-server"], cwd=ROOT, capture_output=True, text=True).stdout or "clean")
