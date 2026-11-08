#!/usr/bin/env python3
"""Reviewer's mutations of src/runfmt.rs (review of compaction 2 step 1). Same method as ../mutations.py: apply one,
run the runfmt tests (filter in argv[1], default "runfmt"), restore. Each mutation is a list of (old, new) pairs.
Usage, from the repository root: python3 -I research/c2-step1-runformat/review/mutations_review.py [test filter] [numbers...]"""
import subprocess, sys, pathlib

SRC = pathlib.Path("src/runfmt.rs")
M = [
    ("the block cache ignores the run (block b of run 0 served for run 1)",
     [("let hit = c.get((ri as u32, b as u32));", "let hit = c.get((0, b as u32));"),
      ("c.insert((ri as u32, b as u32), blk.clone());", "c.insert((0, b as u32), blk.clone());")]),
    ("the customer filter is used for ranges that cross customers",
     [("!in_one || self.filter.may_contain(comp)", "self.filter.may_contain(comp)")]),
    ("the writer adds a customer to the filter only when it starts a block",
     [("if c != &self.last_cust[..] {", "if c != &self.last_cust[..] && self.raw.is_empty() {")]),
    ("newest returns one row too many", [("if got.len() == limit {\n                            break;", "if got.len() > limit {\n                            break;")]),
    ("the writer's separator is the previous key", [("Some(_) => separator(&self.last_key, key),", "Some(_) => self.last_key.clone(),")]),
    ("a point read returns the next row for an absent key", [("Ok(((j < b.len() && b.key(j) == key).then(|| b.row(j)), st))", "Ok(((j < b.len()).then(|| b.row(j)), st))")]),
    ("a point read hashes the wrong bytes for the filter", [("if !run.filter.may_contain(&key[r]) {", "if !run.filter.may_contain(&key[r.start + 1..r.end]) {")]),
    ("a range drops its lower bound inside the first block", [("for i in b.lower_bound(lo)..b.bound(hi) {", "for i in 0..b.bound(hi) {")]),
    ("the front-coded fence forgets the shared bytes", [("cur.truncate(shared);", "cur.truncate(shared.saturating_sub(1));")]),
    ("the group search starts one group late", [("let g = lo - 1;", "let g = (lo - 1).max(1).min(self.restarts.len() - 1);")]),
    ("the streaming reader drops a chunk's last block", [("self.next_block = z + 1;", "self.next_block = z + 1;\n        if z > a { self.blocks.pop_back(); }")]),
    ("no fence check of decoded blocks (defence in depth; the CRC also catches)",
     [('anyhow::ensure!(run.fence.cmp(i, b.key(0)) != Ordering::Greater, "corrupt run: block {i} starts below its separator");', ""),
      ('anyhow::ensure!(run.fence.cmp(i + 1, b.key(b.len() - 1)) == Ordering::Greater, "corrupt run: block {i} reaches the next separator");', ""),
      ('anyhow::ensure!(b.key(b.len() - 1) == &run.last_key[..], "corrupt run: last block does not end with the last key");', ""),
      ('anyhow::ensure!(b.key(0) == &run.first_key[..], "corrupt run: first block does not start with the first key");', "")]),
    ("no end-of-run count check in the stream (defence in depth)",
     [('anyhow::ensure!(self.seen == self.run.entries && self.seen_deleted == self.run.deleted, "corrupt run: entry count");', "")]),
    ("the block CRC no longer covers the block index (writer and reader)",
     # updated by the fix of FIX 1: the block CRC is now computed in one place, `block_crc`
     [("crc32c::crc32c_append(crc32c::crc32c_append(crc32c::crc32c(run_id), &idx.to_le_bytes()), c)", "crc32c::crc32c_append(crc32c::crc32c(run_id), c)")]),
]

orig = SRC.read_text()
filt = sys.argv[1] if len(sys.argv) > 1 else "runfmt"
pick = [int(a) for a in sys.argv[2:]] or list(range(1, len(M) + 1))
caught = 0
try:
    for n in pick:
        name, pairs = M[n - 1]
        t = orig
        for old, new in pairs:
            assert t.count(old) == 1, f"mutation {n}: '{old[:50]}' occurs {t.count(old)} times"
            t = t.replace(old, new)
        SRC.write_text(t)
        b = subprocess.run(["cargo", "test", "--release", "--lib", "--no-run"], capture_output=True, text=True)
        try:
            r = subprocess.run(["cargo", "test", "--release", "--lib", filt], capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            print(f"mutation R{n} ({name}): caught as a hang (300 s)", flush=True)
            caught += 1
            subprocess.run(["pkill", "-f", "tenant_bench-"], capture_output=True)
            continue
        failed = [l.split()[1] for l in r.stdout.splitlines() if l.startswith("test ") and l.endswith("FAILED")]
        built = "error[" not in b.stderr + r.stderr and "could not compile" not in b.stderr + r.stderr
        ok = built and r.returncode != 0 and failed
        caught += bool(ok)
        why = f"caught by {len(failed)} test(s): {', '.join(x.split('::')[-1] for x in failed)}" if ok else ("DID NOT BUILD" if not built else "NOT CAUGHT")
        print(f"mutation R{n} ({name}): {why}", flush=True)
finally:
    SRC.write_text(orig)
print(f"MUTATIONS: {caught} of {len(pick)} caught")
