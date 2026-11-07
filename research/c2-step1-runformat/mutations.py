#!/usr/bin/env python3
"""Re-applies reader mutations to src/runfmt.rs one at a time and runs the runfmt tests: each must make a test fail.

Mutations 1-7 are the 7 reader mutations of the fence-layout review (fenvix-brain
sessions/2026-11-06-review-fence-layout/REPORT.md), mapped onto `RunReader::newest` and its helpers; 8-12 are
new ones for what v1 adds (checksums, the customer filter, the fence lookup).
Usage, from the repository root: python3 -I research/c2-step1-runformat/mutations.py [numbers...]
The file is restored after each mutation (and on exit). Output: one line per mutation, then a summary.
"""
import subprocess, sys, pathlib

SRC = pathlib.Path("src/runfmt.rs")
M = [
    ("stop after the first fetch", "if got.len() >= limit || a == first {", "if true {"),
    ("ignore a run that starts with the customer",
     "(0..self.runs.len()).filter(|&i| below(&self.runs[i].first_key, hi) && &self.runs[i].last_key[..] >= lo)",
     "(0..self.runs.len()).filter(|&i| &self.runs[i].first_key[..] < lo && &self.runs[i].last_key[..] >= lo)"),
    ("read blocks oldest-first", "for b in blocks.iter().rev() {", "for b in blocks.iter() {"),
    ("drop the upper bound (the next customer leaks in)", "let hi = b.bound(e);", "let hi = b.len();"),
    ("drop the lower bound (the previous customer leaks in)", "let lo = b.lower_bound(p);", "let lo = 0;"),
    ("skip the first block", "let a = (last + 1).saturating_sub(self.tail_blocks).max(first);",
     "let a = (last + 1).saturating_sub(self.tail_blocks).max(first + 1);"),
    ("fetch only the last missing block", "if let (Some(&a), Some(&z)) = (miss.first(), miss.last()) {",
     "if let (Some(&a), Some(&z)) = (miss.last(), miss.last()) {"),
    ("no block checksum check", 'anyhow::ensure!(want == got, "corrupt run: block {idx} checksum");', ""),
    ("no footer checksum check", 'anyhow::ensure!(want == got, "corrupt run: footer checksum");', ""),
    ("customer filter answers no for everyone", "if self.bits == 0 {\n            return false;\n        }",
     "if self.bits != u64::MAX {\n            return false;\n        }"),
    ("fence lookup skips the block holding the lower bound", "let first = self.count_below(lo, true).max(1) - 1;",
     "let first = self.count_below(lo, true).min(self.blocks() - 1);"),
    ("point read looks one block too far", "let b = &self.fetch(ri, i - 1, i - 1, &mut st).await?[0];",
     "let b = &self.fetch(ri, i.min(run.fence.blocks() - 1), i.min(run.fence.blocks() - 1), &mut st).await?[0];"),
]

orig = SRC.read_text()
pick = [int(a) for a in sys.argv[1:]] or list(range(1, len(M) + 1))
caught = 0
try:
    for n in pick:
        name, old, new = M[n - 1]
        assert orig.count(old) == 1, f"mutation {n}: the original text occurs {orig.count(old)} times"
        SRC.write_text(orig.replace(old, new))
        r = subprocess.run(["cargo", "test", "--release", "--lib", "runfmt"], capture_output=True, text=True)
        failed = [l.split()[1] for l in r.stdout.splitlines() if l.startswith("test ") and l.endswith("FAILED")]
        built = "error[" not in r.stderr and "could not compile" not in r.stderr
        ok = built and r.returncode != 0 and failed
        caught += bool(ok)
        why = f"caught by {len(failed)} test(s): {', '.join(t.split('::')[-1] for t in failed)}" if ok else ("DID NOT BUILD" if not built else "NOT CAUGHT")
        print(f"mutation {n} ({name}): {why}", flush=True)
finally:
    SRC.write_text(orig)
print(f"MUTATIONS: {caught} of {len(pick)} caught")
