import subprocess, sys, os, shutil
os.chdir('/home/user/s6')
M = {
 'M1-trim-drops-last-relied-row': ('src/fx/counter.rs', "Some(Piece { blk: b.slice(i, j), lo, hi,", "Some(Piece { blk: b.slice(i, j.saturating_sub(1).max(i)), lo, hi,"),
 'M2-trim-claims-rows-below': ('src/fx/counter.rs', "let lo = if i == 0 { self.lo.clone() } else { Some(succ(b.key(i - 1)).into_boxed_slice()) };", "let lo = self.lo.clone();"),
 'M3-trim-claims-rows-above': ('src/fx/counter.rs', "let hi = if j >= b.len() { self.hi.clone() } else { Some(b.key(j).to_vec().into_boxed_slice()) };", "let hi = self.hi.clone();"),
 'M4-slice-crosses-customer': ('src/fx/counter.rs', "    if !inside(cust) {\n        return None;\n    }", "    if false && !inside(cust) {\n        return None;\n    }"),
 'M5-part-not-clipped': ('src/fx/view.rs', "        let i = blk.lower_bound(&need_lo);\n        let j = need_hi.as_deref().map_or(blk.len(), |h| blk.lower_bound(h)).max(i);", "        let i = 0;\n        let j = blk.len();"),
 'M6-down-ignores-top': ('src/fx/counter.rs', "Need::Down(lo, hi) => p.covers_up_to(*hi) &&", "Need::Down(lo, hi) => true ||"),
 'M7-staged-claims-rows-below': ('src/fx/counter.rs', "Piece { blk: b.slice(i, j.max(i)), lo: h.lo.clone().map(|x| x.into_boxed_slice()),", "Piece { blk: b.slice(i, j.max(i)), lo: None,"),
 'M8-no-refetch-past-trim': ('src/fx/view.rs', "    fn beyond(&self, reverse: bool) -> Option<(Vec<u8>, Option<Vec<u8>>)> {\n", "    fn beyond(&self, reverse: bool) -> Option<(Vec<u8>, Option<Vec<u8>>)> {\n        if true { return None; }\n"),
 'M9-filter-says-no-to-present': ('src/fx/view.rs', "                        if !run.filter.may_contain(&self.lo[r]) {", "                        if !run.filter.may_contain(&self.lo[r]) || self.lo.len() % 7 == 0 {"),
}
names = sys.argv[1:] or list(M)
for name in names:
    f, a, b = M[name]
    src = open(f).read()
    assert src.count(a) == 1, (name, src.count(a))
    open(f, 'w').write(src.replace(a, b))
    try:
        res = {}
        for tn, env in [('fx::counter_tests::slices_are_exact_under_writes_and_reseals', {}),
                        ('fx::counter_tests::staged_publish_keeps_hot_customers_warm_across_a_reseal', {}),
                        ('fx::tests::model_publish_every_100_ops_with_crashes', {'FX_MODEL_OPS': '20000', 'FX_SLICES': '2', 'FX_TRIM_MS': '2'})]:
            e = dict(os.environ, **env)
            p = subprocess.run(['cargo', 'test', '--release', '-p', 'tenant-bench', '--lib', tn, '--', '--exact', '--nocapture'], env=e, capture_output=True, text=True, timeout=3000)
            out = p.stdout + p.stderr
            open(f'/home/user/rv/mut/{name}--{tn.split("::")[-1]}.log', 'w').write(out)
            res[tn.split('::')[-1]] = 'CAUGHT' if p.returncode != 0 else 'missed'
            if 'error[E' in out: res[tn.split('::')[-1]] = 'COMPILE ERROR'
        print(name, res, flush=True)
    finally:
        open(f, 'w').write(src)
