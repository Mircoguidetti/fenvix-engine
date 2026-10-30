"""Tests for tools/compat-page/build.py.   python3 -m unittest discover -s tools/compat-page/tests"""
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.dirname(HERE)
ROOT = os.path.dirname(os.path.dirname(TOOL))
sys.path.insert(0, TOOL)
import build  # noqa: E402

LINE = re.compile(r"^(.*?)\s+(\d+)/(\d+)\s+[\d.]+%\s+(\d+)\s+(\d+)\s+(\d+)$")


def coverage_py(*args):
    out = subprocess.run([sys.executable, os.path.join(ROOT, "compat", "coverage.py"), *args],
                         capture_output=True, text=True, check=True).stdout
    table = {}
    for line in out.splitlines():
        m = LINE.match(line)
        if m and not line.startswith("category"):
            table[m.group(1).strip()] = tuple(int(m.group(i)) for i in range(2, 7))  # covered,total,partial,untested,missing
    ov = re.search(r"(\d+)/(\d+) covered = ([\d.]+)%   OPEN (\d+) \(partial (\d+), untested (\d+), missing (\d+)\)", out)
    return table, tuple(map(float, ov.groups())) if ov else None


class BuildTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model, cls.rows, cls.byid = build.build_model()
        cls.md = build.render_md(cls.model, cls.rows, cls.byid)
        cls.js = build.render_json(cls.model)
        with open(os.path.join(ROOT, "compat", "inventory.csv"), newline="") as f:
            cls.csv = {r["id"]: r for r in csv.DictReader(f)}

    def test_overall_counts_equal_coverage_py(self):
        _, ov = coverage_py()
        covered, total, pct, partial_, untested, missing = ov[0], ov[1], ov[2], ov[4], ov[5], ov[6]
        a, s = self.model["audit_rule_covered"], self.model["summary"]
        self.assertEqual((a["covered"], a["total"]), (covered, total))
        self.assertEqual(a["percent"], build.pct(int(covered), int(total)))
        self.assertLessEqual(a["percent"], pct)  # never rounded up
        self.assertEqual(s["total"], total)
        self.assertEqual(s["partial"], partial_)
        self.assertEqual(s["not_supported"], missing)
        # Not tested = untested rows + covered rows that rest on specification tests only
        self.assertEqual(s["not_tested"], untested + a["specification_only"])
        self.assertEqual(s["supported"], covered - a["specification_only"])
        self.assertEqual(s["supported"] + s["partial"] + s["not_tested"] + s["not_supported"], s["total"])

    def test_category_counts_equal_coverage_py(self):
        detail, _ = coverage_py("--detail")
        mine = {c["category"]: (c["covered"], c["total"], c["partial"], c["untested"], c["missing"]) for c in self.model["categories"]}
        self.assertEqual(mine, detail)
        top, _ = coverage_py()
        agg = {}
        for k, v in detail.items():
            agg[k.split(" / ")[0]] = tuple(x + y for x, y in zip(agg.get(k.split(" / ")[0], (0,) * 5), v))
        self.assertEqual(agg, top)

    def test_area_counts_equal_coverage_py(self):
        detail, _ = coverage_py("--detail")
        expect = {}
        for cat, (cov, tot, par, unt, mis) in detail.items():
            e = expect.setdefault(build.area_of(cat), [0, 0, 0, 0, 0])
            for i, v in enumerate((cov, tot, par, unt, mis)):
                e[i] += v
        spec = {}
        for r in self.rows:
            if r["engine_status"] == "covered" and r["differential_test"] == "no":
                spec[r["_area"]] = spec.get(r["_area"], 0) + 1
        for ar in self.model["areas"]:
            cov, tot, par, unt, mis = expect.get(ar["id"], [0] * 5)
            c, sp = ar["counts"], spec.get(ar["id"], 0)
            self.assertEqual((c["total"], c["partial"], c["not_supported"]), (tot, par, mis), ar["id"])
            self.assertEqual(c["supported"], cov - sp, ar["id"])
            self.assertEqual(c["not_tested"], unt + sp, ar["id"])
        self.assertEqual(sum(a["counts"]["total"] for a in self.model["areas"]), self.model["summary"]["total"])

    def test_supported_features_have_evidence(self):
        sup = [f for f in self.model["features"] if f["status"] == "supported"]
        self.assertGreater(len(sup), 0)
        for f in sup:
            row = self.csv[f["evidence_id"]]
            self.assertTrue(row["evidence"].strip(), f["id"])
            self.assertEqual(row["differential_test"], "yes", f["id"])
            self.assertEqual(row["engine_status"], "covered", f["id"])
        # no row marked Supported without a comparison test, and none excluded is listed
        for f in self.model["features"]:
            self.assertEqual(self.csv[f["id"]]["exclusion_approved"], "no")
            if f["status"] == "not_tested" and f["audit_status"] == "covered":
                self.assertEqual(self.csv[f["id"]]["differential_test"], "no")

    def test_json_validates_and_is_documented(self):
        doc = json.loads(self.js)
        self.assertEqual(build.validate_json(doc), [])
        for key in build.SCHEMA["required"]:
            self.assertIn(key, build.__doc__, f"top-level key {key} is not documented at the top of build.py")
        bad = json.loads(self.js)
        bad["features"][0]["status"] = "covered"
        del bad["summary"]["total"]
        errs = build.validate_json(bad)
        self.assertTrue(any("status" in e for e in errs) and any("total" in e for e in errs))

    def test_every_partial_row_is_in_the_page(self):
        for r in self.rows:
            if r["_status"] == "partial":
                self.assertIn(f"`{r['id']}`", self.md)

    def test_no_rounding_up(self):
        self.assertEqual(build.pct(1237, 1268), 97.5)
        self.assertEqual(build.pct(809, 1245), 64.9)  # 64.98...: truncated, not 65.0
        self.assertEqual(build.pct(65, 100), 65.0)

    def test_build_twice_identical(self):
        m2, r2, b2 = build.build_model()
        self.assertEqual(build.render_json(m2), self.js)
        self.assertEqual(build.render_md(m2, r2, b2), self.md)

    def test_files_on_disk_are_current(self):
        for name, text in (("compatibility.md", self.md), ("compatibility.json", self.js)):
            with open(os.path.join(ROOT, "docs", name), encoding="utf-8") as f:
                self.assertEqual(f.read(), text, f"docs/{name} is stale: run python3 tools/compat-page/build.py")


class StabilityTest(unittest.TestCase):
    """The output depends on the data only: not on HEAD, the clock, the refs of the clone or a git history at all."""
    INPUTS = ["compat/inventory.csv", "compat/coverage.py", "compat/COVERAGE.md", "compat/UNVERIFIED.md",
              "compat/exclusions_decided.md", "compat/scan_extra.json", "compat-scan.md"]

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.old_root = build.ROOT
        self.addCleanup(setattr, build, "ROOT", self.old_root)

    def make_tree(self, name, git=True):
        d = os.path.join(self.tmp, name)
        for rel in self.INPUTS:
            os.makedirs(os.path.dirname(os.path.join(d, rel)), exist_ok=True)
            shutil.copy(os.path.join(ROOT, rel), os.path.join(d, rel))
        if git:
            def g(*a):
                subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=d, check=True, capture_output=True,
                               env={**os.environ, "GIT_COMMITTER_DATE": "2026-10-28T12:00:00", "GIT_AUTHOR_DATE": "2026-10-28T12:00:00"})
            g("init", "-q")
            g("add", "-A")
            g("commit", "-q", "-m", "data")
            self.g = g
        return d

    def run_build(self, d, out):
        build.ROOT = d
        model, rows, byid = build.build_model()
        return build.render_md(model, rows, byid), build.render_json(model), model

    def test_new_commit_not_touching_data_changes_nothing(self):
        d = self.make_tree("repo")
        md1, js1, m1 = self.run_build(d, "o1")
        self.assertEqual(m1["generated"]["inventory_last_changed"], "2026-10-28")
        os.makedirs(os.path.join(d, "docs"))
        open(os.path.join(d, "docs", "x.txt"), "w").write("not data")
        self.g("add", "-A")
        self.g("commit", "-q", "-m", "docs only")
        self.g("commit", "-q", "--allow-empty", "-m", "empty")
        md2, js2, m2 = self.run_build(d, "o2")
        self.assertEqual((md1, js1), (md2, js2))
        head = subprocess.run(["git", "rev-parse", "--short=7", "HEAD"], cwd=d, capture_output=True, text=True).stdout.strip()
        self.assertNotIn(head, md2 + js2)  # HEAD is not in the output
        self.assertEqual(m2["generated"]["inventory_last_commit"], m1["generated"]["inventory_last_commit"])

    def test_changing_the_data_changes_the_commit(self):
        d = self.make_tree("repo")
        _, _, m1 = self.run_build(d, "o1")
        p = os.path.join(d, "compat", "inventory.csv")
        text = open(p, newline="").read()
        open(p, "w", newline="").write(text.replace("no differential test and no probe", "no differential test and no probe.", 1))
        self.g("add", "-A")
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "data"], cwd=d, check=True,
                       env={**os.environ, "GIT_COMMITTER_DATE": "2026-10-30T12:00:00", "GIT_AUTHOR_DATE": "2026-10-30T12:00:00"})
        _, _, m2 = self.run_build(d, "o2")
        self.assertEqual(m2["generated"]["inventory_last_changed"], "2026-10-30")
        self.assertNotEqual(m1["generated"]["inventory_last_commit"], m2["generated"]["inventory_last_commit"])

    def test_without_git_says_not_available(self):
        d = self.make_tree("zip", git=False)
        md, js, m = self.run_build(d, "o")
        self.assertIsNone(m["generated"]["inventory_last_changed"])
        self.assertIsNone(m["generated"]["inventory_last_commit"])
        self.assertIn("not available", md)
        self.assertEqual(build.validate_json(json.loads(js)), [])

    def test_shallow_clone_is_not_trusted_at_the_cut_off(self):
        d = self.make_tree("repo")
        full = self.run_build(d, "o")[2]["generated"]
        self.g("commit", "-q", "--allow-empty", "-m", "later")
        c = os.path.join(self.tmp, "shallow")
        subprocess.run(["git", "clone", "-q", "--depth", "1", "file://" + d, c], check=True, capture_output=True)
        self.assertIsNone(self.run_build(c, "o")[2]["generated"]["inventory_last_commit"])
        self.assertIsNotNone(full["inventory_last_commit"])


if __name__ == "__main__":
    unittest.main()
