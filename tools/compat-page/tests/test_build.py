"""Tests for tools/compat-page/build.py.   python3 -m unittest discover -s tools/compat-page/tests"""
import csv
import json
import os
import re
import subprocess
import sys
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


if __name__ == "__main__":
    unittest.main()
