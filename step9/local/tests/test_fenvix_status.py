"""Step 9 · total review N4 (DECISIONS 2026-11-11, the S3-WAL fallback): every AWS program's result is labelled with the
batches our server acknowledged through the S3-WAL fallback (tenantStats.quaderno.failed before and after). Before,
nothing read the counter: a write-peak number could mix the two paths under the label "2 of 3 log copies".
$0. Usage: python3 step9/local/tests/test_fenvix_status.py"""
import json
import os
import subprocess
import sys
import tempfile
import types

HERE = os.path.dirname(os.path.abspath(__file__))
PROG = os.path.join(HERE, "..", "..", "programs")
sys.path.insert(0, PROG)
import fenvix_status  # noqa: E402


def snap(failed):
    return {"quaderno": {"sent": 100, "failed": failed}}


def test_no_fallback():
    assert fenvix_status.label(snap(0), snap(0))["fallback_batches"] == 0


def test_fallback_counted_and_said():
    lab = fenvix_status.label(snap(2), snap(9))
    assert lab["fallback_batches"] == 7 and "7 write batches took the S3-WAL fallback" in lab["label"], lab


def test_unreadable_is_unknown_never_zero():
    lab = fenvix_status.label({"tenantStats_error": "x"}, snap(0))
    assert lab["fallback_batches"] is None and lab["label"].startswith("UNKNOWN"), lab


def test_snapshot_reads_both_commands_and_survives_errors():
    def cmd(name):
        if name == "serverStatus":
            return {"fenvix": {"compactor": {"publishes": 3}, "s3": {"put": 1}}}
        raise RuntimeError("no tenantStats")
    s = fenvix_status.snapshot(types.SimpleNamespace(admin=types.SimpleNamespace(command=cmd)))
    assert s["compactor"]["publishes"] == 3 and "tenantStats_error" in s and fenvix_status.failed(s) is None, s


def test_cli_labels_the_program_result():
    d = tempfile.mkdtemp()
    json.dump(snap(1), open(os.path.join(d, "b.json"), "w"))
    json.dump(snap(4), open(os.path.join(d, "a.json"), "w"))
    json.dump({"rate": 25800}, open(os.path.join(d, "res.json"), "w"))
    r = subprocess.run([sys.executable, os.path.join(PROG, "fenvix_status.py"), "label", "write-peak", os.path.join(d, "b.json"),
                        os.path.join(d, "a.json"), os.path.join(d, "lab.json"), os.path.join(d, "res.json")],
                       capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.startswith("FENVIX-STATUS write-peak: 3 write batches"), r.stdout + r.stderr
    assert json.load(open(os.path.join(d, "res.json")))["fenvix_status"]["fallback_batches"] == 3



def test_stamp_puts_the_binary_into_a_result():
    d = tempfile.mkdtemp()
    f = os.path.join(d, "takeovers-summary.json")
    json.dump({"verdict": "PASS"}, open(f, "w"))
    subprocess.run([sys.executable, os.path.join(PROG, "fenvix_status.py"), "stamp", f, "f" * 64], check=True)
    assert json.load(open(f))["engine_binary_sha256"] == "f" * 64


if __name__ == "__main__":
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        fn()
        print(f"PASS {name}")
    print("ALL PASS")
