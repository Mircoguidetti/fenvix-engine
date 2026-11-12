"""DECISIONS 2026-11-11, as P-integration-4 output 2 point 2 asks: resume[cold] reported but never compared (F4, tested
in test_gate.py), the maximum-load cells NOT VALID by design, no 1 TB command among the round's commands."""
import glob
import json
import os
import re

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def test_max_load_cells_are_said_not_valid_after_the_m30_program():
    src = open(os.path.join(REPO, "step9", "ci", "run.py")).read()
    note = re.search(r"MAX_LOAD_NOTE = \((.*?)\)\n", src, re.S).group(1)
    assert "hot reads per second" in note and "peak writes" in note and "NOT VALID by design" in note
    m30 = src.index("say(f\"m30 program on cook 1:")
    assert src.index("say(MAX_LOAD_NOTE)", m30) - m30 < 200, "the note must follow the m30 outcome line"


def test_no_1tb_command():
    for f in glob.glob(os.path.join(REPO, "step9", "commands", "*.json")):
        c = json.load(open(f))
        text = json.dumps(c).lower()
        assert "1tb" not in text and "1000gb" not in text and "1-tb" not in text, f
        assert not re.search(r"\b1 ?tb\b", os.path.basename(f).lower()), f


def test_cold_is_never_compared_in_the_gate():
    src = open(os.path.join(REPO, "step9", "ci", "gate.py")).read()
    assert 'NOT_COMPARED = {"resume[cold]"}' in src
