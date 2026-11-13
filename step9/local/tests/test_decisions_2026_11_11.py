"""DECISIONS 2026-11-11, as P-integration-4 output 2 point 2 asks: resume[cold] reported but never compared (F4, tested
in test_gate.py), the maximum-load cells NOT VALID by design, no command for the 1 TB round as it was built then."""
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


def test_no_command_asks_for_the_1_tb_round_as_built_on_2026_11_11():
    """DECISIONS 2026-11-11 point 2: "the 1 TB round as built (40 databases of 24.3 GiB, of which it reads the same 2 as
    at 100 GB) is not run; a real 1 TB round comes later". Since generator-1tb (approved 2026-11-13) data "1tb" is ONE
    database of ballast_gib, and the coordinator asked (2026-11-13) for the 1 TB commands of that real round (08, 09,
    10). So the rule kept here: the old shape cannot be asked for any more, and every 1 TB command is that one database.
    Running it is still the founder's yes on the amount; DECISIONS has no entry yet that moves 1 TB into the first
    round (said in the total review's report)."""
    import sys
    sys.path.insert(0, os.path.join(REPO, "step9", "ci"))
    import decide
    assert decide.DATA["1tb"]["ballast_dbs"] == 1 and not hasattr(decide, "BALLAST_DB_GB")
    seen = 0
    for f in glob.glob(os.path.join(REPO, "step9", "commands", "*.json")):
        c = json.load(open(f))
        if c.get("data") == "1tb":
            seen += 1
            assert "ballast_slice" not in c and "ballast_mode" not in c, f     # the generator's one database, nothing else
            assert c.get("pantry", "").startswith("fenvix-kitchen-pantry-"), f  # on the kept pantry of job L
    assert seen == 3


def test_cold_is_never_compared_in_the_gate():
    src = open(os.path.join(REPO, "step9", "ci", "gate.py")).read()
    assert 'NOT_COMPARED = {"resume[cold]"}' in src


def test_documentdb_is_a_priced_competitor_with_declared_source_and_date():
    # DECISIONS 2026-11-11 point 2: DocumentDB in cost.py and prices.json, prices DECLARED with source and date
    prices = json.load(open(os.path.join(REPO, "bench", "agent-checkpoints", "prices.json")))["options"]
    cost_src = open(os.path.join(REPO, "bench", "agent-checkpoints", "cost.py")).read()
    for name in ("documentdb_standard", "documentdb_io_optimized"):
        o = prices[name]
        for k in ("storage_gb_month", "io_per_million"):
            assert o[k]["label"] == "DECLARED", (name, k)
            assert o[k]["url"].startswith("https://aws.amazon.com/documentdb/pricing"), (name, k)
            assert re.search(r"20\d\d-\d\d-\d\d", o[k]["note"]), (name, k, "no date")
        assert name in cost_src or "documentdb" in cost_src, name
