"""DECISIONS 2026-11-11 "Amazon DocumentDB joins the comparison": cost.py prices DocumentDB next to Atlas and Aurora,
from prices.json (DECLARED, with its source), for the MongoDB-compatible target."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import cost  # noqa: E402

PRICES = json.load(open(os.path.join(os.path.dirname(HERE), "prices.json")))["options"]
SUMMARY = {"target": "mongo", "label": "t", "note": "test", "bytes": {"stored_per_checkpoint": 8192.0, "stored": 8192 * 1000,
                                                                     "checkpoints": 1000, "stored_from": "test"}}


def test_documentdb_options_are_priced_and_declared():
    for name, gb, io in (("documentdb_standard", 0.10, 0.20), ("documentdb_io_optimized", 0.30, 0.0)):
        o = PRICES[name]
        assert o["target"] == "mongo"
        assert o["storage_gb_month"] == {**o["storage_gb_month"], "value": gb, "label": "DECLARED"}
        assert o["io_per_million"]["value"] == io and o["io_per_million"]["label"] == "DECLARED"
        assert "2026-11-08-chat-fact-check" in o["storage_gb_month"]["note"]
        assert o["instance_hour"]["label"] == "UNKNOWN"


def test_documentdb_standard_charges_io_and_io_optimized_does_not():
    std = cost.cost(SUMMARY, "documentdb_standard", PRICES["documentdb_standard"], rate=20, sps=200)
    opt = cost.cost(SUMMARY, "documentdb_io_optimized", PRICES["documentdb_io_optimized"], rate=20, sps=200)
    assert std["requests_per_million"] == 2 * 0.20          # ceil(8192 / 4096) write I/Os x $0.20 per million
    assert any("ASSUMPTION: DocumentDB's I/O unit" in l for l in std["lines"])
    assert opt["requests_per_million"] == 0.0
    assert abs(std["storage_1y_per_million"] - 8.192 * 0.10 * 12) < 1e-9
    assert abs(opt["storage_1y_per_million"] - 8.192 * 0.30 * 12) < 1e-9


def test_every_mongo_option_is_listed_for_a_mongo_summary():
    names = [n for n, o in PRICES.items() if o["target"] == "mongo"]
    assert {"atlas_dedicated_m30", "atlas_infinite_m30", "documentdb_standard", "documentdb_io_optimized"} <= set(names)
