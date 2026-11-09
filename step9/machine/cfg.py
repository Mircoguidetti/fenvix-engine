"""Step 9 · print one field of a JSON settings file (dotted path), for the shell scripts.
Lists and objects are printed as JSON, a missing field as an empty line.
Usage: cfg.py FILE FIELD[.SUBFIELD]"""
import json
import sys

v = json.load(open(sys.argv[1]))
for k in sys.argv[2].split("."):
    v = v.get(k) if isinstance(v, dict) else None
print("" if v is None else (json.dumps(v) if isinstance(v, (list, dict)) else v))
