#!/usr/bin/env python3
"""Prints a TLC counterexample as the changed variables of each step (actions show as NextR
in the review copy, so the diff names them)."""
import re, sys
txt = open(sys.argv[1]).read()
states = re.split(r"\nState \d+: ", txt)[1:]
prev = {}
for i, st in enumerate(states, 1):
    head, _, body = st.partition("\n")
    body = body.split("\n\n")[0]
    cur = {}
    for m in re.finditer(r"^/\\ (\w+) = (.*?)(?=^/\\ |\Z)", body, re.S | re.M):
        cur[m.group(1)] = " ".join(m.group(2).split())
    ch = [k for k in cur if prev.get(k) != cur[k]] if prev else ["(initial)"]
    act = head.split(" line")[0].strip("<> ")
    print(f"{i:2} {act}: " + "; ".join(f"{k} = {cur[k]}" for k in ch if k in cur and k not in ("tr", "hist")) if prev else f"{i:2} initial: eenv = {cur.get('eenv')}")
    prev = cur
