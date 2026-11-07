#!/usr/bin/env python3
"""Summarizes TLC outputs: verdict, states, depth, and the action sequence of a counterexample.
Usage: python3 summarize.py output/*.txt"""
import re
import sys

for path in sys.argv[1:]:
    text = open(path).read()
    name = path.split("/")[-1][:-4]
    viol = re.search(r"Error: Invariant (\w+) is violated", text)
    gen = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found", text)
    depth = re.search(r"The depth of the complete state graph search is (\d+)", text)
    noerr = "No error has been found" in text
    left = re.findall(r"distinct states found, ([\d,]+) states left on queue", text)
    closed = noerr and left and left[-1] == "0"
    # with the time budget (MCBudget) TLC also prints "No error has been found", but with states
    # left on the queue: no violation in the states explored, BFS complete up to depth - 1
    verdict = (f"VIOLATED {viol.group(1)}" if viol else "PASS, complete" if closed
               else "PASS within the time budget (not complete)" if noerr else "UNFINISHED")
    states = gen[-1][1] if gen else "?"
    print(f"{name}: {verdict}; distinct states {states}; depth {depth.group(1) if depth else '?'}"
          + (f"; left on queue {left[-1]}" if left and not closed and not viol else ""))
    if viol:
        acts = re.findall(r"^State \d+: <(\w+)(\([^)]*\))?", text, re.M)
        print("   trace: " + " -> ".join(a + b for a, b in acts[1:]))
