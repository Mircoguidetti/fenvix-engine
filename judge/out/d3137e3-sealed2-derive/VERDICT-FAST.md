# Judge verdict (fast), commit d3137e3, with --sealed-runs 2

- readers: PASS (raw readers: PASS (24 matches, all in named files))
- build: PASS
- unit: FAIL (224 passed, 1 failed, see unit.log)
- diff-py: FAIL (412/413 same answers as mongod 7.0, see diff-py.log)
- diff-node: FAIL (412/413 same answers as mongod 7.0, see diff-node.log)
- kill: PASS (see kill.log for counts)
- ctl: FAIL (1 of 3 files: test_05_erase)
