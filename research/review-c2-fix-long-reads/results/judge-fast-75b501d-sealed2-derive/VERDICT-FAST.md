# Judge verdict (fast), commit 75b501d, with --sealed-runs 2

- readers: PASS (raw readers: PASS (24 matches, all in named files))
- build: PASS
- unit: FAIL (225 passed, 1 failed, see unit.log)
- diff-py: FAIL (412/413 same answers as mongod 7.0, see diff-py.log)
- ctl: PASS (3 files)
- kill: PASS (see kill.log for counts)
- diff-node: FAIL (411/413 same answers as mongod 7.0, see diff-node.log)
