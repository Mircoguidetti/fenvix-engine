# Judge verdict (fast), commit 13ebe46, with --sealed-runs 2

- readers: PASS (raw readers: PASS (9 matches, all in named files))
- build: PASS
- unit: FAIL (145 passed, 1 failed, see unit.log)
- diff-py: FAIL (412/413 same answers as mongod 7.0, see diff-py.log)
- diff-node: FAIL (412/413 same answers as mongod 7.0, see diff-node.log)
- ctl: FAIL (1 of 3 files: test_05_erase)
- kill: PASS (see kill.log for counts)
