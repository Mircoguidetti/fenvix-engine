# Merge 6: fix-long-commands 9a52571 on integration-4, own tests once per mode, test_52/52r once sealed2-derive
- No source conflict. Test conflicts and renames: see the merge commit (test_56_streamed_find -> test_71,
  test_57_long_commands_churn -> test_72; test_55_long_commands and test_58r keep their numbers).
- test_55, test_71, test_72, test_58r, test_51_c2_fix_long_reads: sealed2-derive 29 passed, off 29 passed.
- test_52 + test_52r (sealed2-derive; the in-process read-backs are mandatory again, F7 being fixed): 12 passed,
  4 skipped (the controls that need R52_OLD_BIN, an integration-3 binary).
  - test_52 unthrottled: 600,001 acknowledged, 600,001 scanned in-process (refused by F7 in part 1), 0 missing; after a
    restart 0 missing, 0 twice, 0 refused-but-stored. Throttled: 150,001, the same.
  - test_52r mixed: 114,761 and 36,841 acknowledged inserts, all in the scan and both indexes, in-process and after a restart.
  - test_52r split: the known code-1 "db is closed" once (printed as KNOWN DEFECT, not asserted; data right).
