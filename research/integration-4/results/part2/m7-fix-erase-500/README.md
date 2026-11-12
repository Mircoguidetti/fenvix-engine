# Merge 7: fix-erase-500 015ecb9 and test05-erase-inflight 605b261, own tests once per mode
- No conflict. Renames: test_53_absent_leaves_no_usage -> test_73, test_55_erase_fork_usage -> test_74; 61, 62, 65 kept.
- test_73, test_74, test_61, test_62, test_65, test_05_erase, test_43_review_compaction: sealed2-derive 45 passed,
  off 45 passed. test_43's refused-fork case (failing on integration-4 before this merge, part 1 CI) now passes.
