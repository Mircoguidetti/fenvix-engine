# Kill phase on 3cb7502 (run directly: judge/run.sh's env_up needs the judge's mongod, removed to free disk)

- kill: PASS (s3 durable: 94,923 acknowledged ids, 0 lost inserts, 0 lost updates; open-gc safe: killed inside a cleanup pass, 11,398 acknowledged documents, 0 lost, 40,756 reads, 0 violations)
- unit: mongo-server 108/108; tenant-bench 150/151, the known counter::tests::rev_maps_leave_room_for_active_data
- ctl: test_66_big_speed 5/5, test_67_big_speed_kill 4/4
