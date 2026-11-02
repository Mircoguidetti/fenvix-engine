#!/bin/bash
# avvia/ferma il finto S3 su disco
case "$1" in
  stop) [ -f /home/claude/s3.pid ] && kill $(cat /home/claude/s3.pid) 2>/dev/null; rm -f /home/claude/s3.pid ;;
  start) cd /home/claude/fenvix-engine/proto/milione && setsid nohup python3 scripts/finto_s3_disco.py /home/claude/s3disco 9000 > /home/claude/finto.log 2>&1 < /dev/null & echo $! > /home/claude/s3.pid ;;
esac
