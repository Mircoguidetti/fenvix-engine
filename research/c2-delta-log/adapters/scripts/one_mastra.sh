#!/usr/bin/env bash
# run one Mastra test file against a target: one_mastra.sh mongod|ours FILE OUTXML
H=/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/run/bench/agent-adapters
MODE=standalone $H/target.sh $1 >/dev/null && PORT=27021 $H/target.sh $1 > /dev/null || exit 1
cd /home/user/adapters/mastra/stores/mongodb && unset MONGODB_URL MONGODB_URI
s=$(date +%s.%N)
MONGODB_RS_URL="mongodb://localhost:27021/?directConnection=true" MONGODB_REPLICA_SET_URL="mongodb://localhost:27021/?directConnection=true" \
  timeout 900 npx vitest run "$2" --reporter=junit --outputFile.junit="$3" > "$3.log" 2>&1
echo "$1 $2: $(python3 $H/compare.py --count $3) in $(echo "$(date +%s.%N) - $s" | bc) s"
python3 - "$3" <<'P'
import sys, xml.etree.ElementTree as ET
for tc in ET.parse(sys.argv[1]).getroot().iter("testcase"):
    if float(tc.get("time")) > 3 or tc.find("failure") is not None: print("  ", tc.get("name")[:110], tc.get("time"), "FAILED" if tc.find("failure") is not None else "")
P
$H/target.sh stopall
