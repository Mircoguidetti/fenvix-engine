# shellcheck shell=bash
# Kitchen (K2) · the user-data of the machines (used by run.sh and the local test). Like M31 and Ireland: the power-off
# at the time ceiling is the FIRST line, before anything is downloaded; if the download or the tar fails the machine
# powers off at once (power-off = termination, set at launch).
#   write_user_data URL BUCKET ROLE [K]  -> prints the script the machine runs at boot.
#     ROLE = client (runs client.sh BUCKET) or server (runs server.sh BUCKET K).
#     MACHINE_CEILING_MIN: minutes after which the machine powers off by itself (run.sh computes it: the time left to
#     the run, so no machine outlives the job).
write_user_data() {
  local url=$1 bucket=$2 role=$3 k=${4:-}
  local prog=$role.sh
  cat <<UDEOF
#!/bin/bash
shutdown -h +${MACHINE_CEILING_MIN:-100} "kitchen: time ceiling" || true
D=\${MACHINE_DIR:-/opt/kitchen}
mkdir -p "\$D" && cd "\$D" || { shutdown -h now "kitchen: no folder"; exit 1; }
ok=0
for i in 1 2 3 4 5; do curl -fsS -o g.tgz "$url" && { ok=1; break; }; sleep \${DOWNLOAD_RETRY_S:-10}; done
[ "\$ok" = 1 ] || { echo "download failed" > boot-error.txt; shutdown -h now "kitchen: download failed"; exit 1; }
tar xzf g.tgz && chmod +x $prog || { echo "tar failed" > boot-error.txt; shutdown -h now "kitchen: tar failed"; exit 1; }
nohup ./$prog "$bucket" $k > $role.log 2>&1 &
UDEOF
}
