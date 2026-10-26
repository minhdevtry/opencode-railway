#!/usr/bin/env bash
# Xoay danh tính WARP cho tới khi container có IPv6 MỚI còn sống (1 request thật trả 200).
#
# usage: rotate_until_alive.sh <container_suffix> <host_port> <max_attempts> [model]
set -u
SUF="$1"; PORT="$2"; MAX="${3:-6}"; MODEL="${4:-glm-5.3-flash-free}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROBEDIR="$HERE/../probe"
LOG="$HERE/logs/liveness.txt"
DUMP="$HERE/../dumps/gw1.json"

for attempt in $(seq 1 "$MAX"); do
  {
    echo "--- attempt $attempt container=warp-meas-$SUF port=$PORT model=$MODEL at=$(date -u +%FT%TZ)"
  } >> "$LOG"
  IP="$(timeout 40 curl -s -x "socks5h://127.0.0.1:$PORT" https://opencode.ai/cdn-cgi/trace | grep '^ip=' | cut -d= -f2)"
  if [ -z "$IP" ]; then
    echo "trace FAIL (khong lay duoc IPv6) -> xoay lai" >> "$LOG"
    docker exec "warp-meas-$SUF" sh -c "warp-cli --accept-tos registration delete >/dev/null 2>&1; warp-cli --accept-tos registration new >/dev/null && warp-cli --accept-tos connect" >/dev/null 2>&1
    sleep 25
    continue
  fi
  OUT="$(cd "$PROBEDIR" && MODEL="$MODEL" N=1 SES="$(python3 "$HERE/gen_ses.py")" ./probe "$DUMP" bun_clienthello.bin "127.0.0.1:$PORT" 2>/dev/null)"
  echo "ip=$IP out=$OUT" >> "$LOG"
  case "$OUT" in
    *'"status":200'*)
      echo "ALIVE container=warp-meas-$SUF port=$PORT ip=$IP attempt=$attempt" >> "$LOG"
      echo "$SUF $PORT $IP"
      exit 0
      ;;
  esac
  echo "DEAD -> xoay danh tinh" >> "$LOG"
  docker exec "warp-meas-$SUF" sh -c "warp-cli --accept-tos registration delete >/dev/null 2>&1; warp-cli --accept-tos registration new >/dev/null && warp-cli --accept-tos connect" >/dev/null 2>&1
  sleep 25
done
echo "DEAD-EXHAUSTED container=warp-meas-$SUF port=$PORT" >> "$LOG"
exit 1
