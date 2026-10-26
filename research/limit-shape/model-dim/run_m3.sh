#!/usr/bin/env bash
# Ô M3: XEN KẼ model A / model B từ request #1 trên 1 IP, tổng N request.
# Mục tiêu: thấy model nào 429 hạn mức trước, và model kia còn 200 hay không.
#
# usage: run_m3.sh <cell> <proxy_host_port> <n_total> [modelA] [modelB]
set -u
CELL="$1"; PROXY="$2"; NTOT="${3:-200}"; MODELA="${4:-glm-5.3-flash-free}"; MODELB="${5:-nemotron-3.5-lightning-free}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROBEDIR="$HERE/../probe"
LOGDIR="$HERE/logs"; mkdir -p "$LOGDIR"
LOG="$LOGDIR/limitshape_${CELL}.jsonl"
IPLOG="$LOGDIR/limitshape_${CELL}_ip.txt"
RUNLOG="$LOGDIR/run.log"
DUMP="$HERE/../dumps/gw1.json"

export SES="$(python3 "$HERE/gen_ses.py")"
export NEWSES=""
export STOP429=1
# N của probe = 1 request mỗi lần gọi (đặt inline, KHÔNG export để không đè biến vòng lặp NTOT)

ip_of() { timeout 40 curl -s -x "socks5h://$PROXY" https://opencode.ai/cdn-cgi/trace | grep '^ip=' | cut -d= -f2; }

IP_BEFORE="$(ip_of)"
{ echo "cell=$CELL mode=interleave modelA=$MODELA modelB=$MODELB n=$NTOT proxy=$PROXY ses=$SES"
  echo "ip_before=$IP_BEFORE at=$(date -u +%FT%TZ)"; } >> "$IPLOG"
echo "--- cell $CELL start $(date -u +%FT%TZ) interleave A=$MODELA B=$MODELB n=$NTOT proxy=$PROXY ses=$SES ip_before=$IP_BEFORE" >> "$RUNLOG"

DEADSTREAK=0
for i in $(seq 1 "$NTOT"); do
  if [ $((i % 2)) -eq 1 ]; then M="$MODELA"; else M="$MODELB"; fi
  OUT="$(cd "$PROBEDIR" && MODEL="$M" N=1 ./probe "$DUMP" bun_clienthello.bin "$PROXY" 2>>"$RUNLOG" | head -1)"
  [ -z "$OUT" ] && OUT='{"status":0,"err":"empty"}'
  printf '%s\n' "$OUT" | python3 -c "
import json,sys
d=json.loads(sys.stdin.readline())
d['cell']='$CELL'; d['i']=$i; d['model']='$M'; d['ses']='$SES'
print(json.dumps(d,ensure_ascii=False))
" | tee -a "$LOG" | python3 "$HERE/runlog.py" "$CELL" >> "$RUNLOG"
  case "$OUT" in
    *'"status":200'*) DEADSTREAK=0 ;;
    *FreeUsageLimitError*|*'"status":429'*) DEADSTREAK=$((DEADSTREAK+1)) ;;
    *) DEADSTREAK=0 ;;
  esac
  # IP chết hẳn (10 lượt liên tiếp 429 hạn mức) => dừng sớm để tiết kiệm ngân sách request
  if [ "$DEADSTREAK" -ge 10 ]; then
    echo "--- cell $CELL early-stop: 10 request liên tiếp bị 429 hạn mức (tại i=$i)" >> "$RUNLOG"
    break
  fi
done

IP_AFTER="$(ip_of)"
echo "ip_after=$IP_AFTER at=$(date -u +%FT%TZ)" >> "$IPLOG"
echo "--- cell $CELL end $(date -u +%FT%TZ) ip_after=$IP_AFTER" >> "$RUNLOG"
