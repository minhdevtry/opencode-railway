#!/usr/bin/env bash
# Kiểm tra: field "model" trong body CÓ thực sự được upstream dùng để chọn model không?
#
# - model bịa ("zzz-not-a-model-free") => phải trả lỗi (không phải 200, không phải 429 hạn mức).
#   Nếu trả 200 => upstream BỎ QUA field model => toàn bộ phép đo vô hiệu.
# - model khác (beam-free) trên IP mà glm đã cạn => 200 chứng minh IP này còn quota cho model khác.
#
# usage: verify_model_override.sh <proxy_host_port> <cell>
set -u
PROXY="$1"; CELL="${2:-VER}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PROBEDIR="$HERE/../probe"
LOGDIR="$HERE/logs"; mkdir -p "$LOGDIR"
LOG="$LOGDIR/limitshape_${CELL}.jsonl"
RUNLOG="$LOGDIR/run.log"
DUMP="$HERE/../dumps/gw1.json"

IP="$(timeout 40 curl -s -x "socks5h://$PROXY" https://opencode.ai/cdn-cgi/trace | grep '^ip=' | cut -d= -f2)"
echo "--- cell $CELL start $(date -u +%FT%TZ) proxy=$PROXY ip=$IP" >> "$RUNLOG"

i=0
for M in zzz-not-a-model-free beam-free; do
  SES="$(python3 "$HERE/gen_ses.py")"
  OUT="$(cd "$PROBEDIR" && MODEL="$M" N=1 SES="$SES" ./probe "$DUMP" bun_clienthello.bin "$PROXY" 2>>"$RUNLOG" | head -1)"
  [ -z "$OUT" ] && OUT='{"status":0,"err":"empty"}'
  printf '%s\n' "$OUT" | python3 -c "
import json,sys
d=json.loads(sys.stdin.readline())
d['cell']='$CELL'; d['i']=$i; d['model']='$M'; d['ses']='$SES'
print(json.dumps(d,ensure_ascii=False))
" | tee -a "$LOG" | python3 "$HERE/runlog.py" "$CELL" >> "$RUNLOG"
  i=$((i+1))
done
echo "--- cell $CELL end $(date -u +%FT%TZ)" >> "$RUNLOG"
