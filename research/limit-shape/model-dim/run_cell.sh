#!/usr/bin/env bash
# Chạy 1 ô của ma trận đo chiều model hạn mức free opencode.ai (research, không đụng gateway).
#
# usage: run_cell.sh <cell> <dump> <tls: bun|go> <proxy: none|host:port> <ses: fixed|NEW> <n>
#
# Biến môi trường:
#   MODEL   — model ghi đè trong body (mặc định glm-5.3-flash-free)
#   SES     — session id cố định (nếu sesmode=fixed)
set -u
CELL="$1"; DUMP="$2"; TLSMODE="$3"; PROXY="$4"; SESMODE="$5"; N="$6"; shift 6 || true
HERE="$(cd "$(dirname "$0")" && pwd)"
PROBEDIR="$HERE/../probe"
case "$DUMP" in /*) ;; *) DUMP="$HERE/$DUMP" ;; esac
LOGDIR="$HERE/logs"; mkdir -p "$LOGDIR"
LOG="$LOGDIR/limitshape_${CELL}.jsonl"
IPLOG="$LOGDIR/limitshape_${CELL}_ip.txt"
RUNLOG="$LOGDIR/run.log"

# IP upstream THẬT mà opencode.ai thấy = IPv6 riêng của container WARP.
# api.ipify.org trả IPv4 dùng chung của WARP => bẫy đo, không dùng.
exit_ip() {
  if [ "$PROXY" = "none" ]; then
    timeout 30 curl -s https://opencode.ai/cdn-cgi/trace | grep '^ip='
  else
    timeout 30 curl -s -x "socks5h://$PROXY" https://opencode.ai/cdn-cgi/trace | grep '^ip='
  fi
}

IP_BEFORE="$(exit_ip)"
{
  echo "cell=$CELL dump=$(basename "$DUMP") tls=$TLSMODE proxy=$PROXY ses=$SESMODE n=$N model=${MODEL:-glm-5.3-flash-free}"
  echo "ip_before=$IP_BEFORE at=$(date -u +%FT%TZ)"
} >> "$IPLOG"

SOCK=""
[ "$PROXY" != "none" ] && SOCK="$PROXY"

export MODEL="${MODEL:-glm-5.3-flash-free}"
export STOP429=1
export N="$N"
if [ "$SESMODE" = "fixed" ]; then
  export SES="${SES:-$(python3 "$HERE/gen_ses.py")}"
  unset NEWSES
else
  export NEWSES=1
  unset SES
fi
if [ "$TLSMODE" = "go" ]; then
  TLSARG=go
else
  TLSARG="$HERE/bun_clienthello.bin"
fi

echo "--- cell $CELL start $(date -u +%FT%TZ) model=$MODEL proxy=$PROXY ses=${SES:-new-each} n=$N ip_before=$IP_BEFORE" >> "$RUNLOG"
( cd "$PROBEDIR" && ./probe "$DUMP" "$TLSARG" $SOCK ) 2>>"$RUNLOG" | \
  python3 "$HERE/annotate.py" "$CELL" | tee -a "$LOG" | \
  python3 "$HERE/summarize.py" "$CELL" | tee -a "$RUNLOG"

# Dòng log từng request (cell, session, model, status, retry-after, timestamp) cho logs/run.log
python3 "$HERE/runlog.py" "$CELL" "$LOG" >> "$RUNLOG"

IP_AFTER="$(exit_ip)"
echo "ip_after=$IP_AFTER at=$(date -u +%FT%TZ)" >> "$IPLOG"
echo "--- cell $CELL end $(date -u +%FT%TZ) ip_after=$IP_AFTER" >> "$RUNLOG"
