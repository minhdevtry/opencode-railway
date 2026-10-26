#!/usr/bin/env bash
# Chạy 1 ô của ma trận đo hạn mức free opencode.ai (research, không đụng gateway).
# usage: run_cell.sh <cell> <dump> <tls: bun|go> <proxy: none|host:port> <ses: fixed|NEW> <n> [extra env]
set -u
CELL="$1"; DUMP="$2"; TLSMODE="$3"; PROXY="$4"; SESMODE="$5"; N="$6"; shift 6 || true
HERE="$(cd "$(dirname "$0")" && pwd)"
case "$DUMP" in /*) ;; *) DUMP="$HERE/$DUMP" ;; esac
LOGDIR="$HERE/logs"; mkdir -p "$LOGDIR"
LOG="$LOGDIR/limitshape_${CELL}.jsonl"
IPLOG="$LOGDIR/limitshape_${CELL}_ip.txt"

exit_ip() {
  if [ "$PROXY" = "none" ]; then
    timeout 20 curl -s https://api.ipify.org
  else
    timeout 25 curl -s -x "socks5h://$PROXY" https://api.ipify.org
  fi
}

IP_BEFORE="$(exit_ip)"
{
  echo "cell=$CELL dump=$(basename "$DUMP") tls=$TLSMODE proxy=$PROXY ses=$SESMODE n=$N"
  echo "ip_before=$IP_BEFORE at=$(date -u +%FT%TZ)"
} >> "$IPLOG"

SOCK=""
[ "$PROXY" != "none" ] && SOCK="$PROXY"

export MODEL=glm-5.3-flash-free
export STOP429=1
export N="$N"
if [ "$SESMODE" = "fixed" ]; then
  export SES="${SES:-$(python3 "$HERE/gen_ses.py")}"
else
  export NEWSES=1
  unset SES
fi
if [ "$TLSMODE" = "go" ]; then
  TLSARG=go
else
  TLSARG=bun_clienthello.bin
fi

echo "--- cell $CELL start $(date -u +%FT%TZ) ip_before=$IP_BEFORE ses=${SES:-new-each}" | tee -a "$LOGDIR/limitshape_run.log"
( cd "$HERE/probe" && ./probe "$DUMP" "$TLSARG" $SOCK ) 2>>"$LOGDIR/limitshape_run.log" | \
  python3 "$HERE/annotate.py" "$CELL" | tee -a "$LOG" | \
  python3 "$HERE/summarize.py" "$CELL" | tee -a "$LOGDIR/limitshape_run.log"

IP_AFTER="$(exit_ip)"
echo "ip_after=$IP_AFTER at=$(date -u +%FT%TZ)" >> "$IPLOG"
echo "--- cell $CELL end $(date -u +%FT%TZ) ip_after=$IP_AFTER" | tee -a "$LOGDIR/limitshape_run.log"
