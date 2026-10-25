#!/bin/bash
# Chạy gateway thử nghiệm trên :18090 (không đụng prod :8090). Biến: RES, MAX_WAIT, POOL_PORTS, EXTRA (env thêm)
R="$(cd "$(dirname "$0")" && pwd)/run"; mkdir -p "$R"
pkill -f "$R/orw$" 2>/dev/null; sleep 1
POOL=$(for p in ${POOL_PORTS:-$(seq 1080 1095)}; do printf "socks5://127.0.0.1:$p,"; done)
cd $R && env PORT=${PORT:-18090} DISABLE_TOR=true PROXY_POOL="${POOL%,}" RESIDENTIAL_PROXIES="${RES-socks5://160.250.166.13:11448}" \
  MAX_CONCURRENCY=20000 RATE_LIMIT_RPM=0 MAX_WAIT=${MAX_WAIT:-300s} QUEUE_WAIT=600s $EXTRA setsid $R/orw > $R/orw.log 2>&1 &
