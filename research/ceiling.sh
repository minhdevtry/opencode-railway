#!/bin/bash
# Đo trần thông lượng của gateway (model auto) theo bậc concurrency, workload thật. Chạy lúc vừa reset quota (00:00 UTC).
#   research/ceiling.sh [url]   -> research/results/ceiling-<ngày>/
cd "$(dirname "$0")/.."
URL=${1:-http://127.0.0.1:8090}
OUT=research/results/ceiling-$(date +%m%d-%H%M); mkdir -p $OUT
LG=research/run/loadgen
[ -x $LG ] || go build -o $LG ./research/loadgen
step() { # tên c n workload [extra]
  echo "== $1 c=$2 n=$3 $4 $(date +%T)" | tee -a $OUT/log.txt
  $LG -url $URL/v1/chat/completions -c $2 -n $3 -workload $4 ${@:5} -timeout 1800s -out $OUT/$1.jsonl 2>>$OUT/progress.txt | tee $OUT/$1.summary.json >/dev/null
  python3 -I -c "import json; d=json.load(open('$OUT/$1.summary.json')); print('   ok=%d/%d  %.2f ok/s  %d tok/s  p50=%.1fs p95=%.1fs  errors=%s' % (d['ok'],d['n'],d['ok_per_s'],d['out_tok_per_s'],d['p50_s'],d['p95_s'],d['errors']))" | tee -a $OUT/log.txt
  curl -s $URL/status > $OUT/$1.status.json; curl -s $URL/v1/capacity > $OUT/$1.capacity.json
}
step c050 50 300 label -max-tokens 1024
step c100 100 600 label -max-tokens 1024
step c200 200 1000 label -max-tokens 1024
step c400 400 1600 label -max-tokens 1024
step c800 800 2400 label -max-tokens 1024
step long24k 100 300 long -long-kb 24 -max-tokens 1024
echo "xong $(date +%T)" | tee -a $OUT/log.txt
