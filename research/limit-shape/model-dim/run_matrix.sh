#!/usr/bin/env bash
# Ma trận đo chiều model: M1, M2, M4 chạy SONG SONG trên 3 IP khác nhau.
#
# M1 (port 1200): pha 1 model A (session cố định, N=200, STOP429) -> cap_A
#                 pha 2 model B (session MỚI, N=20) ngay trên cùng IP
# M2 (port 1201): pha 1 model B (N=200, STOP429) -> cap_B
#                 pha 2 model A (session MỚI, N=20) ngay trên cùng IP
# M4 (port 1202): đối chứng model B một mình, N=200, STOP429 -> cap_B trên IP sạch
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
MODELA="${MODELA:-glm-5.3-flash-free}"
MODELB="${MODELB:-nemotron-3.5-lightning-free}"
N1="${N1:-200}"
N2="${N2:-20}"
DUMP=../dumps/gw1.json

run() { # run <cell> <port> <model> <sesmode> <n>
  local cell="$1" port="$2" model="$3" sesmode="$4" n="$5"
  echo "=== $(date -u +%FT%TZ) cell=$cell port=$port model=$model ses=$sesmode n=$n" >> logs/matrix.out
  MODEL="$model" ./run_cell.sh "$cell" "$DUMP" bun "127.0.0.1:$port" "$sesmode" "$n" > "logs/${cell}.out" 2>&1
  echo "=== $(date -u +%FT%TZ) cell=$cell DONE rc=$?" >> logs/matrix.out
}

( run M1 1200 "$MODELA" fixed "$N1"
  run M1P2 1200 "$MODELB" fixed "$N2" ) &
P1=$!
( run M2 1201 "$MODELB" fixed "$N1"
  run M2P2 1201 "$MODELA" fixed "$N2" ) &
P2=$!
( run M4 1202 "$MODELB" fixed "$N1" ) &
P3=$!
wait $P1 $P2 $P3
echo "=== $(date -u +%FT%TZ) matrix M1/M2/M4 DONE" >> logs/matrix.out
