#!/usr/bin/env bash
# Runs inside the dev container: 7 algorithms x 10 data fractions.
# Appends CSV rows: alg,pct,time_s,tuples_per_s,count,checksum
set -u
SYSTEST=./build/nes-systests/systest/systest
OUT=scaling_results.csv
: > "$OUT"

ALGS="FK_MERG_L3 FK_MERG_L4 FK_SORT_L3 FK_SORT_L4 NFK_JOIN_L3 HASH_JOIN NESTED_LOOP_JOIN"

for p in 1 2 3 4 5 6 7 8 9 10; do
  for alg in $ALGS; do
    wd="build/nes-systests/scaling_${alg}_p${p}"
    timeout 1800 $SYSTEST -b \
      -t nes-systests/benchmark/Nexmark.test:05 \
      --data "$PWD/scaling-data/p${p}" \
      --workingDir="$wd" \
      --optimizer join_strategy="$alg" \
      -- --worker.default_query_execution.execution_mode=COMPILER \
         --worker.query_engine.number_of_worker_threads=1 \
         --worker.total_memory_in_bytes=12884901888 \
         --worker.unpooled_memory_fraction=0.8935 > "$wd.log" 2>&1 || true
    result=$(tail -1 "$wd/results/Nexmark_5.csv" 2>/dev/null)
    python3 - "$alg" "$p" "$wd" "$result" <<'EOF'
import json, sys
alg, p, wd, result = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) > 4 else ""
try:
    r = json.load(open(f"{wd}/BenchmarkResults.json"))[0]
    line = f"{alg},{int(p)*10},{r['time']:.3f},{r['tuplesPerSecond']:.0f},{result}"
except Exception:
    line = f"{alg},{int(p)*10},FAILED,,see {wd}.log"
print(line)
with open("scaling_results.csv", "a") as f:
    f.write(line + "\n")
EOF
  done
done
