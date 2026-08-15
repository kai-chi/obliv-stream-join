#!/usr/bin/env bash
# Benchmarks all 6 oblivious FK join variants + HASH_JOIN/NLJ baselines on Nexmark Q8 (small).
# 5 reps each, COMPILER mode, 1 worker thread. Emits one CSV line per run: config,rep,time,tuplesPerSecond
set -e
cd /Users/kaichi/dev/nebulastream
SYSTEST=./build-docker/nes-systests/systest/systest
DATA="$PWD/nes-systests/testdata"
REPS=5

run_one() { # $1=name $2=testLocation $3=strategy
  for rep in $(seq 1 $REPS); do
    wd="build-docker/nes-systests/benchall_$1"
    $SYSTEST -b -t "$2" --data "$DATA" --workingDir="$wd" \
      --optimizer join_strategy="$3" \
      -- --worker.default_query_execution.execution_mode=COMPILER \
         --worker.query_engine.number_of_worker_threads=1 > /dev/null 2>&1
    python3 -c "
import json
r = json.load(open('$wd/BenchmarkResults.json'))[0]
print(f\"$1,$rep,{r['time']},{r['tuplesPerSecond']}\")"
  done
}

run_one MERG_L2 nes-systests/benchmark_small/NexmarkFKMergL2.test:01 FK_MERG_L2
run_one MERG_L3 nes-systests/benchmark_small/NexmarkFKMergL3.test:01 FK_MERG_L3
run_one MERG_L4 nes-systests/benchmark_small/NexmarkFKMergL4.test:01 FK_MERG_L4
run_one SORT_L2 nes-systests/benchmark_small/NexmarkFKSortL2.test:01 FK_SORT_L2
run_one SORT_L3 nes-systests/benchmark_small/NexmarkFKSortL3.test:01 FK_SORT_L3
run_one SORT_L4 nes-systests/benchmark_small/NexmarkFKSortL4.test:01 FK_SORT_L4
run_one HASH_JOIN nes-systests/benchmark_small/Nexmark.test:05 HASH_JOIN
run_one NLJ nes-systests/benchmark_small/Nexmark.test:05 NESTED_LOOP_JOIN
