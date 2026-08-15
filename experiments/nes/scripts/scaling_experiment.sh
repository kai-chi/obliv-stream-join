#!/usr/bin/env bash
# Density-scaling experiment: 10%..100% per-tuple samples of the large Nexmark
# dataset x 7 join algorithms. Host-side driver: prepares sampled datasets,
# then runs the measurement loop inside the dev container.
set -u
cd "$HOME/nebulastream"
D=build/nes-systests/testdata/large/nexmark

echo "[prep] generating sampled datasets"
for p in 1 2 3 4 5 6 7 8 9 10; do
  Q=scaling-data/p${p}/large/nexmark
  mkdir -p "$Q"
  [ -s "$Q/bid_653M.csv" ] || awk -v p=$p 'NR%10<p' "$D/bid_653M.csv" > "$Q/bid_653M.csv"
  [ -s "$Q/auction_modified_74M.csv" ] || awk -v p=$p 'NR%10<p' "$D/auction_modified_74M.csv" > "$Q/auction_modified_74M.csv"
done
echo "[prep] done: $(du -sh scaling-data | cut -f1)"

docker run --rm --workdir "$PWD" -v "$PWD:$PWD" nebulastream/nes-development:local \
  bash scaling_inner.sh

echo "ALL_DONE"
