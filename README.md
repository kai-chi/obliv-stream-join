# tee-bench-obliv-stream

This is the code behind the paper *Privacy-Preserving Stream Joins*.

📄 **[Read the FULL VERSION of the paper (PDF)](paper_full.pdf)** - it includes the formal security proofs and additional experiments that the shorter conference version defers to it.

This README tells you how to build the repo, run a join, and reproduce the numbers in the paper.

## Repo layout

```
App/                    untrusted host application (CLI, data generation, timers)
  Lib/                  arg parsing, logging, synthetic data generators
  SHJ/                  non-enclave SHJ used for the insecure "SHJ" and "SHJ-L0" baselines
  OMAP/                 untrusted RAM store backing the enclave's ORAM
Enclave/                trusted code, compiled into the .signed.so enclave
  ObliviousSymmetricHashJoin/   SHJ-L1..L4
  ObliviousComputationApproach/ OCA: FK-EPHI/MERG-L2/L3/L4, NFK-JOIN-L2/L3 (OCAKras), oblivious sort/shuffle/merge/compact primitives
  Opaque/                       FK-SORT-L2/L3/L4 (the "resort everything every batch" baseline OCA is compared against)
  NestedLoopJoin/               NLJ-L4 (worst-case-padded nested loop baseline)
  HashTables/Oblivious/         OBLIWIND: oblivious B-tree (index tier) + oblivious min-heap (expiry tier)
Include/                shared data types (row_t, joinconfig_t, ...), AES, config flags
experiments/            python scripts that drive ./app and reproduce the paper's figures/tables
```

## Building it

You need real SGX hardware with SGXv2 support (the code is compiled with `SGX2` on by default).

Everything below assumes Ubuntu, since that's what `install_dependencies.sh` targets.

**1. System deps + SGX SDK.** 
Run:

```bash
./install_dependencies.sh
```

This needs sudo and will install a kernel driver, so read it before running rather than trusting this README blindly. When it's done:

```bash
source /opt/intel/sgxsdk/environment
```

**2. sgxssl.** The enclave links against Intel SGX SSL for crypto. Build it with:

```bash
./prepare_sgxssl.sh
```

This clones and builds `sgxssl` as a *sibling* of this repo (`../sgxssl`), not inside it — that's where the Makefile's `OPENSSL_PACKAGE` expects to find it. If you ever see link errors about missing `sgx_tsgxssl*` or `crypto` libs, check that `../sgxssl/Linux/package` actually exists before anything else.

**3. Build.**

```bash
make                        # hardware mode, debug build
make SGX_DEBUG=0             # hardware mode, release build
make clean                   # before switching build mode, always
```

This produces `app`, `enclave.so`, and `enclave.signed.so` in the repo root. The Makefile signs the enclave with the test key at `Enclave/Enclave_private_test.pem` — fine for local benchmarking, not for anything you'd ship.

Enclave heap/stack/thread-pool sizing lives in `Enclave/Enclave.config.xml`. If a run dies with an out-of-memory error inside the enclave (this happens with very large windows, since OBLIWIND's ORAM lives in enclave heap), bump `HeapMaxSize` there and rebuild.

## Running a join

```bash
./app --alg SHJ-L2 --dataset synth-1
```

`--dataset` sets a bunch of flags at once for a known config (see `App/Lib/commons.c`). You can also build your own stream by hand:

```bash
./app --alg FK-MERG-L4 --fk-join \
      --r-size 2000000 --s-size 2000000 \
      --r-rate 1024    --s-rate 1024 \
      --r-window 65536 --s-window 65536 \
      --r-batch 1024   --s-batch 1024
```

Run `./app -h` for the full flag list. A few things that aren't obvious from `-h`:

- `--fk-join` must be set for anything in the `FK-*` family (they assume a foreign-key relationship and will refuse to run otherwise). Leave it unset for `NFK-JOIN-*` / non-FK joins.
- `--no-sgx` runs outside the enclave. It's required for the insecure `SHJ` baseline and for `SHJ-L0` (both live in `App/SHJ/`, not `Enclave/`) — everything else needs to run inside SGX to mean anything.
- `--r-path` / `--s-path` load a stream from a file instead of generating one synthetically: one integer key per line, arrival timestamps are synthesized from `--r-rate`/`--s-rate`. This is what `--dataset tpch-1` expects under `data/customer_custkey_01.tbl` / `data/orders_custkey_01.tbl` — those files aren't included in this repo, so `tpch-1` won't run out of the box. Bring your own TPC-H `customer`/`orders` extract (just the `custkey` column, one per line) if you want that dataset.
- `--csv` switches log output to CSV, which is what the `experiments/*.py` scripts scrape for numbers.

### Algorithm reference

| `--alg` | Family | Leakage level | Needs `--fk-join`? | Runs where |
|---|---|---|---|---|
| `SHJ` | insecure baseline | none (plaintext) | no | `--no-sgx` only |
| `SHJ-L0` | SHJ | L0 (deterministic encryption) | no | `--no-sgx` only |
| `SHJ-L1` | SHJ | L1 (randomized encryption) | no | enclave |
| `SHJ-L2` | SHJ | L2 (oblivious lookups) | no | enclave |
| `SHJ-L3` | SHJ | L3 (oblivious batching) | no | enclave |
| `SHJ-L4` | SHJ | L4 (full obliviousness) | no | enclave |
| `NLJ-L4` | nested loop | L4 | no | enclave |
| `FK-EPHI-L2` | OCA | L2 | yes | enclave |
| `FK-MERG-L2` | OCA | L2 | yes | enclave |
| `FK-MERG-L3` | OCA | L3 | yes | enclave |
| `FK-MERG-L4` | OCA (flagship) | L4 | yes | enclave |
| `FK-SORT-L2` | Opaque-style baseline | L2 | yes | enclave |
| `FK-SORT-L3` | Opaque-style baseline | L3 | yes | enclave |
| `FK-SORT-L4` | Opaque-style baseline | L4 | yes | enclave |
| `NFK-JOIN-L2` | OCA (Kras) | L2 | no | enclave |
| `NFK-JOIN-L3` | OCA (Kras) | L3 | no | enclave |


### Reading the output

Each run prints something like:

```
Join matches             : 12345
totalInputTuples         : 4000000
joinTotalTime   [micros] : 987654
joinThroughput [M rec/s] : 4.0506
throughputJoin [K rec/s] : 4050.6
```

`joinTotalTime` is wall-clock for the whole streaming run, not per-tuple latency — the `experiments/*.py` scripts divide it by batch count to get per-batch latency where that matters. With `--csv`, the same fields come out as a comma-separated line instead, which is what the experiment scripts parse.

## Reproducing the paper's experiments

See [experiments/README.md](experiments/README.md) — it maps each figure/table in the paper to the script that generates it.

### Inside a real stream processor (NebulaStream)

The joins here run as a standalone benchmark. They are also ported into
NebulaStream, which puts them behind a real query, execution engine and sink
instead of a driver loop: [kai-chi/nebulastream](https://github.com/kai-chi/nebulastream).

Details of the NES experiment (Nexmark Q8 on Intel TDX) in
[experiments/nes](experiments/nes).


## License

Apache 2.0, see [LICENSE](LICENSE). Large parts of the SGX scaffolding (`Makefile`, `App/App.h`, edger8r-generated glue) are Intel's original SampleEnclave code.
