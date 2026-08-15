#ifndef CONFIG_HPP
#define CONFIG_HPP

// Compile-time switches, shared by App/ and Enclave/. Most of the actual
// join code branches on these with #ifdef rather than reading them at
// runtime, so changing any of these needs a rebuild (`make clean && make`).

// CPU clock in Hz, used to turn rdtsc cycle counts into nanoseconds for the
// per-tuple latency logging (grep CPU_FREQ - it's mostly under MEASURE_LATENCY
// blocks). Wrong value just skews those latency numbers, nothing else.
// Disable frequency scaling/turbo boost on the benchmark machine or this is
// meaningless regardless of what it's set to.
#ifndef CPU_FREQ
#define CPU_FREQ 2000000000
#endif

// Exactly one of SGX1/SGX2 must be defined - BTreeHashTable's timestamp
// packing and a few other spots key off which generation of SGX you're
// building for. This repo targets SGXv2 (the paper's benchmarks were run on
// SGXv2 hardware); flip these if you're stuck on older SGXv1 silicon.
#ifndef SGX1
// #define SGX1
#endif

#ifndef SGX2
#define SGX2
#endif

#if (((defined(SGX1)) && (defined(SGX2))) || ((!defined(SGX1)) && (!defined(SGX2))))
#error "Select EITHER SGX1 OR SGX2"
#endif

// Uncomment to turn on the oblivious B-Tree's internal access-count
// instrumentation (see the MEASURE_PERF blocks in BTree.cpp/BTreeOMAP.cpp) -
// useful for sanity-checking that a B-Tree operation really does touch a
// constant number of nodes, not for normal benchmark runs.
#ifndef MEASURE_PERF
//#define MEASURE_PERF
#endif

#ifndef JOIN_PERF
// #define JOIN_PERF
#endif // JOIN_PERF

// Not defined here, but referenced all over Enclave/ and App/SHJ/ under
// #ifdef MEASURE_LATENCY: turns on rdtsc-based average per-tuple latency
// logging inside the join loops. There's no toggle for it in this file -
// pass it on the command line instead, e.g. `make CFLAGS=-DMEASURE_LATENCY`
// (this is what experiments/commons.py's compile_app(flags=[...]) does).

#endif //CONFIG_HPP
