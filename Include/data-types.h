#ifndef DATA_TYPES_H
#define DATA_TYPES_H

// Shared types between App/ and Enclave/. This header started life in the
// original (static-join) TeeBench and still carries that project's types
// around (join_result_t, hw_counters_t, numa_strategy_t, rusage_reduced_t,
// timers_t) even though the stream joins in this repo don't use them - they
// only matter if you go digging in App/SHJ or old partitioned-join code.
// The types that actually matter for streaming are row_t/table_t (a stream
// tuple / a materialized batch of them), joinconfig_t (what the CLI passes
// down to a join), and result_t/streamjoin_result_t (what comes back out).

#include <stdint.h>

#if !defined PRId64
# undef PRId64
# define PRId64 "lld"
#endif

#if !defined PRIu64
# undef PRIu64
# define PRIu64 "llu"
#endif

#ifndef B_TO_MB
#define B_TO_MB(X) ((double)X/1024.0/1024.0)
#endif

typedef uint32_t type_key;
typedef uint32_t type_value;

typedef struct row_t tuple_t;
typedef struct output_row_t output_tuple_t;
typedef struct output_list_t output_list_t;
typedef struct output_t output_t;
typedef struct table_t relation_t;
typedef struct result_t result_t;
typedef struct threadresult_t threadresult_t;
typedef struct joinconfig_t joinconfig_t;
typedef struct join_result_t join_result_t;
typedef struct streamjoin_result_t streamjoin_result_t;
typedef struct hw_counters_t hw_counters_t;
typedef struct timespec_t timespec_t;
typedef struct row_table_t row_table_t;

struct timespec_t {
    uint64_t tv_sec;
    uint64_t tv_nsec;
};

// row_t plus an index into the batch it came from and which side (R/S) it's
// on. SHJ.cpp's oblivious_expansion/oblivious_distribute build arrays of
// these to track a tuple's join degree through the L3/L4 request-padding
// dance without losing track of which original tuple a request belongs to.
struct row_idx_t {
    timespec_t ts;
    type_key key;
    type_value payload;
    uint32_t index;
    uint8_t table_id; // R - 0, S - 1
};

// row_t tagged with which stream it came from. This is the element type OCA
// and Opaque sort/merge/scan together (table_id_t below) - once R and S
// tuples are interleaved in one sorted array, table_id is what tells the
// scan step which side it's looking at.
struct row_table_t {
    timespec_t ts;
    type_key key;
    type_value payload;
    uint8_t table_id; // R - 0, S - 1
};

// One stream tuple: arrival timestamp, key, payload. This is what a window
// or a batch is an array of.
struct row_t {
    timespec_t ts;
    type_key key;
    type_value payload;
};

// A materialized chunk of a stream - a generated dataset, a batch read off
// disk, or (in App.cpp) the whole synthetic stream before it gets fed in
// batch-by-batch.
struct table_t {
    struct row_t* tuples;
    uint32_t num_tuples;
};

// Like table_t but of tagged row_table_t - the merged/sorted R+S array OCA
// and Opaque build each round before scanning it for matches.
struct table_id_t {
    struct row_table_t* tuples;
    uint32_t num_tuples;
};

// row_t with key/payload replaced by 16-byte AES-DET ciphertext. Only used
// by SHJ-L0 (App/SHJ/SHJ.cpp's DETjoin_st) - equal plaintexts encrypt to
// equal ciphertext under DET, so probing still works, it's just comparing
// bytes instead of ints.
struct row_enc_t {
    timespec_t ts;
    uint8_t key[16];
    uint8_t payload[16];
};

struct table_enc_t {
    struct row_enc_t* tuples;
    uint32_t num_tuples;
};

// A stream's sliding window, as OCA keeps it: a flat array sized for the
// worst case. `window_size` is the configured window (--r-window/--s-window);
// `capacity` is window_size + batch size, i.e. room for one more batch to
// land before retire() trims the window back down; `num_tuples` is how many
// slots are actually occupied right now, so it swings between window_size
// and capacity as batches get appended and old tuples get retired.
struct window_t {
    struct row_t* tuples;
    uint32_t num_tuples; // number of tuples currently in the window
    uint32_t window_size; // the size of window specified by user
    uint32_t capacity; // the max number of tuples the window can have at any point of time
};


struct output_row_t {
    timespec_t tsR;
    type_key   keyR;
    type_value valR;

    timespec_t tsS;
    type_key   keyS;
    type_value valS;
};

struct output_list_t {
    type_key key;
    type_value Rpayload;
    type_value Spayload;
    struct output_list_t * next;
};

struct output_t {
    output_list_t * list;
    uint64_t size;
};

// One entry in Enclave.cpp's sgx_algorithms[] dispatch table: a --alg name
// and the wrapper function it calls. ecall_join does a linear name lookup
// against this array.
typedef struct algorithm_t {
    char name[128];
    result_t *  (*join)(struct table_t*, struct table_t*, joinconfig_t *);
} algorithm_t;

/** Holds the join results of a thread */
struct threadresult_t {
    uint64_t  nresults;
    output_list_t *   results;
    uint32_t threadid;
};

// What a join call hands back. For the stream joins in this repo only
// totalresults and sjr are actually populated - resultlist/nthreads/
// throughput/jr/resulttable/totalJoinTime are static-join-era fields left
// over from before streaming existed and stay zero/null here.
struct result_t {
    uint64_t                totalresults;
    struct threadresult_t * resultlist;
    int                     nthreads;
    double                  throughput;
    const join_result_t *   jr;
    streamjoin_result_t *   sjr;
    relation_t *            resulttable;
    uint64_t                totalJoinTime;
};

struct streamjoin_result_t {
    // Wall-clock time for the whole streaming run, in microseconds (despite
    // what this used to say - it comes from EnclaveTimers, which is backed
    // by ocall_get_system_micros). logResults() prints it as
    // "joinTotalTime [micros]", so trust that label over this comment if
    // they ever disagree again.
    uint64_t joinTotalTime;

};

struct join_result_t {
    uint64_t inputTuplesR;
    uint64_t inputTuplesS;
    uint64_t matches;
    uint64_t phase1Cycles;
    uint64_t phase2Cycles;
    uint64_t phase3Cycles;
    uint64_t totalCycles;
    uint64_t phase1Time;
    uint64_t phase2Time;
    uint64_t totalTime;
    uint64_t ** partitionTime;
    int hwFlag;
    hw_counters_t * phase1HwCounters;
    hw_counters_t * phase2HwCounters;
    hw_counters_t * totalHwCounters;


#ifdef COUNT_SCANNED_TUPLES
    uint64_t scanned_tuples;
#endif
};

struct hw_counters_t {
    uint64_t l3CacheMisses;
    double l3HitRatio;
    uint64_t l2CacheMisses;
    double l2HitRatio;
    double ipc;
    uint64_t ir;
    uint32_t ewb;
    uint32_t voluntaryContextSwitches;
    uint32_t involuntaryContextSwitches;
    uint64_t userCpuTime; // in usec
    uint64_t systemCpuTime; // in usec
};

struct timers_t {
    uint64_t total, timer1, timer2, timer3, timer4;
    uint64_t start, end;
};

struct rusage_reduced_t {
    signed long ru_utime_sec;
    signed long ru_utime_usec;
    signed long ru_stime_sec;
    signed long ru_stime_usec;
    long ru_minflt;
    long ru_majflt;
    long ru_nvcsw;
    long ru_nivcsw;
};

enum numa_strategy_t {RANDOM, RING, NEXT};

// Everything App.cpp knows about the run, passed down through ecall_join to
// whichever algorithm gets picked. totalInputTuples starts out as r_size +
// s_size from the CLI, but most wrappers immediately correct it (subtracting
// the window sizes, since those tuples are pre-loaded to warm up the window
// rather than actually streamed) before using it to compute throughput - so
// don't assume it still matches the --r-size/--s-size you passed in by the
// time logResults() prints it.
struct joinconfig_t {
    int NTHREADS;

    int WRITETOFILE;

    int LOG;
    int CSV_PRINT;

    uint32_t windowRSize;
    uint32_t windowSSize;
    uint32_t batchRSize;
    uint32_t batchSSize;
    int fkJoin;
    uint32_t totalInputTuples;
};

#endif //DATA_TYPES_H
