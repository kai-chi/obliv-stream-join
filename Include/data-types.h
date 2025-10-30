#ifndef DATA_TYPES_H
#define DATA_TYPES_H

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

struct row_idx_t {
    timespec_t ts;
    type_key key;
    type_value payload;
    uint32_t index;
    uint8_t table_id; // R - 0, S - 1
};

struct row_table_t {
    timespec_t ts;
    type_key key;
    type_value payload;
    uint8_t table_id; // R - 0, S - 1
};

struct row_t {
    timespec_t ts;
    type_key key;
    type_value payload;
};

struct table_t {
    struct row_t* tuples;
    uint32_t num_tuples;
};

struct table_id_t {
    struct row_table_t* tuples;
    uint32_t num_tuples;
};

struct row_enc_t {
    timespec_t ts;
    uint8_t key[16];
    uint8_t payload[16];
};

struct table_enc_t {
    struct row_enc_t* tuples;
    uint32_t num_tuples;
};
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

/** Type definition for join results. */
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
    // All timers are in milliseconds
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

/** Join configuration parameters. */
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
