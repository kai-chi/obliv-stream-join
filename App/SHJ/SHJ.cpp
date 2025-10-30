#include "SHJ.h"
#include "BucketChainingHashTable.hpp"
#include "DETBucketChainingHashTable.hpp"
#include "../Lib/Logger.h"
#include "AppTimers.h"
#include "Config.hpp"

static inline uint64_t clock_cycles() {
    unsigned int lo = 0;
    unsigned int hi = 0;
    __asm__ __volatile__ (
            "lfence;rdtsc;lfence" : "=a"(lo), "=d"(hi)
            );
    return ((uint64_t)hi << 32) | lo;
}

uint64_t getCyclesSinceStart(uint64_t start) {
    return clock_cycles() - start;
}

void SHJ::SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize)
{
    if (!htInstantiated) {
        logger(DBG, "Init SHJ: %s", algorithm.c_str());
        if (algorithm == "SHJ-L0") {
            DEThtR = new DETBucketChainingHashTable(1);
            DEThtS = new DETBucketChainingHashTable(0);
            htRMaxSize = windowRSize;
            htSMaxSize = windowSSize;
            htInstantiated = true;
        } else {
            htR = new BucketChainingHashTable(1);
            htS = new BucketChainingHashTable(0);
            htRMaxSize = windowRSize;
            htSMaxSize = windowSSize;
            htInstantiated = true;
        }
    } else {
        logger(DBG, "Init SHJ skipped");
    }
}

result_t * SHJ::join_st(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    (void) (config);

    uint64_t matches = 0;
    auto *eTimer = new AppTimers();
    auto *timer = (streamjoin_result_t *) calloc(1, sizeof(streamjoin_result_t));
    uint32_t processed_r  = 0, processed_s = 0;
    bool next_is_R;


    if (!htInitialized) {
        // initialize left HT
//        eTimer->startTimer(TIMER::LEFT_INIT_TIME);
        htR->initialize(4*htRMaxSize);
//        timer->leftInitTime = eTimer->stopTimer(TIMER::LEFT_INIT_TIME);

        // initialize right HT
//        eTimer->startTimer(TIMER::RIGHT_INIT_TIME);
        htS->initialize(4*htSMaxSize);
//        timer->rightInitTime = eTimer->stopTimer(TIMER::RIGHT_INIT_TIME);
        htInitialized = true;
    }

    for(size_t i = 0; i < htRMaxSize; i++) {
        htR->build(relR->tuples[i]);
        htRSize++;
        processed_r++;
    }

    for(size_t i = 0; i < htSMaxSize; i++) {
        htS->build(relS->tuples[i]);
        htSSize++;
        processed_s++;
    }

#ifdef MEASURE_LATENCY
    uint64_t total_cycles = 0;
    size_t current_measurements = 0;
    size_t measurements = 100;
    uint64_t start;
#endif

    eTimer->startTimer(TIMER::JOIN_TOTAL_TIME);



    while (processed_r < relR->num_tuples || processed_s < relS->num_tuples) {
#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            start = clock_cycles();
        }
#endif
        next_is_R = (relR->tuples[processed_r].ts.tv_sec * 1000000000L + relR->tuples[processed_r].ts.tv_nsec)
                  < (relS->tuples[processed_s].ts.tv_sec * 1000000000L + relS->tuples[processed_s].ts.tv_nsec);

        tuple_t t;
        if (next_is_R) {
            t = relR->tuples[processed_r];
        } else {
            t = relS->tuples[processed_s];
        }

        if (next_is_R) {
            // build left HT
            htR->build(t);
            htRSize++;

            // probe right HT
            matches += htS->probe(t);

            // invalidate retired tuple from left HT
            if (htRSize > htRMaxSize) {
                htR->deleteOldest(1);
                htRSize--;
            }
            processed_r++;

        } else {
            // build right HT
            htS->build(t);
            htSSize++;

            // probe left HT
            matches += htR->probe(t);

            // invalidate retired tuple from right HT
            if (htSSize > htSMaxSize) {
                htS->deleteOldest(1);
                htSSize--;
            }
            processed_s++;
        }
#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            total_cycles += getCyclesSinceStart(start);
            current_measurements++;
            if (current_measurements == measurements) {
                uint64_t avg_cycles = total_cycles / current_measurements;
                logger(INFO, "Average latency cycles: %lu [cycles]", avg_cycles);
                logger(INFO, "Average latency nanos : %lu [ns]", 1000000000*avg_cycles/CPU_FREQ);
            }
        }
#endif
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);
    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->nthreads = 0;
    joinresult->sjr = timer;
    delete eTimer;
    return joinresult;
}

result_t * SHJ::DETjoin_st(table_enc_t * DETrelR, table_enc_t * DETrelS, joinconfig_t * cfg)
{
    (void) (cfg);

    uint64_t matches = 0;
    auto *eTimer = new AppTimers();
    streamjoin_result_t *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t)); //(streamjoin_result_t *) calloc(1, sizeof(streamjoin_result_t));
    memset(timer, 0, sizeof(streamjoin_result_t));
    uint32_t processed_r  = 0, processed_s = 0;
    bool next_is_R;


    if (!htInitialized) {
        // initialize left HT
//        eTimer->startTimer(TIMER::LEFT_INIT_TIME);
        DEThtR->initialize(4*htRMaxSize);
//        timer->leftInitTime = eTimer->stopTimer(TIMER::LEFT_INIT_TIME);

        // initialize right HT
//        eTimer->startTimer(TIMER::RIGHT_INIT_TIME);
        DEThtS->initialize(4*htSMaxSize);
//        timer->rightInitTime = eTimer->stopTimer(TIMER::RIGHT_INIT_TIME);
        htInitialized = true;
    }

    for(size_t i = 0; i < htRMaxSize; i++) {
        DEThtR->build(DETrelR->tuples[i]);
        htRSize++;
        processed_r++;
    }

    for(size_t i = 0; i < htSMaxSize; i++) {
        DEThtS->build(DETrelS->tuples[i]);
        htSSize++;
        processed_s++;
    }

#ifdef MEASURE_LATENCY
    uint64_t total_cycles = 0;
    size_t current_measurements = 0;
    size_t measurements = 100;
    uint64_t start;
#endif

    eTimer->startTimer(TIMER::JOIN_TOTAL_TIME);

    while (processed_r < DETrelR->num_tuples || processed_s < DETrelS->num_tuples) {
#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            start = clock_cycles();
        }
#endif
        next_is_R = (DETrelR->tuples[processed_r].ts.tv_sec * 1000000000L + DETrelR->tuples[processed_r].ts.tv_nsec)
                  < (DETrelS->tuples[processed_s].ts.tv_sec * 1000000000L + DETrelS->tuples[processed_s].ts.tv_nsec);

        row_enc_t t;
        if (next_is_R) {
            t = DETrelR->tuples[processed_r];
        } else {
            t = DETrelS->tuples[processed_s];
        }

        if (next_is_R) {
            // build left HT
            DEThtR->build(t);
            htRSize++;

            // probe right HT
            matches += DEThtS->probe(t);

            // invalidate retired tuple from left HT
            if (htRSize > htRMaxSize) {
                DEThtR->deleteOldest(1);
                htRSize--;
            }
            processed_r++;

        } else {
            // build right HT
            DEThtS->build(t);
            htSSize++;

            // probe left HT
            matches += DEThtR->probe(t);

            // invalidate retired tuple from right HT
            if (htSSize > htSMaxSize) {
                DEThtS->deleteOldest(1);
                htSSize--;
            }
            processed_s++;
        }
#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            total_cycles += getCyclesSinceStart(start);
            current_measurements++;
            if (current_measurements == measurements) {
                uint64_t avg_cycles = total_cycles / current_measurements;
                logger(INFO, "Average latency cycles: %lu [cycles]", avg_cycles);
                logger(INFO, "Average latency nanos : %lu [ns]", 1000000000*avg_cycles/CPU_FREQ);
            }
        }
#endif
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);
    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->nthreads = 0;
    joinresult->sjr = timer;
    delete eTimer;
    return joinresult;
}

void logResults(std::string algorithm, result_t *res, joinconfig_t *cfg, uint32_t input_size)
{
    (void) (cfg);
    logger(INFO, "-- Log %s results --", algorithm.c_str());
    logger(INFO, "Join matches             : %lu", res->totalresults);
    logger(INFO, "joinTotalTime   [micros] : %lu", res->sjr->joinTotalTime);
    logger(INFO, "joinThroughput [M rec/s] : %.4lf", ((double) input_size / (double) res->sjr->joinTotalTime));
    logger(INFO, "--------------------------");
}