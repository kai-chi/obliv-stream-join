#include "NLJ.hpp"
#include "Commons/EnclaveTimers.h"
#include "Enclave.h"
#ifdef MEASURE_LATENCY
#include "Commons/ECommon.h"
#endif

result_t * NLJ_ST::join(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    (void)(config);
    uint32_t matches = 0;
    auto *eTimer = new EnclaveTimers();
    auto *timer = (streamjoin_result_t *) calloc(1, sizeof(streamjoin_result_t));
    uint32_t processed_r  = 0, processed_s = 0;
    bool next_is_R;

    for (size_t i = 0; i < windowR->get_capacity(); i++) {
        windowR->push_back(relR->tuples[i]);
        processed_r++;
    }

    for (size_t i = 0; i < windowS->get_capacity(); i++) {
        windowS->push_back(relS->tuples[i]);
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
            // build left window
            windowR->push_back(t);

            // probe right window
            vector<row_t> res;
            res = windowS->fullScan(t.key, &matches);
            emit_results(res);

            processed_r++;

        } else {
            // build right window
            windowS->push_back(t);

            // probe left window
            vector<row_t> res;
            res = windowR->fullScan(t.key, &matches);
            emit_results(res);

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

void NLJ_ST::emit_results(vector<row_t>& res)
{
    (void) (res);
}
