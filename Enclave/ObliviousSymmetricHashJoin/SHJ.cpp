#include "SHJ.h"

#include "Enclave.h"
#include "Commons/EnclaveTimers.h"
#include "../HashTables/NonOblivious/BucketChainingHashTable.hpp"
#include "../HashTables/Oblivious/BTree/BTreeHashTable.hpp"
#include "../Commons/ECommon.h"
#include "Config.hpp"


void SHJ::SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize)
{
    if (!htInstantiated) {
        logger(DBG, "Init SHJ: %s, htRMaxSize: %d, htSMaxSize: %d", algorithm.c_str(), windowRSize, windowSSize);
        if (algorithm == "SHJ_NO" || algorithm == "SHJ_NO_ST" "" || algorithm == "SHJ-L1") {
            htR = new BucketChainingHashTable(1);
            htS = new BucketChainingHashTable(0);
        } else if (algorithm == "SHJ_BTREE" || algorithm == "SHJ-L2" || algorithm == "SHJ-L3" || algorithm == "SHJ-L4") {
            logger(DBG, "new BTreeHashTables");
            htR = new BTreeHashTable(true);
            htS = new BTreeHashTable(false);
        } else {
            ocall_throw("Unsupported algorithm");
        }
        htRMaxSize = windowRSize;
        htSMaxSize = windowSSize;
        htInstantiated = true;
    } else {
        logger(DBG, "Init SHJ skipped");
    }
}

result_t * SHJ::join(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    uint64_t matches = 0;
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));

    eTimer->startTimer(TIMER::JOIN_TOTAL_TIME);

    if (!htInitialized) {
        // initialize left HT
#ifdef JOIN_PERF
        eTimer->startTimer(TIMER::LEFT_INIT_TIME);
#endif
        htR->initialize(4*htRMaxSize);
#ifdef JOIN_PERF
          timer->leftInitTime = eTimer->stopTimer(TIMER::LEFT_INIT_TIME);
#endif


        // initialize right HT
#ifdef JOIN_PERF
          eTimer->startTimer(TIMER::RIGHT_INIT_TIME);
#endif

        htS->initialize(4*htSMaxSize);
#ifdef JOIN_PERF
          timer->rightInitTime = eTimer->stopTimer(TIMER::RIGHT_INIT_TIME);
#endif

        htInitialized = true;
    }

    // build left HT
    logger(DBG, "%s::%s build left HT with left stream", __FILE__, __func__);
#ifdef JOIN_PERF
      eTimer->startTimer(TIMER::LEFT_BUILD_TIME);
#endif

    htR->buildNodes(relR);
#ifdef JOIN_PERF
      timer->leftBuildTime = eTimer->stopTimer(TIMER::LEFT_BUILD_TIME);
#endif

    htRSize += relR->num_tuples;

    // probe right HT
    logger(DBG, "%s::%s probe right HT with left stream", __FILE__, __func__);
#ifdef JOIN_PERF
      eTimer->startTimer(TIMER::LEFT_PROBE_TIME);
#endif

    matches += htS->probeNodes(relR);
#ifdef JOIN_PERF
      timer->leftProbeTime = eTimer->stopTimer(TIMER::LEFT_PROBE_TIME);
#endif


    //build right HT
    logger(DBG, "%s::%s build right HT with right stream", __FILE__, __func__);
#ifdef JOIN_PERF
      eTimer->startTimer(TIMER::RIGHT_BUILD_TIME);
#endif

    htS->buildNodes(relS);
#ifdef JOIN_PERF
      timer->rightBuildTime = eTimer->stopTimer(TIMER::RIGHT_BUILD_TIME);
#endif

    htSSize += relS->num_tuples;

    //probe left HT
    logger(DBG, "%s::%s probe left HT with right stream", __FILE__, __func__);
#ifdef JOIN_PERF
      eTimer->startTimer(TIMER::RIGHT_PROBE_TIME);
#endif

    matches += htR->probeNodes(relS);
#ifdef JOIN_PERF
      timer->rightProbeTime = eTimer->stopTimer(TIMER::RIGHT_PROBE_TIME);
#endif


    // make sure the hash tables are not too big
    if (htRSize > htRMaxSize) {
#ifdef JOIN_PERF
          eTimer->startTimer(TIMER::LEFT_DELETE_TIME);
#endif

        vector<type_key> deleted = htR->deleteOldest(htRSize - htRMaxSize);
#ifdef JOIN_PERF
          timer->leftDeleteTime = eTimer->stopTimer(TIMER::LEFT_DELETE_TIME);
#endif

        htRSize -= (uint32_t) deleted.size();
    }

    if (htSSize > htSMaxSize) {
#ifdef JOIN_PERF
          eTimer->startTimer(TIMER::RIGHT_DELETE_TIME);
#endif

        vector<type_key> deleted = htS->deleteOldest(htSSize - htSMaxSize);
#ifdef JOIN_PERF
          timer->rightDeleteTime = eTimer->stopTimer(TIMER::RIGHT_DELETE_TIME);
#endif

        htSSize -= (uint32_t) deleted.size();
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);
    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->nthreads = config->NTHREADS;
    joinresult->sjr = timer;
    return joinresult;
}

result_t * SHJ::join_st(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    (void) (config);

    uint64_t matches = 0;
    auto *timer = (streamjoin_result_t *) calloc(1, sizeof(streamjoin_result_t));
    uint32_t processed_r  = 0, processed_s = 0;

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

    for (size_t i = 0; i < htRMaxSize && i < relR->num_tuples; i++) {
        tuple_t t = relR->tuples[i];
        htR->build(t);
    }
    htRSize = htRMaxSize;
    processed_r = htRMaxSize;

    for (size_t i = 0; i < htSMaxSize && relS->num_tuples; i++) {
        tuple_t t = relS->tuples[i];
        htS->build(t);
    }
    htSSize = htSMaxSize;
    processed_s = htSMaxSize;
    logger(INFO, "Windows filled with tuples");

    eTimer->startTimer(TIMER::JOIN_TOTAL_TIME);


    bool next_is_R;

#ifdef MEASURE_LATENCY
        uint64_t total_cycles = 0;
        size_t current_measurements = 0;
        size_t measurements = 100;
        uint64_t start;
#endif


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
#ifdef JOIN_PERF
            eTimer->startTimer(LEFT_BUILD_TIME);
            htR->build(t);
            timer->leftBuildTime += eTimer->stopTimer(LEFT_BUILD_TIME);
#else
            htR->build(t);
#endif //JOIN_PERF
            htRSize++;

            // probe right HT
#ifdef JOIN_PERF
            eTimer->startTimer(RIGHT_PROBE_TIME);
            matches += htS->probe(t);
            timer->rightProbeTime += eTimer->stopTimer(RIGHT_PROBE_TIME);
#else
            matches += htS->probe(t);
#endif //JOIN_PERF

            // invalidate retired tuple from left HT
            if (htRSize > htRMaxSize) {
#ifdef JOIN_PERF
                eTimer->startTimer(TIMER::LEFT_DELETE_TIME);
                htR->deleteOldest(1);
                timer->leftDeleteTime += eTimer->stopTimer(LEFT_DELETE_TIME);
#else
                htR->deleteOldest(1);
#endif //JOIN_PERF
                htRSize--;
            }
            processed_r++;

        } else {
            // build right HT
#ifdef JOIN_PERF
            eTimer->startTimer(RIGHT_BUILD_TIME);
            htS->build(t);
            timer->rightBuildTime += eTimer->stopTimer(RIGHT_BUILD_TIME);
#else
            htS->build(t);
#endif // JOIN_PERF
            htSSize++;

            // probe left HT
#ifdef JOIN_PERF
            eTimer->startTimer(LEFT_PROBE_TIME);
            matches += htR->probe(t);
            timer->leftProbeTime += eTimer->stopTimer(LEFT_PROBE_TIME);
#else
            matches += htR->probe(t);
#endif // JOIN_PERF

            // invalidate retired tuple from right HT
            if (htSSize > htSMaxSize) {
#ifdef JOIN_PERF
                eTimer->startTimer(RIGHT_DELETE_TIME);
                htS->deleteOldest(1);
                timer->rightDeleteTime += eTimer->stopTimer(RIGHT_DELETE_TIME);
#else
                htS->deleteOldest(1);
#endif // JOIN_PERF
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
    return joinresult;
}

void emit_result(type_key key, type_value payload1, type_value payload2) {
    (void) key;
    (void) payload1;
    (void) payload2;

    // Do nothing as we are not passing the tuples to any downstream operator.
    // Filter out dummy tuples, if desired.
    // Here, we would also perform Spark-like selection to validate the tuples'
    //  timestamps with the watermarks (same as Spark Streaming does).
}

result_t * SHJ::join_l3(relation_t *relR, relation_t *relS, joinconfig_t *cfg, bool padding)
{
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));

    uint32_t processed_r = 0, processed_s = 0;
    uint32_t matches = 0;
    row_idx_t dummy_row = {.ts = {0,0}, .key = UINT32_MAX, .payload = UINT32_MAX, .index = UINT32_MAX};

    relation_t *batchR = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batchS = (relation_t*) malloc(sizeof(relation_t));
    requests_t *requests = (requests_t*) malloc(sizeof(requests_t));

    if (!htInitialized) {
        htR->initialize(4*htRMaxSize);
        htS->initialize(4*htSMaxSize);
        htInitialized = true;
    }

    for (size_t i = 0; i < htRMaxSize; i++) {
        tuple_t t = relR->tuples[i];
        htR->build(t);
    }
    htRSize = htRMaxSize;
    processed_r = htRMaxSize;

    for (size_t i = 0; i < htSMaxSize; i++) {
        tuple_t t = relS->tuples[i];
        htS->build(t);
    }
    htSSize = htSMaxSize;
    processed_s = htSMaxSize;
    logger(INFO, "Windows filled with tuples");

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
        uint32_t batchSizeR = (processed_r + cfg->batchRSize < relR->num_tuples)
                                  ? cfg->batchRSize
                                  : (relR->num_tuples - processed_r);
        batchR->tuples = relR->tuples + processed_r;
        batchR->num_tuples = batchSizeR;

        uint32_t batchSizeS = (processed_s + cfg->batchSSize < relS->num_tuples)
                                  ? cfg->batchSSize
                                  : (relS->num_tuples - processed_s);
        batchS->tuples = relS->tuples + processed_s;
        batchS->num_tuples = batchSizeS;

        // leave space for one dummy tuple at the end
        uint32_t requests_size = batchSizeR+batchSizeS+1;
        requests->tuples = (row_idx_t*) malloc(requests_size * sizeof(row_idx_t));
        requests->num_tuples = requests_size;
        uint32_t m = 0;

        for (uint32_t i = 0; i < batchR->num_tuples; i++) {
            // insert batch to W_R
            htR->build(batchR->tuples[i]);
            // get key sizes for R tuples in W_S
            uint32_t weight = htS->getSize(batchR->tuples[i].key);
            if (weight == 0) {
                requests->tuples[i].key = UINT32_MAX;
                requests->tuples[i].payload = UINT32_MAX;
                requests->tuples[i].ts = {0,0};
                requests->tuples[i].index = UINT32_MAX;
                requests->tuples[i].table_id = UINT32_MAX;
            } else {
                requests->tuples[i].key = batchR->tuples[i].key;
                requests->tuples[i].payload = batchR->tuples[i].payload;
                requests->tuples[i].ts = batchR->tuples[i].ts;
                requests->tuples[i].index = m;
                requests->tuples[i].table_id = 0;
            }
            m += weight;
        }

        // insert batch to W_S
        for (uint32_t i = 0; i < batchS->num_tuples; i++) {
            htS->build(batchS->tuples[i]);
        }

        // get the sizes for S tuples in W_R
        for (uint32_t i = 0; i < batchS->num_tuples; i++) {
            uint32_t weight = htR->getSize(batchS->tuples[i].key);
            if (weight == 0) {
                requests->tuples[i].key = UINT32_MAX;
                requests->tuples[i].payload = UINT32_MAX;
                requests->tuples[i].ts = {0,0};
                requests->tuples[i].index = UINT32_MAX;
                requests->tuples[i].table_id = UINT32_MAX;
            } else {
                requests->tuples[i + batchR->num_tuples].key = batchS->tuples[i].key;
                requests->tuples[i + batchR->num_tuples].payload = batchS->tuples[i].payload;
                requests->tuples[i + batchR->num_tuples].ts = batchS->tuples[i].ts;
                requests->tuples[i + batchR->num_tuples].index = m;
                requests->tuples[i + batchR->num_tuples].table_id = 1;
            }
            m += weight;
        }

        // add padding
        requests->tuples[requests_size-1] = dummy_row;
        requests->tuples[requests_size-1].index = m;
        // add worst-case padding
        if (padding) {
            m = (batchSizeR*batchSizeS);
        } else {
            m = m;
        }

        // expand the input table
        oblivious_expansion(requests, m);

        // TODO: search the windows
        std::vector<std::tuple<row_t, row_t>> res;
        for (uint32_t i = 0; i < m; i++) {
            row_idx_t t = requests->tuples[i];
            uint32_t valR = htR->probe(t.key, t.ts);
            uint32_t valS = htS->probe(t.key, t.ts);
            if (t.table_id == 0) {
                emit_result(t.key, t.payload, valS);
            } else {
                emit_result(t.key, t.payload, valR);
            }
        }
        matches += m;

        // obliviously select the result tuples
        // retire tuples in both windows
        if (htR->size > htRMaxSize) {
            htR->deleteOldest(htR->size - htRMaxSize);
        }
        if (htS->size > htSMaxSize) {
            htS->deleteOldest(htS->size - htSMaxSize);
        }

        // emit res tuples

        processed_r += batchSizeR;
        processed_s += batchSizeS;

        free(requests->tuples);
#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            total_cycles += getCyclesSinceStart(start);
            current_measurements++;
            uint64_t avg_cycles = total_cycles / current_measurements;
            logger(INFO, "[%d] Average latency cycles     : %lu [cycles]", current_measurements, avg_cycles);
            logger(INFO, "[%d] Average latency nanos      : %lu [ns]", current_measurements, 1000000000*avg_cycles/CPU_FREQ);
            logger(INFO, "[%d] Average latency nanos/tuple: %lu [ns]", current_measurements, 1000000000*avg_cycles/CPU_FREQ/(batchSizeR+batchSizeS));
            if (current_measurements == measurements) {

                logger(INFO, "Average latency cycles     : %lu [cycles]", avg_cycles);
                logger(INFO, "Average latency nanos      : %lu [ns]", 1000000000*avg_cycles/CPU_FREQ);
                logger(INFO, "Average latency nanos/tuple: %lu [ns]", 1000000000*avg_cycles/CPU_FREQ/(batchSizeR+batchSizeS));
                return nullptr;
            }
        }
#endif
    }
    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);

    //clean up
    free(batchR);
    free(batchS);
    free(requests);

    result_t *joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->sjr = timer;
    return joinresult;
}

inline uint32_t prev_pow_of_two(uint32_t x) {
    uint32_t y = 1;
    while (y < x) y <<= 1;
    return y >>= 1;
}

bool func_comp(const row_idx_t & tmp1, const row_idx_t & tmp2)
{
    if (tmp1.key == UINT32_MAX) return false;
    if (tmp2.key == UINT32_MAX) return true;
    return tmp1.index < tmp2.index;

}

void SHJ::bitonic__compare(requests_t *requests, bool ascend, uint32_t i, uint32_t j) {
    row_idx_t tmp1 = requests->tuples[i];
    row_idx_t tmp2 = requests->tuples[j];

    bool condition = func_comp(tmp1, tmp2);
    if (!condition == ascend) {
        requests->tuples[i] = tmp2;
        requests->tuples[j] = tmp1;
    } else {
        requests->tuples[i] = tmp1;
        requests->tuples[j] = tmp2;
    }

}

void SHJ::bitonic__merge(requests_t *requests, bool ascend, uint32_t lo, uint32_t hi) {
    if (hi <= lo + 1) return;

    uint32_t mid_len = prev_pow_of_two(hi - lo);

    for (uint32_t i = lo; i < hi - mid_len; i++)
        bitonic__compare(requests, ascend, i, i + mid_len);
    bitonic__merge(requests, ascend, lo, lo + mid_len);
    bitonic__merge(requests, ascend, lo + mid_len, hi);
}

void SHJ::bitonic__sort(requests_t *requests, bool ascend, uint32_t lo, uint32_t hi) {
    if (hi == UINT32_MAX) hi = requests->num_tuples;

    uint32_t mid = lo + (hi - lo) / 2;

    if (mid == lo) return;

    bitonic__sort(requests, !ascend, lo, mid);
    bitonic__sort(requests, ascend, mid, hi);
    bitonic__merge(requests, ascend, lo, hi);
}

void SHJ::oblivious_distribute(requests_t *requests, uint32_t m) {
    bitonic__sort(requests);

    // resize requests
    requests->tuples = (row_idx_t*) realloc(requests->tuples, m * sizeof(row_idx_t));
    if (m > requests->num_tuples)
        memset((row_idx_t*)requests->tuples + requests->num_tuples, 0, sizeof(row_idx_t)*(m - requests->num_tuples));
    requests->num_tuples = m;

    for (int j = (int) prev_pow_of_two(m); j >= 1; j /= 2) {
        for (int i = (int)m - j - 1; i >= 0; i--) {
            row_idx_t t = requests->tuples[i];
            uint32_t dest_i = t.index;
            // assert(dest_i < m);
            row_idx_t t1 = requests->tuples[i+j];
            if (dest_i >= (uint32_t)(i + j)) {
                assert(t1.key == UINT32_MAX);
                requests->tuples[i] = t1;
                requests->tuples[i+j] = t;
            }
            else {
                requests->tuples[i] = t;
                requests->tuples[i+j] = t1;
            }
        }
    }
}

void SHJ::oblivious_expansion(requests_t *requests, uint32_t m) {
    // oblivious distribution
    oblivious_distribute(requests, m);

    row_idx_t last = {.key = 0};
    int dupl_off = 0, block_off = 0;
    for (uint32_t i = 0; i < m; i++) {
        row_idx_t e = requests->tuples[i];
        if (e.key != UINT32_MAX) {
            if (i != 0 && e.key != last.key)
                block_off = 0;
            last = e;
            dupl_off = 0;
        }
        else {
            assert(i != 0);
            e = last;
        }
        e.index += dupl_off;
        // e.t1index = int(block_off / e.block_height) +
        //             (block_off % e.block_height) * e.block_width;
        dupl_off++;
        block_off++;
        requests->tuples[i] = e;
    }


}
