#include "Enclave.h"
#include "Enclave_t.h"
#include "OpaqueJoin.hpp"


#include <ObliviousComputationApproach/osort.h>
#include <ObliviousComputationApproach/parcompact.h>
#include <vector>

#ifdef MEASURE_LATENCY
#include "Commons/ECommon.h"
#endif

static int is_res_real(res_type t) {
    return get<0>(t).key != UINT32_MAX && get<1>(t).key != UINT32_MAX;
}

uint32_t OpaqueJoin::scan(table_id_t *t, std::vector<std::tuple<row_table_t, row_table_t>> &results) {
    row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
        .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};
    row_table_t t_pk = inf;
    uint32_t matches = 0;

    for (uint32_t i = 0; i < t->num_tuples; i++) {
        t_pk = conditional_select(t_pk, t->tuples[i], t->tuples[i].table_id);
        bool emit_real = t->tuples[i].table_id == 1 && t->tuples[i].key == t_pk.key;
        matches += emit_real;
        row_table_t t_r = conditional_select(t_pk, inf, emit_real);
        row_table_t t_s = conditional_select(t->tuples[i], inf, emit_real);
        std::tuple<row_table_t, row_table_t> tup = {t_r, t_s};
        results.push_back(tup);
    }

    return matches;
}

uint32_t OpaqueJoin::retire(window_t *window, uint32_t oldest_counter, uint32_t num_threads) {
    int to_remove = (int)(window->num_tuples - window->window_size);
    if (to_remove > 0) {
        bool *marked = (bool*) malloc(window->num_tuples * sizeof(*marked));
        size_t *marked_prefix_sums = (size_t*) malloc(window->num_tuples * sizeof(*marked_prefix_sums));
        invalidate_tuples(window, to_remove, oldest_counter, marked, marked_prefix_sums);
        parcompact_args<row_t> args = {
            .arr = window->tuples,
            .marked = marked,
            .marked_prefix_sums = marked_prefix_sums,
            .start = 0,
            .length = window->num_tuples,
            .offset = 0,
            .num_threads =  num_threads
        };
        orcpar<row_t>(&args);
        window->num_tuples -= to_remove;
        oldest_counter += to_remove;
        free(marked);
        free(marked_prefix_sums);
    }
    return oldest_counter;
}

void OpaqueJoin::invalidate_tuples(window_t *window, int to_remove, uint32_t oldest_counter, bool *marked,
    size_t *marked_prefix_sums) {

    size_t marked_so_far = 0;
    row_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX}, .key = UINT32_MAX, .payload = UINT32_MAX};
    for (uint32_t i = 0; i < window->num_tuples; i++) {
        uint32_t counter = (uint32_t) (window->tuples[i].ts.tv_sec >> 32);
        bool cur_marked;
        if (counter < oldest_counter + to_remove) {
            window->tuples[i] = inf;
            cur_marked = false;
        } else {
            window->tuples[i] = window->tuples[i];
            cur_marked = true;
        }
        marked_so_far += cur_marked;
        marked[i] = cur_marked;
        marked_prefix_sums[i] = marked_so_far;
    }
}

result_t *OpaqueJoin::l3_l4_join(relation_t *relR, relation_t *relS, joinconfig_t *cfg, bool L3)
{
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));

    uint32_t processed_r = 0, processed_s = 0;
    uint32_t oldest_r = 0, oldest_s = 0; // store counters for both windows to retire tuples with oldest counters
    uint32_t num_threads = cfg->NTHREADS;
    uint32_t matches = 0;

    relation_t *batchR = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batchS = (relation_t*) malloc(sizeof(relation_t));
    uint32_t batchSizeR, batchSizeS;

    std::vector<res_type> results;
    results.clear();

    // append R to W_R
    batchR->tuples = relR->tuples;
    batchR->num_tuples = windowR->window_size;
    for (uint32_t i = 0; i < batchR->num_tuples; i++) {
        batchR->tuples[i].ts.tv_sec = ((uint64_t)(processed_r+i) << 32) | (uint32_t) batchR->tuples[i].ts.tv_sec;
    }
    std::copy(batchR->tuples, batchR->tuples + batchR->num_tuples,
        windowR->tuples + windowR->num_tuples);
    windowR->num_tuples += batchR->num_tuples;
    processed_r += windowR->window_size;

    // append S to W_S
    batchS->tuples = relS->tuples;
    batchS->num_tuples = windowS->window_size;
    for (uint32_t i = 0; i < batchS->num_tuples; i++) {
        batchS->tuples[i].ts.tv_sec = ((uint64_t)(processed_s+i) << 32) | (uint32_t) batchS->tuples[i].ts.tv_sec;
    }
    std::copy(batchS->tuples, batchS->tuples + batchS->num_tuples,
        windowS->tuples + windowS->num_tuples);
    windowS->num_tuples += batchS->num_tuples;
    processed_s += windowS->window_size;
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
        batchSizeR = (processed_r + cfg->batchRSize < relR->num_tuples) ?
                     cfg->batchRSize : (relR->num_tuples - processed_r);
        batchR->tuples = relR->tuples + processed_r;
        batchR->num_tuples = batchSizeR;

        batchSizeS = (processed_s + cfg->batchSSize < relS->num_tuples) ?
                     cfg->batchSSize : (relS->num_tuples - processed_s);
        batchS->tuples = relS->tuples + processed_s;
        batchS->num_tuples = batchSizeS;

        uint32_t local_matches = 0;




        // merge S and W_R to T1
        table_id_t t1;
        append_to_window(&t1, batchS, false, windowR);

        // sort T1
        bitonic_sort<row_table_t, table_comp>
            (t1.tuples, t1.num_tuples, num_threads, false);
        t1.num_tuples = windowR->num_tuples + batchS->num_tuples;
        t1.tuples = (row_table_t*) realloc(t1.tuples, t1.num_tuples * sizeof(row_table_t));

        // scan T1
        local_matches += scan(&t1, results);


        // merge R with W_S to T2
        table_id_t t2;
        append_to_window(&t2, batchR, true, windowS);

        // sort T2
        bitonic_sort<row_table_t, table_comp>
            (t2.tuples, t2.num_tuples, num_threads, false);
        t2.num_tuples = windowS->num_tuples + batchR->num_tuples;
        t2.tuples = (row_table_t*) realloc(t2.tuples, t2.num_tuples * sizeof(row_table_t));

        // scan T2
        local_matches += scan(&t2, results);

        // merge R with S to T3
        table_id_t t3;
        append_to_window(&t3, batchR, true, batchS);

        // sort T3
        bitonic_sort<row_table_t, table_comp>
            (t3.tuples, t3.num_tuples, num_threads, false);
        t3.num_tuples = batchS->num_tuples + batchR->num_tuples;
        t3.tuples = (row_table_t*) realloc(t3.tuples, t3.num_tuples * sizeof(row_table_t));

        // scan T2
        local_matches += scan(&t3, results);


        // append R to W_R
        for (uint32_t i = 0; i < batchR->num_tuples; i++) {
            batchR->tuples[i].ts.tv_sec = ((uint64_t)(processed_r+i) << 32) | (uint32_t) batchR->tuples[i].ts.tv_sec;
        }
        std::copy(batchR->tuples, batchR->tuples + batchR->num_tuples,
            windowR->tuples + windowR->num_tuples);
        windowR->num_tuples += batchR->num_tuples;

        // append S to W_S
        for (uint32_t i = 0; i < batchS->num_tuples; i++) {
            batchS->tuples[i].ts.tv_sec = ((uint64_t)(processed_s+i) << 32) | (uint32_t) batchS->tuples[i].ts.tv_sec;
        }
        std::copy(batchS->tuples, batchS->tuples + batchS->num_tuples,
            windowS->tuples + windowS->num_tuples);
        windowS->num_tuples += batchS->num_tuples;

        // retire tuples in both windows
        oldest_r = retire(windowR, oldest_r, num_threads);
        oldest_s = retire(windowS, oldest_s, num_threads);

        // L3: trim dummy tuples
        if (L3) {
            // fill_with_dummies(results);
            bool *marked = (bool*) malloc(results.size() * sizeof(*marked));
            size_t *marked_prefix_sums = (size_t*) malloc(results.size() * sizeof(*marked_prefix_sums));
            size_t marked_so_far = 0;
            for (uint32_t i = 0; i < results.size(); i++) {
                bool cur_marked = is_res_real(results.at(i));
                marked_so_far += cur_marked;
                marked[i] = cur_marked;
                marked_prefix_sums[i] = marked_so_far;
            }
            struct parcompact_args<res_type> par_args = {
                .arr = results.data(),
                .marked = marked,
                .marked_prefix_sums = marked_prefix_sums,
                .start = 0,
                .length = results.size(),
                .num_threads = num_threads,
            };
            // orocpar<res_type>(&par_args);
            orcpar<res_type>(&par_args);
            free(marked);
            free(marked_prefix_sums);

            results.erase(results.end() - ((int)results.size()-local_matches), results.end());
        }

        emit_results(results);

        matches += local_matches;

        processed_r += batchSizeR;
        processed_s += batchSizeS;

        free(t1.tuples);
        free(t2.tuples);
        free(t3.tuples);
        results.clear();
#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            total_cycles += getCyclesSinceStart(start);
            current_measurements++;
            if (current_measurements == measurements) {
                uint64_t avg_cycles = total_cycles / current_measurements;
                logger(INFO, "Average latency cycles     : %lu [cycles]", avg_cycles);
                logger(INFO, "Average latency nanos      : %lu [ns]", 1000000000*avg_cycles/CPU_FREQ);
                logger(INFO, "Average latency nanos/tuple: %lu [ns]", 1000000000*avg_cycles/CPU_FREQ/(batchSizeR+batchSizeS));

            }
        }
#endif
    }
    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);

    //clean up
    free(batchR);
    free(batchS);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->sjr = timer;
    return joinresult;
}



void OpaqueJoin::append_to_window(table_id_t *merged, relation_t *batch, bool batch_is_r, window_t *window) {
    merged->num_tuples = next_power_of_two(window->num_tuples + batch->num_tuples);
    merged->tuples = (row_table_t*) malloc(merged->num_tuples*sizeof(row_table_t));
    for (uint32_t i = 0; i < window->num_tuples; i++) {
        merged->tuples[i].ts       = window->tuples[i].ts;
        merged->tuples[i].key      = window->tuples[i].key;
        merged->tuples[i].payload  = window->tuples[i].payload;
        merged->tuples[i].table_id = batch_is_r;
    }
    for (uint32_t i = window->num_tuples, j = 0; j < batch->num_tuples; i++, j++) {
        merged->tuples[i].ts       = batch->tuples[j].ts;
        merged->tuples[i].key      = batch->tuples[j].key;
        merged->tuples[i].payload  = batch->tuples[j].payload;
        merged->tuples[i].table_id = !batch_is_r;
    }
    for (uint32_t i = window->num_tuples+batch->num_tuples; i < merged->num_tuples; i++) {
        merged->tuples[i].ts       = {UINT64_MAX, UINT64_MAX};
        merged->tuples[i].key      = UINT32_MAX;
        merged->tuples[i].payload  = UINT32_MAX;
        merged->tuples[i].table_id = UINT8_MAX;
    }

}

void OpaqueJoin::append_to_window(table_id_t *merged, relation_t *batch, bool batch_is_r, relation_t *window) {
    merged->num_tuples = next_power_of_two(window->num_tuples + batch->num_tuples);
    merged->tuples = (row_table_t*) malloc(merged->num_tuples*sizeof(row_table_t));
    for (uint32_t i = 0; i < window->num_tuples; i++) {
        merged->tuples[i].ts       = window->tuples[i].ts;
        merged->tuples[i].key      = window->tuples[i].key;
        merged->tuples[i].payload  = window->tuples[i].payload;
        merged->tuples[i].table_id = batch_is_r;
    }
    for (uint32_t i = window->num_tuples, j = 0; j < batch->num_tuples; i++, j++) {
        merged->tuples[i].ts       = batch->tuples[j].ts;
        merged->tuples[i].key      = batch->tuples[j].key;
        merged->tuples[i].payload  = batch->tuples[j].payload;
        merged->tuples[i].table_id = !batch_is_r;
    }
    for (uint32_t i = window->num_tuples+batch->num_tuples; i < merged->num_tuples; i++) {
        merged->tuples[i].ts       = {UINT64_MAX, UINT64_MAX};
        merged->tuples[i].key      = UINT32_MAX;
        merged->tuples[i].payload  = UINT32_MAX;
        merged->tuples[i].table_id = UINT8_MAX;
    }

}

void OpaqueJoin::emit_results(const std::vector<res_type> & results)
{
    (void) (results);
    // logger(DBG, "Emit %d results", results.size());
    // for (auto tuple: results) {
    //     row_table_t r = get<0>(tuple);
    //     row_table_t s = get<1>(tuple);
    //     if (r.key != UINT32_MAX && s.key != UINT32_MAX) {
    //         logger(DBG, "key-val-val: %zu-%zu-%zu",
    //             r.key, r.payload, s.payload);
    //     }
    //
    // }
}
