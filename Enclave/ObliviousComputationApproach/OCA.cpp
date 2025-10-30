//
// Created by kaichi on 11.10.24.
//

#include "OCA.h"

#include <Enclave_t.h>

#include "ocompact.h"
#include "oshuffle.h"
#include "omerge.h"
#include "osort.h"
#include "threading.h"
#include <pthread.h>
#include "parcompact.h"
#include "outil.h"
#ifdef MEASURE_LATENCY
#include "Commons/ECommon.h"
#endif

void emit_results(const vector<res_type> & results);

static int is_res_real(res_type t) {
    return get<0>(t).key != UINT32_MAX && get<1>(t).key != UINT32_MAX;
}


bool inline is_power_of_two(uint32_t n) {
    return n > 0 && (n & (n - 1)) == 0;
}

static bool tid_comp(row_table_t t1, row_table_t t2) {
    if (t1.key == t2.key) {
        return t1.table_id < t2.table_id;
    } else
        return t1.key < t2.key;
}

static uint32_t row_t_key(row_t r) {
    return r.key;
}

void debug_results(const vector<res_type> & vector) {
    logger(DBG, "%zu Results: ", vector.size());
    for (uint32_t i = 0; i < vector.size(); i++) {
        logger(DBG, "[%02d] key-val-val: %zu-%zu-%zu", i, get<0>(vector[i]).key, get<0>(vector[i]).payload, get<1>(vector[i]).payload);
    }
}

void debug_results2(const vector<restype> & vector) {
    logger(DBG, "%zu Results: ", vector.size());
    for (uint32_t i = 0; i < vector.size(); i++) {
        logger(DBG, "[%02d] key-val-val: %zu-%zu-%zu", i, get<0>(vector[i]).key, get<0>(vector[i]).payload, get<1>(vector[i]).payload);
    }
}

void debug_window(window_t* window) {
    logger(DBG, "Window[%zu]: ", window->num_tuples);
    for (uint32_t i = 0; i < window->num_tuples; i++) {
        logger(DBG, "key-val: %zu-%zu", window->tuples[i].key, window->tuples[i].payload);
    }
}

static void *start_thread_work(void* args) {
    (void)(args);
    /* Start work. */
    thread_start_work();
    return 0;
}

void OCA::oblivious_shuffle(window_t *window, size_t num_threads) {
    bool *marked = (bool*) malloc(window->num_tuples * sizeof(*marked));
    if (!marked) {
        logger(ERROR,"malloc marked arr");
        ocall_exit(1);
    }
    size_t *marked_prefix_sums =
        (size_t *) malloc(window->num_tuples * sizeof(*marked_prefix_sums));
    if (!marked_prefix_sums) {
        logger(ERROR,"malloc marked prefix sums arr");
        ocall_exit(1);
    }
    struct shuffle_args<row_t> args = {
        .arr = window->tuples,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = 0,
        .length = window->num_tuples,
        .num_threads = num_threads,
    };

    if (1lu << log2ll(args.length) != args.length) {
        logger(ERROR, "Length must be a multiple of 2");
        // throw std::runtime_error("");
    }
    oshuffle<row_t>(&args);
    free(marked);
    free(marked_prefix_sums);
}

void check_for_dummies(window_t * window) {
    size_t dummies = 0;
    for (size_t i = 0; i < window->num_tuples; i++) {
        if (window->tuples[i].key == UINT32_MAX) {
            dummies++;
        }
    }
    if (dummies) {
        logger(ERROR, "Dummies: %d", dummies);
    }
}


void OCA::invalidate_tuples(window_t * window, int to_remove, uint32_t oldest_counter, bool * marked, size_t * marked_prefix_sums) {
    size_t marked_so_far = 0;
    // row_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX}, .key = UINT32_MAX, .payload = UINT32_MAX};
    marked_prefix_sums[0] = 0;
    for (uint32_t i = 0; i < window->num_tuples; i++) {
        uint32_t counter = (uint32_t) (window->tuples[i].ts.tv_sec >> 32);
        bool cur_marked;
        if (counter < oldest_counter + to_remove) {
            // window->tuples[i] = inf;
            cur_marked = false;
        } else {
            // window->tuples[i] = window->tuples[i];
            cur_marked = true;
        }
        marked_so_far += cur_marked;
        marked[i] = cur_marked;
        marked_prefix_sums[i+1] = marked_so_far;
    }
}

uint32_t OCA::retire(window_t *window, uint32_t oldest_counter, uint32_t num_threads) {
    int to_remove = (int)(window->num_tuples - window->window_size);
    if (to_remove > 0) {
        bool *marked = (bool*) malloc(window->num_tuples * sizeof(*marked));
        size_t *marked_prefix_sums = (size_t*) malloc((window->num_tuples+1) * sizeof(*marked_prefix_sums));
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

void fill_with_dummies(vector<res_type> & table) {
    row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
        .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};
    res_type dummy = {inf, inf};
    uint32_t size = (uint32_t) table.size();
    for (uint32_t i = size; i < next_power_of_two(size); i++) {
        table.push_back(dummy);
    }
}

void emit_results(const vector<res_type> & results) {
    (void) (results);
}

result_t * OCA::l2v2_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg) {
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));
    uint32_t processed_r = 0, processed_s = 0;
    uint32_t oldest_r = 0, oldest_s = 0; // store counters for both windows to retire tuples with oldest counters
    uint32_t matches = 0;
    uint32_t num_threads = cfg->NTHREADS;
    int ret;

    relation_t *batchR = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batchS = (relation_t*) malloc(sizeof(relation_t));

    vector<res_type> results;

    pthread_t threads[num_threads];
    for (size_t i = 1; i < num_threads; i++) {
        ret = pthread_create(&threads[i - 1], NULL, start_thread_work, NULL);
        if (ret) {
            logger(ERROR, "pthread_create");
            return nullptr;
        }
    }

    debug_window(windowR);

    batchR->tuples = relR->tuples;
    batchR->num_tuples = windowR->window_size;
    oblivious_append(batchR, false, windowR, processed_r, num_threads);
    processed_r += windowR->window_size;

    batchS->tuples = relS->tuples;
    batchS->num_tuples = windowS->window_size;
    oblivious_append(batchS, false, windowS, processed_s, num_threads);
    processed_s += windowS->window_size;
    logger(INFO, "Windows filled with tuples");

    bool next_is_R;

#ifdef MEASURE_LATENCY
    uint64_t total_cycles = 0;
    size_t current_measurements = 0;
    size_t measurements = 1000;
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

        uint32_t local_matches = 0;

        if (next_is_R) {
            batchR->tuples = relR->tuples + processed_r;
            batchR->num_tuples = 1;
            // append R to W_R
            oblivious_append(batchR, false, windowR, processed_r, num_threads);
            // merge R with W_S to T2
            table_id_t * t2 = oblivious_append(batchR, true, true, windowS, num_threads);
            // scan T2 and produce remaining join results
            local_matches += scan(t2, results, num_threads);
            // retire tuples
            oldest_r = retire(windowR, oldest_r, num_threads);

            //clean up
            free(t2->tuples);
            free(t2);
            processed_r++;
        } else {
            batchS->tuples = relS->tuples + processed_s;
            batchS->num_tuples = 1;
            // append S to W_S
            oblivious_append(batchS, false, windowS, processed_s, num_threads);
            // merge S with W_R to T1
            table_id_t * t1 = oblivious_append(batchS, false, false, windowR, num_threads);
            // scan T1 and produce join results
            local_matches += scan(t1, results, num_threads);
            // retire tuples
            oldest_s = retire(windowS, oldest_s, num_threads);

            // clean up
            free(t1->tuples);
            free(t1);
            processed_s++;
        }

        // trim dummy tuples
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
        orcpar<res_type>(&par_args);
        free(marked);
        free(marked_prefix_sums);

        results.erase(results.end() - ((int)results.size()-local_matches), results.end());

        emit_results(results);

        matches += local_matches;

        results.clear();

#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            total_cycles += getCyclesSinceStart(start);
            current_measurements++;
            // uint64_t avg_cycles = total_cycles / current_measurements;
            // logger(INFO, "[%d] Average latency cycles     : %lu [cycles]", current_measurements, avg_cycles);
            // logger(INFO, "[%d] Average latency nanos      : %lu [ns]", current_measurements, 1000000000*avg_cycles/CPU_FREQ);
            // logger(INFO, "[%d] Average latency nanos/tuple: %lu [ns]", current_measurements, 1000000000*avg_cycles/CPU_FREQ/(batchSizeR+batchSizeS));
            if (current_measurements == measurements) {
                uint64_t avg_cycles = total_cycles / current_measurements;
                logger(INFO, "Average latency cycles     : %lu [cycles]", avg_cycles);
                logger(INFO, "Average latency nanos      : %lu [ns]", 1000000000*avg_cycles/CPU_FREQ);
                // logger(INFO, "Average latency nanos/tuple: %lu [ns]", 1000000000*avg_cycles/CPU_FREQ/(batchSiz));
            }
        }
#endif
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);

    //clean up
    thread_release_all();
    for (size_t i = 1; i < num_threads; i++) {
        pthread_join(threads[i - 1], NULL);
    }
    thread_unrelease_all();

    free(batchR);
    free(batchS);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->sjr = timer;
    return joinresult;

}

result_t * OCA::l3_l4_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg, bool L3) {
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));
    uint32_t processed_r = 0, processed_s = 0;
    uint32_t oldest_r = 0, oldest_s = 0; // store counters for both windows to retire tuples with oldest counters
    uint32_t matches = 0;
    uint32_t num_threads = cfg->NTHREADS;
    int ret;

    relation_t *batchR = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batchS = (relation_t*) malloc(sizeof(relation_t));
    uint32_t batchSizeR, batchSizeS;

    vector<res_type> results;

    pthread_t threads[num_threads];
    for (size_t i = 1; i < num_threads; i++) {
        ret = pthread_create(&threads[i - 1], NULL, start_thread_work, NULL);
        if (ret) {
            logger(ERROR, "pthread_create");
            return nullptr;
        }
    }

    debug_window(windowR);

    batchR->tuples = relR->tuples;
    batchR->num_tuples = windowR->window_size;
    oblivious_append(batchR, false, windowR, processed_r, num_threads);
    processed_r += windowR->window_size;

    batchS->tuples = relS->tuples;
    batchS->num_tuples = windowS->window_size;
    oblivious_append(batchS, false, windowS, processed_s, num_threads);
    processed_s += windowS->window_size;
    logger(INFO, "Windows filled with tuples");

    eTimer->startTimer(TIMER::JOIN_TOTAL_TIME);

    while (processed_r < relR->num_tuples || processed_s < relS->num_tuples) {
        batchSizeR = (processed_r + cfg->batchRSize < relR->num_tuples) ?
                     cfg->batchRSize : (relR->num_tuples - processed_r);
        batchR->tuples = relR->tuples + processed_r;
        batchR->num_tuples = batchSizeR;

        batchSizeS = (processed_s + cfg->batchSSize < relS->num_tuples) ?
                     cfg->batchSSize : (relS->num_tuples - processed_s);
        batchS->tuples = relS->tuples + processed_s;
        batchS->num_tuples = batchSizeS;

        uint32_t local_matches = 0;

        // append R to W_R
        oblivious_append(batchR, false, windowR, processed_r, num_threads);

        // merge S with W_R to T1
        table_id_t * t1 = oblivious_append(batchS, false, false, windowR, num_threads);

        // scan T1 and produce join results
        local_matches += scan(t1, results, num_threads);

        // merge R with W_S to T2
        table_id_t * t2 = oblivious_append(batchR, true, true, windowS, num_threads);

        // scan T2 and produce remaining join results
        local_matches += scan(t2, results, num_threads);

        // append S to W_S
        oblivious_append(batchS, false, windowS, processed_s, num_threads);

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

        // clean up
        free(t1->tuples);
        free(t1);
        free(t2->tuples);
        free(t2);
        results.clear();
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);

    //clean up
    thread_release_all();
    for (size_t i = 1; i < num_threads; i++) {
        pthread_join(threads[i - 1], NULL);
    }
    thread_unrelease_all();

    free(batchR);
    free(batchS);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->sjr = timer;
    return joinresult;

}

// This function splits the first join into smaller two joins to perform merges only on power of two
result_t * OCA::l3_l4_join2(relation_t *relR, relation_t *relS, joinconfig_t * cfg, bool L3) {
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));
    uint32_t processed_r = 0, processed_s = 0;
    uint32_t oldest_r = 0, oldest_s = 0; // store counters for both windows to retire tuples with oldest counters
    uint32_t matches = 0;
    uint32_t num_threads = cfg->NTHREADS;
    int ret;

    relation_t *batchR = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batchS = (relation_t*) malloc(sizeof(relation_t));
    uint32_t batchSizeR, batchSizeS;

    vector<res_type> results;

    pthread_t threads[num_threads];
    for (size_t i = 1; i < num_threads; i++) {
        ret = pthread_create(&threads[i - 1], NULL, start_thread_work, NULL);
        if (ret) {
            logger(ERROR, "pthread_create");
            return nullptr;
        }
    }

    debug_window(windowR);

    batchR->tuples = relR->tuples;
    batchR->num_tuples = windowR->window_size;
    oblivious_append(batchR, false, windowR, processed_r, num_threads);
    processed_r += windowR->window_size;

    batchS->tuples = relS->tuples;
    batchS->num_tuples = windowS->window_size;
    oblivious_append(batchS, false, windowS, processed_s, num_threads);
    processed_s += windowS->window_size;
    logger(INFO, "Windows filled with tuples");

#ifdef MEASURE_LATENCY
    uint64_t total_cycles = 0;
    size_t current_measurements = 0;
    size_t measurements = 100;
    uint64_t start, sort, append1, scan1, append2, scan2, append3, scan3, append4, append5, retire1, retire2, trim,now,now2;
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

        bitonic_sort<row_t, func_comp<row_t, row_t_key>>(batchR->tuples, batchR->num_tuples, num_threads, true);
        bitonic_sort<row_t, func_comp<row_t, row_t_key>>(batchS->tuples, batchS->num_tuples, num_threads, true);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        sort = now - start;
#endif

        // merge S with W_R to T1
        table_id_t * t1 = oblivious_append(batchS, true, false, windowR, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        append1 = now2 - now;
#endif
        // scan T1 and produce join results
        local_matches += scan(t1, results, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        scan1 = now - now2;
#endif
        // merge R with W_S to T2
        table_id_t * t2 = oblivious_append(batchR, true, true, windowS, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        append2 = now2 - now;
#endif
        // scan T2 and produce remaining join results
        local_matches += scan(t2, results, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        scan2 = now - now2;
#endif
        // merge R with S to T3
        table_id_t * t3 = oblivious_append(batchR, true, true, batchS, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        append3 = now2 - now;
#endif
        local_matches += scan(t3, results, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        scan3 = now - now2;
#endif
        // append R to W_R
        oblivious_append(batchR, true, windowR, processed_r, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        append4 = now2 - now;
#endif
        // append S to W_S
        oblivious_append(batchS, true, windowS, processed_s, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        append5 = now - now2;
#endif

        // retire tuples in both windows
        oldest_r = retire(windowR, oldest_r, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        retire1 = now2 - now;
#endif
        oldest_s = retire(windowS, oldest_s, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        retire2 = now - now2;
#endif

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
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        trim = now2 - now;
#endif

        emit_results(results);

        matches += local_matches;

        processed_r += batchSizeR;
        processed_s += batchSizeS;

        // clean up
        free(t1->tuples);
        free(t1);
        free(t2->tuples);
        free(t2);
        free(t3->tuples);
        free(t3);
        results.clear();

#ifdef MEASURE_LATENCY
        if (current_measurements < measurements) {
            total_cycles = getCyclesSinceStart(start);
            current_measurements++;
            if (current_measurements) {
                logger(INFO, "Total : %lu", total_cycles);
                logger(INFO, "Sort  : %lu (%d%%)", sort, 100*sort/total_cycles);
                logger(INFO, "Scan  : %lu (%d%%)", (scan1+scan2+scan3), 100*(scan1+scan2+scan3)/total_cycles);
                logger(INFO, "Merge1: %lu (%d%%)", (append1), 100*(append1)/total_cycles);
                logger(INFO, "Merge2: %lu (%d%%)", (append2), 100*(append2)/total_cycles);
                logger(INFO, "Merge3: %lu (%d%%)", (append3), 100*(append3)/total_cycles);
                logger(INFO, "Merge : %lu (%d%%)", (append1+append2+append3), 100*(append1+append2+append3)/total_cycles);
                logger(INFO, "Append: %lu (%d%%)", (append4+append5), 100*(append4+append5)/total_cycles);
                logger(INFO, "Retire: %lu (%d%%)", (retire1+retire2), 100*(retire1+retire2)/total_cycles);
                logger(INFO, "Trim  : %lu (%d%%)", trim, 100*trim/total_cycles);
            }
        }
        return nullptr;
#endif
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);

    //clean up
    thread_release_all();
    for (size_t i = 1; i < num_threads; i++) {
        pthread_join(threads[i - 1], NULL);
    }
    thread_unrelease_all();

    free(batchR);
    free(batchS);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->sjr = timer;
    return joinresult;

}

/** 96-99% of the CPU cycles are taken by the odd-even merge.
 * Before parallelizing anything else we have to maximize the perf of merge.
 */
void OCA::oblivious_append(relation_t *batch, bool batch_sorted, window_t *window, uint32_t counter, uint32_t num_threads) {
    assert(batch->num_tuples + window->num_tuples <= window->capacity);

    row_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX}, .key = UINT32_MAX, .payload = UINT32_MAX};

    for (uint32_t i = 0; i < batch->num_tuples; i++) {
        batch->tuples[i].ts.tv_sec = ((uint64_t)(counter+i) << 32) | (uint32_t) batch->tuples[i].ts.tv_sec;
    }

    uint32_t half_size = (window->num_tuples > batch->num_tuples) ?
        window->num_tuples : batch->num_tuples;
    uint32_t new_size = next_power_of_two(2*half_size);

    // TODO: remove reallocs
    window->tuples = (row_t*) realloc(window->tuples, new_size*sizeof(row_t));

    std::copy(batch->tuples, batch->tuples + batch->num_tuples, window->tuples + new_size - batch->num_tuples);
    std::fill(window->tuples + window->num_tuples, window->tuples+(new_size / 2), inf);
    std::fill(window->tuples + (new_size/2), window->tuples+new_size-batch->num_tuples, inf);

    if (!batch_sorted)
        bitonic_sort<row_t, func_comp<row_t, row_t_key>>(window->tuples + new_size - next_power_of_two(batch->num_tuples),
            next_power_of_two(batch->num_tuples), num_threads, true);


    // if (window->num_tuples == new_size / 2 && window->num_tuples > batch->num_tuples) {
    //     odd_even_merge<row_t, func_comp<row_t, row_t_key>>(window->tuples, 0, new_size, num_threads, 1,
    //         window->num_tuples + batch->num_tuples);
    // } else {
    //     logger(WARN, "Not using opt merge");
    //     odd_even_merge<row_t, func_comp<row_t, row_t_key>>(window->tuples, 0, new_size, 1);
    // }

    struct bitonic_merge_args<row_t> args = {
        .arr = window->tuples,
        .start = 0,
        .length = new_size,
        .crossover = false,
        .num_threads = num_threads,
    };
    bitonic_merge<row_t, func_comp<row_t, row_t_key>>(&args);

    new_size = window->num_tuples + batch->num_tuples;
    window->tuples = (row_t*) realloc(window->tuples, (new_size)*sizeof(row_t));
    window->num_tuples = new_size;
}

struct copy_args {
    table_id_t * merged;
    window_t * window;
    relation_t * batch;
    size_t start;
    size_t length;
    size_t filled_size; // temp size when filled with dummies
    bool batch_is_r;
};

void copy (void *args_) {
    struct copy_args* args = (struct copy_args*) args_;
    table_id_t * merged = args->merged;
    window_t * window = args->window;
    relation_t * batch = args->batch;
    size_t start = args->start;
    size_t length = args->length;
    size_t filled_size = args->filled_size;
    bool batch_is_r = args->batch_is_r;

    row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
    .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};

    assert(start + length <= filled_size);

    for (size_t i = start; i < window->num_tuples && i < start + length; i++) {
        merged->tuples[i].ts       = window->tuples[i].ts;
        merged->tuples[i].key      = window->tuples[i].key;
        merged->tuples[i].payload  = window->tuples[i].payload;
        merged->tuples[i].table_id = batch_is_r;
    }

    for (uint32_t i = window->num_tuples; i < filled_size / 2 && i < start+length ; i++) {
        merged->tuples[i] = inf;
    }

    // if (start >= filled_size/2 + batch->num_tuples) {
    std::fill(merged->tuples + filled_size/2, merged->tuples + filled_size - batch->num_tuples, inf);
    // }

    for (uint32_t i =(uint32_t) (filled_size - batch->num_tuples), j = 0; j < batch->num_tuples && i < start+length; i++, j++) {
        merged->tuples[i].ts       = batch->tuples[j].ts;
        merged->tuples[i].key      = batch->tuples[j].key;
        merged->tuples[i].payload  = batch->tuples[j].payload;
        merged->tuples[i].table_id = !batch_is_r;
    }

}

/** 98% of the CPU cycles goes to merge. No need to parallelize other operations */
table_id_t * OCA::oblivious_append(relation_t *batch, bool batch_sorted, bool batch_is_r, window_t *window, uint32_t num_threads) {

    uint32_t half_size = (window->num_tuples > batch->num_tuples) ?
        window->num_tuples : batch->num_tuples;

    table_id_t * merged = (table_id_t*) malloc(sizeof(table_id_t));
    uint32_t filled_size = next_power_of_two(2 * half_size);

    merged->tuples = (row_table_t*) malloc(filled_size*sizeof(row_table_t));
    merged->num_tuples = window->num_tuples + batch->num_tuples;

    // size_t num_per_thread = filled_size / num_threads;

    struct copy_args args[1];
    // struct copy_args args[num_threads];
    // struct thread_work work[num_threads];
    // for (size_t i = 1; i < num_threads; i++) {
    //     args[i] = {
    //         .merged = merged,
    //         .window = window,
    //         .batch =  batch,
    //         .start = (i) * num_per_thread,
    //         .length = (i == num_threads - 1) ?
    //             (window->num_tuples - (i)*num_per_thread) : num_per_thread,
    //         .filled_size = filled_size,
    //         .batch_is_r =  batch_is_r
    //     };
    //     work[i] = {
    //         .type = THREAD_WORK_SINGLE,
    //         .single = {
    //             .func = copy,
    //             .arg = &args[i]
    //         }
    //     };
    //     thread_work_push(&work[i]);
    // }
    args[0] = {
        .merged = merged,
        .window = window,
        .batch = batch,
        .start = 0,
        .length = filled_size,
        .filled_size = filled_size,
        .batch_is_r =  batch_is_r
    };
    copy(&args[0]);
    // for (size_t i = 1; i < num_threads; i++) {
    //     thread_wait(&work[i]);
    // }

    if (!batch_sorted)
        bitonic_sort<row_table_t, table_comp>(merged->tuples + filled_size/2, next_power_of_two(batch->num_tuples), num_threads, true);

    // if (window->num_tuples == filled_size / 2) {
    //     // odd_even_merge_opt<row_table_t, tid_comp>(merged->tuples, 0, tmp_size, 1,
    //     //     merged->num_tuples);
    //     odd_even_merge<row_table_t, tid_comp>(merged->tuples, 0, filled_size, num_threads, 1,
    //         window->num_tuples + batch->num_tuples);
    // } else {
    //     logger(WARN, "Not using opt merge");
    //     odd_even_merge<row_table_t, tid_comp>(merged->tuples, 0, filled_size, 1);
    // }

    struct bitonic_merge_args<row_table_t> merge_args = {
        .arr = merged->tuples,
        .start = 0,
        .length = filled_size,
        .crossover = false,
        .num_threads = num_threads,
    };
    bitonic_merge<row_table_t, tid_comp>(&merge_args);

    merged->tuples = (row_table_t*) realloc(merged->tuples, merged->num_tuples * sizeof(row_table_t));
    return merged;
}

table_id_t * OCA::oblivious_append(relation_t *batch, bool batch_sorted, bool batch_is_r, relation_t *window, uint32_t num_threads) {
    row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
        .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};
    uint32_t half_size = (window->num_tuples > batch->num_tuples) ?
        window->num_tuples : batch->num_tuples;

    // if (!batch_sorted)
    //     bitonic_sort<row_t, func_comp<row_t, row_t_key>>(batch->tuples, batch->num_tuples, num_threads);

    table_id_t * merged = (table_id_t*) malloc(sizeof(table_id_t));
    uint32_t tmp_size = 2 * half_size;
    tmp_size = next_power_of_two(tmp_size);

    merged->tuples = (row_table_t*) malloc(tmp_size*sizeof(row_table_t));
    merged->num_tuples = window->num_tuples + batch->num_tuples;

    for (uint32_t i = 0; i < window->num_tuples; i++) {
        merged->tuples[i].ts       = window->tuples[i].ts;
        merged->tuples[i].key      = window->tuples[i].key;
        merged->tuples[i].payload  = window->tuples[i].payload;
        merged->tuples[i].table_id = batch_is_r;
    }
    for (uint32_t i = window->num_tuples; i < tmp_size / 2 ; i++) {
        merged->tuples[i] = inf;
    }
    // std::copy(batch->tuples, batch->tuples + window->num_tuples, merged->tuples + window->num_tuples);
    for (uint32_t i = tmp_size - batch->num_tuples, j = 0; j < batch->num_tuples; i++, j++) {
        merged->tuples[i].ts       = batch->tuples[j].ts;
        merged->tuples[i].key      = batch->tuples[j].key;
        merged->tuples[i].payload  = batch->tuples[j].payload;
        merged->tuples[i].table_id = !batch_is_r;
    }
    // std::fill(merged->tuples + merged->num_tuples, merged->tuples + (2*half_size), inf);
    for (uint32_t i = tmp_size/2; i < tmp_size-batch->num_tuples; i++) {
        merged->tuples[i] = inf;
    }
    if (!batch_sorted)
        bitonic_sort<row_table_t, table_comp>(merged->tuples + tmp_size/2, next_power_of_two(batch->num_tuples), num_threads, true);

    // uint32_t merged_size_kb = (uint32_t) (sizeof(row_table_t) * tmp_size / 8 / 1024);
    // // TODO: use optimized merge
    // // run paralle merge only if the collection exceeds L2
    // if (merged_size_kb > L2_CACHE_SIZE && num_threads > 1) {
    //     struct merge_args<row_table_t> args = {
    //         .arr = merged->tuples,
    //         .start = 0,
    //         .length = tmp_size,
    //         .num_threads = num_threads,
    //         .r = 1,
    //         .ret = 0,
    //     };
    //     odd_even_merge_par<row_table_t, tid_comp>(&args);
    // } else {
    //     if (window->num_tuples == tmp_size / 2) {
    //         odd_even_merge_opt<row_table_t, tid_comp>(merged->tuples, 0, tmp_size, 1,
    //             merged->num_tuples);
    //     } else {
    //         // logger(WARN, "Not using opt merge");
    //         odd_even_merge<row_table_t, tid_comp>(merged->tuples, 0, tmp_size, 1);
    //     }
    // }
    struct bitonic_merge_args<row_table_t> args = {
        .arr = merged->tuples,
        .start = 0,
        .length = tmp_size,
        .crossover = false,
        .num_threads = num_threads,
    };
    bitonic_merge<row_table_t, tid_comp>(&args);

    merged->tuples = (row_table_t*) realloc(merged->tuples, merged->num_tuples * sizeof(row_table_t));
    return merged;
}

struct scan_args {
    table_id_t * table;
    size_t start;
    size_t length;
    size_t local_matches;
    vector<tuple<row_table_t, row_table_t>>* local_results;
};

void scan_slice(void *args_) {
    struct scan_args * args = (struct scan_args*) args_;
    table_id_t * t = args->table;
    size_t start = args->start;
    size_t length = args->length;
    size_t local_matches = 0;
    vector<tuple<row_table_t, row_table_t>>* local_results = args->local_results;
    row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
        .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};
    row_table_t t_pk = inf;

    for (size_t i = start; i < start + length; i++) {
        t_pk = conditional_select(t_pk, t->tuples[i], t->tuples[i].table_id);
        // OPAQUE would also check if t_pk is real or not but we do it at the moment of emitting the tuple
        bool emit_real = t->tuples[i].table_id == 1;
        local_matches += emit_real;
        row_table_t t_s = conditional_select(t->tuples[i], inf, emit_real);
        std::tuple<row_table_t, row_table_t> tup = {t_pk, t_s};
        local_results->push_back(tup);
    }
    args->local_matches = local_matches;
    return;
}

uint32_t OCA::scan(table_id_t *t, vector<tuple<row_table_t, row_table_t>> &results, uint32_t num_threads) {
    (void) results;
    // row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
    //     .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};
    // row_table_t t_pk = inf;
    uint32_t matches = 0;

    // for (uint32_t i = 0; i < t->num_tuples; i++) {
    //     t_pk = conditional_select(t_pk, t->tuples[i], t->tuples[i].table_id);
    //     bool emit_real = t->tuples[i].table_id == 1 && t->tuples[i].key == t_pk.key;
    //     matches += emit_real;
    //     row_table_t t_r = conditional_select(t_pk, inf, emit_real);
    //     row_table_t t_s = conditional_select(t->tuples[i], inf, emit_real);
    //     std::tuple<row_table_t, row_table_t> tup = {t_r, t_s};
    //     results.push_back(tup);
    // }
    size_t num_per_thread = t->num_tuples / num_threads;
    scan_args args[num_threads];
    struct thread_work work[num_threads];
    vector<vector<tuple<row_table_t, row_table_t>>> vec_results(num_threads);
    for (size_t i = 1; i < num_threads; i++) {
        args[i] = {
            .table = t,
            .start = i * num_per_thread,
            .length = (i == num_threads - 1) ?
                (t->num_tuples - i * num_per_thread) : num_per_thread,
            .local_matches = 0,
            .local_results = &vec_results[i]
        };
        work[i] = {
            .type = THREAD_WORK_SINGLE,
            .single = {
                .func = scan_slice,
                .arg = &args[i]
            }
        };
        thread_work_push(&work[i]);
    }
    args[0] = {
        .table = t,
        .start = 0,
        .length = num_per_thread,
        .local_matches = 0,
        .local_results = &vec_results[0]
    };
    scan_slice(&args[0]);
    for (size_t i = 1; i < num_threads; i++) {
        thread_wait(&work[i]);
    }
    for (size_t i = 0; i < num_threads; i++) {
        matches += (uint32_t) args[i].local_matches;
    }

    return matches;
}

void OCA::window_append(window_t *window, relation_t *batch, uint32_t counter) {
    assert(window->capacity >= window->num_tuples + batch->num_tuples);

    for (uint32_t i = 0; i < batch->num_tuples; i++) {
        batch->tuples[i].ts.tv_sec = ((uint64_t)(counter+i) << 32) | (uint32_t) batch->tuples[i].ts.tv_sec;
    }

    std::copy(batch->tuples, batch->tuples + batch->num_tuples, window->tuples + window->num_tuples);
    window->num_tuples += batch->num_tuples;
}


