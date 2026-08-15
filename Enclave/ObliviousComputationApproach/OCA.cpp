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

/* Below this many elements per thread, a bulk pass isn't worth dispatching. */
#define PAR_BULK_MIN 65536

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


// Marks the survivors and builds the running counts orcpar needs. This is a
// prefix sum, so it looks sequential, but it splits into the standard three
// passes: mark + count locally, add up the per-slice totals, then write the
// running counts with each slice's base folded in. One full pass over the
// window, twice per batch, so it is worth the trouble.
struct mark_args {
    window_t *window;
    bool *marked;
    size_t *marked_prefix_sums;
    uint64_t cutoff;
    size_t begin;
    size_t end;
    size_t base;        // marked elements before `begin` (filled in by pass 2)
    size_t local_count;
};

static void mark_slice(void *args_) {
    struct mark_args *args = (struct mark_args *) args_;
    row_t *tuples = args->window->tuples;
    uint64_t cutoff = args->cutoff;
    bool *marked = args->marked;
    size_t count = 0;
    for (size_t i = args->begin; i < args->end; i++) {
        bool cur_marked = (uint64_t) (tuples[i].ts.tv_sec >> 32) >= cutoff;
        count += cur_marked;
        marked[i] = cur_marked;
    }
    args->local_count = count;
}

static void sum_slice(void *args_) {
    struct mark_args *args = (struct mark_args *) args_;
    size_t running = args->base;
    for (size_t i = args->begin; i < args->end; i++) {
        running += args->marked[i];
        args->marked_prefix_sums[i + 1] = running;
    }
}

void OCA::invalidate_tuples(window_t * window, int to_remove, uint32_t oldest_counter, bool * marked, size_t * marked_prefix_sums, uint32_t num_threads) {
    size_t n = window->num_tuples;
    uint64_t cutoff = (uint64_t) oldest_counter + (uint64_t) to_remove;
    marked_prefix_sums[0] = 0;

    size_t threads = num_threads;
    while (threads > 1 && n / threads < PAR_BULK_MIN) threads--;

    struct mark_args args[threads];
    struct thread_work work[threads];
    for (size_t t = 0; t < threads; t++) {
        args[t] = {
            .window = window,
            .marked = marked,
            .marked_prefix_sums = marked_prefix_sums,
            .cutoff = cutoff,
            .begin = n * t / threads,
            .end = n * (t + 1) / threads,
            .base = 0,
            .local_count = 0,
        };
    }

    for (size_t t = 1; t < threads; t++) {
        work[t] = { .type = THREAD_WORK_SINGLE,
                    .single = { .func = mark_slice, .arg = &args[t] } };
        thread_work_push(&work[t]);
    }
    mark_slice(&args[0]);
    for (size_t t = 1; t < threads; t++) thread_wait(&work[t]);

    size_t running = 0;
    for (size_t t = 0; t < threads; t++) {
        args[t].base = running;
        running += args[t].local_count;
    }

    for (size_t t = 1; t < threads; t++) {
        work[t] = { .type = THREAD_WORK_SINGLE,
                    .single = { .func = sum_slice, .arg = &args[t] } };
        thread_work_push(&work[t]);
    }
    sum_slice(&args[0]);
    for (size_t t = 1; t < threads; t++) thread_wait(&work[t]);
}

uint32_t OCA::retire(window_t *window, uint32_t oldest_counter, uint32_t num_threads) {
    int to_remove = (int)(window->num_tuples - window->window_size);
    if (to_remove > 0) {
        bool *marked = (bool*) malloc(window->num_tuples * sizeof(*marked));
        size_t *marked_prefix_sums = (size_t*) malloc((window->num_tuples+1) * sizeof(*marked_prefix_sums));
        invalidate_tuples(window, to_remove, oldest_counter, marked, marked_prefix_sums, num_threads);
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

// FK-MERG-L2. Same append/merge/scan/retire flow as l3_l4_join2() below, but
// one tuple at a time (OCA_L2v2_wrapper pins batch size to 1) and no L3
// trimming step at the end.
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

// Algorithm 4, FK-MERG-L3/L4. Per batch: append R into W_R, then merge the
// S-batch against that now-bigger W_R (T1) and scan it, then merge the
// R-batch against the still-old W_S (T2) and scan that too, then append S
// into W_S and retire both windows. That's the paper's
// ((R ∪ W_R) ⋈ S) ∪ (R ⋈ W_S) split -- it covers everything new without
// re-emitting W_R ⋈ W_S matches from earlier batches.
// L3 trims the dummy tuples out of `results` with OCOMPACTION afterwards
// (leaks the batch's real output count); L4 just leaves the padding in.
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

// This is the version wired up to FK-MERG-L3/FK-MERG-L4
// (OCA_L3_wrapper_split / OCA_L4_wrapper_split). l3_l4_join() above merges
// the S-batch against a W_R that's already been extended with the R-batch --
// an awkward size for a bitonic merge. Here we avoid that by splitting it
// into two clean merges instead: S-batch against the *old* W_R, and S-batch
// against the R-batch itself. Add in R-batch against the old W_S and you get
// the same union as before, just without ever merging into a lopsided window.
// R and S only get appended into their windows after all three scans are
// done. L3 trims dummies with OCOMPACTION; L4 leaves the padding in.
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
    // Per-phase cycle counts, summed over every batch of the run. (This used
    // to bail out via `return nullptr` after the first batch, which meant the
    // breakdown only ever showed batch #1 -- i.e. it attributed every
    // first-touch page fault and allocator warm-up to whichever phase happened
    // to hit that memory first.)
    uint64_t total_cycles = 0, batches = 0;
    uint64_t c_sort = 0, c_merge1 = 0, c_merge2 = 0, c_merge3 = 0, c_scan = 0;
    uint64_t c_append = 0, c_retire = 0, c_trim = 0;
    uint64_t start, now, now2;
#endif

    eTimer->startTimer(TIMER::JOIN_TOTAL_TIME);

    while (processed_r < relR->num_tuples || processed_s < relS->num_tuples) {
#ifdef MEASURE_LATENCY
        start = clock_cycles();
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
        c_sort += now - start;
#endif

        // merge S with W_R to T1
        table_id_t * t1 = oblivious_append(batchS, true, false, windowR, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        c_merge1 += now2 - now;
#endif
        // scan T1 and produce join results
        local_matches += scan(t1, results, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        c_scan += now - now2;
#endif
        // merge R with W_S to T2
        table_id_t * t2 = oblivious_append(batchR, true, true, windowS, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        c_merge2 += now2 - now;
#endif
        // scan T2 and produce remaining join results
        local_matches += scan(t2, results, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        c_scan += now - now2;
#endif
        // merge R with S to T3
        table_id_t * t3 = oblivious_append(batchR, true, true, batchS, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        c_merge3 += now2 - now;
#endif
        local_matches += scan(t3, results, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        c_scan += now - now2;
#endif
        // append R to W_R
        oblivious_append(batchR, true, windowR, processed_r, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        c_append += now2 - now;
#endif
        // append S to W_S
        oblivious_append(batchS, true, windowS, processed_s, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        c_append += now - now2;
#endif

        // retire tuples in both windows
        oldest_r = retire(windowR, oldest_r, num_threads);
#ifdef MEASURE_LATENCY
        now2 = clock_cycles();
        c_retire += now2 - now;
#endif
        oldest_s = retire(windowS, oldest_s, num_threads);
#ifdef MEASURE_LATENCY
        now = clock_cycles();
        c_retire += now - now2;
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
        c_trim += now2 - now;
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
        total_cycles += getCyclesSinceStart(start);
        batches++;
#endif
    }

    timer->joinTotalTime = eTimer->stopTimer(TIMER::JOIN_TOTAL_TIME);
#ifdef MEASURE_LATENCY
    if (total_cycles) {
        logger(INFO, "Batches: %lu", batches);
        logger(INFO, "Total : %lu", total_cycles);
        logger(INFO, "Sort  : %lu (%d%%)", c_sort, 100*c_sort/total_cycles);
        logger(INFO, "Scan  : %lu (%d%%)", c_scan, 100*c_scan/total_cycles);
        logger(INFO, "Merge1: %lu (%d%%)", c_merge1, 100*c_merge1/total_cycles);
        logger(INFO, "Merge2: %lu (%d%%)", c_merge2, 100*c_merge2/total_cycles);
        logger(INFO, "Merge3: %lu (%d%%)", c_merge3, 100*c_merge3/total_cycles);
        logger(INFO, "Merge : %lu (%d%%)", c_merge1+c_merge2+c_merge3,
               100*(c_merge1+c_merge2+c_merge3)/total_cycles);
        logger(INFO, "Append: %lu (%d%%)", c_append, 100*c_append/total_cycles);
        logger(INFO, "Retire: %lu (%d%%)", c_retire, 100*c_retire/total_cycles);
        logger(INFO, "Trim  : %lu (%d%%)", c_trim, 100*c_trim/total_cycles);
    }
#endif

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

// --- parallel bulk fill ------------------------------------------------------
//
// Padding a window or a merge buffer with dummies is a plain O(window) write
// pass, and there is one (or two) of them per batch. Left single-threaded it
// shows up as a flat floor under everything else once the merge itself starts
// scaling, so slice it across the same thread pool.

template <typename T>
struct fill_args {
    T *arr;
    T val;
    size_t start;
    size_t end;
};

template <typename T>
static void fill_slice(void *args_) {
    struct fill_args<T> *args = (struct fill_args<T> *) args_;
    std::fill(args->arr + args->start, args->arr + args->end, args->val);
}

template <typename T>
static void par_fill(T *arr, size_t begin, size_t end, T val, size_t num_threads) {
    if (end <= begin) return;
    size_t n = end - begin;
    size_t threads = num_threads;
    while (threads > 1 && n / threads < PAR_BULK_MIN) threads--;
    if (threads <= 1) {
        std::fill(arr + begin, arr + end, val);
        return;
    }

    struct fill_args<T> args[threads];
    struct thread_work work[threads];
    for (size_t t = 0; t < threads; t++) {
        args[t] = {
            .arr = arr,
            .val = val,
            .start = begin + n * t / threads,
            .end = begin + n * (t + 1) / threads,
        };
    }
    for (size_t t = 1; t < threads; t++) {
        work[t] = {
            .type = THREAD_WORK_SINGLE,
            .single = { .func = fill_slice<T>, .arg = &args[t] },
        };
        thread_work_push(&work[t]);
    }
    fill_slice<T>(&args[0]);
    for (size_t t = 1; t < threads; t++) {
        thread_wait(&work[t]);
    }
}

/** OAppend, Algorithm 3: sort the batch if it isn't sorted yet, pad both
 * sides with infinity dummies up to a power-of-two size, bitonic-merge them
 * together in place. Window keeps its sort order and grows in place.
 *
 * The merge dominates, but only once everything around it is off the critical
 * path: the window buffer is sized for the padded merge once in the
 * constructor (it used to be realloc'd up and back down here, i.e. two full
 * memcpys of the window per call), and the dummy padding is filled in
 * parallel.
 */
void OCA::oblivious_append(relation_t *batch, bool batch_sorted, window_t *window, uint32_t counter, uint32_t num_threads) {
    row_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX}, .key = UINT32_MAX, .payload = UINT32_MAX};

    for (uint32_t i = 0; i < batch->num_tuples; i++) {
        batch->tuples[i].ts.tv_sec = ((uint64_t)(counter+i) << 32) | (uint32_t) batch->tuples[i].ts.tv_sec;
    }

    uint32_t half_size = (window->num_tuples > batch->num_tuples) ?
        window->num_tuples : batch->num_tuples;
    uint32_t new_size = next_power_of_two(2*half_size);

    assert(new_size <= window->capacity);

    std::copy(batch->tuples, batch->tuples + batch->num_tuples, window->tuples + new_size - batch->num_tuples);
    par_fill<row_t>(window->tuples, window->num_tuples, new_size - batch->num_tuples, inf, num_threads);

    if (!batch_sorted)
        bitonic_sort<row_t, func_comp<row_t, row_t_key>>(window->tuples + new_size - next_power_of_two(batch->num_tuples),
            next_power_of_two(batch->num_tuples), num_threads, true);

    struct bitonic_merge_args<row_t> args = {
        .arr = window->tuples,
        .start = 0,
        .length = new_size,
        .crossover = false,
        .num_threads = num_threads,
    };
    bitonic_merge<row_t, func_comp<row_t, row_t_key>>(&args);

    window->num_tuples += batch->num_tuples;
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

// Materializes one slice, [start, start+length), of the tagged merge input.
// The layout is fixed by the public sizes alone: the window's tuples first,
// then dummies, then the batch parked at the very end (sorted descending by
// its caller, which is what makes the whole array bitonic). Every output index
// therefore has one well-defined source, so slices are independent and can run
// on as many threads as we have.
void copy (void *args_) {
    struct copy_args* args = (struct copy_args*) args_;
    table_id_t * merged = args->merged;
    window_t * window = args->window;
    relation_t * batch = args->batch;
    size_t start = args->start;
    size_t filled_size = args->filled_size;
    bool batch_is_r = args->batch_is_r;
    size_t end = start + args->length;
    if (end > filled_size) end = filled_size;

    row_table_t inf = {.ts = {.tv_sec = UINT64_MAX, .tv_nsec = UINT64_MAX},
    .key = UINT32_MAX, .payload = UINT32_MAX, .table_id = UINT8_MAX};

    assert(start <= filled_size);

    size_t batch_begin = filled_size - batch->num_tuples;

    size_t w_end = (end < window->num_tuples) ? end : window->num_tuples;
    for (size_t i = start; i < w_end; i++) {
        merged->tuples[i].ts       = window->tuples[i].ts;
        merged->tuples[i].key      = window->tuples[i].key;
        merged->tuples[i].payload  = window->tuples[i].payload;
        merged->tuples[i].table_id = batch_is_r;
    }

    size_t d_begin = (start > window->num_tuples) ? start : window->num_tuples;
    size_t d_end = (end < batch_begin) ? end : batch_begin;
    for (size_t i = d_begin; i < d_end; i++) {
        merged->tuples[i] = inf;
    }

    size_t b_begin = (start > batch_begin) ? start : batch_begin;
    for (size_t i = b_begin; i < end; i++) {
        size_t j = i - batch_begin;
        merged->tuples[i].ts       = batch->tuples[j].ts;
        merged->tuples[i].key      = batch->tuples[j].key;
        merged->tuples[i].payload  = batch->tuples[j].payload;
        merged->tuples[i].table_id = !batch_is_r;
    }
}

// Fan `copy` out over the pool. This is a full 2*(window+batch)-element write
// pass per cross-stream OAppend, twice per batch, so leaving it on one thread
// caps the whole join no matter how well the merge scales.
static void par_copy(table_id_t *merged, window_t *window, relation_t *batch,
        size_t filled_size, bool batch_is_r, size_t num_threads) {
    size_t threads = num_threads;
    while (threads > 1 && filled_size / threads < PAR_BULK_MIN) threads--;

    struct copy_args args[threads];
    struct thread_work work[threads];
    for (size_t t = 0; t < threads; t++) {
        size_t begin = filled_size * t / threads;
        args[t] = {
            .merged = merged,
            .window = window,
            .batch = batch,
            .start = begin,
            .length = filled_size * (t + 1) / threads - begin,
            .filled_size = filled_size,
            .batch_is_r = batch_is_r,
        };
    }
    for (size_t t = 1; t < threads; t++) {
        work[t] = {
            .type = THREAD_WORK_SINGLE,
            .single = { .func = copy, .arg = &args[t] },
        };
        thread_work_push(&work[t]);
    }
    copy(&args[0]);
    for (size_t t = 1; t < threads; t++) {
        thread_wait(&work[t]);
    }
}

/** Cross-stream OAppend: same merge as above, but instead of growing
 * `window` in place it produces a fresh, table_id-tagged array holding both
 * window and batch, ready for scan() to walk for matches. `window` itself
 * is left untouched. */
table_id_t * OCA::oblivious_append(relation_t *batch, bool batch_sorted, bool batch_is_r, window_t *window, uint32_t num_threads) {

    uint32_t half_size = (window->num_tuples > batch->num_tuples) ?
        window->num_tuples : batch->num_tuples;

    table_id_t * merged = (table_id_t*) malloc(sizeof(table_id_t));
    uint32_t filled_size = next_power_of_two(2 * half_size);

    merged->tuples = (row_table_t*) malloc(filled_size*sizeof(row_table_t));
    merged->num_tuples = window->num_tuples + batch->num_tuples;

    par_copy(merged, window, batch, filled_size, batch_is_r, num_threads);

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

    // No shrink-realloc here: scan() only ever walks merged->num_tuples, and
    // handing the allocator a 100+ MB block to move costs a memcpy of the whole
    // merge output for nothing.
    return merged;
}

// same thing again, but for merging two plain relations (used by
// l3_l4_join2() to merge the R-batch against the S-batch directly, batch vs
// batch, not batch vs window).
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

    // No shrink-realloc here: scan() only ever walks merged->num_tuples, and
    // handing the allocator a 100+ MB block to move costs a memcpy of the whole
    // merge output for nothing.
    return merged;
}

struct scan_args {
    table_id_t * table;
    size_t start;
    size_t length;
    size_t local_matches;
    vector<tuple<row_table_t, row_table_t>>* local_results;
};

// the actual SCAN from Algorithm 4. Walk the tagged, sorted merge left to
// right, always obliviously keeping the most recent PK-side (table_id 0)
// tuple around in t_pk. Whenever we hit an FK-side (table_id 1) tuple, that's
// the match -- since a valid FK join, by construction, has the PK for a key
// sitting right before all the FK tuples for that key in sort order.
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

// plain copy-to-the-end append, no sorting or shuffling. Only l2_join() uses
// this, since it doesn't rely on window order.
void OCA::window_append(window_t *window, relation_t *batch, uint32_t counter) {
    assert(window->capacity >= window->num_tuples + batch->num_tuples);

    for (uint32_t i = 0; i < batch->num_tuples; i++) {
        batch->tuples[i].ts.tv_sec = ((uint64_t)(counter+i) << 32) | (uint32_t) batch->tuples[i].ts.tv_sec;
    }

    std::copy(batch->tuples, batch->tuples + batch->num_tuples, window->tuples + window->num_tuples);
    window->num_tuples += batch->num_tuples;
}


