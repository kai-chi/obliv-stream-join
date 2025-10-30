//
// Created by kaichi on 07.11.24.
//
#include "OCA.h"
#include "threading.h"
#include "Enclave_t.h"
#include "Enclave.h"
#include <pthread.h>
#include "outil.h"
#include "barrier.h"
#ifdef MEASURE_LATENCY
#include "Commons/ECommon.h"
#endif

struct index_args {
    size_t thread_id;
    std::multimap<type_key, row_t> *maps;
    std::vector<restype> *results;
    size_t num_maps;
    row_t *r_arr;
    size_t r_start;
    size_t r_length;
    row_t *s_arr;
    size_t s_start;
    size_t s_length;
    pthread_barrier_t* barrier;
    size_t local_matches;
};

void process_maps(void *args_) {
    index_args * args = (index_args *) args_;
    std::multimap<type_key, row_t>& my_map = args->maps[args->thread_id];
    std::vector<restype>& results = args->results[args->thread_id];
    row_t * relR = args->r_arr;
    row_t * relS = args->s_arr;
    int rv = 0;

    for (size_t i = 0; i < args->s_length; i++) {
        row_t t = relS[i + args->s_start];
        my_map.emplace(t.key, t);
    }

    barrier_arrive(args->barrier, rv);

    for (size_t i = 0; i < args->r_length; i++) {
        row_t t1 = relR[i + args->r_start];
        for (size_t j = 0; j < args->num_maps; j++) {
            std::multimap<type_key, row_t>& map = args->maps[j];
            auto itr1 = map.lower_bound(t1.key);
            auto itr2 = map.upper_bound(t1.key);
            while(itr1 != itr2) {
                if (itr1->first == t1.key) {
                    restype tuple {t1, itr1->second};
                    results.push_back(tuple);
                    args->local_matches++;
                }
                itr1++;
            }
        }
    }

}

// struct probe_args {
//     std::multimap<type_key, row_t> *maps;
//     size_t thread_id;
//     row_t *arr;
//     size_t start;
//     size_t length;
//     vector<restype>* local_results;
// };
//
// void build_maps(void *args_) {
//     build_args *args = (build_args *) args_;
//     std::multimap<type_key, row_t>& map = args->maps[args->thread_id];
//     row_t *arr = args->arr;
//     size_t start = args->start;
//     size_t length = args->length;
//
//     for (size_t i = 0; i < length; i++) {
//         row_t t = arr[i + start];
//         map.emplace(t.key, t);
//     }
// }
//
// void probe_maps(void *args_) {
//     probe_args *args = (probe_args*) args_;
//     std::multimap<type_key, row_t> *maps
//     size_t start = args->start;
//     size_t length = args->length;
//     row_t *arr = args->arr;
//     vector<restype>* local_results = args->local_results;
//
//     for (size_t i = 0; i < length; i++) {
//         row_t t1 = arr[i + start];
//
//     }
// }

static void *start_thread_work(void* args) {
    (void)(args);
    /* Start work. */
    thread_start_work();
    return 0;
}

void emit_results(vector<restype>& result) {
    for (restype tuple: result) {
        row_t t1 = get<0>(tuple);
        row_t t2 = get<1>(tuple);
        logger(DBG, "%d-%d-%d",t1.key, t2.key, t2.payload);
    }
}

result_t * OCA::l2_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg) {
    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));

    uint32_t processed_r = 0, processed_s = 0;
    uint32_t oldest_r = 0, oldest_s = 0;
    uint32_t matches = 0;
    size_t num_threads = cfg->NTHREADS;
    pthread_barrier_t barrier;
    int rv = 0;
    int b = 0;

    relation_t *batchR = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batchS = (relation_t*) malloc(sizeof(relation_t));
    uint32_t batchSizeR, batchSizeS;

    windowR->window_size -= cfg->batchRSize;
    windowS->window_size -= cfg->batchSSize;
    logger(INFO, "Redefine windows to R: %d, S: %d", windowR->window_size, windowS->window_size);

    batchR->tuples = relR->tuples;
    batchR->num_tuples = windowR->window_size;
    window_append(windowR, batchR, processed_r);
    processed_r += windowR->window_size;

    batchS->tuples = relS->tuples;
    batchS->num_tuples = windowS->window_size;
    window_append(windowS, batchS, processed_s);
    processed_s += windowS->window_size;

    pthread_t threads[num_threads];
    for (size_t i = 1; i < num_threads; i++) {
        int ret = pthread_create(&threads[i - 1], NULL, start_thread_work, NULL);
        if (ret) {
            logger(ERROR, "pthread_create");
            return nullptr;
        }
    }

#ifdef MEASURE_LATENCY
    uint64_t total_cycles = 0;
    size_t current_measurements = 0;
    size_t measurements = 100;
    uint64_t start;
#endif

    rv = pthread_barrier_init(&barrier, NULL, (int) num_threads);
    if (rv != 0) {
        logger(DBG, "Couldn't create the barrier");
        ocall_exit(EXIT_FAILURE);
    }

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


        // append and expire old tuples
        window_append(windowR, batchR, processed_r);
        window_append(windowS, batchS, processed_s);

        // obliviously shuffle both windows
        oblivious_shuffle(windowR, num_threads);
        oblivious_shuffle(windowS, num_threads);

        // setup non-oblivious index
        std::multimap<type_key, row_t> maps[num_threads];
        index_args args[num_threads];
        thread_work work[num_threads];
        vector<restype> results[num_threads];
        size_t s_num_per_thread = windowS->num_tuples / num_threads;
        size_t r_num_per_thread = windowR->num_tuples / num_threads;
        for (size_t i = 0; i < num_threads; i++) {
            args[i] = {
                .thread_id = i,
                .maps = maps,
                .results = results,
                .num_maps = num_threads,
                .r_arr = windowR->tuples,
                .r_start = i * r_num_per_thread,
                .r_length = (i == num_threads - 1) ?
                    (windowR->num_tuples - i * r_num_per_thread) : r_num_per_thread,
                .s_arr = windowS->tuples,
                .s_start = i * s_num_per_thread,
                .s_length = (i == num_threads - 1) ?
                    (windowS->num_tuples - i * s_num_per_thread) : s_num_per_thread,
                .barrier = &barrier,
                .local_matches = 0
            };
            work[i] = {
                .type = THREAD_WORK_SINGLE,
                .single = {
                    .func = process_maps,
                    .arg = &args[i]
                }
            };
            if (i > 0)
                thread_work_push(&work[i]);
        }
        process_maps(&args[0]);
        for (size_t i = 1; i < num_threads; i++) {
            thread_wait(&work[i]);
        }
        // std::map<type_key, std::vector<row_t>> index;
        // for (uint32_t i = 0; i < windowS->num_tuples; i++) {
        //     row_t t = windowS->tuples[i];
        //     index[t.key].push_back(t);
        // }
        // probe the index
        // vector<restype> results;
        // for (uint32_t i = 0; i < windowR->num_tuples; i++) {
        //     row_t t1 = windowR->tuples[i];
        //     auto it = index.find(t1.key);
        //     if (it != index.end()) {
        //         for (row_t t2 : it->second) {
        //             restype tuple = {t1, t2};
        //             results.push_back(tuple);
        //             matches++;
        //         }
        //     }
        // }

        // debug_results2(results);

        // TODO: select the results based on watermarks
        logger(DBG, "Batch %d", b);
        for (size_t i = 0; i < num_threads; i++) {
            emit_results(results[i]);
            matches += (uint32_t) args[i].local_matches;
        }


        // retire the tuples
        oldest_r = retire(windowR, oldest_r, 1);
        oldest_s = retire(windowS, oldest_s, 1);

        processed_r += batchSizeR;
        processed_s += batchSizeS;
        b++;

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
