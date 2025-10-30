//
// Created by kaichi on 11.10.24.
//

#ifndef OCA_H
#define OCA_H

#include "data-types.h"
#include "Commons/EnclaveTimers.h"
#include <vector>
#include <tuple>

#ifndef L2_CACHE_SIZE
#define L2_CACHE_SIZE 2048 // KB
#endif

#ifndef L3_CACHE_SIZE
#define L3_CACHE_SIZE 38400 // KB
#endif

using namespace std;

class OCA {
private:
    window_t * windowR;
    window_t * windowS;
    EnclaveTimers *eTimer;

    void oblivious_append(relation_t * batch, bool batch_sorted, window_t * window, uint32_t counter, uint32_t num_threads);

    table_id_t * oblivious_append(relation_t * batch, bool batch_sorted, bool batch_is_r, window_t * window, uint32_t num_threads);
    table_id_t * oblivious_append(relation_t * batch, bool batch_sorted, bool batch_is_r, relation_t * window, uint32_t num_threads);

    uint32_t scan(table_id_t * t, vector<tuple<row_table_t, row_table_t>>& results, uint32_t num_threads);

    void window_append(window_t * window, relation_t * batch, uint32_t counter);

    void oblivious_shuffle(window_t * window, size_t num_threads);

public:
    OCA(uint32_t windowRSize, uint32_t windowSSize, uint32_t batchRSize, uint32_t batchSSize) {
        this->eTimer = new EnclaveTimers();
        windowR = (window_t*) malloc(sizeof(window_t));
        windowS = (window_t*) malloc(sizeof(window_t));

        windowR->tuples = (row_t*) malloc((windowRSize + batchRSize) * sizeof(row_t));
        windowR->num_tuples = 0;
        windowR->window_size = windowRSize;
        windowR->capacity = windowRSize + batchRSize;
        windowS->tuples = (row_t*) malloc((windowSSize + batchSSize) * sizeof(row_t));
        windowS->num_tuples = 0;
        windowS->window_size = windowSSize;
        windowS->capacity = windowSSize + batchSSize;

    }

    ~OCA() {
        delete eTimer;
        free(windowR->tuples);
        free(windowR);
        free(windowS->tuples);
        free(windowS);
    }

    result_t * l2_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg);

    void invalidate_tuples(window_t * window, int to_remove, uint32_t oldest_counter, bool * marked, size_t * marked_prefix_sums);

    uint32_t retire(window_t * window, uint32_t oldest_counter, uint32_t num_threads);

    result_t * l2v2_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg);

    result_t * l3_l4_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg, bool L3);

    result_t * l3_l4_join2(relation_t *relR, relation_t *relS, joinconfig_t * cfg, bool L3);

    void SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize);

};

#endif //OCA_H
