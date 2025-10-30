#ifndef OPAQUEJOIN_HPP
#define OPAQUEJOIN_HPP

#include "data-types.h"
#include "Commons/EnclaveTimers.h"
#include <ObliviousComputationApproach/outil.h>

class OpaqueJoin
{
private:
    window_t *windowR;
    window_t *windowS;
    EnclaveTimers *eTimer;

    void append_to_window(table_id_t *merged, relation_t * batch, bool batch_is_r, window_t * window);
    void append_to_window(table_id_t *merged, relation_t * batch, bool batch_is_r, relation_t * window);
    uint32_t scan(table_id_t *t, std::vector<std::tuple<row_table_t, row_table_t>> &results);
    uint32_t retire(window_t *window, uint32_t oldest_counter, uint32_t num_threads);
    void invalidate_tuples(window_t * window, int to_remove, uint32_t oldest_counter, bool * marked, size_t * marked_prefix_sums);

public:
    OpaqueJoin(uint32_t windowRSize, uint32_t windowSSize, uint32_t batchRSize, uint32_t batchSSize) {
        this->eTimer = new EnclaveTimers();

        this->windowR = (window_t*) malloc(sizeof(window_t));
        this->windowS = (window_t*) malloc(sizeof(window_t));

        this->windowR->tuples = (row_t*) malloc((windowRSize + batchRSize) * sizeof(row_t));
        this->windowR->num_tuples = 0;
        this->windowR->window_size = windowRSize;
        this->windowR->capacity = windowRSize + batchRSize;
        this->windowS->tuples = (row_t*) malloc((windowSSize + batchSSize) * sizeof(row_t));
        this->windowS->num_tuples = 0;
        this->windowS->window_size = windowSSize;
        this->windowS->capacity = windowSSize + batchSSize;
    }

    ~OpaqueJoin() {
        free(windowR->tuples);
        free(windowR);
        free(windowS->tuples);
        free(windowS);
        delete eTimer;
    }

    /**
     * performs a full join on relR and relS
     * */
    result_t * join(relation_t *relR, relation_t *relS, joinconfig_t *cfg);


    /**
     * performs L4 join optimized for FK join. It joins in micro-batches and splits the joins to smaller ones:
     * (W_R ⨝ S) u (R ⨝ W_S)
     * */
    result_t * l3_l4_join(relation_t *relR, relation_t *relS, joinconfig_t *cfg, bool L3);

    void emit_results(const std::vector<res_type> & results);
};


#endif //OPAQUEJOIN_HPP
