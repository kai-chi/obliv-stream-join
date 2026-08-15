#ifndef OPAQUEJOIN_HPP
#define OPAQUEJOIN_HPP

#include "data-types.h"
#include "Commons/EnclaveTimers.h"
#include <ObliviousComputationApproach/outil.h>

// This is the "SORT" family (FK-SORT-L2/L3/L4) - the Opaque-style baseline
// we compare our own FK-MERG joins against (OCA.h / OCA-L2.cpp). The
// difference: FK-MERG keeps windows sorted and appends batches with OAppend,
// this one doesn't bother keeping anything sorted between batches. Every
// round it just dumps the batch into the window and re-sorts the whole
// thing with bitonic sort before scanning - basically re-running Opaque's
// static join on every micro-batch. That's O(N log^2 N) per batch, vs.
// FK-MERG's O(N log N) via OAppend, so treat this as the "what if we didn't
// optimize" baseline.
//
// Wiring from Enclave.cpp: FK-SORT-L2 -> OPAQ_L2 -> l3_l4_join(..., true),
// batch size forced to 1 (tuple-at-a-time); FK-SORT-L3 -> OPAQ_L3 ->
// l3_l4_join(..., true); FK-SORT-L4 -> OPAQ_L4 -> l3_l4_join(..., false).
// The bool is called L3: true means trim the dummy result slots with
// oblivious compaction before emitting (L3 - leaks per-batch cardinality),
// false means keep every slot, real or dummy (L4 - worst-case padding,
// nothing leaked beyond the public window/batch sizes).
class OpaqueJoin
{
private:
    window_t *windowR;
    window_t *windowS;
    EnclaveTimers *eTimer;

    // Concatenates window + batch into `merged` (window tagged batch_is_r,
    // batch tagged !batch_is_r), then pads with infinity dummies up to the
    // next power of two so the bitonic sort in l3_l4_join has a clean input.
    void append_to_window(table_id_t *merged, relation_t * batch, bool batch_is_r, window_t * window);
    void append_to_window(table_id_t *merged, relation_t * batch, bool batch_is_r, relation_t * window);

    // Walks a sorted, tagged collection and tracks the last R-side
    // (table_id 0, primary key) tuple seen via conditional_select. Every
    // tuple produces exactly one output slot: a real match if it's an
    // S-side tuple whose key equals the tracked PK, a dummy otherwise -
    // so output size always equals input size. Returns how many were real.
    // This is the SCAN step from the paper (track last PK, join against
    // whatever FK tuple comes next).
    uint32_t scan(table_id_t *t, std::vector<std::tuple<row_table_t, row_table_t>> &results, uint32_t num_threads);

    // ORETIRE: marks tuples older than the window's retention threshold
    // (age = counter packed into the top bits of ts.tv_sec, see
    // invalidate_tuples below) and compacts them out obliviously via
    // orcpar. Returns the new oldest-counter watermark.
    uint32_t retire(window_t *window, uint32_t oldest_counter, uint32_t num_threads);

    // Overwrites everything older than oldest_counter + to_remove with an
    // infinity dummy, and builds the marked/marked_prefix_sums arrays that
    // retire()'s oblivious compaction needs.
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
     * (kept for interface symmetry with OCA/OCAKras - not actually defined
     * in OpaqueJoin.cpp, l3_l4_join is the real entry point)
     * */
    result_t * join(relation_t *relR, relation_t *relS, joinconfig_t *cfg);


    /**
     * FK-SORT-L3/L4 entry point, one round per micro-batch. To avoid a full
     * cross join and still get unique results, each round does three
     * merge-sort-scan passes:
     *   T1 = sort(W_R ++ S_batch)     -> new S tuples against the R window
     *   T2 = sort(R_batch ++ W_S)     -> new R tuples against the S window
     *   T3 = sort(R_batch ++ S_batch) -> new R tuples against new S tuples
     * i.e. (W_R join S_batch) u (R_batch join W_S) u (R_batch join S_batch),
     * which covers every pair touching the current batch exactly once.
     * Nothing is kept sorted between rounds - each merge pads to a power of
     * two and gets a full bitonic sort, no OAppend involved. scan() then
     * hands back one result slot per tuple scanned. After that, batches get
     * appended to the windows and retire() evicts anything expired via
     * oblivious compaction. L3=true compacts the dummy slots out of the
     * result before emitting (leaks per-batch cardinality); L3=false keeps
     * them all, i.e. full worst-case padding.
     * */
    result_t * l3_l4_join(relation_t *relR, relation_t *relS, joinconfig_t *cfg, bool L3);

    void emit_results(const std::vector<res_type> & results);
};


#endif //OPAQUEJOIN_HPP
