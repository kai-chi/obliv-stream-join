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

// OCA = Oblivious Computation Approach, the paper's other FK stream-join
// family (the ORAM-free one). Instead of an oblivious index it keeps windows
// as sorted arrays and appends batches with OAppend (Algorithm 3), so a join
// is just a linear scan over a merged, tagged array.
//
// alg name -> Enclave.cpp wrapper -> entry point here:
//   FK-EPHI-L2 -> OCA_L2_wrapper       -> l2_join()   (Algorithm 5, ephemeral hash index)
//   FK-MERG-L2 -> OCA_L2v2_wrapper     -> l2v2_join() (OAppend join, one tuple at a time)
//   FK-MERG-L3 -> OCA_L3_wrapper_split -> l3_l4_join2(cfg, L3=true)
//   FK-MERG-L4 -> OCA_L4_wrapper_split -> l3_l4_join2(cfg, L3=false)
// l3_l4_join() is the earlier non-split version of the same algorithm,
// still reachable via OCA_L3_wrapper/OCA_L4_wrapper. L3 runs OCOMPACTION to
// drop dummy results (leaks batch cardinality), L4 leaves the padding in.
class OCA {
private:
    window_t * windowR;
    window_t * windowS;
    EnclaveTimers *eTimer;

    // OAppend, Algorithm 3: sort the batch if needed, pad it up to the
    // window's size with infinity dummies, bitonic-merge it into `window`
    // in place. Window grows by batch->num_tuples, stays sorted.
    void oblivious_append(relation_t * batch, bool batch_sorted, window_t * window, uint32_t counter, uint32_t num_threads);

    // same idea, but merges `batch` against the *other* stream's window
    // instead of its own, tagging each tuple with which side it came from
    // (table_id). scan() below walks this tagged, sorted output for matches.
    table_id_t * oblivious_append(relation_t * batch, bool batch_sorted, bool batch_is_r, window_t * window, uint32_t num_threads);
    table_id_t * oblivious_append(relation_t * batch, bool batch_sorted, bool batch_is_r, relation_t * window, uint32_t num_threads);

    // the SCAN step from Algorithm 4. Walks a table_id-tagged, key-sorted
    // merge and, for an FK join, carries the last-seen PK tuple forward
    // (obliviously) until it hits the matching FK tuple, emitting a match or
    // a dummy at every position. Returns how many real matches it found.
    uint32_t scan(table_id_t * t, vector<tuple<row_table_t, row_table_t>>& results, uint32_t num_threads);

    // plain (non-oblivious) append, only used by l2_join() since that path
    // builds an ephemeral hash index instead of keeping a sorted window.
    void window_append(window_t * window, relation_t * batch, uint32_t counter);

    // oblivious shuffle used by l2_join() to randomize tuple order before
    // building/probing the ephemeral index (Algorithm 5, FK-EPHI-L2).
    void oblivious_shuffle(window_t * window, size_t num_threads);

public:
    OCA(uint32_t windowRSize, uint32_t windowSSize, uint32_t batchRSize, uint32_t batchSSize) {
        this->eTimer = new EnclaveTimers();
        windowR = (window_t*) malloc(sizeof(window_t));
        windowS = (window_t*) malloc(sizeof(window_t));

        // OAppend bitonic-merges the window against a dummy-padded copy of the
        // batch of the same size, so the buffer has to hold the next power of
        // two above 2*(window+batch). Sizing it once here is what lets
        // oblivious_append skip realloc'ing (and therefore memcpy'ing) the
        // whole window up and back down on every single batch.
        uint32_t capR = 1, capS = 1;
        while (capR < 2 * (windowRSize + batchRSize)) capR <<= 1;
        while (capS < 2 * (windowSSize + batchSSize)) capS <<= 1;

        windowR->tuples = (row_t*) malloc(capR * sizeof(row_t));
        windowR->num_tuples = 0;
        windowR->window_size = windowRSize;
        windowR->capacity = capR;
        windowS->tuples = (row_t*) malloc(capS * sizeof(row_t));
        windowS->num_tuples = 0;
        windowS->window_size = windowSSize;
        windowS->capacity = capS;

    }

    ~OCA() {
        delete eTimer;
        free(windowR->tuples);
        free(windowR);
        free(windowS->tuples);
        free(windowS);
    }

    // FK-EPHI-L2, Algorithm 5: shuffle both windows obliviously, build a
    // throwaway hash index on window S, probe it with window R, then throw
    // the index away. Since the index is never reused across batches, all
    // that leaks is per-tuple match count (L2).
    result_t * l2_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg);

    // marks the window entries that have aged out (based on the batch
    // counter packed into the tuple's timestamp) so retire() can compact
    // them out obliviously.
    void invalidate_tuples(window_t * window, int to_remove, uint32_t oldest_counter, bool * marked, size_t * marked_prefix_sums, uint32_t num_threads);

    // ORETIRE: once a window has grown past its configured size, obliviously
    // compact out the expired tuples and keep only the newest window_size.
    uint32_t retire(window_t * window, uint32_t oldest_counter, uint32_t num_threads);

    // FK-MERG-L2: same OAppend/SCAN machinery as l3_l4_join2 below, just run
    // one tuple at a time instead of in batches (OCA_L2v2_wrapper forces
    // batch size to 1). Same L2 leakage profile as l2_join(), different path.
    result_t * l2v2_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg);

    // Algorithm 4, FK-MERG-L3/L4, first (non-split) version. Per batch:
    // OAppend R into W_R, then OAppend the S-batch against the now-bigger
    // W_R and scan it, then OAppend the R-batch against the old W_S and
    // scan that, then OAppend S into W_S and retire both windows. With
    // L3=true the dummy results get trimmed by OCOMPACTION (leaks batch
    // cardinality); L3=false keeps the full padding, i.e. L4.
    result_t * l3_l4_join(relation_t *relR, relation_t *relS, joinconfig_t * cfg, bool L3);

    // Same result as l3_l4_join(), just reordered so no merge ever touches
    // an already-batch-extended window. Splits that first "S-batch vs
    // extended W_R" merge into two cleaner, power-of-two-sized merges
    // (S-batch vs old W_R, and S-batch vs R-batch) and only appends R/S into
    // their windows once all the scans are done. This is the one actually
    // wired to FK-MERG-L3/FK-MERG-L4.
    result_t * l3_l4_join2(relation_t *relR, relation_t *relS, joinconfig_t * cfg, bool L3);

    void SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize);

};

#endif //OCA_H
