#ifndef OSHJ_H
#define OSHJ_H

#include "data-types.h"
#include "../HashTables/HashTableInterface.hpp"
#include <string>
#include <Commons/EnclaveTimers.h>

// This is the in-enclave SHJ family: SHJ-L1 through SHJ-L4 from the paper.
// They all run the same classic three-step SHJ loop (probe the other stream's
// window, insert into your own, expire what fell out) - what changes between
// levels is the window data structure and how much of the access pattern it
// hides.
//
// L1 just uses a plain hash table, RND-encrypted inside the TEE, so it leaks
// the full access pattern - that's what "randomized confidentiality" buys
// you and nothing more. L2 through L4 swap the window for the OBLIWIND
// oblivious B-tree (BTreeHashTable), so a point lookup no longer reveals
// which tuple it hit. L2 still does one lookup per tuple, so it leaks that
// tuple's degree (O(log^2 N) per Table I). L3 and L4 batch things up and pad
// out the lookup count (Algorithm 2 - OEXPANSION/OSEL) to also hide the
// per-tuple output volume: L3 pads to the batch's real match count (so only
// the per-batch total leaks), L4 pads to the worst case, so nothing leaks
// beyond the public window/batch sizes.
//
// Wiring: J1_wrapper/J2_wrapper in Enclave.cpp call SHJ_init then join_st();
// SHJ_L3/SHJ_L4 call SHJ_init then join_l3(cfg, padding). SHJ-L0 isn't wired
// up here at all (J0_wrapper just throws) - it lives outside the enclave in
// App/SHJ/SHJ.cpp's DETjoin_st, since it compares DET-encrypted ciphertexts
// rather than doing oblivious lookups.
class SHJ {
private:
    bool htInstantiated = false;
    bool htInitialized = false;
    uint32_t htRMaxSize = 0;
    uint32_t htSMaxSize = 0;
    uint32_t htRSize = 0;
    uint32_t htSSize = 0;
    HashTableInterface* htR;
    HashTableInterface* htS;
    EnclaveTimers *eTimer;

    struct requests_t {
        struct row_idx_t* tuples;
        uint32_t num_tuples;
    };

    // Bitonic sort (oblivious, data-independent comparator network) used to
    // order per-tuple index requests by their target output slot.
    void bitonic__compare(requests_t * requests, bool ascend, uint32_t i, uint32_t j);

    void bitonic__merge(requests_t * requests, bool ascend, uint32_t lo, uint32_t hi);

    void bitonic__sort(requests_t * requests, bool ascend = true, uint32_t lo = 0, uint32_t hi = UINT32_MAX);

    // Oblivious "distribute": after bitonic sorting, obliviously routes each
    // request to its final (padded) output index via a butterfly-style
    // swap network, resizing to m slots. Building block of oblivious_expansion.
    void oblivious_distribute(requests_t * requests, uint32_t m);

    // OEXPANSION-style step for SHJ-L3/L4 (see Algorithm 2): duplicates each
    // tuple's request by its (padded) join degree so every probe below reads
    // exactly one fixed, publicly-known slot, hiding per-tuple output volume.
    void oblivious_expansion(requests_t *requests, uint32_t m);

public:
    SHJ() {
        this->eTimer = new EnclaveTimers();
    }

    ~SHJ() {
        delete eTimer;
    }

    // Legacy batch/threaded join inherited from the original (static) TeeBench
    // benchmark. Not reachable from the stream-join CLI dispatch table in
    // Enclave.cpp (all SHJ-* algorithms go through join_st or join_l3).
    result_t * join(relation_t *relR, relation_t *relS, joinconfig_t * config);

    // Tuple-at-a-time SHJ loop used by SHJ-L1 and SHJ-L2. Fills both windows,
    // then processes tuples in timestamp order: build into own window, probe
    // opposite window, expire the oldest tuple once the window overflows.
    // Leakage/complexity depends only on the window implementation chosen in
    // SHJ_init (plain hash table for L1, OBLIWIND oblivious B-tree for L2).
    result_t * join_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

    // Micro-batch SHJ used by SHJ-L3 (padding=false) and SHJ-L4 (padding=true),
    // implementing Algorithm 2 of the paper: compute each tuple's join degree
    // via the oblivious index, pad the batch's total lookup count T (to the
    // true total for L3, or to the worst-case batchR*batchS for L4), obliviously
    // expand the batch of requests to length T, then perform exactly T oblivious
    // point lookups so the batch-level output volume is the only thing leaked
    // (L3) or nothing is leaked beyond public sizes (L4).
    result_t * join_l3(relation_t *relR, relation_t *relS, joinconfig_t * config, bool padding);

    // Selects the window backend: non-oblivious RND-encrypted hash tables for
    // "SHJ-L1" (also "SHJ_NO"/"SHJ_NO_ST" legacy aliases), or the OBLIWIND
    // oblivious B-tree (BTreeHashTable) for "SHJ-L2"/"SHJ-L3"/"SHJ-L4"
    // (also "SHJ_BTREE" legacy alias).
    void SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize);
};


#endif //OSHJ_H
