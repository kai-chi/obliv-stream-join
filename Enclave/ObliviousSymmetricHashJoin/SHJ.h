#ifndef OSHJ_H
#define OSHJ_H

#include "data-types.h"
#include "../HashTables/HashTableInterface.hpp"
#include <string>
#include <Commons/EnclaveTimers.h>

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

    void bitonic__compare(requests_t * requests, bool ascend, uint32_t i, uint32_t j);

    void bitonic__merge(requests_t * requests, bool ascend, uint32_t lo, uint32_t hi);

    void bitonic__sort(requests_t * requests, bool ascend = true, uint32_t lo = 0, uint32_t hi = UINT32_MAX);

    void oblivious_distribute(requests_t * requests, uint32_t m);

    void oblivious_expansion(requests_t *requests, uint32_t m);

public:
    SHJ() {
        this->eTimer = new EnclaveTimers();
    }

    ~SHJ() {
        delete eTimer;
    }

    result_t * join(relation_t *relR, relation_t *relS, joinconfig_t * config);
    result_t * join_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

    result_t * join_l3(relation_t *relR, relation_t *relS, joinconfig_t * config, bool padding);

    void SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize);
};


#endif //OSHJ_H
