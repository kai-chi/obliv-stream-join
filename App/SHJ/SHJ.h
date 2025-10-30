#ifndef OSHJ_H
#define OSHJ_H

#include "data-types.h"
#include "HashTableInterface.hpp"
#include "DETBucketChainingHashTable.hpp"
#include <string>

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

    DETBucketChainingHashTable* DEThtR;
    DETBucketChainingHashTable* DEThtS;
public:
    result_t * join_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

    void SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize);

    result_t * DETjoin_st(table_enc_t * DETrelR, table_enc_t * DETrelS, joinconfig_t * cfg);
};

void logResults(std::string algorithm, result_t *res, joinconfig_t *cfg, uint32_t input_size);

#endif //OSHJ_H
