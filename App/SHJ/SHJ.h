#ifndef OSHJ_H
#define OSHJ_H

#include "data-types.h"
#include "HashTableInterface.hpp"
#include "DETBucketChainingHashTable.hpp"
#include <string>

// This is the non-SGX twin of Enclave/ObliviousSymmetricHashJoin/SHJ, run from
// App.cpp's --no-sgx path. It backs two CLI algorithms: plain "SHJ" (join_st,
// no encryption at all - our insecure baseline, everything else gets compared
// against this) and "SHJ-L0" (DETjoin_st). For L0 the App has already
// DET-encrypted keys/payloads with AES before they get here, and
// DETBucketChainingHashTable just compares ciphertext bytes - since DET
// encryption is deterministic, equal plaintexts still produce equal
// ciphertexts, so the whole access pattern leaks. That's the L0 in the name.
// Both stay outside the enclave on purpose: the "security" is just the encryption scheme.
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
    // Plaintext tuple-at-a-time SHJ loop for the insecure "SHJ" baseline.
    result_t * join_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

    // Allocates plain hash tables for "SHJ", or DET hash tables for "SHJ-L0"
    // (any other algorithm name falls back to the plaintext path).
    void SHJ_init(const std::string& algorithm, uint32_t windowRSize, uint32_t windowSSize);

    // Same tuple-at-a-time SHJ loop as join_st, but operating on DET-encrypted
    // rows (row_enc_t) and probing via ciphertext equality. Implements SHJ-L0.
    result_t * DETjoin_st(table_enc_t * DETrelR, table_enc_t * DETrelS, joinconfig_t * cfg);
};

void logResults(std::string algorithm, result_t *res, joinconfig_t *cfg, uint32_t input_size);

#endif //OSHJ_H
