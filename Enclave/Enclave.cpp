/*
 * Copyright (C) 2011-2020 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "Enclave.h"
#include "Enclave_t.h" /* print_string */
#include <cstdarg>
#include <cstdio> /* vsnprintf */
#include <cstring>
#include "data-types.h"
#include "ObliviousSymmetricHashJoin/SHJ.h"
#include "NestedLoopJoin/NLJ.hpp"
#include "Opaque/OpaqueJoin.hpp"
#include "ObliviousComputationApproach/OCA.h"
#include "ObliviousComputationApproach/OCAKras.h"

/*
 * printf: 
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */

static inline uint64_t rdtsc_asm(void)
{
    uint32_t hi, lo;

    __asm__ __volatile__("rdtsc"
            : "=a"(lo), "=d"(hi));

    return (uint64_t(hi) << 32) | uint64_t(lo);
}

uint64_t rdtsc(void) {
#ifdef SGX2
    return rdtsc_asm();
#else
    uint64_t ret;
    ocall_rdtsc(&ret);
    return ret;
#endif
}

int printf(const char* fmt, ...)
{
    char buf[BUFSIZ] = { '\0' };
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    ocall_print_string(buf);
    return (int)strnlen(buf, BUFSIZ - 1) + 1;
}

void logger(LEVEL level, const char *fmt, ...) {
    char buffer[BUFSIZ] = { '\0' };
    va_list args;
#ifndef DEBUG
    if (level == DBG) return;
#endif

    va_start(args, fmt);
    vsnprintf(buffer, BUFSIZ, fmt, args);
    ocall_log_string(level, buffer);
}

void sgx_assert(int cond, const char *errorMsg)
{
    if (!cond) {
        ocall_throw(errorMsg);
    }
}

void logResults(std::string algorithm, result_t *res, joinconfig_t *cfg)
{
    (void) (cfg);
    logger(INFO, "-- Log %s results --", algorithm.c_str());
    logger(INFO, "Join matches             : %lu", res->totalresults);
    logger(INFO, "totalInputTuples         : %lu", cfg->totalInputTuples);
    logger(INFO, "joinTotalTime   [micros] : %lu", res->sjr->joinTotalTime);
    logger(INFO, "joinThroughput [M rec/s] : %.4lf", ((double) cfg->totalInputTuples / (double) res->sjr->joinTotalTime));
    logger(INFO, "throughputJoin [K rec/s] : %.4lf", ((double) 1000*cfg->totalInputTuples / (double) res->sjr->joinTotalTime));
    logger(INFO, "--------------------------");
}

result_t * J4_wrapper(relation_t *relR, relation_t *relS, joinconfig_t * config) {
    auto *nlj = new NLJ_ST((size_t) config->windowRSize, (size_t) config->windowSSize);
    result_t *res = nlj->join(relR, relS, config);
    logResults("J4", res, config);
    delete nlj;
    return res;
}

result_t* SHJ_ST_wrapper(const string& algorithm, relation_t *relR, relation_t *relS, joinconfig_t *config) {
    auto *shj = new SHJ();
    shj->SHJ_init(algorithm, config->windowRSize, config->windowSSize);
    result_t *res = shj->join_st(relR, relS, config);
    config->totalInputTuples = relR->num_tuples + relS->num_tuples - config->windowRSize - config->windowSSize;
    logResults(algorithm, res, config);
    delete shj;
    return res;
}

result_t* J0_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *config) {
    (void) (relR);
    (void) (relS);
    (void) (config);

    logger(ERROR, "SHJ-L0 should be run out of SGX");
    throw runtime_error("");
    return nullptr;
    // return SHJ_ST_wrapper("J0", relR, relS, config);
}

result_t* J1_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *config) {
    return SHJ_ST_wrapper("SHJ-L1", relR, relS, config);
}

result_t* J2_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *config) {
    return SHJ_ST_wrapper("SHJ-L2", relR, relS, config);
}

result_t* SHJ_L3(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    auto *shj = new SHJ();
    shj->SHJ_init("SHJ-L3", cfg->windowRSize + cfg->batchRSize, cfg->windowSSize + cfg->batchSSize);
    result_t *res = shj->join_l3(relR, relS, cfg, false);
    cfg->totalInputTuples = relR->num_tuples + relS->num_tuples - cfg->windowRSize - cfg->windowSSize;
    logResults("SHJ-L3", res, cfg);
    delete shj;
    return res;
}

result_t* SHJ_L4(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    auto *shj = new SHJ();
    shj->SHJ_init("SHJ-L4", cfg->windowRSize + cfg->batchRSize, cfg->windowSSize + cfg->batchSSize);
    result_t *res = shj->join_l3(relR, relS, cfg, true);
    cfg->totalInputTuples = relR->num_tuples + relS->num_tuples - cfg->windowRSize - cfg->windowSSize;
    logResults("SHJ-L4", res, cfg);
    delete shj;
    return res;
}

result_t* OCA_L2_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "OCA-L2 works only with FK joins");
        return nullptr;
    }
    auto *oca = new OCA(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->l2_join(relR, relS, cfg);
    cfg->totalInputTuples = relR->num_tuples + relS->num_tuples - cfg->windowRSize - cfg->windowSSize;
    logResults("OCA-L2", res, cfg);
    delete oca;
    return res;
}

result_t* OCA_L2v2_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "MERG-L2 works only with FK joins");
        return nullptr;
    }
    cfg->batchRSize = 1;
    cfg->batchSSize = 1;
    auto *oca = new OCA(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->l2v2_join(relR, relS, cfg);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("MERG-L3", res, cfg);
    delete oca;
    return res;
}

result_t* OCA_L3_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "MERG-L3 works only with FK joins");
        return nullptr;
    }
    auto *oca = new OCA(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->l3_l4_join(relR, relS, cfg, true);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("MERG-L3", res, cfg);
    delete oca;
    return res;
}

result_t* OCA_L3_wrapper_split(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "MERG-L3 works only with FK joins");
        return nullptr;
    }
    auto *oca = new OCA(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->l3_l4_join2(relR, relS, cfg, true);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("MERG-L3-SPLIT", res, cfg);
    delete oca;
    return res;
}

result_t* OCA_L4_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "MERG-L4 works only with FK joins");
        return nullptr;
    }
    auto *oca = new OCA(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->l3_l4_join(relR, relS, cfg, false);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("MERG-L4", res, cfg);
    delete oca;
    return res;
}

result_t* KRAS_L2_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    cfg->batchRSize = 1;
    cfg->batchSSize = 1;
    auto *oca = new OCAKras(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->join(relR, relS, cfg);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("JOIN-L2", res, cfg);
    delete oca;
    return res;
}

result_t* KRAS_L3_wrapper(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    auto *oca = new OCAKras(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->join(relR, relS, cfg);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("JOIN-L3", res, cfg);
    delete oca;
    return res;
}

result_t* OCA_L4_wrapper_split(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "MERG-L4 works only with FK joins");
        return nullptr;
    }
    auto *oca = new OCA(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = oca->l3_l4_join2(relR, relS, cfg, false);
    cfg->totalInputTuples -= (cfg->windowRSize + cfg->windowSSize);
    logResults("MERG-L4-SPLIT", res, cfg);
    delete oca;
    return res;
}

result_t* OPAQ_L2(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "OPAQUE runs only for FK join");
        return nullptr;

    }
    cfg->batchRSize = 1;
    cfg->batchSSize = 1;
    auto *opaque = new OpaqueJoin(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = opaque->l3_l4_join(relR, relS, cfg, true);
    logResults("SORT-L2", res, cfg);
    delete opaque;
    return nullptr;
}

result_t* OPAQ_L3(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "OPAQUE runs only for FK join");
        return nullptr;

    }
    auto *opaque = new OpaqueJoin(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = opaque->l3_l4_join(relR, relS, cfg, true);
    logResults("SORT-L3", res, cfg);
    delete opaque;
    return nullptr;
}

result_t* OPAQ_L4(relation_t *relR, relation_t *relS, joinconfig_t *cfg) {
    if (!cfg->fkJoin) {
        logger(ERROR, "OPAQUE runs only for FK join");
        return nullptr;

    }
    auto *opaque = new OpaqueJoin(cfg->windowRSize, cfg->windowSSize, cfg->batchRSize, cfg->batchSSize);
    result_t *res = opaque->l3_l4_join(relR, relS, cfg, false);
    logResults("SORT-L4", res, cfg);
    delete opaque;
    return nullptr;
}

static struct algorithm_t sgx_algorithms[] = {
        {"SHJ-L0",          J0_wrapper},
        {"SHJ-L1",          J1_wrapper},
        {"SHJ-L2",          J2_wrapper},
        {"SHJ-L3",          SHJ_L3},
        {"SHJ-L4",          SHJ_L4},

        {"FK-EPHI-L2",          OCA_L2_wrapper},
        {"FK-MERG-L2",          OCA_L2v2_wrapper},
        {"FK-MERG-L3",          OCA_L3_wrapper_split},
        {"FK-MERG-L4",          OCA_L4_wrapper_split},

        {"NLJ-L4",          J4_wrapper},

        {"FK-SORT-L2",      OPAQ_L2},
        {"FK-SORT-L3",      OPAQ_L3},
        {"FK-SORT-L4",      OPAQ_L4},

        {"NFK-JOIN-L2", KRAS_L2_wrapper},
        {"NFK-JOIN-L3", KRAS_L3_wrapper},
};

void ecall_join(result_t * res, struct table_t * relR, struct table_t * relS, char *algorithm_name, joinconfig_t * config)
{
    int i =0, found = 0;
    algorithm_t *algorithm = nullptr;
    while(sgx_algorithms[i].join)
    {
        if (strcmp(algorithm_name, sgx_algorithms[i].name) == 0)
        {
            found = 1;
            algorithm = &sgx_algorithms[i];
            break;
        }
        i++;
    }
    if (found == 0)
    {
        printf("Algorithm not found: %s", algorithm_name);
        ocall_exit(EXIT_FAILURE);
    }
    result_t * tmp = algorithm->join(relR, relS, config);
    if (tmp != nullptr) {
        memcpy(res, tmp, sizeof(result_t));
    } else {
        res = nullptr;
    }
}
