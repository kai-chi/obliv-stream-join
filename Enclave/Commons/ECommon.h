#ifndef ECOMMON_H
#define ECOMMON_H

#include "Config.hpp"
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include "Enclave.h"
#include "Enclave_t.h"

static inline uint64_t clock_cycles() {
#ifdef SGX2
    unsigned int lo = 0;
    unsigned int hi = 0;
    __asm__ __volatile__ (
            "lfence;rdtsc;lfence" : "=a"(lo), "=d"(hi)
            );
    return ((uint64_t)hi << 32) | lo;
#else
    uint64_t ret;
    sgx_status_t result = ocall_clock_cycles(&ret);
    if (result != SGX_SUCCESS) {
        logger(ERROR, "Error reading clock cycles");
        throw std::runtime_error("");
    }
    return ret;
#endif
}

uint64_t getCyclesSinceStart(uint64_t start);

uint32_t getMicrosSinceStart(uint64_t start);


#endif //ECOMMON_H