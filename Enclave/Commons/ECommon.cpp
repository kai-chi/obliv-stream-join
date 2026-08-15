#include "ECommon.h"

uint64_t getCyclesSinceStart(uint64_t start) {
    return clock_cycles() - start;
}

uint32_t getMicrosSinceStart(uint64_t start) {
    uint64_t cycles = getCyclesSinceStart(start);
    return (uint32_t) ((1000000000*cycles) / CPU_FREQ);
}
