#ifndef CONFIG_HPP
#define CONFIG_HPP

// defines the frequency of the CPU in Hz - remember to disable frequency scaling for experiments
#ifndef CPU_FREQ
#define CPU_FREQ 2000000000
#endif

// tells if SGXv1 is available on this machine - comment out for compilation on SGXv2
#ifndef SGX1
// #define SGX1
#endif

// tells if SGXv2 is available on this machine - comment out for compilation on SGXv1
#ifndef SGX2
#define SGX2
#endif

#if (((defined(SGX1)) && (defined(SGX2))) || ((!defined(SGX1)) && (!defined(SGX2))))
#error "Select EITHER SGX1 OR SGX2"
#endif

#ifndef MEASURE_PERF
//#define MEASURE_PERF
#endif

#ifndef JOIN_PERF
// #define JOIN_PERF
#endif // JOIN_PERF

#endif //CONFIG_HPP
