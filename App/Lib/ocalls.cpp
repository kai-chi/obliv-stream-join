#include "Enclave_u.h"
#include "Logger.h"
#include <cstdio>
#include <fstream>
#include <sys/time.h>
#include <assert.h>

extern int write_to_file;
extern char experiment_filename[512];

static inline u_int64_t rdtsc(void)
{
    u_int32_t hi, lo;

    __asm__ __volatile__("rdtsc"
    : "=a"(lo), "=d"(hi));

    return (u_int64_t(hi) << 32) | u_int64_t(lo);
}

uint64_t ocall_rdtsc() {
    return rdtsc();
}

void ocall_startTimer(u_int64_t* t) {
    *t = rdtsc();
}

void ocall_stopTimer(u_int64_t* t) {
    *t = rdtsc() - *t;
}

void ocall_exit(int exit_status) {
    exit(exit_status);
}

void ocall_print_string(const char *str)
{
    /* Proxy/Bridge will check the length and null-terminate
     * the input string to prevent buffer overflow.
     */
    printf("%s", str);
}

void ocall_log_string(LEVEL level, const char *str)
{
    logger(level, str);
}

void ocall_write_to_file(const char *str)
{
    if (write_to_file) {
        std::ofstream file(experiment_filename, std::ios_base::app);
        if (file.is_open()) {
            file << str;
            file.close();
        } else {
            logger(ERROR, "Error opening experiment file");
        }
    }
}

uint64_t ocall_get_system_micros() {
    struct timeval tv{};
    gettimeofday(&tv, nullptr);
    return tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
}

void ocall_throw(const char* message)
{
    logger(ERROR, "%s", message);
    exit(EXIT_SUCCESS);
}

void sgx_assert(int cond, char *errorMsg)
{
    (void) cond;
    (void) errorMsg;
    assert(cond && errorMsg);
}

uint64_t ocall_clock_cycles() {
    unsigned int lo = 0;
    unsigned int hi = 0;
    __asm__ __volatile__ (
            "lfence;rdtsc;lfence" : "=a"(lo), "=d"(hi)
            );
    return ((uint64_t)hi << 32) | lo;
}