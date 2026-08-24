#ifndef _COMMONS_H_
#define _COMMONS_H_

#include "data-types.h"

// Parsed ./app command-line flags. set_default_args() fills in the defaults,
// then parse_args() overrides whatever was passed on the command line - see
// App.cpp's -h output for the actual flag names, or App/Lib/commons.c for
// what --dataset presets (synth-1/synth-2/tpch-1) set here on your behalf.
typedef struct args_t {
    algorithm_t* algorithm; // only set if parse_args() was given a non-null algorithms[] to validate against - ecall_join does its own lookup either way, so this is usually left null
    char algorithm_name[128]; // this is what actually gets passed to ecall_join
    char r_path[512];
    char s_path[512];
    uint32_t r_size; // ignored when r_from_path is set - gets overwritten with the file's line count
    uint32_t s_size;
    uint32_t r_seed; // seeds the generator before building R, so runs are repeatable
    uint32_t s_seed;
    uint32_t nthreads;
    double skew; // > 0 switches R/S generation to a Zipfian distribution over keys instead of uniform
    int r_from_path;
    int s_from_path;
    char experiment_name[256]; // only used if write_to_file is set, and that only works in DEBUG builds - see the -e case in parse_args()
    int write_to_file;
    int csv_print;
    int self_join; // joins tableR against itself instead of R against S
    uint32_t r_batch;
    uint32_t s_batch;
    /* Size of the R window in tuples */
    int r_window;
    /* Size of the S window in tuples */
    int s_window;
    int fk_join; // required by every FK-*/NFK-JOIN-* algorithm; those refuse to run without it
    /** input data rate for stream R (in tuples/sec) */
    unsigned int r_rate;
    /** input data rate for stream S (in tuples/sec) */
    unsigned int s_rate;
    int no_sgx; // skip the enclave and run App/SHJ/'s plaintext-outside-SGX path instead - only meaningful for "SHJ" and "SHJ-L0"
} args_t;

void set_default_args(args_t * params);

void parse_args(int argc, char ** argv, args_t * params, struct algorithm_t algorithms[]);

#endif // _COMMONS_H_
