#ifndef _COMMONS_H_
#define _COMMONS_H_

#include "data-types.h"

typedef struct args_t {
    algorithm_t* algorithm;
    char algorithm_name[128];
    char r_path[512];
    char s_path[512];
    uint32_t r_size;
    uint32_t s_size;
    uint32_t r_seed;
    uint32_t s_seed;
    uint32_t nthreads;
    double skew;
    int r_from_path;
    int s_from_path;
    char experiment_name[256];
    int write_to_file;
    int csv_print;
    int self_join;
    uint32_t r_batch;
    uint32_t s_batch;
    /* Size of the R window in tuples */
    int r_window;
    /* Size of the S window in tuples */
    int s_window;
    int fk_join;
    /** input data rate for stream R (in tuples/sec) */
    unsigned int r_rate;
    /** input data rate for stream S (in tuples/sec) */
    unsigned int s_rate;
    int no_sgx;
} args_t;

void set_default_args(args_t * params);

void parse_args(int argc, char ** argv, args_t * params, struct algorithm_t algorithms[]);

#endif // _COMMONS_H_
