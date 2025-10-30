#include "commons.h"

#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <tlibc/stdbool.h>
#include "data-types.h"

#include "Logger.h"

void set_default_args(args_t * params) {
    params->r_size        = 1000000;
    params->s_size        = 1000000;
    params->r_seed        = 11111;
    params->s_seed        = 22222;
    params->nthreads      = 1;
    params->skew          = 0;
    params->r_from_path   = 0;
    params->s_from_path   = 0;
    params->csv_print     = 0;
    params->write_to_file = 0;
    params->self_join     = 0;
    params->r_batch       = 0;
    params->s_batch       = 0;
    params->r_window      = 1000000;
    params->s_window      = 1000000;
    params->fk_join       = 0;
    params->r_rate        = 1000;
    params->s_rate        = 1000;
    params->no_sgx        = 0;
    strcpy(params->algorithm_name, "SHJ-L0");
}

void parse_args(int argc, char ** argv, args_t * params, struct algorithm_t algorithms[]) {
    int c, i, found;
    bool def_dataset = false, def_table = false;
    static int csv_print, self_join, fk_join, no_sgx;
    char *eptr;


    while (true) {

        static struct option long_options[] =
        {
                {"csv", no_argument, &csv_print, 1},
                {"self-join", no_argument, &self_join, 1},
                {"fk-join", no_argument, &fk_join, 1},
                {"no-sgx", no_argument, &no_sgx, 1},

                {"alg",    required_argument, 0, 'a'},
                {"r-batch",    required_argument, 0, 'b'},
                {"s-batch",    required_argument, 0, 'c'},
                {"dataset",    required_argument, 0, 'd'},
                {"r-rate",    required_argument, 0, 'p'},
                {"s-rate",    required_argument, 0, 'q'},
                {"r-size",    required_argument, 0, 'r'},
                {"s-size",    required_argument, 0, 's'},
                {"r-path",    required_argument, 0, 't'},
                {"s-path",    required_argument, 0, 'u'},
                {"r-window",    required_argument, 0, 'v'},
                {"s-window",    required_argument, 0, 'w'},
                {"nthreads", required_argument, 0, 'n'},
                {"skew",    required_argument, 0, 'z'},

        };

        int option_index = 0;

        c = getopt_long(argc, argv, "a:b:d:e:k:m:r:p:q:s:t:u:v:w:z:hv",
                   long_options, &option_index);

        if (c == -1) {
            break;
        }
        switch (c) {
            case 0:
                /* If this option set a flag, do nothing else now. */
                if (long_options[option_index].flag != 0)
                    break;
                logger(DBG, "option %s", long_options[option_index].name);
                if (optarg)
                    logger(DBG, "with arg %s", optarg);
                break;
            case 'a':
                if (algorithms == nullptr)
                {
//                    logger(DBG, "Not checking the validity of algorithm when parsing - "
//                                "will check in the enclave.");
                }
                else {
                    i = 0; found = 0;
                    while(i < INT32_MAX) {
                        if (strcmp(optarg, algorithms[i].name) == 0) {
                            params->algorithm = &algorithms[i];
                            found = 1;
                            break;
                        }
                        i++;
                    }
                    if (found == 0) {
                        logger(ERROR, "Join algorithm not found: %s", optarg);
                        exit(EXIT_SUCCESS);
                    }
                }
                strcpy(params->algorithm_name, optarg);
                break;

            case 'b':
                params->r_batch = atoi(optarg);
                break;

            case 'c':
                params->s_batch = atoi(optarg);
            break;

            case 'd':
                if (def_table) {
                    logger(WARN, "Select a predefined dataset OR specify tables sizes");
                }
                def_dataset = 1;
                if (strcmp(optarg, "synth-1") == 0)
                {
                    params->r_rate   = 1024;
                    params->s_rate   = 1024;
                    params->r_size   = 2000000;
                    params->s_size   = 2000000;
                    params->r_window = 65536;
                    params->s_window = 65536;
                    params->r_batch  = 1024;
                    params->s_batch  = 1024;
                    params->skew     = 0;
                    params->fk_join  = 1;
                    fk_join = 1;
                }
                else if (strcmp(optarg, "synth-2") == 0)
                {
                    params->r_rate   = 1024;
                    params->s_rate   = 4096;
                    params->r_size   = 2000000;
                    params->s_size   = 2000000;
                    params->r_window = 65536;
                    params->s_window = 65536;
                    params->r_batch  = 1024;
                    params->s_batch  = 4096;
                    params->skew     = 0;
                    params->fk_join  = 1;
                    fk_join = 1;
                }
                else if (strcmp(optarg, "tpch-1") == 0) // TPCH 1
                {
                    params->r_from_path = 1;
                    params->s_from_path = 1;
                    strcpy(params->r_path, "data/customer_custkey_01.tbl");
                    strcpy(params->s_path, "data/orders_custkey_01.tbl");
                    params->r_rate   = 1024;
                    params->s_rate   = 4096;
                    params->r_window = 65536;
                    params->s_window = 65536;
                    params->r_batch  = 1024;
                    params->s_batch  = 4096;
                    params->fk_join  = 1;
                    fk_join = 1;
                }
                else
                {
                    logger(ERROR, "Unrecognized dataset: %s", optarg);
                    exit(EXIT_FAILURE);
                }
                break;

            case 'e':
#ifdef DEBUG
                logger(DBG, "Experiment name: %s", optarg);
                strcpy(params->experiment_name, optarg);
                params->write_to_file = 1;
#else
                logger(WARN, "Trying to log output to file with DEBUG disabled");
#endif
                break;

            case 'h':
                logger(INFO, "\nThe currently supported list of command line arguments:\n"
                             "* `-a | --alg` - join algorithm name. Currently supported: `SHJ_NO_ST`. Default: `SHJ_NO_ST`\n"
                             "* `-b | --batch ` - size of the stream batch to process.\n"
                             "* `-d` - name of pre-defined dataset. Currently supported: `none`, `none`. Default: `none`\n"
                             "* `-h` - print help message\n"
                             "* `-p | --r-rate` - tuples / s . Default: `1`\n"
                             "* `-q | --s-rate` - tuples / s . Default: `1`\n"
                             "* `-r | --r-size` - number of tuples of stream R. Default: `1000000`\n"
                             "* `-s | --s-size` - number of tuples of stream S. Default: `1000000`\n"
                             "* `-t | --r-path` - filepath to build stream R. Default: `none`\n"
                             "* `-u | --s-path` - filepath to build stream S. Default `none`\n"
                             "* `-v | --r-window` - Default: `1000000`\n"
                             "* `-w | --s-window` - Default: `1000000`\n"
                             "* `-z | --skew` - data skew. Default: `0`\n"
                             "\n"
                             "The supported working list of command line flags:\n"
                             "* `--self-join` - . Default: `false`\n"
                             "* `--fk-join` - . Default: `false`");
                exit(EXIT_SUCCESS);
                break;

            case 'n':
                params->nthreads = atoi(optarg);
                break;

            case 'p':
                params->r_rate = (unsigned int) atoi(optarg);
                break;

            case 'q':
                params->s_rate = (unsigned int) atoi(optarg);
                break;

            case 'r':
                if (def_dataset) {
                    logger(WARN, "Select a predefined dataset OR specify tables sizes");
                }
//                params->r_size = atoi(optarg);
                params->r_size = (uint32_t) strtoul(optarg, &eptr, 10);
                def_table = 1;
                break;

            case 's':
                if (def_dataset) {
                    logger(WARN, "Select a predefined dataset OR specify tables sizes");
                }
//                params->s_size = atoi(optarg);
                params->s_size = (uint32_t) strtoul(optarg, &eptr, 10);
                def_table = 1;
                break;

            case 't':
                params->r_from_path = 1;
                strcpy(params->r_path, optarg);
                break;

            case 'u':
                params->s_from_path = 1;
                strcpy(params->s_path, optarg);
                break;

            case 'v':
                params->r_window = atoi(optarg);
                break;

            case 'w':
                params->s_window = atoi(optarg);
                break;

            case 'z':
                params->skew = atof(optarg);
                break;

            default:
                break;
        }
    }

    params->csv_print = csv_print;
    params->self_join = self_join;
    params->fk_join   = fk_join;
    params->no_sgx    = no_sgx;

    /* Print remaining command line arguments */
    if (optind < argc) {
        logger(INFO, "remaining arguments: ");
        while (optind < argc) {
            logger(INFO, "%s", argv[optind++]);
        }
    }
}