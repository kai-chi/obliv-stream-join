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


#include <cstring>
#include <sys/time.h>
#include <ctime>

#include "sgx_urts.h"
#include "App.h"

#include "AES.hpp"

#include "Lib/ErrorSupport.h"
#include "data-types.h"
#include "Logger.h"
#include "Lib/generator.h"
#include "commons.h"
#include "Enclave_u.h"
#include "SHJ/SHJ.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

char experiment_filename [512];
int write_to_file = 0;

/* Initialize the enclave:
 *   Call sgx_create_enclave to initialize an enclave instance
 */
int initialize_enclave(int no_init)
{
    if (!no_init) {
        sgx_status_t ret = SGX_ERROR_UNEXPECTED;

        logger(INFO, "Initialize enclave");
        /* Call sgx_create_enclave to initialize an enclave instance */
        /* Debug Support: set 2nd parameter to 1 */
        ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL, &global_eid, NULL);
        if (ret != SGX_SUCCESS) {
            ret_error_support(ret);
            return -1;
        }
        logger(INFO, "Enclave id = %d", global_eid);
#ifdef SGX_COUNTERS
        ocall_get_sgx_counters("Start enclave");
#endif
    }
    return 0;
}

block SerialiseInt(uint32_t value) {
    block buffer;
    buffer.push_back(byte_t(value & 0x000000ff));
    buffer.push_back(byte_t((value & 0x0000ff00) >> 8));
    buffer.push_back(byte_t((value & 0x00ff0000) >> 16));
    buffer.push_back(byte_t((value & 0xff000000) >> 24));
    return buffer;
}

/* Application entry */
int SGX_CDECL main(int argc, char *argv[])
{
    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
    set_default_args(&params);

    parse_args(argc, argv, &params, NULL);

    initLogger(params.csv_print);

    struct table_t tableR;
    struct table_t tableS;
    result_t results{};
    sgx_status_t ret;
    struct timespec tw1, tw2;
    double time;
    joinconfig_t joinconfig;

    logger(INFO, "************* TEE_BENCH STREAM APP *************");

    joinconfig.NTHREADS = (int) params.nthreads;
    joinconfig.WRITETOFILE = params.write_to_file;
    joinconfig.LOG = 1;
    joinconfig.CSV_PRINT = params.csv_print;
    joinconfig.windowRSize = params.r_window;
    joinconfig.windowSSize = params.s_window;
    joinconfig.batchRSize  = params.r_batch;
    joinconfig.batchSSize  = params.s_batch;
    joinconfig.fkJoin      = params.fk_join;
    joinconfig.totalInputTuples = (params.r_size + params.s_size);

    if (joinconfig.WRITETOFILE) {
        sprintf(experiment_filename, "%s-%s-%lu",
                params.experiment_name, params.algorithm_name, ocall_get_system_micros()/100000);
        write_to_file = 1;
        logger(DBG, "Experiment filename: %s", experiment_filename);
    }

    logger(INFO, "R Stream: size=%d (tuples) rate=%d (tuples/s), window=%d (tuples), batch=%d",
    	params.r_size, params.r_rate, params.r_window, params.r_batch);
    logger(INFO, "S Stream: size=%d (tuples) rate=%d (tuples/s), window=%d (tuples), batch=%d, skew=%.2f",
           params.s_size, params.s_rate, params.s_window, params.s_batch, params.skew);
    logger(INFO, "nthreads=%d, FK-join=%d, NO-SGX=%d, self-join=%d",
    	params.nthreads, params.fk_join, params.no_sgx, params.self_join);
    logger(INFO, "************************************************");
    seed_generator(params.r_seed);

    if (params.r_from_path)
    {
        logger(INFO, "Build relation R from file %s", params.r_path);
        create_relation_from_file(&tableR, params.r_path, params.r_rate);
        params.r_size = tableR.num_tuples;
        logger(INFO, "R size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
    }
    else if (params.fk_join)
    {
        logger(INFO, "Generate data for a FK join");
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
        create_relation_pk(&tableR, params.r_size, params.r_rate);
    }
    else {
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
        create_relation_nonunique(&tableR, params.r_size, params.r_size, params.r_rate);
    }

    seed_generator(params.s_seed);
    if (params.s_from_path)
    {
        logger(INFO, "Build relation S from file %s", params.s_path);
        create_relation_from_file(&tableS, params.s_path, params.s_rate);
        params.s_size = tableS.num_tuples;
        logger(INFO, "S size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
    }
    else if (params.fk_join)
    {
        logger(INFO, "Generate data for a FK join");
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
        if (params.skew > 0)
        {
            logger(INFO, "Skew relation: %.2lf", params.skew);
            create_relation_zipf(&tableS, params.s_size, params.r_size, params.skew, params.s_rate);
        }
        else {
            create_relation_fk(&tableS, params.s_size, params.r_size, params.s_rate);
        }
    }
    else
    {
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
        if (params.skew > 0) {
            logger(INFO, "Skew relation: %.2lf", params.skew);
            create_relation_zipf(&tableS, params.s_size, params.r_size, params.skew, params.s_rate);
        } else {
            create_relation_nonunique(&tableS, params.s_size, params.r_size, params.s_rate);
        }
    }

    write_relation(&tableR, "R.tbl");
    write_relation(&tableS, "S.tbl");

    initialize_enclave(params.no_sgx);

    logger(INFO, "Running algorithm %s", params.algorithm_name);



    clock_gettime(CLOCK_MONOTONIC, &tw1); // POSIX; use timespec_get in C11
    //
//    if (params.stream_batch_size == 1) {
//
//    }
//    else if (params.stream_batch_size != 0) {
//        uint64_t batch_size = static_cast<uint64_t>(params.stream_batch_size);
//        logger(INFO, "Run streaming-self join (%lu tuples) with stream_batch_size=%d, windowSizeR=%d, windowSizeS=%d",
//               tableR.num_tuples, batch_size, params.windowRSize, params.windowSSize);
//        for (uint64_t i = 0; i < tableR.num_tuples; i += batch_size) {
//            table_t tmpTable{};
//            tmpTable.tuples = tableR.tuples + i;
//            tmpTable.num_tuples = (i + batch_size) <= tableR.num_tuples ?
//                            batch_size : (tableR.num_tuples - i);
//            result_t tmpResults{};
//            logger(DBG, " *** batch table %lu/%lu ***", i, tableR.num_tuples);
//            ret = ecall_join(global_eid,
//                             &tmpResults,
//                             &tmpTable,
//                             &tmpTable,
//                             params.algorithm_name,
//                             &joinconfig);
//        }
//    }
    if (params.self_join) {
        logger(INFO, "Run self join of table R");
        ret = ecall_join(global_eid,
                         &results,
                         &tableR,
                         &tableR,
                         params.algorithm_name,
                         &joinconfig);
    }
    else {
        if (params.no_sgx) {
			if (strcmp(params.algorithm_name, "SHJ-L0") == 0) {
			    bytes<Key> key {0};
			    struct table_enc_t tableEncR;
                struct table_enc_t tableEncS;

			    tableEncR.num_tuples = tableR.num_tuples;
			    tableEncR.tuples =
			        (struct row_enc_t*) alloc_aligned((tableEncR.num_tuples) * sizeof(struct row_enc_t));
			    for (uint32_t i = 0; i < tableEncR.num_tuples; i++) {
			        block buffer = SerialiseInt(tableR.tuples[i].key);
			        block ckey = AES::EncryptDET(key, buffer, 16, sizeof(type_key));
			        buffer = SerialiseInt(tableR.tuples[i].payload);
			        block cpayload = AES::EncryptDET(key, buffer, 16, sizeof(type_value));
			        std::copy(std::begin(ckey), std::end(ckey), std::begin(tableEncR.tuples[i].key));
			        std::copy(std::begin(cpayload), std::end(cpayload), std::begin(tableEncR.tuples[i].payload));
			        tableEncR.tuples[i].ts = tableR.tuples[i].ts;
			    }

			    tableEncS.num_tuples = tableS.num_tuples;
			    tableEncS.tuples =
                    (struct row_enc_t*) alloc_aligned((tableEncS.num_tuples) * sizeof(struct row_enc_t));
			    for (uint32_t i = 0; i < tableEncS.num_tuples; i++) {
			        block buffer = SerialiseInt(tableS.tuples[i].key);
			        block ckey = AES::EncryptDET(key, buffer, 16, sizeof(type_key));
			        buffer = SerialiseInt(tableS.tuples[i].payload);
			        block cpayload = AES::EncryptDET(key, buffer, 16, sizeof(type_value));
			        std::copy(std::begin(ckey), std::end(ckey), std::begin(tableEncS.tuples[i].key));
			        std::copy(std::begin(cpayload), std::end(cpayload), std::begin(tableEncS.tuples[i].payload));
			        tableEncS.tuples[i].ts = tableS.tuples[i].ts;
			    }

			    auto *shj = new SHJ();
			    shj->SHJ_init("SHJ-L0", joinconfig.windowRSize, joinconfig.windowSSize);
			    result_t *res = shj->DETjoin_st(&tableEncR, &tableEncS, &joinconfig);
			    logResults("SHJ-L0", res, &joinconfig, (tableEncR.num_tuples + tableEncS.num_tuples));
			    ret = SGX_SUCCESS;
                free(tableEncR.tuples);
                free(tableEncS.tuples);

			} else {
				logger(INFO, "Running NON-SGX join");
            	auto *shj = new SHJ();
            	shj->SHJ_init("SHJ", joinconfig.windowRSize, joinconfig.windowSSize);
            	result_t *res = shj->join_st(&tableR, &tableS, &joinconfig);
            	logResults("SHJ", res, &joinconfig, (tableR.num_tuples + tableS.num_tuples));
            	ret = SGX_SUCCESS;
			}
        } else {
            ret = ecall_join(global_eid,
                             &results,
                             &tableR,
                             &tableS,
                             params.algorithm_name,
                             &joinconfig);
        }

    }
    clock_gettime(CLOCK_MONOTONIC, &tw2);
    time = 1000.0*(double)tw2.tv_sec + 1e-6*(double)tw2.tv_nsec
                        - (1000.0*(double)tw1.tv_sec + 1e-6*(double)tw1.tv_nsec);
    logger(INFO, "Total join runtime: %.2fs", time/1000);
    logger(INFO, "throughput = %.4lf [M rec / s]",
           (double)(params.r_size + params.s_size)/(1000*time));
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
    }

    sgx_destroy_enclave(global_eid);
    delete_relation(&tableR);
    delete_relation(&tableS);

    return 0;
}
