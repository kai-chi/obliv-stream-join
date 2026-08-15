/**
 * @file    npj_types.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Tue May 22 16:37:59 2012
 * @version $Id: npj_types.h 3017 2012-12-07 10:56:20Z bcagri $
 * 
 * @brief  Provides type definitions used by No Partitioning Join implementations.
 * 
 */
#ifndef NPO_TYPES_H
#define NPO_TYPES_H

#include "data-types.h" /* tuple_t */
#include <sgx_spinlock.h>

/** Number of tuples that each bucket can hold */
#ifndef BUCKET_SIZE
#define BUCKET_SIZE 2
#endif

/** Pre-allocation size for overflow buffers */
#ifndef OVERFLOW_BUF_SIZE
#define OVERFLOW_BUF_SIZE 1024
#endif

/**
 * @defgroup NPOTypes Type definitions used by NPO.
 * @{
 */

typedef struct bucket_t        bucket_t;
typedef struct hashtable_t     hashtable_t;
typedef struct bucket_buffer_t bucket_buffer_t;

typedef struct DETbucket_t        DETbucket_t;
typedef struct DEThashtable_t     DEThashtable_t;
typedef struct DETbucket_buffer_t DETbucket_buffer_t;

/** 
 * Normal hashtable buckets.
 *
 * if KEY_8B then key is 8B and sizeof(bucket_t) = 48B
 * else key is 16B and sizeof(bucket_t) = 32B
 */
struct bucket_t {
    sgx_spinlock_t    latch;
    uint32_t          count;
    struct row_t      tuples[BUCKET_SIZE];
    struct bucket_t * next;
};

struct DETbucket_t {
    sgx_spinlock_t       latch;
    uint32_t             count;
    struct row_enc_t     tuples[BUCKET_SIZE];
    struct DETbucket_t * next;
};


/** Hashtable structure for NPO. */
struct hashtable_t {
    bucket_t * buckets;
    int32_t    num_buckets;
    uint32_t   hash_mask;
    uint32_t   skip_bits;
};

struct DEThashtable_t {
    DETbucket_t * buckets;
    int32_t       num_buckets;
    uint32_t      hash_mask;
    uint32_t      skip_bits;
};

/** Pre-allocated bucket buffers are used for overflow-buckets. */
struct bucket_buffer_t {
    struct  bucket_buffer_t * next;
    uint32_t count;
    bucket_t buf[OVERFLOW_BUF_SIZE];
};

struct DETbucket_buffer_t {
    struct  DETbucket_buffer_t * next;
    uint32_t count;
    DETbucket_t buf[OVERFLOW_BUF_SIZE];
};

/** @} */

#endif /* NPO_TYPES_H */
