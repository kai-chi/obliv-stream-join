//
// Created by kaichi on 20.10.24.
//

#ifndef OCOMPACT_H
#define OCOMPACT_H

#include "data-types.h"
#include <cstddef>
#include <cstdint>
#include "util.h"

template <typename T>
struct compact_args {
    T *arr;
    int *M;
    size_t start;
    size_t length;
    size_t offset;
    size_t num_threads;
    int ret;
};

template <typename T>
static int swap_local_range(T *arr, size_t length, size_t a, size_t b,
        size_t count, size_t offset, size_t left_marked_count) {
    int ret;

    bool s =
        (offset % (length / 2) + left_marked_count >= length / 2)
            != (offset >= length / 2);

    for (size_t i = 0; i < count; i++) {
        // bool cond = s != (a + i >= (offset + left_marked_count) % (length / 2));
        bool cond = s != (i >= (offset + left_marked_count) % (length / 2));
        o_memswap(&arr[a + i], &arr[b + i],
                sizeof(*arr), cond);
    }

    ret = 0;

    return ret;
}

template <typename T>
static int or_swap_local_range(T *arr, size_t a, size_t b,
        int count, size_t left_marked_count) {
    int ret;

    for (int i = 0; i < count; i++) {
        // bool cond = s != (a + i >= (offset + left_marked_count) % (length / 2));
        bool cond = i >= (int)left_marked_count;
        o_memswap(&arr[a + i], &arr[b + i],
                sizeof(*arr), cond);
    }

    ret = 0;

    return ret;
}

template <typename T>
static int swap_range(T *arr, size_t length, size_t a_start, size_t b_start,
        size_t count, size_t offset, size_t left_marked_count,
        size_t num_threads) {
    (void) (num_threads);

    return swap_local_range(arr, length, a_start, b_start, count, offset,
            left_marked_count);
}

static size_t highest_power_of_two(size_t n) {
    // Special case for 0
    if (n == 0) return 0;

    // Initialize result as 1
    size_t n1 = 1;

    // Keep shifting n1 left until it becomes greater than n
    while (n1 <= n) {
        n1 <<= 1;  // Left shift (multiply by 2)
    }

    // Return the largest power of two less than or equal to n
    return n1 >> 1;  // Shift right to get the previous power of two
}

template <typename T>
static void oblivious_compaction(void *args_) {
    struct compact_args<T> *args = static_cast<struct compact_args<T>*>(args_);
    T *arr = args->arr;
    int *M = args->M;
    size_t start = args->start;
    size_t length = args->length;
    size_t offset = args->offset;
    size_t num_threads = args->num_threads;

    // size_t mid_idx;
    struct compact_args<T> left_args;
    struct compact_args<T> right_args;
    uint32_t m = 0;

    int ret;

    if (length < 2) {
        ret = 0;
        goto exit;
    }

    // make sure length is a power of two
    assert((length > 0) && (length & (length - 1)) == 0 );

    if (length == 2) {
        // bool cond = (key_func(arr[0]) & ~key_func(arr[1]) & 1) != (bool) offset;
        // bool cond = (key_func(arr[start]) & ~(key_func(arr[start+1])) & 1) != (bool) offset;
        bool cond = (~M[start] & M[start+1]) != (bool) offset;
        o_memswap(&arr[start], &arr[start+1],
                sizeof(*arr), cond);
        ret = 0;
        goto exit;
    }

    // mid_idx = start + length / 2 - 1;

    // size_t left_marked_count;
    // size_t mid_prefix_sum;
    // /* We are also the master, so set the local variable. */
    // mid_prefix_sum =
    //     arr[mid_idx - start].compact_marked_prefix_sum;
    //
    // /* Compute the number of marked elements. */
    // left_marked_count =
    //     mid_prefix_sum - arr[0].compact_marked_prefix_sum
    //         + !(arr[0].key & 1);

    for (uint32_t i = 0; i < length / 2; i++) {
        // m += is_res_real(args->arr[i]);;
        m += M[start+i];
    }

    /* Recursively compact. */
    left_args = {
        .arr = arr,
        .M = M,
        .start = start,
        .length = length / 2,
        .offset = offset % (length / 2),
    };
    right_args = {
        .arr = arr,
        .M = M,
        .start = start + length / 2,
        .length = length / 2,
        .offset = (offset + m) % (length / 2),
    };

    /* Do both in our own thread. */
    left_args.num_threads = 1;
    right_args.num_threads = 1;
    oblivious_compaction<T>(&left_args);
    if (left_args.ret) {
        ret = left_args.ret;
        goto exit;
    }
    oblivious_compaction<T>(&right_args);
    if (right_args.ret) {
        ret = right_args.ret;
        goto exit;
    }

    /* Swap. */
    ret =
        swap_range<T>(arr, length, start, start + length / 2, length / 2, offset,
                m, num_threads);
    if (ret) {
        logger(ERROR,
                "Error swapping range with start %lu and length %lu", start,
                start + length / 2);
        goto exit;
    }

exit:
    args->ret = ret;
}

template<typename T>
void or_compaction(void *args_) {
    struct compact_args<T> *args = static_cast<struct compact_args<T>*>(args_);
    T *arr = args->arr;
    int *M = args->M;
    size_t start = args->start;
    size_t length = args->length;

    struct compact_args<T> left_args;
    struct compact_args<T> right_args;
    uint32_t m = 0;
    size_t n1;
    size_t n2;

    int ret;

    if (length == 0) {
        ret = 0;
        goto exit;
    }

    n1 = highest_power_of_two(length);
    n2 = length - n1;
    for (uint32_t i = 0; i < n2; i++) {
        m += M[start+i];
    }
    /* Recursively compact. */
    left_args = {
        .arr = arr,
        .M = M,
        .start = start,
        .length = n2,
    };
    right_args = {
        .arr = arr,
        .M = M,
        .start = start + n2,
        .length = length - n2,
        .offset = (n1 - n2 + m) % n1,
    };

    /* Do both in our own thread. */
    left_args.num_threads = 1;
    right_args.num_threads = 1;
    or_compaction<T>(&left_args);
    if (left_args.ret) {
        ret = left_args.ret;
        goto exit;
    }
    oblivious_compaction<T>(&right_args);
    if (right_args.ret) {
        ret = right_args.ret;
        goto exit;
    }

    /* Swap. */
    ret =
        or_swap_local_range<T>(arr, start, start + n1, ((int)n2 - 1), m);
    if (ret) {
        logger(ERROR,
                "Error swapping range with start %lu and length %lu", start,
                start + length / 2);
        goto exit;
    }


    exit:
        args->ret = ret;
}

#endif //OCOMPACT_H
