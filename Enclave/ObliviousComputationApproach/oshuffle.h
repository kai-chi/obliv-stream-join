//
// Created by kaichi on 20.10.24.
//

#ifndef OSHUFFLE_H
#define OSHUFFLE_H

#include "sgx_trts.h"
#include "parcompact.h"

#define MARK_COINS 2048
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

template <typename T>
struct shuffle_args {
    T *arr;
    bool *marked;
    size_t *marked_prefix_sums;
    size_t start;
    size_t length;
    size_t num_threads;
    int ret;
};

static inline int rand_bit(bool *bit) {
    uint8_t randval;
    sgx_status_t ret = sgx_read_rand((unsigned char *) &randval, 1);
    *bit = (randval >> 7) & 1;
    return ret;
}

template<typename T>
static void oshuffle(void *args_) {
    struct shuffle_args<T> *args = (struct shuffle_args<T>*) args_;
    T *arr = args->arr;
    bool *marked = args->marked;
    size_t *marked_prefix_sums = args->marked_prefix_sums;
    size_t start = args->start;
    size_t length = args->length;
    size_t num_threads = args->num_threads;
    int ret;

    struct parcompact_args<T> compact_args;
    struct shuffle_args<T> left_args, right_args;

    size_t start_idx = start;
    size_t end_idx = start + length;
    size_t total_left = end_idx - start_idx;
    size_t marked_so_far = 0;
    size_t num_to_mark = length / 2;

    if (length < 2) {
        ret = 0;
        goto exit;
    }

    if (length == 2) {
        bool cond;
        ret = rand_bit(&cond);
        if (ret) {
            goto exit;
        }
        o_memswap(&arr[start], &arr[start + 1],
                sizeof(*arr), cond);
        goto exit;
    }

    /* Mark exactly NUM_TO_MARK elems in our partition. */

    for (size_t i = 0; i < end_idx - start_idx; i += MARK_COINS) {
        uint32_t coins[MARK_COINS];
        size_t elems_to_mark = MIN(end_idx - start_idx - i, MARK_COINS);
        // ret = rand_read(coins, elems_to_mark * sizeof(*coins));
        ret = sgx_read_rand((unsigned char *) &coins, sizeof(uint32_t) * elems_to_mark);
        if (ret) {
            logger(ERROR,"Error getting random coins for marking");
            goto exit;
        }

        for (size_t j = 0; j < MIN(end_idx - start_idx - i, MARK_COINS); j++) {
            bool cur_marked =
                ((uint64_t) coins[j] * total_left) >> 32
                    >= num_to_mark - marked_so_far;
            marked_so_far += cur_marked;
            marked[i + j] = cur_marked;
            marked_prefix_sums[i + j] = marked_so_far;
            total_left--;
        }
    }

    /* Obliviously compact. */
    compact_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start,
        .length = length,
        .offset = 0,
        .num_threads = num_threads,
    };
    orocpar<T>(&compact_args);
    if (compact_args.ret) {
        ret = compact_args.ret;
        goto exit;
    }

    /* Recursively shuffle. */
    left_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start,
        .length = length / 2,
    };
    right_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start + length / 2,
        .length = length / 2,
    };
    if (num_threads > 1) {
        /* Do both in a threaded manner. */
        left_args.num_threads = num_threads / 2;
        right_args.num_threads = num_threads / 2;
        struct thread_work right_work = {
            .type = THREAD_WORK_SINGLE,
            .single = {
                .func = oshuffle<T>,
                .arg = &right_args,
            },
        };
        thread_work_push(&right_work);
        oshuffle<T>(&left_args);
        if (left_args.ret) {
            ret = left_args.ret;
            goto exit;
        }
        thread_wait(&right_work);
    } else {
        /* Do both in our own thread. */
        left_args.num_threads = 1;
        right_args.num_threads = 1;
        oshuffle<T>(&left_args);
        if (left_args.ret) {
            ret = left_args.ret;
            goto exit;
        }
        oshuffle<T>(&right_args);
        if (right_args.ret) {
            ret = right_args.ret;
            goto exit;
        }
    }

    ret = 0;

exit:
    args->ret = ret;
}

#endif //OSHUFFLE_H
