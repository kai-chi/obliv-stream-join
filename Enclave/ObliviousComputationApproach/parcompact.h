#ifndef PARCOMPACT_H
#define PARCOMPACT_H


#include "threading.h"
#include "Enclave.h"
#include "LoggerTypes.h"
#include "outil.h"

template <typename T>
struct parcompact_args {
    T *arr;
    bool *marked;
    size_t *marked_prefix_sums;
    size_t start;
    size_t length;
    size_t offset;
    size_t num_threads;
    int ret;
};

template <typename T>
static int swap_range_par(T *arr, size_t length, size_t a_start, size_t b_start,
        size_t count, size_t offset, size_t left_marked_count,
        size_t num_threads) {
    (void) num_threads;
    int ret;

    bool s =
        (offset % (length / 2) + left_marked_count >= length / 2)
            != (offset >= length / 2);

    for (size_t i = 0; i < count; i++) {
        bool cond = s != (i >= (offset + left_marked_count) % (length / 2));
        o_memswap(&arr[a_start + i], &arr[b_start + i],
                sizeof(*arr), cond);
    }

    ret = 0;

    return ret;
}

template <typename T>
static int orc_swap_range_par(T *arr, size_t a_start, size_t b_start,
        int count, size_t left_marked_count, size_t num_threads) {
    (void) num_threads;
    int ret;

    for (int i = 0; i < count; i++) {
        bool cond = i >= (int) left_marked_count;
        o_memswap(&arr[a_start + i], &arr[b_start + i],
                sizeof(*arr), cond);
    }

    ret = 0;

    return ret;
}

template <typename T>
void orocpar(void *args_) {
    struct parcompact_args<T> *args = (struct parcompact_args<T>*) args_;
    T *arr = args->arr;
    bool *marked = args->marked;
    size_t *marked_prefix_sums = args->marked_prefix_sums;
    size_t start = args->start;
    size_t length = args->length;
    size_t offset = args->offset;
    size_t num_threads = args->num_threads;
    int ret;

    if (length < 2) {
        ret = 0;
        goto exit;
    }

    if (length == 2) {
        bool cond = (!marked[start] & marked[start+1]) != (bool) offset;
        o_memswap(&arr[start], &arr[start + 1],
                sizeof(*arr), cond);
        ret = 0;
        goto exit;
    }

    if (length == 0) {
        ret = 0;
        goto exit;
    }

    /* Get number of elements in the left half that are marked. The elements
     * contains the prefix sums, so taking the final prefix sum minus the first
     * prefix sum plus 1 if first element is marked should be sufficient. */

    /* Use START + LENGTH / 2 as the tag (the midpoint index) since that's
     * guaranteed to be unique across iterations. */
    size_t mid_idx;
    mid_idx = start + length / 2 - 1;
    size_t left_marked_count;
    size_t mid_prefix_sum;
    /* We are also the master, so set the local variable. */
    mid_prefix_sum = marked_prefix_sums[mid_idx];
    /* Compute the number of marked elements. */
    left_marked_count =
            mid_prefix_sum - marked_prefix_sums[start]
                + marked[start];

    /* Recursively compact. */
    struct parcompact_args<T> left_args;
    left_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start,
        .length = length / 2,
        .offset = offset % (length / 2),
    };
    struct parcompact_args<T> right_args;
    right_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start + length / 2,
        .length = length / 2,
        .offset = (offset + left_marked_count) % (length / 2),
    };
    if (num_threads > 1) {
        /* Do both in a threaded manner. */
        left_args.num_threads = num_threads / 2;
        right_args.num_threads = num_threads / 2;
        struct thread_work right_work = {
            .type = THREAD_WORK_SINGLE,
            .single = {
                .func = orocpar<T>,
                .arg = &right_args,
            },
        };
        thread_work_push(&right_work);
        orocpar<T>(&left_args);
        if (left_args.ret) {
            ret = left_args.ret;
            goto exit;
        }
        thread_wait(&right_work);
    } else {
        /* Do both in our own thread. */
        left_args.num_threads = 1;
        right_args.num_threads = 1;
        orocpar<T>(&left_args);
        if (left_args.ret) {
            ret = left_args.ret;
            goto exit;
        }
        orocpar<T>(&right_args);
        if (right_args.ret) {
            ret = right_args.ret;
            goto exit;
        }
    }

    /* Swap. */
    ret =
        swap_range_par<T>(arr, length, start, start + length / 2, length / 2, offset,
                left_marked_count, num_threads);
    if (ret) {
        logger(ERROR,
                "Error swapping range with start %lu and length %lu", start,
                start + length / 2);
        goto exit;
    }

exit:
    args->ret = ret;
}

static size_t highest_power_of_2(size_t n) {
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
void orcpar(void *args_) {
    struct parcompact_args<T> *args = (struct parcompact_args<T>*) args_;
    T *arr = args->arr;
    bool *marked = args->marked;
    size_t *marked_prefix_sums = args->marked_prefix_sums;
    size_t start = args->start;
    size_t length = args->length;
    size_t num_threads = args->num_threads;
    int ret;

    if (length == 0) {
        ret = 0;
        goto exit;
    }

    int n1;
    int n2;
    n1 = (int) highest_power_of_2(length);
    n2 = (int) length - n1;

    /* Get number of elements in the left half that are marked. The elements
     * contains the prefix sums, so taking the final prefix sum minus the first
     * prefix sum plus 1 if first element is marked should be sufficient. */

    /* Use START + LENGTH / 2 as the tag (the midpoint index) since that's
     * guaranteed to be unique across iterations. */
    size_t mid_prefix_sum;
    /* We are also the master, so set the local variable. */
    mid_prefix_sum = marked_prefix_sums[n2];

    /* Recursively compact. */
    struct parcompact_args<T> left_args;
    left_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start,
        .length = (size_t) n2,
    };
    struct parcompact_args<T> right_args;
    right_args = {
        .arr = arr,
        .marked = marked,
        .marked_prefix_sums = marked_prefix_sums,
        .start = start + n2,
        .length = length - n2,
        .offset = (n1 - n2 + mid_prefix_sum) % n1,
    };
    if (num_threads > 1) {
        /* Do both in a threaded manner. */
        left_args.num_threads = num_threads / 2;
        right_args.num_threads = num_threads / 2;
        struct thread_work right_work = {
            .type = THREAD_WORK_SINGLE,
            .single = {
                .func = orocpar<T>,
                .arg = &right_args,
            },
        };
        thread_work_push(&right_work);
        orcpar<T>(&left_args);
        if (left_args.ret) {
            ret = left_args.ret;
            goto exit;
        }
        thread_wait(&right_work);
    } else {
        /* Do both in our own thread. */
        left_args.num_threads = 1;
        right_args.num_threads = 1;
        orcpar<T>(&left_args);
        if (left_args.ret) {
            ret = left_args.ret;
            goto exit;
        }
        orocpar<T>(&right_args);
        if (right_args.ret) {
            ret = right_args.ret;
            goto exit;
        }
    }

    /* Swap. */
    ret =
        orc_swap_range_par<T>(arr, start, start + n1, (int) (n2),
                mid_prefix_sum, num_threads);
    if (ret) {
        logger(ERROR,
                "Error swapping range with start %lu and length %lu", start,
                start + length / 2);
        goto exit;
    }

exit:
    args->ret = ret;
}

#endif //PARCOMPACT_H
