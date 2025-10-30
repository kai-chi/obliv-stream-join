//
// Created by kaichi on 22.10.24.
//

#ifndef OSORT_H
#define OSORT_H

#include <cstdint>
#include "util.h"
#include <climits>
#include "threading.h"
/*
 * This bitonic sort is based on the sort from Ngai et al.
 */



static inline unsigned long long log2ll(unsigned long long x) {
#ifdef __GNUC__
    return sizeof(x) * CHAR_BIT - __builtin_clzll(x) - 1;
#else
    unsigned long long log = -1;
    while (x) {
        log++;
        x >>= 1;
    }
    return log;
#endif
}

template <typename T>
struct bitonic_sort_args {
    T *arr;
    size_t start;
    size_t length;
    size_t num_threads;
    bool descending;
};

template <typename T>
struct bitonic_merge_args {
    T *arr;
    size_t start;
    size_t length;
    bool crossover;
    size_t num_threads;
    bool descending;
};

template <typename T, bool (*comp_func)(T t1, T t2)>
static void swap(T *arr, size_t a, size_t b, size_t count, bool crossover, size_t num_threads, bool descending) {
    (void) (num_threads);
    if (crossover) {
        for (size_t i = 0; i < count; i++) {
            // bool cond = arr[a + i].key > arr[b + count - 1 - i].key;
            bool cond = comp_func(arr[b + count - 1 - i], arr[a + i]) ^ descending;
            o_memswap(&arr[a + i],
                    &arr[b + count - 1 - i], sizeof(*arr),
                    cond);
        }
    } else {
        for (size_t i = 0; i < count; i++) {
            // bool cond = arr[a + i].key > arr[b + i].key;
            bool cond = comp_func(arr[b + i], arr[a + i]) ^ descending;
            o_memswap(&arr[a + i], &arr[b + i],
                    sizeof(*arr), cond);
        }
    }
}

template <typename T, bool (*comp_func)(T t1, T t2)>
static void bitonic_merge(void *args_) {
    struct bitonic_merge_args<T> *args = (struct bitonic_merge_args<T> *) args_;
    T *arr = args->arr;
    size_t start = args->start;
    size_t length = args->length;
    bool crossover = args->crossover;
    size_t num_threads = args->num_threads;
    bool descending = args->descending;

    switch (length) {
        case 0:
        case 1:
            /* Do nothing. */
            break;

        case 2:
            swap<T, comp_func>(arr, start, start + 1, 1, false, 1, descending);
            break;

        default: {
            size_t left_length = length / 2;
            size_t right_length = length - left_length;
            size_t right_start = start + left_length;
            swap<T, comp_func>(arr, start, right_start, left_length, crossover,
                    num_threads, descending);

            /* Recursively merge. */
            struct bitonic_merge_args<T> left_args = {
                .arr = arr,
                .start = start,
                .length = left_length,
                .crossover = false,
                .descending = descending,
            };
            struct bitonic_merge_args<T> right_args = {
                .arr = arr,
                .start = right_start,
                .length = right_length,
                .crossover = false,
                .descending = descending,
            };
            if (num_threads > 1) {
                /* Merge both with separate threads. */
                size_t right_threads = num_threads / 2;
                left_args.num_threads = num_threads - right_threads;
                right_args.num_threads = right_threads;
                struct thread_work right_work = {
                    .type = THREAD_WORK_SINGLE,
                    .single = {
                        .func = bitonic_merge<T, comp_func>,
                        .arg = &right_args,
                    },
                };
                thread_work_push(&right_work);
                bitonic_merge<T, comp_func>(&left_args);
                thread_wait(&right_work);
            } else {
                /* Merge both in our own thread. */
                left_args.num_threads = 1;
                right_args.num_threads = 1;
                bitonic_merge<T, comp_func>(&left_args);
                bitonic_merge<T, comp_func>(&right_args);
            }
            break;
         }
    }
}

/*
 * This bitonic sort is based on the sort from Ngai et al.
 */
template <typename T, bool (*comp_func)(T t1, T t2)>
void bitonic_sort(void *args_) {
    struct bitonic_sort_args<T> *args = static_cast<struct bitonic_sort_args<T>*>(args_);
    T *arr = args->arr;
    size_t start = args->start;
    size_t length = args->length;
    size_t num_threads = args->num_threads;
    bool descending = args->descending;

    switch (length) {
        case 0:
        case 1:
            /* Do nothing. */
            break;

        case 2:
            swap<T, comp_func>(arr, start, start + 1, 1, false, 1, descending);
            break;

        default: {
            /* Recursively sort left and right halves. */
            size_t left_length = length / 2;
            size_t right_length = length - left_length;
            size_t right_start = start + left_length;
            struct bitonic_sort_args<T> left_args = {
                .arr = arr,
                .start = start,
                .length = left_length,
                .descending = descending,
            };
            struct bitonic_sort_args<T> right_args = {
                .arr = arr,
                .start = right_start,
                .length = right_length,
                .descending = descending,
            };
            if (num_threads > 1) {
                /* Sort both with separate threads. */
                size_t right_threads = num_threads / 2;
                left_args.num_threads = num_threads - right_threads;
                right_args.num_threads = right_threads;
                struct thread_work right_work = {
                    .type = THREAD_WORK_SINGLE,
                    .single = {
                        .func = bitonic_sort<T, comp_func>,
                        .arg = &right_args,
                    },
                };
                thread_work_push(&right_work);
                bitonic_sort<T, comp_func>(&left_args);
                thread_wait(&right_work);
            } else {
                /* Sort both in our own thread. */
                left_args.num_threads = 1;
                right_args.num_threads = 1;
                bitonic_sort<T, comp_func>(&left_args);
                bitonic_sort<T, comp_func>(&right_args);
            }

            /* Bitonic merge. */
            struct bitonic_merge_args<T> merge_args = {
                .arr = arr,
                .start = start,
                .length = length,
                .crossover = true,
                .num_threads = num_threads,
                .descending = descending,
            };
            bitonic_merge<T, comp_func>(&merge_args);
            break;
        }
    }
}

template <typename T, bool (*comp_func)(T t1, T t2)>
void bitonic_sort(T *arr, size_t length, size_t num_threads, bool descending) {

    if (1lu << log2ll(length) != length) {
        logger(ERROR, "Length must be a multiple of 2");
        goto exit;
    }

    /* Start work for this thread. */
    struct bitonic_sort_args<T> args;
    args = {
        .arr = arr,
        .start = 0,
        .length = length,
        .num_threads = num_threads,
        .descending = descending,
    };
    bitonic_sort<T, comp_func>(&args);

    exit:
        ;
}

// BITONIC SORT FOR NOT POWERS OF TWO TAKEN FROM KRAST
static inline int prev_pow_of_two(int x) {
    int y = 1;
    while (y < x) y <<= 1;
    return y >>= 1;
}

template <typename T, bool (*comp_func)(T e1, T e2)>
void obli_compare(T *arr, bool ascend, int i, int j) {
    T e1 = arr[i];
    T e2 = arr[j];
    if (!comp_func(e1, e2) == ascend) {
        arr[i] = e2;
        arr[j] = e1;
    }
    else {
        arr[i] = e1;
        arr[j] = e2;
    }
}

template <typename T, bool (*comp_func)(T e1, T e2)>
void obli_merge(T *arr, bool ascend, int lo, int hi) {
    if (hi <= lo + 1) return;

    int mid_len = prev_pow_of_two(hi - lo);

    for (int i = lo; i < hi - mid_len; i++)
        obli_compare<T, comp_func>(arr, ascend, i, i + mid_len);
    obli_merge<T, comp_func>(arr, ascend, lo, lo + mid_len);
    obli_merge<T, comp_func>(arr, ascend, lo + mid_len, hi);
}
template <typename T, bool (*comp_func)(T e1, T e2)>
void obli_sort(T *arr, size_t length, bool ascend = true, int lo = 0, int hi = -1) {
    if (hi == -1) hi = (int) length;

    int mid = lo + (hi - lo) / 2;

    if (mid == lo) return;

    obli_sort<T, comp_func>(arr, length, !ascend, lo, mid);
    obli_sort<T, comp_func>(arr, length, ascend, mid, hi);
    obli_merge<T, comp_func>(arr, ascend, lo, hi);
}
#endif //OSORT_H
