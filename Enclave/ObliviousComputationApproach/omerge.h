//
// Created by kaichi on 22.10.24.
//

#ifndef OMERGE_H
#define OMERGE_H

#include <cstdint>
#include "threading.h"

// Oblivious merge: takes two sorted runs sitting next to each other in the
// same array and merges them into one sorted run, via a parallel Batcher
// odd-even merge network, O(n log n). This is the cheap primitive OAppend
// (Algorithm 3 in the paper) leans on -- instead of re-sorting a whole
// window every batch, sort just the small incoming batch and merge it into
// the already-sorted window.
//
// The "_opt" variants take a `real_elements` cutoff and skip comparisons
// past it. That's safe to do without leaking anything because window/batch
// sizes are public in our threat model (see paper Sec. III-B) -- it just
// avoids wasted comparator work on the padding/dummy tail.

template <typename T>
struct merge_args {
    T *arr;
    size_t start;
    size_t length;
    size_t real_elements;
    size_t num_threads;
    uint32_t r;
    int ret;
    bool opt;
};

template <typename T>
void exchange(T *table, uint32_t i, uint32_t j) {
    T t = (table)[i];
    (table)[i] = (table)[j];
    (table)[j] = t;
}

// One comparator of the network: swap i/j if out of order. Note this
// branches on comp_func's result instead of using the branch-free o_memswap
// trick from outil.h -- the pair of indices touched is still fixed by the
// network structure, just the swap itself isn't constant-time.
template <typename T, bool (*comp_func)(T t1, T t2)>
void od_compare(T *table, uint32_t i, uint32_t j) {
    if (!comp_func(table[i], table[j])) {
        exchange<T>(table, i, j);
    }
}

template <typename T, bool (*comp_func)(T t1, T t2)>
void od_compare_opt(T *table, uint32_t i, uint32_t j, size_t real_elements) {
    if (i < real_elements && j < real_elements && !comp_func(table[i], table[j])) {
        exchange<T>(table, i, j);
    }
}


// Recursive worker for the parallel odd-even merge. `r` is the current
// comparator distance (doubles each recursion level, starts at whatever the
// caller passed in via the public odd_even_merge() below). `opt` turns on
// the real_elements cutoff described up top.
template <typename T, bool (*comp_func)(T t1, T t2)>
static void odd_even_merge_par(void *args_) {
    struct merge_args<T> *args = (struct merge_args<T>*) args_;
    T *arr = args->arr;
    size_t start = args->start;
    size_t length = args->length;
    size_t real_elements = args->real_elements;
    size_t num_threads = args->num_threads;
    bool opt = args->opt;
    uint32_t r = args->r;
    int ret;

    uint32_t m = r * 2;
    if (m < length) {
        // recursively merge
        struct merge_args<T> left_args;
        left_args = {
            .arr = arr,
            .start = start,
            .length = length,
            .real_elements = real_elements,
            .r = m,
            .opt = opt,
        };
        struct merge_args<T> right_args;
        right_args = {
            .arr = arr,
            .start = start + r,
            .length = length,
            .real_elements = real_elements,
            .r = m,
            .opt = opt
        };
        if (num_threads > 1) {
            /* Do both in a threaded manner. */
            right_args.num_threads = num_threads / 2;
            left_args.num_threads = num_threads - right_args.num_threads;
            struct thread_work right_work = {
                .type = THREAD_WORK_SINGLE,
                .single = {
                    .func = odd_even_merge_par<T, comp_func>,
                    .arg = &right_args,
                },
            };
            thread_work_push(&right_work);
            odd_even_merge_par<T, comp_func>(&left_args);
            if (left_args.ret) {
                ret = left_args.ret;
                goto exit;
            }
            thread_wait(&right_work);
        } else {
            /* Do both in our own thread. */
            left_args.num_threads = 1;
            right_args.num_threads = 1;
            odd_even_merge_par<T, comp_func>(&left_args);
            if (left_args.ret) {
                ret = left_args.ret;
                goto exit;
            }
            odd_even_merge_par<T, comp_func>(&right_args);
            if (right_args.ret) {
                ret = right_args.ret;
                goto exit;
            }
        }
        if (opt) {
            for (uint32_t i = (uint32_t)(start+r); i+r<start+length && i+r < real_elements; i+=m) {
                od_compare<T, comp_func>(arr, i, i+r);
            }
        } else {
            for (uint32_t i = (uint32_t)(start+r); i+r<start+length; i+=m) {
                od_compare<T, comp_func>(arr, i, i+r);
            }
        }

        ret = 0;
        goto exit;
    } else {
        if (opt) {
            if (start+r < real_elements) {
                od_compare<T, comp_func>(arr, (uint32_t)start, (uint32_t)start+r);
            }
        } else {
            od_compare<T, comp_func>(arr, (uint32_t)start, (uint32_t)start+r);
        }

        ret = 0;
        goto exit;
    }

exit:
    args->ret = ret;
}

static inline uint32_t prev_pow_of_two(uint32_t x) {
    uint32_t y = 1;
    while (y < x) y <<= 1;
    return y >>= 1;
}

template <typename T, bool (*comp_func)(T t1, T t2)>
void od_compare_opt(T *table, uint32_t i, uint32_t j, uint32_t real) {
    if (i < real && j < real && !comp_func(table[i], table[j])) {
        exchange<T>(table, i, j);
    }
}

// Single-threaded version of the same network, no real_elements cutoff.
template <typename T, bool (*comp_func)(T t1, T t2)>
void odd_even_merge(T *table, uint32_t lo, uint32_t n, uint32_t r) {
    uint32_t m = r * 2;
    if (m < n) {
        odd_even_merge<T, comp_func>(table, lo, n, m);
        odd_even_merge<T, comp_func>(table, lo+r, n, m);
        for (uint32_t i = lo+r; i+r<lo+n; i+=m) {
            od_compare<T, comp_func>(table, i, i+r);
        }
    } else {
        od_compare<T, comp_func>(table, lo, lo+r);
    }
}

template <typename T, bool (*comp_func)(T t1, T t2)>
void odd_even_merge_opt(T *table, uint32_t lo, uint32_t n, uint32_t r, uint32_t real) {
    uint32_t m = r * 2;
    if (m < n) {
        odd_even_merge_opt<T, comp_func>(table, lo, n, m, real);
        odd_even_merge_opt<T, comp_func>(table, lo+r, n, m, real);
        for (uint32_t i = lo+r; i+r<lo+n && i+r<real; i+=m) {
            od_compare<T, comp_func>(table, i, i+r);
        }
    } else {
        od_compare_opt<T, comp_func>(table, lo, lo+r, real);
    }
}

/** Wrapper for parallel optimized odd even merge */
template <typename T, bool (*comp_func)(T t1, T t2)>
void odd_even_merge(T *arr, size_t start, size_t length, size_t num_threads, size_t r, size_t real_elements) {
    struct merge_args<T> args = {
        .arr = arr,
        .start = start,
        .length = length,
        .real_elements = real_elements,
        .num_threads =  num_threads,
        .r = r,
        .ret = 0,
        .opt = true,
    };
    odd_even_merge_par<T, comp_func>(&args);
}

// template <typename T, bool (*comp_func)(T t1, T t2)>
// void oblivious_compare(T *table, bool ascend, uint32_t i, uint32_t j) {
//     T tmp1 = table[i];
//     T tmp2 = table[j];
//
//     bool condition = comp_func(tmp1, tmp2);
//     if (!condition == ascend) {
//         table[i] = tmp2;
//         table[j] = tmp1;
//     } else {
//         table[i] = tmp1;
//         table[j] = tmp2;
//     }
// }
//
// template <typename T, bool (*comp_func)(T t1, T t2)>
// void oblivious_merge(T *table, bool ascend, uint32_t lo, uint32_t hi) {
//     if (hi <= lo + 1) return;
//
//     uint32_t mid_len = prev_pow_of_two(hi - lo);
//
//     for (uint32_t i = lo; i < hi - mid_len; i++)
//         oblivious_compare<T, comp_func>(table, ascend, i, i + mid_len);
//     oblivious_merge<T, comp_func>(table, ascend, lo, lo + mid_len);
//     oblivious_merge<T, comp_func>(table, ascend, lo + mid_len, hi);
// }

#endif //OMERGE_H
