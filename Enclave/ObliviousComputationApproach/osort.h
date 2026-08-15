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

// Oblivious Sort primitive (paper Sec. II-A "Oblivious sort") via comparator
// networks: every swap decision is executed through the branch-free
// o_memswap (outil.h), so the sequence and count of memory accesses is fixed
// by the input length alone, independent of data or outcome.
//
// Two independent implementations live in this file:
//  - bitonic_sort/bitonic_merge: a multi-threaded bitonic sorting network
//    (Ngai et al.), O(N log^2 N) comparisons, requires length to be a power
//    of two, and can fan sub-sorts out across enclave threads.
//  - obli_sort/obli_merge/obli_compare: a simpler single-threaded recursive
//    bitonic sort adapted from Krastnikov et al.'s static oblivious join
//    (see "BITONIC SORT FOR NOT POWERS OF TWO" below) that also tolerates
//    non-power-of-two lengths.

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

// Branch-free compare-and-conditionally-swap over `count` contiguous pairs.
// `crossover` selects the pairing: true gives the bitonic "butterfly" pairing
// (arr[a+i] with arr[b+count-1-i], used when building a bitonic sequence from
// two sorted halves), false gives the straight pairing (arr[a+i] with
// arr[b+i], used inside bitonic_merge). Every pair is compared and the
// o_memswap always touches both addresses, so access pattern doesn't depend
// on the comparison outcome.
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

// Merge stage of the bitonic network: assumes both [start, start+length/2)
// and [start+length/2, start+length) are already bitonic and merges them in
// place, recursing until sub-ranges of size <= 2 are reached. This is the
// single-threaded network; the multi-threaded entry point below runs the very
// same comparators in the same order, just spread over threads, so both
// produce bit-identical output.
template <typename T, bool (*comp_func)(T t1, T t2)>
static void bitonic_merge_seq(T *arr, size_t start, size_t length, bool crossover,
        bool descending) {
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
                     1, descending);
            bitonic_merge_seq<T, comp_func>(arr, start, left_length, false, descending);
            bitonic_merge_seq<T, comp_func>(arr, right_start, right_length, false, descending);
            break;
        }
    }
}

// --- multi-threaded bitonic merge -------------------------------------------
//
// A bitonic merge over a power-of-two range is log2(length) *levels* of
// comparators. Level l compares every element with the one `length/2^(l+1)`
// slots away, and all length/2 comparators of a level are independent of each
// other. The recursion above expresses those levels as a tree, which is why
// the obvious way to thread it -- hand one subtree to another thread, as this
// file used to do -- scales badly: the top level's length/2 comparators all
// run in the thread that owns the root, level 1's in two threads, and so on.
// That leaves a serial critical path of length/2 + length/4 + ... ~ length
// comparators out of a total of (length/2)*log2(length), i.e. a hard speedup
// ceiling of ~2*log2(length)... but only if every level below is perfectly
// parallel, so in practice it plateaus much earlier (measured: ~3.7x at 8+
// threads for a 2^23-tuple window, and getting worse past that).
//
// So instead we go level-synchronous: split each level's comparators evenly
// across all threads, then join. After log2(nblocks) levels the range has
// decayed into `nblocks` independent bitonic blocks, so from that point on
// every thread just runs the sequential network on its own contiguous blocks
// with no synchronisation at all. Total synchronisation per merge is
// log2(nblocks)+1 joins instead of a serial O(length) prefix.
//
// This changes nothing about obliviousness: which comparators exist, and the
// addresses each one touches, are still fixed by `length` alone (public in our
// threat model), and each comparator is still the branch-free o_memswap.

/* Don't bother threading a level with fewer than this many comparators per
 * thread -- the dispatch would cost more than the work. */
#ifndef BITONIC_MIN_PAIRS_PER_THREAD
#define BITONIC_MIN_PAIRS_PER_THREAD 512
#endif
/* Smallest contiguous block we hand to a thread in the sequential tail. */
#ifndef BITONIC_MIN_BLOCK
#define BITONIC_MIN_BLOCK 1024
#endif
/* Sub-sorts shorter than this stay in the calling thread. */
#ifndef BITONIC_MIN_PARALLEL_SORT
#define BITONIC_MIN_PARALLEL_SORT 512
#endif

static inline size_t pow2_ceil_sz(size_t x) {
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static inline bool is_pow2_sz(size_t x) {
    return x && !(x & (x - 1));
}

/* One thread's slice of one comparator level. Pair index p in
 * [pair_begin, pair_end) identifies a comparator: with `stride` = s, pair p
 * lives in block p/s at offset p%s, i.e. it compares element
 * (p/s)*2s + p%s with the one s slots later. `crossover` (only ever set on
 * the first level) instead pairs the range end-to-end, which is how two
 * ascending runs get turned into a bitonic one. */
template <typename T>
struct bitonic_level_args {
    T *arr;
    size_t start;
    size_t length;
    size_t stride;
    size_t stride_log;
    size_t pair_begin;
    size_t pair_end;
    bool crossover;
    bool descending;
};

template <typename T, bool (*comp_func)(T t1, T t2)>
static void bitonic_level(void *args_) {
    struct bitonic_level_args<T> *args = (struct bitonic_level_args<T> *) args_;
    T *arr = args->arr;
    bool descending = args->descending;
    size_t pair_end = args->pair_end;

    if (args->crossover) {
        size_t lo = args->start;
        size_t hi = args->start + args->length - 1;
        for (size_t p = args->pair_begin; p < pair_end; p++) {
            bool cond = comp_func(arr[hi - p], arr[lo + p]) ^ descending;
            o_memswap(&arr[lo + p], &arr[hi - p], sizeof(*arr), cond);
        }
    } else {
        size_t s = args->stride;
        size_t s_log = args->stride_log;
        size_t off_mask = s - 1;
        for (size_t p = args->pair_begin; p < pair_end; p++) {
            size_t i = args->start + ((p >> s_log) << (s_log + 1)) + (p & off_mask);
            bool cond = comp_func(arr[i + s], arr[i]) ^ descending;
            o_memswap(&arr[i], &arr[i + s], sizeof(*arr), cond);
        }
    }
}

/* One thread's share of the sequential tail: a contiguous run of independent
 * bitonic blocks, each finished with the single-threaded network. */
template <typename T>
struct bitonic_tail_args {
    T *arr;
    size_t start;
    size_t block_length;
    size_t num_blocks;
    bool descending;
};

template <typename T, bool (*comp_func)(T t1, T t2)>
static void bitonic_tail(void *args_) {
    struct bitonic_tail_args<T> *args = (struct bitonic_tail_args<T> *) args_;
    for (size_t b = 0; b < args->num_blocks; b++) {
        bitonic_merge_seq<T, comp_func>(args->arr, args->start + b * args->block_length,
                args->block_length, false, args->descending);
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

    /* The level-synchronous decomposition assumes a power-of-two range (all
     * callers pad up to one); anything else falls back to the network above. */
    if (num_threads <= 1 || length < 4 || !is_pow2_sz(length)) {
        bitonic_merge_seq<T, comp_func>(arr, start, length, crossover, descending);
        return;
    }

    size_t pairs = length / 2;
    size_t threads = num_threads;
    while (threads > 1 && pairs / threads < BITONIC_MIN_PAIRS_PER_THREAD) {
        threads--;
    }
    if (threads <= 1) {
        bitonic_merge_seq<T, comp_func>(arr, start, length, crossover, descending);
        return;
    }

    /* Peel levels until the range has decayed into at least one block per
     * thread. Blocks all cost the same, so a power-of-two thread count gets an
     * exact split; otherwise oversubscribe 4x to even out the remainder. */
    size_t num_blocks = pow2_ceil_sz(threads);
    if (!is_pow2_sz(threads)) {
        size_t oversubscribed = num_blocks * 4;
        while (num_blocks < oversubscribed && length / (num_blocks * 2) >= BITONIC_MIN_BLOCK) {
            num_blocks <<= 1;
        }
    }
    if (num_blocks > length / 2) num_blocks = length / 2;

    struct bitonic_level_args<T> largs[threads];
    struct bitonic_tail_args<T> targs[threads];
    struct thread_work work[threads];

    size_t stride = length / 2;
    size_t stride_log = log2ll(length) - 1;
    for (size_t blocks = 1; blocks < num_blocks; blocks <<= 1) {
        bool cross = (blocks == 1) && crossover;
        for (size_t t = 0; t < threads; t++) {
            largs[t] = {
                .arr = arr,
                .start = start,
                .length = length,
                .stride = stride,
                .stride_log = stride_log,
                .pair_begin = pairs * t / threads,
                .pair_end = pairs * (t + 1) / threads,
                .crossover = cross,
                .descending = descending,
            };
        }
        for (size_t t = 1; t < threads; t++) {
            work[t] = {
                .type = THREAD_WORK_SINGLE,
                .single = {
                    .func = bitonic_level<T, comp_func>,
                    .arg = &largs[t],
                },
            };
            thread_work_push(&work[t]);
        }
        bitonic_level<T, comp_func>(&largs[0]);
        for (size_t t = 1; t < threads; t++) {
            thread_wait(&work[t]);
        }
        stride >>= 1;
        stride_log--;
    }

    /* What's left is `num_blocks` independent bitonic blocks. */
    size_t block_length = length / num_blocks;
    for (size_t t = 0; t < threads; t++) {
        size_t first = num_blocks * t / threads;
        size_t last = num_blocks * (t + 1) / threads;
        targs[t] = {
            .arr = arr,
            .start = start + first * block_length,
            .block_length = block_length,
            .num_blocks = last - first,
            .descending = descending,
        };
    }
    for (size_t t = 1; t < threads; t++) {
        work[t] = {
            .type = THREAD_WORK_SINGLE,
            .single = {
                .func = bitonic_tail<T, comp_func>,
                .arg = &targs[t],
            },
        };
        thread_work_push(&work[t]);
    }
    bitonic_tail<T, comp_func>(&targs[0]);
    for (size_t t = 1; t < threads; t++) {
        thread_wait(&work[t]);
    }
}

/*
 * This bitonic sort is based on the sort from Ngai et al.
 * Recursive worker: sorts each half (in parallel if num_threads > 1), then
 * bitonic_merge's them into one sorted run.
 */
template <typename T, bool (*comp_func)(T t1, T t2)>
void bitonic_sort(void *args_) {
    struct bitonic_sort_args<T> *args = static_cast<struct bitonic_sort_args<T>*>(args_);
    T *arr = args->arr;
    size_t start = args->start;
    size_t length = args->length;
    size_t num_threads = args->num_threads;
    bool descending = args->descending;

    /* Sub-sorts this small are cheaper to just do here than to hand to
     * another thread. */
    if (length <= BITONIC_MIN_PARALLEL_SORT) num_threads = 1;

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

// Public entry point: obliviously sorts arr[0..length) ascending/descending
// (per `descending`) using `num_threads` enclave threads. `length` MUST be a
// power of two (checked below; logs an error and returns unsorted otherwise
// -- callers are responsible for padding). `comp_func` must be a pure,
// data-independent comparator: the sort's obliviousness only holds if
// comparing/swapping never itself branches on secret data outside the
// o_memswap mechanism.
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

// Single comparator step of the Krastnikov-style bitonic network: compares
// arr[i]/arr[j] and writes back in the order required by `ascend`. Note this
// always performs the same two reads/two writes regardless of the outcome
// (only the *values* placed at i/j depend on the comparison), so the address
// pattern is fixed while the branch itself is data-dependent.
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

// Merges arr[lo..hi) assuming it is bitonic, tolerating non-power-of-two
// ranges by splitting at the previous power of two instead of the midpoint
// (standard trick for making bitonic merge work on arbitrary lengths).
template <typename T, bool (*comp_func)(T e1, T e2)>
void obli_merge(T *arr, bool ascend, int lo, int hi) {
    if (hi <= lo + 1) return;

    int mid_len = prev_pow_of_two(hi - lo);

    for (int i = lo; i < hi - mid_len; i++)
        obli_compare<T, comp_func>(arr, ascend, i, i + mid_len);
    obli_merge<T, comp_func>(arr, ascend, lo, lo + mid_len);
    obli_merge<T, comp_func>(arr, ascend, lo + mid_len, hi);
}

// Single-threaded oblivious sort of arr[lo..hi) (defaults to the whole
// array), safe for any length (not just powers of two). Recursively sorts
// the two halves in opposite directions to form a bitonic sequence, then
// obli_merge's them -- the classic bitonic-sort-network recursion, minus the
// power-of-two restriction that bitonic_sort() above imposes.
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
