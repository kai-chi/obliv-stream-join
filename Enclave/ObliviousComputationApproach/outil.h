#ifndef OUTIL_H
#define OUTIL_H

#include "data-types.h"
#include <tuple>

// Small shared toolbox for the oblivious primitives in this folder: a
// branch-free byte swap, branch-free select, and the comparators the
// sort/merge templates get instantiated with.

using namespace std;

typedef std::tuple<row_table_t, row_table_t> res_type;
typedef std::tuple<row_t, row_t> restype;

// XOR-swap trick, masked by `cond`. When cond is false the mask is all
// zeros and nothing changes; when true it's all ones and a/b swap. No
// branch, so this is the one actually doing the "oblivious" part of
// oblivious sort/compaction/shuffle -- same instructions and same two
// addresses touched either way.
static inline void o_swapc(unsigned char * a, unsigned char * b,
bool cond) {
    unsigned char mask = ~((unsigned char) cond - 1);
    *a ^= *b;
    *b ^= *a & mask;
    *a ^= *b;
}

// Same trick over an n-byte range, byte at a time.
static inline void o_memswap(void * a_, void * b_, size_t n,
        bool cond) {
    unsigned char * a = (unsigned char *) a_;
    unsigned char * b = (unsigned char *) b_;

    for (size_t i = 0; i < n; i++) {
        o_swapc(&a[i], &b[i], cond);
    }
}

// Generic comparator for the sort/merge templates: orders by key_func(t),
// treating UINT32_MAX as the dummy/"infinity" key so padding tuples always
// sort last.
template <typename T, uint32_t (*key_func)(T t)>
bool func_comp(T t1, T t2)
{
    if (key_func(t1) == UINT32_MAX) return false;
    if (key_func(t2) == UINT32_MAX) return true;
    return key_func(t1) < key_func(t2);

}

// Comparator for row_table_t: order by key, and for equal keys put R before
// S (table_id 0 < 1) so a scan sees R and S tuples with the same key in a
// deterministic, predictable order.
inline bool table_comp(row_table_t e1, row_table_t e2) {
    if (e1.key == e2.key) {
        return e1.table_id < e2.table_id;
    }
    return e1.key < e2.key;
}

// Branch-free "pick a if choice else b" -- used e.g. by OSel to pick the
// right candidate result without a data-dependent branch.
static uint32_t conditional_select(uint32_t a, uint32_t b, int choice) {
    uint32_t one = 1;
    return (~((uint32_t) choice - one) & a) | ((uint32_t) ((uint32_t) choice - one) & b);
}

static uint64_t conditional_select(uint64_t a, uint64_t b, int choice) {
    uint64_t one = 1;
    return (~((uint64_t) choice - one) & a) | ((uint64_t) ((uint64_t) choice - one) & b);
}

// choice = 1 -> return a, choice = 0 -> return b
static row_table_t conditional_select(const row_table_t & a, const row_table_t & b, bool choice) {
    row_table_t res;
    res.ts.tv_sec = conditional_select(a.ts.tv_sec, b.ts.tv_sec, choice);
    res.ts.tv_nsec = conditional_select(a.ts.tv_nsec, b.ts.tv_nsec, choice);
    res.key = conditional_select(a.key, b.key, choice);
    res.payload = conditional_select(a.payload, b.payload, choice);
    res.table_id = (uint8_t)conditional_select((uint32_t)a.table_id, (uint32_t)b.table_id, choice);
    return res;
}

// Round up to the next power of two -- most of the sort/compact/shuffle
// primitives here need power-of-two-sized input, so callers pad up first.
static uint32_t next_power_of_two(uint32_t n) {
    // If n is 0, return 1 as the next power of 2
    if (n == 0) return 1;

    // Subtract 1 to handle the case when n is already a power of 2
    n--;

    // Set all bits after the most significant 1 to 1
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;

    // Add 1 to get the next power of 2
    return n + 1;
}

#endif