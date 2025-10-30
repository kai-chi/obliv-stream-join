#ifndef OUTIL_H
#define OUTIL_H

#include "data-types.h"
#include <tuple>


using namespace std;

typedef std::tuple<row_table_t, row_table_t> res_type;
typedef std::tuple<row_t, row_t> restype;

static inline void o_swapc(unsigned char * a, unsigned char * b,
bool cond) {
    unsigned char mask = ~((unsigned char) cond - 1);
    *a ^= *b;
    *b ^= *a & mask;
    *a ^= *b;
}

static inline void o_memswap(void * a_, void * b_, size_t n,
        bool cond) {
    unsigned char * a = (unsigned char *) a_;
    unsigned char * b = (unsigned char *) b_;

    for (size_t i = 0; i < n; i++) {
        o_swapc(&a[i], &b[i], cond);
    }
}

template <typename T, uint32_t (*key_func)(T t)>
bool func_comp(T t1, T t2)
{
    if (key_func(t1) == UINT32_MAX) return false;
    if (key_func(t2) == UINT32_MAX) return true;
    return key_func(t1) < key_func(t2);

}

inline bool table_comp(row_table_t e1, row_table_t e2) {
    if (e1.key == e2.key) {
        return e1.table_id < e2.table_id;
    }
    return e1.key < e2.key;
}

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