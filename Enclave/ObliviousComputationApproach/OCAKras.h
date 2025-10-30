//
// Created by kaichi on 09.11.24.
//

#ifndef OCAKRAS_H
#define OCAKRAS_H

#include <Commons/EnclaveTimers.h>

#include "data-types.h"
#include "osort.h"

#define EMPTY_ENTRY 0
#define REG_ENTRY 1

struct table_entry {
    int entry_type = EMPTY_ENTRY;

    int table_id;
    timespec_t ts;
    type_key key;
    type_value payload;

    // auxillary data
    bool fresh;
    bool contributes;
    int block_height;
    int block_width;
    int index;
    int t1index;
};

struct window_entry {
    struct table_entry * tuples;
    uint32_t num_tuples; // number of tuples currently in the window
    uint32_t window_size; // the size of window specified by user
    uint32_t capacity; // the max number of tuples the window can have at any point of time
};

class OCAKras {
    window_entry * window_r;
    window_entry * window_s;
    EnclaveTimers * e_timer;

    void append(window_entry * window, relation_t * batch, uint32_t counter, bool is_window_r, bool is_fresh);

    void invalidate_tuples(window_entry * window, int to_remove, uint32_t oldest_counter, bool * marked, size_t * marked_prefix_sums);

    uint32_t retire(window_entry * window, uint32_t oldest_counter);

    uint32_t write_block_sizes(window_entry * table);

    uint32_t join_windows(std::vector<std::tuple<table_entry, table_entry>> &results);

    template <int (*weight_func)(table_entry e)>
    void oblivious_expand(window_entry * table);

    template<int (*ind_func)(table_entry e)>
    void oblivious_distribute(window_entry * table_entry, int m);
public:
    OCAKras(uint32_t windowRSize, uint32_t windowSSize, uint32_t batchRSize, uint32_t batchSSize) {
        this->e_timer = new EnclaveTimers();
        window_r = (window_entry*) malloc(sizeof(window_entry));
        window_s = (window_entry*) malloc(sizeof(window_entry));

        window_r->tuples = (table_entry*) malloc((windowRSize + batchRSize) * sizeof(table_entry));
        window_r->num_tuples = 0;
        window_r->window_size = windowRSize;
        window_r->capacity = windowRSize + batchRSize;
        window_s->tuples = (table_entry*) malloc((windowSSize + batchSSize) * sizeof(table_entry));
        window_s->num_tuples = 0;
        window_s->window_size = windowSSize;
        window_s->capacity = windowSSize + batchSSize;
    }

    ~OCAKras() {
        delete e_timer;
        free(window_r->tuples);
        free(window_s->tuples);
        free(window_r);
        free(window_s);
    }

    void emit_results(const std::vector<std::tuple<table_entry, table_entry>> & vector);

    result_t * join(relation_t *rel_r, relation_t *rel_s, joinconfig_t *cfg);
};

static int entry_ind(table_entry e) {
    if (e.entry_type == EMPTY_ENTRY || !e.contributes) return -1;
    return e.index;
}

static bool entry_ind_func_comp(table_entry e1, table_entry e2) {
    if (entry_ind(e1) == -1) return false;
    if (entry_ind(e2) == -1) return true;
    return entry_ind(e1) < entry_ind(e2);
}

template<int (*ind_func)(table_entry e)>
void OCAKras::oblivious_distribute(window_entry * table, int m) {
    obli_sort<table_entry, entry_ind_func_comp>(table->tuples, table->num_tuples);

    if (m > 0)
        table->tuples = (table_entry*) realloc(table->tuples, m * sizeof(table_entry));
    if (table->tuples == nullptr) {
        logger(ERROR, "realloc failed");
        throw std::runtime_error("");
    }
    table->capacity = m;
    table->num_tuples = m;

    for (int j = prev_pow_of_two(m); j >= 1; j /= 2) {
        for (int i = m - j - 1; i >= 0; i --) {
            table_entry e = table->tuples[i];
            int dest_i = ind_func(e);
            assert(dest_i < m);
            table_entry e1 = table->tuples[i+j];
            if (dest_i >= i + j) {
                assert(ind_func(e1) == -1);
                table->tuples[i] = e1;
                table->tuples[i + j] = e;
            } else {
                table->tuples[i] = e;
                table->tuples[i + j] = e1;
            }
        }
    }
}

template<int(*weight_func)(table_entry e)>
void OCAKras::oblivious_expand(window_entry *table) {
    int csum = 0;
    for (size_t i = 0; i < table->num_tuples; i++) {
        table_entry *e = &table->tuples[i];
        int weight = weight_func(*e);
        if (weight == 0) {
            e->entry_type = EMPTY_ENTRY;
        } else
            e->index = csum;
        csum += weight;
    }

        oblivious_distribute<entry_ind>(table, csum);

    table_entry last{};
    int dupl_off = 0, block_off = 0;
    for (int i = 0; i < csum; i++) {
        table_entry *e = &table->tuples[i];
        if (e->entry_type != EMPTY_ENTRY) {
            if (i != 0 && e->key != last.key)
                block_off = 0;
            last = *e;
            dupl_off = 0;
        }
        else {
            assert(i != 0);
            e = &last;
        }
        e->index += dupl_off;
        e->t1index = int(block_off / e->block_height) +
                    (block_off % e->block_height) * e->block_width;
        dupl_off++;
        block_off++;
    }
}


#endif //OCAKRAS_H
