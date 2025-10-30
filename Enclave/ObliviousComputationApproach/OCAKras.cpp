//
// Created by kaichi on 09.11.24.
//

#include "OCAKras.h"
#include <cstdlib>
#include <vector>

#include "osort.h"
#include "parcompact.h"

void debug_window_entry(window_entry* window) {
    logger(DBG, "key-tid-fresh-contributes-height-weight");
    for (size_t i = 0; i < window->num_tuples; i++) {
        table_entry e = window->tuples[i];
        logger(DBG, "%02d-%d-%d-%d-%d-%d", e.key, e.table_id, e.fresh, e.contributes, e.block_height, e.block_width);
    }
}

result_t * OCAKras::join(relation_t *rel_r, relation_t *rel_s, joinconfig_t *cfg) {

    auto *timer = (streamjoin_result_t *) malloc(sizeof(streamjoin_result_t));
    uint32_t processed_r = 0, processed_s = 0;
    uint32_t oldest_r = 0, oldest_s = 0; // store counters for both windows to retire tuples with oldest counters
    size_t matches = 0;
    relation_t *batch_r = (relation_t*) malloc(sizeof(relation_t));
    relation_t *batch_s = (relation_t*) malloc(sizeof(relation_t));
    uint32_t batch_size_r, batch_size_s;

    // pre-fill windows with input tuples to avoid the startup effect
    batch_r->tuples = rel_r->tuples;
    batch_r->num_tuples = window_r->window_size;
    append(window_r, batch_r, processed_r, true, false);
    processed_r += window_r->window_size;

    batch_s->tuples = rel_s->tuples;
    batch_s->num_tuples = window_s->window_size;
    append(window_s, batch_s, processed_s, false, false);
    processed_s += window_s->window_size;

    // debug_window_entry(window_r);
    // debug_window_entry(window_s);
    logger(INFO, "Windows warmed up");


    e_timer->startTimer(JOIN_TOTAL_TIME);

    while(processed_r < rel_r->num_tuples || processed_s < rel_s->num_tuples) {
        batch_size_r = (processed_r + cfg->batchRSize < rel_r->num_tuples) ?
             cfg->batchRSize : (rel_r->num_tuples - processed_r);
        batch_r->tuples = rel_r->tuples + processed_r;
        batch_r->num_tuples = batch_size_r;

        batch_size_s = (processed_s + cfg->batchSSize < rel_s->num_tuples) ?
                     cfg->batchSSize : (rel_s->num_tuples - processed_s);
        batch_s->tuples = rel_s->tuples + processed_s;
        batch_s->num_tuples = batch_size_s;

        // append batch tuples to windows
        append(window_r, batch_r, processed_r, true, true);
        append(window_s, batch_s, processed_s, false, true);

        // join both windows
        size_t local_matches = 0;
        std::vector<std::tuple<table_entry, table_entry>> results;
        local_matches = join_windows(results);
        // retire old tuples
        oldest_r = retire(window_r, oldest_r);
        oldest_s = retire(window_s, oldest_s);

        emit_results(results);

        processed_r += batch_size_r;
        processed_s += batch_size_s;
        matches += local_matches;
        results.clear();
    }

    timer->joinTotalTime = e_timer->stopTimer(TIMER::JOIN_TOTAL_TIME);

    free(batch_r);
    free(batch_s);
    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->sjr = timer;
    return joinresult;
}

void OCAKras::append(window_entry *window, relation_t *batch, uint32_t counter, bool is_window_r, bool is_fresh) {
    assert(window->capacity >= window->num_tuples + batch->num_tuples);
    size_t offset = window->num_tuples;
    for (size_t i = 0; i < batch->num_tuples; i++) {
        window->tuples[i + offset].ts.tv_sec = ((uint64_t) (counter + i) << 32) | (uint32_t) batch->tuples[i].ts.tv_sec;
        window->tuples[i + offset].key = batch->tuples[i].key;
        window->tuples[i + offset].payload = batch->tuples[i].payload;
        window->tuples[i + offset].entry_type = REG_ENTRY;
        window->tuples[i + offset].table_id = !is_window_r;
        window->tuples[i + offset].contributes = false;
        window->tuples[i + offset].fresh = is_fresh;
    }
    window->num_tuples += batch->num_tuples;
}

uint32_t OCAKras::retire(window_entry *window, uint32_t oldest_counter) {
    int to_remove = (int)(window->num_tuples - window->window_size);
    if (to_remove > 0) {
        bool *marked = (bool*) malloc(window->num_tuples * sizeof(*marked));
        size_t *marked_prefix_sums = (size_t*) malloc((window->num_tuples+1) * sizeof(*marked_prefix_sums));
        invalidate_tuples(window, to_remove, oldest_counter, marked, marked_prefix_sums);
        parcompact_args<table_entry> args = {
            .arr = window->tuples,
            .marked = marked,
            .marked_prefix_sums = marked_prefix_sums,
            .start = 0,
            .length = window->num_tuples,
            .offset = 0,
            .num_threads =  1
        };
        orcpar<table_entry>(&args);
        window->num_tuples -= to_remove;
        oldest_counter += to_remove;
        free(marked);
        free(marked_prefix_sums);
    }
    return oldest_counter;
}

uint32_t OCAKras::write_block_sizes(window_entry *table) {
    uint32_t output_size = 0;

    // scan forward for the first part of contributes
    type_key last_key = 0;
    bool new_in_key = false;
    for (size_t i = 0; i < table->num_tuples; i++) {
        table_entry *e = &table->tuples[i];
        bool same_attr = e->key == last_key;
        if (e->table_id == 0 && !same_attr) {
            e->contributes = e->fresh;
            new_in_key = e->fresh;
        } else if (e->table_id == 0 && same_attr) {
            e->contributes = e->fresh;
            new_in_key |= e->fresh;
        } else if (e->table_id == 1 && !same_attr) {
            e->contributes = e->fresh; // set to false?
            new_in_key = new_in_key;
        } else if (e->table_id == 1 && same_attr) {
            e->contributes = new_in_key | e->fresh;
            new_in_key = new_in_key;
        }
        last_key = e->key;
    }
    // scan backward for the second part of contributes
    last_key = 0;
    for (int i = (int) table->num_tuples - 1; i >= 0; i--) {
        table_entry *e = &table->tuples[i];
        bool same_attr = e->key == last_key;
        if (e->table_id == 0 && !same_attr) {
            e->contributes = e->contributes;
            new_in_key = new_in_key;
        } else if (e->table_id == 0 && same_attr) {
            e->contributes |= new_in_key;
            new_in_key = new_in_key;
        } else if (e->table_id == 1 && !same_attr) {
            e->contributes = e->contributes;
            new_in_key = e->fresh;
        } else if (e->table_id == 1 && same_attr) {
            e->contributes = e->contributes;
            new_in_key |= e->fresh;
        }
        last_key = e->key;
    }
    // do the same scans as Krastnikov but including the contributes flag
    // scan in forward direction to fill in height fields for table 1 entries
    int height = 0, width = 0;
    last_key = 0;
    for (size_t i = 0; i < table->num_tuples; i++) {
        table_entry*e = &table->tuples[i];
        bool same_attr = e->key == last_key;
        if (e->table_id == 0 && !same_attr) {
            height = e->contributes;
        }
        else if (e->table_id == 0 && same_attr) {
            height = e->contributes ? height + 1 : height;
        }
        else if (e->table_id == 1 && !same_attr) {
            height = 0;
            e->block_height = 0;
        }
        else if (e->table_id == 1 && same_attr) {
            e->block_height = height;
        }
        last_key = e->key;
    }
    // scan in backward direction to fill in width + height fields for table 0 entries
    height = 0; width = 0, last_key = 0;
    for (int i = (int) table->num_tuples - 1; i >= 0; i--) {
        table_entry *e = &table->tuples[i];
        bool same_attr = e->key == last_key;

        if (e->table_id == 0 && !same_attr) {
            width = 0;
            e->block_width = 0;
            height = 0;
            e->block_height = 0;
        }
        else if (e->table_id == 0 && same_attr) {
            e->block_width = width;
            e->block_height = height;
        }
        else if (e->table_id == 1 && !same_attr) {
            width = e->contributes;
            height = e->block_height;
        }
        else if (e->table_id == 1 && same_attr) {
            width = e->contributes ? width + 1 : width;
            height = e->block_height;
        }
        last_key = e->key;
    }
    // scan in forward direction to fill in width fields for table 1 entries
    height = 0; width = 0, last_key = INT_MIN;
    for (size_t i = 0; i < table->num_tuples; i++) {
        table_entry *e = &table->tuples[i];
        bool same_attr = e->key == last_key;

        if (e->table_id == 0 && !same_attr) {
            width = e->block_width;
            output_size += e->block_height * e->block_width;
        }
        else if (e->table_id == 0 && same_attr) {
            width = e->block_width;
        }
        else if (e->table_id == 1 && !same_attr) {
            width = 0;
            e->block_width = 0;
        }
        else if (e->table_id == 1 && same_attr) {
            e->block_width = width;
        }

        last_key = e->key;
    }
    return output_size;
}

void OCAKras::invalidate_tuples(window_entry *window, int to_remove, uint32_t oldest_counter, bool *marked,
                                size_t *marked_prefix_sums) {

    size_t marked_so_far = 0;
    marked_prefix_sums[0] = 0;
    for (uint32_t i = 0; i < window->num_tuples; i++) {
        uint32_t counter = (uint32_t) (window->tuples[i].ts.tv_sec >> 32);
        window->tuples[i].fresh = false;
        bool cur_marked;
        if (counter < oldest_counter + to_remove) {
            cur_marked = false;
        } else {
            cur_marked = true;
        }
        marked_so_far += cur_marked;
        marked[i] = cur_marked;
        marked_prefix_sums[i+1] = marked_so_far;
    }
}

static bool attr_comp(table_entry t1, table_entry t2) {
    return (t1.key == t2.key) ? (t1.table_id < t2.table_id) : (t1.key < t2.key);
}

static bool tid_comp(table_entry t1, table_entry t2) {
    if (t1.table_id == t2.table_id) {
        if (t1.key == t2.key)
            return t1.payload < t2.payload;
        else
            return t1.key < t2.key;
    }
    else
        return t1.table_id < t2.table_id;
}

static bool t1_comp(table_entry e1, table_entry e2) {
    return (e1.key == e2.key) ? (e1.t1index < e2.t1index) : (e1.key < e2.key);
}

static int entry_height(table_entry e) {
    if (e.entry_type == EMPTY_ENTRY || !e.contributes) return 0;
    return e.block_height;
}

static int entry_width(table_entry e) {
    if (e.entry_type == EMPTY_ENTRY || !e.contributes) return 0;
    return e.block_width;
}

uint32_t OCAKras::join_windows(std::vector<std::tuple<table_entry, table_entry>> &results) {

    uint32_t n1 = window_r->num_tuples;
    uint32_t n2 = window_s->num_tuples;
    uint32_t n = n1 + n2;

    window_entry * table = (window_entry*) malloc(sizeof(window_entry));
    table->tuples = (table_entry*) malloc (n * sizeof(table_entry));
    table->num_tuples = n;

    window_entry * t0 = (window_entry*) malloc(sizeof(window_entry));
    t0->tuples = (table_entry*) malloc (n1 * sizeof(table_entry));
    t0->num_tuples = n1;
    t0->capacity = n1;

    window_entry * t1 = (window_entry*) malloc(sizeof(window_entry));
    t1->tuples = (table_entry*) malloc (n2 * sizeof(table_entry));
    t1->num_tuples = n2;
    t1->capacity = n2;


    // append both windows to a single array
    std::copy(window_r->tuples, window_r->tuples + window_r->num_tuples, table->tuples);
    std::copy(window_s->tuples, window_s->tuples + window_s->num_tuples, table->tuples + window_r->num_tuples);

    // sort the array by (join_attr, table_id)
    obli_sort<table_entry, attr_comp>(table->tuples, table->num_tuples);

    // fill in block heights and widths after initial sort & get output_size
    uint32_t output_size = write_block_sizes(table);

    // resort lexicographically by (table_id, join_attr, data_attr)
    obli_sort<table_entry, tid_comp>(table->tuples, table->num_tuples);

    // split table to t0 and t1
    std::copy(table->tuples, table->tuples + n1, t0->tuples);
    std::copy(table->tuples + n1, table->tuples + n, t1->tuples);

    //obliviously expand both tables
    oblivious_expand<entry_width>(t0);
    oblivious_expand<entry_height>(t1);

    // assert(t0->num_tuples == output_size);
    // assert(t1->num_tuples == output_size);

    //align second table
    obli_sort<table_entry, t1_comp>(t1->tuples, t1->num_tuples);

    for (size_t i = 0; i < output_size; i++) {
        std::tuple<table_entry, table_entry> t = {t0->tuples[i], t1->tuples[i]};
        results.push_back(t);
    }

    free(table->tuples);
    free(table);
    free(t0->tuples);
    free(t0);
    free(t1->tuples);
    free(t1);
    return output_size;
}

void OCAKras::emit_results(const std::vector<std::tuple<table_entry, table_entry>> &vector) {
    (void) vector;
    logger(DBG, "Emit %d results", vector.size());
    for (size_t i = 0; i < vector.size(); i++) {
        std::tuple<table_entry, table_entry> t = vector.at(i);
        logger(DBG, "%d-%d-%d", get<0>(t).key, get<1>(t).key, get<1>(t).payload);
    }
}
