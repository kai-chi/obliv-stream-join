#ifndef BUCKETCHAININGHASHTABLE_HPP
#define BUCKETCHAININGHASHTABLE_HPP

#include "HashTableInterface.hpp"
#include "../../Include/npj_types.h"
#include "../../Include/LoggerTypes.h"
#include "../../Include/data-types.h"
#include <stdint.h>
#include <stdlib.h>
#include <cstring>
#include <vector>
#include <queue>
#include "../Lib/Logger.h"
#include <tuple>

using namespace std;

#ifndef NEXT_POW_2
/**
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif

#ifndef HASH
#define HASH(X, MASK, SKIP) (((X) & MASK) >> SKIP)
#endif

#if defined(__cplusplus)
extern "C" {
#endif

class BucketChainingHashTable : public HashTableInterface
{
private:
    hashtable_t * ht;

    typedef tuple<uint64_t, uint32_t> pi;
    priority_queue<pi, vector<pi>, greater<pi>> pq;

    void allocate_hashtable(hashtable_t ** ppht, uint32_t nbuckets)
    {
        ht              = (hashtable_t*)malloc(sizeof(hashtable_t));
        ht->num_buckets = nbuckets;
        NEXT_POW_2((ht->num_buckets));

        size_t CACHE_LINE_SIZE = 64;
        /* allocate hashtable buckets cache line aligned */
        ht->buckets = (bucket_t*) aligned_alloc(CACHE_LINE_SIZE, ht->num_buckets * sizeof(bucket_t));
        if (!ht || !ht->buckets) {
            logger(ERROR, "Memory allocation for the hashtable failed!");
            exit(EXIT_FAILURE);
        }
        memset(ht->buckets, 0, ht->num_buckets * sizeof(bucket_t));
        ht->skip_bits = 0; /* the default for modulo hash */
        ht->hash_mask = (ht->num_buckets - 1) << ht->skip_bits;
        *ppht = ht;
    }

    void emit_result(tuple_t r, tuple_t s) override {
        (void) (r);
        (void) (s);
   }

public:
    BucketChainingHashTable(int _isLeft) : HashTableInterface(_isLeft) {}

    ~BucketChainingHashTable()
    {
        free(ht->buckets);
        free(ht);
    }
    void initialize(uint32_t numTuples) override
    {
        uint32_t nbuckets = (numTuples / BUCKET_SIZE);
        allocate_hashtable(&ht, nbuckets);
        this->capacity = numTuples;
    }

    uint32_t probe(tuple_t t) override {
        uint32_t matches = 0;
        type_key idx = HASH(t.key, ht->hash_mask, ht->skip_bits);
        bucket_t * b = ht->buckets+idx;

        do {
            for(uint32_t j = 0; j < b->count; j++) {
                if(t.key == b->tuples[j].key){
                    matches ++;
                    emit_result(t, b->tuples[j]);
                }
            }

            b = b->next;/* follow overflow pointer */
        } while(b);

        return matches;
    }

    uint32_t probeNodes(relation_t *rel)
    {
        uint32_t i, j, matches = 0;
        const uint32_t hashmask = ht->hash_mask;
        const uint32_t skipbits = ht->skip_bits;

        for (i = 0; i < rel->num_tuples; i++)
        {
            type_key idx = HASH(rel->tuples[i].key, hashmask, skipbits);
            bucket_t * b = ht->buckets+idx;

            do {
                for(j = 0; j < b->count; j++) {
                    if(rel->tuples[i].key == b->tuples[j].key){
                        matches ++;
                        /* we don't materialize the results. */
                    }
                }

                b = b->next;/* follow overflow pointer */
            } while(b);
        }
        return matches;
    }

    // Function to convert timespec to nanoseconds
    uint64_t timespec_to_ns(struct timespec_t ts) {
        return (uint64_t)ts.tv_sec * 1000000000L + (uint64_t)ts.tv_nsec;
    }

    void build(tuple_t t) override {
        struct row_t * dest;
        bucket_t * curr, * nxt;
        int64_t idx = HASH(t.key, ht->hash_mask, ht->skip_bits);

        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = ht->buckets + idx;
        nxt  = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                bucket_t * b;
                b = (bucket_t*) calloc(1, sizeof(bucket_t));
                curr->next = b;
                b->next = nxt;
                b->count = 1;
                dest = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }
        *dest = t;

        pq.push(make_tuple(timespec_to_ns(t.ts), t.key));
    }

    void buildNodes(relation_t *rel) override
    {
        {
            uint64_t i;
            const uint32_t hashMask = ht->hash_mask;
            const uint32_t skipBits = ht->skip_bits;

            for(i=0; i < rel->num_tuples; i++){
                struct row_t * dest;
                bucket_t * curr, * nxt;
                int64_t idx = HASH(rel->tuples[i].key, hashMask, skipBits);

                /* copy the tuple to appropriate hash bucket */
                /* if full, follow nxt pointer to find correct place */
                curr = ht->buckets + idx;
                nxt  = curr->next;

                if(curr->count == BUCKET_SIZE) {
                    if(!nxt || nxt->count == BUCKET_SIZE) {
                        bucket_t * b;
                        b = (bucket_t*) calloc(1, sizeof(bucket_t));
                        curr->next = b;
                        b->next = nxt;
                        b->count = 1;
                        dest = b->tuples;
                    }
                    else {
                        dest = nxt->tuples + nxt->count;
                        nxt->count ++;
                    }
                }
                else {
                    dest = curr->tuples + curr->count;
                    curr->count ++;
                }
                *dest = rel->tuples[i];

                pq.push(make_tuple(timespec_to_ns(rel->tuples[i].ts), rel->tuples[i].key));
            }
        }
    }

    static bool isTsEqual(const timespec_t *ts1, const timespec_t *ts2) {
        return (ts1->tv_sec == ts2->tv_sec && ts1->tv_nsec == ts2->tv_nsec);
    }

    void removeNode(tuple_t t) override {
        const uint32_t hashMask = ht->hash_mask;
        const uint32_t skipBits = ht->skip_bits;

        int64_t idx = HASH(t.key, hashMask, skipBits);
        bucket_t * curr = ht->buckets+idx;
        bucket_t * prev = curr;

        do {
            for (uint64_t j = 0; j < curr->count; j++) {
                if (isTsEqual(&t.ts, &curr->tuples[j].ts) && t.key == curr->tuples[j].key) {
                    // if it's the only tuple remove the bucket
                    if (curr->count == 1) {
                        curr->count--;
                        if (prev != curr) {
                            prev->next = curr->next;
                            free(curr);
                            return;
                        }
                    }
                    // if there's more tuples in the bucket
                    else {
                        // shift all the tuples up
                        for (uint64_t k = j; k < BUCKET_SIZE - 1; k++) {
                            curr->tuples[k].ts = curr->tuples[k+1].ts;
                            curr->tuples[k].key = curr->tuples[k+1].key;
                            curr->tuples[k].payload = curr->tuples[k+1].payload;
                        }
                        curr->count--;
                        return;
                    }
                }
            }
            prev = curr;
            curr = curr->next;
        } while (curr);
    }


    std::vector<type_key> deleteOldest(uint32_t num) override
    {
        vector<type_key> res;
        for (uint32_t i = 0; i < num; i++) {
            pi t = pq.top();
            timespec_t ts = (timespec_t) { .tv_sec  = get<0>(t) / 1000000000L,
                    .tv_nsec = get<0>(t) % 1000000000L };
            tuple_t tuple = {.ts = ts, .key = get<1>(t), .payload = 0};
            removeNode(tuple);
            pq.pop();
            res.push_back(tuple.key);
        }
        return res;
    }


    std::string vectorToString(const std::vector<type_key>& vec) {
        std::string result;
        for (auto it = vec.begin(); it != vec.end(); ++it) {
            result += std::to_string(*it) + " ";
        }
        // Remove the trailing space
        if (!result.empty()) {
            result.pop_back();
        }
        return result;
    }

    std::string tssToString(const std::vector<timespec_t>& vec) {
        std::string result;
        for (auto it = vec.begin(); it != vec.end(); ++it) {
            result += std::to_string(it->tv_sec) + "." + std::to_string(it->tv_nsec) + " ";
        }
        // Remove the trailing space
        if (!result.empty()) {
            result.pop_back();
        }
        return result;
    }

    void printAll() override {
        vector<type_key> keys;
        vector<timespec_t> tss;
        for (int32_t i = 0; i < ht->num_buckets; i++) {
            bucket_t *b = ht->buckets+i;

            do {
                for (uint32_t j = 0; j < b->count; j++) {
                    if (b->tuples[j].key != 0) {
                        keys.push_back(b->tuples[j].key);
                        tss.push_back(b->tuples[j].ts);
                    }
                }
                b = b->next;
            } while (b);
        }
        logger(DBG, "%s-HT content keys: %s", isLeft ? "R" : "S", vectorToString(keys).c_str());
//        logger(DBG, "%s-HT content tss : %s", isLeft ? "R" : "S", tssToString(tss).c_str());
    }
};

#if defined(__cplusplus)
}
#endif

#endif //BUCKETCHAININGHASHTABLE_HPP
