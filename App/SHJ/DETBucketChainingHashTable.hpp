#ifndef DETBUCKETCHAININGHASHTABLE_HPP
#define DETBUCKETCHAININGHASHTABLE_HPP

#include "../../Include/npj_types.h"
#include "../../Include/LoggerTypes.h"
#include <stdint.h>
#include <stdlib.h>
#include <cstring>
#include <vector>
#include <queue>
#include <tuple>
#include <malloc.h>
#include <string>
#include "Logger.h"
#include <functional>

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

class DETBucketChainingHashTable
{
private:
    DEThashtable_t * DETht = nullptr;
    bool isLeft;
    bool initialized = false;

    /*
     * number of currently stored tuples in the hash table
     */
    uint32_t size = 0;

    /**
     * number of tuples the HT is capable of storing
     * */
    uint32_t capacity = 0;

    typedef tuple<uint64_t, std::array<uint8_t,16>> pi2;
    priority_queue<pi2, vector<pi2>, greater<pi2>> pq;

    void DETallocate_hashtable(DEThashtable_t ** ppht, uint32_t nbuckets)
    {
        DETht              = (DEThashtable_t*)malloc(sizeof(DEThashtable_t));
        DETht->num_buckets = nbuckets;
        NEXT_POW_2((DETht->num_buckets));

        /* allocate hashtable buckets cache line aligned */
        size_t size_local = DETht->num_buckets * sizeof(DETbucket_t);
        DETht->buckets = (DETbucket_t*) malloc(size_local); //(DETbucket_t*) memalign(CACHE_LINE_SIZE, ht->num_buckets * sizeof(DETbucket_t));
        if (!DETht || !DETht->buckets) {
            logger(ERROR, "Memory allocation for the hashtable failed!");
            exit(EXIT_FAILURE);
        }
        memset(DETht->buckets, 0, DETht->num_buckets * sizeof(DETbucket_t));
        DETht->skip_bits = 0; /* the default for modulo hash */
        DETht->hash_mask = (DETht->num_buckets - 1) << DETht->skip_bits;
        *ppht = DETht;
    }

    void emit_result(row_enc_t r, row_enc_t s) {
        (void) (r);
        (void) (s);
   }

public:

    DETBucketChainingHashTable(int _isLeft) : isLeft(_isLeft) {};

    ~DETBucketChainingHashTable()
    {
        free(DETht->buckets);
        free(DETht);
    }
    void initialize(uint32_t numTuples)
    {
        uint32_t nbuckets = (numTuples / BUCKET_SIZE);
        DETallocate_hashtable(&DETht, nbuckets);
        this->capacity = numTuples;
    }

    uint32_t probe(row_enc_t t)
    {
        uint32_t matches = 0;
        std::string vec(t.key, t.key + 16);
        size_t hash = std::hash<std::string>{}(vec);
        type_key idx = HASH((type_key)hash, DETht->hash_mask, DETht->skip_bits);
        DETbucket_t * b = DETht->buckets+idx;

        do {
            for(uint32_t j = 0; j < b->count; j++) {
                // if(t.key == b->tuples[j].key){
                if(std::equal(t.key, t.key+16, b->tuples[j].key)){
                    matches ++;
                    emit_result(t, b->tuples[j]);
                }
            }

            b = b->next;/* follow overflow pointer */
        } while(b);

        return matches;
    }

    // Function to convert timespec to nanoseconds
    uint64_t timespec_to_ns(struct timespec_t ts) {
        return (uint64_t)ts.tv_sec * 1000000000L + (uint64_t)ts.tv_nsec;
    }

    void build(row_enc_t t)
    {
        struct row_enc_t * dest;
        DETbucket_t * curr, * nxt;
        std::string vec(t.key, t.key + 16);
        size_t hash = std::hash<std::string>{}(vec);
        int64_t idx = HASH(hash, DETht->hash_mask, DETht->skip_bits);

        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = DETht->buckets + idx;
        nxt  = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                DETbucket_t * b;
                b = (DETbucket_t*) calloc(1, sizeof(DETbucket_t));
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

        uint64_t ts = timespec_to_ns(t.ts);
        tuple<uint64_t, std::array<uint8_t, 16>> tuple{}; // = make_tuple(ts, t.key);
        std::get<0>(tuple) = ts;
        // for (uint8_t i = 0; i < 16; i++) {
        //     std::get<1>(tuple)[i] = t.key[i];
        // }
        std::copy(std::begin(t.key), std::end(t.key), std::get<1>(tuple).begin());
        pq.push(tuple);
    }

    static bool isTsEqual(const timespec_t *ts1, const timespec_t *ts2) {
        return (ts1->tv_sec == ts2->tv_sec && ts1->tv_nsec == ts2->tv_nsec);
    }

    void removeNode(row_enc_t t)
    {
        const uint32_t hashMask = DETht->hash_mask;
        const uint32_t skipBits = DETht->skip_bits;

        std::string vec(t.key, t.key + 16);
        size_t hash = std::hash<std::string>{}(vec);
        int64_t idx = HASH(hash, hashMask, skipBits);
        DETbucket_t * curr = DETht->buckets+idx;
        DETbucket_t * prev = curr;

        do {
            for (uint64_t j = 0; j < curr->count; j++) {
                if (isTsEqual(&t.ts, &curr->tuples[j].ts) && std::equal(t.key, t.key+16, curr->tuples[j].key)) {
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
                            // curr->tuples[k].key = curr->tuples[k+1].key;
                            std::copy(curr->tuples[k+1].key, curr->tuples[k+1].key+16, curr->tuples[k].key);
                            // curr->tuples[k].payload = curr->tuples[k+1].payload;
                            std::copy(curr->tuples[k+1].payload, curr->tuples[k+1].payload+16, curr->tuples[k].payload);
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


    std::vector<std::array<uint8_t, 16>> deleteOldest(uint32_t num)
    {
        vector<std::array<uint8_t, 16>> res;
        for (uint32_t i = 0; i < num; i++) {
            pi2 t = pq.top();
            timespec_t ts = (timespec_t) { .tv_sec  = get<0>(t) / 1000000000L,
                    .tv_nsec = get<0>(t) % 1000000000L };
            auto& key = get<1>(t);
            row_enc_t tuple{};
            tuple.ts = ts;
            for (uint8_t j = 0; j < 16; j++) {
                tuple.key[j] = key[j];
            }
            removeNode(tuple);
            pq.pop();
            res.push_back(key);
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

    void printAll() {
        vector<type_key> keys;
        vector<timespec_t> tss;
        for (int32_t i = 0; i < DETht->num_buckets; i++) {
            DETbucket_t *b = DETht->buckets+i;

            do {
                for (uint32_t j = 0; j < b->count; j++) {
                    if (b->tuples[j].key != 0) {
                        keys.push_back(b->tuples[j].key[0]);
                        tss.push_back(b->tuples[j].ts);
                    }
                }
                b = b->next;
            } while (b);
        }
        logger(DBG, "%s-HT content keys: %s", isLeft ? "R" : "S", vectorToString(keys).c_str());
//        logger(DBG, "%s-HT content tss : %s", isLeft ? "R" : "S", tssToString(tss).c_str());
    }

    bool isOverflown() {
        return capacity > size;
    }
};

#if defined(__cplusplus)
}
#endif

#endif //DETBUCKETCHAININGHASHTABLE_HPP
