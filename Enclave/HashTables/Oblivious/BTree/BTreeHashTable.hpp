#ifndef BTREEHASHTABLE_HPP
#define BTREEHASHTABLE_HPP

#include "../../HashTableInterface.hpp"
#include "OBTree/BTreeOMAP.h"
#include "OBTree/BTree.h"
#include "OBTree/BTreeORAM.hpp"
#include "OBTree/BTreeEnclaveInterface.h"
#include "../DOHEAPEnclaveInterface.hpp"

#if defined(__cplusplus)
extern "C" {
#endif

using namespace std;

// This is OBLIWIND (paper Sec V-A, Fig 2), the doubly-oblivious sliding
// window used by SHJ-L2/L3/L4. It's a HashTableInterface adapter so SHJ.cpp
// can use it as a drop-in window, but underneath it's really two structures
// kept in sync: `interface` is the index tier, an oblivious B-Tree over
// Path-ORAM keyed by <key, timestamp> so duplicates under the same key stay
// ordered by arrival time; `interfaceHeap` is the expiry tier, a doubly
// oblivious min-heap (DOHEAP / Path Oblivious Heap) ordered by timestamp so
// we can find and evict the oldest tuple in O(log N). Insert writes to both;
// expire pops the heap min and deletes the matching key from the B-Tree.
class BTreeHashTable : public HashTableInterface {
private:

    BTreeEnclaveInterface *interface;
    DOHEAPEnclaveInterface *interfaceHeap;

    // Packs a timestamp into 32 bits (21 bits sec, 21 bits sub-second) so it
    // fits next to a 32-bit key in one composite ORAM key. Caps timestamps at
    // ~34 minutes (see the guard in build()/buildNodes()) - fine for these
    // benchmarks.
    static uint32_t timespec_to_32_bit(timespec_t* ts) {
        uint32_t sec = (uint32_t) ts->tv_sec << 21;
        uint32_t nsec = (uint32_t) (ts->tv_nsec * 2097152 / 999999999); // 2^21 /
        return sec | nsec;
    }

public:
    BTreeHashTable(bool _isLeft) : HashTableInterface(_isLeft) {
        interface = new BTreeEnclaveInterface();
        interfaceHeap = new DOHEAPEnclaveInterface();
    }

    ~BTreeHashTable() override
    {
        delete interface;
        delete interfaceHeap;
    }


    void initialize(uint32_t numTuples) override
    {
        interface->ecall_btree_setup_omap_by_client_with_local_ramStore(this->isLeft, numTuples);
        interfaceHeap->setup_oheap(this->isLeft, numTuples, 1);
        this->capacity = numTuples;
        this->size = 0;
        this->initialized = true;
    }

    // This is OBLIWIND's search(k, tau): one oblivious B-Tree lookup on the
    // composite <k, tau> key.
    uint32_t probe(uint32_t key, timespec_t ts) override {
        uint32_t index;
        uint64_t key_ts = (uint64_t) key << 32 | timespec_to_32_bit(&ts);
        uint32_t val = interface->ecall_btree_read_node_with_ts(this->isLeft, key_ts, &index);
        return val;
    }

    // Walks all duplicates under t.key by repeatedly asking the B-Tree for
    // the next successor key, stopping once the successor key changes (i.e.
    // we've walked past the last tuple with this key). Used to find every
    // match for an incoming join tuple, one oblivious index read per match.
    uint32_t probe(tuple_t t) override
    {
        uint32_t matches = 0;
        uint64_t key = (uint64_t) t.key << 32;
        uint32_t value;
        uint64_t searchKey;
        do {
            searchKey = key;
            interface->ecall_btree_search_succ(isLeft, searchKey, &key, &value);
            if (key != 0 && key != searchKey) {
                matches++;
                emit_result(t, {.key = (type_key) (key >>32), .payload = value});
            }
        } while (key != 0 && key != searchKey);
        return matches;
    }

    uint32_t probeNodes(relation_t *rel) override
    {
        uint32_t matches = 0;
        for (uint32_t i = 0; i < rel->num_tuples; i++) {
            uint64_t key = (uint64_t) rel->tuples[i].key << 32;
            uint32_t value;
            uint64_t searchKey;
            do {
                searchKey = key;
                interface->ecall_btree_search_succ(isLeft, searchKey, &key, &value);
                if (key != 0 && key != searchKey) {
                    matches++;
                    emit_result(rel->tuples[i], {.key = (type_key) (key >>32), .payload = value});
                }
            } while (key != 0 && key != searchKey);
        }
        return matches;
    }

    // insert(tau, k, p): writes into both tiers - the B-Tree gets the tuple
    // under its composite key, the heap gets a (timestamp -> key) entry so
    // expiry can later find it again. op=2 on the heap means insert (see
    // DOHEAPEnclaveInterface::execute_heap_operation).
    void build(tuple_t t) override
    {
        if (t.ts.tv_sec > 2048) {
            logger(ERROR, "Timestamps larger than 34 minutes");
            ocall_throw("");
        }
        uint32_t ts = timespec_to_32_bit(&t.ts);
        interface->ecall_btree_write_node(this->isLeft, ts, t.key, t.payload);
        uint32_t key = t.key;
        interfaceHeap->execute_heap_operation(isLeft, &key, &ts, 2);
        size ++;
    }

    void buildNodes(relation_t *rel) override
    {
        if (!this->initialized) {
            logger(WARN, "Initialized the HT before building it");
        }

        for (uint32_t i = 0; i < rel->num_tuples; i++) {
            if (rel->tuples[i].ts.tv_sec > 2048) {
                logger(ERROR, "Timestamps larger than 34 minutes");
                ocall_throw("");
            }
            uint32_t ts = timespec_to_32_bit(&rel->tuples[i].ts);
            interface->ecall_btree_write_node(this->isLeft, ts, rel->tuples[i].key, rel->tuples[i].payload);
            uint32_t key = rel->tuples[i].key;
            interfaceHeap->execute_heap_operation(isLeft, &key, &ts, 2);
        }
        size += rel->num_tuples;
    }

    void removeNode(tuple_t t) override
    {
        (void) t;
    }

    // expire(): pops the oldest entries off the heap (op=1 = extract-min) and
    // deletes the matching key from the B-Tree, keeping the two tiers in
    // sync. This is the window-eviction step of OBLIWIND.
    vector<type_key> deleteOldest(uint32_t num) override
    {
        uint32_t it = 0;
        uint32_t key=0, v=0;
        vector<type_key> res;
        while(size > 0 && it < num) {
            interfaceHeap->execute_heap_operation(isLeft, &key, &v, 1);
            // logger(DBG, "Delete key=%u ts=%u from BTreeHashTable", key, v);
            uint64_t toDel = (uint64_t) key << 32 | v;
            interface->ecall_btree_remove_node(isLeft, toDel);
            res.push_back(key);
            size--;
            it++;
        }
        return res;
    }

    void printAll() override
    {
        interface->ecall_btree_print_tree(isLeft);
    }

    // OBLIWIND's size(k): two oblivious reads (index of key, index of key+1
    // with tau=0, i.e. the oldest entries of each) and the gap between the
    // two positions is the count of tuples under k. Relies on the B-Tree's
    // order-statistic augmentation (subtree size counters on each node).
    uint32_t getSize(uint32_t key) override
    {
        uint32_t idx1, idx2;
        interface->ecall_btree_read_node(isLeft, key, &idx1);
        interface->ecall_btree_read_node(isLeft, key+1, &idx2);
        return (idx2-idx1);
    }

protected:
    void emit_result(tuple_t r, tuple_t s) override
    {
        (void) (r);
        (void) (s);
    }
};

#if defined(__cplusplus)
}
#endif

#endif //BTREEHASHTABLE_HPP
