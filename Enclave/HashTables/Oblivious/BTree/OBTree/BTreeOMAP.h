#ifndef BTREEOMAP_H
#define BTREEOMAP_H
#include <iostream>
#include "BTreeORAM.hpp"
#include <functional>
#include <fstream>
#include <cstdio>
#include <cstring>
#include <iostream>
#include "BTree.h"
using namespace std;

// Thin wrapper around BTree that owns the root pointer (index + ORAM
// position) and forwards to the actual oblivious operations. This is what
// BTreeEnclaveInterface talks to; BTree itself doesn't track its own root.
// search()/insert() call BTree's "2"-suffixed methods (search2/insert2), the
// oblivious ones actually used at runtime - see the comment in BTree.h for
// why there are several search/insert/remove variants.
class BTreeOMAP {
private:
    uint32_t rootIndex;
    uint32_t rootPos;


public:
    BTree* btreeHandler;

    BTreeOMAP(int maxSize, bytes<Key> key, int useLocalRamStore, bool isLeft);
    BTreeOMAP(int maxSize, bytes<Key> secretKey, map<uint32_t, uint32_t>* pairs, map<unsigned long long, unsigned long long>* permutation, int useLocalRamStore, bool isLeft);
    BTreeOMAP(int maxSize, uint32_t rootIndex, uint32_t rootPos, bytes<Key> secretKey, int useLocalRamStore, bool isLeft);
    virtual ~BTreeOMAP();
    void insert(uint32_t ts, uint32_t key, uint32_t value);
    // removeX under the hood - the oblivious delete path actually wired up.
    void remove(uint64_t key);
    // remove3 under the hood - non-oblivious, kept around for debugging/tests.
    void removeNO(uint64_t key);
    // Exact-match lookup on the composite <key,ts> key (OBLIWIND search(k,tau)).
    uint32_t search(uint64_t key_ts, uint32_t &index);
    // Finds the smallest stored key >= the given key (used to walk all
    // duplicates of a join key one successor at a time).
    BTreeKeyValuePair* searchSucc(uint64_t key);
    void printTree();
    vector<uint32_t> traverse();

    void writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks);
};

#endif /* BTREEOMAP_H */

