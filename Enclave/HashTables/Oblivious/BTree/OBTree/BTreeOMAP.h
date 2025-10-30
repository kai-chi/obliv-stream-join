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
    void remove(uint64_t key);
    void removeNO(uint64_t key);
    uint32_t search(uint64_t key_ts, uint32_t &index);
    BTreeKeyValuePair* searchSucc(uint64_t key);
    void printTree();
    vector<uint32_t> traverse();

    void writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks);
};

#endif /* BTREEOMAP_H */

