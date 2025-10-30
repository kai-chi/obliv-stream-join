#ifndef BTREE_ORAM_HPP
#define BTREE_ORAM_HPP

#include "AES.hpp"
#include "BTreeNode.h"
#include <random>
#include <vector>
#include <unordered_map>
#include <string>
#include <iostream>
#include <map>
#include <set>
#include "../../LocalRAMStore.hpp"
#include "Config.hpp"

using namespace std;

class Cache {
public:
    vector<BTreeNode*> nodes;

    void preAllocate(int n) {
        nodes.reserve(n);
    }

    void insert(BTreeNode* node) {
        nodes.push_back(node);
    };

};

class BTreeORAM {
private:

    uint32_t INF;
    unsigned int PERMANENT_STASH_SIZE;

    size_t blockSize;
    unordered_map<long long, Bucket> virtualStorage;
    Cache stash, incStash;
    uint32_t currentLeaf;

    bytes<Key> key;
    size_t plaintext_size;
    uint32_t bucketCount;
    size_t clen_size;
    bool batchWrite = false;
    uint32_t maxOfRandom;
//    long long maxHeightOfAVLTree;
    bool useLocalRamStore;
    LocalRAMStore* localStore = nullptr;
    int storeBlockSize;
    int stashCounter = 0;
    bool isIncomepleteRead = false;
    bool isLeft;

    uint32_t RandomPath();
    long long GetNodeOnPath(long long leaf, int depth);

    void FetchPath(long long leaf);

    block SerialiseBucket(Bucket bucket);
    Bucket DeserialiseBucket(block buffer);
    void ReadBuckets(vector<uint32_t> indexes);
    void EvictBuckets();
    void WriteBucket(uint32_t index, Bucket bucket);


    BTreeNode* convertBlockToNode(block b);
    block convertNodeToBlock(BTreeNode* node);

public:
    BTreeORAM(uint32_t maxSize, bytes<Key> key, bool simulation, bool isEmptyMap, int degree, bool _useLocalRamStore, bool _isLeft);
    ~BTreeORAM();

    void InitializeORAMBuckets();
    void InitializeBucketsOneByOne();

    double evicttime = 0;
    int evictcount = 0;
    uint32_t nextDummyCounter;
    int readCnt = 0;
    int depth;
    int accessCounter = 0;
    uint64_t cpu_cycles = 0;
    uint64_t eviciton_cycles = 0;
    //-----------------------------------------------------------
    bool evictBuckets = false; //is used for AVL calls. It should be set the same as values in default values
    //-----------------------------------------------------------

    BTreeNode* ReadWrite(uint32_t index, BTreeNode* node, uint32_t lastLeaf, uint32_t newLeaf, bool isRead, bool isDummy, bool isIncompleteRead);
    BTreeNode* ReadWriteTest(uint32_t index, BTreeNode* node, uint32_t lastLeaf, uint32_t newLeaf, bool isRead, bool isDummy, bool isIncompleteRead);
    BTreeNode* ReadWrite(uint32_t index, BTreeNode* node, uint32_t lastLeaf, uint32_t newLeaf, bool isRead, bool isDummy, std::array< byte_t, 16> value, bool overwrite, bool isIncompleteRead);
    BTreeNode* ReadWrite(uint32_t index, uint32_t lastLeaf, uint32_t newLeaf, bool isDummy, uint32_t newChildPos, uint32_t targetNode);
    BTreeNode* ReadWriteBTree(uint32_t index, uint32_t lastLeaf, uint32_t newLeaf, bool isDummy, uint32_t newChildPos, uint64_t targetNode);

    void start(bool batchWrite);
    void prepareForEvictionTest();
    void evict(bool evictBuckets);
    void finilize(bool noDummyOp = false);
    bool profile = false;

    void writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks);
};

#endif
