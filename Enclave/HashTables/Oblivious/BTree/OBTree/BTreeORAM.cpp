#include "BTreeORAM.hpp"
#include "../../../../../Include/BTreeNode.h"
#include "../../../../../Include/Common.h"
#include <algorithm>
#include <fstream>
#include <cmath>
#include <cstring>
#include <map>
#include <stdexcept>
#include "sgx_trts.h"
#include "BTreeObliviousOperations.h"
#include "Enclave_t.h"  /* print_string */
#include "Enclave.h"

#ifndef ORAM_DEBUG
#ifdef SGX_DEBUG
//#define ORAM_DEBUG
#endif
#endif

BTreeORAM::BTreeORAM(uint32_t maxSize, bytes<Key> oram_key, bool simulation, bool isEmptyMap, int degree, bool _useLocalRamStore, bool _isLeft)
        : key(oram_key), useLocalRamStore(_useLocalRamStore), isLeft(_isLeft) {
//    logger(DBG, "***** Construct ORAM *****");
//    depth = (int) (ceil(log2(maxSize)) - 1) + 1;
//    maxOfRandom = (long long) (pow(2, depth));
    depth = max_depth_btree(maxSize, degree);
    maxOfRandom = max_of_random_btree(depth, degree);

    AES::Setup();
    bucketCount = maxOfRandom * 2 - 1;
    INF = 2147483647 - (bucketCount);
    PERMANENT_STASH_SIZE = 90;
    stash.preAllocate(PERMANENT_STASH_SIZE * 4);

    nextDummyCounter = INF;
    blockSize = sizeof (BTreeNode); // B
//    logger(DBG, "maxSize: %d, degree: %d, number of leaves:%lld, depth:%lld, blockSize: %d", maxSize, degree, maxOfRandom, depth, blockSize);
    size_t blockCount = (size_t) (Z * bucketCount);
    storeBlockSize = (size_t) (IV + AES::GetCiphertextLength((int) (Z * (blockSize))));
    clen_size = AES::GetCiphertextLength((int) (blockSize) * Z);
    plaintext_size = (blockSize) * Z;
    if (!simulation) {
        if (useLocalRamStore) {
//            logger(DBG, "Use local RAMStore num=%zu size=%zu", blockCount, plaintext_size);
            localStore = new LocalRAMStore(blockCount, plaintext_size);
        } else {
            logger(WARN, "Using non-local RAMStore");
            ocall_setup_ramStore(isLeft, blockCount, storeBlockSize);
        }
    } else {
        ocall_setup_ramStore(isLeft, depth, -1);
    }

//    maxHeightOfAVLTree = (int) floor(log2(blockCount)) + 1;

//    logger(DBG, "Initializing ORAM Buckets");
    Bucket bucket;
    for (uint32_t z = 0; z < Z; z++) {
        bucket[z].id = 0;
        bucket[z].data.resize(blockSize, 0);
    }
    if (!simulation && isEmptyMap) {
        InitializeORAMBuckets();
    }
    for (unsigned int i = 0; i < PERMANENT_STASH_SIZE; i++) {
        BTreeNode* dummy = new BTreeNode();
        dummy->index = nextDummyCounter;
        dummy->evictionNode = -1;
        dummy->isDummy = true;
        dummy->n = 0;
        dummy->leaf = false;
        dummy->pos = 0;
        dummy->height = 1;
        stash.insert(dummy);
        nextDummyCounter++;
    }
//    logger(DBG, "End of Initialization");
}

BTreeORAM::~BTreeORAM() {
    AES::Cleanup();
    if (useLocalRamStore) {
        delete localStore;
    }
}

void BTreeORAM::InitializeBucketsOneByOne() {
    for (long long i = 0; i < bucketCount; i++) {
        if (i % 10000 == 0) {
            logger(DBG, "%d/%d", i, bucketCount);
        }
        Bucket bucket;
        for (uint32_t z = 0; z < Z; z++) {
            bucket[z].id = 0;
            bucket[z].data.resize(blockSize, 0);
        }
        WriteBucket((int) i, bucket);
    }
}


void BTreeORAM::InitializeORAMBuckets() {
    double time;
    ocall_start_timer(687);

    //        InitializeBuckets(0, bucketCount, bucket);
    InitializeBucketsOneByOne();
    //    InitializeBucketsInBatch();


    ocall_stop_timer(&time, 687);
    logger(DBG, "ORAM Initialization Time:%f", time);
}

void BTreeORAM::WriteBucket(uint32_t index, Bucket bucket) {
    block b = SerialiseBucket(bucket);
    if (useLocalRamStore) {
        localStore->Write(index, b);
    } else {
        block ciphertext = AES::Encrypt(key, b, clen_size, plaintext_size);
        ocall_write_ramStore(isLeft, index, (const char*) ciphertext.data(), (size_t) ciphertext.size());
    }
}
// Fetches the array index a bucket that lise on a specific path

long long BTreeORAM::GetNodeOnPath(long long leaf, int curDepth) {
    leaf += bucketCount / 2;
    for (int d = depth - 1; d >= 0; d--) {
        bool cond = !BTreeNode::CTeq(BTreeNode::CTcmp(d, curDepth), -1);
        leaf = BTreeNode::conditional_select((leaf + 1) / 2 - 1, leaf, cond);
    }
    return leaf;
}

// Write bucket to a single block

block BTreeORAM::SerialiseBucket(Bucket bucket) {
    block buffer;
    for (uint32_t z = 0; z < Z; z++) {
        Block b = bucket[z];
        buffer.insert(buffer.end(), b.data.begin(), b.data.end());
    }
    assert(buffer.size() == Z * (blockSize));
    return buffer;
}

Bucket BTreeORAM::DeserialiseBucket(block buffer) {
    assert(buffer.size() == Z * (blockSize));
    Bucket bucket;
    for (uint32_t z = 0; z < Z; z++) {
        Block &curBlock = bucket[z];
        curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
        BTreeNode* node = convertBlockToNode(curBlock.data);
        bool cond = BTreeNode::CTeq(node->index, (uint32_t) 0);
        node->index = BTreeNode::conditional_select(node->index, nextDummyCounter, !cond);
        node->isDummy = BTreeNode::conditional_select(0, 1, !cond);
        if (isIncomepleteRead) {
            incStash.insert(node);
        } else {
            stash.insert(node);
        }
        buffer.erase(buffer.begin(), buffer.begin() + blockSize);
    }
    return bucket;
}

void BTreeORAM::ReadBuckets(vector<uint32_t> indexes) {
    if (indexes.size() == 0) {
        return;
    }
    if (useLocalRamStore) {
        for (unsigned int i = 0; i < indexes.size(); i++) {
            block buffer = localStore->Read(indexes[i]);
//            block buffer = AES::Decrypt(key, ciphertext, clen_size);
            Bucket bucket = DeserialiseBucket(buffer);
            virtualStorage[indexes[i]] = bucket;
        }
    } else {
        size_t readSize;
        char* tmp = new char[indexes.size() * storeBlockSize];
        ocall_nread_ramStore(&readSize, isLeft, indexes.size(), indexes.data(), tmp, indexes.size() * storeBlockSize);
        for (unsigned int i = 0; i < indexes.size(); i++) {
            block ciphertext(tmp + i*readSize, tmp + (i + 1) * readSize);
            block buffer = AES::Decrypt(key, ciphertext, clen_size);
            Bucket bucket = DeserialiseBucket(buffer);
            virtualStorage[indexes[i]] = bucket;
        }
        delete[] tmp;
    }
}

void BTreeORAM::EvictBuckets() {
    unordered_map<long long, Bucket>::iterator it = virtualStorage.begin();
    if (useLocalRamStore) {
        for (it = virtualStorage.begin(); it != virtualStorage.end(); it++) {
            block b = SerialiseBucket(it->second);
//            block ciphertext = AES::Encrypt(key, b, clen_size, plaintext_size);
            localStore->Write(it->first, b);
        }
    } else {
        for (unsigned int j = 0; j <= virtualStorage.size() / 10000; j++) {
            char* tmp = new char[10000 * storeBlockSize];
            vector<uint32_t> indexes;
            size_t cipherSize = 0;
            size_t blockCount = min((int) (virtualStorage.size() - j * 10000), 10000);
            for (size_t i = 0; i < blockCount; i++) {
                block b = SerialiseBucket(it->second);
                indexes.push_back((uint32_t) it->first);
                block ciphertext = AES::Encrypt(key, b, clen_size, plaintext_size);
                std::memcpy(tmp + i * ciphertext.size(), ciphertext.data(), ciphertext.size());
                cipherSize = ciphertext.size();
                it++;
            }
            if (blockCount != 0) {
                size_t len = cipherSize * blockCount;
                ocall_nwrite_ramStore(isLeft, blockCount, indexes.data(), (const char*) tmp, len);
            }
            delete[] tmp;
            indexes.clear();
        }
    }
    virtualStorage.clear();
}
// Fetches blocks along a path, adding them to the stash

void BTreeORAM::FetchPath(long long leaf) {
    readCnt++;
    vector<uint32_t> nodesIndex;
    vector<uint32_t> existingIndexes;

    long long node = leaf;

    node += bucketCount / 2;
    if (virtualStorage.count(node) == 0) {
        nodesIndex.push_back((uint32_t) node);
    } else {
        existingIndexes.push_back((uint32_t) node);
    }

    for (int d = depth - 1; d >= 0; d--) {
        node = (node + 1) / 2 - 1;
        if (virtualStorage.count(node) == 0) {
            nodesIndex.push_back((uint32_t) node);
        } else {
            existingIndexes.push_back((uint32_t) node);
        }
    }

    ReadBuckets(nodesIndex);

    for (unsigned int i = 0; i < existingIndexes.size(); i++) {
        Bucket bucket = virtualStorage[existingIndexes[i]];
        for (uint32_t z = 0; z < Z; z++) {
            Block &curBlock = bucket[z];
            BTreeNode* node_local = convertBlockToNode(curBlock.data);
            bool cond = BTreeNode::CTeq(node_local->index, (uint32_t) 0);
            node_local->index = BTreeNode::conditional_select(node_local->index, nextDummyCounter, !cond);
            node_local->isDummy = BTreeNode::conditional_select(0, 1, !cond);
            if (isIncomepleteRead) {
                incStash.insert(node_local);
            } else {
                stash.insert(node_local);
            }
        }
    }

}

//    for (int i = 0; i < stash.nodes.size(); i++) {
//        if (stash.nodes[i]->isDummy == false)
//            printf("1.index:%llu key:%lld  pos:%llu, isDummy:%d leftKey:%lld left Pos:%llu right key:%lld right pos:%llu\n", stash.nodes[i]->index, stash.nodes[i]->key.getValue(), stash.nodes[i]->pos, stash.nodes[i]->isDummy ? 1 : 0, stash.nodes[i]->leftID.getValue(), stash.nodes[i]->leftPos, stash.nodes[i]->rightID.getValue(), stash.nodes[i]->rightPos);
//    }

BTreeNode* BTreeORAM::ReadWrite(uint32_t index, BTreeNode* inputnode, uint32_t lastLeaf, uint32_t newLeaf, bool isRead, bool isDummy, bool isIncRead) {
    if (index == 0) {
        logger(ERROR, "bid is 0 dummy is:%d", isDummy ? 1 : 0);
        throw runtime_error("Node id is not set");
    }
#ifdef ORAM_DEBUG
    logger(DBG, "ORAM::ReadWrite index: %u, inputnode (index: %u, pos: %u) lastLeaf: %u, newLeaf: %u, isDummy: %d",
           index, inputnode->index, inputnode->pos, lastLeaf, newLeaf, isDummy);
#endif

    accessCounter++;

    isIncomepleteRead = isIncRead;

    uint32_t newPos = RandomPath();
    uint32_t fetchPos = BTreeNode::conditional_select(newPos, lastLeaf, isDummy);

    inputnode->pos = fetchPos;

    FetchPath(fetchPos);


    if (!isIncomepleteRead) {
        currentLeaf = fetchPos;
    }

    BTreeNode* tmpWrite = BTreeNode::clone(inputnode);
    tmpWrite->pos = newLeaf;

    BTreeNode* res = new BTreeNode();
    res->isDummy = true;
    res->index = nextDummyCounter++;
    bool write = !isRead;

    vector<BTreeNode*> nodesList(stash.nodes.begin(), stash.nodes.end());

    if (isIncomepleteRead) {
        nodesList.insert(nodesList.end(), incStash.nodes.begin(), incStash.nodes.end());
    }

    for (BTreeNode* node : nodesList) {
        bool match = BTreeNode::CTeq(BTreeNode::CTcmp(node->index, index), 0) && !node->isDummy;
        node->isDummy = BTreeNode::conditional_select(true, node->isDummy, !isDummy && match && write);
        node->pos = BTreeNode::conditional_select(newLeaf, node->pos, !isDummy && match);
        bool choice = !isDummy && match && isRead && !node->isDummy;
        res->index = BTreeNode::conditional_select((uint32_t) node->index, (uint32_t) res->index, choice);
        res->isDummy = BTreeNode::conditional_select(node->isDummy, res->isDummy, choice);
        res->pos = BTreeNode::conditional_select((uint32_t) node->pos, (uint32_t) res->pos, choice);
        res->evictionNode = BTreeNode::conditional_select(node->evictionNode, res->evictionNode, choice);
        res->height = BTreeNode::conditional_select(node->height, res->height, choice);

        res->n = BTreeNode::conditional_select(node->n, res->n, choice);
        res->leaf = BTreeNode::conditional_select(node->leaf, res->leaf, choice);
        for (uint32_t k = 0; k < KV_SIZE; k++) {
            res->kvPairs[k].key = BTreeNode::conditional_select(node->kvPairs[k].key, res->kvPairs[k].key, choice);
            res->kvPairs[k].value = BTreeNode::conditional_select(node->kvPairs[k].value , res->kvPairs[k].value , choice);
        }

        for (uint32_t k = 0; k < CHILDREN_SIZE; k++) {
            res->children[k].index = BTreeNode::conditional_select(node->children[k].index, res->children[k].index, choice);
            res->children[k].oramPos = BTreeNode::conditional_select(node->children[k].oramPos, res->children[k].oramPos, choice);
            res->children[k].count = BTreeNode::conditional_select(node->children[k].count, res->children[k].count, choice);
        }
    }

    if (!isIncomepleteRead) {
        stash.insert(tmpWrite);
    } else {
        incStash.insert(tmpWrite);
    }

    if (!isIncomepleteRead) {
        evict(evictBuckets);
    } else {
        for (BTreeNode* item : incStash.nodes) {
            delete item;
        }
        incStash.nodes.clear();
    }

    isIncomepleteRead = false;

    return res;
}

BTreeNode* BTreeORAM::ReadWriteTest(uint32_t index, BTreeNode* inputnode, uint32_t lastLeaf, uint32_t newLeaf, bool isRead, bool isDummy, bool isIncRead) {
    if (index == 0) {
        logger(ERROR, "bid is 0 dummy is:%d", isDummy ? 1 : 0);
        throw runtime_error("Node id is not set");
    }
    accessCounter++;

    isIncomepleteRead = isIncRead;

    uint32_t newPos = 0;
    uint32_t fetchPos = BTreeNode::conditional_select(newPos, lastLeaf, isDummy);

    inputnode->pos = fetchPos;

    FetchPath(fetchPos);


    if (!isIncomepleteRead) {
        currentLeaf = fetchPos;
    }

    BTreeNode* tmpWrite = BTreeNode::clone(inputnode);
    tmpWrite->pos = newLeaf;

    BTreeNode* res = new BTreeNode();
    res->isDummy = true;
    res->index = nextDummyCounter++;
    bool write = !isRead;

    vector<BTreeNode*> nodesList(stash.nodes.begin(), stash.nodes.end());

    if (isIncomepleteRead) {
        nodesList.insert(nodesList.end(), incStash.nodes.begin(), incStash.nodes.end());
    }

    for (BTreeNode* node : nodesList) {
        bool match = BTreeNode::CTeq(BTreeNode::CTcmp(node->index, index), 0) && !node->isDummy;
        node->isDummy = BTreeNode::conditional_select(true, node->isDummy, !isDummy && match && write);
        node->pos = BTreeNode::conditional_select(newLeaf, node->pos, !isDummy && match);
        bool choice = !isDummy && match && isRead && !node->isDummy;
        res->index = BTreeNode::conditional_select((uint32_t) node->index, (uint32_t) res->index, choice);
        res->isDummy = BTreeNode::conditional_select(node->isDummy, res->isDummy, choice);
        res->pos = BTreeNode::conditional_select((uint32_t) node->pos, (uint32_t) res->pos, choice);
        res->evictionNode = BTreeNode::conditional_select(node->evictionNode, res->evictionNode, choice);
        res->height = BTreeNode::conditional_select(node->height, res->height, choice);

        res->n = BTreeNode::conditional_select(node->n, res->n, choice);
        res->leaf = BTreeNode::conditional_select(node->leaf, res->leaf, choice);
        for (uint32_t k = 0; k < KV_SIZE; k++) {
            res->kvPairs[k].key = BTreeNode::conditional_select(node->kvPairs[k].key, res->kvPairs[k].key, choice);
            res->kvPairs[k].value = BTreeNode::conditional_select(node->kvPairs[k].value , res->kvPairs[k].value, choice);
        }

        for (uint32_t k = 0; k < CHILDREN_SIZE; k++) {
            res->children[k].index = BTreeNode::conditional_select(node->children[k].index, res->children[k].index, choice);
            res->children[k].oramPos = BTreeNode::conditional_select(node->children[k].oramPos, res->children[k].oramPos, choice);
            res->children[k].count = BTreeNode::conditional_select(node->children[k].count, res->children[k].count, choice);
        }
    }

    if (!isIncomepleteRead) {
        stash.insert(tmpWrite);
    } else {
        incStash.insert(tmpWrite);
    }

    if (!isIncomepleteRead) {
        evict(evictBuckets);
    } else {
        for (BTreeNode* item : incStash.nodes) {
            delete item;
        }
        incStash.nodes.clear();
    }

    isIncomepleteRead = false;

    return res;
}

BTreeNode* BTreeORAM::ReadWrite(uint32_t index, uint32_t lastLeaf, uint32_t newLeaf, bool isDummy, uint32_t newChildPos, uint32_t targetNode) {
    (void) (newChildPos);
    (void) (targetNode);
    if (index == 0) {
        logger(ERROR,"bid is 0 dummy is:%d", isDummy ? 1 : 0);
        throw runtime_error("Node id is not set");
    }
#ifdef ORAM_DEBUG
    logger(DBG, "ORAM::ReadWrite index: %u, lastLeaf: %u, newLeaf: %u, isDummy: %d",
           index, lastLeaf, newLeaf, isDummy);
#endif

    accessCounter++;


    uint32_t newPos = RandomPath();
    uint32_t fetchPos = BTreeNode::conditional_select(newPos, lastLeaf, isDummy);

    FetchPath(fetchPos);


    currentLeaf = fetchPos;

    BTreeNode* res = new BTreeNode();
    res->isDummy = true;
    res->index = nextDummyCounter++;

    for (BTreeNode* node : stash.nodes) {
        bool match = BTreeNode::CTeq(BTreeNode::CTcmp(node->index, index), 0) && !node->isDummy;
        node->pos = BTreeNode::conditional_select(newLeaf, node->pos, !isDummy && match);

        bool choice = !isDummy && match && !node->isDummy;

        res->index = BTreeNode::conditional_select((uint32_t) node->index, (uint32_t) res->index, choice);
        res->isDummy = BTreeNode::conditional_select(node->isDummy, res->isDummy, choice);
        res->pos = BTreeNode::conditional_select((uint32_t) node->pos, (uint32_t) res->pos, choice);
        res->evictionNode = BTreeNode::conditional_select(node->evictionNode, res->evictionNode, choice);
        res->height = BTreeNode::conditional_select(node->height, res->height, choice);

        res->n = BTreeNode::conditional_select(node->n, res->n, choice);
        res->leaf = BTreeNode::conditional_select(node->leaf, res->leaf, choice);
        for (uint32_t k = 0; k < KV_SIZE; k++) {
            res->kvPairs[k].key = BTreeNode::conditional_select(node->kvPairs[k].key, res->kvPairs[k].key, choice);
            res->kvPairs[k].value = BTreeNode::conditional_select(node->kvPairs[k].value , res->kvPairs[k].value , choice);
        }

        for (uint32_t k = 0; k < CHILDREN_SIZE; k++) {
            res->children[k].index = BTreeNode::conditional_select(node->children[k].index, res->children[k].index, choice);
            res->children[k].oramPos = BTreeNode::conditional_select(node->children[k].oramPos, res->children[k].oramPos, choice);
            res->children[k].count = BTreeNode::conditional_select(node->children[k].count, res->children[k].count, choice);
        }


        //these 2 should be after result set(here is correct)
//        node->leftPos = Node::conditional_select(newChildPos, node->leftPos, !isDummy && match && leftChild);
//        node->rightPos = Node::conditional_select(newChildPos, node->rightPos, !isDummy && match && rightChild);

        if (!isDummy && match) {
            //printf("previous pos:%lld new pos:%lld\n",lastLeaf,newLeaf);
            //printf("in read and set-node:%d:%d:%d:%d:%d:%d:%d\n", node->key.getValue(), node->height, node->pos, node->leftID.getValue(), node->leftPos, node->rightID.getValue(), node->rightPos);
            //printf("in read and set-res:%d:%d:%d:%d:%d:%d:%d\n", res->key.getValue(), res->height, res->pos, res->leftID.getValue(), res->leftPos, res->rightID.getValue(), res->rightPos);
        }
    }

    evict(evictBuckets);
    return res;
}

BTreeNode* BTreeORAM::ReadWriteBTree(uint32_t index, uint32_t lastLeaf, uint32_t newLeaf, bool isDummy, uint32_t newChildPos, uint64_t targetNode) {
    if (index == 0) {
        logger(ERROR,"bid is 0 dummy is:%d", isDummy ? 1 : 0);
        throw runtime_error("Node id is not set");
    }
#ifdef ORAM_DEBUG
    logger(DBG, "ReadWriteBTree index: %u, lastLeaf: %u, newLeaf: %u, isDummy: %d, newChildPos: %u, targetNode: %lu(%u.%u)",
           index, lastLeaf, newLeaf, isDummy, newChildPos, targetNode, targetNode>>32, targetNode&0xffffffff);
#endif
    accessCounter++;


    uint32_t newPos = RandomPath();
    uint32_t fetchPos = BTreeNode::conditional_select(newPos, lastLeaf, isDummy);

    FetchPath(fetchPos);


    currentLeaf = fetchPos;

    BTreeNode* res = new BTreeNode();
    res->isDummy = true;
    res->index = nextDummyCounter++;

    for (BTreeNode* node : stash.nodes) {
        bool match = BTreeNode::CTeq(BTreeNode::CTcmp(node->index, index), 0) && !node->isDummy;
        node->pos = BTreeNode::conditional_select(newLeaf, node->pos, !isDummy && match);

        bool choice = !isDummy && match && !node->isDummy;

//        bool leftChild = Node::CTeq(Bid::CTcmp(node->key, targetNode), 1);
//        bool rightChild = Node::CTeq(Bid::CTcmp(node->key, targetNode), -1);

        size_t childIndex = node->CTfindKey(targetNode);
        bool updateChildPos = false;
        for (size_t i = 0; i < KV_SIZE + 1; i++) {
//            updateChildPos = updateChildPos || (childIndex == i && !node->leaf && node->getKVPairKey(i) == targetNode);
            bool c0 = node->getKVPairKey(i % KV_SIZE) == targetNode && i < (size_t) node->n;
            bool c1 = !node->leaf;
            bool c2 = childIndex == i;
            updateChildPos = updateChildPos || (!isDummy && match && c1 && c2 && !c0);
        }

        res->index = BTreeNode::conditional_select((uint32_t) node->index, (uint32_t) res->index, choice);
        res->isDummy = BTreeNode::conditional_select(node->isDummy, res->isDummy, choice);
        res->pos = BTreeNode::conditional_select((uint32_t) node->pos, (uint32_t) res->pos, choice);
        res->evictionNode = BTreeNode::conditional_select(node->evictionNode, res->evictionNode, choice);
        res->height = BTreeNode::conditional_select(node->height, res->height, choice);

        res->n = BTreeNode::conditional_select(node->n, res->n, choice);
        res->leaf = BTreeNode::conditional_select(node->leaf, res->leaf, choice);
        for (uint32_t k = 0; k < KV_SIZE; k++) {
            res->kvPairs[k].key = BTreeNode::conditional_select(node->kvPairs[k].key, res->kvPairs[k].key, choice);
            res->kvPairs[k].value = BTreeNode::conditional_select(node->kvPairs[k].value , res->kvPairs[k].value , choice);
        }

        for (uint32_t k = 0; k < CHILDREN_SIZE; k++) {
            res->children[k].index = BTreeNode::conditional_select(node->children[k].index, res->children[k].index, choice);
            res->children[k].oramPos = BTreeNode::conditional_select(node->children[k].oramPos, res->children[k].oramPos, choice);
            res->children[k].count = BTreeNode::conditional_select(node->children[k].count, res->children[k].count, choice);
        }


        //these 2 should be after result set(here is correct)
//        node->leftPos = Node::conditional_select(newChildPos, node->leftPos, !isDummy && match && leftChild);
//        node->rightPos = Node::conditional_select(newChildPos, node->rightPos, !isDummy && match && rightChild);
        for (size_t i = 0; i < CHILDREN_SIZE; i++) {
            bool c = (i == childIndex && updateChildPos);
            node->setChildPos(i, BTreeNode::conditional_select(newChildPos, node->getChildPos(i), c));
        }

        if (!isDummy && match) {
            //printf("previous pos:%lld new pos:%lld\n",lastLeaf,newLeaf);
            //printf("in read and set-node:%d:%d:%d:%d:%d:%d:%d\n", node->key.getValue(), node->height, node->pos, node->leftID.getValue(), node->leftPos, node->rightID.getValue(), node->rightPos);
            //printf("in read and set-res:%d:%d:%d:%d:%d:%d:%d\n", res->key.getValue(), res->height, res->pos, res->leftID.getValue(), res->leftPos, res->rightID.getValue(), res->rightPos);
        }
    }

    evict(evictBuckets);
    return res;
}

BTreeNode* BTreeORAM::ReadWrite(uint32_t index, BTreeNode* inputnode, uint32_t lastLeaf, uint32_t newLeaf, bool isRead, bool isDummy, std::array< byte_t, 16> value, bool overwrite, bool isIncRead) {
    (void) (value);
    (void) (overwrite);
    if (index == 0) {
        logger(ERROR,"bid is 0 dummy is:%d", isDummy ? 1 : 0);
        throw runtime_error("Node id is not set");
    }
#ifdef ORAM_DEBUG
    logger(DBG, "ORAM::ReadWrite overwrite index: %u, lastLeaf: %u, newLeaf: %u, isDummy: %d",
           index, lastLeaf, newLeaf, isDummy);
#endif
    accessCounter++;
    isIncomepleteRead = isIncRead;

    uint32_t newPos = RandomPath();
    uint32_t fetchPos = BTreeNode::conditional_select(newPos, lastLeaf, isDummy);

    inputnode->pos = fetchPos;

    FetchPath(fetchPos);

    if (!isIncomepleteRead) {
        currentLeaf = fetchPos;
    }

    BTreeNode* tmpWrite = BTreeNode::clone(inputnode);
    tmpWrite->pos = newLeaf;

    BTreeNode* res = new BTreeNode();
    res->isDummy = true;
    res->index = nextDummyCounter++;
    bool write = !isRead;


    vector<BTreeNode*> nodesList(stash.nodes.begin(), stash.nodes.end());

    if (isIncomepleteRead) {
        nodesList.insert(nodesList.end(), incStash.nodes.begin(), incStash.nodes.end());
    }

    for (BTreeNode* node : nodesList) {
        bool match = BTreeNode::CTeq(BTreeNode::CTcmp(node->index, index), 0) && !node->isDummy;
        node->isDummy = BTreeNode::conditional_select(true, node->isDummy, !isDummy && match && write);
        node->pos = BTreeNode::conditional_select(newLeaf, node->pos, !isDummy && match);
        bool choice = !isDummy && match && isRead && !node->isDummy;
        res->index = BTreeNode::conditional_select((uint32_t) node->index, (uint32_t) res->index, choice);
        res->isDummy = BTreeNode::conditional_select(node->isDummy, res->isDummy, choice);
        res->pos = BTreeNode::conditional_select((uint32_t) node->pos, (uint32_t) res->pos, choice);
        res->evictionNode = BTreeNode::conditional_select(node->evictionNode, res->evictionNode, choice);
        res->height = BTreeNode::conditional_select(node->height, res->height, choice);
    }

    if (!isIncomepleteRead) {
        stash.insert(tmpWrite);
    } else {
        incStash.insert(tmpWrite);
    }

    if (!isIncomepleteRead) {
        evict(evictBuckets);
    } else {
        for (BTreeNode* item : incStash.nodes) {
            delete item;
        }
        incStash.nodes.clear();
    }

    isIncomepleteRead = false;
    return res;
}

BTreeNode* BTreeORAM::convertBlockToNode(block b) {
    BTreeNode* node = new BTreeNode();
    std::array<byte_t, sizeof (BTreeNode) > arr;
    std::copy(b.begin(), b.begin() + sizeof (BTreeNode), arr.begin());
    from_bytes(arr, *node);
    return node;
}

block BTreeORAM::convertNodeToBlock(BTreeNode* node) {
    std::array<byte_t, sizeof (BTreeNode) > data = to_bytes(*node);
    block b(data.begin(), data.end());
    return b;
}

void BTreeORAM::finilize(bool noDummyOp) {
    if (!noDummyOp && stashCounter == 25) {
        stashCounter = 0;
        EvictBuckets();
    } else {
        stashCounter++;
    }
}

void BTreeORAM::evict(bool evictBucketsForORAM) {
    double time;
    if (profile) {
        ocall_start_timer(15);
        ocall_start_timer(10);
    }

    vector<long long> firstIndexes;
    long long tmpleaf = currentLeaf;
    tmpleaf += bucketCount / 2;
    firstIndexes.push_back(tmpleaf);

    for (int d = depth - 1; d >= 0; d--) {
        tmpleaf = (tmpleaf + 1) / 2 - 1;
        firstIndexes.push_back(tmpleaf);
    }

    for (BTreeNode* node : stash.nodes) {
        long long xorVal = 0;
        xorVal = BTreeNode::conditional_select((unsigned long long) 0, (unsigned long long) node->pos ^ currentLeaf, node->isDummy);
        long long indx = 0;

        indx = (long long) floor(log2(BTreeNode::conditional_select(xorVal, (long long) 1, BTreeNode::CTcmp(xorVal, 0))));
        indx = indx + BTreeNode::conditional_select(1, 0, BTreeNode::CTcmp(xorVal, 0));

        for (long long i = 0; (size_t) i < firstIndexes.size(); i++) {
            bool choice = BTreeNode::CTeq(i, indx);
//            long long value = firstIndexes[i];
            if ((size_t) indx >= firstIndexes.size()) {
                logger(DBG, "indx too big %d > %d", indx, firstIndexes.size());
                throw runtime_error("");
            }
            node->evictionNode = BTreeNode::conditional_select(firstIndexes[indx], node->evictionNode, !node->isDummy && choice);
        }
    }

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Assigning stash blocks to lowest possible level:%f", time);
        ocall_start_timer(10);
    }

    long long node = currentLeaf + bucketCount / 2;
    for (int d = (int) depth; d >= 0; d--) {
        for (uint32_t j = 0; j < Z; j++) {
            BTreeNode* dummy = new BTreeNode();
            dummy->index = nextDummyCounter;
            nextDummyCounter++;
            dummy->evictionNode = node;
            dummy->isDummy = true;
            stash.nodes.push_back(dummy);
        }
        node = (node + 1) / 2 - 1;
    }

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Creating Dummy Blocks for each Bucket:%f", time);
        ocall_start_timer(10);
    }

    BTreeObliviousOperations::oblixmergesort(&stash.nodes);

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "First Oblivious Sort: %f", time);
        ocall_start_timer(10);
    }

    long long currentID = GetNodeOnPath(currentLeaf, depth);
    int level = depth;
    int counter = 0;

    for (unsigned long long i = 0; i < stash.nodes.size(); i++) {
        BTreeNode* curNode = stash.nodes[i];
        bool firstCond = (!BTreeNode::CTeq(BTreeNode::CTcmp(counter - (depth - level) * Z, Z), -1));
        bool secondCond = BTreeNode::CTeq(BTreeNode::CTcmp(curNode->evictionNode, currentID), 0);
        bool thirdCond = (!BTreeNode::CTeq(BTreeNode::CTcmp(counter, Z * depth), -1)) || curNode->isDummy;
        bool fourthCond = BTreeNode::CTeq((long long) curNode->evictionNode, currentID);

        long long tmpEvictionNode = GetNodeOnPath(currentLeaf, depth - (int) floor(counter / Z));
        long long tmpcurrentID = GetNodeOnPath(currentLeaf, level - 1);
        curNode->evictionNode = BTreeNode::conditional_select((long long) - 1, (long long) curNode->evictionNode, firstCond && secondCond && thirdCond);
        curNode->evictionNode = BTreeNode::conditional_select(tmpEvictionNode, (long long) curNode->evictionNode, firstCond && secondCond && !thirdCond);
        counter = BTreeNode::conditional_select(counter + 1, counter, firstCond && secondCond && !thirdCond);
        counter = BTreeNode::conditional_select(counter + 1, counter, !firstCond && fourthCond);
        level = BTreeNode::conditional_select(level - 1, level, firstCond && !secondCond);
        i = BTreeNode::conditional_select(i - 1, i, firstCond && !secondCond);
        currentID = BTreeNode::conditional_select(tmpcurrentID, currentID, firstCond && !secondCond);

        //        if (firstCond) {
        //            if (secondCond) {
        //                if (thirdCond) {
        //                    long long tmpEvictionNode = GetNodeOnPath(currentLeaf, depth - (int) floor(counter / Z));
        //                    curNode->evictionNode = -1;
        //                    counter = counter;
        //                    level = level;
        //                    i = i;
        //                } else {
        //                    long long tmpEvictionNode = GetNodeOnPath(currentLeaf, depth - (int) floor(counter / Z));
        //                    curNode->evictionNode = tmpEvictionNode;
        //                    counter++;
        //                    level = level;
        //                    i = i;
        //                }
        //            } else {
        //                currentID = GetNodeOnPath(currentLeaf, level - 1);
        //                curNode->evictionNode = curNode->evictionNode;
        //                counter = counter;
        //                level--;
        //                i--;
        //            }
        //
        //        } else if (curNode->evictionNode == currentID) {
        //            long long tmpID = GetNodeOnPath(currentLeaf, level - 1);
        //            curNode->evictionNode = curNode->evictionNode;
        //            counter++;
        //            level = level;
        //            i = i;
        //        } else {
        //            long long tmpID = GetNodeOnPath(currentLeaf, level - 1);
        //            curNode->evictionNode = curNode->evictionNode;
        //            counter = counter;
        //            level = level;
        //            i = i;
        //        }
    }

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Sequential Scan on Stash Blocks to assign blocks to blocks:%f", time);
        ocall_start_timer(10);
    }

    BTreeObliviousOperations::oblixmergesort(&stash.nodes);

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Oblivious Compaction: %f", time);
        ocall_start_timer(10);
    }

    unsigned int j = 0;
    Bucket* bucket = new Bucket();
    for (uint32_t i = 0; i < (depth + 1) * Z; i++) {
        BTreeNode* cureNode = stash.nodes[i];
        long long curBucketID = cureNode->evictionNode;
        Block &curBlock = (*bucket)[j];
        curBlock.data.resize(blockSize, 0);
        block tmp = convertNodeToBlock(cureNode);
        curBlock.id = BTreeNode::conditional_select((unsigned long long) 0, (unsigned long long) cureNode->index, cureNode->isDummy);
        for (size_t k = 0; k < tmp.size(); k++) {
            curBlock.data[k] = BTreeNode::conditional_select(curBlock.data[k], tmp[k], cureNode->isDummy);
        }
        delete cureNode;
        j++;

        if (j == Z) {
            if (virtualStorage.count(curBucketID) != 0) {
                virtualStorage.erase(curBucketID);
            }
            virtualStorage[curBucketID] = (*bucket);
            delete bucket;
            bucket = new Bucket();
            j = 0;
        }
    }
    delete bucket;

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Creating Buckets to write:%f", time);
        ocall_start_timer(10);
    }

    stash.nodes.erase(stash.nodes.begin(), stash.nodes.begin()+((depth + 1) * Z));

    for (unsigned int i = PERMANENT_STASH_SIZE; i < stash.nodes.size(); i++) {
        delete stash.nodes[i];
    }
    stash.nodes.erase(stash.nodes.begin() + PERMANENT_STASH_SIZE, stash.nodes.end());

    nextDummyCounter = INF;

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Padding stash:%f", time);
        ocall_start_timer(10);
    }

    if (evictBucketsForORAM) {
        EvictBuckets();
    }

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Out of SGX memory write:%f", time);
    }

    evictcount++;

    if (profile) {
        ocall_stop_timer(&time, 15);
        evicttime += time;
        logger(DBG, "eviction time:%f", time);
    }
}

void BTreeORAM::start(bool isBatchWrite) {
    this->batchWrite = isBatchWrite;
    readCnt = 0;
    accessCounter = 0;
}

void BTreeORAM::prepareForEvictionTest() {
    uint32_t leaf = 10;
    currentLeaf = leaf;
    BTreeNode* nd = new BTreeNode();
    nd->isDummy = false;
    nd->evictionNode = GetNodeOnPath(leaf, depth);
    nd->index = 1;
    nextDummyCounter++;
    nd->pos = leaf;
    stash.insert(nd);
    for (uint32_t i = 0; i <= Z * depth; i++) {
        BTreeNode* n = new BTreeNode();
        n->isDummy = true;
        n->evictionNode = GetNodeOnPath(leaf, depth);
        n->index = nextDummyCounter;
        nextDummyCounter++;
        n->pos = leaf;
        stash.insert(n);
    }
}

uint32_t BTreeORAM::RandomPath() {
    uint32_t val;
    sgx_read_rand((unsigned char *) &val, 4);
    return val % (maxOfRandom);
}


void BTreeORAM::writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks)
{
    if (useLocalRamStore) {
        for (unsigned int i = 0; i < (*indexes).size(); i++) {
            localStore->Write((*indexes)[i], (*blocks)[i]);
        }
    }
}
