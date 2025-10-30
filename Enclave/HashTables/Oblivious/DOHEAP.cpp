#include "DOHEAP.hpp"
#include <algorithm>
#include <iomanip>
#include <fstream>
#include <random>
#include <cmath>
#include <cassert>
#include <cstring>
#include <map>
#include <stdexcept>
#include "sgx_trts.h"
#include "HeapObliviousOperations.h"
#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include <algorithm>
#include <stdlib.h>
#include <vector>
#include "../../DOHEAPEnclaveInterface.hpp"

DOHEAP::DOHEAP(uint32_t maxSize, bytes<Key> oram_key, bool simulation, int _isLeft, int _useLocalRamStore)
: key(oram_key), useLocalRamStore(_useLocalRamStore), isLeft(_isLeft) {
//    logger(DBG, "***** Construct DOHEAP *****");
    (void) (simulation);
    depth = (int) (ceil(log2(maxSize)) - 1) + 1;
    maxOfRandom = (long long) (pow(2, depth));
//    logger(DBG, "Number of leaves:%lld", maxOfRandom);
//    logger(DBG, "depth:%lld", depth);
    AES::Setup();
    bucketCount = (long long) maxOfRandom * 2 - 1;
    INF = 9223372036854775807 - (bucketCount);
    PERMANENT_STASH_SIZE = 90;
    stash.preAllocate(PERMANENT_STASH_SIZE * 4);

    nextDummyCounter = INF;
    blockSize = sizeof (HeapNode); // B    
//    logger(DBG, "block size:%d", blockSize);
    size_t blockCount = (size_t) (Z * bucketCount);
    storeBlockSize = (size_t) (IV + AES::GetCiphertextLength((int) ((Z + 1) * (blockSize))));
    clen_size = AES::GetCiphertextLength((int) (blockSize) * (Z + 1));
    plaintext_size = (blockSize) * (Z + 1);
    if (useLocalRamStore) {
        localStore = new LocalRAMStore(blockCount, plaintext_size);
    } else {
        ocall_setup_heapStore(isLeft, blockCount, storeBlockSize);
    }

    maxHeightOfAVLTree = (int) floor(log2(blockCount)) + 1;
    times.push_back(vector<double>());
    times.push_back(vector<double>());
    times.push_back(vector<double>());
    times.push_back(vector<double>());
    times.push_back(vector<double>());

//    logger(DBG, "Initializing DOHEAP Buckets");
    HeapBucket bucket;
    for (uint32_t z = 0; z < Z; z++) {
        bucket.blocks[z].id = 0;
        bucket.blocks[z].data.resize(blockSize, 0);
    }
    bucket.subtree_min.id = 0;
    bucket.subtree_min.data.resize(blockSize, 0);
    //        InitializeBuckets(0, bucketCount, bucket);
    long long i;
    for (i = 0; i < maxOfRandom - 1; i++) {
//        if (i % 10000 == 0) {
//            logger(DBG, "%d/%d", i, bucketCount);
//        }
        HeapBucket hp;
        for (uint32_t z = 0; z < Z; z++) {
            hp.blocks[z].id = 0;
            hp.blocks[z].data.resize(blockSize, 0);
        }
        hp.subtree_min.id = 0;
        hp.subtree_min.data.resize(blockSize, 0);
        WriteBucket((int) i, hp);
    }
    for (long long j = 0; i < bucketCount; i++, j++) {
//        if (i % 10000 == 0) {
//            logger(DBG, "%d/%d", i, bucketCount);
//        }
        HeapBucket hp;

        hp.blocks[0].id = j;
        HeapNode* tmp = new HeapNode();
        tmp->index = 0;
        tmp->pos = j;
        hp.blocks[0].data = convertNodeToBlock(tmp);
        delete tmp;
        for (uint32_t z = 1; z < Z; z++) {
            hp.blocks[z].id = 0;
            hp.blocks[z].data.resize(blockSize, 0);
        }
        hp.subtree_min.id = 0;
        hp.subtree_min.data.resize(blockSize, 0);
        WriteBucket((long long) i, hp);
    }
    for (unsigned int j = 0; j < PERMANENT_STASH_SIZE; j++) {
        HeapNode* dummy = new HeapNode();
        dummy->index = nextDummyCounter;
        dummy->evictionNode = -1;
        dummy->isDummy = true;
        stash.insert(dummy);
        nextDummyCounter++;
    }
//    logger(DBG, "End of DOHEAP Initialization");
}

DOHEAP::~DOHEAP() {
    AES::Cleanup();
}

// Fetches the array index a bucket that lise on a specific path

void DOHEAP::WriteBucket(long long index, HeapBucket bucket) {
    block b = SerialiseBucket(bucket);
    if (useLocalRamStore) {
        localStore->Write(index, b);
    } else {
        block ciphertext = AES::Encrypt(key, b, clen_size, plaintext_size);
        ocall_write_heapStore(isLeft, index, (const char*) ciphertext.data(), (size_t) ciphertext.size());
    }
}

long long DOHEAP::GetNodeOnPath(long long leaf, int curDepth) {
    leaf += bucketCount / 2;
    for (int d = depth - 1; d >= 0; d--) {
        bool cond = !HeapNode::CTeq(HeapNode::CTcmp(d, curDepth), -1);
        leaf = HeapNode::conditional_select((leaf + 1) / 2 - 1, leaf, cond);
    }
    return leaf;
}

// Write bucket to a single block

block DOHEAP::SerialiseBucket(HeapBucket bucket) {
    block buffer;
    for (uint32_t z = 0; z < Z; z++) {
        HeapBlock b = bucket.blocks[z];
        buffer.insert(buffer.end(), b.data.begin(), b.data.end());
    }
    HeapBlock b = bucket.subtree_min;
    buffer.insert(buffer.end(), b.data.begin(), b.data.end());
    return buffer;
}

HeapBucket DOHEAP::DeserialiseBucket(block buffer) {
    HeapBucket bucket;
    for (uint32_t z = 0; z < Z; z++) {
        HeapBlock &curBlock = bucket.blocks[z];
        curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
        HeapNode* node = convertBlockToNode(curBlock.data);
        bool cond = HeapNode::CTeq(node->index, (unsigned long long) 0);
        node->index = HeapNode::conditional_select(node->index, nextDummyCounter, !cond);
        for (uint32_t k = 0; k < node->value.size(); k++) {
            node->value[k] = HeapNode::conditional_select(node->value[k], (byte_t) 0, !cond);
        }
        for (uint32_t k = 0; k < node->key.id.size(); k++) {
            node->key.id[k] = HeapNode::conditional_select(node->key.id[k], (byte_t) 0, !cond);
        }
        node->isDummy = HeapNode::conditional_select(0, 1, !cond);
        stash.insert(node);
        buffer.erase(buffer.begin(), buffer.begin() + blockSize);
    }
    HeapBlock &curBlock = bucket.subtree_min;
    curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
    return bucket;
}

vector<HeapBucket> DOHEAP::ReadBuckets(vector<long long> indexes) {
    vector<HeapBucket> res;
    if (indexes.size() == 0) {
        return res;
    }
    size_t readSize;

    if (useLocalRamStore) {
        for (unsigned int i = 0; i < indexes.size(); i++) {
            block buffer = localStore->Read(indexes[i]);
            HeapBucket bucket = DeserialiseBucket(buffer);
            virtualStorage[indexes[i]] = bucket;
            res.push_back(bucket);
        }
    } else {
        char* tmp = new char[indexes.size() * storeBlockSize];
        ocall_nread_heapStore(&readSize, isLeft, indexes.size(), indexes.data(), tmp, indexes.size() * storeBlockSize);
        for (unsigned int i = 0; i < indexes.size(); i++) {
            block ciphertext(tmp + i*readSize, tmp + (i + 1) * readSize);
            block buffer = AES::Decrypt(key, ciphertext, clen_size);
            HeapBucket bucket = DeserialiseBucket(buffer);
            res.push_back(bucket);
            virtualStorage[indexes[i]] = bucket;
        }
        delete[] tmp;
    }
    return res;
}

void DOHEAP::EvictBuckets() {
    unordered_map<long long, HeapBucket>::iterator it = virtualStorage.begin();
    if (useLocalRamStore) {
        for (auto item : virtualStorage) {
            block b = SerialiseBucket(item.second);
//            block ciphertext = AES::Encrypt(key, b, clen_size, plaintext_size);
            localStore->Write(item.first, b);
        }
    } else {
        for (unsigned int j = 0; j <= virtualStorage.size() / 10000; j++) {
            char* tmp = new char[10000 * storeBlockSize];
            vector<long long> indexes;
            size_t cipherSize = 0;
            for (int i = 0; i < min((int) (virtualStorage.size() - j * 10000), 10000); i++) {
                block b = SerialiseBucket(it->second);
                indexes.push_back(it->first);
                block ciphertext = AES::Encrypt(key, b, clen_size, plaintext_size);
                std::memcpy(tmp + i * ciphertext.size(), ciphertext.data(), ciphertext.size());
                cipherSize = ciphertext.size();
                it++;
            }
            if (min((int) (virtualStorage.size() - j * 10000), 10000) != 0) {
                ocall_nwrite_heapStore(isLeft, min((int) (virtualStorage.size() - j * 10000), 10000), indexes.data(), (const char*) tmp, cipherSize * min((int) (virtualStorage.size() - j * 10000), 10000));
            }
            delete tmp;
            indexes.clear();
        }
    }
    virtualStorage.clear();
}
// Fetches blocks along a path, adding them to the stash

void DOHEAP::FetchPath(long long leaf) {
    readCnt++;
    vector<long long> nodesIndex;
    vector<long long> existingIndexes;

    long long node = leaf;

    node += bucketCount / 2;
    if (virtualStorage.count(node) == 0) {
        nodesIndex.push_back(node);
    } else {
        existingIndexes.push_back(node);
    }

    for (int d = depth - 1; d >= 0; d--) {
        node = (node + 1) / 2 - 1;
        if (virtualStorage.count(node) == 0) {
            nodesIndex.push_back(node);
        } else {
            existingIndexes.push_back(node);
        }
    }

    ReadBuckets(nodesIndex);

    for (unsigned int i = 0; i < existingIndexes.size(); i++) {
        HeapBucket bucket = virtualStorage[existingIndexes[i]];
        for (uint32_t z = 0; z < Z; z++) {
            HeapBlock &curBlock = bucket.blocks[z];
            HeapNode* node_local = convertBlockToNode(curBlock.data);
            bool cond = HeapNode::CTeq(node_local->index, (unsigned long long) 0);
            node_local->index = HeapNode::conditional_select(node_local->index, nextDummyCounter, !cond);
            node_local->isDummy = HeapNode::conditional_select(0, 1, !cond);
            stash.insert(node_local);
        }
    }
}

void DOHEAP::UpdateMin() {
    vector<long long> nodesIndex;

    long long node = currentLeaf;
    node += bucketCount / 2;
    for (int d = depth - 1; d >= 0; d--) {
        long long bucketID = node;
        if (virtualStorage.count(bucketID) == 0) {
            nodesIndex.push_back(bucketID);
        }
        if (bucketID % 2 == 0) {
            bucketID--;
        } else {
            bucketID++;
        }
        if (virtualStorage.count(bucketID) == 0) {
            nodesIndex.push_back(bucketID);
        }
        node = (node + 1) / 2 - 1;
    }

    if (virtualStorage.count(0) == 0) {
        nodesIndex.push_back(0);
    }
    if (nodesIndex.size() > 0) {
        if (useLocalRamStore) {
            for (unsigned int i = 0; i < nodesIndex.size(); i++) {
                block buffer = localStore->Read(nodesIndex.data()[i]);
                HeapBucket bucket;
                for (uint32_t z = 0; z < Z; z++) {
                    HeapBlock &curBlock = bucket.blocks[z];
                    curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
                    buffer.erase(buffer.begin(), buffer.begin() + blockSize);
                }
                HeapBlock &curBlock = bucket.subtree_min;
                curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
                virtualStorage[nodesIndex[i]] = bucket;
            }
        } else {
            size_t readSize;
            char* tmp = new char[nodesIndex.size() * storeBlockSize];
            ocall_nread_heapStore(&readSize, isLeft, nodesIndex.size(), nodesIndex.data(), tmp, nodesIndex.size() * storeBlockSize);
            for (unsigned int i = 0; i < nodesIndex.size(); i++) {
                block ciphertext(tmp + i*readSize, tmp + (i + 1) * readSize);
                block buffer = AES::Decrypt(key, ciphertext, clen_size);
                HeapBucket bucket;
                for (uint32_t z = 0; z < Z; z++) {
                    HeapBlock &curBlock = bucket.blocks[z];
                    curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
                    buffer.erase(buffer.begin(), buffer.begin() + blockSize);
                }
                HeapBlock &curBlock = bucket.subtree_min;
                curBlock.data.assign(buffer.begin(), buffer.begin() + blockSize);
                virtualStorage[nodesIndex[i]] = bucket;
            }
            delete[] tmp;
        }

    }

    node = currentLeaf;
    node += bucketCount / 2;
    for (int d = depth; d >= 0; d--) {
        HeapBucket& curBucket = virtualStorage[node];
        HeapNode localMin;
        unsigned long long localID = 0;
        localMin.key.setInfinity();
        localMin.isDummy = true;
        for (uint32_t i = 0; i < Z; i++) {
            HeapNode* node_local = convertBlockToNode(curBucket.blocks[i].data);
            bool cond = Bid::CTeq(1, Bid::CTcmp(localMin.key, node_local->key)) && !node_local->isDummy;
            HeapNode::conditional_assign(&localMin, node_local, cond);
            localID = HeapNode::conditional_select(localID, curBucket.blocks[i].id, cond);
            delete node_local;
        }
        if (d != depth) {
            HeapBucket leftBucket = virtualStorage[((node + 1)*2) - 1];
            HeapNode* leftnode = convertBlockToNode(leftBucket.subtree_min.data);
            bool cond = Bid::CTeq(1, Bid::CTcmp(localMin.key, leftnode->key)) && !leftnode->isDummy;
            HeapNode::conditional_assign(&localMin, leftnode, cond);
            localID = HeapNode::conditional_select(localID, leftBucket.subtree_min.id, cond);
            delete leftnode;

            HeapBucket rightBucket = virtualStorage[((node + 1)*2)];
            HeapNode* rightnode = convertBlockToNode(rightBucket.subtree_min.data);
            cond = Bid::CTeq(1, Bid::CTcmp(localMin.key, rightnode->key)) && !rightnode->isDummy;
            HeapNode::conditional_assign(&localMin, rightnode, cond);
            localID = HeapNode::conditional_select(localID, rightBucket.subtree_min.id, cond);
            delete rightnode;
        }

        curBucket.subtree_min.id = localID;
        block tmp = convertNodeToBlock(&localMin);
        for (size_t k = 0; k < tmp.size(); k++) {
            curBucket.subtree_min.data[k] = HeapNode::conditional_select(curBucket.subtree_min.data[k], tmp[k], localMin.isDummy);
        }

        node = (node + 1) / 2 - 1;
    }
}

/**
 * 
 * @param k k[0] is least significant byte and k[16] is the most significant byte
 * @param v v[0] is least significant byte and v[16] is the most significant byte
 */
void DOHEAP::insert(Bid k, array<byte_t, 16> v) {
    HeapNode* node = new HeapNode();
    node->pos = RandomPath();
    node->key = k;
    node->value = v;
    node->index = 1;
    node->isDummy = false;
    stash.insert(node);
    currentLeaf = RandomPath() / 2;
    FetchPath(currentLeaf);
    evict(true);
    currentLeaf = RandomPath() / 2 + (maxOfRandom / 2);
    FetchPath(currentLeaf);
    evict(true);
    EvictBuckets();
}

void DOHEAP::dummy() {
    currentLeaf = RandomPath() / 2;
    FetchPath(currentLeaf);
    evict(true);
    currentLeaf = RandomPath() / 2 + (maxOfRandom / 2);
    FetchPath(currentLeaf);
    evict(true);
    EvictBuckets();
}

/**
 * @param OP:1 extract-min  2:insert    3: dummy
 * @return 
 */
pair<Bid,array<byte_t, 16> > DOHEAP::execute(Bid k, array<byte_t, 16> v, int op) {
    pair<Bid,array<byte_t, 16> > res;
    HeapNode* node = new HeapNode();
    node->pos = RandomPath();
    Bid dummyKey;
    dummyKey.setInfinity();
    bool isInsert = HeapNode::CTeq(op, 2);
    bool isExtract = HeapNode::CTeq(op, 1);
    node->key = Bid::conditional_select(k, dummyKey, isInsert);
    node->value = v;
    node->index = HeapNode::conditional_select(1, 0, isInsert);
    node->isDummy = !isInsert;
    stash.insert(node);

    array<byte_t, 16> result;
    HeapBlock curBlock;
    if (virtualStorage.count(0) == 0) {
        vector<long long> nodesIndex;
        nodesIndex.push_back(0);

        if (useLocalRamStore) {
            size_t readSize;
            char* tmp = new char[nodesIndex.size() * plaintext_size];
            for (unsigned int i = 0; i < nodesIndex.size(); i++) {
                block c = localStore->Read(nodesIndex[i]);
                size_t resLen = c.size();
                std::memcpy(tmp + i*resLen, c.data(), c.size());
                readSize = resLen;
            }
            block buffer(tmp, tmp + readSize);
            curBlock.data.assign(buffer.begin() + blockSize*Z, buffer.begin() + blockSize * (Z + 1));
            delete[] tmp;
        } else {
            size_t readSize;
            char* tmp = new char[nodesIndex.size() * storeBlockSize];
            ocall_nread_heapStore(&readSize, isLeft, nodesIndex.size(), nodesIndex.data(),
                                  tmp, nodesIndex.size() * storeBlockSize);
            block ciphertext(tmp, tmp + readSize);
            block buffer = AES::Decrypt(key, ciphertext, clen_size);
            curBlock.data.assign(buffer.begin() + blockSize*Z, buffer.begin() + blockSize * (Z + 1));
            delete[] tmp;
        }

    } else {
        curBlock.data.assign(virtualStorage[0].subtree_min.data.begin(), virtualStorage[0].subtree_min.data.end());
    }
    HeapNode* rootnode = convertBlockToNode(curBlock.data);
    HeapNode* minnode = new HeapNode();
    HeapNode::conditional_assign(minnode,rootnode,true);
    bool isInStash=false;
    
    for (HeapNode* node_local : stash.nodes) {
        isInStash = HeapNode::CTeq(-1, Bid::CTcmp(node_local->key, minnode->key)) && isExtract && !node_local->isDummy;
        HeapNode::conditional_assign(minnode,node_local,isInStash);
    }  
    
    for (uint32_t i = 0; i < minnode->value.size(); i++) {
        result[i] = minnode->value[i];
    }
    res.second = result;
    res.first = minnode->key;

    currentLeaf = RandomPath() / 2;
    currentLeaf = HeapNode::conditional_select(minnode->pos, (unsigned long long) currentLeaf, isExtract);

    FetchPath(currentLeaf);
    for (HeapNode* n : stash.nodes) {
        bool choice = HeapNode::CTeq(0, Bid::CTcmp(n->key, minnode->key)) && isExtract && HeapNode::CTeq(0, Bid::CTcmp(n->value, minnode->value));
        n->isDummy = HeapNode::conditional_select(true, n->isDummy, choice);
        n->index = HeapNode::conditional_select((unsigned long long) 0, n->index, choice);
    }
    evict(true);
    currentLeaf = RandomPath() / 2 + (maxOfRandom / 2);
    FetchPath(currentLeaf);
    evict(true);
    EvictBuckets();
    delete minnode;
    delete rootnode;
    return res;
}

HeapNode* DOHEAP::convertBlockToNode(block b) {
    HeapNode* node = new HeapNode();
    std::array<byte_t, sizeof (HeapNode) > arr;
    std::copy(b.begin(), b.begin() + sizeof (HeapNode), arr.begin());
    from_bytes(arr, *node);
    node->isDummy = HeapNode::CTeq(node->index, (unsigned long long) 0);
    return node;
}

block DOHEAP::convertNodeToBlock(HeapNode* node) {
    std::array<byte_t, sizeof (HeapNode) > data = to_bytes(*node);
    block b(data.begin(), data.end());
    return b;
}

void DOHEAP::evict(bool evictBuckets) {
    (void) (evictBuckets);
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


    for (HeapNode* node : stash.nodes) {
        long long xorVal = 0;
        xorVal = HeapNode::conditional_select((unsigned long long) 0, node->pos ^ currentLeaf, node->isDummy);
        long long indx = 0;

        indx = (long long) floor(log2(HeapNode::conditional_select(xorVal, (long long) 1, HeapNode::CTcmp(xorVal, 0))));
        indx = indx + HeapNode::conditional_select(1, 0, HeapNode::CTcmp(xorVal, 0));

        for (size_t i = 0; i < firstIndexes.size(); i++) {
            bool choice = HeapNode::CTeq(i, indx);
//            long long value = firstIndexes[i];
            node->evictionNode = HeapNode::conditional_select(firstIndexes[indx], node->evictionNode, !node->isDummy && choice);
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
            HeapNode* dummy = new HeapNode();
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
        logger(DBG, "Creating Dummy Blocks for each HeapBucket:%f", time);
        ocall_start_timer(10);
    }

    if (beginProfile) {
        ocall_start_timer(10);
    }

    HeapObliviousOperations::oblixmergesort(&stash.nodes);

    if (beginProfile) {
        ocall_stop_timer(&time, 10);
        times[1].push_back(time);
        ocall_start_timer(10);
    }

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "First Oblivious Sort: %f", time);
        ocall_start_timer(10);
    }

    long long currentID = currentLeaf + (bucketCount / 2);
    int level = depth;
    int counter = 0;

    for (unsigned long long i = 0; i < stash.nodes.size(); i++) {
        HeapNode* curNode = stash.nodes[i];
        bool firstCond = (!HeapNode::CTeq(HeapNode::CTcmp(counter - (depth - level) * Z, Z), -1));
        bool secondCond = HeapNode::CTeq(HeapNode::CTcmp(curNode->evictionNode, currentID), 0);
        bool thirdCond = (!HeapNode::CTeq(HeapNode::CTcmp(counter, Z * depth), -1)) || curNode->isDummy;
        bool fourthCond = HeapNode::CTeq(curNode->evictionNode, currentID);

        long long tmpEvictionNode = GetNodeOnPath(currentLeaf, depth - (int) floor(counter / Z));
        long long tmpcurrentID = GetNodeOnPath(currentLeaf, level - 1);
        curNode->evictionNode = HeapNode::conditional_select((long long) - 1, curNode->evictionNode, firstCond && secondCond && thirdCond);
        curNode->evictionNode = HeapNode::conditional_select(tmpEvictionNode, curNode->evictionNode, firstCond && secondCond && !thirdCond);
        counter = HeapNode::conditional_select(counter + 1, counter, firstCond && secondCond && !thirdCond);
        counter = HeapNode::conditional_select(counter + 1, counter, !firstCond && fourthCond);
        level = HeapNode::conditional_select(level - 1, level, firstCond && !secondCond);
        i = HeapNode::conditional_select(i - 1, i, firstCond && !secondCond);
        currentID = HeapNode::conditional_select(tmpcurrentID, currentID, firstCond&&!secondCond);
    }

    if (beginProfile) {
        ocall_stop_timer(&time, 10);
        times[2].push_back(time);
    }

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Sequential Scan on Stash Blocks to assign blocks to blocks:%f", time);
        ocall_start_timer(10);
    }

    //    HeapObliviousOperations::compaction(&stash.nodes);
    HeapObliviousOperations::oblixmergesort(&stash.nodes);

    if (profile) {
        ocall_stop_timer(&time, 10);
        logger(DBG, "Oblivious Compaction: %f", time);
        ocall_start_timer(10);
    }

    unsigned int j = 0;
    HeapBucket* bucket = new HeapBucket();
    bucket->subtree_min.id = 0;
    bucket->subtree_min.data.resize(blockSize, 0);
    for (unsigned int i = 0; i < (depth + 1) * Z; i++) {
        HeapNode* cureNode = stash.nodes[i];
        long long curBucketID = cureNode->evictionNode;
        HeapBlock &curBlock = (*bucket).blocks[j];
        curBlock.data.resize(blockSize, 0);
        block tmp = convertNodeToBlock(cureNode);
        curBlock.id = HeapNode::conditional_select((unsigned long long) 0, cureNode->index, cureNode->isDummy);
        for (uint32_t k = 0; k < tmp.size(); k++) {
            curBlock.data[k] = HeapNode::conditional_select(curBlock.data[k], tmp[k], cureNode->isDummy);
        }
        delete cureNode;
        j++;

        if (j == Z) {
            virtualStorage.erase(curBucketID);
            virtualStorage[curBucketID] = (*bucket);
            delete bucket;
            bucket = new HeapBucket();
            bucket->subtree_min.id = 0;
            bucket->subtree_min.data.resize(blockSize, 0);
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

    if (beginProfile) {
        ocall_start_timer(10);
    }

    UpdateMin();

    if (beginProfile) {
        ocall_stop_timer(&time, 10);
        times[3].push_back(time);
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

    if (beginProfile) {
        ocall_stop_timer(&time, 15);
        times[4].push_back(time);
    }
}

void DOHEAP::start(bool isBatchWrite) {
    this->batchWrite = isBatchWrite;
    readCnt = 0;
}

unsigned long long DOHEAP::RandomPath() {
    uint32_t val;
    sgx_read_rand((unsigned char *) &val, 4);
    return val % (maxOfRandom);
}
