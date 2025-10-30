#include "BTree.h"
#include "BTreeORAM.hpp"
#include "Enclave.h"
#include <sgx_trts.h>
#include "Common.h"
#include <sstream>
#include "../../../../../Enclave/Commons/ECommon.h"

#ifndef BTREE_DEBUG
#ifdef SGX_DEBUG
//#define BTREE_DEBUG
#endif
#endif


BTree::BTree(uint32_t maxSize, bytes<Key> secretKey, bool isEmptyMap, int _degree, int useLocalRamStore, bool isLeft) {
    beginTimestamp = clock_cycles();
    degree = _degree;
    oram = new BTreeORAM(maxSize, secretKey, false, isEmptyMap, degree, useLocalRamStore, isLeft);
    // h_max = ceil(log_d((n+1)/2)
    int depth = max_depth_btree(maxSize, degree);
    maxOfRandom = max_of_random_btree(depth, degree);
    max_depth = max_depth_btree_real(maxSize, degree);
    printer = new BTreePrinter();
    printer->btreeOram = oram;
//    logger(DBG, "[B-TREE] maxOfRandom=%d, maxDepth=%d", maxOfRandom, max_depth);
#ifdef MEASURE_PERF
    performance_cycles.push_back(vector<uint64_t>());
    performance_cycles.push_back(vector<uint64_t>());
    performance_cycles.push_back(vector<uint64_t>());
#endif
}

BTree::~BTree() {
    delete oram;
    delete printer;
}

void BTree::startOperation(bool batchWrite) {
#ifdef MEASURE_PERF
    this->tmp_cpu_cycles = clock_cycles();
    oram->cpu_cycles = 0;
    oram->eviciton_cycles = 0;
#endif
    oram->start(batchWrite);
    totheight = 0;
    exist = false;
}

void BTree::finishOperation() {
#ifdef MEASURE_PERF
    uint64_t c1 = clock_cycles();
#endif
    oram->finilize();
#ifdef MEASURE_PERF
    uint64_t c2 = clock_cycles();
    oram->eviciton_cycles += (c2 - c1);
    printf("ORAM finalize took [cycles]: %lu\n", (c2-c1));
    oram->cpu_cycles += (c2 - c1);
    this->cpu_cycles = (c2 - this->tmp_cpu_cycles);
    printf("This operation took [cycles]: %lu\n", (c2 - this->tmp_cpu_cycles));
#endif
    for (auto item : btreeCache) {
        delete item;
    }
    btreeCache.clear();
}

uint32_t BTree::RandomPath() {
    uint32_t val;
    sgx_read_rand((unsigned char *) &val, 4);
    return val % (maxOfRandom);
}

uint32_t BTree::search3(BTreeNode *rootNode, uint64_t key, uint32_t newRootNodePos, BTreeKeyValuePair lastRes) {
    BTreeNode *tmpDummyNode = new BTreeNode();
    bool found = false;
    tmpDummyNode->isDummy = true;
    uint32_t newP = RandomPath();
    if (newRootNodePos == (uint32_t) -1) {
        newRootNodePos = newP;
    } else {
        newRootNodePos = newRootNodePos;
    }
    uint32_t newChildPos = RandomPath();
    BTreeNode *n = oram->ReadWrite(rootNode->index, rootNode->pos, newRootNodePos, false, newChildPos, 0);
    rootNode->pos = newRootNodePos;
    n->pos = newRootNodePos;
    uint32_t res = 0;
//    for(int i = 0; i < n->value.size(); i++) {
//        res[i] = n->value[i];
//    }
//    return res;
    // Find the first key greater than or equal to k
    size_t i = 0;
    for (size_t j = 0; j < KV_SIZE; j++) {
        if (!found && key > n->getKVPairKey(i) && i < (size_t) n->n) {
            i++;
            found = found;
        } else {
            i = i;
            found = true;
        }
    }

    // If the found key is equal to k, return this node
    if (!n->isDummy && i < (size_t) n->n && (n->getKVPairKey(i) == key)) {
        res = n->getKVPairValue(i);
        delete tmpDummyNode;
        return res;
    }

    // If the found key is same as key store it as lastRes
    if ((n->getKVPairKey(i) >> 32) == (key >> 32)) {
        lastRes.key = n->getKVPairKey(i);
        lastRes.value = n->getKVPairValue(i);
    }

    // If key is not found here and this is a leaf node
    if (n->leaf) {
        delete tmpDummyNode;
        return lastRes.value;
    }



    // Go to the appropriate child
    BTreeNode *child = new BTreeNode();
    child->index = n->getChildIndex(i);
    child->pos = n->getChildPos(i);
    uint32_t s = search3(child, key, newChildPos, lastRes);
    n->CTupdateChildPos(child->index, child->pos);
    newP = RandomPath();
    oram->ReadWrite(n->index, n, n->pos, newP, false, false, false);
    n->pos = newP;
    rootNode->pos = newP;
    delete tmpDummyNode;
    return s;
}

BTreeKeyValuePair *BTree::search2(BTreeNode *rootNode, uint64_t key, uint32_t newRootNodePos, uint32_t& index_counter) {
    (void) (newRootNodePos);
    BTreeNode *tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    bool foundKey = false;
    int upperBound = max_depth;
    uint32_t res = 0;
    int dummyState = 0;
    BTreeNode *head;
    uint32_t lastPos = rootNode->pos;
    uint32_t newPos = RandomPath();
    rootNode->pos = newPos;
    uint32_t curIndex = rootNode->index;
    uint32_t dummyID = oram->nextDummyCounter++;
    BTreeKeyValuePair *lastRes = new BTreeKeyValuePair();

    do {
        uint32_t newP = RandomPath();
        uint32_t newP2 = RandomPath();
        bool isDummyAction = BTreeNode::CTeq(BTreeNode::CTcmp(dummyState, 1), 0);
#ifdef MEASURE_PERF
        uint64_t c = clock_cycles();
#endif
        head = oram->ReadWriteBTree(curIndex, lastPos, newPos, isDummyAction, newP2, key);
#ifdef MEASURE_PERF
        c = clock_cycles() - c;
        oram->cpu_cycles += c;
        printf("ORAM Read took [cycles]: %lu\n", c);
#endif

        // Find the first key greater than or equal to k
        size_t i = 0;
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c2 = BTreeNode::CTeq(BTreeNode::CTcmp(j, head->n), -1); // j < head->n
            bool c3 = BTreeNode::CTeq(BTreeNode::CTcmp(key, head->getKVPairKey(j)), 1); // key > keys[j]
            bool match = BTreeNode::CTeq(BTreeNode::CTcmp(key, head->getKVPairKey(j)), 0); // key == keys[j]
            foundKey = BTreeNode::conditional_select(true, foundKey, match && c2);

            res = BTreeNode::conditional_select(head->getKVPairValue(j), res, !isDummyAction && c2 && match);
            bool c5 = BTreeNode::CTeq(BTreeNode::CTcmp(i, head->n), -1); // i < head->n
            bool c6 = BTreeNode::CTeq(i, j);
            i = BTreeNode::conditional_select(i + 1, i, c3 && c5 && c6);
        }

        for (uint32_t ii = 0; ii < CHILDREN_SIZE; ii++) {
            index_counter = BTreeNode::conditional_select(index_counter + head->getCounter(ii), index_counter, !isDummyAction && (ii < i));
        }
        index_counter = BTreeNode::conditional_select(uint32_t(index_counter + i), index_counter, !isDummyAction);
        // If the found key is same as key store it as lastRes
        // TODO: make oblivious
        if (!isDummyAction && !head->isDummy && (i<(size_t) head->n) && ((head->getKVPairKey(i) >> 32) == (key >> 32))) {
            lastRes->key = head->getKVPairKey(i);
            lastRes->value = head->getKVPairValue(i);
        }
        dummyState = BTreeNode::conditional_select(1, dummyState, foundKey || (!foundKey && head->leaf));

        // Go to the appropriate child
        curIndex = dummyID;
        curIndex = BTreeNode::conditional_select(head->getChildIndex(i), curIndex, !dummyState);
        lastPos = BTreeNode::conditional_select(newP, lastPos, dummyState);
        lastPos = BTreeNode::conditional_select(head->getChildPos(i), lastPos, !dummyState);
        newPos = BTreeNode::conditional_select(newP, newPos, dummyState);
        newPos = BTreeNode::conditional_select(newP2, newPos, !dummyState /*&& !found*/);
        delete head;
    } while (oram->readCnt <= upperBound);

    return lastRes;
}

BTreeKeyValuePair* BTree::searchSucc(BTreeNode *rootNode, uint64_t key) {
    BTreeNode *tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    bool foundKey = false;
    bool foundKeyInHead = false;
    bool foundSucc = false;
    int upperBound = max_depth;
    uint32_t res = 0;
    int dummyState = 0;
    BTreeNode *head;
    uint32_t lastPos = rootNode->pos;
    uint32_t newPos = RandomPath();
    rootNode->pos = newPos;
    uint32_t curIndex = rootNode->index;
    uint32_t dummyID = oram->nextDummyCounter++;
    BTreeKeyValuePair *smallestSucc = new BTreeKeyValuePair();
    smallestSucc->key = key;
    smallestSucc->value = 0;
    uint32_t key_id = (uint32_t) (key >> 32);
    uint32_t key_ts = key & 0xffffffff;

    do {
        uint32_t newP = RandomPath();
        uint32_t newP2 = RandomPath();
        foundSucc = false;
        bool isDummyAction = BTreeNode::CTeq(BTreeNode::CTcmp(dummyState, 1), 0);
        head = oram->ReadWriteBTree(curIndex, lastPos, newPos, isDummyAction, newP2, key+1);
        foundKeyInHead = false;
        // Find the first key greater than or equal to k
        size_t i = 0;
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c2 = BTreeNode::CTeq(BTreeNode::CTcmp(j, head->n), -1); // j < head->n
            bool c3 = BTreeNode::CTeq(BTreeNode::CTcmp(key, head->getKVPairKey(j)), 1); // key > keys[j]
            bool match = BTreeNode::CTeq(BTreeNode::CTcmp(key, head->getKVPairKey(j)), 0); // key == keys[j]
            foundKey = BTreeNode::conditional_select(true, foundKey, match && c2);
            foundKeyInHead = BTreeNode::conditional_select(true, foundKeyInHead, match && c2);

            res = BTreeNode::conditional_select(head->getKVPairValue(j), res, !isDummyAction && c2 && match);
            bool c5 = BTreeNode::CTeq(BTreeNode::CTcmp(i, head->n), -1); // i < head->n
            bool c6 = BTreeNode::CTeq(i, j);
            i = BTreeNode::conditional_select(i + 1, i, c3 && c5 && c6);

            uint32_t head_key_id = (uint32_t) (head->getKVPairKey(j) >> 32);
            uint32_t head_key_ts = head->getKVPairKey(j) & 0xffffffff;
            if (!isDummyAction && !head->isDummy && j < (size_t) head->n && head_key_id==key_id && head_key_ts > key_ts && !foundSucc) {
                foundSucc = true;
                smallestSucc->key = head->getKVPairKey(j);
                smallestSucc->value = head->getKVPairValue(j);
            }
        }
        i = BTreeNode::conditional_select(i + 1, i, foundKeyInHead);
        dummyState = BTreeNode::conditional_select(1, dummyState, head->leaf);

        // Go to the appropriate child
        curIndex = dummyID;
        curIndex = BTreeNode::conditional_select(head->getChildIndex(i), curIndex, !dummyState);
        lastPos = BTreeNode::conditional_select(newP, lastPos, dummyState);
        lastPos = BTreeNode::conditional_select(head->getChildPos(i), lastPos, !dummyState);
        newPos = BTreeNode::conditional_select(newP, newPos, dummyState);
        newPos = BTreeNode::conditional_select(newP2, newPos, !dummyState /*&& !found*/);
        delete head;
    } while (oram->readCnt <= upperBound);

    return smallestSucc;
}


BTreeNode* BTree::newNode() {
    BTreeNode* node = new BTreeNode();
    node->index = index++;
    node->pos = 0;
    node->isDummy = false;
    node->height = 1; // TODO: new node is initially added at leaf?
    node->n = 0;
    node->leaf = true;
    for (uint32_t i = 0; i < KV_SIZE; i++) {
        node->kvPairs[i].key = UINT32_MAX;
        node->kvPairs[i].value = UINT32_MAX;
    }

    for (uint32_t i = 0; i < CHILDREN_SIZE; i++) {
        node->children[i].index = UINT32_MAX;
        node->children[i].oramPos = UINT32_MAX;
    }
    return node;
}

BTreeNode* BTree::newDummyNode() {
    BTreeNode* node = new BTreeNode();
    node->index = oram->nextDummyCounter++;
    node->pos = 0;
    node->isDummy = true;
    node->height = 1; // TODO: new node is initially added at leaf?
    node->n = 0;
    node->leaf = true;
    for (uint32_t i = 0; i < KV_SIZE; i++) {
        node->kvPairs[i].key = UINT32_MAX;
        node->kvPairs[i].value = UINT32_MAX;
    }

    for (uint32_t i = 0; i < CHILDREN_SIZE; i++) {
        node->children[i].index = UINT32_MAX;
        node->children[i].oramPos = UINT32_MAX;
        node->children[i].count = 0;
    }
    return node;
}

uint32_t BTree::insert3(uint32_t rootIndex, uint32_t& rootPos, uint32_t ts, uint32_t key, uint32_t value, int &height, uint32_t lastID, bool isDummyIns) {
    (void) (lastID);
    (void) (isDummyIns);
    (void) (ts);
    uint32_t retIndex;
    uint32_t t = getMicrosSinceStart(beginTimestamp);
    uint64_t insertKey = (uint64_t) key << 32 | t;
//     If tree is empty
    if (rootIndex == 0) {
        BTreeNode *newRoot = newNode();
        newRoot->pos = RandomPath();
        newRoot->setKVPairKey(0, insertKey);
        newRoot->setKVPairValue(0, value);
        newRoot->n = 1;
        newRoot->leaf = true;

        height = newRoot->height;
        rootPos = newRoot->pos;
        oram->ReadWrite(newRoot->index, newRoot, newRoot->pos, newRoot->pos, false, false, false);
        retIndex = newRoot->index;
    }
    else {// if tree is not empty
        uint32_t newP = RandomPath();
        uint32_t newP2 = RandomPath();
        BTreeNode *n = oram->ReadWrite(rootIndex, rootPos, newP, false, newP2, (uint32_t) insertKey);
        rootPos = newP;
        n->pos = newP;
        // If root is full, then tree grows in height
        if (n->n == KV_SIZE) {
            // Allocate memory for new root
            BTreeNode *newRoot = newNode();
            newRoot->leaf = false;

            // Make old root as child of new root
//            s->C[0] = root;
            newRoot->setChildIndex(0, n->index);
            newRoot->setChildPos(0, n->pos);

            // Split the old root and move 1 key to the new root
//            s->splitChild(0, root);

            splitChild(0, newRoot, n);

            // New root has two children now.  Decide which of the
            // two children is going to have new key
            int i = 0;
//            if (s->keys[0] < k)
//                i++;
//            s->C[i]->insertNonFull(k);
            if (newRoot->getKVPairKey(0) < insertKey) {
                i++;
            } else {
                i = i;
            }
            BTreeChild child = newRoot->getChild(i);
            BTreeNode *c = oram->ReadWrite(child.index, child.oramPos, child.oramPos, false, newP2, (uint32_t) insertKey);
            insertNonFull(c, insertKey, value);
            newP = RandomPath();
            oram->ReadWrite(c->index, c, c->pos, newP, false, false, false);
            c->pos = newP;
            newRoot->updateChildPos(c->index, c->pos);
            newP = RandomPath();
            oram->ReadWrite(newRoot->index, newRoot, newRoot->pos, newP, false, false, false);
            newRoot->pos = newP;



            // Change root
//            root = s;
            retIndex = newRoot->index;
            rootPos = newRoot->pos;
        }
        else { // If root is not full, call insertNonFull for root
            insertNonFull(n, insertKey, value);
            retIndex = n->index;
            rootPos = n->pos;
        }
    }
    return retIndex;
}

uint32_t BTree::insert2(uint32_t rootIndex, uint32_t& rootPos, uint32_t ts, uint32_t key, uint32_t value, int &height, uint32_t lastID, bool isDummyIns) {
    (void) (lastID);
    (void) (isDummyIns);
    //    uint32_t t = getMicrosSinceStart(beginTimestamp);
    uint64_t insertKey = (uint64_t) key << 32 | ts;
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("%u, %u, %lu (%u|%u), %u, %d, %u, %d\n", rootIndex, rootPos, insertKey, key, t, value, height, lastID, isDummyIns);
#endif
    uint32_t retIndex = -1;
    totheight++;

    uint32_t newRealIndex = getIndex();
    uint32_t newDummyIndex = oram->nextDummyCounter++;
    uint32_t newP = RandomPath();
    BTreeNode *dummyNode = newDummyNode();
    BTreeNode *newRoot = nullptr, *n = nullptr, *c=nullptr, *newRootNonEmpty;
    //     If tree is empty
    bool c1 = BTreeNode::CTeq(BTreeNode::CTcmp(rootIndex, 0), 0);
#ifdef BTREE_DEBUG
    printf("If tree is empty c1 = %d\n", c1);
#endif
    //create new root
    newRoot = BTreeNode::clone(dummyNode);
    newRoot->index = BTreeNode::conditional_select(newRealIndex, newDummyIndex, c1);
    newRoot->isDummy = BTreeNode::conditional_select(false, true, c1);
    newRoot->pos = BTreeNode::conditional_select(newP, newRoot->pos, c1);
    newRoot->n = BTreeNode::conditional_select(1, newRoot->n, c1);
    newRoot->leaf = BTreeNode::conditional_select(true, newRoot->leaf, c1);
    newRoot->CTupdateKVPair(0, insertKey, value, c1);
    height = BTreeNode::conditional_select(newRoot->height, height, c1);
    // I think I can defer this write
//    oram->ReadWrite(newRoot->index, newRoot, newRoot->pos, newRoot->pos, false, !c1, false);
    rootPos = BTreeNode::conditional_select(newRoot->pos, rootPos, c1);
    retIndex = BTreeNode::conditional_select(newRoot->index, retIndex, c1);

    // if tree is not empty
    newP = RandomPath();
    uint32_t tmpRootIndex = BTreeNode::conditional_select(rootIndex, oram->nextDummyCounter, !c1);
    n = oram->ReadWrite(tmpRootIndex, rootPos, newP, c1, 0, 0);
    n->pos = BTreeNode::conditional_select(newP, n->pos, !c1);
    rootPos = BTreeNode::conditional_select(newP, rootPos, !c1);
    // If root is full, then tree grows in height
    bool c2 = BTreeNode::CTeq(BTreeNode::CTcmp(n->n, KV_SIZE), 0);
    // if the tree is not empty and the root is full
    bool c3 = !c1 && c2;
#ifdef BTREE_DEBUG
    printf("the tree is not empty and the root is full - %d\n", c3);
#endif
    // if the tree is not empty and the root is not full
    bool c4 = !c1 && !c2;
#ifdef BTREE_DEBUG
    printf("the tree is not empty and the root is not full - %d\n", c4);
#endif
    newRootNonEmpty = newDummyNode();
    newRootNonEmpty->index = BTreeNode::conditional_select(newRealIndex, newDummyIndex, c3);
    newRootNonEmpty->isDummy = BTreeNode::conditional_select(false, true, c3);
    newRootNonEmpty->leaf = BTreeNode::conditional_select(false, newRootNonEmpty->leaf, c3);
    // Make old root as child of new root
    newRootNonEmpty->CTupdateChild(0, n->index, n->pos, c3, n->getSubTreeSize());
    // Split the old root and move 1 key to the new root
#ifdef BTREE_DEBUG
    printf("Split the old root and move 1 key to the new root - isDummySplit - %d\n", !c3);
#endif
    splitChild(0, newRootNonEmpty, n, !c3);
    if (c3) {
        newRootNonEmpty->setCounter(0, n->getSubTreeSize());
    }
    // New root has two children now.  Decide which of the
    // two children is going to have new key
    int i = BTreeNode::conditional_select(1, 0, c3 && BTreeNode::CTeq(BTreeNode::CTcmp(newRootNonEmpty->getKVPairKey(0), insertKey), -1));
    BTreeChild child = newRootNonEmpty->CTgetChild(i);
    c = oram->ReadWrite(child.index, child.oramPos, child.oramPos, !c3, 0, 0);

    BTreeNode *tmp = BTreeNode::clone(c);
    // If root is not full, call insertNonFull for root
    BTreeNode::conditional_assign(tmp, n, c4);

#ifdef BTREE_DEBUG
    printf("call insertNonFull - isDummyIns=%d\n", !c3 && !c4);
#endif
    if (c3) {
        newRootNonEmpty->setCounter(i, newRootNonEmpty->getCounter(i)+1);
    }
    insertNonFull(tmp, insertKey, value, !c3 && !c4, (max_depth - totheight));
    //newP = RandomPath();
    //oram->ReadWrite(c->index, c, c->pos, newP, false, false, false);
    //c->pos = newP;
    newRootNonEmpty->CTupdateChildPos(tmp->index, tmp->pos);
    newP = RandomPath();
//    oram->ReadWrite(newRoot->index, newRoot, newRoot->pos, newP, false, !c3, false);
    BTreeNode *tmp2 = new BTreeNode();
    BTreeNode::conditional_assign(tmp2, newRoot, c1);
    BTreeNode::conditional_assign(tmp2, newRootNonEmpty, c3);
    BTreeNode::conditional_assign(tmp2, dummyNode, !c1 && !c3);
    oram->ReadWrite(tmp2->index, tmp2, tmp2->pos, newP, false, !c1 && !c3, false);
    tmp2->pos = newP;
    // Change root
    retIndex = BTreeNode::conditional_select(tmp2->index, retIndex, c1 || c3);
    rootPos = BTreeNode::conditional_select(tmp2->pos, rootPos, c1 || c3);

    // If root is not full, call insertNonFull for root
    retIndex = BTreeNode::conditional_select(tmp->index, retIndex, c4);
    rootPos = BTreeNode::conditional_select(tmp->pos, rootPos, c4);

    delete newRoot;
    delete n;
    delete c;
    delete tmp;
    delete tmp2;
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n\n", __PRETTY_FUNCTION__);
#endif

    return retIndex;
}

// A utility function to insert a new key in this node
// The assumption is, the node must be non-full when this
// function is called
uint32_t BTree::insertNonFull(BTreeNode *node, uint64_t key, uint32_t value)
{
    uint32_t newP = RandomPath();
    // Initialize index as index of rightmost element
    int i = node->n - 1;

    // If this is a leaf node
    if (node->leaf)
    {
        // Append new key and value at the end
//        node->setKVPairKey(i+1, key);
//        node->setKVPairValue(i+1, value);
//        node->n++;
//
//        // sort the keys obliviously
//        node->sortKVPairs();
//
//        oram->ReadWrite(node->key, node, node->pos, newP, false, false, false);
//        node->pos = newP;
//        return node->key;


        // The following loop does two things
        // a) Finds the location of new key to be inserted
        // b) Moves all greater keys to one place ahead
        for (int j = KV_SIZE - 1; j >= 0; j--) {
          bool choice = (j >= 0 && j <= node->n-1 && node->getKVPairKey(j) > key);
          i = choice ? (i-1) : (i);
          int mod_j = positive_modulo(j+1, KV_SIZE);
          node->setKVPairKey(mod_j, BTreeNode::conditional_select(node->getKVPairKey(j), node->getKVPairKey(mod_j), choice));
          node->setKVPairValue(mod_j, BTreeNode::conditional_select(node->getKVPairValue(j), node->getKVPairValue(mod_j), choice));
        }

        // Insert the new key at found location
        for (size_t j = 0; j < KV_SIZE; ++j) {
          int choice = (j == (size_t) i+1);
          node->setKVPairKey(j, BTreeNode::conditional_select(key, node->getKVPairKey(j), choice));
          node->setKVPairValue(j, BTreeNode::conditional_select(value, node->getKVPairValue(j), choice));
        }

        node->n++;
        oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
        node->pos = newP;
        return node->index;
    }
    else // If this node is not leaf
    {
        // Find the child which is going to have the new key
//        while (i >= 0 && keys[i] > k)
//            i--;
        for (int j = KV_SIZE - 1; j >= 0; j--) {
            if (i >= 0 && node->getKVPairKey(i) > key) {
                i--;
            } else {
                i = i;
            }
        }

        // See if the found child is full
//        if (C[i+1]->n == 2*t-1)
//        {
//            // If the child is full, then split it
//            splitChild(i+1, C[i+1]);
//
//            // After split, the middle key of C[i] goes up and
//            // C[i] is splitted into two.  See which of the two
//            // is going to have the new key
//            if (keys[i+1] < k)
//                i++;
//        }
//        C[i+1]->insertNonFull(k);

        auto c = node->getChild(i+1);
        BTreeNode *child = oram->ReadWrite(c.index, c.oramPos, newP, false, RandomPath(), (uint32_t) key);
        child->pos = newP;
        node->CTupdateChildPos(child->index, newP);
        // See if the found child is full
        if (child->n == KV_SIZE) {
            // If the child is full, then split it
            splitChild(i+1, node, child);

            // After split, the middle key of C[i] goes up and
            // C[i] is split into two.  See which of the two
            // is going to have the new key
            if (node->getKVPairKey(i+1) < key) {
                i++;
            } else {
                i = i;
            }
        } else {
            //TODO: add dummy split
        }

        newP = RandomPath();
        BTreeNode *child2 = oram->ReadWrite(node->getChildIndex(i + 1), node->getChildPos(i + 1), newP, false, RandomPath(), (uint32_t) key);
        child2->pos = newP;
        node->CTupdateChildPos(child2->index, newP);
        insertNonFull(child2, key, value);
        node->CTupdateChildPos(child2->index, child2->pos);
        newP = RandomPath();
        oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
        node->pos = newP;
        delete child;
        delete child2;
        return node->index;
    }
}

uint32_t BTree::insertNonFull(BTreeNode *node, uint64_t key, uint32_t value, bool isDummyIns, int iterations_left)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s - %d\n", __PRETTY_FUNCTION__, iterations_left);
    printf("node(%u/%u), %lu, %u, %d, %d\n", node->index, node->pos, key, value, isDummyIns, iterations_left);
#endif

    if (iterations_left <= 0 && isDummyIns) return node->index;

    uint32_t retIndex = node->index;
    uint32_t newP = RandomPath();
    BTreeNode *dummyNode = new BTreeNode();
    dummyNode->isDummy = true;
    dummyNode->index = oram->nextDummyCounter;
    // Initialize index as index of rightmost element
    int i = node->n - 1;

    // If this is a leaf node

    // The following loop does two things
    // a) Finds the location of new key to be inserted
    // b) Moves all greater keys to one place ahead
    for (int j = KV_SIZE - 1; j >= 0; j--) {
        bool choice = (!isDummyIns && node->leaf && j >= 0 && j <= node->n-1 && node->getKVPairKey(j) > key);
        i = choice ? (i-1) : (i);
        int mod_j = positive_modulo(j+1, KV_SIZE);
        node->setKVPairKey(mod_j, BTreeNode::conditional_select(node->getKVPairKey(j), node->getKVPairKey(mod_j), choice));
        node->setKVPairValue(mod_j, BTreeNode::conditional_select(node->getKVPairValue(j), node->getKVPairValue(mod_j), choice));
    }

    // Insert the new key at found location
    for (size_t j = 0; j < KV_SIZE; ++j) {
        bool choice = !isDummyIns && node->leaf && (j == (size_t) i+1);
        node->setKVPairKey(j, BTreeNode::conditional_select(key, node->getKVPairKey(j), choice));
        node->setKVPairValue(j, BTreeNode::conditional_select(value, node->getKVPairValue(j), choice));
    }

    node->n = BTreeNode::conditional_select(node->n + 1, node->n, !isDummyIns && node->leaf);
    bool isDummyOp = isDummyIns || !node->leaf;
    BTreeNode *finalWriteNode = BTreeNode::clone(dummyNode);
    BTreeNode::conditional_assign(finalWriteNode, node, !isDummyOp);
    oram->ReadWrite(finalWriteNode->index, finalWriteNode, finalWriteNode->pos, newP, false, isDummyOp, false);
    node->pos = BTreeNode::conditional_select(node->pos, newP, isDummyOp);
    retIndex = BTreeNode::conditional_select(retIndex, node->index, isDummyOp);

    // If this node is not leaf

    // Find the child which is going to have the new key
    for (int j = KV_SIZE - 1; j >= 0; j--) {
        bool choice = !isDummyIns && !node->leaf && i>=0 && node->getKVPairKey(i) > key;
        i = BTreeNode::conditional_select(i - 1, i, choice);
    }
    // See if the found child is full
//    auto c = node->getChild(i+1);
    isDummyOp = isDummyIns || node->leaf;
    // make sure i is a reasonable number
    i = BTreeNode::conditional_select(0, i, isDummyOp);
    uint32_t cIndex = BTreeNode::conditional_select(oram->nextDummyCounter, node->getChildIndex(i + 1), isDummyOp);
    uint32_t cPos = BTreeNode::conditional_select((uint32_t) 0, node->getChildPos(i + 1), isDummyOp);
    BTreeNode *child = nullptr, *child2 = nullptr;
    newP = RandomPath();
    child = oram->ReadWrite(cIndex, cPos, newP, isDummyOp, RandomPath(), (uint32_t) key);
    child->pos = BTreeNode::conditional_select(child->pos, newP, isDummyOp);
    node->CTupdateChildPos(child->index, child->pos);

    // See if the found child is full
    bool isChildFull = BTreeNode::CTeq(BTreeNode::CTcmp(child->n, KV_SIZE), 0);
    splitChild(i+1, node, child, isDummyIns || child->isDummy || !isChildFull);
    if (!isDummyIns && !child->isDummy && isChildFull) {
        node->setCounter(i+1, child->getSubTreeSize());
    }
    i = BTreeNode::conditional_select(i + 1, i, !isDummyIns && isChildFull && BTreeNode::CTeq(BTreeNode::CTcmp(node->getKVPairKey(i + 1), key), -1));

    // perform updates and clean up
    newP = RandomPath();
    cIndex = BTreeNode::conditional_select(oram->nextDummyCounter, node->getChildIndex(i + 1), isDummyOp);
    cPos = BTreeNode::conditional_select((uint32_t) 0, node->getChildPos(i + 1), isDummyOp);
    child2 = oram->ReadWrite(cIndex, cPos, newP, isDummyOp, 0, (uint32_t) key);
    child2->pos = BTreeNode::conditional_select(child2->pos, newP, isDummyOp);
    node->CTupdateChildPos(child2->index, child2->pos);
    // if we reached a leaf the next iterations should be dummy
    bool nextInsertDummy = isDummyIns || node->leaf;
    if (!nextInsertDummy) {
        node->setCounter(i+1, node->getCounter(i+1)+1);
    }
    insertNonFull(child2, key, value, nextInsertDummy, iterations_left-1);
    node->CTupdateChildPos(child2->index, child2->pos);
    newP = RandomPath();
    BTreeNode::conditional_assign(finalWriteNode, dummyNode, isDummyOp);
    BTreeNode::conditional_assign(finalWriteNode, node, !isDummyOp);
    oram->ReadWrite(finalWriteNode->index, finalWriteNode, finalWriteNode->pos, newP, false, isDummyOp, false);
    node->pos = BTreeNode::conditional_select(node->pos, newP, isDummyOp);
    retIndex = BTreeNode::conditional_select(retIndex, node->index, isDummyOp);
    delete child;
    delete child2;
    delete dummyNode;
    delete finalWriteNode;
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s - %d\n\n", __PRETTY_FUNCTION__, iterations_left);
#endif
    return retIndex;
}

void BTree::splitChild(int i, BTreeNode *parent, BTreeNode *child)
{
    // Create a new node which is going to store (t-1) keys
    // of y
//    BTreeNode *z = new BTreeNode(y->t, y->leaf);
//    z->n = t - 1;

    BTreeNode *newChild = newNode();
    newChild->leaf = child->leaf;
    newChild->n = BTREE_DEGREE - 1;

    // Copy the last (t-1) keys of y to z
//    for (int j = 0; j < t-1; j++)
//        z->keys[j] = y->keys[j+t];
    for (size_t j = 0; j < KV_SIZE; j++) {
        int choice = (j < BTREE_DEGREE - 1);
        newChild->setKVPairKey(j, BTreeNode::conditional_select(child->kvPairs[(j + BTREE_DEGREE) % KV_SIZE].key, UINT64_MAX, choice));
        newChild->setKVPairValue(j, BTreeNode::conditional_select(child->kvPairs[(j + BTREE_DEGREE) % KV_SIZE].value, UINT32_MAX, choice));
    }

    // Copy the last t children of y to z
//    if (y->leaf == false)
//    {
//        for (int j = 0; j < t; j++)
//            z->C[j] = y->C[j+t];
//    }
    for (size_t j = 0; j < CHILDREN_SIZE; j++) {
        bool choice = !child->leaf && (j < BTREE_DEGREE);
        newChild->setChildIndex(j, BTreeNode::conditional_select(child->getChildIndex((j + BTREE_DEGREE) % CHILDREN_SIZE), UINT32_MAX, choice));
        newChild->setChildPos(j, BTreeNode::conditional_select(child->getChildPos((j + BTREE_DEGREE) % CHILDREN_SIZE), UINT32_MAX, choice));
    }

    uint32_t newP = RandomPath();
    oram->ReadWrite(newChild->index, newChild, newChild->pos, newP, false, false, false);
    newChild->pos = newP;

    // Reduce the number of keys in y
//    y->n = t - 1;
    child->n = BTREE_DEGREE - 1;


    // Since this node is going to have a new child,
    // create space of new child
//    for (int j = n; j >= i+1; j--)
//        C[j+1] = C[j];
    for (int j = CHILDREN_SIZE - 1; j >= 0; j--) {
        bool choice = (j <= parent->n) && (j >= i+1);
        size_t index_local = (j+1) % CHILDREN_SIZE;
        parent->setChildIndex(index_local, BTreeNode::conditional_select(parent->getChildIndex(j), parent->getChildIndex(index_local), choice));
        parent->setChildPos(index_local, BTreeNode::conditional_select(parent->getChildPos(j), parent->getChildPos(index_local), choice));
    }

    // Link the new child to this node
//    C[i+1] = z;
    for (size_t j = 0; j < CHILDREN_SIZE; j++) {
        bool choice = (j == (size_t) i + 1);
        parent->setChildIndex(j, BTreeNode::conditional_select(newChild->index, parent->getChildIndex(j), choice));
        parent->setChildPos(j, BTreeNode::conditional_select(newChild->pos, parent->getChildPos(j), choice));
    }

    // A key of y will move to this node. Find the location of
    // new key and move all greater keys one space ahead
//    for (int j = n-1; j >= i; j--)
//        keys[j+1] = keys[j];
    for (int j = KV_SIZE-1; j >= 0; j--) {
        bool choice = (j <= parent->n-1) && (j >= i);
        size_t idx = (uint32_t) (j+1) % KV_SIZE;
        parent->setKVPairKey(idx, BTreeNode::conditional_select(parent->getKVPairKey(j), parent->getKVPairKey(idx), choice));
        parent->setKVPairValue(idx, BTreeNode::conditional_select(parent->getKVPairValue(j), parent->getKVPairValue(idx), choice));
    }

    // Copy the middle key of y to this node
//        keys[i] = y->keys[t-1];
    for (size_t j = 0; j < KV_SIZE; j++) {
        bool choice = (j == (size_t) i);
        parent->setKVPairKey(j, BTreeNode::conditional_select(child->getKVPairKey(BTREE_DEGREE - 1), parent->getKVPairKey(j), choice));
        parent->setKVPairValue(j, BTreeNode::conditional_select(child->getKVPairValue(BTREE_DEGREE - 1), parent->getKVPairValue(j), choice));
    }

    // invalidate keys in child
    for (size_t j = 0; j < KV_SIZE; j++) {
        int choice = (j >= BTREE_DEGREE - 1);
        child->setKVPairKey(j, BTreeNode::conditional_select(UINT64_MAX, child->getKVPairKey(j), (int) choice));
        child->setKVPairValue(j, BTreeNode::conditional_select(UINT32_MAX, child->getKVPairValue(j), choice));
    }

    newP = RandomPath();
    oram->ReadWrite(child->index, child, child->pos, newP, false, false, false);
    child->pos = newP;

    // Update child pos in parent
    parent->CTupdateChildPos(child->index, child->pos);

    // Copy the middle key of y to this node
//    keys[i] = y->keys[t-1];

    // Increment count of keys in this node
//    n = n + 1;
    parent->n = parent->n + 1;

    newP = RandomPath();
    oram->ReadWrite(parent->index, parent, parent->pos, newP, false, false, false);
    parent->pos = newP;
}

void BTree::splitChild(int i, BTreeNode *parent, BTreeNode *child, bool isDummySplit)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("%d, parent(%u/%u), child(%u/%u), %d\n", i, parent->index, parent->pos, child->index, child->pos, isDummySplit);
#endif
    // Create a new node which is going to store (t-1) keys
    // of y
//    BTreeNode *z = new BTreeNode(y->t, y->leaf);
//    z->n = t - 1;
    BTreeNode *dummy = new BTreeNode();
    dummy->isDummy = true;
    dummy->index = oram->nextDummyCounter++;

    BTreeNode *newChild = newDummyNode();
    newChild->leaf = child->leaf;
    newChild->n = BTREE_DEGREE - 1;
    newChild->isDummy = isDummySplit;
    newChild->index = BTreeNode::conditional_select(oram->nextDummyCounter++, getIndex(), isDummySplit);
    newChild->pos = 0;

    // Copy the last (t-1) keys of y to z
//    for (int j = 0; j < t-1; j++)
//        z->keys[j] = y->keys[j+t];
    for (size_t j = 0; j < KV_SIZE; j++) {
        bool choice = (j < BTREE_DEGREE - 1);
        newChild->setKVPairKey(j, BTreeNode::conditional_select(child->kvPairs[(j + BTREE_DEGREE) % KV_SIZE].key, UINT64_MAX, choice));
        newChild->setKVPairValue(j, BTreeNode::conditional_select(child->kvPairs[(j + BTREE_DEGREE) % KV_SIZE].value, UINT32_MAX, choice));
    }

    // Copy the last t children of y to z
//    if (y->leaf == false)
//    {
//        for (int j = 0; j < t; j++)
//            z->C[j] = y->C[j+t];
//    }
    for (size_t j = 0; j < CHILDREN_SIZE; j++) {
        bool choice = !child->leaf && (j < BTREE_DEGREE);
        newChild->setChildIndex(j, BTreeNode::conditional_select(child->getChildIndex((j + BTREE_DEGREE) % CHILDREN_SIZE), UINT32_MAX, choice));
        newChild->setChildPos(j, BTreeNode::conditional_select(child->getChildPos((j + BTREE_DEGREE) % CHILDREN_SIZE), UINT32_MAX, choice));
        newChild->setCounter((uint32_t)j, BTreeNode::conditional_select(child->getCounter((j + BTREE_DEGREE) % CHILDREN_SIZE), (uint32_t)0, choice));
    }
    uint32_t newP = RandomPath();
    oram->ReadWrite(newChild->index, newChild, newChild->pos, newP, false, isDummySplit, false);
    newChild->pos = newP;

    // Reduce the number of keys in y
//    y->n = t - 1;
//    child->n = BTREE_DEGREE - 1;
    child->n = BTreeNode::conditional_select(child->n, BTREE_DEGREE - 1, isDummySplit);


    // Since this node is going to have a new child,
    // create space of new child
//    for (int j = n; j >= i+1; j--)
//        C[j+1] = C[j];
    for (int j = CHILDREN_SIZE - 1; j >= 0; j--) {
        bool choice = !isDummySplit && (j <= parent->n) && (j >= i+1);
        size_t index_local = (j+1) % CHILDREN_SIZE;
        parent->setChildIndex(index_local, BTreeNode::conditional_select(parent->getChildIndex(j), parent->getChildIndex(index_local), choice));
        parent->setChildPos(index_local, BTreeNode::conditional_select(parent->getChildPos(j), parent->getChildPos(index_local), choice));
        parent->setCounter((uint32_t)index_local, BTreeNode::conditional_select(parent->getCounter(j), parent->getCounter(index_local), choice));
    }

    // Link the new child to this node
//    C[i+1] = z;
    for (int j = 0; (uint32_t) j < CHILDREN_SIZE; j++) {
        bool choice = !isDummySplit && (j == i + 1);
        parent->setChildIndex(j, BTreeNode::conditional_select(newChild->index, parent->getChildIndex(j), choice));
        parent->setChildPos(j, BTreeNode::conditional_select(newChild->pos, parent->getChildPos(j), choice));
        parent->setCounter(j, BTreeNode::conditional_select(newChild->getSubTreeSize(), parent->getCounter(j), choice));
    }

    // A key of y will move to this node. Find the location of
    // new key and move all greater keys one space ahead
//    for (int j = n-1; j >= i; j--)
//        keys[j+1] = keys[j];
    for (int j = KV_SIZE-1; j >= 0; j--) {
        bool choice = !isDummySplit && (j <= parent->n-1) && (j >= i);
        size_t index_local = (j+1) % KV_SIZE;
        parent->setKVPairKey(index_local, BTreeNode::conditional_select(parent->getKVPairKey(j), parent->getKVPairKey(index_local), choice));
        parent->setKVPairValue(index_local, BTreeNode::conditional_select(parent->getKVPairValue(j), parent->getKVPairValue(index_local), choice));
    }

    // Copy the middle key of y to this node
//        keys[i] = y->keys[t-1];
    for (int j = 0; (uint32_t) j < KV_SIZE; j++) {
        bool choice = !isDummySplit && (j == i);
        parent->setKVPairKey(j, BTreeNode::conditional_select(child->getKVPairKey(BTREE_DEGREE - 1), parent->getKVPairKey(j), choice));
        parent->setKVPairValue(j, BTreeNode::conditional_select(child->getKVPairValue(BTREE_DEGREE - 1), parent->getKVPairValue(j), choice));
    }

    // invalidate keys in child
    for (int j = 0; (uint32_t) j < KV_SIZE; j++) {
        bool choice = !isDummySplit && (j >= BTREE_DEGREE - 1);
        child->setKVPairKey(j, BTreeNode::conditional_select(UINT64_MAX, child->getKVPairKey(j), choice));
        child->setKVPairValue(j, BTreeNode::conditional_select(UINT32_MAX, child->getKVPairValue(j), choice));
    }

    newP = RandomPath();
    BTreeNode *finalWrite = BTreeNode::clone(dummy);
    BTreeNode::conditional_assign(finalWrite, child, !isDummySplit);
    oram->ReadWrite(finalWrite->index, finalWrite, finalWrite->pos, newP, false, isDummySplit, false);
    child->pos = BTreeNode::conditional_select(child->pos, newP, isDummySplit);

    // Update child pos in parent
    parent->CTupdateChildPos(child->index, child->pos);

    // Copy the middle key of y to this node
//    keys[i] = y->keys[t-1];

    // Increment count of keys in this node
//    n = n + 1;
//    parent->n = parent->n + 1;
    parent->n = BTreeNode::conditional_select(parent->n, parent->n + 1, isDummySplit);

    newP = RandomPath();
    BTreeNode::conditional_assign(finalWrite, parent, !isDummySplit);
    oram->ReadWrite(finalWrite->index, finalWrite, finalWrite->pos, newP, false, isDummySplit, false);
    parent->pos = BTreeNode::conditional_select(parent->pos, newP, isDummySplit);
    delete newChild;
    delete dummy;
    delete finalWrite;
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n\n", __PRETTY_FUNCTION__);
#endif
}


void BTree::printTree(BTreeNode *rt)
{
    printer->print(rt);
}

uint32_t BTree::removeX(uint32_t rootIndex, uint32_t &rootPos, uint64_t key, bool isDummyDel) {

#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("\trootIndex: %u, rootPos: %u, key: %u, isDummyDel: %d\n", rootIndex, rootPos, key>>32, isDummyDel);
#endif
    uint32_t retIndex = rootIndex;
    totheight++;
    uint32_t newP = RandomPath();
    BTreeNode *root = oram->ReadWrite(rootIndex, rootPos, newP, false, 0, (uint32_t) key);
    root->pos = newP;
    rootPos = newP;
    BTreeNode* tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    tmpDummyNode->index = oram->nextDummyCounter++;
    bool remainderIsDummy = false;

//    if (isDummyDel && !CTeq(CTcmp(totheight, max_depth), -1)) {
    if (!CTeq(CTcmp(totheight, max_depth), -1)) {
#ifdef BTREE_DEBUG
        printf("\tFinal if-statement\n");
#endif
        int idx = root->CTfindKey(key);
        // idx < root->n && root->getKVPairKey(idx) == key;
        uint64_t key_tmp, key_this;
        if ((key & 0xffffffff) == 0) {
            key_tmp = key >> 32;
            key_this = root->CTgetKVPairKey((size_t) idx) >> 32;
        } else {
            key_tmp = key;
            key_this = root->CTgetKVPairKey((size_t) idx);
        }
        bool cond = CTeq(CTcmp(idx, root->n), -1) && (CTeq(key_this, key_tmp));
        CTremoveFromLeaf(root, idx, !cond || !root->leaf);
        // If the root node has 0 keys, make its first child as the new root
        //  if it has a child, otherwise set root as NULL
        bool rootEmpty = CTeq(root->n, 0);
        retIndex = conditional_select((uint32_t) 0, retIndex, rootEmpty && root->leaf);
        retIndex = conditional_select(root->getChildIndex(0), retIndex, rootEmpty && !root->leaf);
        rootPos = conditional_select((uint32_t) -1, rootPos, rootEmpty && root->leaf);
        rootPos = conditional_select(root->getChildPos(0), rootPos, rootEmpty && !root->leaf);
        BTreeNode::conditional_assign(root, tmpDummyNode, rootEmpty);
        // Free the old root
        BTreeNode *tmp = nullptr;
        newP = RandomPath();
        if (rootEmpty) {
            tmp = oram->ReadWrite(root->index, tmpDummyNode, root->pos, root->pos, false, false, false);
            root->pos = root->pos;
            rootPos = rootPos;
        } else {
//            tmp = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, tmpDummyNode->pos, tmpDummyNode->pos, false, true, false);
            tmp = oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
            root->pos = newP;
            rootPos = newP;
        }
        delete root;
        delete tmp;
        return retIndex;
    }

    if (!isDummyDel && !root->isDummy && root->index!= 0 && root->index != UINT32_MAX) {
        remainderIsDummy = false;
    } else {
        remainderIsDummy = true;
    }

    size_t idx = (size_t) root->CTfindKey(key);
    uint64_t key_tmp, key_this;
    if ((key & 0xffffffff) == 0) {
        key_tmp = key >> 32;
        key_this = root->CTgetKVPairKey((size_t) idx) >> 32;
    } else {
        key_tmp = key;
        key_this = root->CTgetKVPairKey((size_t) idx);
    }

    bool cond = CTeq(CTcmp((int)idx, root->n), -1) && (CTeq(key_tmp, key_this));
    bool removeFromLeafCase = cond && root->leaf;
    bool removeFromNonLeafCase = cond && !root->leaf;
#ifdef BTREE_DEBUG
    printf("\tremoveFromLeaf: %d\n", removeFromLeafCase);
    printf("\tremoveFromNonLeaf: %d\n", removeFromNonLeafCase);
#endif
    // removeFromNonLeaf
//    uint64_t k = root->CTgetKVPair(idx)->key;
    // If the child that precedes k (C[idx]) has at least t keys,
    // find the predecessor 'pred' of k in the subtree rooted at
    // C[idx]. Replace k by pred. Recursively delete pred
    // in C[idx]
    newP = RandomPath();
    BTreeNode *predChild = nullptr, *predChild2 = nullptr;
    if (root->leaf) {
        predChild = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode->pos, newP, true, 0, 0);
    } else {
        predChild = oram->ReadWrite(root->CTgetChildIndex(idx), root->CTgetChildPos(idx), newP, false, 0, 0);
    }
    predChild->pos = newP;
    root->CTupdateChildPos(predChild->index, predChild->pos);

    newP = RandomPath();
    if (root->leaf || (size_t) root->n <= idx) {
        predChild2 = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode->pos, newP, true, 0, 0);
    } else {
        predChild2 = oram->ReadWrite(root->CTgetChildIndex(idx+1), root->CTgetChildPos(idx+1), newP, false, 0, 0);
    }
    predChild2->pos = newP;
    root->CTupdateChildPos(predChild2->index, predChild2->pos);

    // If the child that precedes k (C[idx]) has atleast t keys,
    // find the predecessor 'pred' of k in the subtree rooted at
    // C[idx]. Replace k by pred. Recursively delete pred
    // in C[idx]
    // if (C[idx]->n >= t)
    bool c1 = (BTreeNode::CTeq(BTreeNode::CTcmp(predChild->n, BTREE_DEGREE), 1) || BTreeNode::CTeq(predChild->n, BTREE_DEGREE));

    // If the child C[idx] has less that t keys, examine C[idx+1].
    // If C[idx+1] has at least t keys, find the successor 'succ' of k in
    // the subtree rooted at C[idx+1]
    // Replace k by succ
    // Recursively delete succ in C[idx+1]
    // else if  (C[idx+1]->n >= t)
    bool c2 = (BTreeNode::CTeq(BTreeNode::CTcmp(predChild2->n, BTREE_DEGREE), 1) || BTreeNode::CTeq(predChild2->n, BTREE_DEGREE));

    // If both C[idx] and C[idx+1] has less that t keys,merge k and all of C[idx+1]
    // into C[idx]
    // Now C[idx] contains 2t-1 keys
    // Free C[idx+1] and recursively delete k from C[idx]
    bool c3 = (!c1 && !c2);

    BTreeNode::conditional_assign(predChild, predChild2, !c1 && c2);

    BTreeKeyValuePair predOrSucc = getPredOrSucc(root, (int) idx, c1, c2, !removeFromNonLeafCase, max_depth - 1, true);
#ifdef BTREE_DEBUG
    printf("predOrSucc: %d.%d / %d\n", predOrSucc.key >> 32, predOrSucc.key & 0xffffffff, predOrSucc.value);
#endif
    // exclude the merge case
    root->CTupdateKVPair(idx, predOrSucc.key, predOrSucc.value, removeFromNonLeafCase && !c3);
    predChild->pos = BTreeNode::conditional_select(root->getChildPos(idx), predChild->pos, removeFromNonLeafCase && c1);
    predChild->pos = BTreeNode::conditional_select(root->getChildPos((idx + 1) % CHILDREN_SIZE), predChild->pos, removeFromNonLeafCase && !c1 && c2);
    //todo: remove, node update child pos, merge branch


    bool case3 = !cond && !root->leaf;

#ifdef BTREE_DEBUG
    printf("Case3 - the key to be removed is not present in this node: %d\n", case3);
#endif
    // The key to be removed is present in the sub-tree rooted with this node
    // The flag indicates whether the key is present in the sub-tree rooted
    // with the last child of this node
    bool flag = CTeq(idx, (size_t) root->n);

    // If the child where the key is supposed to exist has less that t keys,
    // we fill that child
    newP = RandomPath();
    BTreeNode *tmp = oram->ReadWrite(root->getChildIndex(idx), root->getChildPos(idx), newP, !case3, ERROR_POS, 0);
    tmp->pos = newP;
    root->updateChildPos(tmp->index, tmp->pos);
    // fill
    bool fillFlag = CTeq(CTcmp(tmp->n, BTREE_DEGREE), -1);
#ifdef BTREE_DEBUG
     printf("\tfillFlag: %d\n", fillFlag);
#endif
//    if (tmp->n < BTREE_DEGREE) {
//        fill(root, idx);
//    }
    BTreeNode *prevChild = nullptr, *nextChild = nullptr, *dummy1 = nullptr, *dummy2 = nullptr;
    newP = RandomPath();
    if (idx==0 || root->leaf) {
        prevChild = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode->pos, newP, true, 0, 0);
    } else {
        prevChild = oram->ReadWrite(root->getChildIndex(idx-1), root->getChildPos(idx-1), newP, false, 0, 0);
    }

    prevChild->pos = newP;
    root->CTupdateChildPos(prevChild->index, prevChild->pos);
    newP = RandomPath();
    if (idx == (size_t) root->n || root->leaf) {
        nextChild = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode->pos, newP, true, 0, 0);
    } else {
        nextChild = oram->ReadWrite(root->getChildIndex(idx+1), root->getChildPos(idx+1), newP, false, 0, 0);
    }
    nextChild->pos = newP;
    root->CTupdateChildPos(nextChild->index, nextChild->pos);

    bool caseFill1 = !CTeq(idx,(size_t)0) && !CTeq(CTcmp(prevChild->n, BTREE_DEGREE), -1);
    bool caseFill2 = !CTeq(idx,(size_t)root->n) && !CTeq(CTcmp(nextChild->n, BTREE_DEGREE), -1);
    bool caseFill3 = !caseFill1 && !caseFill2;

    if (case3 && fillFlag && caseFill1) {
        borrowFromPrev(root, (int) idx);
    } else if (case3 && fillFlag && caseFill2) {
        borrowFromNext(root, (int) idx);
    } else {
        dummy1 = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, newP, newP, true, true, true);
        dummy2 = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, newP, newP, true, true, true);

        oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, newP, newP, false, true, true);
        oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, newP, newP, false, true, true);
    }
    delete prevChild;
    delete nextChild;
    delete dummy1;
    delete dummy2;

    // Merge for case2 and case3
    // Merge C[idx] with its sibling
    // If C[idx] is the last child, merge it with its previous sibling
    // Otherwise merge it with its next sibling
//        if (idx != node->n)
//            merge(node, idx);
//        else
//            merge(node, idx-1);
    bool m1 = removeFromNonLeafCase && c3;
    bool m2 = case3 && fillFlag && caseFill3;

    size_t merge_idx = idx;
    merge_idx = BTreeNode::conditional_select(idx - 1, merge_idx, m2 && CTeq(idx, (size_t)root->n));
    merge(root, (int) merge_idx, !(m1 || m2));
    // If the last child has been merged, it must have merged with the previous
    // child and so we recurse on the (idx-1)th child. Else, we recurse on the
    // (idx)th child which now has at least t keys
//    int c_idx = (flag && idx > root->n) ? (idx-1) : (idx);
    size_t c_idx = BTreeNode::conditional_select((idx - 1), idx, flag && CTeq(CTcmp(idx, root->n), 1));
    BTreeChild c = root->getChild(c_idx);

//    predChild->pos = Node::conditional_select(root->getChildPos(idx), predChild->pos, case2 && c1);
    predChild->pos = root->CTgetChildPosByIndex(predChild->index);
    newP = RandomPath();
    oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
    root->pos = newP;
    rootPos = newP;

    if (removeFromNonLeafCase) {
        if (m1) {
            removeX(predChild->index, predChild->pos, key, remainderIsDummy);
        } else {
            removeX(predChild->index, predChild->pos, predOrSucc.key, remainderIsDummy);
        }
        root->CTupdateChildPos(predChild->index, predChild->pos);
        root->CTupdateChildCount(predChild->index, int(-1));
    }
    else if (case3) {
        removeX(c.index, c.oramPos, key, remainderIsDummy);
        root->CTupdateChildPos(c.index, c.oramPos);
        root->CTupdateChildCount(c.index, int(-1));
    } else if (removeFromLeafCase) {
        retIndex = removeX(root->index, root->pos, key, false);
        root->CTupdateChildPos(tmpDummyNode->index, tmpDummyNode->pos);
        newP = RandomPath();
        oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, tmpDummyNode->pos, newP, false, true, false);
        rootPos = root->pos;
        root->pos = root->pos;
        retIndex = retIndex;
        retIndex = retIndex;
        rootPos = rootPos;
        rootPos = rootPos;
        // free or update root
        BTreeNode *tmp3 = nullptr;
        if (true) {
            tmp3 = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode, tmpDummyNode->pos, newP, false, true, false);;
        } else {
            tmp3 = oram->ReadWrite(root->index, root, root->pos, root->pos, false, false,false);
        }
        delete root;
        delete tmp;
        delete tmp3;
        return retIndex;
    }
    else {
//        printf("Undefined case I think should not appear here\n");
        removeX(tmpDummyNode->index, tmpDummyNode->pos, key, true);
        root->CTupdateChildPos(tmpDummyNode->index, tmpDummyNode->pos);
    }

    newP = RandomPath();
    oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
    rootPos = newP;
    root->pos = newP;

    // If the root node has 0 keys, make its first child as the new root
    //  if it has a child, otherwise set root as NULL
    bool rootEmpty = CTeq(root->n, 0);
    retIndex = BTreeNode::conditional_select((uint32_t)0, retIndex, rootEmpty && root->leaf);
    retIndex = BTreeNode::conditional_select(root->getChildIndex(0), retIndex, rootEmpty && !root->leaf);
    rootPos = BTreeNode::conditional_select((uint32_t)-1, rootPos, rootEmpty && root->leaf);
    rootPos = BTreeNode::conditional_select(root->getChildPos(0), rootPos, rootEmpty && !root->leaf);
    // free or update root
    BTreeNode *tmp3 = nullptr;
    if (rootEmpty) {
        tmp3 = oram->ReadWrite(root->index, tmpDummyNode, root->pos, root->pos, false, false,false);
    } else {
        tmp3 = oram->ReadWrite(root->index, root, root->pos, root->pos, false, false,false);
    }
    delete root;
    delete tmp;
    delete tmp3;
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n\n", __PRETTY_FUNCTION__);
#endif
    return retIndex;
}

uint32_t BTree::remove2(uint32_t rootIndex, uint32_t &rootPos, uint64_t key, bool isDummyDel)
{
    (void) (isDummyDel);
    uint32_t retKey = rootIndex;
    uint32_t newP = RandomPath();
    BTreeNode* tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    BTreeNode *root = oram->ReadWrite(rootIndex, rootPos, newP, false, 0, (uint32_t) key);
    root->pos = newP;
    rootPos = newP;
    if (root->isDummy || root->index==0 || root->index==UINT32_MAX) {
        printf("The tree is empty\n");
        return retKey;
    }

    // Call the remove function for root
//    root->remove(k);

    int idx = root->CTfindKey(key);
    // The key to be removed is present in this node
    if (idx < root->n && root->getKVPairKey(idx) == key) {
        // If the node is a leaf node - removeFromLeaf is called
        // Otherwise, removeFromNonLeaf function is called
        if (root->leaf) {
            CTremoveFromLeaf(root, idx, false);
        }
        else {
            removeFromNonLeaf(root, idx);
        }
        newP = RandomPath();
        oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
        rootPos = newP;
        root->pos = newP;
    }
    else
    {
        // If this node is a leaf node, then the key is not present in tree
        if (root->leaf)
        {
            printf("The key %ld is does not exist in the tree\n", key);
            return retKey;
        }

        // The key to be removed is present in the sub-tree rooted with this node
        // The flag indicates whether the key is present in the sub-tree rooted
        // with the last child of this node
        bool flag = (idx == root->n);

        // If the child where the key is supposed to exist has less that t keys,
        // we fill that child
        newP = RandomPath();
        BTreeNode *tmp = oram->ReadWrite(root->getChildIndex(idx), root->getChildPos(idx), newP, false, ERROR_POS, 0);
        tmp->pos = newP;
        root->CTupdateChildPos(tmp->index, tmp->pos);
        if (tmp->n < BTREE_DEGREE) {
            fill(root, idx);
        }

        // If the last child has been merged, it must have merged with the previous
        // child and so we recurse on the (idx-1)th child. Else, we recurse on the
        // (idx)th child which now has at least t keys
        int c_idx = (flag && idx > root->n) ? (idx-1) : (idx);
        BTreeChild c = root->getChild(c_idx);
        remove2(c.index, c.oramPos, key);
        root->CTupdateChildPos(c.index, c.oramPos);
        newP = RandomPath();
        oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
        rootPos = newP;
        root->pos = newP;
//        if (flag && idx > root->n)
//            C[idx-1]->remove(k);
//        else
//            C[idx]->remove(k);
    }

    // If the root node has 0 keys, make its first child as the new root
    //  if it has a child, otherwise set root as NULL
    if (root->n==0)
    {
//        BTreeNodeNonOblivious *tmp = root;
        if (root->leaf) {
//            root = NULL;
            retKey = 0;
            rootPos = (uint32_t)-1;
        }
        else {
//            root = root->C[0];
            retKey = root->getChildIndex(0);
            rootPos = root->getChildPos(0);
        }

        // Free the old root
//        delete tmp;
        BTreeNode *tmp = oram->ReadWrite(root->index, tmpDummyNode, root->pos, root->pos, false, false, false);
        delete root;
        delete tmp;
    }
    return retKey;
}


uint32_t BTree::remove3(uint32_t rootIndex, uint32_t &rootPos, uint64_t key, bool isDummyDel)
{
    (void) (isDummyDel);
    uint32_t retKey = rootIndex;
    uint32_t newP = RandomPath();
    BTreeNode* tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    BTreeNode *root = oram->ReadWrite(rootIndex, rootPos, newP, false, 0, (uint32_t) key);
    root->pos = newP;
    rootPos = newP;
    if (root->isDummy || root->index==0 || root->index==UINT32_MAX) {
        printf("The tree is empty\n");
        return retKey;
    }

    // Call the remove function for root
//    root->remove(k);

    int idx = root->findKey(key);

    uint32_t key_v  = (uint32_t) (key>>32);
    uint32_t key_ts = key & 0xffffffff;
    uint64_t key_tmp, key_this;
    if (key_ts == 0) {
        key_tmp = key_v;
        key_this = root->getKVPairKey(idx) >> 32;
    } else {
        key_tmp = key;
        key_this = root->getKVPairKey(idx);
    }
    // The key to be removed is present in this node
    if (idx < root->n && key_this == key_tmp) {
        // If the node is a leaf node - removeFromLeaf is called
        // Otherwise, removeFromNonLeaf function is called
        if (root->leaf) {
            removeFromLeaf(root, idx);
        }
        else {
            removeFromNonLeaf(root, idx);
        }
        newP = RandomPath();
        oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
        rootPos = newP;
        root->pos = newP;
    }
    else
    {
        // If this node is a leaf node, then the key is not present in tree
        if (root->leaf)
        {
            printf("The key %ld.%ld is does not exist in the tree\n", key>>32, key&0xffffffff);
            return retKey;
        }

        // The key to be removed is present in the sub-tree rooted with this node
        // The flag indicates whether the key is present in the sub-tree rooted
        // with the last child of this node
        bool flag = (idx == root->n);

        // If the child where the key is supposed to exist has less that t keys,
        // we fill that child
        newP = RandomPath();
        BTreeNode *tmp = oram->ReadWrite(root->getChildIndex(idx), root->getChildPos(idx), newP, false, ERROR_POS, 0);
        tmp->pos = newP;
        root->updateChildPos(tmp->index, tmp->pos);
        if (tmp->n < BTREE_DEGREE) {
            fill(root, idx);
        }

        // If the last child has been merged, it must have merged with the previous
        // child and so we recurse on the (idx-1)th child. Else, we recurse on the
        // (idx)th child which now has at least t keys
        int c_idx = (flag && idx > root->n) ? (idx-1) : (idx);
        BTreeChild c = root->getChild(c_idx);
        remove3(c.index, c.oramPos, key);
        root->updateChildPos(c.index, c.oramPos);
        newP = RandomPath();
        oram->ReadWrite(root->index, root, root->pos, newP, false, false, false);
        rootPos = newP;
        root->pos = newP;
//        if (flag && idx > root->n)
//            C[idx-1]->remove(k);
//        else
//            C[idx]->remove(k);
    }

    // If the root node has 0 keys, make its first child as the new root
    //  if it has a child, otherwise set root as NULL
    if (root->n==0)
    {
//        BTreeNodeNonOblivious *tmp = root;
        if (root->leaf) {
//            root = NULL;
            retKey = 0;
            rootPos = (uint32_t)-1;
        }
        else {
//            root = root->C[0];
            retKey = root->getChildIndex(0);
            rootPos = root->getChildPos(0);
        }

        // Free the old root
//        delete tmp;
        BTreeNode *tmp = oram->ReadWrite(root->index, tmpDummyNode, root->pos, root->pos, false, false, false);
        delete root;
        delete tmp;
    }
    return retKey;
}

inline int BTree::positive_modulo(int i, int n) {
    return (i % n + n) % n;
}

void BTree::removeFromLeaf(BTreeNode *node, int idx)
{
    // Move all the keys after the idx-th pos one place backward
//    for (int i=idx+1; i<n; ++i)
//        keys[i-1] = keys[i];
    for(int i = 0; (uint32_t) i < KV_SIZE; i++) {
        bool choice = (i >= idx+1 && i < node->n);
        int k = positive_modulo(i-1, KV_SIZE);
        node->setKVPairKey(k, BTreeNode::conditional_select(node->getKVPairKey(i), node->getKVPairKey(k), choice));
        node->setKVPairValue(k, BTreeNode::conditional_select(node->getKVPairValue(i), node->getKVPairValue(k), choice));
    }

    // Reduce the count of keys
    node->n--;
}


void BTree::CTremoveFromLeaf(BTreeNode *node, int idx, bool isDummy)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("\t%u, %d, %d\n", node->index, idx, isDummy);
#endif
    // Move all the keys after the idx-th pos one place backward
//    for (int i=idx+1; i<n; ++i)
//        keys[i-1] = keys[i];
    for(int i = 0; (uint32_t) i < KV_SIZE; i++) {
        bool c1 = BTreeNode::CTeq(BTreeNode::CTcmp(i, idx + 1), -1); // i < idx+1
        bool c2 = BTreeNode::CTeq(BTreeNode::CTcmp(i, node->n), -1); // i < node->n
        bool choice = (!isDummy && !c1 && c2);
        int k = positive_modulo(i-1, KV_SIZE);
        node->setKVPairKey(k, BTreeNode::conditional_select(node->getKVPairKey(i), node->getKVPairKey(k), choice));
        node->setKVPairValue(k, BTreeNode::conditional_select(node->getKVPairValue(i), node->getKVPairValue(k), choice));
    }

    // Reduce the count of keys
    node->n = BTreeNode::conditional_select(node->n - 1, node->n, !isDummy);
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n", __PRETTY_FUNCTION__);
#endif
}

void BTree::removeFromNonLeaf(BTreeNode *node, int idx)
{
    uint64_t k = node->getKVPair(idx)->key;

    // If the child that precedes k (C[idx]) has at least t keys,
    // find the predecessor 'pred' of k in the subtree rooted at
    // C[idx]. Replace k by pred. Recursively delete pred
    // in C[idx]
    uint32_t newP = RandomPath();
    BTreeNode *predChild = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newP, false, 0, 0);
    predChild->pos = newP;
    node->CTupdateChildPos(predChild->index, predChild->pos);

    newP = RandomPath();
    BTreeNode *predChild2 = oram->ReadWrite(node->getChildIndex(idx + 1), node->getChildPos(idx + 1), newP, false, 0, 0);
    predChild2->pos = newP;
    node->CTupdateChildPos(predChild2->index, predChild2->pos);

    if (predChild->n >= BTREE_DEGREE)
    {
        BTreeKeyValuePair pred = getPred(node, idx);
        node->setKVPairKey(idx, pred.key);
        node->setKVPairValue(idx, pred.value);
        predChild->pos = node->getChildPos(idx);
        remove3(predChild->index, predChild->pos, pred.key);
        node->CTupdateChildPos(predChild->index, predChild->pos);
    }

    // If the child C[idx] has less that t keys, examine C[idx+1].
    // If C[idx+1] has atleast t keys, find the successor 'succ' of k in
    // the subtree rooted at C[idx+1]
    // Replace k by succ
    // Recursively delete succ in C[idx+1]
    else if  (predChild2->n >= BTREE_DEGREE)
    {
        BTreeKeyValuePair succ = getSucc(node, idx+1);
        node->setKVPairKey(idx, succ.key);
        node->setKVPairValue(idx, succ.value);
        predChild2->pos = node->getChildPos(idx+1);
        remove3(predChild2->index, predChild2->pos, succ.key);
        node->CTupdateChildPos(predChild2->index, predChild2->pos);
    }
    // If both C[idx] and C[idx+1] has less that t keys,merge k and all of C[idx+1]
    // into C[idx]
    // Now C[idx] contains 2t-1 keys
    // Free C[idx+1] and recursively delete k from C[idx]
    else
    {
        merge(node, idx);
        predChild->pos = node->getChildPos(idx);
        remove3(predChild->index, predChild->pos, k);
        node->CTupdateChildPos(predChild->index, predChild->pos);

    }
}

void BTree::CTremoveFromNonLeaf(BTreeNode *node, int idx, bool isDummy)
{
    (void) (isDummy);
    uint32_t k = (uint32_t) node->CTgetKVPair(idx)->key;


    // If the child that precedes k (C[idx]) has at least t keys,
    // find the predecessor 'pred' of k in the subtree rooted at
    // C[idx]. Replace k by pred. Recursively delete pred
    // in C[idx]
    uint32_t newP = RandomPath();
    BTreeNode *predChild = oram->ReadWrite(node->CTgetChildIndex(idx), node->CTgetChildPos(idx), newP, false, 0, 0);
    predChild->pos = newP;
    node->CTupdateChildPos(predChild->index, predChild->pos);

    newP = RandomPath();
    BTreeNode *predChild2 = oram->ReadWrite(node->CTgetChildIndex(idx + 1), node->CTgetChildPos(idx + 1), newP, false, 0, 0);
    predChild2->pos = newP;
    node->CTupdateChildPos(predChild2->index, predChild2->pos);

    bool c11 = BTreeNode::CTeq(BTreeNode::CTcmp(predChild->n, BTREE_DEGREE), 1);
    bool c12 = BTreeNode::CTeq(predChild->n, BTREE_DEGREE);
    bool c1 = (c11 || c12);

    bool c21 = BTreeNode::CTeq(BTreeNode::CTcmp(predChild2->n, BTREE_DEGREE), 1);
    bool c22 = BTreeNode::CTeq(predChild2->n, BTREE_DEGREE);
    bool c2 = (c21 || c22);

    bool c3 = (!c1 && !c2);
    (void) (c3);

    if (predChild->n >= BTREE_DEGREE)
    {
        BTreeKeyValuePair pred = getPred(node, idx);
        node->setKVPairKey(idx, pred.key);
        node->setKVPairValue(idx, pred.value);
        predChild->pos = node->getChildPos(idx);
        remove2(predChild->index, predChild->pos, pred.key);
        node->CTupdateChildPos(predChild->index, predChild->pos);
    }

        // If the child C[idx] has less that t keys, examine C[idx+1].
        // If C[idx+1] has atleast t keys, find the successor 'succ' of k in
        // the subtree rooted at C[idx+1]
        // Replace k by succ
        // Recursively delete succ in C[idx+1]
    else if  (predChild2->n >= BTREE_DEGREE)
    {
        BTreeKeyValuePair succ = getSucc(node, idx+1);
        node->setKVPairKey(idx, succ.key);
        node->setKVPairValue(idx, succ.value);
        predChild2->pos = node->getChildPos(idx+1);
        remove2(predChild2->index, predChild2->pos, succ.key);
        node->CTupdateChildPos(predChild2->index, predChild2->pos);
    }
        // If both C[idx] and C[idx+1] has less that t keys,merge k and all of C[idx+1]
        // into C[idx]
        // Now C[idx] contains 2t-1 keys
        // Free C[idx+1] and recursively delete k from C[idx]
    else
    {
        merge(node, idx);
        predChild->pos = node->getChildPos(idx);
        remove2(predChild->index, predChild->pos, k);
        node->CTupdateChildPos(predChild->index, predChild->pos);

    }
}


BTreeKeyValuePair BTree::getPred(BTreeNode *node, int idx)
{
    // Keep moving to the right most node until we reach a leaf
//    BTreeNodeNonOblivious *cur=C[idx];
//    while (!cur->leaf)
//        cur = cur->C[cur->n];
    uint32_t newP = RandomPath();
    BTreeNode *cur = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newP, false, 0, 0);
    cur->pos = newP;
    node->updateChildPos(cur->index, cur->pos);
    if (cur->leaf) {
        BTreeKeyValuePair p;
        p.key = cur->getKVPairKey(cur->n-1);
        p.value = cur->getKVPairValue(cur->n-1);
        delete cur;
        return p;
    }

    newP = RandomPath();
    BTreeKeyValuePair pair = getPred(cur, cur->n);
    newP = RandomPath();
    oram->ReadWrite(cur->index, cur, cur->pos, newP, false, false, false);
    cur->pos = newP;
    node->updateChildPos(cur->index, cur->pos);
    newP = RandomPath();
    oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
    node->pos = newP;
    delete cur;
    return pair;
}


BTreeKeyValuePair BTree::getPred(BTreeNode *node, int idx, bool isDummy, int iterations_left)
{
    // Keep moving to the right most node until we reach a leaf
//    BTreeNodeNonOblivious *cur=C[idx];
//    while (!cur->leaf)
//        cur = cur->C[cur->n];
    uint32_t newP = RandomPath();
    BTreeNode *cur = oram->ReadWrite(node->CTgetChildIndex(idx), node->CTgetChildPos(idx), newP,
                                     isDummy, 0, 0);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    if (cur->leaf || iterations_left <= 0) {
        BTreeKeyValuePair p;
        p.key = cur->CTgetKVPairKey(cur->n-1);
        p.value = cur->CTgetKVPairValue(cur->n-1);
        delete cur;
        return p;
    }

    newP = RandomPath();
    BTreeKeyValuePair pair = getPred(cur, cur->n, isDummy, iterations_left--);
    newP = RandomPath();
    oram->ReadWrite(cur->index, cur, cur->pos, newP, false, false, false);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    newP = RandomPath();
    oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
    node->pos = newP;
    delete cur;
    return pair;
}

BTreeKeyValuePair BTree::getSucc(BTreeNode *node, int idx)
{
    // Keep moving the left most node starting from C[idx+1] until we reach a leaf
    uint32_t newP = RandomPath();
    BTreeNode *cur = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newP, false, 0, 0);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    if (cur->leaf) {
        // Return the first key of the leaf
        BTreeKeyValuePair p;
        p.key = cur->getKVPairKey(0);
        p.value = cur->getKVPairValue(0);
        delete cur;
        return p;
    }

    BTreeKeyValuePair pair = getSucc(cur, 0);
    newP = RandomPath();
    oram->ReadWrite(cur->index, cur, cur->pos, newP, false, false, false);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    newP = RandomPath();
    oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
    node->pos = newP;
    delete cur;
    return pair;
}

BTreeKeyValuePair BTree::getSucc(BTreeNode *node, int idx, bool isDummy, int iterations_left)
{
    // Keep moving the left most node starting from C[idx+1] until we reach a leaf
    uint32_t newP = RandomPath();
    BTreeNode *cur = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newP, false, 0, 0);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    if (cur->leaf || iterations_left <= 0) {
        // Return the first key of the leaf
        BTreeKeyValuePair p;
        p.key = cur->getKVPairKey(0);
        p.value = cur->getKVPairValue(0);
        delete cur;
        return p;
    }

    BTreeKeyValuePair pair = getSucc(cur, 0, isDummy, iterations_left--);
    newP = RandomPath();
    oram->ReadWrite(cur->index, cur, cur->pos, newP, false, false, false);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    newP = RandomPath();
    oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
    node->pos = newP;
    delete cur;
    return pair;
}

BTreeKeyValuePair BTree::getPredOrSucc(BTreeNode *node, int idx, bool isPred, bool isSucc, bool isDummy, int iterations_left, bool isFirstIteration) {
    // Keep moving the left most node starting from C[idx+1] until we reach a leaf
    // OR
    // Keep moving to the right most node until we reach a leaf
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("node: %u, idx: %d, isPred: %d, isSucc: %d, isDummy: %d, iter_left: %d\n", node->index, idx, isPred, isSucc, isDummy, iterations_left);
#endif
    uint32_t newP = RandomPath();
    BTreeNode *cur = nullptr;
    BTreeNode *tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    tmpDummyNode->index = oram->nextDummyCounter++;
    idx = BTreeNode::conditional_select(idx + 1, idx, !isPred && isSucc && isFirstIteration);
    if (isDummy || node->leaf || node->isDummy) {
        cur = oram->ReadWrite(tmpDummyNode->index, tmpDummyNode->pos, newP, true, 0, 0);
    } else {
        cur = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newP, false, 0, 0);
    }
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    if (cur->leaf || iterations_left <= 0) {
        // Return the first key of the leaf
        BTreeKeyValuePair p{};
        int i = BTreeNode::conditional_select(cur->n - 1, 0, !isDummy && isPred);
        i = BTreeNode::conditional_select(0, i, !isDummy && !isPred && isSucc);
        BTreeKeyValuePair *tmp = cur->CTgetKVPair((size_t) i);
        p.key = tmp->key;
        p.value = tmp->value;
        delete cur;
#ifdef BTREE_DEBUG
        printf("[BTREE-END] %s\n", __PRETTY_FUNCTION__);
#endif
        return p;
    }

    int tmp_idx = BTreeNode::conditional_select(cur->n, 0, isPred);
    BTreeKeyValuePair pair = getPredOrSucc(cur, tmp_idx, isPred, isSucc, isDummy, --iterations_left, false);
    newP = RandomPath();
    oram->ReadWrite(cur->index, cur, cur->pos, newP, false, false, false);
    cur->pos = newP;
    node->CTupdateChildPos(cur->index, cur->pos);
    newP = RandomPath();
    oram->ReadWrite(node->index, node, node->pos, newP, false, false, false);
    node->pos = newP;
    delete cur;
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n", __PRETTY_FUNCTION__);
#endif
    return pair;
}

void BTree::fill(BTreeNode *node, int idx)
{
    BTreeNode *prevChild = nullptr, *nextChild = nullptr;
    uint32_t newP;
    if (idx != 0) {
        newP = RandomPath();
        prevChild = oram->ReadWrite(node->getChildIndex(idx-1), node->getChildPos(idx-1), newP, false, 0, 0);
        prevChild->pos = newP;
        node->CTupdateChildPos(prevChild->index, prevChild->pos);
    }

    if (idx != node->n) {
        newP = RandomPath();
        nextChild = oram->ReadWrite(node->getChildIndex(idx+1), node->getChildPos(idx+1), newP, false, 0, 0);
        nextChild->pos = newP;
        node->CTupdateChildPos(nextChild->index, nextChild->pos);
    }

    // If the previous child(C[idx-1]) has more than t-1 keys, borrow a key
    // from that child
    if (idx!=0 && prevChild->n >= BTREE_DEGREE) {
        borrowFromPrev(node, idx);
    }
//        // If the next child(C[idx+1]) has more than t-1 keys, borrow a key
//        // from that child
    else if (idx!=node->n && nextChild->n >= BTREE_DEGREE) {
        borrowFromNext(node, idx);
    }
        // Merge C[idx] with its sibling
        // If C[idx] is the last child, merge it with its previous sibling
        // Otherwise merge it with its next sibling
    else
    {
        if (idx != node->n)
            merge(node, idx);
        else
            merge(node, idx-1);
    }

    delete prevChild;
    delete nextChild;
}

void BTree::borrowFromPrev(BTreeNode *node, int idx)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("%u, %d\n", node->index, idx);
#endif
    uint32_t newChildPos = RandomPath();
    uint32_t newSiblingPos = RandomPath();
    BTreeNode *child = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newChildPos, false, 0, node->getChildIndex(idx));
    BTreeNode *sibling = oram->ReadWrite(node->getChildIndex(idx - 1), node->getChildPos(idx - 1), newSiblingPos, false, 0, node->getChildIndex(idx - 1));
    child->pos = newChildPos;
    sibling->pos = newSiblingPos;

    // The last key from C[idx-1] goes up to the parent and key[idx-1]
    // from parent is inserted as the first key in C[idx]. Thus, the  loses
    // sibling one key and child gains one key

    // Moving all key in C[idx] one step ahead
    for (int i = KV_SIZE - 1; i >= 0; --i) {
        bool choice = (i >= 0 && i <= (child->n-1));
        int mod_i = positive_modulo(i+1, KV_SIZE);
        child->setKVPairKey(mod_i, BTreeNode::conditional_select(child->getKVPairKey(i), child->getKVPairKey(mod_i), choice));
        child->setKVPairValue(mod_i, BTreeNode::conditional_select(child->getKVPairValue(i), child->getKVPairValue(mod_i), choice));
    }

    // If C[idx] is not a leaf, move all its child pointers one step ahead
    for (int i = CHILDREN_SIZE - 1; i >= 0; i--) {
        bool choice = (!child->leaf && i >= 0 && i <= child->n);
        int mod_i = positive_modulo(i+1, CHILDREN_SIZE);
        child->setChildIndex(mod_i, BTreeNode::conditional_select(child->getChildIndex(i), child->getChildIndex(mod_i), choice));
        child->setChildPos(mod_i, BTreeNode::conditional_select(child->getChildPos(i), child->getChildPos(mod_i), choice));
        child->setCounter(mod_i, BTreeNode::conditional_select(child->getCounter(i), child->getCounter(mod_i), choice));
    }

    // Setting child's first key equal to keys[idx-1] from the current node
//    child->keys[0] = keys[idx-1];
    for (size_t i = 0; i < KV_SIZE; i++) {
        bool choice = (i == 0);
        child->setKVPairKey(i, BTreeNode::conditional_select(node->getKVPairKey(idx - 1), child->getKVPairKey(i), choice));
        child->setKVPairValue(i, BTreeNode::conditional_select(node->getKVPairValue(idx - 1), child->getKVPairValue(i), choice));
    }

    // Moving sibling's last child as C[idx]'s first child
    for (size_t i = 0; i < CHILDREN_SIZE; i++) {
        bool cond = (!child->leaf && i==0);
        child->setChildIndex(i, BTreeNode::conditional_select(sibling->getChildIndex(sibling->n), child->getChildIndex(i), cond));
        child->setChildPos(i, BTreeNode::conditional_select(sibling->getChildPos(sibling->n), child->getChildPos(i), cond));
        child->setCounter(i, BTreeNode::conditional_select(sibling->getCounter(sibling->n), child->getCounter(i), cond));
    }

    // Moving the key from the sibling to the parent
    // This reduces the number of keys in the sibling
    for (size_t i = 0; i < KV_SIZE; ++i) {
        bool choice = ((int) i == idx-1);
        node->setKVPairKey(i, BTreeNode::conditional_select(sibling->getKVPairKey(sibling->n - 1), node->getKVPairKey(i), choice));
        node->setKVPairValue(i, BTreeNode::conditional_select(sibling->getKVPairValue(sibling->n - 1), node->getKVPairValue(i), choice));
    }

    child->n += 1;
    sibling->n -= 1;

    node->setCounter(idx, child->getSubTreeSize());
    node->setCounter(idx-1, sibling->getSubTreeSize());

    newChildPos = RandomPath();
    newSiblingPos = RandomPath();
    oram->ReadWrite(child->index, child, child->pos, newChildPos, false, false, false);
    child->pos = newChildPos;
    oram->ReadWrite(sibling->index, sibling, sibling->pos, newSiblingPos, false, false, false);
    sibling->pos = newSiblingPos;
    node->CTupdateChildPos(child->index, child->pos);
    node->CTupdateChildPos(sibling->index, sibling->pos);
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n\n", __PRETTY_FUNCTION__);
#endif
}

void BTree::borrowFromNext(BTreeNode *node, int idx)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("%u, %d\n", node->index, idx);
#endif
    uint32_t newChildPos = RandomPath();
    uint32_t newSiblingPos = RandomPath();
    BTreeNode *child = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newChildPos, false, 0, node->getChildIndex(idx));
    BTreeNode *sibling = oram->ReadWrite(node->getChildIndex(idx + 1), node->getChildPos(idx + 1), newSiblingPos, false, 0, node->getChildIndex(idx + 1));

    // keys[idx] is inserted as the last key in C[idx]
//    child->keys[(child->n)] = keys[idx];
    for (size_t i = 0; i < KV_SIZE; ++i) {
      bool choice = (i == (size_t) child->n);
      child->setKVPairKey(i, BTreeNode::conditional_select(node->getKVPairKey(idx), child->getKVPairKey(i), choice));
      child->setKVPairValue(i, BTreeNode::conditional_select(node->getKVPairValue(idx), child->getKVPairValue(i), choice));
    }

    // Sibling's first child is inserted as the last child
    // into C[idx]
    for (size_t i = 0; i < CHILDREN_SIZE; ++i) {
      bool choice = (!(child->leaf) && i == (size_t) ((child->n)+1));
      child->setChildIndex(i, BTreeNode::conditional_select(sibling->getChildIndex(0), child->getChildIndex(i), choice));
      child->setChildPos(i, BTreeNode::conditional_select(sibling->getChildPos(0), child->getChildPos(i), choice));
      child->setCounter(i, BTreeNode::conditional_select(sibling->getCounter(0), child->getCounter(i), choice));
    }

    //The first key from sibling is inserted into keys[idx]
//    keys[idx] = sibling->keys[0];
    for (size_t i = 0; i < KV_SIZE; ++i) {
      bool choice = (i == (size_t) idx);
      node->setKVPairKey(i, BTreeNode::conditional_select(sibling->getKVPairKey(0), node->getKVPairKey(i), choice));
      node->setKVPairValue(i, BTreeNode::conditional_select(sibling->getKVPairValue(0), node->getKVPairValue(i), choice));
    }

    // Moving all keys in sibling one step behind
//    for (int i=1; i<sibling->n; ++i) {
//        sibling->keys[i-1] = sibling->keys[i];
//    }
    for (int i = 0; i < (int) KV_SIZE; ++i) {
      bool choice = (i >= 1 && i < sibling->n);
      auto mod_i = (size_t) positive_modulo(i-1, KV_SIZE);
      sibling->setKVPairKey(mod_i, BTreeNode::conditional_select(sibling->getKVPairKey((size_t)i), sibling->getKVPairKey(mod_i), choice));
      sibling->setKVPairValue(mod_i, BTreeNode::conditional_select(sibling->getKVPairValue((size_t)i), sibling->getKVPairValue(mod_i), choice));
    }

    // Moving the child pointers one step behind
    for (int i = 0; i < (int) CHILDREN_SIZE; ++i) {
      bool choice = (!sibling->leaf && i >= 1 && i <= sibling->n);
      auto mod_i = (size_t) positive_modulo(i-1, CHILDREN_SIZE);
      sibling->setChildIndex(mod_i, BTreeNode::conditional_select(sibling->getChildIndex((size_t)i), sibling->getChildIndex(mod_i), choice));
      sibling->setChildPos(mod_i, BTreeNode::conditional_select(sibling->getChildPos((size_t)i), sibling->getChildPos(mod_i), choice));
      sibling->setCounter(mod_i, BTreeNode::conditional_select(sibling->getCounter((size_t)i), sibling->getCounter(mod_i), choice));
    }

    // Increasing and decreasing the key count of C[idx] and C[idx+1]
    // respectively
    child->n += 1;
    sibling->n -= 1;

    node->setCounter(idx, child->getSubTreeSize());
    node->setCounter(idx+1, sibling->getSubTreeSize());

    newChildPos = RandomPath();
    newSiblingPos = RandomPath();
    oram->ReadWrite(child->index, child, child->pos, newChildPos, false, false, false);
    child->pos = newChildPos;
    oram->ReadWrite(sibling->index, sibling, sibling->pos, newSiblingPos, false, false, false);
    sibling->pos = newSiblingPos;
    node->CTupdateChildPos(child->index, child->pos);
    node->CTupdateChildPos(sibling->index, sibling->pos);
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n\n", __PRETTY_FUNCTION__);
#endif
}

void BTree::merge(BTreeNode *node, int idx)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("%u, %d\n", node->index, idx);
#endif
    uint32_t newChildPos = RandomPath();
    uint32_t newSiblingPos = RandomPath();
    BTreeNode *child = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newChildPos, false, 0, node->getChildIndex(idx));
    BTreeNode *sibling = oram->ReadWrite(node->getChildIndex(idx + 1), node->getChildPos(idx + 1), newSiblingPos, false, 0, node->getChildIndex(idx + 1));
    child->pos = newChildPos;
    sibling->pos = newSiblingPos;

    // Pulling a key from the current node and inserting it into (t-1)th
    // position of C[idx]
    for (size_t i = 0; i < KV_SIZE; ++i) {
      bool choice = (i == BTREE_DEGREE-1);
      child->setKVPairKey(i, BTreeNode::conditional_select(node->getKVPairKey(idx), child->getKVPairKey(i), choice));
      child->setKVPairValue(i, BTreeNode::conditional_select(node->getKVPairValue(idx), child->getKVPairValue(i), choice));
    }

    // Copying the keys from C[idx+1] to C[idx] at the end
//    for (int i=0; i<sibling->n; ++i)
//        child->keys[i+t] = sibling->keys[i];
    for (size_t i = 0; i < KV_SIZE; ++i) {
      bool choice = i < (size_t) sibling->n;
      auto mod_i = (size_t) positive_modulo((int) i+BTREE_DEGREE, KV_SIZE);
      child->setKVPairKey(mod_i, BTreeNode::conditional_select(sibling->getKVPairKey(i), child->getKVPairKey(mod_i), choice));
      child->setKVPairValue(mod_i, BTreeNode::conditional_select(sibling->getKVPairValue(i), child->getKVPairValue(mod_i), choice));
    }

    // Copying the child pointers from C[idx+1] to C[idx]
//    if (!child->leaf)
//    {
//        for(int i=0; i<=sibling->n; ++i)
//            child->C[i+t] = sibling->C[i];
//    }
    for (size_t i = 0; i < CHILDREN_SIZE; ++i) {
      bool choice = (!child->leaf && i <= (size_t) sibling->n);
      auto mod_i = (size_t) positive_modulo((int) i+BTREE_DEGREE, CHILDREN_SIZE);
      child->setChildIndex(mod_i, BTreeNode::conditional_select(sibling->getChildIndex(i), child->getChildIndex(mod_i), choice));
      child->setChildPos(mod_i, BTreeNode::conditional_select(sibling->getChildPos(i), child->getChildPos(mod_i), choice));
      child->setCounter(mod_i, BTreeNode::conditional_select(sibling->getCounter(i), child->getCounter(mod_i), choice));
    }

    // Moving all keys after idx in the current node one step before -
    // to fill the gap created by moving keys[idx] to C[idx]
//    for (int i=idx+1; i<n; ++i)
//        keys[i-1] = keys[i];
    for (size_t i = 0; i < KV_SIZE; ++i) {
      bool choice = (i>= (size_t) idx+1 && i < (size_t) node->n);
      auto mod_i = (size_t) positive_modulo((int) (i-1), KV_SIZE);
      node->setKVPairKey(mod_i, BTreeNode::conditional_select(node->getKVPairKey(i), node->getKVPairKey(mod_i), choice));
      node->setKVPairValue(mod_i, BTreeNode::conditional_select(node->getKVPairValue(i), node->getKVPairValue(mod_i), choice));
    }

    // Moving the child pointers after (idx+1) in the current node one
    // step before
//    for (int i=idx+2; i<=n; ++i)
//        C[i-1] = C[i];
    for (size_t i = 0; i < CHILDREN_SIZE; ++i) {
      bool choice = (i>= (size_t) idx+2 && i<= (size_t) node->n);
      auto mod_i = (size_t) positive_modulo((int) (i-1), CHILDREN_SIZE);
      node->setChildIndex(mod_i, BTreeNode::conditional_select(node->getChildIndex(i), node->getChildIndex(mod_i), choice));
      node->setChildPos(mod_i, BTreeNode::conditional_select(node->getChildPos(i), node->getChildPos(mod_i), choice));
      node->setCounter(mod_i, BTreeNode::conditional_select(node->getCounter(i), node->getCounter(mod_i), choice));
    }

    // Updating the key count of child and the current node
    child->n += sibling->n+1;
    node->n--;

    node->setCounter(idx, child->getSubTreeSize());

    newChildPos = RandomPath();
    oram->ReadWrite(child->index, child, child->pos, newChildPos, false, false, false);
    child->pos = newChildPos;
    node->CTupdateChildPos(child->index, child->pos);


    // Freeing the memory occupied by sibling
    BTreeNode *dummy = new BTreeNode();
    dummy->isDummy = true;
    dummy->index = oram->nextDummyCounter++;

    newSiblingPos = RandomPath();
    oram->ReadWrite(sibling->index, dummy, sibling->pos, newSiblingPos, false, false, false);
    uint32_t max = UINT32_MAX;
    node->CTupdateChild(sibling->index, max, (uint32_t) -1, 0);
    delete(sibling);
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n\n", __PRETTY_FUNCTION__);
#endif
}

void BTree::merge(BTreeNode *node, int idx, bool isDummy)
{
#ifdef BTREE_DEBUG
    printf("[BTREE-START] %s\n", __PRETTY_FUNCTION__);
    printf("node: %u, idx: %d, isDummy: %d\n", node->index, idx, isDummy);
#endif
    BTreeNode *dummy = new BTreeNode();
    dummy->isDummy = true;
    dummy->index = oram->nextDummyCounter++;
    uint32_t newChildPos = RandomPath();
    uint32_t newSiblingPos = RandomPath();
    BTreeNode *child=nullptr, *sibling=nullptr;
    if (!isDummy) {
        child = oram->ReadWrite(node->getChildIndex(idx), node->getChildPos(idx), newChildPos, false, 0, node->getChildIndex(idx));
        sibling = oram->ReadWrite(node->getChildIndex(idx+1), node->getChildPos(idx+1), newSiblingPos, false, 0, node->getChildIndex(idx+1));
    } else {
        child = oram->ReadWrite(dummy->index, dummy->pos, newChildPos, false, 0, dummy->index);
        sibling = oram->ReadWrite(dummy->index, dummy->pos, newSiblingPos, false, 0, dummy->index);
    }
    child->pos = newChildPos;
    sibling->pos = newSiblingPos;
    node->CTupdateChildPos(child->index, child->pos);
    node->CTupdateChildPos(sibling->index, sibling->pos);

    // Pulling a key from the current node and inserting it into (t-1)th
    // position of C[idx]
    for (size_t i = 0; i < KV_SIZE; ++i) {
        bool choice = !isDummy && CTeq(i, (size_t)(BTREE_DEGREE-1));
        child->setKVPairKey(i, BTreeNode::conditional_select(node->getKVPairKey(idx % KV_SIZE), child->getKVPairKey(i), choice));
        child->setKVPairValue(i, BTreeNode::conditional_select(node->getKVPairValue(idx % KV_SIZE), child->getKVPairValue(i), choice));
    }

    // Copying the keys from C[idx+1] to C[idx] at the end
//    for (int i=0; i<sibling->n; ++i)
//        child->keys[i+t] = sibling->keys[i];
    for (size_t i = 0; i < KV_SIZE; ++i) {
//        bool choice = (i >= 0 && i < sibling->n);
        bool choice = !isDummy && !CTeq(CTcmp(i, (size_t)0), -1) &&
                CTeq(CTcmp(i, sibling->n), -1);
        auto mod_i = (size_t) positive_modulo((int) i+BTREE_DEGREE, KV_SIZE);
        child->setKVPairKey(mod_i, BTreeNode::conditional_select(sibling->getKVPairKey(i), child->getKVPairKey(mod_i), choice));
        child->setKVPairValue(mod_i, BTreeNode::conditional_select(sibling->getKVPairValue(i), child->getKVPairValue(mod_i), choice));
    }

    // Copying the child pointers from C[idx+1] to C[idx]
//    if (!child->leaf)
//    {
//        for(int i=0; i<=sibling->n; ++i)
//            child->C[i+t] = sibling->C[i];
//    }
    for (size_t i = 0; i < CHILDREN_SIZE; ++i) {
//        bool choice = (!child->leaf && i >= 0 && i <= sibling->n);
        bool choice = !isDummy && !child->leaf && !CTeq(CTcmp(i, 0), -1) &&
                !CTeq(CTcmp(i, sibling->n), 1);
        auto mod_i = (size_t) positive_modulo((int) i+BTREE_DEGREE, CHILDREN_SIZE);
        child->setChildIndex(mod_i, BTreeNode::conditional_select(sibling->getChildIndex(i), child->getChildIndex(mod_i), choice));
        child->setChildPos(mod_i, BTreeNode::conditional_select(sibling->getChildPos(i), child->getChildPos(mod_i), choice));
        child->setCounter(mod_i, BTreeNode::conditional_select(sibling->getCounter(i), child->getCounter(mod_i), choice));
    }

    // Moving all keys after idx in the current node one step before -
    // to fill the gap created by moving keys[idx] to C[idx]
//    for (int i=idx+1; i<n; ++i)
//        keys[i-1] = keys[i];
    for (size_t i = 0; i < KV_SIZE; ++i) {
//        bool choice = (i>=idx+1 && i < node->n);
        bool choice = !isDummy && !CTeq(CTcmp(i, idx+1), -1) && CTeq(CTcmp(i, node->n), -1);
        auto mod_i = (size_t) positive_modulo((int) (i-1), KV_SIZE);
        node->setKVPairKey(mod_i, BTreeNode::conditional_select(node->getKVPairKey(i), node->getKVPairKey(mod_i), choice));
        node->setKVPairValue(mod_i, BTreeNode::conditional_select(node->getKVPairValue(i), node->getKVPairValue(mod_i), choice));
    }

    // Moving the child pointers after (idx+1) in the current node one
    // step before
//    for (int i=idx+2; i<=n; ++i)
//        C[i-1] = C[i];
    for (size_t i = 0; i < CHILDREN_SIZE; ++i) {
//        bool choice = (i>=idx+2 && i<=node->n);
        bool choice = !isDummy && !CTeq(CTcmp(i, idx+2), -1) && !CTeq(CTcmp(i, node->n), 1);
        auto mod_i = (size_t) positive_modulo((int) (i-1), CHILDREN_SIZE);
        node->setChildIndex(mod_i, BTreeNode::conditional_select(node->getChildIndex(i), node->getChildIndex(mod_i), choice));
        node->setChildPos(mod_i, BTreeNode::conditional_select(node->getChildPos(i), node->getChildPos(mod_i), choice));
        node->setCounter(mod_i, BTreeNode::conditional_select(node->getCounter(i), node->getCounter(mod_i), choice));
    }

    // Updating the key count of child and the current node
//    child->n += sibling->n+1;
//    node->n--;
    child->n = BTreeNode::conditional_select(child->n, child->n + sibling->n + 1, isDummy);
    node->n = BTreeNode::conditional_select(node->n, node->n - 1, isDummy);

    if (!isDummy) {
        node->setCounter(idx, child->getSubTreeSize());
    }


    newChildPos = RandomPath();
    BTreeNode *tmp = new BTreeNode();
    tmp->isDummy = true;
    tmp->index = oram->nextDummyCounter++;
    BTreeNode::conditional_assign(tmp, child, !isDummy);
    oram->ReadWrite(tmp->index, tmp, tmp->pos, newChildPos, false, isDummy, false);
    tmp->pos = newChildPos;
    node->CTupdateChildPos(tmp->index, tmp->pos);


    // Freeing the memory occupied by sibling
    newSiblingPos = RandomPath();
    if (isDummy) {
        oram->ReadWrite(dummy->index, dummy, dummy->pos, newSiblingPos, false, true, false);
        node->CTupdateChild(dummy->index, 0, 0, 0);
    } else {
        oram->ReadWrite(sibling->index, dummy, sibling->pos, newSiblingPos, false, false, false);
        node->CTupdateChild(sibling->index, UINT32_MAX, (uint32_t) -1, 0);
    }
    delete(sibling);
    delete tmp;
#ifdef BTREE_DEBUG
    printf("[BTREE-END] %s\n", __PRETTY_FUNCTION__);
#endif
}


uint32_t BTree::getIndex()
{
    return index++;
}

void BTree::writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks)
{
    oram->writeToLocalRamStore(indexes, blocks);
}

uint64_t BTree::getOramCycles()
{
    return oram->cpu_cycles;
}

uint64_t BTree::getOramEvictionCycles()
{
    return oram->eviciton_cycles;
}

BTreeNode *BTree::readWriteCacheNode(uint32_t inputIndex, BTreeNode *inputnode, bool isRead, bool isDummy)
{
    BTreeNode* tmpWrite = BTreeNode::clone(inputnode);


    BTreeNode* res = new BTreeNode();
    res->isDummy = true;
    res->index = oram->nextDummyCounter++;
    bool write = !isRead;


    for (BTreeNode* node : btreeCache) {
        bool match = BTreeNode::CTeq(node->index, inputIndex) && !node->isDummy;


        node->isDummy = BTreeNode::conditional_select(true, node->isDummy, !isDummy && match && write);
        bool choice = !isDummy && match && isRead && !node->isDummy;
        res->index = BTreeNode::conditional_select(node->index, res->index, choice);
        res->isDummy = BTreeNode::conditional_select(node->isDummy, res->isDummy, choice);
        res->pos = BTreeNode::conditional_select(node->pos, res->pos, choice);
        res->evictionNode = BTreeNode::conditional_select(node->evictionNode, res->evictionNode, choice);
        res->modified = BTreeNode::conditional_select(true, res->modified, choice);
        res->height = BTreeNode::conditional_select(node->height, res->height, choice);
        res->n = BTreeNode::conditional_select(node->n, res->n, choice);
        res->leaf = BTreeNode::conditional_select(node->leaf, res->leaf, choice);
        for (size_t k = 0; k < KV_SIZE; k++) {
            res->kvPairs[k].key = BTreeNode::conditional_select(node->kvPairs[k].key, res->kvPairs[k].key, choice);
            res->kvPairs[k].value = BTreeNode::conditional_select(node->kvPairs[k].value , res->kvPairs[k].value , choice);
        }


        for (size_t k = 0; k < CHILDREN_SIZE; k++) {
            res->children[k].index = BTreeNode::conditional_select(node->children[k].index, res->children[k].index, choice);
            res->children[k].oramPos = BTreeNode::conditional_select(node->children[k].oramPos, res->children[k].oramPos, choice);
        }
    }


    btreeCache.push_back(tmpWrite);
    return res;
}

void BTree::traverse(uint32_t rt_index, uint32_t rt_pos, vector<uint32_t> *res)
{
    BTreeNode* tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    if (rt_index == 0) {
        logger(INFO, "Traverse: BTree is empty");
        return;
    }
    BTreeNode* root = oram->ReadWriteTest(rt_index, tmpDummyNode, rt_pos, rt_pos, true, false, true);
    unsigned int i;
    for (i = 0; i < (unsigned int) root->n; i++) {
        res->push_back((uint32_t) (root->getKVPairKey(i) >> 32));
    }

    for (i = 0; i < (unsigned int) root->n; i++)
    {
        // If this is not leaf, then before printing key[i],
        // traverse the subtree rooted with child C[i].
        if (root->leaf == false)
            traverse(root->getChildIndex(i), root->getChildPos(i), res);
    }

    // Print the subtree rooted with last child
    if (root->leaf == false)
        traverse(root->getChildIndex(i), root->getChildPos(i), res);

    return;
}


void BTree::BTreePrinter::visit (BTreeNode *node, unsigned level, unsigned child_index)
{
    if (level >= levels.size())
        levels.resize(level + 1);

    LevelInfo &level_info = levels[level];
    NodeInfo info;

    info.text_pos = 0;
    if (!level_info.empty())  // one blank between nodes, one extra blank if left-most child
        info.text_pos = level_info.back().text_end + (child_index == 0 ? 2 : 1);

    BTreeNode* tmpDummyNode = new BTreeNode();
    tmpDummyNode->isDummy = true;
    BTreeNode* root = btreeOram->ReadWriteTest(node->index, tmpDummyNode, node->pos, node->pos, true, false, true);
    if (root->isDummy || root->isZero()) {
        logger(WARN, "Error reading real node");
    }
    info.text = node_text(root);
    info.text_counters = node_counters_text(root);

    if (root->leaf)
    {
        info.text_end = info.text_pos + unsigned(info.text.length());
    }
    else // non-leaf -> do all children so that .text_end for the right-most child becomes known
    {
        for (unsigned i = 0, e = unsigned(root->n); i <= e; ++i) {// one more pointer than there are keys
            BTreeNode *c = new BTreeNode();
            c->index = root->getChildIndex(i);
            c->pos = root->getChildPos(i);
            visit(c, level + 1, i);
        }

        info.text_end = levels[level + 1].back().text_end;
    }

    levels[level].push_back(info);
    delete tmpDummyNode;
    delete root;
    delete node;
}

std::string BTree::BTreePrinter::node_text (BTreeNode *node)
{
    std::ostringstream os;

#ifdef SGX_DEBUG
    os << "(" << to_string(node->index) << "/" << to_string(node->pos) << ")";
#endif
    os << "[";
    for (int i = 0; i < node->n; i++) {
        os << " " << to_string(node->getKVPairKey(i)>>32) << "." << to_string(node->getKVPairKey(i) & 0xffffffff);
//        os << ".";
//        os << to_string(node->getKVPairKey(i) & 0xffffffff);
    }
    os << " ]";

    return os.str();
}

std::string BTree::BTreePrinter::node_counters_text (BTreeNode *node)
{
    std::ostringstream os;
    char const *sep = "";

    os << "(" << to_string(node->getSubTreeSize()) << ")(";
    for (int i = 0; i < node->n+1; ++i, sep = " ")
        os << sep << to_string(node->children.at(i).count);
    os << ")";

    return os.str();
}

void BTree::BTreePrinter::print_blanks (unsigned n)
{
    while (n--)
        printf(" %s", "");
}

void BTree::BTreePrinter::after_traversal ()
{
    for (std::size_t l = 0, level_count = levels.size(); ; )
    {
        auto const &level = levels[l];
        unsigned prev_end = 0;

        for (auto const &node: level)
        {
            unsigned total = node.text_end - node.text_pos;
            unsigned slack = total - unsigned(node.text.length());
            unsigned blanks_before = node.text_pos - prev_end;

            print_blanks(blanks_before + slack / 2);
            printf("%s", node.text.c_str());
            printf("%s", node.text_counters.c_str());

            if (&node == &level.back())
                break;

            print_blanks(slack - slack / 2);

            prev_end += blanks_before + total;
        }

        if (++l == level_count)
            break;

        printf("\n\n%s", "");
    }

    printf("\n%s", "");
}