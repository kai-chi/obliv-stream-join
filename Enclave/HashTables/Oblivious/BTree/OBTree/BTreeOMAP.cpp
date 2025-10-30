#include "BTreeOMAP.h"
#include "Enclave.h"
#include "Enclave_t.h"

using namespace std;

BTreeOMAP::BTreeOMAP(int maxSize, bytes<Key> secretKey, int useLocalRamStore, bool isLeft) {
    btreeHandler = new BTree(maxSize, secretKey, true, BTREE_DEGREE, useLocalRamStore, isLeft);
    rootIndex = 0;
    rootPos = 0;
}

BTreeOMAP::BTreeOMAP(int maxSize, bytes<Key> secretKey, map<uint32_t, uint32_t>* pairs,
                     map<unsigned long long, unsigned long long>* permutation, int useLocalRamStore, bool isLeft) {
    (void) (maxSize);
    (void) (secretKey);
    (void) (pairs);
    (void) (permutation);
    (void) (useLocalRamStore);
    (void) (isLeft);
    throw runtime_error("");
//        btreeHandler = new BTree(maxSize, secretKey, true, BTREE_DEGREE);
//        btreeHandler = new AVLTree(maxSize, secretKey, rootKey, rootPos, pairs, permutation);
}

BTreeOMAP::BTreeOMAP(int maxSize, uint32_t init_rootIndex, uint32_t init_rootPos, bytes<Key> secretKey, int useLocalRamStore, bool isLeft){
    btreeHandler = new BTree(maxSize, secretKey, false, BTREE_DEGREE, useLocalRamStore, isLeft);
    this->rootIndex = init_rootIndex;
    this->rootPos = init_rootPos;
}

BTreeOMAP::~BTreeOMAP() {

}

uint32_t BTreeOMAP::search(uint64_t omapKey_ts, uint32_t& index) {
    double y;
    uint32_t res = 0;

    if (btreeHandler->logTime) {
        ocall_start_timer(950);
    }
    if (rootIndex == 0) {
        return res;
    }
    res = UINT32_MAX;
    btreeHandler->startOperation(false);
    BTreeNode* node = new BTreeNode();
    node->index = rootIndex;
    node->pos = rootPos;
    BTreeKeyValuePair *pair = btreeHandler->search2(node, omapKey_ts, (unsigned int) -1, index);
    res = pair->value;
    rootPos = node->pos;
    delete node;
    if (btreeHandler->logTime) {
        ocall_stop_timer(&y, 950);
        //        btreeHandler->times[2].push_back(y);
        ocall_start_timer(950);
    }
    btreeHandler->finishOperation();
#ifdef MEASURE_PERF
    btreeHandler->performance_cycles[0].push_back(btreeHandler->cpu_cycles);
    btreeHandler->performance_cycles[1].push_back(btreeHandler->getOramCycles());
    btreeHandler->performance_cycles[2].push_back(btreeHandler->getOramEvictionCycles());
#endif
    if (btreeHandler->logTime) {
        ocall_stop_timer(&y, 950);
        //        btreeHandler->times[3].push_back(y);
    }
    delete pair;
    return res;
}

BTreeKeyValuePair* BTreeOMAP::searchSucc(uint64_t omapKey) {
    double y;

    if (btreeHandler->logTime) {
        ocall_start_timer(950);
    }
    if (rootIndex == 0) {
        return nullptr;
    }
    btreeHandler->startOperation(false);
    BTreeNode* node = new BTreeNode();
    node->index = rootIndex;
    node->pos = rootPos;
    BTreeKeyValuePair *pair = btreeHandler->searchSucc(node, omapKey);
    rootPos = node->pos;
    delete node;
    if (btreeHandler->logTime) {
        ocall_stop_timer(&y, 950);
//        btreeHandler->times[2].push_back(y);
        ocall_start_timer(950);
    }
    btreeHandler->finishOperation();
    if (btreeHandler->logTime) {
        ocall_stop_timer(&y, 950);
//        btreeHandler->times[3].push_back(y);
    }
    return pair;
}


void BTreeOMAP::insert(uint32_t ts, uint32_t omapKey, uint32_t value) {
    if (btreeHandler->logTime) {
//        btreeHandler->times[0].push_back(0);
    }
    btreeHandler->totheight = 0;
    int height;
    btreeHandler->startOperation(false);
    if (rootIndex == 0) {
        rootIndex = btreeHandler->insert2(0, rootPos, ts, omapKey, value, height, omapKey, false);
    } else {
        rootIndex = btreeHandler->insert2(rootIndex, rootPos, ts, omapKey, value, height, omapKey, false);
    }
    double y;
    if (btreeHandler->logTime) {
        ocall_start_timer(898);
    }
    btreeHandler->finishOperation();
    if (btreeHandler->logTime) {
        ocall_stop_timer(&y, 898);
//        btreeHandler->times[1].push_back(y);
    }
}

void BTreeOMAP::printTree() {
    BTreeNode* node = new BTreeNode();
    node->index = rootIndex;
    node->pos = rootPos;
    if (!rootIndex == 0 && rootIndex != INT_MAX && rootPos != ERROR_POS) {
        btreeHandler->printTree(node);
    } else {
        logger(INFO, "Tree is empty");
    }
}

void BTreeOMAP::removeNO(uint64_t key)
{
    btreeHandler->startOperation(false);

    if (rootIndex == 0) {
        rootIndex = btreeHandler->remove3(0, rootPos, key, false);
    } else {
        rootIndex = btreeHandler->remove3(rootIndex, rootPos, key, false);
    }

    btreeHandler->finishOperation();
}

void BTreeOMAP::remove(uint64_t key)
{
    btreeHandler->startOperation(false);

    if (rootIndex == 0) {
        rootIndex = btreeHandler->removeX(0, rootPos, key, false);
    } else {
        rootIndex = btreeHandler->removeX(rootIndex, rootPos, key, false);
    }

    btreeHandler->finishOperation();
}

void BTreeOMAP::writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks)
{
    btreeHandler->writeToLocalRamStore(indexes, blocks);
}

vector<uint32_t> BTreeOMAP::traverse()
{
    vector<uint32_t> res;
    btreeHandler->traverse(rootIndex, rootPos, &res);
    return res;
}
