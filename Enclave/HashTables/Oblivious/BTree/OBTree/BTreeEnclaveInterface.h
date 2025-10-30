#ifndef BTREEENCLAVEINTERFACE_H
#define BTREEENCLAVEINTERFACE_H

#include "Enclave.h"
#include "Enclave_t.h"
#include "BTreeOMAP.h"
#include <string>
#include "ORAMInit.hpp"
#include <sgx_trts.h>
#include "Config.hpp"

class BTreeEnclaveInterface {
private:
    BTreeOMAP* btreeOMAPLeft = nullptr;
    BTreeOMAP* btreeOMAPRight = nullptr;
public:
    BTreeEnclaveInterface() {}

    virtual ~BTreeEnclaveInterface()
    {
        delete btreeOMAPLeft;
        delete btreeOMAPRight;
    }

    void ecall_btree_setup_oram(int isLeft, int max_size, int useLocalRamStore) {
        bytes<Key> tmpkey{0};
        if (isLeft) {
            btreeOMAPLeft = new BTreeOMAP(max_size, tmpkey, useLocalRamStore, isLeft);
        } else {
            btreeOMAPRight = new BTreeOMAP(max_size, tmpkey, useLocalRamStore, isLeft);
        }
    }

    void ecall_btree_print_tree(int isLeft) {
        logger(INFO,"***** B-Tree *****");
        if (isLeft) {
            if (btreeOMAPLeft != nullptr) {
                btreeOMAPLeft->printTree();
            }
        } else {
            if (btreeOMAPRight != nullptr) {
                btreeOMAPRight->printTree();
            }
        }
        logger(INFO,"******************\n");

    }

    void ecall_btree_setup_omap_by_client(int isLeft, int max_size, uint32_t rootIndex, uint32_t rootPos,const char* secretKey){
        bytes<Key> tmpkey;
        std::memcpy(tmpkey.data(), secretKey, Key);
        if (isLeft) {
            btreeOMAPLeft = new BTreeOMAP(max_size, rootIndex, rootPos, tmpkey, false, isLeft);
        } else {
            btreeOMAPRight = new BTreeOMAP(max_size, rootIndex, rootPos, tmpkey, false, isLeft);
        }
    }



    void ecall_btree_setup_omap_by_client_with_local_ramStore(int isLeft, uint32_t max_size){
        bytes<Key> secretkey{0};
        uint32_t rootIndex;
        uint32_t rootPos;
        map<uint32_t, string> pairs;
        vector<uint32_t> indexes;
        vector<block> blocks;
        size_t blockCount;
        unsigned long long storeBlockSize;

        ORAMInit::initializeORAM(max_size, secretkey, rootIndex, rootPos, &pairs, &indexes, &blocks, false, blockCount, storeBlockSize);

        if (isLeft) {
            btreeOMAPLeft = new BTreeOMAP(max_size, rootIndex, rootPos, secretkey, true, isLeft);
            btreeOMAPLeft->writeToLocalRamStore(&indexes, &blocks);
        } else {
            btreeOMAPRight = new BTreeOMAP(max_size, rootIndex, rootPos, secretkey, true, isLeft);
            btreeOMAPRight->writeToLocalRamStore(&indexes, &blocks);
        }

    }


    uint32_t ecall_btree_read_node(int isLeft, uint32_t key, uint32_t* outIndex) {
        uint32_t idx = 0, val;
        uint64_t key_ts = (uint64_t) key << 32;
        if (isLeft) {
            val = btreeOMAPLeft->search(key_ts, idx);
        } else {
            val = btreeOMAPRight->search(key_ts, idx);
        }
        *outIndex = idx;
        return val;
    }

    uint32_t ecall_btree_read_node_with_ts(int isLeft, uint64_t key, uint32_t* outIndex) {
        uint32_t idx = 0, val;
        if (isLeft) {
            val = btreeOMAPLeft->search(key, idx);
        } else {
            val = btreeOMAPRight->search(key, idx);
        }
        *outIndex = idx;
        return val;
    }

    void ecall_btree_search_succ(int isLeft, uint64_t key, uint64_t* outKey, uint32_t* outValue) {
        BTreeKeyValuePair *tmp = nullptr;
        if (isLeft) {
            tmp = btreeOMAPLeft->searchSucc(key);
        } else {
            tmp = btreeOMAPRight->searchSucc(key);
        }
        if (tmp == nullptr) {
            (*outKey) = 0;
            (*outValue) = 0;
        } else {
            memcpy(outKey, &tmp->key, sizeof(uint64_t));
            memcpy(outValue, &tmp->value, sizeof(uint32_t));
        }
        delete tmp;
    }

    void ecall_btree_write_node(int isLeft, uint32_t ts, uint32_t key, uint32_t value) {
        if (isLeft) {
            btreeOMAPLeft->insert(ts, key, value);
        } else {
            btreeOMAPRight->insert(ts, key, value);
        }

    }

    void ecall_btree_remove_node(int isLeft, uint64_t key) {
        if (isLeft) {
            btreeOMAPLeft->remove(key);
        } else {
            btreeOMAPRight->remove(key);
        }

    }

    void ecall_btree_traverse(int isLeft, uint32_t *arr, size_t len) {
        (void) len;
        vector<uint32_t> vec;
        if (isLeft) {
            vec = btreeOMAPLeft->traverse();
        } else {
            vec = btreeOMAPRight->traverse();
        }

        std::copy(vec.begin(), vec.end(), arr);
    }

};

#endif // BTREEENCLAVEINTERFACE_H