#ifndef ORAMINIT_HPP
#define ORAMINIT_HPP

#include "BTreeNode.h"
#include "Common.h"

void bitonicSort(vector<BTreeNode*>* nodes, int step);
void bitonic_sort(vector<BTreeNode*>* nodes, int low, int n, int dir, int step);
void bitonic_merge(vector<BTreeNode*>* nodes, int low, int n, int dir, int step);
void compare_and_swap(BTreeNode* item_i, BTreeNode* item_j, int dir, int step);
int greatest_power_of_two_less_than(int n);

class ORAMInit {
private:
    static void bitonic_sort(vector<BTreeNode*>* nodes, int low, int n, int dir, int step) {
        if (n > 1) {
            int middle = n / 2;
            bitonic_sort(nodes, low, middle, !dir, step);
            bitonic_sort(nodes, low + middle, n - middle, dir, step);
            bitonic_merge(nodes, low, n, dir, step);
        }
    }

    static void bitonic_merge(vector<BTreeNode*>* nodes, int low, int n, int dir, int step) {
        if (n > 1) {
            int m = greatest_power_of_two_less_than(n);

            for (int i = low; i < (low + n - m); i++) {
                if (i != (i + m)) {
                    compare_and_swap((*nodes)[i], (*nodes)[i + m], dir, step);
                }
            }

            bitonic_merge(nodes, low, m, dir, step);
            bitonic_merge(nodes, low + m, n - m, dir, step);
        }
    }

    static void compare_and_swap(BTreeNode* item_i, BTreeNode* item_j, int dir, int step) {
        if (step == 1) {
            if (dir == (item_i->index > item_j->index ? 1 : 0)) {
                std::swap(*item_i, *item_j);
            }
        } else {
            if (dir == (item_i->evictionNode > item_j->evictionNode ? 1 : 0)) {
                std::swap(*item_i, *item_j);
            }
        }
    }

    static int greatest_power_of_two_less_than(int n) {
        int k = 1;
        while (k > 0 && k < n) {
            k = k << 1;
        }
        return k >> 1;
    }

    static int sortedArrayToBST(vector<BTreeNode*>* nodes, long long start, long long end, uint32_t& pos, uint32_t& node,
                         uint32_t& permutationIterator, map<uint32_t,uint32_t>& permutation) {
        if (start > end) {
            pos = -1;
            node = 0;
            return 0;
        }

        unsigned long long mid = (start + end) / 2;
        BTreeNode *root = (*nodes)[mid];

//    int leftHeight = sortedArrayToBST(nodes, start, mid - 1, root->leftPos, root->leftID);

//    int rightHeight = sortedArrayToBST(nodes, mid + 1, end, root->rightPos, root->rightID);
        root->pos = (uint32_t) permutation[permutationIterator];
        permutationIterator++;
//    root->height = max(leftHeight, rightHeight) + 1;
        pos = root->pos;
        node = root->index;
        return root->height;
    }

public:
    static void bitonicSort(vector<BTreeNode*>* nodes, int step) {
        int len = (int) nodes->size();
        bitonic_sort(nodes, 0, len, 1, step);
    }

    static void initializeORAM(long long maxSize, bytes<Key> secretkey, uint32_t& rootKey, uint32_t& rootPos, map<uint32_t, string>* pairs, vector<uint32_t>* indexes, vector<block>* blocks, bool encryptBlocks, size_t& blockCount, unsigned long long& storeBlockSize) {
#ifdef BTREE
        uint32_t depth = (uint32_t) max_depth_btree(maxSize, BTREE_DEGREE);
        uint32_t maxOfRandom = (uint32_t) max_of_random_btree((int)depth, BTREE_DEGREE);
#else
        int depth = (int) (ceil(log2(maxSize)) - 1) + 1;
        int maxOfRandom = (long long) (pow(2, depth));
#endif

        uint32_t permutationIterator = 0;
        uint32_t indx = 2147483647;// = 1;
        map<uint32_t, uint32_t> permutation;
        unsigned long long blockSize = sizeof (BTreeNode); // B

        logger(DBG, "InitORAM ** maxSize: %llu, degree: %d, number of leaves: %d, depth: %d, blocksize: %llu bytes", maxSize, BTREE_DEGREE, maxOfRandom, depth, blockSize);
        int j = 0;
        int cnt = 0;
        for (uint32_t i = 0; i < maxOfRandom * 4; i++) {
            if (cnt == 4) {
                j++;
                cnt = 0;
            }
            permutation[i] = (j + 1) % maxOfRandom;
            cnt++;
        }



        vector<BTreeNode*> nodes;
        for (auto pair : (*pairs)) {
            BTreeNode* node = new BTreeNode();
            node->index = (uint32_t) indx++;
            node->pos = 0;
            node->isDummy = false;
            node->height = 1; // new node is initially added at leaf
            node->n = 0;
            node->leaf = true;
            for (size_t i = 0; i < KV_SIZE; i++) {
                node->kvPairs[i].key = UINT32_MAX;
                node->kvPairs[i].value = UINT32_MAX;
            }

            for (size_t i = 0; i < CHILDREN_SIZE; i++) {
                node->children[i].index = UINT32_MAX;
                node->children[i].oramPos = UINT32_MAX;
            }
            nodes.push_back(node);
        }

        bitonicSort(&nodes, 1);
//        logger(INFO, "Creating BST of %zu Nodes", nodes.size());
        sortedArrayToBST(&nodes, 0, nodes.size() - 1, rootPos, rootKey, permutationIterator, permutation);

        for (size_t i = nodes.size(); i < maxOfRandom * Z; i++) {
            BTreeNode* node = new BTreeNode();
            node->index = (uint32_t) indx++;
            node->pos = 0;
            node->isDummy = false;
            node->height = 1; // new node is initially added at leaf
            node->isDummy = true;
            node->pos = permutation[permutationIterator];
            node->n = 0;
            node->leaf = true;
            node->n = 0;
            node->leaf = true;
            for (size_t k = 0; k < KV_SIZE; k++) {
                node->kvPairs[k].key = UINT32_MAX;
                node->kvPairs[k].value = UINT32_MAX;
            }

            for (size_t k = 0; k < CHILDREN_SIZE; k++) {
                node->children[k].index = UINT32_MAX;
                node->children[k].oramPos = UINT32_MAX;
            }
            permutationIterator++;
            nodes.push_back(node);
        }

        //----------------------------------------------------------------
        AES::Setup();
        uint32_t bucketCount = (uint32_t) maxOfRandom * 2 - 1;
//    unsigned long long INF = 9223372036854775807 - (bucketCount);


        blockCount = (Z * bucketCount);
        storeBlockSize = (size_t) (IV + AES::GetCiphertextLength((int) (Z * (blockSize))));
        unsigned long long clen_size = AES::GetCiphertextLength((int) (blockSize) * Z);
        unsigned long long plaintext_size = (blockSize) * Z;
//    unsigned long long maxHeightOfAVLTree = (int) floor(log2(blockCount)) + 1;

        unsigned long long first_leaf = bucketCount / 2;

        Bucket* bucket = new Bucket();



        for (unsigned int i = 0; i < nodes.size(); i++) {
            nodes[i]->evictionNode = nodes[i]->pos + first_leaf;
        }

        bitonicSort(&nodes, 2);

        vector<Bucket> buckets;

        uint32_t first_bucket_of_last_level = bucketCount / 2;
        j = 0;

        for (unsigned int i = 0; i < nodes.size(); i++) {
            BTreeNode* cureNode = nodes[i];
            uint32_t curBucketID = (uint32_t) nodes[i]->evictionNode;

            Block &curBlock = (*bucket)[j];
            curBlock.data.resize(blockSize, 0);

            std::array<byte_t, sizeof (BTreeNode) > data;

            const byte_t* begin = reinterpret_cast<const byte_t*> (std::addressof(*cureNode));
            const byte_t* end = begin + sizeof (BTreeNode);
            std::copy(begin, end, std::begin(data));

            block tmp(data.begin(), data.end());

            if (cureNode->isDummy) {
                curBlock.id = 0;
            } else {
                curBlock.id = cureNode->index;
            }
            for (unsigned int k = 0; k < tmp.size(); k++) {
                if (cureNode->isDummy == false) {
                    curBlock.data[k] = tmp[k];
                }
            }
            delete cureNode;
            j++;

            if (j == Z) {
                (*indexes).push_back(curBucketID);
                buckets.push_back((*bucket));
                delete bucket;
                bucket = new Bucket();
                j = 0;
            }
        }

        for (uint32_t ii = 0; ii < first_bucket_of_last_level; ii++) {
//            if (ii % 100000 == 0) {
//                logger(DBG, "Adding Upper Levels Dummy Buckets:%u/%zu", ii, nodes.size());
//            }
            for (uint32_t z = 0; z < Z; z++) {
                Block &curBlock = (*bucket)[z];
                curBlock.id = 0;
                curBlock.data.resize(blockSize, 0);
            }
            (*indexes).push_back(ii);
            buckets.push_back((*bucket));
            delete bucket;
            bucket = new Bucket();
        }
        delete bucket;

        for (size_t k = 0; k < (*indexes).size(); k++) {
            block buffer;
            for (uint32_t z = 0; z < Z; z++) {
                Block b = buckets[k][z];
                buffer.insert(buffer.end(), b.data.begin(), b.data.end());
            }
            if (encryptBlocks) {
                block ciphertext = AES::Encrypt(secretkey, buffer, clen_size, plaintext_size);
                (*blocks).push_back(ciphertext);
            } else {
                (*blocks).push_back(buffer);
            }
        }
    }

};

#endif // ORAMINIT_HPP