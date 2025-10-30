#ifndef BTREE_H
#define BTREE_H

#include <cstddef>
#include "BTreeNode.h"
#include "BTreeORAM.hpp"
#include "Config.hpp"
#include <vector>

// A BTree node
//class BTreeNode
//{
//public:
//
//    int *keys;  // An array of keys
//    int degree;      // Minimum degree (defines the range for number of keys)
//    BTreeNode **C; // An array of child pointers
//    int n;     // Current number of keys
//    bool leaf; // Is true when node is leaf. Otherwise false
//
//
//
//    BTreeNode(int _degree, bool _leaf);   // Constructor
//
//    // A function to traverse all nodes in a subtree rooted with this node
//    void traverse();
//
//    // A function to search a key in subtree rooted with this node.
//    BTreeNode *search(int k);   // returns NULL if k is not present.
//
//    // A function that returns the index of the first key that is greater
//    // or equal to k
//    int findKey(int k);
//
//    // A utility function to insert a new key in the subtree rooted with
//    // this node. The assumption is, the node must be non-full when this
//    // function is called
//    void insertNonFull(int k);
//
//    // A utility function to split the child y of this node. i is index
//    // of y in child array C[].  The Child y must be full when this
//    // function is called
//    void splitChild(int i, BTreeNode *y);
//
//    // A wrapper function to remove the key k in subtree rooted with
//    // this node.
//    void remove(int k);
//
//    // A function to remove the key present in idx-th position in
//    // this node which is a leaf
//    void removeFromLeaf(int idx);
//
//    // A function to remove the key present in idx-th position in
//    // this node which is a non-leaf node
//    void removeFromNonLeaf(int idx);
//
//    // A function to get the predecessor of the key- where the key
//    // is present in the idx-th position in the node
//    int getPred(int idx);
//
//    // A function to get the successor of the key- where the key
//    // is present in the idx-th position in the node
//    int getSucc(int idx);
//
//    // A function to fill up the child node present in the idx-th
//    // position in the C[] array if that child has less than t-1 keys
//    void fill(int idx);
//
//    // A function to borrow a key from the C[idx-1]-th node and place
//    // it in C[idx]th node
//    void borrowFromPrev(int idx);
//
//    // A function to borrow a key from the C[idx+1]-th node and place it
//    // in C[idx]th node
//    void borrowFromNext(int idx);
//
//    // A function to merge idx-th child of the node with (idx+1)th child of
//    // the node
//    void merge(int idx);
//
//    // Make BTree friend of this so that we can access private members of
//    // this class in BTree functions
//    friend class BTree;
//};

class BTree
{
//    BTreeNode *root; // Pointer to root node
private:
    int degree;  // Minimum degree
    int maxOfRandom;
    BTreeORAM *oram;
    uint32_t index = 1;
    int max_depth;
    uint64_t beginTimestamp;
    uint32_t insertCounter = 1;
#ifdef MEASURE_PERF
    uint64_t tmp_cpu_cycles = 0;
#endif

    uint32_t RandomPath();
    BTreeNode* newNode();
    BTreeNode* newDummyNode();

    uint32_t getIndex();
    vector<BTreeNode*> btreeCache;

    BTreeNode* readWriteCacheNode(uint32_t inputIndex, BTreeNode* node, bool isRead, bool isDummy);

    class BTreePrinter
    {
        struct NodeInfo
        {
            std::string text;
            std::string text_counters;
            unsigned text_pos, text_end;  // half-open range
        };


        typedef std::vector<NodeInfo> LevelInfo;

        std::vector<LevelInfo> levels;

        std::string node_text (BTreeNode *node);
        std::string node_counters_text (BTreeNode *node);

        void before_traversal ()
        {
            levels.resize(0);
            levels.reserve(10);   // far beyond anything that could usefully be printed
        }

        void visit (BTreeNode *node, unsigned level = 0, unsigned child_index = 0);

        void after_traversal ();

        void print_blanks (unsigned n);

    public:
        BTreeORAM *btreeOram;
        void print (BTreeNode *root)
        {
            before_traversal();
            visit(root);
            after_traversal();
        }
    };

    // A utility function to insert a new key in the subtree rooted with
    // this node. The assumption is, the node must be non-full when this
    // function is called
    uint32_t insertNonFull(BTreeNode *node, uint64_t key, uint32_t value);
    uint32_t insertNonFull(BTreeNode *node, uint64_t key, uint32_t value, bool isDummyIns, int iterations_left);

    // A utility function to split the child y of this node. i is index of y in
    // child array C[].  The Child y must be full when this function is called
    void splitChild(int i, BTreeNode *parent, BTreeNode *child);
    void splitChild(int i, BTreeNode *parent, BTreeNode *child, bool isDummySplit);

    // A function to remove the key present in idx-th position in
    // this node which is a leaf
    void removeFromLeaf(BTreeNode *node, int idx);
    void CTremoveFromLeaf(BTreeNode *node, int idx, bool isDummy);

    // A function to remove the key present in idx-th position in
    // this node which is a non-leaf node
    void removeFromNonLeaf(BTreeNode *node, int idx);
    void CTremoveFromNonLeaf(BTreeNode *node, int idx, bool isDummy);

    // A function to get the predecessor of the key- where the key
    // is present in the idx-th position in the node
    BTreeKeyValuePair getPred(BTreeNode *node, int idx);
    BTreeKeyValuePair getPred(BTreeNode *node, int idx, bool isDummy, int iterations_left);

    // A function to get the successor of the key- where the key
    // is present in the idx-th position in the node
    BTreeKeyValuePair getSucc(BTreeNode *node, int idx);
    BTreeKeyValuePair getSucc(BTreeNode *node, int idx, bool isDummy, int iterations_left);

    BTreeKeyValuePair getPredOrSucc(BTreeNode *node, int idx, bool isPred, bool isSucc, bool isDummy, int iterations_left, bool isFirstIteration);

    // A function to fill up the child node present in the idx-th
    // position in the C[] array if that child has less than t-1 keys
    void fill(BTreeNode *node, int idx);

    // A function to borrow a key from the C[idx-1]-th node and place
    // it in C[idx]th node
    void borrowFromPrev(BTreeNode *node, int idx);

    // A function to borrow a key from the C[idx+1]-th node and place it
    // in C[idx]th node
    void borrowFromNext(BTreeNode *node, int idx);

    // A function to merge idx-th child of the node with (idx+1)th child of
    // the node
    void merge(BTreeNode *node, int idx);
    void merge(BTreeNode *node, int idx, bool isDummy);

    inline int positive_modulo(int i, int n);

    BTreePrinter *printer;

    static bool CTeq(int a, int b) {
        return !(a^b);
    }

    static bool CTeq(uint64_t a, uint64_t b) {
        return !(a^b);
    }

    static bool CTeq(long long a, long long b) {
        return !(a^b);
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static long long conditional_select(long long a, long long b, int choice) {
        unsigned long long one = 1;
        return (~((unsigned long long) choice - one) & a) | ((unsigned long long) (choice - one) & b);
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static int conditional_select(int a, int b, int choice) {
        unsigned int one = 1;
        return (~((unsigned int) choice - one) & a) | ((unsigned int) (choice - one) & b);
    }

    static uint32_t conditional_select(uint32_t a, uint32_t b, int choice) {
        uint32_t one = 1;
        return (~((uint32_t) choice - one) & a) | ((uint32_t) ((uint32_t)choice - one) & b);
    }

    /**
     * constant time comparator
     * @param left
     * @param right
     * @return left < right -> -1,  left = right -> 0, left > right -> 1
     */
    static int CTcmp(long long lhs, long long rhs) {
        unsigned __int128 overflowing_iff_lt = (__int128) lhs - (__int128) rhs;
        unsigned __int128 overflowing_iff_gt = (__int128) rhs - (__int128) lhs;
        int is_less_than = (int) -(overflowing_iff_lt >> 127); // -1 if self < other, 0 otherwise.
        int is_greater_than = (int) (overflowing_iff_gt >> 127); // 1 if self > other, 0 otherwise.
        int result = is_less_than + is_greater_than;
        return result;
    }

public:
    bool logTime = false;
    int totheight = 0;
    bool exist;
#ifdef MEASURE_PERF
    uint64_t cpu_cycles = 0;
    // 0 - search2 entire operation
    // 1 - search2 ORAM total cycles
    // 2 - serach2 ORAM eviction cycles
    vector<vector<uint64_t>> performance_cycles;
#endif

    void startOperation(bool batchWrite = false);
    void finishOperation();

    // Constructor (Initializes tree as empty)
    BTree(uint32_t maxSize, bytes<Key> secretKey, bool isEmptyMap, int _degree, int useLocalRamStore, bool isLeft);

    ~BTree();

    void printTree(BTreeNode *rt);

    // function to search a key in this tree
    // 1. search3 - WITHOUT ANY OBLIVIOUSNESS
    // 2. search2 - with the required number of accesses
    // 3. search1 - with constant-time functions
    uint32_t search3(BTreeNode *rootNode, uint64_t key, uint32_t newRootNodePos, BTreeKeyValuePair lastRes);
    BTreeKeyValuePair *search2(BTreeNode *rootNode, uint64_t key, uint32_t newRootNodePos, uint32_t& index_counter);

    BTreeKeyValuePair* searchSucc(BTreeNode *rootNode, uint64_t key);


    // The main function that inserts a new key in this B-Tree
    // returns: new rootIndex
    uint32_t insert3 (uint32_t rootIndex, uint32_t& rootPos, uint32_t ts, uint32_t key, uint32_t value, int &height, uint32_t lastID, bool isDummyIns = false);
    uint32_t insert2 (uint32_t rootIndex, uint32_t& rootPos, uint32_t ts, uint32_t key, uint32_t value, int &height, uint32_t lastID, bool isDummyIns = false);


    // The main function that removes a new key in this B-Tree
    // 1. remove3 - absolutely no obliviousness
    // 2. remove2 - refactoring for obliviousness
    // 3. removeX - refactoring for obliviousness again! from the schema
    uint32_t remove3(uint32_t rootIndex, uint32_t& rootPos, uint64_t key, bool isDummyDel = false);
    uint32_t remove2(uint32_t rootIndex, uint32_t& rootPos, uint64_t key, bool isDummyDel = false);
    uint32_t removeX(uint32_t rootIndex, uint32_t& rootPos, uint64_t key, bool isDummyDel = false);

    void writeToLocalRamStore(vector<uint32_t> *indexes, vector<block> *blocks);

    uint64_t getOramCycles();

    uint64_t getOramEvictionCycles();

    void traverse(uint32_t index, uint32_t pos, vector<uint32_t> *res);
};
#endif //BTREE_H
