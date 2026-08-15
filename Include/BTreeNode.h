#ifndef BTREENODE_H
#define BTREENODE_H

#include "AES.hpp"

#ifndef BTREE_DEGREE
#define BTREE_DEGREE 3
#endif
#define BTREE

struct BTreeChild {
    uint32_t index;
    uint32_t oramPos;
    uint32_t count; // stores a count of the number of elements stored in that whole subtree
};

struct BTreeKeyValuePair {
    uint64_t key;
    uint32_t value;
};

static const uint32_t KV_SIZE = 2 * BTREE_DEGREE - 1;
static const uint32_t CHILDREN_SIZE = 2 * BTREE_DEGREE;
static const uint32_t ERROR_POS = UINT32_MAX;

class BTreeNode {
private:

public:

    BTreeNode() = default;

    ~BTreeNode() = default;

    void setCounter(size_t local_pos, uint32_t counter) {
        for (size_t i = 0; i < CHILDREN_SIZE; i++) {
            int c = CTeq(i, local_pos);
            children.at(i).count = conditional_select(counter, children.at(i).count, c);
        }

    }

    uint32_t getSubTreeSize() {
        uint32_t res = n;
        for (uint32_t i = 0; i < CHILDREN_SIZE; i++) {
            int c = CTeq(CTcmp(i, n+1), -1);
            uint32_t tmp = conditional_select(children.at(i).count, (uint32_t)0, c);
            res += tmp;
        }
        return res;
    }

    uint32_t getCounter(size_t i) {
        return children.at(i).count;
    }

    uint32_t index = 0;
    uint32_t pos = 0;
    int height = 0;
    long long evictionNode = 0;
    bool isDummy = false;
    bool modified = 0;

    // B-Tree
    int n;
    bool leaf;
    std::array<BTreeKeyValuePair, KV_SIZE> kvPairs;
    std::array<BTreeChild, CHILDREN_SIZE> children;

    bool isZero() {
        return CTeq(CTcmp(index, 0), 0);
    }

    uint64_t getKVPairKey(size_t i) {
        return this->kvPairs.at(i).key;
    }

    uint64_t CTgetKVPairKey(size_t i) {
        uint64_t key = 0;
        for (size_t j = 0; j < KV_SIZE; j++) {
            int c = BTreeNode::CTeq(i, j);
            key = conditional_select(this->kvPairs.at(j).key, key, c);
        }
        return key;
    }

    void setKVPairKey(size_t i, uint64_t key) {
        this->kvPairs.at(i).key = key;
    }

    void CTsetKVPairKey(size_t i, uint64_t key) {
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c = BTreeNode::CTeq(i, j);
            this->kvPairs.at(j).key = conditional_select(key, this->kvPairs.at(j).key, c);
        }
    }

    void CTsetKVPairKey(size_t i, uint64_t key, bool _isDummy) {
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c = BTreeNode::CTeq(i, j);
            this->kvPairs.at(j).key = conditional_select(key, this->kvPairs.at(j).key, !_isDummy && c);
        }
    }

    void CTupdateKVPairKey(size_t i, uint64_t key, bool condition) {
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c = condition && (i==j);
            kvPairs[j].key = conditional_select(key, kvPairs[j].key, c);
        }
    }

    void CTupdateKVPair(size_t i, uint64_t key, uint32_t value, bool condition) {
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c = condition && CTeq(i,j);
            kvPairs[j].key = conditional_select(key, kvPairs[j].key, c);
            kvPairs[j].value = conditional_select(value, kvPairs[j].value, c);
        }
    }

    uint32_t getKVPairValue(size_t i) {
        return this->kvPairs.at(i).value;
    }

    uint32_t CTgetKVPairValue(size_t i) {
        uint32_t value = 0;
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c = BTreeNode::CTeq(i, j);
            value = conditional_select(this->kvPairs.at(j).value, value, c);
        }
        return value;
    }

    void setKVPairValue(size_t i, uint32_t val) {
        this->kvPairs.at(i).value = val;
    }

    void CTsetKVPairValue(size_t i, uint32_t value) {
        for (size_t j = 0; j < KV_SIZE; j++) {
            bool c = BTreeNode::CTeq(i, j);
            this->kvPairs.at(j).value = conditional_select(value, this->kvPairs.at(j).value, c);
        }
    }

    BTreeKeyValuePair * getKVPair(size_t i) {
        return &this->kvPairs.at(i);
    }

    BTreeKeyValuePair * CTgetKVPair(size_t i) {
        return &this->kvPairs.at(i);
    }

    uint32_t getChildIndex(size_t i) {
        return this->children.at(i).index;
    }

    uint32_t CTgetChildIndex(size_t i) {
        uint32_t idx = 0;
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool c = BTreeNode::CTeq(i,j);
            idx = BTreeNode::conditional_select(this->children.at(j).index, idx, c);
        }
        return idx;
    }

    uint32_t CTgetChildPosByIndex(uint32_t _index) {
        uint32_t opos = 0;
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool c = BTreeNode::CTeq(this->children.at(j).index,_index);
            opos = BTreeNode::conditional_select(this->children.at(j).oramPos, opos, c);
        }
        return opos;
    }

    void setChildIndex(size_t i, uint32_t key) {
        this->children.at(i).index = key;
    }

    uint32_t getChildPos(size_t i) {
        return this->children.at(i).oramPos;
    }

    uint32_t CTgetChildPos(size_t i) {
        uint32_t oramPos = 0;
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool c = BTreeNode::CTeq(i,j);
            oramPos = BTreeNode::conditional_select(this->children.at(j).oramPos, oramPos, c);
        }
        return oramPos;
    }

    void setChildPos(size_t i, uint32_t _pos) {
        this->children.at(i).oramPos = _pos;
    }

    BTreeChild getChild(size_t i) {
        return this->children.at(i);
    }

    BTreeChild CTgetChild(size_t i) {
        BTreeChild child{};
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool cond = CTeq(i, j);
            child.index = conditional_select(children.at(j).index, child.index, cond);
            child.oramPos = conditional_select(children.at(j).oramPos, child.oramPos, cond);
            child.count = conditional_select(children.at(j).count, child.count, cond);
        }
        return child;
    }

    void setChild(size_t i, BTreeChild *c) {
        setChildIndex(i, c->index);
        setChildPos(i, c->oramPos);
        setCounter(i, c->count);
    }

    // TODO: later delete this one
    void updateChildPos(uint32_t key, uint32_t newPos) {
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool choice = BTreeNode::CTeq(BTreeNode::CTcmp(key, this->getChildIndex(j)), 0);
            this->setChildPos(j, BTreeNode::conditional_select(newPos, this->getChildPos(j), choice));
        }
    }

    void CTupdateChildPos(uint32_t key, uint32_t newPos) {
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool choice = BTreeNode::CTeq(BTreeNode::CTcmp(key, this->getChildIndex(j)), 0);
            this->setChildPos(j, BTreeNode::conditional_select(newPos, this->getChildPos(j), choice));
        }
    }

    void CTupdateChildCount(uint32_t key, int change) {
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool choice = BTreeNode::CTeq(BTreeNode::CTcmp(key, this->getChildIndex(j)), 0);
            uint32_t old = this->getCounter(j);
            this->setCounter(j, BTreeNode::conditional_select(uint32_t((int)old+change), old, choice));
        }
    }



    // TODO: later delete this version
    void updateChild(uint32_t key, uint32_t newKey, uint32_t newPos) {
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool choice = BTreeNode::CTeq(BTreeNode::CTcmp(key, this->getChildIndex(j)), 0);
            this->setChildIndex(j, BTreeNode::conditional_select(newKey, this->getChildIndex(j), choice));
            this->setChildPos(j, BTreeNode::conditional_select(newPos, this->getChildPos(j), choice));
        }
    }

    void CTupdateChild(uint32_t key, uint32_t newKey, uint32_t newPos, uint32_t newCount) {
        for (size_t j = 0; j < CHILDREN_SIZE; j++) {
            bool choice = BTreeNode::CTeq(key, this->getChildIndex(j));
            this->setChildIndex(j, BTreeNode::conditional_select(newKey, this->getChildIndex(j), choice));
            this->setChildPos(j, BTreeNode::conditional_select(newPos, this->getChildPos(j), choice));
            this->setCounter(j, BTreeNode::conditional_select(newCount, this->getCounter(j), choice));
        }
    }

    void CTupdateChild(uint32_t idx, uint32_t newIndex, uint32_t newPos, bool condition, uint32_t newCount) {
        for (uint32_t j = 0; j < CHILDREN_SIZE; j++) {
            bool c = condition && CTeq(idx, j);
            children.at(j).index = conditional_select(newIndex, children.at(j).index, c);
            children.at(j).oramPos = conditional_select(newPos, children.at(j).oramPos, c);
            children.at(j).count = conditional_select(newCount, children.at(j).count, c);
        }
    }

    void CTsetChild(BTreeChild newChild, bool condition) {
        for (size_t i = 0; i < CHILDREN_SIZE; i++) {
            setChildIndex(i, BTreeNode::conditional_select(newChild.index, getChildIndex(i), condition));
            setChildPos(i, BTreeNode::conditional_select(newChild.oramPos, getChildPos(i), condition));
            setCounter(i, BTreeNode::conditional_select(newChild.count, getCounter(i), condition));
        }
    }

    // A utility function that returns the index of the first key that is
    // greater than or equal to k
    int findKey(uint64_t k) {
        int idx = 0;
        for (size_t i = 0; i < KV_SIZE; i++) {
            if (idx < n && getKVPairKey((size_t) idx) < k) {
                ++idx;
            } else {
                idx = idx;
            }
        }
        return idx;
    }

    int CTfindKey(uint64_t k) {
        int idx = 0;
        for (size_t i = 0; i < KV_SIZE; i++) {
            bool c1 = BTreeNode::CTeq(BTreeNode::CTcmp(idx, n), -1); // idx < n
            bool c2 = BTreeNode::CTeq(BTreeNode::CTcmp(getKVPairKey((size_t) idx), k), -1); // keys[idx] < k
            idx = conditional_select(idx+1, idx, (int) (c1 && c2));
        }
        return idx;
    }

    static BTreeNode* clone(BTreeNode* oldNode) {
        BTreeNode* newNode = new BTreeNode();
        newNode->evictionNode = oldNode->evictionNode;
        newNode->index = oldNode->index;
        newNode->pos = oldNode->pos;
        newNode->isDummy = oldNode->isDummy;
        newNode->modified = oldNode->modified;
        newNode->height = oldNode->height;

        newNode->n = oldNode->n;
        newNode->leaf = oldNode->leaf;
        std::copy(oldNode->kvPairs.begin(), oldNode->kvPairs.end(), newNode->kvPairs.begin());
        std::copy(oldNode->children.begin(), oldNode->children.end(), newNode->children.begin());
        return newNode;
    }

    /**
     * constant time comparator
     * @param left
     * @param right
     * @return left < right -> -1,  left = right -> 0, left > right -> 1
     */
    static int CTcmp(long long lhs, long long rhs) {
        unsigned __int128 overflowing_iff_lt = (unsigned __int128) ((__int128) lhs - (__int128) rhs);
        unsigned __int128 overflowing_iff_gt = (unsigned __int128) ((__int128) rhs - (__int128) lhs);
        int is_less_than = (int) -(overflowing_iff_lt >> 127); // -1 if self < other, 0 otherwise.
        int is_greater_than = (int) (overflowing_iff_gt >> 127); // 1 if self > other, 0 otherwise.
        int result = is_less_than + is_greater_than;
        return result;
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
        unsigned long long mask = ((unsigned long long) choice - one);
        return (long long) ((~mask & (unsigned long long) a) | (mask & (unsigned long long) b));
    }

    static unsigned long long conditional_select(unsigned long long a, unsigned long long b, int choice) {
        unsigned long long one = 1;
        return (~((unsigned long long) choice - one) & a) | ((unsigned long long) ((unsigned long long) choice - one) & b);
    }

    static unsigned int conditional_select(unsigned int a, unsigned int b, int choice) {
        unsigned int one = 1;
        return (~((unsigned int) choice - one) & a) | ((unsigned int) ((unsigned int) choice - one) & b);
    }

    static unsigned __int128 conditional_select(unsigned __int128 a, unsigned __int128 b, int choice) {
        unsigned __int128 one = 1;
        return (~((unsigned __int128) choice - one) & a) | ((unsigned __int128) ((unsigned __int128) choice - one) & b);
    }

    static byte_t conditional_select(byte_t a, byte_t b, int choice) {
        byte_t one = 1;
        return (byte_t) ((~((byte_t) choice - one) & a) | ((byte_t) (choice - one) & b));
    }

    static int conditional_select(int a, int b, int choice) {
        unsigned int one = 1;
        unsigned int mask = ((unsigned int) choice - one);
        return (int) ((~mask & (unsigned int) a) | (mask & (unsigned int) b));
    }

    static uint64_t conditional_select(uint64_t a, uint64_t b, int choice) {
        uint64_t one = 1;
        return (~((uint64_t) choice - one) & a) | ((uint64_t) ((uint64_t)choice - one) & b);
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> b->a , choice = 0 -> return a->a
     */
    static void conditional_assign(BTreeNode* a, BTreeNode* b, int choice) {
        a->index = (uint32_t) BTreeNode::conditional_select((long long) b->index, (long long) a->index, choice);
        a->isDummy = BTreeNode::conditional_select(b->isDummy, a->isDummy, choice);
        a->pos = (uint32_t) BTreeNode::conditional_select((long long) b->pos, (long long) a->pos, choice);
        a->evictionNode = BTreeNode::conditional_select(b->evictionNode, a->evictionNode, choice);
        a->modified = BTreeNode::conditional_select(b->modified, a->modified, choice);
        a->height = BTreeNode::conditional_select(b->height, a->height, choice);

        a->n = BTreeNode::conditional_select(b->n, a->n, choice);
        a->leaf = BTreeNode::conditional_select(b->leaf, a->leaf, choice);

        for (uint32_t k = 0; k < KV_SIZE; k++) {
            a->kvPairs[k].key = BTreeNode::conditional_select(b->kvPairs[k].key, a->kvPairs[k].key, (bool)choice);
            a->kvPairs[k].value = BTreeNode::conditional_select(b->kvPairs[k].value , a->kvPairs[k].value , choice);
        }

        for (uint32_t k = 0; k < CHILDREN_SIZE; k++) {
            a->children[k].index = BTreeNode::conditional_select(b->children[k].index, a->children[k].index, choice);
            a->children[k].oramPos = BTreeNode::conditional_select(b->children[k].oramPos, a->children[k].oramPos, choice);
            a->children[k].count = BTreeNode::conditional_select(b->children[k].count, a->children[k].count, choice);
        }
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static void conditional_swap(BTreeNode* a, BTreeNode* b, int choice) {
        BTreeNode tmp = *b;
        b->index = BTreeNode::conditional_select(a->index, b->index, choice);
        b->isDummy = BTreeNode::conditional_select(a->isDummy, b->isDummy, choice);
        b->pos = BTreeNode::conditional_select(a->pos, b->pos, choice);
        b->evictionNode = BTreeNode::conditional_select(a->evictionNode, b->evictionNode, choice);
        b->modified = BTreeNode::conditional_select(a->modified, b->modified, choice);
        b->height = BTreeNode::conditional_select(a->height, b->height, choice);
        b->n = BTreeNode::conditional_select(a->n, b->n, choice);
        b->leaf = BTreeNode::conditional_select(a->leaf, b->leaf, choice);
        for (size_t k = 0; k < KV_SIZE; k++) {
            b->kvPairs[k].key = BTreeNode::conditional_select(a->kvPairs[k].key, b->kvPairs[k].key, choice);
            b->kvPairs[k].value = BTreeNode::conditional_select(a->kvPairs[k].value , b->kvPairs[k].value , choice);
        }

        for (size_t k = 0; k < CHILDREN_SIZE; k++) {
            b->children[k].index = BTreeNode::conditional_select(a->children[k].index, b->children[k].index, choice);
            b->children[k].oramPos = BTreeNode::conditional_select(a->children[k].oramPos, b->children[k].oramPos, choice);
            b->children[k].count = BTreeNode::conditional_select(a->children[k].count, b->children[k].count, choice);
        }

        a->index = BTreeNode::conditional_select(tmp.index, a->index, choice);
        a->isDummy = BTreeNode::conditional_select(tmp.isDummy, a->isDummy, choice);
        a->pos = BTreeNode::conditional_select(tmp.pos, a->pos, choice);
        a->evictionNode = BTreeNode::conditional_select(tmp.evictionNode, a->evictionNode, choice);
        a->modified = BTreeNode::conditional_select(tmp.modified, a->modified, choice);
        a->height = BTreeNode::conditional_select(tmp.height, a->height, choice);
        a->n = BTreeNode::conditional_select(tmp.n, a->n, choice);
        a->leaf = BTreeNode::conditional_select(tmp.leaf, a->leaf, choice);
        for (size_t k = 0; k < KV_SIZE; k++) {
            a->kvPairs[k].key = BTreeNode::conditional_select(tmp.kvPairs[k].key, a->kvPairs[k].key, choice);
            a->kvPairs[k].value = BTreeNode::conditional_select(tmp.kvPairs[k].value , a->kvPairs[k].value , choice);
        }

        for (size_t k = 0; k < CHILDREN_SIZE; k++) {
            a->children[k].index = BTreeNode::conditional_select(tmp.children[k].index, a->children[k].index, choice);
            a->children[k].oramPos = BTreeNode::conditional_select(tmp.children[k].oramPos, a->children[k].oramPos, choice);
            a->children[k].count = BTreeNode::conditional_select(tmp.children[k].count, a->children[k].count, choice);
        }
    }

    static void conditional_swap(unsigned long long& a, unsigned long long& b, int choice) {
        unsigned long long tmp = b;
        b = BTreeNode::conditional_select((unsigned long long) a, (unsigned long long) b, choice);
        a = BTreeNode::conditional_select((unsigned long long) tmp, (unsigned long long) a, choice);
    }

    static bool CTeq(int a, int b) {
        return !(a^b);
    }

    static bool CTeq(uint32_t a, uint32_t b) {
        return !(a^b);
    }

    static bool CTeq(size_t a, size_t b) {
        return !(a^b);
    }

    static bool CTeq(long long a, long long b) {
        return !(a^b);
    }

    static bool CTeq(unsigned __int128 a, unsigned __int128 b) {
        return !(a^b);
    }

    static bool CTeq(unsigned long long a, unsigned long long b) {
        return !(a^b);
    }


};

#endif /* BTREENODE_H */

