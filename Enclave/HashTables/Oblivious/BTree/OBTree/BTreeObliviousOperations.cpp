#include "BTreeObliviousOperations.h"
#include "../Enclave.h"
#include "Enclave_t.h"

BTreeObliviousOperations::BTreeObliviousOperations() {
}

BTreeObliviousOperations::~BTreeObliviousOperations() {
}

void BTreeObliviousOperations::oblixmergesort(std::vector<BTreeNode*>* data) {
    if (data->size() == 0 || data->size() == 1) {
        return;
    }
    int len = (int) data->size();
    int t = (int) ceil(log2(len));
    long long p = 1 << (t - 1);

    while (p > 0) {
        long long q = 1 << (t - 1);
        long long r = 0;
        long long d = p;

        while (d > 0) {
            long long i = 0;
            while (i < len - d) {
                if ((i & p) == r) {
                    long long j = i + d;
                    if (i != j) {
                        int node_cmp = BTreeNode::CTcmp((*data)[j]->evictionNode, (*data)[i]->evictionNode);
                        int dummy_blocks_last = BTreeNode::CTcmp((*data)[i]->isDummy, (*data)[j]->isDummy);
                        int same_nodes = BTreeNode::CTeq(node_cmp, 0);
                        bool cond = BTreeNode::CTeq(BTreeNode::conditional_select(dummy_blocks_last, node_cmp, same_nodes), -1);
                        BTreeNode::conditional_swap((*data)[i], (*data)[j], cond);
                    }
                }
                i += 1;
            }
            d = q - p;
            q /= 2;
            r = p;
        }
        p /= 2;
    }
    std::reverse(data->begin(), data->end());
}

int BTreeObliviousOperations::greatest_power_of_two_less_than(int n) {
    int k = 1;
    while (k > 0 && k < n) {
        k = k << 1;
    }
    return k >> 1;
}

void BTreeObliviousOperations::bitonicSort(vector<BTreeNode*>* nodes) {
    int len = (int) nodes->size();
    bitonic_sort(nodes, 0, len, 1);
}

void BTreeObliviousOperations::bitonic_sort(vector<BTreeNode*>* nodes, int low, int n, int dir) {
    if (n > 1) {
        int middle = n / 2;
        bitonic_sort(nodes, low, middle, !dir);
        bitonic_sort(nodes, low + middle, n - middle, dir);
        bitonic_merge(nodes, low, n, dir);
    }
}

void BTreeObliviousOperations::bitonic_merge(vector<BTreeNode*>* nodes, int low, int n, int dir) {
    if (n > 1) {
        int m = greatest_power_of_two_less_than(n);

        for (int i = low; i < (low + n - m); i++) {
            if (i != (i + m)) {
                compare_and_swap((*nodes)[i], (*nodes)[i + m], dir);
            }
        }

        bitonic_merge(nodes, low, m, dir);
        bitonic_merge(nodes, low + m, n - m, dir);
    }
}

void BTreeObliviousOperations::compare_and_swap(BTreeNode* item_i, BTreeNode* item_j, int dir) {
    int res = BTreeNode::CTcmp(item_i->evictionNode, item_j->evictionNode);
    int cmp = BTreeNode::CTeq(res, 1);
    BTreeNode::conditional_swap(item_i, item_j, BTreeNode::CTeq(cmp, dir));
}
