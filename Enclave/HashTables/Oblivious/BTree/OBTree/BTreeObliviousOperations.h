#ifndef BTREEOBLIVIOUSOPERATIONS_H
#define BTREEOBLIVIOUSOPERATIONS_H

#include <vector>
#include <cassert>
#include <stdlib.h>
#include <array>
#include "BTreeORAM.hpp"

using namespace std;

// Sorting helpers for ORAM path eviction, not the OSORT primitive the paper
// uses in the OCA join algorithms (that one lives under
// Enclave/ObliviousComputationApproach/osort.h) - this is a smaller, local
// thing: bitonic-sort a handful of BTreeNodes by their assigned ORAM
// eviction path so writeback can process them in path order.
class BTreeObliviousOperations {
private:
    static void bitonic_sort(vector<BTreeNode*>* nodes, int low, int n, int dir);
    static void bitonic_merge(vector<BTreeNode*>* nodes, int low, int n, int dir);
    static void compare_and_swap(BTreeNode* item_i, BTreeNode* item_j, int dir);
    static int greatest_power_of_two_less_than(int n);

public:
    static long long INF;
    BTreeObliviousOperations();
    virtual ~BTreeObliviousOperations();
    // Sorts by evictionNode (the ORAM path a node is assigned to), pushing
    // dummy blocks to the end - the compare/swap is still done obliviously
    // over the whole array either way, dummies just lose every comparison.
    static void oblixmergesort(std::vector<BTreeNode*> *data);
    // Plain bitonic sort by evictionNode, no dummy-last handling.
    static void bitonicSort(vector<BTreeNode*>* nodes);

};

#endif /* BTREEOBLIVIOUSOPERATIONS_H */

