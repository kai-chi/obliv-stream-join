#ifndef BTREEOBLIVIOUSOPERATIONS_H
#define BTREEOBLIVIOUSOPERATIONS_H

#include <vector>
#include <cassert>
#include <stdlib.h>
#include <array>
#include "BTreeORAM.hpp"

using namespace std;

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
    static void oblixmergesort(std::vector<BTreeNode*> *data);
    static void bitonicSort(vector<BTreeNode*>* nodes);

};

#endif /* BTREEOBLIVIOUSOPERATIONS_H */

