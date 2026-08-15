#ifndef GRAPHOSOBLIVIOUSOPERATIONS_H
#define GRAPHOSOBLIVIOUSOPERATIONS_H

#include <vector>
#include "DOHEAP.hpp"

using namespace std;

// Heap-side counterpart to BTreeObliviousOperations: local sort/compaction
// helpers for ORAM stash/bucket bookkeeping, not the paper's general-purpose
// OSORT/OCOMPACTION primitives used by the OCA joins (those live under
// Enclave/ObliviousComputationApproach/).
class HeapObliviousOperations {
private:
    static void bitonic_sort(vector<HeapNode*>* nodes, int low, int n, int dir);
    static void bitonic_merge(vector<HeapNode*>* nodes, int low, int n, int dir);
    static void compare_and_swap(HeapNode* item_i, HeapNode* item_j, int dir);
    static int greatest_power_of_two_less_than(int n);

public:
    static long long INF;
    HeapObliviousOperations();
    virtual ~HeapObliviousOperations();
    // Sorts by evictionNode, dummies last - same idea as
    // BTreeObliviousOperations::oblixmergesort but for heap nodes.
    static void oblixmergesort(std::vector<HeapNode*> *data);
    static void bitonicSort(vector<HeapNode*>* nodes);
    // Pushes nodes with evictionNode == -1 (empty slots) to the back and
    // slides everything else forward, obliviously - used to repack a
    // bucket/stash after entries have been consumed rather than to filter
    // arbitrary "marked" records.
    static void compaction(std::vector<HeapNode*>* data);

};

#endif /* GRAPHOSOBLIVIOUSOPERATIONS_H */

