#ifndef NLJ_HPP
#define NLJ_HPP

#include "data-types.h"
#include "Commons/CircularVector.hpp"

// NLJ-L4 - the paper's strawman for full obliviousness. Windows here are
// plain circular buffers, nothing fancy, but every probe scans the *entire*
// opposite window instead of stopping at a match (CircularVector::fullScan),
// emitting a dummy tuple for every slot that isn't a real hit. Since the scan
// always touches every slot no matter what's in it, the access pattern looks
// the same for any input of the same size - that's L4, paid for with an
// O(window size) scan per tuple instead of ORAM or oblivious sorting.
// Dispatched from Enclave.cpp's J4_wrapper.
class NLJ_ST {
private:
    CircularVector* windowR;
    CircularVector* windowS;

    // No-op sink, mirroring SHJ::emit_result: dummy and real results must be
    // indistinguishable to any downstream observer, so nothing here can
    // depend on which one was produced.
    void emit_results(vector<row_t>& res);

public:
    NLJ_ST(uint32_t windowRSize, uint32_t windowSSize) {
        this->windowR = new CircularVector(windowRSize);
        this->windowS = new CircularVector(windowSSize);
    }

    ~NLJ_ST() {
        delete windowR;
        delete windowS;
    }

    // Tuple-at-a-time loop: fills both windows, then for each arriving tuple
    // (in timestamp order) inserts it into its own window and does a full
    // window-length scan of the opposite window via CircularVector::fullScan.
    result_t * join(relation_t *relR, relation_t *relS, joinconfig_t * config);
};

#endif //NLJ_HPP
