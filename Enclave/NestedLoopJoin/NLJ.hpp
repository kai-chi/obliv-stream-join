#ifndef NLJ_HPP
#define NLJ_HPP

#include "data-types.h"
#include "Commons/CircularVector.hpp"

class NLJ_ST {
private:
    CircularVector* windowR;
    CircularVector* windowS;

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

    result_t * join(relation_t *relR, relation_t *relS, joinconfig_t * config);
};

#endif //NLJ_HPP
