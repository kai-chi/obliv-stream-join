#ifndef COMMON_H
#define COMMON_H

#include <cmath>

static int max_depth_btree(long long maxSize, int degree) {
    (void) (degree);
//    return (int) (ceil(log2((maxSize+1)/2)/log2(degree)));
    return (int) (ceil(log2(maxSize)) - 1) + 1;;
}

inline int max_depth_btree_real(long long maxSize, int degree) {
    return (int) (ceil(log2((maxSize+1)/2)/log2(degree)));
}

static int max_of_random_btree(int depth, int degree) {
//    return (int) (pow((2*degree), (depth+1)))-1;
    (void) (degree);
    return (int) (pow(2, depth));
}
#endif // COMMON_H

