#ifndef HASHTABLEINTERFACE_HPP
#define HASHTABLEINTERFACE_HPP

#include "../../Include/data-types.h"
#include <vector>

#if defined(__cplusplus)
extern "C" {
#endif

class HashTableInterface
{
protected:
    bool isLeft;
    bool initialized = false;


    /**
     * number of tuples the HT is capable of storing
     * */
    uint32_t capacity = 0;

    virtual void emit_result(tuple_t r, tuple_t s) = 0;

public:
    /*
     * number of currently stored tuples in the hash table
     */
    uint32_t size = 0;

    HashTableInterface(int _isLeft) : isLeft(_isLeft) {}
    virtual ~HashTableInterface(){}
    virtual void initialize(uint32_t numTuples) = 0;
    virtual uint32_t probe(tuple_t t) = 0;
    virtual uint32_t probe(uint32_t key, timespec_t ts) = 0;
    virtual uint32_t probeNodes(relation_t *rel) = 0;
    virtual void build(tuple_t t) = 0;
    virtual void buildNodes(relation_t *rel) = 0;
    virtual void removeNode(tuple_t t) = 0;
    virtual uint32_t getSize(uint32_t key) = 0;

    bool isOverflown() {
        return size > capacity;
    }
    /*
     * Delete the oldest elements in the hash table.
     *
     * @param num how many tuples should be deleted
     * @return the deleted tuples
     * */
    virtual std::vector<type_key> deleteOldest(uint32_t num) = 0;

    virtual void printAll() = 0;
};

#if defined(__cplusplus)
}
#endif

#endif //HASHTABLEINTERFACE_HPP
