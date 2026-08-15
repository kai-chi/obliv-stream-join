#ifndef DOHEAPENCLAVEINTERFACE_HPP
#define DOHEAPENCLAVEINTERFACE_HPP

#include "DOHEAP.hpp"

// In-enclave glue around two DOHEAP instances (left/right = window R/S),
// same idea as BTreeEnclaveInterface on the index-tier side - no OCALLs,
// just a narrower call surface for BTreeHashTable to use.
class DOHEAPEnclaveInterface {
private:
    DOHEAP* oheapLeft = nullptr;
    DOHEAP* oheapRight = nullptr;
public:
    DOHEAPEnclaveInterface(){}

    virtual ~DOHEAPEnclaveInterface()
    {
        delete oheapLeft;
        delete oheapRight;
    }

    void setup_oheap(int isLeft, uint32_t maxSize, int useLocalRamStore) {
        bytes<Key> tmpkey{0};
        if (isLeft) {
            oheapLeft = new DOHEAP(maxSize, tmpkey, false, isLeft, useLocalRamStore);
        } else {
            oheapRight = new DOHEAP(maxSize, tmpkey, false, isLeft, useLocalRamStore);
        }
    }

    void set_new_minheap_node(int isLeft, int newMinHeapNodeV, int newMinHeapNodeDist) {
        //    oheap->setNewMinHeapNode(newMinHeapNodeV, newMinHeapNodeDist);
        Bid id = newMinHeapNodeDist;
        array<byte_t, 16> value;
        std::fill(value.begin(), value.end(), 0);
        for (uint32_t i = 0; i < 4; i++) {
            value[i] = (byte_t) (newMinHeapNodeV >> (i * 8));
        }
        if (isLeft) {
            oheapLeft->execute(id, value, 2);
        } else {
            oheapRight->execute(id, value, 2);
        }
    }

    // op=2 (insert): *dist is the timestamp key, *v is the join key to store.
    // op=1 (extract-min): ignores the inputs, pops the oldest entry and
    // writes its key back into *v and its timestamp into *dist.
    void execute_heap_operation(int isLeft, uint32_t* v, uint32_t* dist, int op) {
        //        oheap->execute(*v, *dist, op);
        uint32_t d = *dist;
        Bid id = d;
        uint32_t val = *v;
        array<byte_t, 16> value;
        std::fill(value.begin(), value.end(), 0);
        for (uint32_t i = 0; i < 4; i++) {
            value[i] = (byte_t) (val >> (i * 8));
        }
        pair<Bid, array<byte_t, 16> > res;
        if (isLeft) {
            res = oheapLeft->execute(id, value, op);
        } else {
            res = oheapRight->execute(id, value, op);
        }
        uint32_t rr = (uint32_t) res.first.getValue();
        *dist = rr;
        std::memcpy(v, res.second.data(), sizeof (int));
    }

};
#endif //DOHEAPENCLAVEINTERFACE_HPP
