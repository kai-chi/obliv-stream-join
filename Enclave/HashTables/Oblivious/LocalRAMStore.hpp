#ifndef LOCALRAMSTORE_HPP
#define LOCALRAMSTORE_HPP
#pragma once
#include <map>
#include <array>
#include <vector>

using namespace std;

using byte_t = uint8_t;
using block = std::vector<byte_t>;

// The "untrusted storage" both ORAMs (BTreeORAM, DOHEAP) read/write paths
// against - except here it's just a std::vector living in enclave memory,
// not actual untrusted host RAM reached via OCALLs. That's the point: the
// paper's SHJ-L2 design keeps the whole ORAM server in-enclave so window
// operations never cross the ecall/ocall boundary. Nothing fancy here, it's
// literally an array indexed by ORAM block position.
class LocalRAMStore {
    std::vector<block> store;
    size_t size;

public:
    LocalRAMStore(size_t num, size_t size);
    ~LocalRAMStore();

    block Read(size_t pos);
    void Write(size_t pos, block b);

    void nWrite(size_t blockCount, uint32_t* indexes, const char *blk, size_t len);
};

#endif /* LOCALRAMSTORE_HPP */
