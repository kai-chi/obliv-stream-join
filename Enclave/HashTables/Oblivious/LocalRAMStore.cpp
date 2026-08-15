#include "LocalRAMStore.hpp"
#include "Enclave.h"

LocalRAMStore::LocalRAMStore(size_t count, size_t ram_size)
: store(count), size(ram_size) {
}

LocalRAMStore::~LocalRAMStore() {
}

block LocalRAMStore::Read(size_t pos) {
    return store[pos];
}

void LocalRAMStore::Write(size_t pos, block b) {
    store[pos] = b;
}

void LocalRAMStore::nWrite(size_t blockCount, uint32_t *indexes, const char *blk, size_t len)
{
    assert(len % blockCount == 0);
    size_t eachSize = len / blockCount;
    for (unsigned int i = 0; i < blockCount; i++) {
        block ciphertext(blk + (i * eachSize), blk + (i + 1) * eachSize);
        Write(indexes[i], ciphertext);
    }
}
