#include "RAMStore.hpp"
#include "Utilities.h"
#include <cassert>
#include <cstring>
#include "Enclave_u.h"
#include "Logger.h"

static RAMStore* storeLeft = NULL;
static RAMStore* storeRight = NULL;
static RAMStore* heapStoreLeft = NULL;
static RAMStore* heapStoreRight = NULL;

void ocall_setup_heapStore(int isLeft, size_t num, int size) {
    (void) (size);
    if (isLeft) {
        if (heapStoreLeft == NULL) {
            heapStoreLeft = new RAMStore(num, num, false);
        }
    } else {
        if (heapStoreRight == NULL) {
            heapStoreRight = new RAMStore(num, num, false);
        }
    }
}

void ocall_nwrite_heapStore(int isLeft, size_t blockCount, long long* indexes, const char *blk, size_t len) {
    assert(len % blockCount == 0);
    size_t eachSize = len / blockCount;
    for (unsigned int i = 0; i < blockCount; i++) {
        block ciphertext(blk + (i * eachSize), blk + (i + 1) * eachSize);
        if (isLeft) {
            heapStoreLeft->Write(indexes[i], ciphertext);
        } else {
            heapStoreRight->Write(indexes[i], ciphertext);
        }
    }
}

size_t ocall_nread_heapStore(int isLeft, size_t blockCount, long long* indexes, char *blk, size_t len) {
    (void) len;
    assert(len % blockCount == 0);
    size_t resLen = -1;
    for (unsigned int i = 0; i < blockCount; i++) {
        block ciphertext;
        if (isLeft) {
            ciphertext = heapStoreLeft->Read(indexes[i]);
        } else {
            ciphertext = heapStoreRight->Read(indexes[i]);
        }
        resLen = ciphertext.size();
        std::memcpy(blk + i * resLen, ciphertext.data(), ciphertext.size());
    }
    return resLen;
}

void ocall_initialize_heapStore(int isLeft, long long begin, long long end, const char *blk, size_t len) {
    block ciphertext(blk, blk + len);
    for (long long i = begin; i < end; i++) {
        if (isLeft) {
            heapStoreLeft->Write(i, ciphertext);
        } else {
            heapStoreRight->Write(i, ciphertext);
        }
    }
}

void ocall_write_heapStore(int isLeft, long long index, const char *blk, size_t len) {
    block ciphertext(blk, blk + len);
    if (isLeft) {
        heapStoreLeft->Write(index, ciphertext);
    } else {
        heapStoreRight->Write(index, ciphertext);
    }
}

void ocall_setup_ramStore(int isLeft, size_t num, int size) {
    logger(DBG, "Setup %s with blockCount=%d and storeBlockSize=%d",
           isLeft ? "RAMStoreLeft":"RAMStoreRight", num, size);
    if (isLeft) {
        if (storeLeft == NULL) {
            if (size != -1) {
                storeLeft = new RAMStore(num, size, false);
            } else {
                storeLeft = new RAMStore(num, size, true);
            }
        }
    } else {
        if (storeRight == NULL) {
            if (size != -1) {
                storeRight = new RAMStore(num, size, false);
            } else {
                storeRight = new RAMStore(num, size, true);
            }
        }
    }
}

void ocall_nwrite_ramStore(int isLeft, size_t blockCount, uint32_t * indexes, const char *blk, size_t len) {
    assert(len % blockCount == 0);
    size_t eachSize = len / blockCount;
    for (unsigned int i = 0; i < blockCount; i++) {
        block ciphertext(blk + (i * eachSize), blk + (i + 1) * eachSize);
        if (isLeft) {
            storeLeft->Write(indexes[i], ciphertext);
        } else {
            storeRight->Write(indexes[i], ciphertext);
        }
    }
}

void ocall_write_rawRamStore(int isLeft, long long index, const char *blk, size_t len) {
    size_t eachSize = len;
    block ciphertext(blk, blk + eachSize);
    if (isLeft) {
        storeLeft->WriteRawStore(index, ciphertext);
    } else {
        storeRight->WriteRawStore(index, ciphertext);
    }
}

void ocall_nwrite_rawRamStore(int isLeft, size_t blockCount, long long* indexes, const char *blk, size_t len) {
    assert(len % blockCount == 0);
    size_t eachSize = len / blockCount;
    for (unsigned int i = 0; i < blockCount; i++) {
        block ciphertext(blk + (i * eachSize), blk + (i + 1) * eachSize);
        if (isLeft) {
            storeLeft->WriteRawStore(indexes[i], ciphertext);
        } else {
            storeRight->WriteRawStore(indexes[i], ciphertext);
        }
    }
}

void ocall_nwrite_ramStore_by_client(int isLeft, vector<long long>* indexes, vector<block>* ciphertexts) {
    for (unsigned int i = 0; i < (*indexes).size(); i++) {
        if (isLeft) {
            storeLeft->Write((*indexes)[i], (*ciphertexts)[i]);
        } else {
            storeRight->Write((*indexes)[i], (*ciphertexts)[i]);
        }
    }
}

void ocall_nwrite_raw_ramStore(int isLeft, vector<block>* ciphertexts) {
    for (unsigned int i = 0; i < (*ciphertexts).size(); i++) {
        if (isLeft) {
            storeLeft->WriteRawStore(i, (*ciphertexts)[i]);

        } else {
            storeRight->WriteRawStore(i, (*ciphertexts)[i]);
        }
    }
}

size_t ocall_nread_ramStore(int isLeft, size_t blockCount, uint32_t* indexes, char *blk, size_t len) {
    (void) len;
    assert(len % blockCount == 0);
    size_t resLen = -1;
    for (unsigned int i = 0; i < blockCount; i++) {
        block ciphertext;
        if (isLeft) {
            ciphertext = storeLeft->Read(indexes[i]);
        } else {
            ciphertext = storeRight->Read(indexes[i]);
        }
        resLen = ciphertext.size();
        std::memcpy(blk + i * resLen, ciphertext.data(), ciphertext.size());
    }
    return resLen;
}

size_t ocall_read_rawRamStore(int isLeft, size_t index, char *blk, size_t len) {
    (void) (len);
    size_t resLen = -1;
    block ciphertext;
    if (isLeft) {
        ciphertext = storeLeft->ReadRawStore(index);
    } else {
        ciphertext = storeRight->ReadRawStore(index);
    }
    resLen = ciphertext.size();
    std::memcpy(blk, ciphertext.data(), ciphertext.size());
    return resLen;
}

size_t ocall_nread_rawRamStore(int isLeft, size_t blockCount, size_t begin, char *blk, size_t len) {
    (void) len;
    assert(len % blockCount == 0);
    size_t resLen = -1;
    size_t rawSize;
    if (isLeft) {
        rawSize = storeLeft->tmpstore.size();
    } else {
        rawSize = storeRight->tmpstore.size();
    }
    for (unsigned int i = 0; i < blockCount && (begin + i) < rawSize; i++) {
        block ciphertext;
        if (isLeft) {
            ciphertext = storeLeft->ReadRawStore(i + begin);
        } else {
            ciphertext = storeRight->ReadRawStore(i + begin);
        }
        resLen = ciphertext.size();
        std::memcpy(blk + i * resLen, ciphertext.data(), ciphertext.size());
    }
    return resLen;
}

void ocall_initialize_ramStore(int isLeft, long long begin, long long end, const char *blk, size_t len) {
    block ciphertext(blk, blk + len);
    for (long long i = begin; i < end; i++) {
        if (isLeft) {
            storeLeft->Write(i, ciphertext);
        } else {
            storeRight->Write(i, ciphertext);
        }
    }
}

void ocall_write_ramStore(int isLeft, uint32_t index, const char *blk, size_t len) {
    block ciphertext(blk, blk + len);
    if (isLeft) {
        storeLeft->Write(index, ciphertext);
    } else {
        storeRight->Write(index, ciphertext);
    }
}

void ocall_start_timer(int timerID) {
    Utilities::startTimer(timerID);
}

double ocall_stop_timer(int timerID) {
    return Utilities::stopTimer(timerID);
}

