#ifndef CIRCULARVECTOR_HPP
#define CIRCULARVECTOR_HPP

#include <vector>
#include "Enclave.h"
#include <sstream>

using namespace std;

class CircularVector {
private:
    vector<row_t> buffer;
    uint32_t head;
    uint32_t tail;
    uint32_t capacity;

public:
    explicit CircularVector(uint32_t _capacity) {
        this->capacity = _capacity + 1;
        this->head = 0;
        this->tail = 0;
        buffer.resize(_capacity + 1);
    }

    /** Add an element to the buffer */
    void push_back(row_t element) {
        buffer[head] = element;
        head = (head + 1) % capacity;
        if (head == tail) {
            tail = (tail + 1) % capacity;
        }
    }

    void push_back(relation_t *rel) {
        for (uint32_t i = 0; i < rel->num_tuples; i++) {
            push_back(rel->tuples[i]);
        }
    }

    /** Remove an element from the buffer*/
    void pop_front() {
        if (empty()) return;
        tail = (tail + 1) % capacity;
    }

    bool empty() const {
        return head == tail;
    }

    bool full() const {
        return (head + 1) % capacity == tail;
    }

    uint32_t get_capacity() {
        return capacity;
    }

    uint32_t size() const {
        if (head >= tail) {
            return head - tail;
        }
        return capacity - (tail - head);
    };

    void printBuffer() {
        size_t idx = tail;
        while (idx != head) {
            printf("%d ", buffer[idx].key);
            idx = (idx + 1) % capacity;
        }
        printf("%s\n", "");
    }

    string getBuffer() {
        std::ostringstream os;
        size_t idx = tail;
        while (idx != head) {
            os << to_string(buffer[idx].key) << " ";
            idx = (idx + 1) % capacity;
        }
        return os.str();
    }

    vector<row_t> fullScan(type_key key, uint32_t *matches) {
        vector<row_t> res;
        if (empty()) return res;
        res.resize(capacity);
        row_t dummy = {.ts = {0, 0}, .key = 0, .payload = 0};
        size_t idx = tail;
        while(idx != head) {

            if (buffer[idx].key == key) {
                res.push_back(buffer[idx]);
                (*matches)++;
            } else {
                res.push_back(dummy);
            }
            idx = (idx + 1) % capacity;
        }
        return res;
    }

};
#endif //CIRCULARVECTOR_HPP
