#include "EnclaveTimers.h"
#include "Enclave.h"
#include "Enclave_t.h"

EnclaveTimers::EnclaveTimers() {

}

EnclaveTimers::~EnclaveTimers() {

}

void EnclaveTimers::startTimer(int id) {
    uint64_t t;
    ocall_get_system_micros(&t);
    m_timers[id] = t;
}

uint64_t EnclaveTimers::stopTimer(int id) {
    uint64_t t;
    ocall_get_system_micros(&t);
    t = t - m_timers[id];
    m_timers[id] = t;
    return t;
}