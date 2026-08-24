#include "AppTimers.h"
#include <sys/time.h>

AppTimers::AppTimers() {

}

AppTimers::~AppTimers() {

}

uint64_t AppTimers::get_system_micros() {
    struct timeval tv{};
    gettimeofday(&tv, nullptr);
    return tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
}

void AppTimers::startTimer(int id) {
    //uint64_t t = get_system_micros();
    m_timers[id] = get_system_micros();
}

uint64_t AppTimers::stopTimer(int id) {
    uint64_t t = get_system_micros();
    t = t - m_timers[id];
    m_timers[id] = t;
    return t;
}