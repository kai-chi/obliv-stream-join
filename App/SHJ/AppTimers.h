#ifndef APPTIMERS_H
#define APPTIMERS_H

#include <map>
#include <cstdint>

class AppTimers {
private:
    uint64_t get_system_micros();

public:
    AppTimers();
    virtual ~AppTimers();
    void startTimer(int id);
    uint64_t stopTimer (int id);
    std::map<int, uint64_t> m_timers;
};

enum TIMER {
    LEFT_INIT_TIME = 0,
    RIGHT_INIT_TIME = 1,
    LEFT_BUILD_TIME = 2,
    RIGHT_BUILD_TIME = 3,
    LEFT_PROBE_TIME = 4,
    RIGHT_PROBE_TIME = 5,
    LEFT_DELETE_TIME = 6,
    RIGHT_DELETE_TIME = 7,
    JOIN_TOTAL_TIME = 8
};
#endif //APPTIMERS_H
