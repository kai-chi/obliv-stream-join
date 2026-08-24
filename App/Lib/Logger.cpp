#include "Logger.h"
#include <stdio.h>
#include <time.h>
#include <stdarg.h>

struct timespec ts_start;
int CSV_LOG = 0;

static const char* level_strings[] = {
        "INFO",
        "WARN",
        "ERROR",
        "ENCL",
        "DEBUG",
        "PCM",
        "CSV"
};

static char const * get_log_color(LEVEL level) {
    switch (level) {
        case INFO:
            return ANSI_COLOR_GREEN;
        case WARN:
            return ANSI_COLOR_YELLOW;
        case ERROR:
            return ANSI_COLOR_RED;
        case ENCLAVE:
            return ANSI_COLOR_MAGENTA;
        case DBG:
            return ANSI_COLOR_WHITE;
        case PCMLOG:
        case CSVLOG:
            return ANSI_COLOR_BLUE;
        default:
            return "";
    }
}

void initLogger(int csv_log)
{
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    CSV_LOG = csv_log;
    setbuf(stdout, NULL);
}

void logger(LEVEL level, const char *fmt, ...) {
#ifndef DEBUG
    if (level == DBG) return;
#endif
    char buffer[BUFSIZ] = { '\0' };
    const char* color;
    va_list args;
    double time;
    struct timespec tw;

    va_start(args, fmt);
    vsnprintf(buffer, BUFSIZ, fmt, args);

    if (CSV_LOG) {
        if (level == CSVLOG) {
            printf("%s\n", buffer);
            return;
        } else {
            return;
        }
    }

    color = get_log_color(level);

//    time = (double) clock() / CLOCKS_PER_SEC;
    clock_gettime(CLOCK_MONOTONIC, &tw);
    time = (1000.0*(double)tw.tv_sec + 1e-6*(double)tw.tv_nsec)
            - (1000.0*(double)ts_start.tv_sec + 1e-6*(double)ts_start.tv_nsec);
    time /= 1000;


    printf("%s[%8.4f][%5s] %s" ANSI_COLOR_RESET "\n",
           color,
           time,
           level_strings[level],
           buffer);
}