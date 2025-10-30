#ifndef SGXJOINEVALUATION_LOGGER_H
#define SGXJOINEVALUATION_LOGGER_H

#include "LoggerTypes.h"

void initLogger(int csv_log);

void logger(LEVEL level, const char *fmt, ...);

#endif //SGXJOINEVALUATION_LOGGER_H
