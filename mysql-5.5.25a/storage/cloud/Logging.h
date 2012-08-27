#ifndef LOGGING_H
#define LOGGING_H

#define DEFAULT_LOG_PATH "/tmp/cloud.log"

void setup_logging(const char* log_path);
void close_logging();
void log_print(const char* level, const char* format, ...);

#endif
