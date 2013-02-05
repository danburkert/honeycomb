#ifndef LOGGING_H
#define LOGGING_H

namespace Logging
{
  void setup_logging(const char* log_path);
  void close_logging();
  void print(const char* level, const char* format, ...);
  void info(const char* format, ...);
  void warn(const char* format, ...);
  void error(const char* format, ...);
  void fatal(const char* format, ...);
}

#endif
