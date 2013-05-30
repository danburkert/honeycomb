/*
 * Copyright (C) 2013 Altamira Corporation
 *
 * This file is part of Honeycomb Storage Engine.
 *
 * Honeycomb Storage Engine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Honeycomb Storage Engine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Honeycomb Storage Engine.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "Logging.h"
#include "Util.h"

namespace Logging
{
  /**
   * @brief File to log out to
   */
  static FILE* log_file;

  /**
   * @brief Logging lock
   */
  static pthread_mutex_t log_lock;

  void time_string(char* buffer)
  {
    time_t current_time;
    time(&current_time);
    ctime_r(&current_time, buffer);
    int time_length = strlen(buffer);
    buffer[time_length - 1] = '\0';
  }

  void setup_logging(const char* path)
  {
    log_file = fopen(path, "a");
    if (log_file == NULL)
    {
      fprintf(stderr, "Error \"%s\" while trying to open log file %s. Falling back to stderr.\n", strerror(errno), path);
      log_file = stderr;
    }
    else
    {
      fprintf(stderr, "Detailed logging output configured to: %s\n", path);
    }

    pthread_mutex_init(&log_lock, NULL);
    info("Log opened");
  }

  void close_logging()
  {
    if (log_file != NULL)
    {
      fclose(log_file);
    }

    pthread_mutex_destroy(&log_lock);
  }

  void vlog_print(const char* level, const char* format, va_list args)
  {
    char time_str[128];
    pthread_mutex_lock(&log_lock);
    time_string(time_str);
    fprintf(log_file, "%s %s - ", level, time_str);
    vfprintf(log_file, format, args);
    fprintf(log_file, "\n");
    fflush(log_file);
    pthread_mutex_unlock(&log_lock);
  }

  void print(const char* level, const char* format, ...)
  {
    va_list args;
    va_start(args,format);

    vlog_print(level, format, args);

    va_end(args);
  }

  void info(const char* format, ...)
  {
    va_list args;
    va_start(args,format);

    vlog_print("INFO", format, args);

    va_end(args);
  }

  void warn(const char* format, ...)
  {
    va_list args;
    va_start(args,format);

    vlog_print("WARN", format, args);

    va_end(args);
  }

  void error(const char* format, ...)
  {
    va_list args;
    va_start(args,format);

    vlog_print("ERROR", format, args);

    va_end(args);
  }

  void fatal(const char* format, ...)
  {
    va_list args;
    va_start(args,format);

    vlog_print("FATAL", format, args);

    va_end(args);
  }
}
