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


#ifndef LOGGING_H
#define LOGGING_H

namespace Logging
{
  /**
   * @brief Open the logger to the specified path. If the path cannot
   * be open then fallback to stderr
   *
   * @param path Logging path
   */
  void setup_logging(const char* path);

  /**
   * @brief Close the current logger
   */
  void close_logging();

  /**
   * @brief Print to the log file a message
   *
   * @param level Level of logging
   * @param format Printf format for the log message
   * @param ... Arguments for the format
   */
  void print(const char* level, const char* format, ...);

  /**
   * @brief Print to the log file a info message
   *
   * @param format Printf format for the log message
   * @param ... Arguments for the format
   */
  void info(const char* format, ...);

  /**
   * @brief Print to the log file a warning message
   *
   * @param format Printf format for the log message
   * @param ... Arguments for the format
   */
  void warn(const char* format, ...);

  /**
   * @brief Print to the log file a error message
   *
   * @param format Printf format for the log message
   * @param ... Arguments for the format
   */
  void error(const char* format, ...);

  /**
   * @brief Print to the log file a fatal message
   *
   * @param format Printf format for the log message
   * @param ... Arguments for the format
   */
  void fatal(const char* format, ...);
}

#endif
