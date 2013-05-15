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
  bool try_setup_logging(const char* path);
  void close_logging();
  void print(const char* level, const char* format, ...);
  void info(const char* format, ...);
  void warn(const char* format, ...);
  void error(const char* format, ...);
  void fatal(const char* format, ...);
}

#endif
