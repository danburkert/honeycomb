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


#ifndef SERIALIZABLE_H

#define SERIALIZABLE_H

/**
 * @brief Interface to mark a class able to serialize and deserialize.
 */
class Serializable
{
  public:

    /**
     * @brief Virtual destructor
     */
    virtual ~Serializable(){}

    /**
     * @brief Serialize this class into a buffer
     *
     * @param buf Buffer
     * @param len Length of serialized class
     *
     * @return Success if 0 else error code
     */
    virtual int serialize(const char** buf, size_t* len) = 0;

    /**
     * @brief Deserialize this class from a buffer
     *
     * @param buf Buffer
     * @param len Length of buffer
     *
     * @return Success if 0 else error code
     */
    virtual int deserialize(const char* buf, int64_t len) = 0;
};

#endif
