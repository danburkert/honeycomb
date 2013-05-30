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


#ifndef SETTINGS_H
#define SETTINGS_H

struct JavaVMOption;
typedef JavaVMOption JavaVMOption;

class SettingsPrivate;

/**
 * @brief Holds settings read from a configuration file.
 */
class Settings
{
  private:
    SettingsPrivate* settings;
    void read_options();
    bool test_config_file(const char* config_file);
    bool test_file_readable(const char* config_file);
    bool test_file_owned_by_mysql(const char* config_file);
  public:
    Settings();

    /**
     * @brief Release resources held by options.
     */
    virtual ~Settings();

    
    /**
     * @brief Try to load settings from a file and schema
     *
     * @param filename
     * @param schema
     *
     * @return 
     */
    bool try_load(char* filename, char* schema);

    /**
     * @brief Retrieve the JNI options found in the file.
     *
     * @return JNI options
     */
    JavaVMOption* get_options() const;

    /**
     * @brief Retrieve the number of options found in the file.
     *
     * @return Number of options found
     */
    unsigned int get_optioncount() const;

    /**
     * @brief Retrieves the error message from reading. Returns NULL if there was no error.
     *
     * @return Error during reading
     */
    const char* get_errormessage() const;

    /**
     * @brief Describes whether there was an error while trying to read the options.
     *
     * @return Was an error during reading
     */
    bool has_error() const;

    /**
     * @brief Retrieve the xml file name
     *
     * @return XML file name
     */
    const char* get_filename() const;

    /**
     * @brief Retrieve the xml schema file name
     *
     * @return XML schema file name
     */
    const char* get_schema() const;
};
#endif
