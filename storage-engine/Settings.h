#ifndef SETTINGS_H
#define SETTINGS_H

struct JavaVMOption;
typedef JavaVMOption JavaVMOption;

class SettingsPrivate;

class Settings
{
  private:
    SettingsPrivate* settings;
    void read_options();
    bool test_config_file(const char* config_file);
    bool test_file_readable(const char* config_file);
    bool test_file_owned_by_mysql(const char* config_file);
  public:
    /**
     * @brief  Reads in the options from a file.
     *
     * @param filename Path to xml configuration file
     * @param schema Path to xml schema
     */
    Settings(const char* filename, const char* schema);

    /**
     * @brief Release resources held by options.
     */
    virtual ~Settings();

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
