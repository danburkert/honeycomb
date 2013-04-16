#ifndef SETTINGS_H
#define SETTINGS_H
#include <jni.h>
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
     * @brief Reads in the options from a file.
     *
     * @param filename Path to xml configuration file
     *
     * @return Parser for file
     */
    Settings(const char* filename, const char* schema);

    /**
     * @brief Release resources held by options.
     *
     * @param settings Option settings
     */
    virtual ~Settings();

    /**
     * @brief Retrieve the JNI options found in the file.
     *
     * @param settings Option settings
     *
     * @return JNI options
     */
    JavaVMOption* get_options() const;

    /**
     * @brief Retrieve the number of options found in the file.
     *
     * @param settings Option settings
     *
     * @return Number of options found
     */
    unsigned int get_optioncount() const;

    /**
     * @brief Retrieves the error message from reading. Returns NULL if there was no error.
     *
     * @param settings Option settings
     *
     * @return Error during reading
     */
    const char* get_errormessage() const;

    /**
     * @brief Describes whether there was an error while trying to read the options.
     *
     * @param settings Option settings
     *
     * @return Was an error during reading
     */
    bool has_error() const;
};
#endif
