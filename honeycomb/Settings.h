#ifndef SETTINGS_H
#define SETTINGS_H
#include <jni.h>
typedef struct st_settings Settings;

/**
 * @brief Reads in the options from a file.
 *
 * @param filename Path to xml configuration file
 *
 * @return Parser for file
 */
Settings* read_settings(const char* filename, const char* schema);

/**
 * @brief Release resources held by options.
 *
 * @param settings Option settings
 */
void free_settings(Settings* settings);

/**
 * @brief Retrieve the JNI options found in the file.
 *
 * @param settings Option settings
 *
 * @return JNI options
 */
JavaVMOption* get_options(Settings* settings);

/**
 * @brief Retrieve the number of options found in the file.
 *
 * @param settings Option settings
 *
 * @return Number of options found
 */
unsigned int get_optioncount(Settings* settings);

/**
 * @brief Retrieves the error message from reading. Returns NULL if there was no error.
 *
 * @param settings Option settings
 *
 * @return Error during reading
 */
char* get_errormessage(Settings* settings);

/**
 * @brief Describes whether there was an error while trying to read the options.
 *
 * @param settings Option settings
 *
 * @return Was an error during reading
 */
bool has_error(Settings* settings);
#endif
