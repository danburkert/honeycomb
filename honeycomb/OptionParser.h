#ifndef OPTIONPARSER_H
#define OPTIONPARSER_H
#include <jni.h>
typedef struct st_options Option;

/**
 * @brief Reads in the options from a file.
 *
 * @param filename Path to xml configuration file
 *
 * @return Options in file
 */
Option* new_options(const char* filename);

/**
 * @brief Release resources held by options.
 *
 * @param options Options
 */
void free_options(Option* options);

/**
 * @brief Retrieve the JNI options found in the file.
 *
 * @param options Options
 *
 * @return JNI options
 */
JavaVMOption* get_options(Option* options);

/**
 * @brief Retrieve the number of options found in the file.
 *
 * @param options Options
 *
 * @return Number of options found
 */
unsigned int get_optioncount(Option* options);

/**
 * @brief Retrieves the error message from reading. Returns NULL if there was no error.
 *
 * @param options Options
 *
 * @return Error during reading
 */
char* get_errormessage(Option* options);

/**
 * @brief Describes whether there was an error while trying to read the options.
 *
 * @param options Options
 *
 * @return Was an error during reading
 */
bool has_error(Option* options);
#endif
