#ifndef OPTIONPARSER_H
#define OPTIONPARSER_H
#include <jni.h>
typedef struct st_optionparser OptionParser;

/**
 * @brief Reads in the options from a file.
 *
 * @param filename Path to xml configuration file
 *
 * @return Parser for file
 */
OptionParser* new_parser(const char* filename);

/**
 * @brief Release resources held by options.
 *
 * @param parser Option parser
 */
void free_parser(OptionParser* parser);

/**
 * @brief Retrieve the JNI options found in the file.
 *
 * @param parser Option parser
 *
 * @return JNI options
 */
JavaVMOption* get_options(OptionParser* parser);

/**
 * @brief Retrieve the number of options found in the file.
 *
 * @param parser Option parser
 *
 * @return Number of options found
 */
unsigned int get_optioncount(OptionParser* parser);

/**
 * @brief Retrieves the error message from reading. Returns NULL if there was no error.
 *
 * @param parser Option parser
 *
 * @return Error during reading
 */
char* get_errormessage(OptionParser* parser);

/**
 * @brief Describes whether there was an error while trying to read the options.
 *
 * @param parser Option parser
 *
 * @return Was an error during reading
 */
bool has_error(OptionParser* parser);
#endif
