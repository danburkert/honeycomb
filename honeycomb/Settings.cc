#include "Settings.h"

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xmlschemas.h>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <pwd.h>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "Macros.h"
#include "Util.h"

struct st_settings
{
  JavaVMOption* options;
  unsigned int count;
  bool has_error;
  char* error_message;
  xmlErrorPtr error;
};

static void print_perm(const char* file)
{
  struct stat fileStat;
  if (stat(file,&fileStat) < 0)
  {
    printf("File %s does not appear to exist.\n", file);
    return;
  }

  printf("Information for %s\n",file);
  printf("---------------------------\n");
  printf("File Permissions: \t");
  printf( (S_ISDIR(fileStat.st_mode)) ? "d" : "-");
  printf( (fileStat.st_mode & S_IRUSR) ? "r" : "-");
  printf( (fileStat.st_mode & S_IWUSR) ? "w" : "-");
  printf( (fileStat.st_mode & S_IXUSR) ? "x" : "-");
  printf( (fileStat.st_mode & S_IRGRP) ? "r" : "-");
  printf( (fileStat.st_mode & S_IWGRP) ? "w" : "-");
  printf( (fileStat.st_mode & S_IXGRP) ? "x" : "-");
  printf( (fileStat.st_mode & S_IROTH) ? "r" : "-");
  printf( (fileStat.st_mode & S_IWOTH) ? "w" : "-");
  printf( (fileStat.st_mode & S_IXOTH) ? "x" : "-");
  printf("\n\n");
}

static void format_error(Settings* settings, int size, const char* message, ...)
{
    va_list args;
    va_start(args,message);
    settings->error_message = (char*)std::malloc(size);
    vsnprintf(settings->error_message, size, message, args);
    settings->has_error = true;
    va_end(args);
}

static bool test_file_owned_by_mysql(Settings* settings, const char* config_file)
{
  struct stat fStat;
  if (stat(config_file, &fStat) == -1)
  {
    const char* message = "Could not stat %s. It mostly likely doesn't exist. Check the path";
    int size = strlen(message) + strlen(config_file) + 1;
    format_error(settings, size, message, config_file);
    return false;
  }

  if (!is_owned_by_mysql(config_file))
  {
    struct passwd passwd_entry;
    struct passwd* temp;
    char buffer[256];
    getpwuid_r(getuid(), &passwd_entry, buffer, sizeof(buffer), &temp);
    const char* message = "The file %s must be owned by %s";
    int size = strlen(message) + strlen(config_file) + strlen(passwd_entry.pw_name) + 1;
    format_error(settings, size, message, config_file, passwd_entry.pw_name);
    return false;
  }

  return true;
}

static bool test_file_readable(Settings* settings, const char* config_file)
{
  FILE* config = fopen(config_file, "r");
  if(config != NULL)
  {
    fclose(config);
    return true;
  }
  else
  {
    print_perm(config_file);
    const char* message = "Could not open \"%s\". File must be readable.";
    int size = strlen(message) + strlen(config_file) + 1;
    format_error(settings, size, message, config_file);
    return false;
  }
}
/**
 * @brief Ensure that the configuration file is there and readable.
 *
 * @param config_file Configuration file path
 */
static bool test_config_file(Settings* settings, const char* config_file)
{
  return test_file_owned_by_mysql(settings, config_file) &&
    test_file_readable(settings, config_file);
}

/**
 * Trim whitespace from right of string.
 */
static char* rtrim(char* string)
{
  char* original = string + std::strlen(string);
  while(std::isspace(*--original));
  *(original + 1) = '\0';
  return string;
}

/**
 * Trim whitespace from left of string.
 */
static char* ltrim(char *string)
{
  char* original = string;
  char *p = original;
  int trimmed = 0;
  do
  {
    if (!std::isspace(*original) || trimmed)
    {
      trimmed = 1;
      *p++ = *original;
    }
  }
  while (*original++ != '\0');
  return string;
}

/**
 * @brief Validates an xml document against a schema
 *
 * @param doc XML document
 * @param schema_filename Name of the xml schema file
 *
 * @return 1 if valid else 0. Less than zero if error during validation.
 */
static int validate_against_schema(const xmlDocPtr doc, const char *schema_filename)
{
  xmlDocPtr schema_doc = NULL;
  xmlSchemaValidCtxtPtr valid_ctxt = NULL;
  xmlSchemaParserCtxtPtr settings_ctxt = NULL;
  xmlSchemaPtr schema = NULL;
  int rc = 0, is_valid;

  schema_doc = xmlReadFile(schema_filename, NULL, XML_PARSE_NONET);
  if (schema_doc == NULL) {
    /* the schema cannot be loaded or is not well-formed */
    rc = -1;
    goto cleanup;
  }
  settings_ctxt = xmlSchemaNewDocParserCtxt(schema_doc);
  if (settings_ctxt == NULL) {
    /* unable to create a settings context for the schema */
    rc = -2;
    goto cleanup;
  }
  schema = xmlSchemaParse(settings_ctxt);
  if (schema == NULL) {
    /* the schema itself is not valid */
    rc = -3;
    goto cleanup;
  }
  valid_ctxt = xmlSchemaNewValidCtxt(schema);
  if (valid_ctxt == NULL) {
    /* unable to create a validation context for the schema */
    rc = -4;
    goto cleanup;
  }

  is_valid = (xmlSchemaValidateDoc(valid_ctxt, doc) == 0);
  rc = is_valid ? 1 : 0;
cleanup:
  if (valid_ctxt != NULL) { xmlSchemaFreeValidCtxt(valid_ctxt); }
  if (schema != NULL) { xmlSchemaFree(schema); }
  if (settings_ctxt != NULL) { xmlSchemaFreeParserCtxt(settings_ctxt); }
  if (schema_doc != NULL) { xmlFreeDoc(schema_doc); }

  return rc;
}

static void extract_values(Settings* settings, xmlDocPtr doc, xmlNodeSetPtr option_nodes)
{
  for(unsigned int i = 0; i < settings->count; i++)
  {
    xmlNodePtr current_option = option_nodes->nodeTab[i];
    char* opt = (char*)xmlNodeListGetString(doc, current_option->xmlChildrenNode, 1);
    settings->options[i].optionString = opt != NULL ? ltrim(rtrim(opt)) : NULL;
  }
}

static void read_options(Settings* settings, const char* filename, const char* schema)
{
  const xmlChar* xpath = (const xmlChar*)"/options/jvmoptions/jvmoption";
  xmlXPathObjectPtr jvm_options;
  xmlXPathContextPtr xpath_ctx;
  xmlDocPtr doc;
  xmlNodeSetPtr option_nodes;

  xmlInitParser();
  doc = xmlParseFile(filename);
  if (doc == NULL) { goto error; }

  if(validate_against_schema(doc, schema) != 1) { goto error; }

  xpath_ctx = xmlXPathNewContext(doc);
  if (xpath_ctx == NULL) { goto error; }
  jvm_options = xmlXPathEvalExpression(xpath, xpath_ctx);
  if (jvm_options == NULL) { goto error; }

  option_nodes = jvm_options->nodesetval;
  settings->count = option_nodes->nodeNr;
  settings->options = (JavaVMOption*)malloc(settings->count * sizeof(JavaVMOption));
  if (settings == NULL)
  {
    const char* error = "Could not allocate memory.";
    format_error(settings, strlen(error) + 1, error);
    goto cleanup;
  }

  extract_values(settings, doc, option_nodes);
  goto cleanup;

error:
  settings->error = xmlGetLastError();
  if (settings->error != NULL)
  {
    settings->has_error = true;
  }

cleanup:
  if(option_nodes != NULL) { xmlXPathFreeNodeSet(option_nodes); }
  if(xpath_ctx != NULL) { xmlXPathFreeContext(xpath_ctx); }
  if(doc != NULL) { xmlFreeDoc(doc); }
  xmlCleanupParser();
}

unsigned int get_optioncount(Settings* settings)
{
  return settings->count;
}

JavaVMOption* get_options(Settings* settings)
{
  return settings->options;
}

char* get_errormessage(Settings* settings)
{
  if(!settings->has_error)
  {
    return NULL;
  }

  if(settings->error != NULL)
  {
    return settings->error->message;
  }

  return settings->error_message;
}

bool has_error(Settings* settings)
{
  return settings->has_error;
}

Settings* read_settings(const char* filename, const char* schema)
{
  Settings* settings = (Settings*)std::calloc(1, sizeof(Settings));
  if (!test_config_file(settings, filename))
  {
    return settings;
  }

  if (!test_config_file(settings, schema))
  {
    return settings;
  }

  read_options(settings, filename, schema);

  return settings;
}

void free_settings(Settings* settings)
{
  if (settings != NULL)
  {
    for(unsigned int i = 0; i < settings->count; i++)
    {
      if (settings->options[i].optionString != NULL)
      {
        xmlFree(settings->options[i].optionString);
      }
    }

    free(settings->options);
    settings->options = NULL;
    free(settings);
    settings = NULL;
  }
}
