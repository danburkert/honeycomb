#include "Settings.h"

#include "Util.h"
#include <libxml/xpath.h>
#include <libxml/xmlschemas.h>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <pwd.h>
#include <jni.h>

class SettingsPrivate
{
  public:
  JavaVMOption* options;
  unsigned int count;
  bool has_error;
  char* error_message;
  xmlErrorPtr error;
  const char* filename;
  const char* schema;
  SettingsPrivate() : options(NULL), count(0), has_error(false), error_message(NULL), error(NULL), filename(NULL), schema(NULL)
  {}
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

static void format_error(SettingsPrivate* settings, int size, const char* message, ...)
{
    va_list args;
    va_start(args,message);
    settings->error_message = (char*)std::malloc(size);
    vsnprintf(settings->error_message, size, message, args);
    settings->has_error = true;
    va_end(args);
}

bool Settings::test_file_owned_by_mysql(const char* config_file)
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

bool Settings::test_file_readable(const char* config_file)
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
bool Settings::test_config_file(const char* config_file)
{
  return test_file_owned_by_mysql(config_file) &&
    test_file_readable(config_file);
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

static void extract_values(SettingsPrivate* settings, xmlDocPtr doc, xmlNodeSetPtr option_nodes)
{
  for(unsigned int i = 0; i < settings->count; i++)
  {
    xmlNodePtr current_option = option_nodes->nodeTab[i];
    char* opt = (char*)xmlNodeListGetString(doc, current_option->xmlChildrenNode, 1);
    settings->options[i].optionString = opt != NULL ? ltrim(rtrim(opt)) : NULL;
  }
}

void Settings::read_options()
{
  const xmlChar* xpath = (const xmlChar*)"/options/jvmoptions/jvmoption";
  xmlXPathObjectPtr jvm_options;
  xmlXPathContextPtr xpath_ctx;
  xmlDocPtr doc;
  xmlNodeSetPtr option_nodes;

  if (settings == NULL)
  {
    const char* error = "Could not allocate memory.";
    format_error(settings, strlen(error) + 1, error);
    goto cleanup;
  }

  xmlInitParser();
  doc = xmlParseFile(settings->filename);
  if (doc == NULL) { goto error; }

  if(validate_against_schema(doc, settings->schema) != 1) { goto error; }

  xpath_ctx = xmlXPathNewContext(doc);
  if (xpath_ctx == NULL) { goto error; }
  jvm_options = xmlXPathEvalExpression(xpath, xpath_ctx);
  if (jvm_options == NULL) { goto error; }

  option_nodes = jvm_options->nodesetval;
  settings->count = option_nodes->nodeNr;
  settings->options = (JavaVMOption*)malloc(settings->count * sizeof(JavaVMOption));

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

unsigned int Settings::get_optioncount() const
{
  return settings->count;
}

JavaVMOption* Settings::get_options() const
{
  return settings->options;
}

const char* Settings::get_errormessage() const
{
  if(!settings->has_error)
  {
    return "";
  }

  if(settings->error != NULL)
  {
    return settings->error->message;
  }

  return settings->error_message;
}

bool Settings::has_error() const
{
  return settings->has_error;
}

const char* Settings::get_filename() const
{
  return settings->filename;
}

const char* Settings::get_schema() const
{
  return settings->schema;
}

Settings::Settings(const char* filename, const char* schema) : settings(new SettingsPrivate)
{
  if (test_config_file(filename) && test_config_file(schema))
  {
    settings->filename = filename;
    settings->schema = schema;
    settings->has_error = false;
    read_options();
  }
}

Settings::~Settings()
{
  for(unsigned int i = 0; i < settings->count; i++)
  {
    if (settings->options[i].optionString != NULL)
    {
      xmlFree(settings->options[i].optionString);
    }
  }

  free(settings->options);
  delete settings;
}
