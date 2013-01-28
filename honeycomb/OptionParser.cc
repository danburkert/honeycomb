#include "OptionParser.h"

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xmlschemas.h>
#include <cstring>
#include <cstdlib>
#include <cctype>

const char* schema = "/etc/mysql/honeycomb.xsd";

struct st_options
{
  JavaVMOption* options;
  unsigned int count;
  bool has_error;
  char* error_message;
  xmlErrorPtr error;
};

static void format_error(Option* options, int size, const char* message, ...)
{
    va_list args;
    va_start(args,message);
    options->error_message = (char*)std::malloc(size); 
    vsnprintf(options->error_message, size, message, args);
    options->has_error = true;
    va_end(args);
}
/**
 * @brief Ensure that the configuration file is there and readable.
 *
 * @param config_file Configuration file path
 */
static bool test_config_file(Option* options, const char* config_file)
{
  FILE* config = fopen(config_file, "r");
  if(config != NULL)
  {
    fclose(config);
    return true;
  }
  else
  {
    const char* message = "Could not open \"%s\". File must be readable.";
    int size = strlen(message) + strlen(config_file) + 1;
    format_error(options, size, message, config_file);
    return false;
  }
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
  xmlSchemaParserCtxtPtr parser_ctxt = NULL; 
  xmlSchemaPtr schema = NULL; 
  int rc = 0, is_valid;

  schema_doc = xmlReadFile(schema_filename, NULL, XML_PARSE_NONET);
  if (schema_doc == NULL) {
    /* the schema cannot be loaded or is not well-formed */
    rc = -1;
    goto cleanup;
  }
  parser_ctxt = xmlSchemaNewDocParserCtxt(schema_doc);
  if (parser_ctxt == NULL) {
    /* unable to create a parser context for the schema */
    rc = -2;
    goto cleanup;
  }
  schema = xmlSchemaParse(parser_ctxt);
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
  if (parser_ctxt != NULL) { xmlSchemaFreeParserCtxt(parser_ctxt); }
  if (schema_doc != NULL) { xmlFreeDoc(schema_doc); }

  return rc;
}

static void extract_values(Option* options, xmlDocPtr doc, xmlNodeSetPtr option_nodes)
{
  for(int i = 0; i < options->count; i++)
  {
    xmlNodePtr current_option = option_nodes->nodeTab[i];
    char* opt = (char*)xmlNodeListGetString(doc, current_option->xmlChildrenNode, 1);
    options->options[i].optionString = opt != NULL ? ltrim(rtrim(opt)) : NULL;
  }
}

static void read_options(Option* options, const char* filename) 
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
  options->count = option_nodes->nodeNr;
  options->options = (JavaVMOption*)malloc(options->count * sizeof(JavaVMOption));
  if (options == NULL)
  {
    const char* error = "Could not allocate memory.";
    format_error(options, strlen(error) + 1, error);
    goto cleanup;
  }

  extract_values(options, doc, option_nodes);
  goto cleanup;

error:
  options->error = xmlGetLastError();
  if (options->error != NULL)
  {
    options->has_error = true;
  }

cleanup:
  if(option_nodes != NULL) { xmlXPathFreeNodeSet(option_nodes); }
  if(xpath_ctx != NULL) { xmlXPathFreeContext(xpath_ctx); }
  if(doc != NULL) { xmlFreeDoc(doc); }
  xmlCleanupParser();
}

unsigned int get_optioncount(Option* options)
{
  return options->count;
}

JavaVMOption* get_options(Option* options)
{
  return options->options;
}

char* get_errormessage(Option* options)
{
  if(!options->has_error)
  {
    return NULL;
  }

  if(options->error != NULL)
  {
    return options->error->message;
  }

  return options->error_message;
}

bool has_error(Option* options)
{
  return options->has_error;
}

Option* new_options(const char* filename) 
{
  Option* options = (Option*)std::calloc(1, sizeof(Option));
  if (!test_config_file(options, filename))
  {
    return options;
  }

  read_options(options, filename);

  return options;
}

void free_options(Option* options)
{
  if (options != NULL)
  {
    for(int i = 0; i < options->count; i++)
    {
      if (options->options[i].optionString != NULL)
      {
        xmlFree(options->options[i].optionString);
      }
    }

    free(options->options);
    options->options = NULL;
    free(options);
    options = NULL;
  }
}
