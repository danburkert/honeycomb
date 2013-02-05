#include "OptionParser.h"

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xmlschemas.h>
#include <cstring>
#include <cstdlib>
#include <cctype>

static const char* schema = "/etc/mysql/honeycomb.xsd";

struct st_optionparser
{
  JavaVMOption* options;
  unsigned int count;
  bool has_error;
  char* error_message;
  xmlErrorPtr error;
};

static void format_error(OptionParser* parser, int size, const char* message, ...)
{
    va_list args;
    va_start(args,message);
    parser->error_message = (char*)std::malloc(size); 
    vsnprintf(parser->error_message, size, message, args);
    parser->has_error = true;
    va_end(args);
}
/**
 * @brief Ensure that the configuration file is there and readable.
 *
 * @param config_file Configuration file path
 */
static bool test_config_file(OptionParser* parser, const char* config_file)
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
    format_error(parser, size, message, config_file);
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

static void extract_values(OptionParser* parser, xmlDocPtr doc, xmlNodeSetPtr option_nodes)
{
  for(unsigned int i = 0; i < parser->count; i++)
  {
    xmlNodePtr current_option = option_nodes->nodeTab[i];
    char* opt = (char*)xmlNodeListGetString(doc, current_option->xmlChildrenNode, 1);
    parser->options[i].optionString = opt != NULL ? ltrim(rtrim(opt)) : NULL;
  }
}

static void read_options(OptionParser* parser, const char* filename) 
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
  parser->count = option_nodes->nodeNr;
  parser->options = (JavaVMOption*)malloc(parser->count * sizeof(JavaVMOption));
  if (parser == NULL)
  {
    const char* error = "Could not allocate memory.";
    format_error(parser, strlen(error) + 1, error);
    goto cleanup;
  }

  extract_values(parser, doc, option_nodes);
  goto cleanup;

error:
  parser->error = xmlGetLastError();
  if (parser->error != NULL)
  {
    parser->has_error = true;
  }

cleanup:
  if(option_nodes != NULL) { xmlXPathFreeNodeSet(option_nodes); }
  if(xpath_ctx != NULL) { xmlXPathFreeContext(xpath_ctx); }
  if(doc != NULL) { xmlFreeDoc(doc); }
  xmlCleanupParser();
}

unsigned int get_optioncount(OptionParser* parser)
{
  return parser->count;
}

JavaVMOption* get_options(OptionParser* parser)
{
  return parser->options;
}

char* get_errormessage(OptionParser* parser)
{
  if(!parser->has_error)
  {
    return NULL;
  }

  if(parser->error != NULL)
  {
    return parser->error->message;
  }

  return parser->error_message;
}

bool has_error(OptionParser* parser)
{
  return parser->has_error;
}

OptionParser* new_parser(const char* filename) 
{
  OptionParser* parser = (OptionParser*)std::calloc(1, sizeof(OptionParser));
  if (!test_config_file(parser, filename))
  {
    return parser;
  }

  read_options(parser, filename);

  return parser;
}

void free_parser(OptionParser* parser)
{
  if (parser != NULL)
  {
    for(unsigned int i = 0; i < parser->count; i++)
    {
      if (parser->options[i].optionString != NULL)
      {
        xmlFree(parser->options[i].optionString);
      }
    }

    free(parser->options);
    parser->options = NULL;
    free(parser);
    parser = NULL;
  }
}
