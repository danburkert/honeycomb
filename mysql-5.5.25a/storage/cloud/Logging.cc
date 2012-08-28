#include "Logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "my_pthread.h" 
static FILE* log_file;
static pthread_mutex_t log_lock;

char* time_string()
{
  time_t current_time;
  time(&current_time);
  char* time_string = ctime(&current_time);
  int time_length = strlen(time_string);
  time_string[time_length - 1] = '\0';
  return time_string;
}

void setup_logging(const char* log_path)
{
  const char* path;
  if(log_path == NULL)
  {
    path = DEFAULT_LOG_PATH;
  }
  else
  {
    path = log_path;
  }

  pthread_mutex_init(&log_lock, NULL);
  log_file = fopen(path, "w");
  if (log_file == NULL)
  {
    fprintf(stderr, "Log file %s could not be opened.", path);
  }
  else
  {
    fprintf(log_file, "INFO %s - Log opened\n", time_string());
  }
}

void close_logging()
{
  if (log_file != NULL)
  {
    fclose(log_file);
  }
}

void log_print(const char* level, const char* format, ...)
{
  va_list args;
  va_start(args,format);

  pthread_mutex_lock(&log_lock);
  fprintf(log_file, "%s %s - ", level, time_string());
  vfprintf(log_file, format, args);
  fprintf(log_file, "\n");
  fflush(log_file);
  pthread_mutex_unlock(&log_lock);

  va_end(args);
}
