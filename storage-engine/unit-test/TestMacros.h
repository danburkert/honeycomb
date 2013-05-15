#ifndef MACROS_H
#define MACROS_H
#define assert_that(call, msg) \
  do { \
    if (!call) { \
      fprintf(stderr, msg "\n"); \
      return -1; \
    } \
  } while (0)

#define try(call, msg) \
  do { \
    if (call) { \
      fprintf(stderr, msg "\n"); \
    } \
  } while (0)
#endif
