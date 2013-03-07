#ifndef GENERATOR_H
#define GENERATOR_H

/**
 * Generate random byte string of required length
 */
void gen_random_bytes(char *s, const int len);

/**
 * Generate random C string of required length
 */
void gen_random_string(char *s, const int len);

#endif
