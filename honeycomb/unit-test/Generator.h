#ifndef GENERATOR_H
#define GENERATOR_H

/**
 * Generate random byte string of required length
 */
void gen_random_bytes(char *s, const int len) {
  size_t int_size = sizeof(int);

  for (int i = 0; i < len; ++i) {
    int rint = rand();
    s[i] = ((char*) &rint)[0]; // grab LSB of random int
  }
}

/**
 * Generate random C string of required length
 */
void gen_random_string(char *s, const int len) {
  size_t int_size = sizeof(int);
  for (int i = 0; i < (len - 1); ++i) {
    int rint = rand();
    s[i] = ((char*) &rint)[0]; // grab LSB of random int
    s[i] = (s[i] == 0x00) ? 0x01 : s[i]; // ensure not null byte
  }
  s[len - 1] = 0;
}

#endif
