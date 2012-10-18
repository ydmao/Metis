#ifndef VALUES_H
#define VALUES_H

#include "mr-types.hh"

void values_insert(keyvals_t * kvs, void *val);
void values_mv(keyvals_t * dst, keyvals_t * src);
void values_mv(keyvals_t *dst, keyvals_len_t *src);
void values_mv(keyvals_t *dst, keyval_t *src);

#endif
