#ifndef VALUES_H
#define VALUES_H

#include "mr-types.hh"

void map_values_insert(keyvals_t * kvs, void *val);
void map_values_mv(keyvals_t * dst, keyvals_t * src);
void map_values_mv(keyvals_t *dst, keyvals_len_t *src);
void map_values_mv(keyvals_t *dst, keyval_t *src);

#endif
