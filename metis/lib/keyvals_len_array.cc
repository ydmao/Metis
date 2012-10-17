#include <assert.h>
#include <string.h>
#include "pchandler.hh"
#include "apphelper.hh"
#include "bench.hh"
#include "pch_kvslenarray.hh"

enum { init_array_size = 8 };

void pch_kvslenarray::pch_init(void *node)
{
    memset(node, 0, sizeof(keyvals_len_arr_t));
}

void pch_kvslenarray::pch_insert_kvslen(void *coll, void *key, void **vals, uint64_t len)
{
    keyvals_len_arr_t *arr = (keyvals_len_arr_t *) coll;
    keyvals_len_t tmp(key, vals, len);
    arr->push_back(&tmp);
}

uint64_t pch_kvslenarray::pch_get_len(void *coll)
{
    assert(coll);
    return ((keyvals_len_arr_t *) coll)->size();
}

size_t pch_kvslenarray::pch_get_pair_size(void)
{
    return sizeof(keyvals_len_t);
}

size_t pch_kvslenarray::pch_get_parr_size(void)
{
    return sizeof(keyvals_len_arr_t);
}

void *pch_kvslenarray::pch_get_arr_elems(void *coll)
{
    return ((keyvals_len_arr_t *) coll)->array();
}

void *pch_kvslenarray::pch_get_key(const void *pair)
{
    return ((keyvals_len_t *) pair)->key;
}

void pch_kvslenarray::pch_set_elems(void *coll, void *elems, int len)
{
    keyvals_len_arr_t *arr = (keyvals_len_arr_t *) coll;
    arr->set_array((keyvals_len_t *)elems, len);
}

void pch_kvslenarray::pch_shallow_free(void *coll)
{
    assert(coll);
    keyvals_len_arr_t *arr = (keyvals_len_arr_t *) coll;
    arr->shallow_free();
}
