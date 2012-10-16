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
    if (arr->alloc_len == 0) {
	arr->alloc_len = init_array_size;
	arr->arr = new keyvals_len_t[arr->alloc_len];
    } else if (arr->len == arr->alloc_len) {
	arr->alloc_len *= 2;
	assert(arr->arr = (keyvals_len_t *)
	       realloc(arr->arr, arr->alloc_len * sizeof(keyvals_len_t)));
    }
    arr->arr[arr->len].key = key;
    arr->arr[arr->len].vals = vals;
    arr->arr[arr->len].len = len;
    arr->len++;
}

uint64_t pch_kvslenarray::pch_get_len(void *coll)
{
    assert(coll);
    return ((keyvals_len_arr_t *) coll)->len;
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
    return ((keyvals_len_arr_t *) coll)->arr;
}

void *pch_kvslenarray::pch_get_key(const void *pair)
{
    return ((keyvals_len_t *) pair)->key;
}

void pch_kvslenarray::pch_set_elems(void *coll, void *elems, int len)
{
    keyvals_len_arr_t *arr = (keyvals_len_arr_t *) coll;
    arr->arr = (keyvals_len_t *)elems;
    arr->len = len;
    arr->alloc_len = len;
}

void pch_kvslenarray::pch_shallow_free(void *coll)
{
    assert(coll);
    keyvals_len_arr_t *arr = (keyvals_len_arr_t *) coll;
    if (arr->arr) {
	free(arr->arr);
	arr->len = 0;
	arr->arr = NULL;
    }
}
