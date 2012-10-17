#include "pchandler.hh"
#include "value_helper.hh"
#include "bench.hh"
#include "pch_kvarray.hh"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif

enum { init_array_size = 8 };

void pch_kvarray::pch_set_util(key_cmp_t keycmp_)
{
}

void pch_kvarray::pch_init(void *coll)
{
    memset(coll, 0, sizeof(keyval_arr_t));
}

int pch_kvarray::pch_insert_kv(void *coll, void *key, void *val, size_t keylen, unsigned hash)
{
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    if (arr->alloc_len == 0) {
	arr->alloc_len = init_array_size;
	arr->arr = new keyval_t[arr->alloc_len];
    } else if (arr->alloc_len == arr->len) {
	arr->alloc_len *= 2;
	assert(arr->arr = (keyval_t *)
	       realloc(arr->arr, arr->alloc_len * sizeof(keyval_t)));
    }
    if (keylen && mrkeycopy)
	arr->arr[arr->len].key = mrkeycopy(key, keylen);
    else
	arr->arr[arr->len].key = key;
    arr->arr[arr->len].val = val;
    arr->arr[arr->len].hash = hash;
    arr->len++;
    return 1;
}

uint64_t pch_kvarray::pch_copy_kv(void *coll, keyval_t * dst)
{
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    memcpy(dst, arr->arr, arr->len * sizeof(keyval_t));
    return arr->len;
}

uint64_t pch_kvarray::pch_get_len(void *coll)
{
    assert(coll);
    return ((keyval_arr_t *) coll)->len;
}

size_t pch_kvarray::pch_get_pair_size(void)
{
    return sizeof(keyval_t);
}

size_t pch_kvarray::pch_get_parr_size(void)
{
    return sizeof(keyval_arr_t);
}

void *pch_kvarray::pch_get_arr_elems(void *coll)
{
    return ((keyval_arr_t *) coll)->arr;
}

void *pch_kvarray::pch_get_key(const void *pair)
{
    return ((keyval_t *) pair)->key;
}

void pch_kvarray::pch_set_elems(void *coll, void *elems, int len)
{
    assert(coll);
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    arr->arr = (keyval_t *)elems;
    arr->len = len;
    arr->alloc_len = len;
}

void pch_kvarray::pch_shallow_free(void *coll)
{
    assert(coll);
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    if (arr->arr) {
	free(arr->arr);
	arr->len = 0;
	arr->arr = NULL;
    }
}

