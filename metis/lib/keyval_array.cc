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
    void *ik = (keylen && mrkeycopy) ? mrkeycopy(key, keylen) : key;
    keyval_t tmp(ik, val, hash);
    arr->push_back(&tmp);
    return 1;
}

uint64_t pch_kvarray::pch_copy_kv(void *coll, keyval_t * dst)
{
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    return arr->copy(dst);
}

uint64_t pch_kvarray::pch_get_len(void *coll)
{
    assert(coll);
    return ((keyval_arr_t *) coll)->size();
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
    return ((keyval_arr_t *) coll)->array();
}

void *pch_kvarray::pch_get_key(const void *pair)
{
    return ((keyval_t *) pair)->key;
}

void pch_kvarray::pch_set_elems(void *coll, void *elems, int len)
{
    assert(coll);
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    arr->set_array((keyval_t *) elems, len);
}

void pch_kvarray::pch_shallow_free(void *coll)
{
    assert(coll);
    keyval_arr_t *arr = (keyval_arr_t *) coll;
    arr->shallow_free();
}

