#include "pch_kvsarray.hh"
#include "bsearch.hh"
#include "value_helper.hh"
#include "bench.hh"
#include <string.h>
#include <assert.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif

static key_cmp_t JSHARED_ATTR keycmp = 0;
enum { init_array_size = 8 };

void pch_kvsarray::pch_set_util(key_cmp_t keycmp_)
{
    keycmp = keycmp_;
}

void pch_kvsarray::pch_init(void *coll)
{
    memset(coll, 0, sizeof(keyvals_arr_t));
}

static int
keyvals_cmp(const void *k1, const void *k2)
{
    keyvals_t *p1 = (keyvals_t *) k1;
    keyvals_t *p2 = (keyvals_t *) k2;
    return keycmp(p1->key, p2->key);
}

void pch_kvsarray::pch_append_kvs(void *coll, const keyvals_t * kvs)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    arr->push_back(kvs);
}

void pch_kvsarray::pch_insert_kvs(void *coll, const keyvals_t * kvs)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    assert(arr->insert_new(kvs, keyvals_cmp));
}

int pch_kvsarray::pch_insert_kv(void *coll, void *key, void *val, size_t keylen, unsigned hash)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    keyvals_t tmp(key, hash);
    int pos = 0;
    bool bnew = arr->insert_new(&tmp, keyvals_cmp, &pos);
    if (bnew && keylen && mrkeycopy)
	(*arr)[pos].key = mrkeycopy(key, keylen);
    values_insert(&(*arr)[pos], val);
    return bnew;
}

uint64_t pch_kvsarray::pch_get_len(void *coll)
{
    assert(coll);
    return ((keyvals_arr_t *) coll)->size();
}

size_t pch_kvsarray::pch_get_pair_size(void)
{
    return sizeof(keyvals_t);
}

size_t pch_kvsarray::pch_get_parr_size(void)
{
    return sizeof(keyvals_arr_t);
}

void *pch_kvsarray::pch_get_arr_elems(void *coll)
{
    return ((keyvals_arr_t *) coll)->array();
}

void *pch_kvsarray::pch_get_key(const void *pair)
{
    return ((keyvals_t *) pair)->key;
}

void pch_kvsarray::pch_set_elems(void *coll, void *elems, int len)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    arr->set_array(reinterpret_cast<keyvals_t *>(elems), len);
}

void pch_kvsarray::pch_shallow_free(void *coll)
{
    assert(coll);
    keyvals_arr_t *parr = (keyvals_arr_t *) coll;
    parr->shallow_free();
}

