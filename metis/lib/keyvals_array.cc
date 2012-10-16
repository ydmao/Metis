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

void checkfull(keyvals_arr_t * arr)
{
    if (arr->alloc_len == 0) {
	arr->alloc_len = init_array_size;
	arr->arr = new keyvals_t[arr->alloc_len];
    } else if (arr->len == arr->alloc_len) {
	arr->alloc_len *= 2;
	assert(arr->arr = (keyvals_t *)
	       realloc(arr->arr, arr->alloc_len * sizeof(keyvals_t)));
    }
}

void pch_kvsarray::pch_append_kvs(void *coll, const keyvals_t * kvs)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    checkfull(arr);
    arr->arr[arr->len++] = *kvs;
}

void pch_kvsarray::pch_insert_kvs(void *coll, const keyvals_t * kvs)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    checkfull(arr);
    int bfound = 0;
    int dst = bsearch_eq(kvs, arr->arr, arr->len, sizeof(keyvals_t),
			 keyvals_cmp, &bfound);
    assert(!bfound);
    if (dst < int(arr->len))
	memmove(&arr->arr[dst + 1], &arr->arr[dst],
		sizeof(keyvals_t) * (arr->len - dst));
    arr->arr[dst] = *kvs;
    arr->len++;
}

int pch_kvsarray::pch_insert_kv(void *coll, void *key, void *val, size_t keylen, unsigned hash)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    int bfound = 0;
    keyvals_t tmp;
    tmp.key = key;
    int dst = bsearch_eq(&tmp, arr->arr, arr->len, sizeof(keyvals_t),
			 keyvals_cmp, &bfound);
    if (bfound) {
	values_insert(&arr->arr[dst], val);
	return 0;
    }
    // insert the node into the keynode set
    checkfull(arr);
    if (dst < int(arr->len))
	memmove(&arr->arr[dst + 1], &arr->arr[dst],
		sizeof(keyvals_t) * (arr->len - dst));
    arr->len++;
    arr->arr[dst].alloc_len = 0;
    arr->arr[dst].len = 0;
    arr->arr[dst].hash = hash;
    if (keylen && mrkeycopy)
	arr->arr[dst].key = mrkeycopy(key, keylen);
    else
	arr->arr[dst].key = key;
    values_insert(&arr->arr[dst], val);
    return 1;
}

uint64_t pch_kvsarray::pch_get_len(void *coll)
{
    assert(coll);
    return ((keyvals_arr_t *) coll)->len;
}

typedef struct {
    int next;
} iter_t;

int pch_kvsarray::pch_iter_begin(void *coll, void **iter_)
{
    if (!coll) {
	*iter_ = 0;
	return 1;
    }
    iter_t *iter;
    assert(iter = (iter_t *) malloc(sizeof(iter_t)));
    iter->next = 0;
    *iter_ = iter;
    return 0;
}

int pch_kvsarray::pch_iter_next_kvs(void *coll, keyvals_t * kvs, void *iter_, int bclear)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    iter_t *iter = (iter_t *) iter_;
    if (iter->next == int(arr->len))
	return 1;
    *kvs = arr->arr[iter->next];
    if (bclear)
	memset(&arr->arr[iter->next], 0, sizeof(keyvals_t));
    iter->next++;
    return 0;
}

void pch_kvsarray::pch_iter_end(void *iter)
{
    if (iter)
	free(iter);
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
    return ((keyvals_arr_t *) coll)->arr;
}

void *pch_kvsarray::pch_get_key(const void *pair)
{
    return ((keyvals_t *) pair)->key;
}

void pch_kvsarray::pch_set_elems(void *coll, void *elems, int len)
{
    keyvals_arr_t *arr = (keyvals_arr_t *) coll;
    arr->arr = reinterpret_cast<keyvals_t *>(elems);
    arr->len = len;
    arr->alloc_len = len;
}

void pch_kvsarray::pch_shallow_free(void *coll)
{
    assert(coll);
    keyvals_arr_t *parr = (keyvals_arr_t *) coll;
    if (parr->arr) {
	free(parr->arr);
	parr->arr = NULL;
	parr->len = 0;
    }
}

