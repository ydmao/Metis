#include "value_helper.hh"
#include "bsearch.hh"
//#include "values.hh"
#include "pch_kvsbtree.hh"
#include "pch_kvsarray.hh"
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif

void pch_kvsbtree::pch_set_util(key_cmp_t kcmp)
{
    btree_type::set_key_compare(kcmp);
}

void pch_kvsbtree::pch_init(void *coll)
{
    assert(coll);
    btree_type *b = reinterpret_cast<btree_type *>(coll);
    b->init();
}

// left < splitkey <= right. Right is the new sibling
int pch_kvsbtree::pch_insert_kv(void *coll, void *key, void *val, size_t keylen, unsigned hash)
{
    assert(coll);
    btree_type *bt = (btree_type *) coll;
    return bt->insert_kv(key, val, keylen, hash);
}

void pch_kvsbtree::pch_insert_kvs(void *coll, const keyvals_t * kvs)
{
    btree_type *bt = (btree_type *) coll;
    bt->insert_kvs(kvs);
}

uint64_t pch_kvsbtree::pch_get_len(void *coll)
{
    assert(coll);
    return ((btree_type *) coll)->size();
}

void pch_kvsbtree::pch_shallow_free(void *coll)
{
    assert(coll);
    btree_type *b = reinterpret_cast<btree_type *>(coll);
    b->shallow_free();
}

int pch_kvsbtree::pch_iter_begin(void *coll, void **iter)
{
    assert(coll);
    btree_type *bt = (btree_type *) coll;
    btree_type::iterator *i = new btree_type::iterator(bt->begin());
    *iter = i;
    return *i == bt->end();
}

int pch_kvsbtree::pch_iter_next_kvs(void *coll, keyvals_t * kvs, void *iter, int bclear)
{
    btree_type *bt = (btree_type *)coll;
    btree_type::iterator *it = (btree_type::iterator *)iter;
    ++(*it);
    if (*it == bt->end())
        return 1;
    memcpy(kvs, &(*it), sizeof(keyvals_t));
    if (bclear) {
        (*it)->len = (*it)->alloc_len = 0;
        (*it)->vals = 0;
    }
    return 0;
}

void pch_kvsbtree::pch_iter_end(void *iter)
{
    if (iter)
	free(iter);
}

uint64_t pch_kvsbtree::pch_copy_kvs(void *coll, keyvals_t *dst)
{
    btree_type *bt = (btree_type *) coll;
    return bt->copy_kvs(dst);
}

