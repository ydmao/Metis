#ifndef PCHANDLER_H
#define PCHANDLER_H

#include "mr-types.hh"
#include "btree.hh"

extern keycopy_t mrkeycopy;

/* pair collection handler */
struct pc_handler_t {
    virtual void pch_set_util(key_cmp_t keycmp) = 0;
    virtual void pch_init(void *coll) = 0;
    /* insert. When keylen > 0 and keycopy function is provided, the handler 
     * will store a copy of the key using keycopy function, and returns 1;
     * otherwise, it returns 0. keyval_array always returns 1 sicne it does
     * not group pairs by key. */
    virtual int pch_insert_kv(void *coll, void *key, void *val, size_t keylen,
			  unsigned hash) = 0;
    /* keep sorted */
    virtual void pch_insert_kvs(void *coll, const keyvals_t * kvs) = 0;
    //virtual void pch_append_kvs(void *coll, const keyvals_t * kvs) = 0;
    virtual void pch_insert_kvslen(void *coll, void *key, void **vals,
			       uint64_t len) = 0;
    /* get results */
    virtual uint64_t pch_copy_kvs(void *coll, keyvals_t * dst) = 0;
    virtual uint64_t pch_copy_kv(void *coll, keyval_t * dst) = 0;
    /* free the collection, but not the values of the pairs */
    virtual void pch_shallow_free(void *coll) = 0;
    /* properties of the pair handler which handles pairs arrays */
    virtual uint64_t pch_get_len(void *coll) = 0;
    virtual size_t pch_get_pair_size(void) = 0;
    virtual size_t pch_get_parr_size(void) = 0;
    virtual void *pch_get_arr_elems(void *coll) = 0;
    virtual void *pch_get_key(const void *pair) = 0;
    /* set elements */
    virtual void pch_set_elems(void *coll, void *elems, int len) = 0;
};

#endif
