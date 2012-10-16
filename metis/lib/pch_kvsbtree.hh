#ifndef PC_KVSBTREE_HH_
#define PC_KVSBTREE_HH_ 1

#include "pchandler.hh"
#include <assert.h>

struct pch_kvsbtree : public pc_handler_t {
    void pch_set_util(key_cmp_t keycmp);
    void pch_init(void *coll);
    int pch_insert_kv(void *coll, void *key, void *val, size_t keylen,
			  unsigned hash);
    /* keep sorted */
    void pch_insert_kvs(void *coll, const keyvals_t * kvs);
    static void pch_append_kvs(void *coll, const keyvals_t * kvs);
    void pch_insert_kvslen(void *coll, void *key, void **vals,
			       uint64_t len) {
        assert(0 && "should never call");
    }
    /* get results */
    uint64_t pch_copy_kvs(void *coll, keyvals_t * dst);
    uint64_t pch_copy_kv(void *coll, keyval_t * dst) {
        assert(0 && "should never call");
    }
    /* iteartor */
    int pch_iter_begin(void *coll, void **iter);
    int pch_iter_next_kvs(void *coll, keyvals_t * next, void *iter,
	                  int bclear);
    int pch_iter_next_kv(void *coll, keyval_t * next, void *iter) {
        assert(0 && "Should never call");
    }
    void pch_iter_end(void *iter);
    /* free the collection, but not the values of the pairs */
    void pch_shallow_free(void *coll);
    /* properties of the pair handler which handles pairs arrays */
    uint64_t pch_get_len(void *coll);
    size_t pch_get_pair_size(void) {
        assert(0 && "Should never call");
    }
    size_t pch_get_parr_size(void) {
        assert(0 && "Should never call");
    }
    void *pch_get_arr_elems(void *coll) {
         assert(0 && "Should never call");
    }
    void *pch_get_key(const void *pair) {
         assert(0 && "Should never call");
    }
    /* set elements */
    void pch_set_elems(void *coll, void *elems, int len) {
        assert(0 && "Should never call");
    }
};

#endif
