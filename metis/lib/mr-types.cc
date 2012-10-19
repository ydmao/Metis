#include "mr-types.hh"
#include "comparator.hh"
#include "value_helper.hh"
#include "reduce.hh"
#include "btree.hh"

extern keycopy_t mrkeycopy;

bool keyval_arr_t::map_append(void *key, void *val, size_t keylen, unsigned hash) {
    void *ik = (keylen && mrkeycopy) ? mrkeycopy(key, keylen) : key;
    keyval_t tmp(ik, val, hash);
    push_back(&tmp);
    return true;
}

bool keyvals_arr_t::map_insert_sorted(void *key, void *val, size_t keylen, unsigned hash) {
    keyvals_t tmp(key, hash);
    int pos = 0;
    bool bnew = insert_new(&tmp, comparator::keyvals_pair_comp, &pos);
    if (bnew && keylen && mrkeycopy)
        at(pos).key = mrkeycopy(key, keylen);
    map_values_insert(&at(pos), val);
    return bnew;
}

void transfer(xarray<keyvals_t> *dst, xarray<keyval_t> *src) {
    append_functor f(dst);
    group_one_sorted(*src, f);
    src->init();
}

void transfer(xarray<keyvals_t> *dst, btree_type *src) {
    src->transfer(dst);
}

void transfer(xarray<keyvals_t> *dst, xarray<keyvals_t> *src) {
    src->transfer(dst);
}

void transfer(xarray<keyval_t> *dst, xarray<keyval_t> *src) {
    src->transfer(dst);
}

