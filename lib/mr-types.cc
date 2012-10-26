#include "mr-types.hh"
#include "comparator.hh"
#include "value_helper.hh"
#include "group.hh"
#include "btree.hh"

extern mapreduce_appbase *the_app_;

bool keyval_arr_t::map_append_copy(void *key, void *val, size_t keylen, unsigned hash) {
    void *ik = the_app_->key_copy(key, keylen);
    keyval_t tmp(ik, val, hash);
    push_back(tmp);
    return true;
}

void keyval_arr_t::map_append_raw(keyval_t *t) {
    push_back(*t);
}

bool keyvals_arr_t::map_insert_sorted_copy_on_new(void *key, void *val, size_t keylen, unsigned hash) {
    keyvals_t tmp(key, hash);
    int pos = 0;
    bool newkey = atomic_insert(&tmp, comparator::raw_comp<keyvals_t>::impl, &pos);
    if (newkey)
        at(pos).key = the_app_->key_copy(key, keylen);
    map_values_insert(&at(pos), val);
    return newkey;
}

void keyvals_arr_t::map_insert_sorted_new_and_raw(keyvals_t *p) {
    int pos = 0;
    bool newkey = atomic_insert(p, comparator::raw_comp<keyvals_t>::impl, &pos);
    assert(newkey);
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

