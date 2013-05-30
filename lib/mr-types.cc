/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
#include "mr-types.hh"
#include "group.hh"
#include "btree.hh"
#include "appbase.hh"

struct append_functor {
    append_functor(xarray<keyvals_t> *x) : x_(x) {}
    void operator()(keyvals_t &kvs) {
	// kvs.vals is owned by callee
        x_->push_back(kvs);
        kvs.init();
    }
  private:
    xarray<keyvals_t> *x_;
};

bool keyval_arr_t::map_append_copy(void *key, void *val, size_t keylen, unsigned hash) {
    void *ik = static_appbase::key_copy(key, keylen);
    keyval_t tmp(ik, val, hash);
    push_back(tmp);
    return true;
}

void keyval_arr_t::map_append_raw(keyval_t *t) {
    push_back(*t);
}

bool keyvals_arr_t::map_insert_sorted_copy_on_new(void *key, void *val, size_t keylen, unsigned hash) {
    keyvals_t tmp(key, hash);
    size_t pos = 0;
    bool newkey = atomic_insert(&tmp, static_appbase::pair_comp<keyvals_t>, &pos);
    if (newkey)
        at(pos)->key_ = static_appbase::key_copy(key, keylen);
    at(pos)->map_value_insert(val);
    return newkey;
}

void keyvals_arr_t::map_insert_sorted_new_and_raw(keyvals_t *p) {
    size_t pos = 0;
    bool newkey = atomic_insert(p, static_appbase::pair_comp<keyvals_t>, &pos);
    assert(newkey);
}

void keyval_arr_t::transfer(xarray<keyvals_t> *dst) {
    append_functor f(dst);
    group_one_sorted(*this, f, static_appbase::key_free);
    this->init();
}

void keyvals_t::map_value_insert(void *v) {
    static_appbase::map_values_insert(this, v);
}

void keyvals_t::map_value_move(keyval_t *src) {
    map_value_insert(src->val);
    src->reset();
}

void keyvals_t::map_value_move(keyvals_t *src) {
    static_appbase::map_values_move(this, src);
}

void keyvals_t::map_value_move(keyvals_len_t *src) {
    assert(static_appbase::application_type() == atype_mapgroup);  // must be mapgroup
    append(src->vals, src->len);
    src->reset();
}
