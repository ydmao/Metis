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
#ifndef MR_TYPES_HH_
#define MR_TYPES_HH_

#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>
#include <string.h>
#include "array.hh"

struct split_t {
    void *data;
    size_t length;
};

struct keyval_t {
    void *key_;
    void *val;
    unsigned hash;
    keyval_t() {
        init();
    }
    explicit keyval_t(void *k) {
        set(k, NULL, 0);
    }
    keyval_t(void *k, unsigned h) {
        set(k, NULL, h);
    }
    keyval_t(void *k, void *v, unsigned h) {
        set(k, v, h);
    }
    keyval_t(void *k, void *v) {
        set(k, v, 0);
    }
    ~keyval_t() {
        reset();
    }
    void assign(const keyval_t &a) {
        set(a.key_, a.val, a.hash);
    }
    void init() {
        set(NULL, NULL, 0);
    }
    void reset() {
        init();
    }
  private:
    void set(void *k, void *v, unsigned h) {
        key_ = k;
        val = v;
        hash = h;
    }
};

struct keyvals_len_t {
    void *key_;
    void **vals;
    uint64_t len;
    keyvals_len_t() {
        init();
    }
    explicit keyvals_len_t(void *k) {
        set(k, NULL, 0);
    }
    keyvals_len_t(void *k, void **v, uint64_t l) {
        set(k, v, l);
    }
    ~keyvals_len_t() {
        reset();
    }
    void assign(const keyvals_len_t &a) {
        set(a.key_, a.vals, a.len);
    }
    void init() {
        set(NULL, NULL, 0);
    }
    /* @brief: may need to free memory */
    void reset() {
        if (vals)
            free(vals);
        init();
    }
  private:
    void set(void *k, void **v, uint64_t l) {
        key_ = k;
        vals = v;
        len = l;
    }
};

/* types used internally */
struct keyvals_len_arr_t: public xarray<keyvals_len_t> {
};

struct keyvals_t : public xarray<void *> {
    void *key_;			/* put key at the same offset with keyval_t */
    unsigned hash;
    keyvals_t() {
        init();
    }
    explicit keyvals_t(void *k) {
        init();
        set(k, 0);
    }
    keyvals_t(void *k, unsigned h) {
        reset();
        set(k, h);
    }
    ~keyvals_t() {
        reset();
    }
    void init() {
        set(NULL, 0);
        xarray<void *>::init();
    }
    void reset() {
        set(0, 0);
        xarray<void *>::clear();
    }
    void assign(const keyvals_t &a) {
        set(a.key_, a.hash);
        xarray<void *>::assign(a);
    }
    void map_value_insert(void *v);
    void map_value_move(keyval_t *src);
    void map_value_move(keyvals_t *src);
    void map_value_move(keyvals_len_t *src);
  private:
    void set(void *k, unsigned h) {
        key_ = k;
        hash = h;
    }
};

struct keyval_arr_t : public xarray<keyval_t> {
    bool map_append_copy(void *k, void *v, size_t keylen, unsigned hash);
    void map_append_raw(keyval_t *p);
    void transfer(xarray<keyvals_t> *dst);
    using xarray<keyval_t>::transfer;
};

struct keyvals_arr_t : public xarray<keyvals_t> {
    bool map_insert_sorted_copy_on_new(void *k, void *v, size_t keylen, unsigned hash);
    void map_insert_sorted_new_and_raw(keyvals_t *p);
};

enum task_type_t {
    MAP,
    REDUCE,
    MERGE,
    MR_PHASES,
};

/* suggested number of map tasks per core. */
enum { def_nsplits_per_core = 16 };

enum app_type_t {
    atype_maponly = 0,
    atype_mapgroup,
    atype_mapreduce
};

#endif
