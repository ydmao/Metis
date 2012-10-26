#ifndef MR_TYPES_H
#define MR_TYPES_H

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
    void *key;
    void *val;
    unsigned hash;
    keyval_t() {
        init();
    }
    ~keyval_t() {
        reset();
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
    void assign(const keyval_t &a) {
        set(a.key, a.val, a.hash);
    }
    void init() {
        set(NULL, NULL, 0);
    }
    void reset() {
        init();
    }
  private:
    void set(void *k, void *v, unsigned h) {
        key = k;
        val = v;
        hash = h;
    }
};

struct keyvals_len_t {
    void *key;
    void **vals;
    uint64_t len;
    ~keyvals_len_t() {
        reset();
    }
    keyvals_len_t() {
        init();
    }
    explicit keyvals_len_t(void *k) {
        set(k, NULL, 0);
    }
    keyvals_len_t(void *k, void **v, uint64_t l) {
        set(k, v, l);
    }
    void assign(const keyvals_len_t &a) {
        set(a.key, a.vals, a.len);
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
        key = k;
        vals = v;
        len = l;
    }
};

/* types used internally */
struct keyvals_len_arr_t: public xarray<keyvals_len_t> {
};

struct keyvals_t : public xarray<void *> {
    void *key;			/* put key at the same offset with keyval_t */
    unsigned hash;
    keyvals_t() {
        init();
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
    explicit keyvals_t(void *k) {
        init();
        set(k, 0);
    }
    keyvals_t(void *k, unsigned h) {
        reset();
        set(k, h);
    }
    void assign(const keyvals_t &a) {
        set(a.key, a.hash);
        xarray<void *>::assign(a);
    }
  private:
    void set(void *k, unsigned h) {
        key = k;
        hash = h;
    }
};

struct keyval_arr_t : public xarray<keyval_t> {
    bool map_append(void *key, void *val, size_t keylen, unsigned hash);
};

void transfer(xarray<keyvals_t> *dst, xarray<keyvals_t> *src);
void transfer(xarray<keyval_t> *dst, xarray<keyval_t> *src);
void transfer(xarray<keyvals_t> *dst, xarray<keyval_t> *src);
struct btree_type;
void transfer(xarray<keyvals_t> *dst, btree_type *src);

struct keyvals_arr_t : public xarray<keyvals_t> {
    bool map_insert_sorted(void *key, void *val, size_t keylen, unsigned hash);
};

typedef enum {
    MAP,
    REDUCE,
    MERGE,
    MR_PHASES,
} task_type_t;

typedef int (*pair_cmp_t)(const void *, const void *);

/* suggested number of map tasks per core. */
enum { def_nsplits_per_core = 16 };

enum app_type_t {
    atype_maponly = 0,
    atype_mapgroup,
    atype_mapreduce
};

#endif
