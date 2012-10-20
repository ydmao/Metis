#ifndef MR_TYPES_H
#define MR_TYPES_H

#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>
#include <string.h>
#include "array.hh"

typedef struct {
    void *data;
    size_t length;
} split_t;

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
        h = h;
    }
};

struct final_data_kv_t {
    keyval_t *data;
    size_t length;
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

typedef struct {
    keyvals_len_t *data;
    size_t length;
} final_data_kvs_len_t;

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

/* suggested number of map tasks per core. */
enum { def_nsplits_per_core = 16 };

typedef int (*splitter_t) (void *arg, split_t * ret, int ncores);
typedef void (*map_t) (split_t *);
/* values are owned by Metis library */
typedef void (*reduce_t) (void *, void **, size_t);
typedef int (*combine_t) (void *, void **, size_t);
typedef unsigned (*partition_t) (void *, int);
typedef void *(*keycopy_t) (void *key, size_t);
typedef void *(*vmodifier_t) (void *oldv, void *newv, int isnew);
typedef int (*key_cmp_t) (const void *, const void *);
typedef int (*kv_out_cmp_t) (const keyval_t *, const keyval_t *);
typedef int (*kvs_out_cmp_t) (const keyvals_len_t *, const keyvals_len_t *);
typedef int (*pair_cmp_t) (const void *, const void *);

/* default splitter */
struct defsplitter_state {
    size_t split_pos;
    size_t data_size;
    uint64_t nsplits;
    size_t align;
    void *data;
    pthread_mutex_t mu;
};

int defsplitter(void *arg, split_t * ma, int ncores);
void defsplitter_init(struct defsplitter_state *ds, void *data,
		      size_t data_size, uint64_t nsplits, size_t align);

enum app_type_t {
    atype_maponly = 0,
    atype_mapgroup,
    atype_mapreduce
};

union app_arg_t {
    app_type_t atype;
    struct {
	app_type_t atype;
	final_data_kv_t *results;	/* output data, <key, reduced value> */
	kv_out_cmp_t outcmp;	/* optional output compare function */
	int reduce_tasks;	/* if not zero, disable the sampling */
	reduce_t reduce_func;	/* no reduce_func should be provided when using vm */
	combine_t combiner;	/* no combiner should be provided when using vm */
	vmodifier_t vm;		/* called for each key/value pair to update the value */
    } mapreduce;
    struct {
	app_type_t atype;
	final_data_kv_t *results;	/* output data, <key, mapped value> */
	kv_out_cmp_t outcmp;	/* optional output copare function */
    } maponly;
    struct {
	app_type_t atype;
	final_data_kvs_len_t *results;	/* output data, <key, values> */
	kvs_out_cmp_t outcmp;	/* optional output compare function */
	int group_tasks;	/* if not zero, disables the sampling. */
    } mapgroup;
    /* the following structs are used internally */
    struct {
	app_type_t atype;
	final_data_kv_t *results;
	kv_out_cmp_t outcmp;
    } mapor;			/* maponly + mapreduce */
    struct {
	app_type_t atype;
	void *results;
	kv_out_cmp_t outcmp;
	int tasks;
    } mapgr;			/* mapgroup + mapreduce */
    struct {
	app_type_t atype;
	void *results;
	pair_cmp_t outcmp;
	int tasks;
    } any;			/* mapgroup + mapreduce + maponly */
};

#endif
