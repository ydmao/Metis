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
    static const int key_values = false;
    void *key;
    void *val;
    unsigned hash;
    keyval_t() {
        memset(this, 0, sizeof(*this));
    }
    keyval_t(void *k) {
        memset(this, 0, sizeof(*this));
        key = k;
    }
    keyval_t(void *k, unsigned h) {
        memset(this, 0, sizeof(*this));
        key = k;
        hash = h;
    }
    keyval_t(void *k, void *v, unsigned h) {
        memset(this, 0, sizeof(*this));
        key = k;
        val = v;
        hash = h;
    }
    keyval_t(void *k, void *v) {
        memset(this, 0, sizeof(*this));
        key = k;
        val = v;
        hash = 0;
    }
    void assign(const keyval_t &a) {
        key = a.key;
        val = a.val;
        hash = a.hash;
    }
    void init() {
        key = val = NULL;
        hash = 0;
    }
    void reset() {
        init();
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
    keyvals_len_t() {
        memset(this, 0, sizeof(*this));
    }
    keyvals_len_t(void *k) {
        memset(this, 0, sizeof(*this));
        key = k;
    }
    keyvals_len_t(void *k, void **v, uint64_t l) {
        memset(this, 0, sizeof(*this));
        key = k;
        vals = v;
        len = l;
    }
    void assign(const keyvals_len_t &a) {
        key = a.key;
        vals = a.vals;
        len = a.len;
    }
    void reset() {
        key = NULL;
        vals = NULL;
        len = 0;
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
    static const int key_values = true;
    void *key;			/* put key at the same offset with keyval_t */
    unsigned hash;
    keyvals_t() {
        reset();
    }
    ~keyvals_t() {
        reset();
    }
    void init() {
        key = NULL;
        hash = 0;
        xarray<void *>::init();
    }
    void reset() {
        key = NULL;
        hash = 0;
        xarray<void *>::clear();
    }
    keyvals_t(void *k) {
        reset();
        key = k;
    }
    keyvals_t(void *k, unsigned h) {
        reset();
        key = k;
        hash = h;
    }
    void assign(const keyvals_t &a) {
        key = a.key;
        hash = a.hash;
        xarray<void *>::assign(a);
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

typedef enum {
    atype_maponly = 0,
    atype_mapgroup,
    atype_mapreduce
} app_type_t;

typedef union {
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
} app_arg_t;

#endif
