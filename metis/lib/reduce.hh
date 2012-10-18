#ifndef REDUCE_H
#define REDUCE_H

#include "mr-types.hh"
#include "value_helper.hh"
#include "apphelper.hh"
#include "rbktsmgr.hh"
#include "mr-sched.hh"
#include "bench.hh"
#include "compiler.hh"
#include <assert.h>
#include <string.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif

struct reduce_emit_functor {
    void operator()(keyvals_t &kvs) const {
        if (the_app.atype == atype_mapreduce) {
	    if (the_app.mapreduce.vm) {
		assert(kvs.size() == 1);
		reduce_bucket_manager::instance()->emit(kvs.key, kvs.multiplex_value());
		memset(&kvs, 0, sizeof(kvs));
	    } else {
		the_app.mapreduce.reduce_func(kvs.key, kvs.array(), kvs.size());
		// Reuse the values
		kvs.trim(0);
	    }
	} else {		// mapgroup
	    reduce_bucket_manager::instance()->emit(kvs.key, kvs.array(), kvs.size());
	    // kvs.vals is owned by callee
            kvs.reset();
	}
    }
    static reduce_emit_functor &instance() {
        static reduce_emit_functor in;
        return in;
    }
};

struct append_functor {
    append_functor(xarray<keyvals_t> *x) {}
    void operator()(keyvals_t &kvs) {
	// kvs.vals is owned by callee
        x_->push_back(kvs);
        kvs.reset();
    }
  private:
    xarray<keyvals_t> *x_;
};

// Each node contains an iteratable collection of keyval_t
// reduce or group key/values pairs from different nodes
// (each node contains pairs sorted by key)
template <typename C, typename F>
inline void group(C **colls, int n, F &f);

template <>
inline void 
group<xarray<keyval_t>, append_functor>(xarray<keyval_t> **nodes, 
                                        int n, append_functor &f) {
    if (!n)
        return;
    size_t total_len = 0;
    for (int i = 0; i < n; i++)
	total_len += nodes[i]->size();
    xarray<keyval_t> *arr = NULL;
    if (n > 1) {
	arr = new xarray<keyval_t>;
        arr->resize(total_len);
	for (int i = 0; i < n; i++)
            arr->append(*nodes[i]);
    } else
	arr = nodes[0];
    arr->sort(comparator::keyval_pair_comp);

    size_t start = 0;
    keyvals_t kvs;
    while (start < total_len) {
	size_t end = start + 1;
	while (end < total_len && !comparator::keycmp()(arr->at(start).key, arr->at(end).key))
	    end++;
	kvs.key = arr->at(start).key;
	for (size_t i = start; i < end; i++)
	    values_insert(&kvs, arr->at(i).val);
        f(kvs);
	start = end;
    }
    if (n > 1 && arr)
        delete arr;
}

template <typename C, typename F>
inline void group(C **nodes, int n, F &f) {
    if (!n)
        return;
    typename C::iterator it[JOS_NCPU];
    for (int i = 0; i < n; i++)
	 it[i] = nodes[i]->begin();
    int marks[JOS_NCPU];
    keyvals_t dst;
    while (1) {
	int min_idx = -1;
	memset(marks, 0, sizeof(marks[0]) * n);
	int m = 0;
	// Find minimum key
	for (int i = 0; i < n; ++i) {
	    if (it[i] == nodes[i]->end())
		continue;
	    int cmp = 0;
	    if (min_idx >= 0)
		cmp = comparator::keycmp()(it[min_idx]->key, it[i]->key);
	    if (min_idx < 0 || cmp > 0) {
		++ m;
		marks[i] = m;
		min_idx = i;
	    } else if (!cmp)
		marks[i] = m;
	}
	if (min_idx < 0)
	    break;
	// Merge all the values with the same mimimum key.
	// Since keys may duplicate in each node, vlen
	// is temporary.
	dst.key = it[min_idx]->key;
	// Eat up all the pairs with the same key
	for (int i = 0; i < n; i++) {
	    if (marks[i] != m)
		continue;
	    do {
		values_mv(&dst, &it[i]);
                ++it[i];
	    } while (it[i] != nodes[i]->end() && comparator::keycmp()(dst.key, it[i]->key) == 0);
	}
        f(dst);
    }
}

#endif
