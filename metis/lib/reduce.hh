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

typedef void (*group_emit_t) (void *arg, const keyvals_t * kvs);

// Each node contains an iteratable collection of keyval_t
// reduce or group key/values pairs from different nodes
// (each node contains pairs sorted by key)
template <typename C>
inline void reduce_or_group_go(C **colls, int n, group_emit_t meth, void *arg);

template <>
inline void reduce_or_group_go<xarray<keyval_t> >(xarray<keyval_t> **nodes, int n,
                                                  group_emit_t meth, void *arg) {
    if (!n)
        return;
    uint64_t total_len = 0;
    for (int i = 0; i < n; i++)
	total_len += nodes[i]->size();
    keyval_t *arr = 0;
    if (n > 1) {
	arr = (keyval_t *) malloc(total_len * sizeof(keyval_t));
	int idx = 0;
	for (int i = 0; i < n; i++) {
            memcpy(&arr[idx], nodes[i]->array(),
                   nodes[i]->size() * sizeof(keyval_t));
	    idx += nodes[i]->size();
        }
    } else
	arr = (keyval_t *)nodes[0]->array();

    qsort(arr, total_len, sizeof(keyval_t), comparator::keyval_pair_comp);
    int start = 0;
    keyvals_t kvs;
    while (start < int(total_len)) {
	int end = start + 1;
	while (end < int(total_len) && !comparator::keycmp()(arr[start].key, arr[end].key))
	    end++;
	kvs.key = arr[start].key;
	for (int i = 0; i < end - start; i++)
	    values_insert(&kvs, arr[start + i].val);
	if (meth) {
	    meth(arg, &kvs);
	    // kvs.vals is owned by callee
            kvs.reset();
	} else if (the_app.atype == atype_mapreduce) {
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
	start = end;
    }
    if (n > 1 && arr)
	free(arr);
}

template <typename C>
inline void reduce_or_group_go(C **nodes, int n, group_emit_t, void *) {
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
	if (the_app.atype == atype_mapreduce) {
	    if (the_app.mapreduce.vm) {
		reduce_bucket_manager::instance()->emit(dst.key, dst.multiplex_value());
                dst.reset();
	    } else {
		the_app.mapreduce.reduce_func(dst.key, dst.array(), dst.size());
		dst.trim(0);  // Reuse the values array
	    }
	} else {		// mapgroup
	    reduce_bucket_manager::instance()->emit(dst.key, dst.array(), dst.size());
            dst.reset();
	}
    }
}

#endif
