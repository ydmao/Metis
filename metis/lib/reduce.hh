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

// kvs.vals is owned by callee
struct reduce_or_group {
    typedef void (*group_emit_t) (void *arg, const keyvals_t * kvs);
    static key_cmp_t keycmp_;

    static void setcmp(key_cmp_t cmp) {
        keycmp_ = cmp;
    }

    // Each node contains an iteratable collection of keyval_t
    // reduce or group key/values pairs from different nodes
    // (each node contains pairs sorted by key)
    template <typename C>
    static void do_kv(C **colls, int n, group_emit_t meth, void *arg);
    // Each node contains an iteratable collection of keyvals_t
    // reduce or group key/value pairs from different nodes
    template <typename C>
    static void do_kvs(C **colls, int n);

    enum { init_valloclen = 8 };

    static int keyval_cmp(const void *kvs1, const void *kvs2) {
        return keycmp_(((keyval_t *) kvs1)->key, ((keyval_t *) kvs2)->key);
    }
};

template <typename C>
void reduce_or_group::do_kvs(C **nodes, int n) {
    if (!n)
        return;
    typename C::iterator it[JOS_NCPU];
    for (int i = 0; i < n; i++)
	 it[i] = nodes[i]->begin();
    int marks[JOS_NCPU];
    keyvals_t dst;
    memset(&dst, 0, sizeof(dst));
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
		cmp = keycmp_(it[min_idx]->key, it[i]->key);
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
	    } while (it[i] != nodes[i]->end() && keycmp_(dst.key, it[i]->key) == 0);
	}
	if (the_app.atype == atype_mapreduce) {
	    if (the_app.mapreduce.vm) {
		rbkts_emit_kv(dst.key, (void *) dst.vals);
		memset(&dst, 0, sizeof(dst));
	    } else {
		the_app.mapreduce.reduce_func(dst.key, dst.vals, dst.len);
		// Reuse the values array
		dst.len = 0;
	    }
	} else {		// mapgroup
	    rbkts_emit_kvs_len(dst.key, dst.vals, dst.len);
	    memset(&dst, 0, sizeof(dst));
	}
    }
    values_deep_free(&dst);
}

template <typename C>
void reduce_or_group::do_kv(C **nodes, int n, group_emit_t meth, void *arg) {
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

    qsort(arr, total_len, sizeof(keyval_t), keyval_cmp);
    int start = 0;
    keyvals_t kvs;
    memset(&kvs, 0, sizeof(kvs));
    while (start < int(total_len)) {
	int end = start + 1;
	while (end < int(total_len) && !keycmp_(arr[start].key, arr[end].key))
	    end++;
	kvs.key = arr[start].key;
	for (int i = 0; i < end - start; i++)
	    values_insert(&kvs, arr[start + i].val);
	if (meth) {
	    meth(arg, &kvs);
	    // kvs.vals is owned by callee
	    memset(&kvs, 0, sizeof(kvs));
	} else if (the_app.atype == atype_mapreduce) {
	    if (the_app.mapreduce.vm) {
		assert(kvs.len == 1);
		rbkts_emit_kv(kvs.key, (void *) kvs.vals);
		memset(&kvs, 0, sizeof(kvs));
	    } else {
		the_app.mapreduce.reduce_func(kvs.key, kvs.vals, kvs.len);
		// Reuse the values
		kvs.len = 0;
	    }
	} else {		// mapgroup
	    rbkts_emit_kvs_len(kvs.key, kvs.vals, kvs.len);
	    // kvs.vals is owned by callee
	    memset(&kvs, 0, sizeof(kvs));
	}
	start = end;
    }
    values_deep_free(&kvs);
    if (n > 1 && arr)
	free(arr);
}
#endif
