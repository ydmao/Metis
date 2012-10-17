#ifndef REDUCE_H
#define REDUCE_H

#include "mr-types.hh"
#include "pchandler.hh"
#include "value_helper.hh"
#include "apphelper.hh"
#include "rbktsmgr.hh"
#include "mr-sched.hh"
#include "bench.hh"
#include "pch_kvsbtree.hh"
#include "pch_kvsarray.hh"
#include "pch_kvarray.hh"
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
    static void do_kv(pc_handler_t * pch, void **colls, int n,
  		           group_emit_t meth, void *arg);
    // Each node contains an iteratable collection of keyvals_t
    static void do_kvs(pc_handler_t * pch, void **colls, int n);

    enum { init_valloclen = 8 };

    static int keyval_cmp(const void *kvs1, const void *kvs2) {
        return keycmp_(((keyval_t *) kvs1)->key, ((keyval_t *) kvs2)->key);
    }
};

// reduce or group key/values pairs from different nodes
// (each node contains pairs sorted by key)
inline void reduce_or_group::do_kvs(pc_handler_t * pch, void **nodes, int n)
{
    assert(instanceOf<pch_kvsarray>(pch)|| instanceOf<pch_kvsbtree>(pch));
    void *iters[JOS_NCPU];
    keyvals_t kvs[JOS_NCPU];
    int ended[JOS_NCPU];
    for (int i = 0; i < n; i++)
	ended[i] = pch->pch_iter_begin(nodes[i], &iters[i]);
    for (int i = 0; i < n; i++) {
	if (ended[i])
	    continue;
	ended[i] = pch->pch_iter_next_kvs(nodes[i], &kvs[i], iters[i], 1);
    }
    int marks[JOS_NCPU];
    keyvals_t dst;
    memset(&dst, 0, sizeof(dst));
    while (1) {
	int min_idx = -1;
	memset(marks, 0, sizeof(marks[0]) * n);
	int m = 0;
	// Find minimum key
	for (int i = 0; i < n; i++) {
	    if (ended[i])
		continue;
	    int res = 0;
	    if (min_idx >= 0)
		res = keycmp_(kvs[min_idx].key, kvs[i].key);
	    if (min_idx < 0 || res > 0) {
		m++;
		marks[i] = m;
		min_idx = i;
	    } else if (!res) {
		marks[i] = m;
	    }
	}
	if (min_idx < 0)
	    break;
	// Merge all the values with the same mimimum key.
	// Since keys may duplicate in each node, vlen
	// is temporary.
	dst.key = kvs[min_idx].key;
	// Eat up all the pairs with the same key
	for (int i = 0; i < n; i++) {
	    if (marks[i] != m)
		continue;
	    do {
		values_mv(&dst, &kvs[i]);
		ended[i] =
		    pch->pch_iter_next_kvs(nodes[i], &kvs[i], iters[i], 1);
	    }
	    while (!ended[i] && keycmp_(dst.key, kvs[i].key) == 0);
	}
	if (the_app.atype == atype_mapreduce) {
	    if (the_app.mapreduce.vm) {
		rbkts_emit_kv(dst.key, (void *) dst.vals);
		memset(&dst, 0, sizeof(dst));
	    } else {
		the_app.mapreduce.reduce_func(dst.key, dst.vals, dst.len);
		// Reuse the values
		dst.len = 0;
	    }
	} else {		// mapgroup
	    rbkts_emit_kvs_len(dst.key, dst.vals, dst.len);
	    memset(&dst, 0, sizeof(dst));
	}
    }
    values_deep_free(&dst);
    for (int i = 0; i < n; i++)
	pch->pch_iter_end(iters[i]);
}

// reduce or group key/value pairs from different nodes
inline void reduce_or_group::do_kv(pc_handler_t * pch, void **nodes, int n,
		            group_emit_t meth, void *arg) {
    assert(instanceOf<pch_kvarray>(pch));
    uint64_t total_len = 0;
    for (int i = 0; i < n; i++)
	total_len += pch->pch_get_len(nodes[i]);
    keyval_t *arr = 0;
    if (n > 1) {
	arr = (keyval_t *) malloc(total_len * sizeof(keyval_t));
	int idx = 0;
	for (int i = 0; i < n; i++)
	    idx += pch->pch_copy_kv(nodes[i], &arr[idx]);
    } else {
	arr = (keyval_t *)pch->pch_get_arr_elems(nodes[0]);
    }
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
