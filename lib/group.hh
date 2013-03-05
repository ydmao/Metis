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
#ifndef GROUP_HH_
#define GROUP_HH_ 1

#include "mr-types.hh"
#include "bench.hh"
#include "appbase.hh"
#include <assert.h>
#include <string.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif

template <typename C, typename F, typename KF>
inline void group_one_sorted(C &a, F &f, KF &kf) {
    // group and apply functor
    size_t n = a.size();
    keyvals_t kvs;
    for (size_t i = 0; i < n;) {
	kvs.key_ = a[i].key_;
        kvs.map_value_move(&a[i]);
        ++i;
        for (; i < n && !static_appbase::key_compare(kvs.key_, a[i].key_); ++i) {
            kf(a[i].key_);
	    kvs.map_value_move(&a[i]);
        }
        f(kvs);
    }
}

template <typename C, typename F, typename PC, typename KF>
inline void group_unsorted(C **a, int na, F &f, PC &pc, KF &kf) {
    if (na == 1) {
        a[0]->sort(pc);
        group_one_sorted(*a[0], f, kf);
    }
    if (na <= 1)
        return;
    size_t np = 0;
    for (int i = 0; i < na; i++)
        np += a[i]->size();
    C *one = new C;
    one->set_capacity(np);
    for (int i = 0; i < na; i++)
        one->append(*a[i]);
    one->sort(pc);
    group_one_sorted(*one, f, kf);
    delete one;
}

template <typename C, typename F, typename KF>
inline void group_sorted(C **nodes, int n, F &f, KF &kf) {
    if (!n)
        return;
    typename C::iterator it[JOS_NCPU];
    for (int i = 0; i < n; i++)
	 it[i] = nodes[i]->begin();
    int marks[JOS_NCPU];
    keyvals_t dst;
    while (1) {
	int min_idx = -1;
	bzero(marks, sizeof(marks));
	int m = 0;
	// Find minimum key
	for (int i = 0; i < n; ++i) {
	    if (it[i] == nodes[i]->end())
		continue;
	    int cmp = 0;
	    if (min_idx >= 0)
		cmp = static_appbase::key_compare(it[min_idx]->key_, it[i]->key_);
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
	dst.key_ = it[min_idx]->key_;
	for (int i = 0; i < n; ++i) {
	    if (marks[i] != m)
		continue;
	    dst.map_value_move(&(*it[i]));
            ++it[i];
	    for (; it[i] != nodes[i]->end() &&
                   static_appbase::key_compare(dst.key_, it[i]->key_) == 0; ++it[i]) {
                kf(it[i]->key_);
                it[i]->key_ = NULL;
		dst.map_value_move(&(*it[i]));
	    }
	}
        f(dst);
    }
}

#endif
