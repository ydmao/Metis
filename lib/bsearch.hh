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
#ifndef BSEARCH_HH_
#define BSEARCH_HH_

namespace xsearch {

template <typename T>
inline T set_true(bool *x, const T &t) {
    *x = true;
    return t;
}

template <typename F, typename T>
int lower_bound(const T *k, const T *a, int n, const F &f, bool *found) {
    *found = false;
    if (!n) return 0;
    int l = 0, r = n - 1;
    // invariant: the lower_bound is >= l
    while (l < r) {
	const int m = (l + r) / 2;
	const int c = f(k, &a[m]);
	if (!c)
	    return set_true(found, m);
	if (c < 0)
	    r = m - 1;
	else
	    l = m + 1;
    }
    if (l > r)
        return l;
    const int c = f(k, &a[l]);
    if (!c)
	return set_true(found, l);
    return l + (c > 0);
}

template <typename F, typename T>
int upper_bound(const T *key, const T *a, int n, const F &f) {
    bool found = false;
    int p = lower_bound(key, a, n, f, &found);
    return p + found;
}

};
#endif
