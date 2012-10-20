#ifndef BSEARCH_H
#define BSEARCH_H

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
