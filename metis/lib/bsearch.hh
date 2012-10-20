#ifndef BSEARCH_H
#define BSEARCH_H

namespace xsearch {

template <typename T>
inline T set_true(bool *x, const T &t) {
    *x = true;
    return t;
}

template <typename F, typename T>
int lower_bound(const T *key, const T *a, int n, const F &f, bool *bfound) {
    *bfound = false;
    if (!n) return 0;
    // invariants: the lower_bound is >= left
    int left = 0;
    int right = n - 1;
    while (left < right) {
	int mid = (left + right) / 2;
	int r = f(key, &a[mid]);
	if (!r)
	    return set_true(bfound, mid);
	else if (r < 0)
	    right = mid - 1;
	else
	    left = mid + 1;
    }
    int r = f(key, &a[left]);
    if (!r)
	return set_true(bfound, left);
    return left + (r > 0);
}

template <typename F, typename T>
int upper_bound(const T *key, const T *a, int n, const F &f) {
    bool found = false;
    int p = lower_bound(key, a, n, f, &found);
    return p + found;
}


};
#endif
