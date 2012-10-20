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
    if (!n)
	return 0;
    int r = f(key, &a[n - 1]);
    if (!r)
        return set_true(bfound, n - 1);
    if (r > 0)
	return n;
    if (n == 1)
	return 0;
    if (n == 2) {
	r = f(key, &a[0]);
	if (r == 0)
	    return set_true(bfound, 0);
	if (r < 0)
	    return 0;
	return 1;
    }
    int left = 0;
    int right = n - 2;
    int mid;
    while (left < right) {
	mid = (left + right) / 2;
	r = f(key, &a[mid]);
	if (!r)
	    return set_true(bfound, mid);
	else if (r < 0)
	    right = mid - 1;
	else
	    left = mid + 1;
    }
    r = f(key, &a[left]);
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
