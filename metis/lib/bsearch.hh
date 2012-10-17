#ifndef BSEARCH_H
#define BSEARCH_H

#include "mr-types.hh"

//typedef int (*bsearch_cmp_t) (const void *, const void *);
// return the position of the first element that is greater than or eqaul to key.
template <typename F>
int bsearch_eq(const void *key, const void *base, int nelems, size_t size,
	       const F &keycmp, bool *bfound);
// return the position of the first element that is greater than key
template <typename F>
int bsearch_lar(const void *key, const void *base, int nelems, size_t size,
		const F &keycmp);

#define ELEM(idx) (void *)(uint64_t(base) + size * (idx))

template <typename F>
int bsearch_lar(const void *key, const void *base, int nelems, size_t size,
	    const F &keycmp)
{
    if (!nelems)
	return 0;
    int res = keycmp(key, ELEM(nelems - 1));
    if (res >= 0)
	return nelems;
    if (nelems == 1)
	return 0;
    if (nelems == 2) {
	if (keycmp(key, ELEM(0)) < 0)
	    return 0;
	return 1;
    }
    int left = 0;
    int right = nelems - 2;
    int mid;
    while (left < right) {
	mid = (left + right) / 2;
	res = keycmp(key, ELEM(mid));
	if (res >= 0)
	    left = mid + 1;
	else if (res < 0)
	    right = mid - 1;
    }
    res = keycmp(key, ELEM(left));
    if (res >= 0)
	return left + 1;
    return left;
}

template <typename F>
int bsearch_eq(const void *key, const void *base, int nelems, size_t size,
	       const F &keycmp, bool *bfound)
{
    *bfound = false;
    if (!nelems)
	return 0;
    int res = keycmp(key, ELEM(nelems - 1));
    if (!res) {
	*bfound = true;
	return nelems - 1;
    }
    if (res > 0)
	return nelems;
    if (nelems == 1)
	return 0;
    if (nelems == 2) {
	int res = keycmp(key, ELEM(0));
	if (res == 0) {
	    *bfound = true;
	    return 0;
	}
	if (res < 0)
	    return 0;
	return 1;
    }
    int left = 0;
    int right = nelems - 2;
    int mid;
    while (left < right) {
	mid = (left + right) / 2;
	res = keycmp(key, ELEM(mid));
	if (!res) {
	    *bfound = true;
	    return mid;
	} else if (res < 0)
	    right = mid - 1;
	else
	    left = mid + 1;
    }
    res = keycmp(key, ELEM(left));
    if (!res) {
	*bfound = true;
	return left;
    }
    if (res > 0)
	return left + 1;
    return left;
}
#endif
