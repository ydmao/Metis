#ifndef MERGESORT_H
#define MERGESORT_H

#include "bench.hh"
#include "mr-types.hh"

/** @brief: Merge @a[@afirst + @astep * i] (0 <= i < @nmya), and output to @sized_output */
template <typename C, typename F>
void mergesort_impl(C *a, int nmya, int afirst, int astep, F &pcmp, C &sized_output) {
    typedef typename C::iterator iterator_type;
    xarray<iterator_type> ai;
    for (int i = 0; i < nmya; ++i) {
        iterator_type mi = a[afirst + i * astep].begin();
        if (mi != mi.parent_end())
            ai.push_back(mi);
    }
    size_t nsorted = 0;
    while (nsorted < sized_output.size()) {
        assert(ai.size());
	int min_idx = 0;
	typename C::element_type *min_pair = ai[0].current();
	for (size_t i = 1; i < ai.size(); ++i)
	    if (pcmp(min_pair, ai[i].current()) > 0) {
		min_pair = ai[i].current();
		min_idx = i;
	    }
        sized_output[nsorted ++] = *min_pair;
        ++ai[min_idx];
        if (ai[min_idx] == ai[min_idx].parent_end())
            ai.remove(min_idx);
    }
    assert(!ai.size());
}

template <typename C, typename F>
C *mergesort(xarray<C> &in, int ncpus, int lcpu, F &pcmp) {
    return mergesort(in.array(), in.size(), ncpus, lcpu, pcmp);
}

template <typename C, typename F>
C *mergesort(C *a, int na, int ncpus, int lcpu, F &pcmp) {
    int nmya = na / ncpus + (lcpu < (na % ncpus));
    size_t np = 0;
    for (int i = 0; i < nmya; i++)
	np += a[lcpu + i * ncpus].size();
    C *out = new C(np);
    if (np == 0)
	return out;
    mergesort_impl(a, nmya, lcpu, ncpus, pcmp, *out);
    dprintf("merge_worker: cpu %d total_cpu %d (collections %d : nr-kvs %zu)\n",
 	    lcpu, ncpus, na, np);
    return out;
}

#endif
