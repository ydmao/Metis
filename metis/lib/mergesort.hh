#ifndef MERGESORT_H
#define MERGESORT_H

#include "bench.hh"
#include "mr-types.hh"

/** @brief: Merge @a[@afirst + @astep * i] (0 <= i < @nmya), and output to @sized_output */
template <typename C, typename F>
void mergesort_impl(C *a, int nmya, int afirst, int astep, F &pcmp, C &sized_output) {
    uint32_t apos[nmya];
    bzero(apos, sizeof(apos));
    size_t nsorted = 0;
    while (nsorted < sized_output.size()) {
	int min_idx = 0;
	typename C::element_type *min_pair = NULL;
	for (int i = 0; i < nmya; i++) {
            C &ca = a[afirst + i * astep];
	    if (apos[i] == ca.size())
		continue;
	    if (min_pair == NULL || pcmp(min_pair, &ca[apos[i]]) > 0) {
		min_pair = &ca[apos[i]];
		min_idx = i;
	    }
	}
        sized_output[nsorted ++] = *min_pair;
	++apos[min_idx];
    }
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
    C *out = new C;
    if (np == 0)
	return out;
    out->resize(np);
    mergesort_impl(a, nmya, lcpu, ncpus, pcmp, *out);
    dprintf("merge_worker: cpu %d total_cpu %d (collections %d : nr-kvs %zu)\n",
	    lcpu, ncpus, na, np);
    return out;
}

#endif
