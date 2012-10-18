#ifndef MERGESORT_H
#define MERGESORT_H

#include "bench.hh"
#include "mr-types.hh"

/** @brief: Merge @a[@afirst + @astep * i] (0 <= i < @nmya), and output to @sized_output */
template <typename C>
void mergesort_impl(C *a, int nmya, int afirst, int astep, pair_cmp_t pcmp, C &sized_output) {
    if (!sized_output.size())
        return;
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
	apos[min_idx]++;
    }
}

template <typename C>
void mergesort(C *a, int na, int ncpus, int lcpu, pair_cmp_t pcmp) {
    int nmya = na / ncpus + (lcpu < (na % ncpus));
    size_t npairs = 0;
    for (int i = 0; i < nmya; i++)
	npairs += a[lcpu + i * ncpus].size();
    if (npairs == 0)
	return;
    C out;
    out.resize(npairs);
    mergesort_impl(a, nmya, lcpu, ncpus, pcmp, out);
    for (int i = 0; i < nmya; i++)
        a[lcpu + i * ncpus].shallow_free();
    a[lcpu].swap(out);
    dprintf("merge_worker: cpu %d total_cpu %d (collections %d : nr-kvs %zu)\n",
	    lcpu, ncpus, na, npairs);
}

#endif
