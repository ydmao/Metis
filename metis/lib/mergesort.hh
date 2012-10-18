#ifndef MERGESORT_H
#define MERGESORT_H

#include "bench.hh"
#include "mr-types.hh"

template <typename C>
void mergesort(C *a, int n, int ncpus, int lcpu, pair_cmp_t pcmp) {
    int mycolls = n / ncpus;
    mycolls += (lcpu < (n % ncpus));
    size_t npairs = 0;
    for (int i = 0; i < mycolls; i++)
	npairs += a[lcpu + i * ncpus].size();
    if (npairs == 0)
	return;
    C out;
    out.resize(npairs);
    uint32_t *task_pos = (uint32_t *) calloc(mycolls, sizeof(uint32_t));
    size_t nsorted = 0;
    while (nsorted < npairs) {
	int min_idx = 0;
	typename C::element_type *min_pair = NULL;
	for (int i = 0; i < mycolls; i++) {
            C &ca = a[lcpu + i * ncpus];
	    if (task_pos[i] == ca.size())
		continue;
	    if (min_pair == NULL || pcmp(min_pair, &ca[task_pos[i]]) > 0) {
		min_pair = &ca[task_pos[i]];
		min_idx = i;
	    }
	}
        out[nsorted ++] = *min_pair;
	task_pos[min_idx]++;
    }
    free(task_pos);
    for (int i = 0; i < mycolls; i++)
        a[lcpu + i * ncpus].shallow_free();
    a[lcpu].swap(out);
    dprintf("merge_worker: cpu %d total_cpu %d (collections %d : nr-kvs %zu)\n",
	    lcpu, ncpus, n, npairs);
}

#endif
