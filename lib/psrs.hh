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
#ifndef PSRS_HH_
#define PSRS_HH_ 1

#include <algorithm>
#include "bench.hh"
#include "bsearch.hh"
#include "mergesort.hh"
#include "cpumap.hh"

template <typename C>
struct psrs {
    void cpu_barrier(int me, int ncpus);
    template <typename F>
    C *do_psrs(xarray<C> &a, int ncpus, int me, F &pcmp);
    C *init(int me, size_t output_size) {
        assert(me == main_core && output_ == NULL && status_ == STOP);
        return (output_ = new C(output_size));
    }
    psrs() : lpairs_(JOS_NCPU), status_(STOP) {
        bzero(ready_, sizeof(ready_));
        deinit();
    }
  private:
    typedef typename C::element_type pair_type;
    /* @brief: Divide a[start..end] into subarrays using [pivots[fp], pivots[lp]],
     * so that subsize[at + i] is the first element that is > pivots[i]
     */
    template <typename F>
    void divide(C &a, int start, int end, int *subsize,
                const pair_type *pivots, int fp, int lp, F &pcmp);
    C *copy_elem(xarray<C> &a, int dst_start, int dst_end);
    template <typename F>
    void mergesort(xarray<C *> &localpairs, int npairs, int *subsize, int me,
                   pair_type *out, int ncpus, F &pcmp);

    void deinit() {
        output_ = NULL;
	bzero(pivots_, sizeof(pivots_));
	bzero(subsize_, sizeof(subsize_));
	bzero(partsize_, sizeof(partsize_));
        lpairs_.zero();
    }
    void check_inited() {
        assert(output_ && status_ == STOP);
    }

    enum { STOP, START };
    union {
        char __pad[JOS_CLINE];
        volatile bool v;
    } ready_[JOS_NCPU];

    pair_type pivots_[JOS_NCPU * (JOS_NCPU - 1)];
    C *output_;
    int subsize_[JOS_NCPU * (JOS_NCPU + 1)];
    int partsize_[JOS_NCPU];
    xarray<C *> lpairs_;
    volatile int status_;
};

template <typename C>
void psrs<C>::cpu_barrier(int me, int ncore) {
    if (me != main_core) {
	while (status_ != START)
            ;
	ready_[me].v = true;
	mfence();
	while (status_ != STOP)
            ;
	ready_[me].v = false;
    } else {
	status_ = START;
	mfence();
	for (int i = 0; i < ncore; ++i)
	    if (i != main_core)
	        while (!ready_[i].v)
                    ;
	status_ = STOP;
	mfence();
	for (int i = 0; i < ncore; ++i)
	    if (i != main_core)
	        while (ready_[i].v)
                    ;
    }
}

template <typename C> template <typename F>
void psrs<C>::divide(C &a, int start, int end, int *subsize, const pair_type *pivots,
	             int fp, int lp, F &pcmp) {
    int mid = (fp + lp) / 2;
    const pair_type *pv = &pivots[mid];
    // Find first element that is > pv
    int pos = xsearch::upper_bound(pv, &a[start], end - start - 1, pcmp);
    pos += start;
    subsize[mid] = pos;
    if (fp < mid) {
	if (start < pos)
	    divide(a, start, pos - 1, subsize, pivots, fp, mid - 1, pcmp);
	else {
	    while (fp < mid)
		subsize[fp++] = start;
	}
    }
    if (mid < lp) {
	if (pos <= end)
	    divide(a, pos, end, subsize, pivots, mid + 1, lp, pcmp);
	else {
	    mid++;
	    while (mid <= lp)
		subsize[mid++] = end + 1;
	}
    }
}

template <typename C> template <typename F>
void psrs<C>::mergesort(xarray<C *> &per_core_pairs, int npairs, int *subsize,
                        int me, typename psrs<C>::pair_type *out,
	                int ncore, F &pcmp) {
    C a[JOS_NCPU];
    for (int i = 0; i < ncore; ++i) {
        int s = subsize[i * (ncore + 1) + me];
        int e = subsize[i * (ncore + 1) + me + 1];
        a[i].set_array(per_core_pairs[i]->at(s), e - s);
    }
    C output;
    output.set_array(out, npairs);
    mergesort_impl(a, ncore, 0, 1, pcmp, output);
    // don't free the array! You guys don't own it!
    for (int i = 0; i < ncore; ++i)
        a[i].init();
    output.init();
}

/* @brief: Consider all elements in all arrays of @a as a single array.  Then
 * this function return the [dst_start, dst_end] sub-array. */
template <typename C>
C *psrs<C>::copy_elem(xarray<C> &a, int dst_start, int dst_end) {
    C *output = new C(dst_end - dst_start + 1);
    int glb_start = 0;		// global index of first element of current array
    int glb_end = 0;		// global index of last element of current array
    size_t off = 0;
    for (size_t i = 0; i < a.size(); ++i) {
	if (!a[i].size())
	    continue;
	glb_end = glb_start + a[i].size() - 1;
	if (glb_start <= dst_end && glb_end >= dst_start) {
	    // local index of first elements to be copied
	    int loc_start = std::max(dst_start, glb_start) - glb_start;
	    // local index of last elements to be copied
	    int loc_end = std::min(dst_end, glb_end) - glb_start;
            int n = loc_end - loc_start + 1;
            a[i].copy(output->at(off), loc_start, n);
	    off += n;
	}
	glb_start = glb_end + 1;
    }
    assert(off == output->size());
    return output;
}

/* @brief: Sort the elements of an array of collections. 
 * @return: A equal share of the output that this core get. Note that the caller
 *          does not own the returned elements. */
template <typename C> template <typename F>
C *psrs<C>::do_psrs(xarray<C> &a, int ncpus, int me, F &pcmp) {
    if (me == main_core)
	check_inited();
    cpu_barrier(me, ncpus);

    // get the [start, end] subarray
    const int total_len = output_->size();
    const int w = (total_len + ncpus - 1) / ncpus;
    int start = w * me;
    int end = std::min(w * (me + 1), total_len) - 1;
    if (total_len < ncpus * ncpus * ncpus) {
	if (me != main_core)
	    return new C;
	start = 0;
	end = total_len - 1;
    }
    C *localpairs = copy_elem(a, start, end);
    lpairs_[me] = localpairs;
    // sort the subarray locally
    localpairs->sort(pcmp);
    if (ncpus == 1 || total_len < ncpus * ncpus * ncpus) {
	assert(me == main_core && size_t(total_len) == localpairs->size());
        // transfer memory ownership to output_
        output_->shallow_free();
        output_->set_array(localpairs->array(), localpairs->size());
        deinit();
        return localpairs;
    }
    // sends (p - 1) local pivots to main cpu
    const int interval = (localpairs->size() + ncpus - 1) / ncpus;
    for (size_t i = 0; i < size_t(ncpus - 1); ++i)
	if ((i + 1) * interval < localpairs->size())
            pivots_[me * (ncpus - 1) + i].assign(*localpairs->at((i + 1) * interval));
	else
            pivots_[me * (ncpus - 1) + i].assign(localpairs->back());
    cpu_barrier(me, ncpus);

    if (me == main_core) {
	// sort p * (p - 1) pivots.
	qsort(pivots_, ncpus * (ncpus - 1), sizeof(pair_type), pcmp);
	// select (p - 1) pivots into pivots[1 : (p - 1)]
	for (int i = 0; i < ncpus - 1; ++i)
            pivots_[i + 1] = pivots_[i * ncpus + ncpus / 2];
    }
    cpu_barrier(me, ncpus);

    // divide the local list into p sublists by the (p - 1) pivots received from main cpu
    subsize_[me * (ncpus + 1)] = 0;
    subsize_[me * (ncpus + 1) + ncpus] = localpairs->size();
    divide(*localpairs, 0, localpairs->size() - 1,
           &subsize_[me * (ncpus + 1)], pivots_, 1, ncpus - 1, pcmp);
    cpu_barrier(me, ncpus);

    // decides the size of the me-th sublist
    partsize_[me] = 0;
    for (int i = 0; i < ncpus; ++i) {
	int s = subsize_[i * (ncpus + 1) + me];
	int e = subsize_[i * (ncpus + 1) + me + 1];
	partsize_[me] += e - s;
    }
    cpu_barrier(me, ncpus);

    // merge each partition in parallel
    // determines the position in the final results for local partition
    int output_offset = 0;
    for (int i = 0; i < me; ++i)
        output_offset += partsize_[i];
    mergesort(lpairs_, partsize_[me], subsize_,
              me, output_->at(output_offset), ncpus, pcmp);
    cpu_barrier(me, ncpus);

    localpairs->shallow_free();
    localpairs->set_array(output_->at(output_offset), partsize_[me]);
    // apply a barrier before deinit to make sure no one is using output_
    cpu_barrier(me, ncpus);

    if (me == main_core)
        deinit();
    return localpairs;
}

#endif
