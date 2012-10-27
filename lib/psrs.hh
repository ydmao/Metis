#ifndef PSRS_H
#define PSRS_H

#include <algorithm>
#include "bench.hh"
#include "bsearch.hh"
#include "mergesort.hh"
#include "cpumap.hh"

template <typename C>
struct psrs {
    typedef typename C::element_type pair_type;
    void cpu_barrier(int me, int ncpus);
    C *do_psrs(xarray<C> &a, int ncpus, int me, pair_cmp_t pcmp);
    C *init(int me, size_t output_size) {
        assert(me == main_core && output_ == NULL && status == STOP);
        output_ = new C;
        output_->resize(output_size);
        return output_;
    }
    psrs() : status(STOP) {
        bzero(ready, sizeof(ready));
        deinit();
        lpairs_.resize(JOS_NCPU);
    }
  private:
    /* @brief: Divide a[start..end] into subarrays using [pivots[fp], pivots[lp]],
     * so that subsize[at + i] is the first element that is > pivots[i]
     */
    void divide(C &a, int start, int end, int *subsize,
                const pair_type *pivots, int fp, int lp, pair_cmp_t pcmp);
    C *copy_elem(xarray<C> &a, int dst_start, int dst_end);
    void mergesort(xarray<C *> &localpairs, int npairs, int *subsize, int me,
                   pair_type *out, int ncpus, pair_cmp_t pcmp);

    void deinit() {
        output_ = NULL;
	bzero(pivots_, sizeof(pivots_));
	bzero(subsize_, sizeof(subsize_));
	bzero(partsize_, sizeof(partsize_));
        lpairs_.zero();
    }
    void check_inited() {
        assert(output_ && status == STOP);
    }

    enum { STOP, START };
    union {
        char __pad[JOS_CLINE];
        volatile int v;
    } ready[JOS_NCPU];

    pair_type pivots_[JOS_NCPU * (JOS_NCPU - 1)];
    C *output_;
    int subsize_[JOS_NCPU * (JOS_NCPU + 1)];
    int partsize_[JOS_NCPU];
    xarray<C *> lpairs_;
    volatile int status;
};

template <typename C>
void psrs<C>::cpu_barrier(int me, int ncpus) {
    if (me != main_core) {
	while (status != START)
            ;
	ready[me].v = 1;
	mfence();
	while (status != STOP)
            ;
	ready[me].v = 0;
    } else {
	status = START;
	mfence();
	for (int i = 0; i < ncpus; ++i) {
	    if (i == main_core)
		continue;
	    while (!ready[i].v)
                ;
	}
	status = STOP;
	mfence();
	for (int i = 0; i < ncpus; ++i) {
	    if (i == main_core)
		continue;
	    while (ready[i].v)
                ;
	}
    }
}

template <typename C>
void psrs<C>::divide(C &a, int start, int end, int *subsize, const pair_type *pivots,
	             int fp, int lp, pair_cmp_t pcmp) {
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

template <typename C>
void psrs<C>::mergesort(xarray<C *> &per_core_pairs, int npairs, int *subsize,
                        int me, typename psrs<C>::pair_type *out,
	                int ncore, pair_cmp_t pcmp) {
    C a[JOS_NCPU];
    for (int i = 0; i < ncore; ++i) {
        int s = subsize[i * (ncore + 1) + me];
        int e = subsize[i * (ncore + 1) + me + 1];
        a[i].set_array(&per_core_pairs[i]->at(s), e - s);
    }
    C output;
    output.set_array(out, npairs);
    mergesort_impl(a, ncore, 0, 1, pcmp, output);
    // don't free the array! You guys don't own it!
    for (int i = 0; i < ncore; ++i)
        a[i].init();
    output.init();
}

/* Suppose all elements in all arrays of arr_parr are indexed globally.
 * Then this function copies the [needed_start, needed_end] range of
 * the global array into one.
 */
template <typename C>
C *psrs<C>::copy_elem(xarray<C> &a, int dst_start, int dst_end) {
    C *output = new C;
    output->resize(dst_end - dst_start + 1);
    int glb_start = 0;		// global index of first elements of current array
    int glb_end = 0;		// global index of last elements of current array
    int copied = 0;
    for (size_t i = 0; i < a.size(); ++i) {
	if (!a[i].size())
	    continue;
	glb_end = glb_start + a[i].size() - 1;
	if (glb_start <= dst_end && glb_end >= dst_start) {
	    // local index of first elements to be copied
	    int loc_start = std::max(dst_start, glb_start) - glb_start;
	    // local index of last elements to be copied
	    int loc_end = std::min(dst_end, glb_end) - glb_start;
            memcpy(&output->at(copied), &a[i].at(loc_start),
                   (loc_end - loc_start + 1) * C::elem_size());
	    copied += loc_end - loc_start + 1;
	}
	glb_start = glb_end + 1;
    }
    assert(copied == dst_end - dst_start + 1);
    return output;
}

/* sort the elements of an array of collections.
 * If doreduce, reduce on each partition and put the elements into rbuckets;
 * otherwise, put the output into the first array of acolls;
 */
template <typename C>
C *psrs<C>::do_psrs(xarray<C> &a, int ncpus, int me, pair_cmp_t pcmp) {
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
    int copied = localpairs->size();
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
    int rsize = (copied + ncpus - 1) / ncpus;
    // sends (p - 1) local pivots to main cpu
    for (int i = 0; i < ncpus - 1; i++) {
	if ((i + 1) * rsize < copied)
            pivots_[me * (ncpus - 1) + i].assign(localpairs->at((i + 1) * rsize));
	else
            pivots_[me * (ncpus - 1) + i].assign(localpairs->at(copied - 1));
    }
    cpu_barrier(me, ncpus);
    if (me == main_core) {
	// sort p * (p - 1) pivots.
	qsort(pivots_, ncpus * (ncpus - 1), sizeof(pair_type), pcmp);
	// select (p - 1) pivots into pivots[1 : (p - 1)]
	for (int i = 0; i < ncpus - 1; i++)
            pivots_[i + 1] = pivots_[i * ncpus + ncpus / 2];
	cpu_barrier(me, ncpus);
    } else
	cpu_barrier(me, ncpus);
    // divide the local list into p sublists by the (p - 1) pivots received from main cpu
    subsize_[me * (ncpus + 1)] = 0;
    subsize_[me * (ncpus + 1) + ncpus] = copied;
    divide(*localpairs, 0, copied - 1, &subsize_[me * (ncpus + 1)],
	   pivots_, 1, ncpus - 1, pcmp);
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
              me, &output_->at(output_offset), ncpus, pcmp);
    cpu_barrier(me, ncpus);
    localpairs->shallow_free();
    localpairs->set_array(&output_->at(output_offset), partsize_[me]);
    // apply a barrier before deinit to make sure no one is using output_
    cpu_barrier(me, ncpus);
    if (me == main_core)
        deinit();
    return localpairs;
}

#endif
