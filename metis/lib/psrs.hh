#ifndef PSRS_H
#define PSRS_H

#include <algorithm>
#include "bench.hh"
#include "reduce.hh"

template <typename C>
struct psrs {
    typedef typename C::element_type pair_type;
    static psrs<C> *instance() {
        static psrs<C> instance;
        return &instance;
    }
    void cpu_barrier(int lcpu, int ncpus);
    /* Divide array[start, end] into subarrays using [pivots[fp], pivots[lp]],
     * so that subsize[at + i] is the first element that is > pivots[i]
     */
    void sublists(pair_type *base, int start, int end, int *subsize,
                  const pair_type *pivots, int fp, int lp, pair_cmp_t pcmp);
    void reduce_or_group(pair_type **elems, int *subsize, int lcpu, int ncpus);
    C *copy_elems(C *arr_colls, int ncolls, int dst_start, int dst_end);
    void mergesort(pair_type **lpairs, int npairs, int *subsize, int lcpu,
                   pair_type *out, int ncpus, pair_cmp_t pcmp);
    void free_arr_colls(C *a, int n);
    void do_psrs(C *a, int n, int ncpus, int lcpu, pair_cmp_t pcmp, int doreduce);

  private:
    psrs() {
        init();
    }
    void init() {
        total_len = 0;
	pivots = new pair_type[JOS_NCPU * (JOS_NCPU - 1)];
	memset(pivots, 0, JOS_NCPU * (JOS_NCPU - 1) * C::elem_size());
        output = NULL;
	memset(subsize, 0, sizeof(subsize));
	memset(partsize, 0, sizeof(partsize));
 	memset(lpairs, 0, sizeof(lpairs));
        status = STOP;
    }

    enum { main_lcpu = 0 };
    // the cpu frees the reduce buckets. Can be any one but the main cpu
    enum { free_lcpu = 1}; 
    enum { STOP, START };
    union {
        char __pad[JOS_CLINE];
        volatile int v;
    } ready[JOS_NCPU];

    int total_len;
    pair_type *pivots;
    pair_type *output;
    int subsize[JOS_NCPU * (JOS_NCPU + 1)];
    int partsize[JOS_NCPU];
    pair_type *lpairs[JOS_NCPU];
    volatile int status;
};

template <typename C>
void psrs<C>::cpu_barrier(int lcpu, int ncpus)
{
    if (lcpu != main_lcpu) {
	while (status != START) ;
	ready[lcpu].v = 1;
	mfence();
	while (status != STOP) ;
	ready[lcpu].v = 0;
    } else {
	status = START;
	mfence();
	for (int i = 0; i < ncpus; i++) {
	    if (i == main_lcpu)
		continue;
	    while (!ready[i].v) ;
	}
	status = STOP;
	mfence();
	for (int i = 0; i < ncpus; i++) {
	    if (i == main_lcpu)
		continue;
	    while (ready[i].v)
                ;
	}
    }
}

template <typename C>
void psrs<C>::sublists(pair_type *base, int start, int end, int *subsize, const pair_type *pivots,
	               int fp, int lp, pair_cmp_t pcmp) {
    int mid = (fp + lp) / 2;
    const pair_type *pv = &pivots[mid];
    // Find first element that is > pv
    int pos = bsearch_lar(pv, &base[start], end - start - 1, C::elem_size(), pcmp);
    pos += start;
    subsize[mid] = pos;
    if (fp < mid) {
	if (start < pos)
	    sublists(base, start, pos - 1, subsize, pivots, fp, mid - 1, pcmp);
	else {
	    while (fp < mid)
		subsize[fp++] = start;
	}
    }
    if (mid < lp) {
	if (pos <= end)
	    sublists(base, pos, end, subsize, pivots, mid + 1, lp, pcmp);
	else {
	    mid++;
	    while (mid <= lp)
		subsize[mid++] = end + 1;
	}
    }
}

template <typename C>
void psrs<C>::mergesort(typename psrs<C>::pair_type **lpairs, int npairs, int *subsize,
                        int lcpu, typename psrs<C>::pair_type *out,
	                int ncpus, pair_cmp_t pcmp)
{
    uint32_t task_pos[JOS_NCPU];
    for (int i = 0; i < ncpus; i++)
	task_pos[i] = subsize[i * (ncpus + 1) + lcpu];
    int nsorted = 0;
    while (nsorted < npairs) {
	int min_idx = 0;
	pair_type *min_pair = NULL;
	for (int i = 0; i < ncpus; i++) {
	    if (task_pos[i] >= uint32_t(subsize[i * (ncpus + 1) + lcpu + 1]))
		continue;
	    pair_type *ca = lpairs[i];
	    if (min_pair == NULL || pcmp(min_pair, &ca[task_pos[i]]) > 0) {
		min_pair = &ca[task_pos[i]];
		min_idx = i;
	    }
	}
        out[nsorted ++] = *min_pair;
	task_pos[min_idx]++;
    }
}

/* input: lpairs
 * output: rbuckets
 */
template <typename C>
void psrs<C>::reduce_or_group(typename psrs<C>::pair_type **elems, int *subsize, int lcpu, int ncpus)
{
    C colls[JOS_NCPU];
    C *pcolls[JOS_NCPU];
    for (int i = 0; i < ncpus; i++) {
        const int subsize_offset = subsize[i * (ncpus + 1) + lcpu];
        const int elem_offset = subsize[subsize_offset];
        pair_type *elem = &elems[i][elem_offset];
        const int n = subsize[subsize_offset + 1] - elem_offset;
        colls[i].set_array(elem, n);
	pcolls[i] = &colls[i];
    }
    // TODO: fix this hack
    //if (sizeof(pair_type) == sizeof(keyvals_t))
    //    reduce_or_group::do_kvs(pcolls, ncpus);
    //else 
    //if (sizeof(pair_type) == sizeof(keyval_t))
    reduce_or_group_go(pcolls, ncpus, NULL, NULL);
    // don't free memory because colls doesn't own them
    for (int i = 0; i < ncpus; ++i)
        colls[i].pull_array();
}

/* Suppose all elements in all arrays of arr_parr are indexed globally.
 * Then this function copies the [needed_start, needed_end] range of
 * the global array into one.
 */
template <typename C>
C *psrs<C>::copy_elems(C *arr_colls, int ncolls, int dst_start, int dst_end) {
    C *output = new C;
    output->resize(dst_end - dst_start + 1);
    int glb_start = 0;		// global index of first elements of current array
    int glb_end = 0;		// global index of last elements of current array
    int copied = 0;
    for (int i = 0; i < ncolls; ++i) {
	C *parr = &arr_colls[i];
	if (!parr->size())
	    continue;
	glb_end = glb_start + parr->size() - 1;
	if (glb_start <= dst_end && glb_end >= dst_start) {
	    // local index of first elements to be copied
	    int loc_start = std::max(dst_start, glb_start) - glb_start;
	    // local index of last elements to be copied
	    int loc_end = std::min(dst_end, glb_end) - glb_start;
            memcpy(&(*output)[copied], &(*parr)[loc_start],
                   (loc_end - loc_start + 1) * C::elem_size());
	    copied += loc_end - loc_start + 1;
	}
	glb_start = glb_end + 1;
    }
    assert(copied == dst_end - dst_start + 1);
    return output;
}

template <typename C>
void psrs<C>::free_arr_colls(C *a, int n)
{
    for (int i = 0; i < n; ++i)
        a[i].shallow_free();
}

/* sort the elements of an array of collections.
 * If doreduce, reduce on each partition and put the elements into rbuckets;
 * otherwise, put the output into the first array of acolls;
 */
template <typename C>
void psrs<C>::do_psrs(C *a, int n, int ncpus, int lcpu, pair_cmp_t pcmp, int doreduce)
{
    if (lcpu == main_lcpu) {
	init();
	total_len = 0;
	for (int i = 0; i < n; ++i)
	    total_len += a[i].size();
    }
    cpu_barrier(lcpu, ncpus);
    // get the [start, end] subarray
    int w = (total_len + ncpus - 1) / ncpus;
    int start = w * lcpu;
    int end = w * (lcpu + 1) - 1;
    if (end >= total_len)
	end = total_len - 1;
    if (total_len < ncpus * ncpus * ncpus) {
	if (lcpu != main_lcpu)
	    return;
	start = 0;
	end = total_len - 1;
    }
    C *localpairs = copy_elems(a, n, start, end);
    int copied = localpairs->size();
    lpairs[lcpu] = localpairs->array();
    // sort the subarray locally
    localpairs->sort(pcmp);
    if (ncpus == 1 || total_len < ncpus * ncpus * ncpus) {
	assert(lcpu == main_lcpu && size_t(total_len) == localpairs->size());
	free_arr_colls(a, n);
	if (!doreduce) {
            a[0].pull_array();
            a[0].swap(*localpairs);
	} else {
	    subsize[0] = 0;
	    subsize[1] = total_len;
	    reduce_or_group(lpairs, subsize, lcpu, 1);
            localpairs->shallow_free();
	}
        delete localpairs;
	return;
    }
    int rsize = (copied + ncpus - 1) / ncpus;
    // sends (p - 1) local pivots to main cpu
    for (int i = 0; i < ncpus - 1; i++) {
	if ((i + 1) * rsize < copied)
            pivots[lcpu * (ncpus - 1) + i].assign(localpairs->at((i + 1) * rsize));
	else
            pivots[lcpu * (ncpus - 1) + i].assign(localpairs->at(copied - 1));
    }
    cpu_barrier(lcpu, ncpus);
    if (lcpu == main_lcpu) {
	// sort p * (p - 1) pivots.
	qsort(pivots, ncpus * (ncpus - 1), sizeof(pair_type), pcmp);
	// select (p - 1) pivots into pivots[1 : (p - 1)]
	for (int i = 0; i < ncpus - 1; i++)
            pivots[i + 1] = pivots[i * ncpus + ncpus / 2];
	cpu_barrier(lcpu, ncpus);
    } else {
	if (lcpu == free_lcpu)
	    free_arr_colls(a, n);
	cpu_barrier(lcpu, ncpus);
    }
    // divide the local list into p sublists by the (p - 1) pivots received from main cpu
    subsize[lcpu * (ncpus + 1)] = 0;
    subsize[lcpu * (ncpus + 1) + ncpus] = copied;
    sublists(localpairs->array(), 0, copied - 1, &subsize[lcpu * (ncpus + 1)],
	     pivots, 1, ncpus - 1, pcmp);
    cpu_barrier(lcpu, ncpus);
    // decides the size of the lcpu-th sublist
    partsize[lcpu] = 0;
    for (int i = 0; i < ncpus; i++) {
	int start = subsize[i * (ncpus + 1) + lcpu];
	int end = subsize[i * (ncpus + 1) + lcpu + 1];
	partsize[lcpu] += end - start;
    }
    if (lcpu == main_lcpu && !doreduce) {
	// allocate and set the output
        a[0].pull_array();
        a[0].resize(total_len);
        output = a[0].array();
    }
    cpu_barrier(lcpu, ncpus);
    // merge (and reduce if required) each partition in parallel
    if (!doreduce) {
	// determines the position in the final results for local partition
	int start_pos = 0;
	for (int i = 0; i < lcpu; i++)
	    start_pos += partsize[i];
	mergesort(reinterpret_cast<pair_type **>(lpairs),
                  partsize[lcpu], reinterpret_cast<int *>(subsize),
                  lcpu, &output[start_pos], ncpus, pcmp);
    } else
	reduce_or_group((pair_type **)lpairs, (int *) subsize, lcpu, ncpus);
    cpu_barrier(lcpu, ncpus);
    localpairs->shallow_free();
    delete localpairs;
}
#endif
