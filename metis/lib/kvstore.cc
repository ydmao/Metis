#include <math.h>
#include <assert.h>
#include "kvstore.hh"
#include "reduce.hh"
#include "bench.hh"
#include "value_helper.hh"
#include "apphelper.hh"
#include "estimation.hh"
#include "mr-conf.hh"
#include "reduce_bucket_manager.hh"
#include "comparator.hh"
#include "psrs.hh"
#include "btree.hh"
#include "map_bucket_manager.hh"

enum { index_appendbktmgr, index_btreebktmgr, index_arraybktmgr };
#ifdef FORCE_APPEND
// forced to use index_appendbkt
enum { def_imgr = index_appendbktmgr };
#else
// available options are index_arraybkt, index_appendbkt, index_btreebkt
enum { def_imgr = index_btreebktmgr };
#endif

template <typename T, typename F>
void psrs_and_reduce_impl(xarray_base *a, int na, size_t np, int ncpus, int lcpu, F &f) {
    typedef xarray<T> C;
    C *xo = NULL;
    if (psrs<C>::main_cpu(lcpu)) {
        xo = new C;
        xo->resize(np);
        psrs<C>::instance()->init(xo);
    }
    // reduce the output of psrs
    reduce_bucket_manager::instance()->set_current_reduce_task(lcpu);
    if (C *out = psrs<C>::instance()->do_psrs((C *)a, na, ncpus, lcpu, f))
        group_one_sorted(*out, reduce_emit_functor::instance());
    // apply a barrier before freeing xo to make sure no
    // one is accessing xo anymore.
    psrs<C>::instance()->cpu_barrier(lcpu, ncpus);
    if (psrs<C>::main_cpu(lcpu)) {
        xo->shallow_free();
        delete xo;
    }
}

void metis_runtime::set_map_bucket_manager(int index) {
    switch (index) {
        case index_appendbktmgr:
#if SINGLE_APPEND_GROUP_MERGE_FIRST
            current_manager_ = new map_bucket_manager<false, keyval_arr_t, keyvals_t>;
#else
            current_manager_ = new map_bucket_manager<false, keyval_arr_t, keyval_t>;
#endif
            break;
        case index_btreebktmgr:
            current_manager_ = new map_bucket_manager<true, btree_type, keyvals_t>;
            break;
        case index_arraybktmgr:
            current_manager_ = new map_bucket_manager<true, keyvals_arr_t, keyvals_t>;
            break;
        default:
            assert(0);
    }
};

void metis_runtime::sample_init(int rows, int cols) {
    sample_manager_ = NULL;
    set_map_bucket_manager(def_imgr);
    current_manager_->init(rows, cols);
    est_init();
    sampling_ = true;
}

void metis_runtime::map_task_finished(int row) {
    if (sampling_)
	est_task_finished(row);
}

uint64_t metis_runtime::sample_finished(int ntotal) {
    uint64_t nkeys_per_mapper = 0;
    uint64_t npairs_per_mapper = 0;
    int nvalid = 0;
    for (int i = 0; i < current_manager_->nrow(); ++i)
	if (est_get_finished(i)) {
	    nvalid++;
	    uint64_t nkeys = 0;
	    uint64_t npairs = 0;
	    est_estimate(&nkeys, &npairs, i, ntotal);
	    nkeys_per_mapper += nkeys;
	    npairs_per_mapper += npairs;
	}
    nkeys_per_mapper /= nvalid;
    npairs_per_mapper /= nvalid;
    sample_manager_ = current_manager_;
    current_manager_ = NULL;

    // Compute the estimated tasks
    uint64_t ntasks = nkeys_per_mapper / nkeys_per_bkt;
    while (1) {
	bool prime = true;
	for (int q = 2; q < sqrt(double(ntasks)); ++q)
	    if (ntasks % q == 0) {
		prime = false;
		break;
	    }
	if (!prime) {
	    ntasks++;
	    continue;
	} else
	    break;
    };
    dprintf("Estimated %" PRIu64 " keys, %" PRIu64 " pairs, %"
	    PRIu64 " reduce tasks, %" PRIu64 " per bucket\n",
	    nkeys_per_mapper, npairs_per_mapper, ntasks,
	    nkeys_per_mapper / ntasks);
    sampling_ = false;
    return ntasks;
}

void metis_runtime::map_worker_init(int row) {
    if (sample_manager_) {
	assert(the_app.atype != atype_maponly);
	current_manager_->rehash(row, sample_manager_);
    }
}

void metis_runtime::init_map(int rows, int cols, int nsplits) {
#ifdef FORCE_APPEND
    set_map_bucket_manager(index_appendbktmgr);
#else
    if (the_app.atype == atype_maponly)
	set_map_bucket_manager(index_appendbktmgr);
    else
	set_map_bucket_manager(def_imgr);
#endif
    current_manager_->init(rows, cols);
    reduce_bucket_manager::instance()->init(nsplits);
}

void metis_runtime::initialize(void) {
    reduce_bucket_manager::instance()->destroy();
    if (current_manager_)
        current_manager_->destroy();
    if (sample_manager_)
        sample_manager_->destroy();
}

void metis_runtime::map_emit(int row, void *key, void *val,
                             size_t keylen, unsigned hash) {
    current_manager_->emit(row, key, val, keylen, hash);
}

void metis_runtime::set_util(key_cmp_t kcmp, keycopy_t kcp) {
    comparator::set_key_compare(kcmp);
    app_set_util(kcp);
}

void metis_runtime::reduce_do_task(int row, int col) {
    assert(the_app.atype != atype_maponly);
    reduce_bucket_manager::instance()->set_current_reduce_task(col);
    current_manager_->do_reduce_task(col);
}

void metis_runtime::map_worker_finished(int row, int reduce_skipped) {
    if (reduce_skipped) {
	assert(!sampling_);
	current_manager_->prepare_merge(row);
    }
}

void metis_runtime::merge(int ncpus, int lcpu, int reduce_skipped) {
    if (the_app.atype == atype_maponly || !reduce_skipped)
	reduce_bucket_manager::instance()->merge_reduced_buckets(ncpus, lcpu);
    else {
        // make sure we are using psrs so that after merge_reduced_buckets,
        // the final results is already in reduce bucket 0
        assert(use_psrs);  
	int n;
        bool kvs = false;
	xarray_base *a = current_manager_->get_output(&n, &kvs);
        // merge using psrs, and do the reduce
        size_t np = 0;
        for (int i = 0; i < n; ++i)
            np += a[i].size();
        if (kvs)
            psrs_and_reduce_impl<keyvals_t>(a, n, np, ncpus, lcpu,
                                            comparator::keyvals_pair_comp);
        else
            psrs_and_reduce_impl<keyval_t>(a, n, np, ncpus, lcpu,
                                           comparator::keyval_pair_comp);
        for (int i = 0; i < n; ++i)
            a[i].shallow_free();
        // merge reduced bucekts
	reduce_bucket_manager::instance()->merge_reduced_buckets(ncpus, lcpu);
    }
}

void metis_runtime::reduce_emit(void *key, void *val) {
    reduce_bucket_manager::instance()->emit(key, val);
}
