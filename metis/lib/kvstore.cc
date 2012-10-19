#include <math.h>
#include <assert.h>
#include "kvstore.hh"
#include "reduce.hh"
#include "bench.hh"
#include "value_helper.hh"
#include "apphelper.hh"
#include "estimation.hh"
#include "mr-conf.hh"
#include "rbktsmgr.hh"
#include "comparator.hh"
#include "psrs.hh"
#include "map_bucket_manager.hh"
#include "btree.hh"

enum { index_appendbktmgr, index_btreebktmgr, index_arraybktmgr };

map_bucket_manager_base *create(int index) {
    switch (index) {
        case index_appendbktmgr:
#if SINGLE_APPEND_GROUP_MERGE_FIRST
            return new map_bucket_manager<false, keyval_arr_t, keyvals_t>;
#else
            return new map_bucket_manager<false, keyval_arr_t, keyval_t>;
#endif
        case index_btreebktmgr:
            return new map_bucket_manager<true, btree_type, keyvals_t>;
        case index_arraybktmgr:
            return new map_bucket_manager<true, keyvals_arr_t, keyvals_t>;
        default:
            assert(0);
    }
};

#ifdef FORCE_APPEND
// forced to use index_appendbkt
enum { def_imgr = index_appendbktmgr };
#else
// available options are index_arraybkt, index_appendbkt, index_btreebkt
enum { def_imgr = index_btreebktmgr };
#endif

static map_bucket_manager_base *the_bucket_manager = NULL;
static map_bucket_manager_base *backup_manager = NULL;
static int bsampling = 0;
static uint64_t nkeys_per_mapper = 0;
static uint64_t npairs_per_mapper = 0;

keycopy_t mrkeycopy = NULL;

static void
kvst_set_bktmgr(int idx)
{
    the_bucket_manager = create(idx);
}

void
kvst_sample_init(int rows, int cols)
{
    backup_manager = NULL;
    kvst_set_bktmgr(def_imgr);
    the_bucket_manager->init(rows, cols);
    est_init();
    bsampling = 1;
}

void
kvst_map_task_finished(int row)
{
    if (bsampling)
	est_task_finished(row);
}

uint64_t
kvst_sample_finished(int ntotal)
{
    int nvalid = 0;
    for (int i = 0; i < the_bucket_manager->nrow(); i++) {
	if (est_get_finished(i)) {
	    nvalid++;
	    uint64_t nkeys = 0;
	    uint64_t npairs = 0;
	    est_estimate(&nkeys, &npairs, i, ntotal);
	    nkeys_per_mapper += nkeys;
	    npairs_per_mapper += npairs;
	}
    }
    nkeys_per_mapper /= nvalid;
    npairs_per_mapper /= nvalid;
    backup_manager = the_bucket_manager;

    // Compute the estimated tasks
    uint64_t ntasks = nkeys_per_mapper / nkeys_per_bkt;
    while (1) {
	int prime = 1;
	for (int q = 2; q < sqrt((double) ntasks); q++) {
	    if (ntasks % q == 0) {
		prime = 0;
		break;
	    }
	}
	if (!prime) {
	    ntasks++;
	    continue;
	} else {
	    break;
	}
    };
    dprintf("Estimated %" PRIu64 " keys, %" PRIu64 " pairs, %"
	    PRIu64 " reduce tasks, %" PRIu64 " per bucket\n",
	    nkeys_per_mapper, npairs_per_mapper, ntasks,
	    nkeys_per_mapper / ntasks);
    bsampling = 0;
    return ntasks;
}

void
kvst_map_worker_init(int row)
{
    if (backup_manager) {
	assert(the_app.atype != atype_maponly);
	the_bucket_manager->rehash(row, backup_manager);
    }
}

void
kvst_init_map(int rows, int cols, int nsplits)
{
#ifdef FORCE_APPEND
    kvst_set_bktmgr(index_appendbktmgr);
#else
    if (the_app.atype == atype_maponly)
	kvst_set_bktmgr(index_appendbktmgr);
    else
	kvst_set_bktmgr(def_imgr);
#endif
    the_bucket_manager->init(rows, cols);
    reduce_bucket_manager::instance()->init(nsplits);
}

void
kvst_initialize(void)
{
    reduce_bucket_manager::instance()->destroy();
    if (the_bucket_manager)
        the_bucket_manager->destroy();
    if (backup_manager)
        backup_manager->destroy();
}

void
kvst_map_put(int row, void *key, void *val, size_t keylen, unsigned hash)
{
    the_bucket_manager->emit(row, key, val, keylen, hash);
}

void
kvst_set_util(key_cmp_t kcmp, keycopy_t kcp)
{
    comparator::set_key_compare(kcmp);
    mrkeycopy = kcp;
}

void
kvst_reduce_do_task(int row, int col)
{
    assert(the_app.atype != atype_maponly);
    reduce_bucket_manager::instance()->set_current_reduce_task(col);
    the_bucket_manager->do_reduce_task(col);
}

void
kvst_map_worker_finished(int row, int reduce_skipped)
{
    if (reduce_skipped) {
	assert(!bsampling);
	the_bucket_manager->prepare_merge(row);
    }
}

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

void
kvst_merge(int ncpus, int lcpu, int reduce_skipped)
{
    if (the_app.atype == atype_maponly || !reduce_skipped)
	reduce_bucket_manager::instance()->merge_reduced_buckets(ncpus, lcpu);
    else {
        // make sure we are using psrs so that after merge_reduced_buckets,
        // the final results is already in reduce bucket 0
        assert(use_psrs);  
	int n;
        bool kvs = false;
	xarray_base *a = the_bucket_manager->get_output(&n, &kvs);
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

void
kvst_reduce_put(void *key, void *val)
{
    reduce_bucket_manager::instance()->emit(key, val);
}
