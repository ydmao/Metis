#include <math.h>
#include <assert.h>
#include "metis_runtime.hh"
#include "reduce.hh"
#include "bench.hh"
#include "value_helper.hh"
#include "reduce_bucket_manager.hh"
#include "comparator.hh"
#include "psrs.hh"
#include "btree.hh"
#include "map_bucket_manager.hh"
#include "application.hh"

enum { expected_keys_per_bucket = 10 };

void metis_runtime::create_map_bucket_manager() {
    enum { index_append, index_btree, index_array };
#ifdef FORCE_APPEND
    int index = index_append;
#else
    int index = (the_app_->application_type() == atype_maponly) ? index_append : index_btree;
#endif
    assert(current_manager_ == NULL);
    switch (index) {
    case index_append:
#ifdef SINGLE_APPEND_GROUP_MERGE_FIRST
        current_manager_ = new map_bucket_manager<false, keyval_arr_t, keyvals_t>;
#else
        current_manager_ = new map_bucket_manager<false, keyval_arr_t, keyval_t>;
#endif
        break;
    case index_btree:
        current_manager_ = new map_bucket_manager<true, btree_type, keyvals_t>;
        break;
    case index_array:
        current_manager_ = new map_bucket_manager<true, keyvals_arr_t, keyvals_t>;
        break;
    default:
        assert(0);
    }
};

metis_runtime::~metis_runtime() {
    reset();
}

void metis_runtime::sample_init(int rows, int cols) {
    create_map_bucket_manager();
    current_manager_->init(rows, cols);
    bzero(e_, sizeof(e_));
    sampling_ = true;
}

void metis_runtime::map_task_finished(int row) {
    if (sampling_)
	e_[row].task_finished();
}

uint64_t metis_runtime::sample_finished(int ntotal) {
    assert(sampling_);
    uint64_t nk = 0;
    uint64_t np = 0;  // # of keys and pairs per mapper
    int nvalid = 0;  // # of workers that has sampled
    for (int i = 0; i < current_manager_->nrow(); ++i)
	if (e_[i].valid())
	    ++ nvalid, e_[i].inc_predict(&nk, &np, ntotal);
    nk /= nvalid;
    np /= nvalid;

    // Compute the # tasks as the closest prime
    uint64_t ntasks = nk / expected_keys_per_bucket;
    for (int q = 2; q < sqrt(double(ntasks)); ++q)
        if (ntasks % q == 0)
            ++ntasks, q = 1;  // restart
    sample_manager_ = current_manager_;
    current_manager_ = NULL;
    sampling_ = false;
    dprintf("Estimated %" PRIu64 " keys, %" PRIu64 " pairs, %"
	    PRIu64 " reduce tasks, %" PRIu64 " per bucket\n",
	    nk, np, ntasks, nk / ntasks);
    return ntasks;
}

void metis_runtime::map_worker_init(int row) {
    if (sample_manager_) {
	assert(the_app_->application_type() != atype_maponly);
	current_manager_->rehash(row, sample_manager_);
    }
}

void metis_runtime::init_map(int rows, int cols) {
    create_map_bucket_manager();
    current_manager_->init(rows, cols);
}

void metis_runtime::reset() {
    reduce_bucket_manager<keyval_t>::instance()->reset();
    reduce_bucket_manager<keyvals_len_t>::instance()->reset();
    if (current_manager_) {
        delete current_manager_;
        current_manager_ = NULL;
    }
    if (sample_manager_) {
        delete sample_manager_;
        sample_manager_ = NULL;
    }
    sampling_ = false;
}

void metis_runtime::map_emit(int row, void *key, void *val,
                             size_t keylen, unsigned hash) {
    if (sampling_) {
        bool newkey = current_manager_->emit(row, key, val, keylen, hash);
        e_[row].onepair(newkey);
    } else
        current_manager_->emit(row, key, val, keylen, hash);
}

void metis_runtime::reduce_do_task(int row, int col) {
    current_manager_->do_reduce_task(col);
}

void metis_runtime::map_worker_finished(int row, int reduce_skipped) {
    if (reduce_skipped) {
	assert(!sampling_);
	current_manager_->prepare_merge(row);
    }
}

void metis_runtime::merge(int nin, int ncpus, int lcpu, int reduce_skipped) {
    if (the_app_->application_type() == atype_maponly || !reduce_skipped)
	the_app_->get_reduce_bucket_manager()->merge_reduced_buckets(nin, ncpus, lcpu);
    else {
        current_manager_->merge_output_and_reduce(ncpus, lcpu);
        // merge reduced bucekts
	the_app_->get_reduce_bucket_manager()->merge_reduced_buckets(nin, ncpus, lcpu);
    }
}

