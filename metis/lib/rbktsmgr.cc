#include <assert.h>
#include <string.h>
#include "psrs.hh"
#include "bench.hh"
#include "mr-conf.hh"
#include "mergesort.hh"
#include "rbktsmgr.hh"

key_cmp_t JSHARED_ATTR reduce_bucket_manager::keycmp_ = NULL;
extern app_arg_t the_app;
JTLS int reduce_bucket_manager::cur_task_;

void reduce_bucket_manager::set_key_cmp(key_cmp_t keycmp) {
    keycmp_ = keycmp;
}

void reduce_bucket_manager::init(int n) {
    rb_.resize(n);
}

void reduce_bucket_manager::destroy() {
    rb_.resize(0);
}

xarray_base *reduce_bucket_manager::get(int p) {
    return &rb_[p];
}

void reduce_bucket_manager::merge(int ncpus, int lcpu) {
    if (app_output_pair_type() == vt_keyval) {
        xarray<keyval_t> *a = (xarray<keyval_t> *)rb_.array();
        if (use_psrs)
	    psrs<xarray<keyval_t> >::instance()->do_psrs(
                    a, rb_.size(), ncpus, lcpu, pair_cmp, 0);
        else
            mergesort(a, rb_.size(), ncpus, lcpu, pair_cmp);
    } else if (app_output_pair_type() == vt_keyvals_len) {
        xarray<keyvals_len_t> *a = (xarray<keyvals_len_t> *)rb_.array();
        if (use_psrs)
             psrs<xarray<keyvals_len_t> >::instance()->do_psrs(
                    a, rb_.size(), ncpus, lcpu, pair_cmp, 0);
        else
	    mergesort(a, rb_.size(), ncpus, lcpu, pair_cmp);
    }
}

void reduce_bucket_manager::merge_reduce(xarray_base *a, int n,
                                         bool kvs, int ncpus, int lcpu) {
        assert(use_psrs);
        set_current_reduce_task(lcpu);
        if (kvs)
            psrs<xarray<keyvals_t> >::instance()->do_psrs(
                (xarray<keyvals_t> *)a, n, ncpus, lcpu, keyvals_pair_cmp, 1);
        else
            psrs<xarray<keyval_t> >::instance()->do_psrs(
                (xarray<keyval_t> *)a, n, ncpus, lcpu, keyval_pair_cmp, 1);

        if (the_app.any.outcmp) {
            if (app_output_pair_type() == vt_keyval)
                psrs<xarray<keyval_t> >::instance()->do_psrs(
                        (xarray<keyval_t> *)rb_.array(), rb_.size(), ncpus, lcpu, pair_cmp, 0);
            else
                psrs<xarray<keyvals_len_t> >::instance()->do_psrs(
                        (xarray<keyvals_len_t> *)rb_.array(), rb_.size(), ncpus, lcpu, pair_cmp, 0);
        }
        /* cat the reduce buckets to produce the final results. It is safe
         * for cpu 0 to use all reduce buckets because psrs applies a barrier
         * across all reduce workers. */
        if (lcpu == 0) {
            if (app_output_pair_type() == vt_keyval)
                cat_all<keyval_t>();
            else
                cat_all<keyvals_len_t>();
        }
}

