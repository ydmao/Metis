#include <assert.h>
#include <string.h>
#include "psrs.hh"
#include "bench.hh"
#include "mr-conf.hh"
#include "mergesort.hh"
#include "rbktsmgr.hh"
#include "reduce.hh"

extern app_arg_t the_app;
JTLS int reduce_bucket_manager::cur_task_;

void reduce_bucket_manager::init(int n) {
    rb_.resize(n);
}

void reduce_bucket_manager::destroy() {
    rb_.resize(0);
}

xarray<void *> *reduce_bucket_manager::get(int p) {
    return &rb_[p];
}

template <typename T>
xarray<T> *merge_impl(xarray<xarray<void *> > &rb, size_t subsize, int ncpus, int lcpu) {
    typedef xarray<T> C;
    C *a = (C *)rb.array();
    if (!use_psrs)
        return mergesort(a, rb.size(), ncpus, lcpu,
                         comparator::final_output_pair_comp);
    C *xo = NULL;
    if (psrs<C>::main_cpu(lcpu)) {
        xo = new C;
        xo->resize(subsize);
        psrs<C>::instance()->init(xo);
    }
    psrs<C>::instance()->do_psrs(a, rb.size(), ncpus, lcpu,
                comparator::final_output_pair_comp);
    return (psrs<C>::main_cpu(lcpu)) ? xo :NULL;
}

void reduce_bucket_manager::merge_reduced_buckets(int ncpus, int lcpu) {
    if (app_output_pair_type() == vt_keyval) {
        xarray<keyval_t> *xo = merge_impl<keyval_t>(rb_, subsize(), ncpus, lcpu);
        shallow_free_buckets();
        swap(lcpu, xo);
        delete xo;
    } else if (app_output_pair_type() == vt_keyvals_len) {
        xarray<keyvals_len_t> *xo = merge_impl<keyvals_len_t>(rb_, subsize(), ncpus, lcpu);
        shallow_free_buckets();
        swap(lcpu, xo);
        delete xo;
    }
}

