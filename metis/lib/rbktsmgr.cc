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

/** @brief: merge the output buckets of reduce phase, i.e. the final output.
 * For psrs, the result is stored in rb[0]; for mergesort, the result are
 * spread in rb[0..(ncpus - 1)]. */
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
    // apply a barrier before freeing xo to make sure not
    // one is using it anymore.
    psrs<C>::instance()->cpu_barrier(lcpu, ncpus);
    if (psrs<C>::main_cpu(lcpu)) {
        xo->shallow_free();
        delete xo;
    }
}

void reduce_bucket_manager::merge_and_reduce(xarray_base *a, int na,
                                             bool kvs, int ncpus, int lcpu) {
    assert(use_psrs && "Must use psrs!");
    const size_t np = subsize();
    if (kvs)
        psrs_and_reduce_impl<keyvals_t>(a, na, np, ncpus, lcpu,
                                        comparator::keyvals_pair_comp);
    else
        psrs_and_reduce_impl<keyval_t>(a, na, np, ncpus, lcpu,
                                       comparator::keyval_pair_comp);
    for (int i = 0; i < na; ++i)
        a[i].shallow_free();

    if (the_app.any.outcmp)
        merge_reduced_buckets(ncpus, lcpu);
    else {
        if (app_output_pair_type() == vt_keyval)
            cat_all<keyval_t>();
        else
            cat_all<keyvals_len_t>();
    }
}

