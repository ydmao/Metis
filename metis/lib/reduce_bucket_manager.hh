#ifndef RBKTSMGR_H
#define RBKTSMGR_H

#include "mr-conf.hh"
#include "mr-types.hh"
#include "apphelper.hh"
#include "comparator.hh"
#include "psrs.hh"

template <typename T>
inline xarray<T> *merge_impl(xarray<xarray<void *> > &rb, size_t subsize,
                             int ncpus, int lcpu) {
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

struct reduce_bucket_manager {
    static reduce_bucket_manager *instance() {
        static reduce_bucket_manager instance;
        return &instance;
    }
    void init(int n) {
        rb_.resize(n);
        for (int i = 0; i < n; ++i)
            rb_[i].init();
        assert(pthread_key_create(&current_task_key_, NULL) == 0);
        set_current_reduce_task(0);
    }
    void destroy() {
        rb_.resize(0);
    }
    xarray<keyval_t> *as_kvarray(int p) {
        return (xarray<keyval_t> *)&rb_[p];
    }
    xarray<keyvals_len_t> *as_kvslen_array(int p) {
        return (xarray<keyvals_len_t> *)&rb_[p];
    }
    void emit(void *key, void *val) {
        xarray<keyval_t> *x = as_kvarray(current_task());
        keyval_t tmp(key, val);
        x->push_back(tmp);
    }
    void emit(void *key, void **vals, uint64_t len) {
        xarray<keyvals_len_t> *x = as_kvslen_array(current_task());
        keyvals_len_t tmp(key, vals, len);
        x->push_back(tmp);
    }
    void set_current_reduce_task(int ir) {
        assert(size_t(ir) < rb_.size());
        assert(pthread_setspecific(current_task_key_,
                                   (void *)intptr_t(ir)) == 0);
    }
    /** @brief: merge the output buckets of reduce phase, i.e. the final output.
        For psrs, the result is stored in rb_[0]; for mergesort, the result are
        spread in rb[0..(ncpus - 1)]. */
    void merge_reduced_buckets(int ncpus, int lcpu) {
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
  private:
    int current_task() {
        return intptr_t(pthread_getspecific(current_task_key_));
    }
    void shallow_free_buckets() {
        for (size_t i = 0; i < rb_.size(); ++i)
            rb_[i].shallow_free();
    }
    size_t subsize() {
        size_t n = 0;
        for (size_t i = 0; i < rb_.size(); ++i)
            n += rb_[i].size();
        return n;
    }
    template <typename C>
    void swap(int i, C *x) {
        if (!x)
            return;
        reinterpret_cast<C *>(&rb_[i])->swap(*x);
    }
    reduce_bucket_manager() {}
    xarray<xarray_base> rb_; // reduce buckets
    pthread_key_t current_task_key_;
};

#endif
